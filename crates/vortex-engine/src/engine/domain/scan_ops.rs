use super::*;

const SCAN_CURSOR_SHARD_SHIFT: u32 = 32;
const DEFAULT_RANDOM_SEED: u64 = 0xDEAD_BEEF_CAFE_BABE;
pub(crate) const SCAN_MAX_RESULTS_PER_CALL: usize = 1024;
pub(crate) const SCAN_MAX_SLOTS_PER_CALL: usize = 2048;
pub(crate) const KEYS_MAX_RESULTS_PER_CALL: usize = SCAN_MAX_RESULTS_PER_CALL;

struct ScanTableProgress {
    next_slot: usize,
    slots_scanned: usize,
    budget_exhausted: bool,
}

fn scan_table_slots(
    table: &SwissTable,
    start_slot: usize,
    pattern: Option<&[u8]>,
    count: usize,
    slot_budget: usize,
    type_filter: Option<&[u8]>,
    now_nanos: u64,
    results: &mut Vec<VortexKey>,
) -> ScanTableProgress {
    let total_slots = table.total_slots();
    if table.is_empty() || start_slot >= total_slots {
        return ScanTableProgress {
            next_slot: total_slots,
            slots_scanned: 0,
            budget_exhausted: false,
        };
    }

    let pattern_filter = match pattern {
        Some(b"*") | None => None,
        Some(pattern) => Some(pattern),
    };

    let mut slot = start_slot;
    let mut slots_scanned = 0usize;
    while slot < total_slots && slots_scanned < slot_budget {
        let Some((key, value)) = table.slot_key_value(slot) else {
            slot += 1;
            slots_scanned += 1;
            continue;
        };
        let ttl = table.slot_entry_ttl(slot);
        if ttl != 0 && ttl <= now_nanos {
            slot += 1;
            slots_scanned += 1;
            continue;
        }

        if let Some(pattern) = pattern_filter {
            if !glob_match(pattern, key) {
                slot += 1;
                slots_scanned += 1;
                continue;
            }
        }
        if let Some(filter) = type_filter {
            if !filter.eq_ignore_ascii_case(value.type_name().as_bytes()) {
                slot += 1;
                slots_scanned += 1;
                continue;
            }
        }

        results.push(VortexKey::from_bytes(key));
        slot += 1;
        slots_scanned += 1;
        if results.len() >= count {
            return ScanTableProgress {
                next_slot: slot,
                slots_scanned,
                budget_exhausted: false,
            };
        }
    }

    ScanTableProgress {
        next_slot: slot,
        slots_scanned,
        budget_exhausted: slot < total_slots,
    }
}

fn collect_matching_keys_limited(
    table: &SwissTable,
    pattern: &[u8],
    now_nanos: u64,
    limit: usize,
    results: &mut Vec<VortexKey>,
) -> bool {
    let match_all = pattern == b"*";
    for slot in 0..table.total_slots() {
        let Some((key, _value)) = table.slot_key_value(slot) else {
            continue;
        };
        let ttl = table.slot_entry_ttl(slot);
        if ttl != 0 && ttl <= now_nanos {
            continue;
        }
        if match_all || glob_match(pattern, key) {
            if results.len() >= limit {
                return true;
            }
            results.push(VortexKey::from_bytes(key));
        }
    }
    false
}

fn random_live_key_from_table(table: &SwissTable, seed: u64, now_nanos: u64) -> Option<VortexKey> {
    if table.is_empty() {
        return None;
    }

    let total_slots = table.total_slots();
    let mask = total_slots - 1;
    let mut rng = if seed == 0 { DEFAULT_RANDOM_SEED } else { seed };
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    let mut slot = (rng as usize) & mask;

    for _ in 0..total_slots {
        if let Some((key, _value)) = table.slot_key_value(slot) {
            let ttl = table.slot_entry_ttl(slot);
            if ttl == 0 || ttl > now_nanos {
                return Some(VortexKey::from_bytes(key));
            }
        }
        slot = (slot + 1) & mask;
    }

    None
}

fn encode_scan_cursor(shard_index: usize, slot_index: usize) -> u64 {
    ((shard_index as u64) << SCAN_CURSOR_SHARD_SHIFT) | slot_index as u64
}

fn decode_scan_cursor(cursor: u64) -> (usize, usize) {
    (
        (cursor >> SCAN_CURSOR_SHARD_SHIFT) as usize,
        (cursor & 0xFFFF_FFFF) as usize,
    )
}

impl ConcurrentKeyspace {
    pub(crate) fn scan_keys(
        &self,
        cursor: u64,
        pattern: Option<&[u8]>,
        count: usize,
        type_filter: Option<&[u8]>,
        now_nanos: u64,
    ) -> (u64, Vec<VortexKey>) {
        let count = count.clamp(1, SCAN_MAX_RESULTS_PER_CALL);
        let (mut shard_index, mut slot_index) = decode_scan_cursor(cursor);
        let shard_count = self.num_shards();
        if shard_index >= shard_count {
            shard_index = 0;
            slot_index = 0;
        }

        let mut results = Vec::with_capacity(count);
        let mut remaining_slot_budget = SCAN_MAX_SLOTS_PER_CALL;
        for current_shard in shard_index..shard_count {
            let guard = self.read_shard_by_index(current_shard);
            let start_slot = if current_shard == shard_index {
                slot_index
            } else {
                0
            };
            let progress = scan_table_slots(
                &guard,
                start_slot,
                pattern,
                count,
                remaining_slot_budget,
                type_filter,
                now_nanos,
                &mut results,
            );
            remaining_slot_budget = remaining_slot_budget.saturating_sub(progress.slots_scanned);

            if results.len() >= count || progress.budget_exhausted || remaining_slot_budget == 0 {
                if progress.next_slot < guard.total_slots() {
                    return (
                        encode_scan_cursor(current_shard, progress.next_slot),
                        results,
                    );
                }
                if current_shard + 1 < shard_count {
                    return (encode_scan_cursor(current_shard + 1, 0), results);
                }
                return (0, results);
            }
        }

        (0, results)
    }

    pub(crate) fn keys_matching_limited(
        &self,
        pattern: &[u8],
        limit: usize,
        now_nanos: u64,
    ) -> (Vec<VortexKey>, bool) {
        let mut results = Vec::with_capacity(limit.min(128));
        for shard_index in 0..self.num_shards() {
            let guard = self.read_shard_by_index(shard_index);
            let limit_exceeded =
                collect_matching_keys_limited(&guard, pattern, now_nanos, limit, &mut results);
            if limit_exceeded {
                return (results, true);
            }
        }
        (results, false)
    }

    pub(crate) fn random_key(&self, seed: u64, now_nanos: u64) -> Option<VortexKey> {
        let shard_count = self.num_shards();
        if shard_count == 0 {
            return None;
        }

        let start_shard = (seed as usize) & (shard_count - 1);
        for offset in 0..shard_count {
            let shard_index = (start_shard + offset) & (shard_count - 1);
            let guard = self.read_shard_by_index(shard_index);
            if let Some(key) = random_live_key_from_table(&guard, seed ^ offset as u64, now_nanos) {
                return Some(key);
            }
        }
        None
    }
}
