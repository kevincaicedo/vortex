use smallvec::SmallVec;
use vortex_common::VortexKey;

use crate::effects::{AofRecord, AofRecords};
use crate::eviction::{
    EVICTION_MAX_SHARDS_PER_ADMISSION, EVICTION_SWEEP_WINDOW, EvictionPolicy, next_random_u64,
};
use crate::table::{SwissTable, TableHash};

use super::{AofLsn, ConcurrentKeyspace, ExpiryTransition, MutationFeatures};

#[derive(Debug)]
pub(crate) struct EvictedKey {
    pub(crate) lsn: AofLsn,
    pub(crate) key: VortexKey,
}

pub(crate) type EvictedKeys = Option<Box<[EvictedKey]>>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct EvictionScanReport {
    pub(super) shards_scanned: usize,
    pub(super) slots_sampled: usize,
    pub(super) bytes_freed: usize,
    pub(super) oom_after_scan: bool,
}

#[derive(Debug, Default)]
pub struct EvictionMaintenanceSlice {
    pub aof_records: AofRecords,
    pub shards_scanned: usize,
    pub slots_sampled: usize,
    pub bytes_freed: usize,
    pub oom_after_scan: bool,
}

#[derive(Debug, Default)]
struct EvictionEffects {
    expiry_transitions: SmallVec<[(usize, ExpiryTransition); 4]>,
    watch_invalidations: SmallVec<[(VortexKey, TableHash); 4]>,
    aof_records: SmallVec<[EvictedKey; 4]>,
}

impl EvictionEffects {
    #[inline]
    fn push_expiry_transition(&mut self, shard_idx: usize, transition: ExpiryTransition) {
        self.expiry_transitions.push((shard_idx, transition));
    }

    #[inline]
    fn push_watch_invalidation(&mut self, key: VortexKey, table_hash: TableHash) {
        self.watch_invalidations.push((key, table_hash));
    }

    #[inline]
    fn push_aof_record(&mut self, evicted: EvictedKey) {
        self.aof_records.push(evicted);
    }

    #[inline]
    fn absorb(&mut self, mut other: Self) {
        self.expiry_transitions
            .extend(other.expiry_transitions.drain(..));
        self.watch_invalidations
            .extend(other.watch_invalidations.drain(..));
        self.aof_records.extend(other.aof_records.drain(..));
    }

    fn apply(self, keyspace: &ConcurrentKeyspace, evicted: &mut Vec<EvictedKey>) {
        for (shard_idx, transition) in self.expiry_transitions {
            keyspace.apply_expiry_transition(shard_idx, transition);
        }
        for (key, table_hash) in self.watch_invalidations {
            keyspace.bump_watch_key_known_active(key.as_bytes(), table_hash);
        }
        evicted.extend(self.aof_records);
    }
}

#[derive(Debug, Default)]
struct EvictionDeletion {
    freed_bytes: usize,
    effects: EvictionEffects,
}

#[derive(Debug, Default)]
struct EvictionSweepResult {
    next_slot: usize,
    freed_bytes: usize,
    slots_sampled: usize,
    effects: EvictionEffects,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EvictionSweepContext {
    shard_idx: usize,
    start_slot: usize,
    bytes_needed: usize,
    now_nanos: u64,
    volatile_only: bool,
    features: MutationFeatures,
}

#[inline]
fn record_eviction_deletion(
    freed_bytes: &mut usize,
    effects: &mut EvictionEffects,
    deletion: EvictionDeletion,
) {
    *freed_bytes += deletion.freed_bytes;
    effects.absorb(deletion.effects);
}

/// Action returned by a [`SweepPolicy`] for a single slot evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotAction {
    /// Skip this slot; the entry should not be evicted yet.
    Skip,
    /// Delete this slot immediately.
    Delete,
    /// The policy recorded this slot as a deferred candidate.
    Accumulate,
}

/// Zero-cost eviction policy trait for the unified sweep driver.
///
/// Implementations are used as generic type parameters so the compiler
/// monomorphizes each policy's inner loop; there is no virtual dispatch.
trait SweepPolicy {
    /// Whether the driver should handle expired-entry deletion before calling
    /// `evaluate_slot`.
    fn handles_expiry(&self) -> bool;

    /// Evaluate a live, non-expired slot.
    fn evaluate_slot(
        &mut self,
        keyspace: &ConcurrentKeyspace,
        table: &mut SwissTable,
        slot: usize,
        ttl: u64,
        context: &EvictionSweepContext,
    ) -> SlotAction;

    /// Called after the scan loop completes. Policies that accumulate a
    /// best-candidate during evaluation can delete it here if the scan did not
    /// free enough bytes.
    fn finalize(
        &mut self,
        _keyspace: &ConcurrentKeyspace,
        _table: &mut SwissTable,
        _context: &EvictionSweepContext,
        _freed_bytes: &mut usize,
        _effects: &mut EvictionEffects,
    ) {
        // Default: no deferred candidate.
    }
}

struct ClockPolicy;

impl SweepPolicy for ClockPolicy {
    #[inline(always)]
    fn handles_expiry(&self) -> bool {
        true
    }

    #[inline(always)]
    fn evaluate_slot(
        &mut self,
        _keyspace: &ConcurrentKeyspace,
        table: &mut SwissTable,
        slot: usize,
        _ttl: u64,
        _context: &EvictionSweepContext,
    ) -> SlotAction {
        let Some(entry) = table.slot_entry(slot) else {
            return SlotAction::Skip;
        };
        if entry.decrement_eviction_counter() {
            SlotAction::Skip
        } else {
            SlotAction::Delete
        }
    }
}

struct LfuClockPolicy {
    best_candidate: Option<usize>,
    best_frequency: u8,
}

impl LfuClockPolicy {
    #[inline]
    fn new() -> Self {
        Self {
            best_candidate: None,
            best_frequency: u8::MAX,
        }
    }
}

impl SweepPolicy for LfuClockPolicy {
    #[inline(always)]
    fn handles_expiry(&self) -> bool {
        true
    }

    #[inline(always)]
    fn evaluate_slot(
        &mut self,
        keyspace: &ConcurrentKeyspace,
        table: &mut SwissTable,
        slot: usize,
        _ttl: u64,
        _context: &EvictionSweepContext,
    ) -> SlotAction {
        let Some(entry) = table.slot_entry(slot) else {
            return SlotAction::Skip;
        };
        if entry.decrement_eviction_counter() {
            return SlotAction::Skip;
        }

        let Some((key, _)) = table.slot_key_value(slot) else {
            return SlotAction::Skip;
        };
        let frequency = keyspace
            .frequency_sketch
            .estimate(table.hash_key_bytes(key.as_bytes()));
        if self.best_candidate.is_none() || frequency < self.best_frequency {
            self.best_candidate = Some(slot);
            self.best_frequency = frequency;
        }
        SlotAction::Accumulate
    }

    fn finalize(
        &mut self,
        keyspace: &ConcurrentKeyspace,
        table: &mut SwissTable,
        context: &EvictionSweepContext,
        freed_bytes: &mut usize,
        effects: &mut EvictionEffects,
    ) {
        if *freed_bytes < context.bytes_needed {
            if let Some(candidate) = self.best_candidate {
                record_eviction_deletion(
                    freed_bytes,
                    effects,
                    keyspace.delete_evictable_slot(
                        context.shard_idx,
                        table,
                        candidate,
                        context.features,
                    ),
                );
            }
        }
    }
}

struct RandomPolicy;

impl SweepPolicy for RandomPolicy {
    #[inline(always)]
    fn handles_expiry(&self) -> bool {
        false
    }

    #[inline(always)]
    fn evaluate_slot(
        &mut self,
        _keyspace: &ConcurrentKeyspace,
        _table: &mut SwissTable,
        _slot: usize,
        _ttl: u64,
        _context: &EvictionSweepContext,
    ) -> SlotAction {
        SlotAction::Delete
    }
}

struct VolatileTtlPolicy {
    best_slot: Option<usize>,
    best_deadline: u64,
}

impl VolatileTtlPolicy {
    #[inline]
    fn new() -> Self {
        Self {
            best_slot: None,
            best_deadline: u64::MAX,
        }
    }
}

impl SweepPolicy for VolatileTtlPolicy {
    #[inline(always)]
    fn handles_expiry(&self) -> bool {
        true
    }

    #[inline(always)]
    fn evaluate_slot(
        &mut self,
        _keyspace: &ConcurrentKeyspace,
        _table: &mut SwissTable,
        slot: usize,
        ttl: u64,
        _context: &EvictionSweepContext,
    ) -> SlotAction {
        if ttl != 0 && ttl < self.best_deadline {
            self.best_deadline = ttl;
            self.best_slot = Some(slot);
        }
        SlotAction::Accumulate
    }

    fn finalize(
        &mut self,
        keyspace: &ConcurrentKeyspace,
        table: &mut SwissTable,
        context: &EvictionSweepContext,
        freed_bytes: &mut usize,
        effects: &mut EvictionEffects,
    ) {
        if *freed_bytes >= context.bytes_needed {
            return;
        }
        if let Some(candidate) = self.best_slot {
            record_eviction_deletion(
                freed_bytes,
                effects,
                keyspace.delete_evictable_slot(
                    context.shard_idx,
                    table,
                    candidate,
                    context.features,
                ),
            );
        }
    }
}

impl ConcurrentKeyspace {
    #[inline]
    pub fn eviction_pressure_active(&self) -> bool {
        let snapshot = self.eviction_config();
        snapshot.max_memory != 0
            && !snapshot.policy.is_noeviction()
            && self.committed_memory_pressure() > snapshot.max_memory
    }

    pub fn run_eviction_maintenance_on_shard(
        &self,
        preferred_shard: usize,
        now_nanos: u64,
    ) -> EvictionMaintenanceSlice {
        let snapshot = self.eviction_config();
        if snapshot.max_memory == 0 || snapshot.policy.is_noeviction() {
            return EvictionMaintenanceSlice::default();
        }

        let pressure = self.committed_memory_pressure();
        if pressure <= snapshot.max_memory {
            return EvictionMaintenanceSlice::default();
        }

        let shard_count = self.shards.len();
        if shard_count == 0 {
            return EvictionMaintenanceSlice::default();
        }

        let shard_idx = preferred_shard & (shard_count - 1);
        if snapshot.policy.is_volatile_only() && !self.shard_has_expiring_keys(shard_idx) {
            return EvictionMaintenanceSlice {
                oom_after_scan: self.committed_memory_pressure() > snapshot.max_memory,
                ..EvictionMaintenanceSlice::default()
            };
        }

        let mut report = EvictionScanReport {
            shards_scanned: 1,
            ..EvictionScanReport::default()
        };
        let target_used = snapshot.max_memory.saturating_sub(
            self.memory_reserved
                .load(std::sync::atomic::Ordering::Acquire),
        );
        let bytes_needed = pressure.saturating_sub(target_used);
        let scan_start = if self.runtime_profile_timers_enabled() {
            Some(vortex_common::Timestamp::now().as_nanos())
        } else {
            None
        };
        let mut evicted = Vec::new();
        report.bytes_freed = self.evict_from_shard(
            shard_idx,
            snapshot.policy,
            bytes_needed,
            now_nanos,
            &mut report,
            &mut evicted,
        );
        report.oom_after_scan = self.committed_memory_pressure() > snapshot.max_memory;
        let scan_nanos = scan_start
            .map(|start| {
                vortex_common::Timestamp::now()
                    .as_nanos()
                    .saturating_sub(start)
                    .max(1)
            })
            .unwrap_or(0);
        self.eviction_metrics
            .record_with_duration(report, scan_nanos);

        EvictionMaintenanceSlice {
            aof_records: evicted_keys_to_aof_records(evicted),
            shards_scanned: report.shards_scanned,
            slots_sampled: report.slots_sampled,
            bytes_freed: report.bytes_freed,
            oom_after_scan: report.oom_after_scan,
        }
    }

    pub(super) fn evict_until_target(
        &self,
        preferred_shard: usize,
        target_used: usize,
        policy: EvictionPolicy,
        now_nanos: u64,
        report: &mut EvictionScanReport,
        evicted: &mut Vec<EvictedKey>,
    ) -> usize {
        let shard_count = self.shards.len();
        if shard_count == 0 {
            return 0;
        }

        let start_shard = preferred_shard & (shard_count - 1);
        let shard_budget = shard_count.min(EVICTION_MAX_SHARDS_PER_ADMISSION);
        let initial_used = self.published_memory_used();
        let mut current_used = initial_used;
        let mut passes_without_progress = 0usize;

        while current_used > target_used
            && passes_without_progress < shard_count
            && report.shards_scanned < shard_budget
        {
            let mut progress = false;

            for offset in 0..shard_count {
                if report.shards_scanned >= shard_budget {
                    break;
                }
                let shard_idx = (start_shard + offset) & (shard_count - 1);
                if policy.is_volatile_only() && !self.shard_has_expiring_keys(shard_idx) {
                    continue;
                }

                report.shards_scanned += 1;
                let remaining = current_used.saturating_sub(target_used);
                let freed =
                    self.evict_from_shard(shard_idx, policy, remaining, now_nanos, report, evicted);
                if freed == 0 {
                    continue;
                }

                current_used = current_used.saturating_sub(freed);
                progress = true;
                if current_used <= target_used {
                    break;
                }
            }

            if progress {
                passes_without_progress = 0;
            } else {
                passes_without_progress += 1;
            }
        }

        initial_used.saturating_sub(current_used)
    }

    pub(super) fn evict_from_shard(
        &self,
        shard_idx: usize,
        policy: EvictionPolicy,
        bytes_needed: usize,
        now_nanos: u64,
        report: &mut EvictionScanReport,
        evicted: &mut Vec<EvictedKey>,
    ) -> usize {
        #[cfg(feature = "lock-profile")]
        let _lock_profile =
            self.enter_lock_profile_scope(crate::keyspace::LockProfileClass::Eviction);
        let mut guard = self.write_shard_by_index(shard_idx);
        let total_slots = guard.total_slots();
        if total_slots == 0 {
            return 0;
        }

        let start_slot = self.clock_hand(shard_idx) % total_slots;
        let context = EvictionSweepContext {
            shard_idx,
            start_slot,
            bytes_needed,
            now_nanos,
            volatile_only: policy.is_volatile_only(),
            features: self.mutation_features(),
        };
        let sweep = match policy {
            EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => {
                let random_start = (next_random_u64() as usize) & (total_slots - 1);
                let adjusted = EvictionSweepContext {
                    start_slot: (context.start_slot + random_start) % total_slots,
                    ..context
                };
                self.run_sweep_driver(&mut guard, adjusted, RandomPolicy)
            }
            EvictionPolicy::VolatileTtl => {
                self.run_sweep_driver(&mut guard, context, VolatileTtlPolicy::new())
            }
            policy if policy.is_lfu() => {
                self.run_sweep_driver(&mut guard, context, LfuClockPolicy::new())
            }
            _ => self.run_sweep_driver(&mut guard, context, ClockPolicy),
        };
        report.slots_sampled += sweep.slots_sampled;
        self.set_clock_hand(shard_idx, sweep.next_slot);
        let freed_bytes = sweep.freed_bytes;
        sweep.effects.apply(self, evicted);
        freed_bytes
    }

    fn run_sweep_driver<P: SweepPolicy>(
        &self,
        table: &mut SwissTable,
        context: EvictionSweepContext,
        mut policy: P,
    ) -> EvictionSweepResult {
        let total_slots = table.total_slots();
        if total_slots == 0 {
            return EvictionSweepResult::default();
        }

        let mut slot = context.start_slot % total_slots;
        let mut freed_bytes = 0usize;
        let mut effects = EvictionEffects::default();
        let mut slots_sampled = 0usize;
        let sweep_len = EVICTION_SWEEP_WINDOW.min(total_slots);

        for _ in 0..sweep_len {
            let current_slot = slot;
            slot = (slot + 1) % total_slots;
            slots_sampled += 1;

            let ttl = table.slot_entry_ttl(current_slot);
            if ttl == 0 && context.volatile_only {
                continue;
            }

            if table.slot_entry(current_slot).is_none() {
                continue;
            }

            if policy.handles_expiry() && ttl != 0 && ttl <= context.now_nanos {
                record_eviction_deletion(
                    &mut freed_bytes,
                    &mut effects,
                    self.delete_evictable_slot(
                        context.shard_idx,
                        table,
                        current_slot,
                        context.features,
                    ),
                );
                if freed_bytes >= context.bytes_needed {
                    break;
                }
                continue;
            }

            let action = policy.evaluate_slot(self, table, current_slot, ttl, &context);
            match action {
                SlotAction::Skip => {}
                SlotAction::Delete => {
                    record_eviction_deletion(
                        &mut freed_bytes,
                        &mut effects,
                        self.delete_evictable_slot(
                            context.shard_idx,
                            table,
                            current_slot,
                            context.features,
                        ),
                    );
                    if freed_bytes >= context.bytes_needed {
                        break;
                    }
                }
                SlotAction::Accumulate => {}
            }
        }

        policy.finalize(self, table, &context, &mut freed_bytes, &mut effects);

        EvictionSweepResult {
            next_slot: slot,
            freed_bytes,
            slots_sampled,
            effects,
        }
    }

    fn delete_evictable_slot(
        &self,
        shard_idx: usize,
        table: &mut SwissTable,
        slot: usize,
        features: MutationFeatures,
    ) -> EvictionDeletion {
        let ttl = table.slot_entry_ttl(slot);
        let bytes = table.slot_memory_bytes(slot);
        if bytes == 0 {
            return EvictionDeletion::default();
        }
        let record_aof = features.aof();
        let track_watch = features.watch();
        let Some((key, _)) = table.slot_key_value(slot) else {
            return EvictionDeletion::default();
        };
        let key = (record_aof || track_watch).then(|| key.clone());
        let aof_lsn = record_aof.then(|| AofLsn::from_allocated_lsn(self.next_lsn()));

        let _ = table.delete_slot(slot);
        let mut effects = EvictionEffects::default();
        if ttl != 0 {
            effects.push_expiry_transition(shard_idx, ExpiryTransition::ttl_removed());
        }
        if let Some(key) = key {
            if track_watch {
                effects.push_watch_invalidation(key.clone(), self.table_hash_key(key.as_bytes()));
            }
            if let Some(lsn) = aof_lsn {
                effects.push_aof_record(EvictedKey { lsn, key });
            }
        }
        EvictionDeletion {
            freed_bytes: bytes,
            effects,
        }
    }
}

fn evicted_keys_to_aof_records(evicted: Vec<EvictedKey>) -> AofRecords {
    if evicted.is_empty() {
        return None;
    }

    let mut records = Vec::with_capacity(evicted.len());
    for evicted in evicted {
        records.push(AofRecord {
            lsn: evicted.lsn,
            key: evicted.key,
        });
    }
    Some(records.into_boxed_slice())
}
