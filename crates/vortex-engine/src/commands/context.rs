//! Keyspace command helpers — lock-topology and table-level operations.
//!
//! After P1.9 cleanup, command handlers call methods directly on
//! `ConcurrentKeyspace` (defined here via `impl ConcurrentKeyspace`).
//! No trait indirection, no dynamic dispatch.

use bytes::Bytes;
use vortex_common::value::InlineBytes;
use vortex_common::{VortexKey, VortexValue};

use crate::SwissTable;
use crate::concurrent_keyspace::ConcurrentKeyspace;
use crate::shard::{SetOptions, SetResult};

use super::pattern::glob_match;
use super::{ERR_NOT_FLOAT, ERR_NOT_INTEGER, ERR_OVERFLOW, ERR_WRONG_TYPE};

const SCAN_CURSOR_SHARD_SHIFT: u32 = 32;
const DEFAULT_RANDOM_SEED: u64 = 0xDEAD_BEEF_CAFE_BABE;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TtlState {
    Missing,
    Persistent,
    Deadline(u64),
}

pub(crate) fn remove_if_expired(table: &mut SwissTable, key: &VortexKey, now_nanos: u64) -> bool {
    match table.get_entry_ttl(key) {
        Some(deadline) if deadline != 0 && deadline <= now_nanos => {
            table.remove(key);
            true
        }
        _ => false,
    }
}

fn remove_live_value(
    table: &mut SwissTable,
    key: &VortexKey,
    now_nanos: u64,
) -> Option<VortexValue> {
    if remove_if_expired(table, key, now_nanos) {
        return None;
    }
    table.remove(key)
}

fn set_with_options_on_table(
    table: &mut SwissTable,
    key: VortexKey,
    value: VortexValue,
    options: SetOptions,
    now_nanos: u64,
) -> SetResult {
    let _ = remove_if_expired(table, &key, now_nanos);

    let existing = match table.get_with_ttl(&key) {
        Some((current, ttl)) if ttl == 0 || ttl > now_nanos => Some((current.clone(), ttl)),
        _ => None,
    };
    let exists = existing.is_some();

    if options.nx && exists {
        return if options.get {
            SetResult::NotSetGet(existing.map(|(current, _)| current))
        } else {
            SetResult::NotSet
        };
    }

    if options.xx && !exists {
        return if options.get {
            SetResult::NotSetGet(None)
        } else {
            SetResult::NotSet
        };
    }

    let effective_ttl = if options.keepttl {
        existing.as_ref().map(|(_, ttl)| *ttl).unwrap_or(0)
    } else {
        options.ttl_deadline
    };

    let previous = if effective_ttl != 0 {
        table.insert_with_ttl(key, value, effective_ttl)
    } else {
        table.insert(key, value)
    };

    if options.get {
        SetResult::OkGet(previous)
    } else {
        SetResult::Ok
    }
}

fn increment_table_by(
    table: &mut SwissTable,
    key: VortexKey,
    delta: i64,
    now_nanos: u64,
) -> Result<i64, &'static [u8]> {
    let _ = remove_if_expired(table, &key, now_nanos);

    match table.get_mut(&key) {
        Some(value) => {
            let (result, delta_bytes) = {
                let old_bytes = value.memory_usage();
                let current = match value {
                    VortexValue::Integer(number) => *number,
                    VortexValue::InlineString(inline) => {
                        crate::commands::parse_i64(inline.as_bytes()).ok_or(ERR_NOT_INTEGER)?
                    }
                    VortexValue::String(bytes) => {
                        crate::commands::parse_i64(bytes.as_ref()).ok_or(ERR_NOT_INTEGER)?
                    }
                    _ => return Err(ERR_WRONG_TYPE),
                };

                let result = current.checked_add(delta).ok_or(ERR_OVERFLOW)?;
                let new_value = VortexValue::Integer(result);
                let delta_bytes = new_value.memory_usage() as isize - old_bytes as isize;
                *value = new_value;
                (result, delta_bytes)
            };
            table.record_memory_delta(delta_bytes);
            Ok(result)
        }
        None => {
            table.insert(key, VortexValue::Integer(delta));
            Ok(delta)
        }
    }
}

fn increment_table_by_float(
    table: &mut SwissTable,
    key: VortexKey,
    increment: f64,
    now_nanos: u64,
) -> Result<Bytes, &'static [u8]> {
    let _ = remove_if_expired(table, &key, now_nanos);

    let current = match table.get_mut(&key) {
        Some(value) => match value {
            VortexValue::Integer(number) => *number as f64,
            VortexValue::InlineString(inline) => std::str::from_utf8(inline.as_bytes())
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or(ERR_NOT_FLOAT)?,
            VortexValue::String(bytes) => std::str::from_utf8(bytes.as_ref())
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or(ERR_NOT_FLOAT)?,
            _ => return Err(ERR_WRONG_TYPE),
        },
        None => 0.0,
    };

    let result = current + increment;
    if result.is_nan() || result.is_infinite() {
        return Err(ERR_NOT_FLOAT);
    }

    let mut buffer = ryu::Buffer::new();
    let text = buffer.format(result);
    let bytes = Bytes::copy_from_slice(text.as_bytes());
    table.insert(key, VortexValue::from_bytes(bytes.as_ref()));
    Ok(bytes)
}

fn append_to_table(
    table: &mut SwissTable,
    key: VortexKey,
    append_bytes: &[u8],
    now_nanos: u64,
) -> Result<usize, &'static [u8]> {
    let _ = remove_if_expired(table, &key, now_nanos);

    let hash = table.hash_key_bytes(key.as_bytes());
    if !table.contains_key_prehashed(key.as_bytes(), hash) {
        table.insert_new_prehashed(key, VortexValue::from_bytes(append_bytes), hash);
        return Ok(append_bytes.len());
    }

    let (length, delta_bytes) = {
        let existing = table
            .get_mut(&key)
            .expect("key must exist after contains_key_prehashed");
        let old_bytes = existing.memory_usage();
        match existing {
            VortexValue::InlineString(inline) => {
                if inline.try_extend(append_bytes) {
                    (inline.len(), append_bytes.len() as isize)
                } else {
                    let mut combined = Vec::with_capacity(inline.len() + append_bytes.len());
                    combined.extend_from_slice(inline.as_bytes());
                    combined.extend_from_slice(append_bytes);
                    let new_value = VortexValue::String(Bytes::from(combined));
                    let length = match &new_value {
                        VortexValue::String(bytes) => bytes.len(),
                        _ => 0,
                    };
                    let delta_bytes = new_value.memory_usage() as isize - old_bytes as isize;
                    *existing = new_value;
                    (length, delta_bytes)
                }
            }
            VortexValue::String(bytes) => {
                let new_len = bytes.len() + append_bytes.len();
                let mut combined = Vec::with_capacity(new_len);
                combined.extend_from_slice(bytes.as_ref());
                combined.extend_from_slice(append_bytes);
                let new_value = if new_len <= 23 {
                    VortexValue::InlineString(InlineBytes::from_slice(&combined))
                } else {
                    VortexValue::String(Bytes::from(combined))
                };
                let length = match &new_value {
                    VortexValue::InlineString(inline) => inline.len(),
                    VortexValue::String(bytes) => bytes.len(),
                    _ => 0,
                };
                let delta_bytes = new_value.memory_usage() as isize - old_bytes as isize;
                *existing = new_value;
                (length, delta_bytes)
            }
            VortexValue::Integer(number) => {
                let mut buffer = itoa::Buffer::new();
                let text = buffer.format(*number);
                let mut combined = Vec::with_capacity(text.len() + append_bytes.len());
                combined.extend_from_slice(text.as_bytes());
                combined.extend_from_slice(append_bytes);
                let new_value = VortexValue::from_bytes(&combined);
                let length = new_value.strlen();
                let delta_bytes = new_value.memory_usage() as isize - old_bytes as isize;
                *existing = new_value;
                (length, delta_bytes)
            }
            _ => return Err(ERR_WRONG_TYPE),
        }
    };
    table.record_memory_delta(delta_bytes);
    Ok(length)
}

/// Compute GETRANGE on a borrowed value. Extracted so both the legacy table
/// path and the double-checked-locking concurrent path can share the logic.
fn getrange_from_value(
    value: &VortexValue,
    start: i64,
    end: i64,
) -> Result<Option<Bytes>, &'static [u8]> {
    let source = match value {
        VortexValue::InlineString(inline) => inline.as_bytes().to_vec(),
        VortexValue::String(bytes) => bytes.to_vec(),
        VortexValue::Integer(number) => {
            let mut buffer = itoa::Buffer::new();
            buffer.format(*number).as_bytes().to_vec()
        }
        _ => return Err(ERR_WRONG_TYPE),
    };

    if source.is_empty() {
        return Ok(Some(Bytes::new()));
    }

    let len = source.len() as i64;
    let start_index = if start < 0 {
        (len + start).max(0) as usize
    } else {
        start.min(len) as usize
    };
    let end_index = if end < 0 {
        (len + end).max(0) as usize
    } else {
        end.min(len - 1).max(0) as usize
    };

    if start_index > end_index || start_index >= source.len() {
        return Ok(Some(Bytes::new()));
    }

    Ok(Some(Bytes::copy_from_slice(
        &source[start_index..=end_index.min(source.len() - 1)],
    )))
}

fn setrange_in_table(
    table: &mut SwissTable,
    key: VortexKey,
    offset: usize,
    new_bytes: &[u8],
    now_nanos: u64,
) -> Result<usize, &'static [u8]> {
    let _ = remove_if_expired(table, &key, now_nanos);

    let mut data = match table.get_mut(&key) {
        Some(value) => match value {
            VortexValue::InlineString(inline) => inline.as_bytes().to_vec(),
            VortexValue::String(bytes) => bytes.to_vec(),
            VortexValue::Integer(number) => {
                let mut buffer = itoa::Buffer::new();
                buffer.format(*number).as_bytes().to_vec()
            }
            _ => return Err(ERR_WRONG_TYPE),
        },
        None => Vec::new(),
    };

    let required_len = offset + new_bytes.len();
    if data.len() < required_len {
        data.resize(required_len, 0);
    }
    data[offset..offset + new_bytes.len()].copy_from_slice(new_bytes);

    let length = data.len();
    table.insert(key, VortexValue::from_bytes(&data));
    Ok(length)
}

fn rename_within_table(
    table: &mut SwissTable,
    old_key: &VortexKey,
    new_key: VortexKey,
    now_nanos: u64,
    nx: bool,
) -> Result<bool, &'static [u8]> {
    let _ = remove_if_expired(table, old_key, now_nanos);
    if !table.contains_key(old_key) {
        return Err(b"-ERR no such key\r\n");
    }

    if nx && old_key == &new_key {
        return Ok(false);
    }
    if old_key == &new_key {
        return Ok(true);
    }

    let _ = remove_if_expired(table, &new_key, now_nanos);
    if nx && table.contains_key(&new_key) {
        return Ok(false);
    }

    let (value, ttl) = table
        .remove_with_ttl(old_key)
        .expect("source key must exist after contains_key check");
    table.remove(&new_key);
    if ttl != 0 && ttl > now_nanos {
        table.insert_with_ttl(new_key, value, ttl);
    } else {
        table.insert(new_key, value);
    }
    Ok(true)
}

fn copy_within_table(
    table: &mut SwissTable,
    src: &VortexKey,
    dst: VortexKey,
    replace: bool,
    now_nanos: u64,
) -> bool {
    let _ = remove_if_expired(table, src, now_nanos);
    let (value_clone, ttl) = {
        let Some((value, ttl)) = table.get_with_ttl(src) else {
            return false;
        };
        if ttl != 0 && ttl <= now_nanos {
            return false;
        }
        (value.clone(), ttl)
    };

    let _ = remove_if_expired(table, &dst, now_nanos);
    if !replace && table.contains_key(&dst) {
        return false;
    }

    if ttl != 0 && ttl > now_nanos {
        table.insert_with_ttl(dst, value_clone, ttl);
    } else {
        table.insert(dst, value_clone);
    }
    true
}

fn scan_table_slots(
    table: &SwissTable,
    start_slot: usize,
    pattern: Option<&[u8]>,
    count: usize,
    type_filter: Option<&[u8]>,
    now_nanos: u64,
    results: &mut Vec<VortexKey>,
) -> usize {
    let total_slots = table.total_slots();
    let match_all = pattern.is_none() || pattern == Some(b"*");

    for slot in start_slot..total_slots {
        let Some((key, value)) = table.slot_key_value(slot) else {
            continue;
        };
        let ttl = table.slot_entry_ttl(slot);
        if ttl != 0 && ttl <= now_nanos {
            continue;
        }

        if !match_all && !glob_match(pattern.expect("pattern checked above"), key.as_bytes()) {
            continue;
        }
        if let Some(filter) = type_filter {
            if !filter.eq_ignore_ascii_case(value.type_name().as_bytes()) {
                continue;
            }
        }

        results.push(key.clone());
        if results.len() >= count {
            return slot + 1;
        }
    }

    total_slots
}

fn collect_matching_keys(table: &SwissTable, pattern: &[u8], now_nanos: u64) -> Vec<VortexKey> {
    let match_all = pattern == b"*";
    let mut results = Vec::new();
    for slot in 0..table.total_slots() {
        let Some((key, _value)) = table.slot_key_value(slot) else {
            continue;
        };
        let ttl = table.slot_entry_ttl(slot);
        if ttl != 0 && ttl <= now_nanos {
            continue;
        }
        if match_all || glob_match(pattern, key.as_bytes()) {
            results.push(key.clone());
        }
    }
    results
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
                return Some(key.clone());
            }
        }
        slot = (slot + 1) & mask;
    }

    None
}

fn count_live_expires(table: &SwissTable, now_nanos: u64) -> usize {
    let mut count = 0usize;
    for slot in 0..table.total_slots() {
        if table.slot_key_value(slot).is_none() {
            continue;
        }
        let ttl = table.slot_entry_ttl(slot);
        if ttl != 0 && ttl > now_nanos {
            count += 1;
        }
    }
    count
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

// ── ConcurrentKeyspace command methods ─────────────────────────────────

impl ConcurrentKeyspace {
    pub(crate) fn get_value(&self, key: &VortexKey, now_nanos: u64) -> Option<VortexValue> {
        let shard_index = self.shard_index(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value.clone()),
            Some(_) => {
                // Expired — drop read lock, escalate to write lock, double-check.
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                remove_if_expired(&mut wguard, key, now_nanos);
                None
            }
            None => None,
        }
    }

    pub(crate) fn set_value_with_ttl(
        &self,
        key: VortexKey,
        value: VortexValue,
        ttl_deadline_nanos: u64,
    ) -> Option<VortexValue> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        guard.insert_with_ttl(key, value, ttl_deadline_nanos)
    }

    pub(crate) fn set_value_with_options(
        &self,
        key: VortexKey,
        value: VortexValue,
        options: SetOptions,
        now_nanos: u64,
    ) -> SetResult {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        set_with_options_on_table(&mut guard, key, value, options, now_nanos)
    }

    pub(crate) fn mget_values(
        &self,
        keys: &[VortexKey],
        now_nanos: u64,
    ) -> Vec<Option<VortexValue>> {
        let key_refs: Vec<&[u8]> = keys.iter().map(VortexKey::as_bytes).collect();
        let (guards, sorted_shards, per_key_shards) = self.multi_read(&key_refs);

        // Phase 1: Read pass — collect results and track expired key indices.
        let mut results = Vec::with_capacity(keys.len());
        let mut expired_indices: Vec<usize> = Vec::new();

        for (idx, (key, &shard_index)) in keys.iter().zip(per_key_shards.iter()).enumerate() {
            let position = ConcurrentKeyspace::guard_position(&sorted_shards, shard_index);
            match guards[position].1.get_with_ttl(key) {
                Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                    results.push(Some(value.clone()));
                }
                Some(_) => {
                    // Expired — mark for deferred cleanup.
                    results.push(None);
                    expired_indices.push(idx);
                }
                None => results.push(None),
            }
        }

        // Phase 2: Drop all read locks before acquiring any write locks.
        drop(guards);

        // Phase 3: Clean up expired entries under write locks (double-checked).
        // Cold path — only triggers when expired keys are encountered.
        if !expired_indices.is_empty() {
            for &idx in &expired_indices {
                let shard_index = per_key_shards[idx];
                let mut wguard = self.write_shard_by_index(shard_index);
                remove_if_expired(&mut wguard, &keys[idx], now_nanos);
            }
        }

        results
    }

    pub(crate) fn mset_values(&self, pairs: Vec<(VortexKey, VortexValue)>) {
        if pairs.is_empty() {
            return;
        }
        let key_refs: Vec<&[u8]> = pairs.iter().map(|(key, _)| key.as_bytes()).collect();
        let (mut guards, sorted_shards, per_key_shards) = self.multi_write(&key_refs);
        for ((key, value), shard_index) in pairs.into_iter().zip(per_key_shards) {
            let position = ConcurrentKeyspace::guard_position(&sorted_shards, shard_index);
            guards[position].1.insert(key, value);
        }
    }

    pub(crate) fn msetnx_values(
        &self,
        pairs: Vec<(VortexKey, VortexValue)>,
        now_nanos: u64,
    ) -> bool {
        if pairs.is_empty() {
            return true;
        }

        let key_refs: Vec<&[u8]> = pairs.iter().map(|(key, _)| key.as_bytes()).collect();
        let (mut guards, sorted_shards, per_key_shards) = self.multi_write(&key_refs);

        for (key, shard_index) in pairs
            .iter()
            .map(|(key, _)| key)
            .zip(per_key_shards.iter().copied())
        {
            let position = ConcurrentKeyspace::guard_position(&sorted_shards, shard_index);
            let table = &mut *guards[position].1;
            let _ = remove_if_expired(table, key, now_nanos);
            if table.contains_key(key) {
                return false;
            }
        }

        for ((key, value), shard_index) in pairs.into_iter().zip(per_key_shards) {
            let position = ConcurrentKeyspace::guard_position(&sorted_shards, shard_index);
            let table = &mut *guards[position].1;
            let hash = table.hash_key_bytes(key.as_bytes());
            table.insert_new_prehashed(key, value, hash);
        }

        true
    }

    pub(crate) fn remove_value(&self, key: &VortexKey, now_nanos: u64) -> Option<VortexValue> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        remove_live_value(&mut guard, key, now_nanos)
    }

    pub(crate) fn increment_by(
        &self,
        key: VortexKey,
        delta: i64,
        now_nanos: u64,
    ) -> Result<i64, &'static [u8]> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        increment_table_by(&mut guard, key, delta, now_nanos)
    }

    pub(crate) fn increment_by_float(
        &self,
        key: VortexKey,
        increment: f64,
        now_nanos: u64,
    ) -> Result<Bytes, &'static [u8]> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        increment_table_by_float(&mut guard, key, increment, now_nanos)
    }

    pub(crate) fn append_value(
        &self,
        key: VortexKey,
        append_bytes: &[u8],
        now_nanos: u64,
    ) -> Result<usize, &'static [u8]> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        append_to_table(&mut guard, key, append_bytes, now_nanos)
    }

    pub(crate) fn strlen_value(
        &self,
        key: &VortexKey,
        now_nanos: u64,
    ) -> Result<Option<usize>, &'static [u8]> {
        let shard_index = self.shard_index(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                if !value.is_string() {
                    Err(ERR_WRONG_TYPE)
                } else {
                    Ok(Some(value.strlen()))
                }
            }
            Some(_) => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                remove_if_expired(&mut wguard, key, now_nanos);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    pub(crate) fn getrange_value(
        &self,
        key: &VortexKey,
        start: i64,
        end: i64,
        now_nanos: u64,
    ) -> Result<Option<Bytes>, &'static [u8]> {
        let shard_index = self.shard_index(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                // Live key — compute range while read guard is held.
                getrange_from_value(value, start, end)
            }
            Some(_) => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                remove_if_expired(&mut wguard, key, now_nanos);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    pub(crate) fn setrange_value(
        &self,
        key: VortexKey,
        offset: usize,
        new_bytes: &[u8],
        now_nanos: u64,
    ) -> Result<usize, &'static [u8]> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        setrange_in_table(&mut guard, key, offset, new_bytes, now_nanos)
    }

    pub(crate) fn delete_keys(&self, keys: &[VortexKey], now_nanos: u64) -> i64 {
        if keys.is_empty() {
            return 0;
        }
        if keys.len() == 1 {
            return i64::from(self.remove_value(&keys[0], now_nanos).is_some());
        }

        let key_refs: Vec<&[u8]> = keys.iter().map(VortexKey::as_bytes).collect();
        let (mut guards, sorted_shards, per_key_shards) = self.multi_write(&key_refs);
        let mut deleted = 0i64;

        for (key, shard_index) in keys.iter().zip(per_key_shards.iter().copied()) {
            let position = ConcurrentKeyspace::guard_position(&sorted_shards, shard_index);
            if remove_live_value(&mut guards[position].1, key, now_nanos).is_some() {
                deleted += 1;
            }
        }

        deleted
    }

    pub(crate) fn count_existing(&self, keys: &[VortexKey], now_nanos: u64) -> i64 {
        if keys.is_empty() {
            return 0;
        }
        let key_refs: Vec<&[u8]> = keys.iter().map(VortexKey::as_bytes).collect();
        let (guards, sorted_shards, per_key_shards) = self.multi_read(&key_refs);
        let mut count = 0i64;
        let mut expired_indices: Vec<usize> = Vec::new();

        for (idx, (key, shard_index)) in keys.iter().zip(per_key_shards.iter().copied()).enumerate()
        {
            let position = ConcurrentKeyspace::guard_position(&sorted_shards, shard_index);
            match guards[position].1.get_with_ttl(key) {
                Some((_, ttl)) if ttl == 0 || ttl > now_nanos => count += 1,
                Some(_) => expired_indices.push(idx),
                None => {}
            }
        }

        // Drop all read locks before acquiring write locks for cleanup.
        drop(guards);

        if !expired_indices.is_empty() {
            for &idx in &expired_indices {
                let shard_index = per_key_shards[idx];
                let mut wguard = self.write_shard_by_index(shard_index);
                remove_if_expired(&mut wguard, &keys[idx], now_nanos);
            }
        }

        count
    }

    pub(crate) fn expire_key(&self, key: &VortexKey, deadline_nanos: u64, now_nanos: u64) -> bool {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        let _ = remove_if_expired(&mut guard, key, now_nanos);
        guard.set_entry_ttl(key, deadline_nanos)
    }

    pub(crate) fn persist_key(&self, key: &VortexKey) -> bool {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        guard.clear_entry_ttl(key)
    }

    pub(crate) fn ttl_state(&self, key: &VortexKey, now_nanos: u64) -> TtlState {
        let shard_index = self.shard_index(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            None => TtlState::Missing,
            Some((_, 0)) => TtlState::Persistent,
            Some((_, deadline)) if deadline <= now_nanos => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                remove_if_expired(&mut wguard, key, now_nanos);
                TtlState::Missing
            }
            Some((_, deadline)) => TtlState::Deadline(deadline),
        }
    }

    pub(crate) fn type_of_key(&self, key: &VortexKey, now_nanos: u64) -> Option<&'static str> {
        let shard_index = self.shard_index(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value.type_name()),
            Some(_) => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                remove_if_expired(&mut wguard, key, now_nanos);
                None
            }
            None => None,
        }
    }

    pub(crate) fn rename_key(
        &self,
        old_key: &VortexKey,
        new_key: VortexKey,
        now_nanos: u64,
        nx: bool,
    ) -> Result<bool, &'static [u8]> {
        let source_shard = self.shard_index(old_key.as_bytes());
        let destination_shard = self.shard_index(new_key.as_bytes());

        if source_shard == destination_shard {
            let mut guard = self.write_shard_by_index(source_shard);
            return rename_within_table(&mut guard, old_key, new_key, now_nanos, nx);
        }

        let key_refs = [old_key.as_bytes(), new_key.as_bytes()];
        let (mut guards, sorted_shards, per_key_shards) = self.multi_write(&key_refs);
        let src_position = ConcurrentKeyspace::guard_position(&sorted_shards, per_key_shards[0]);
        let dst_position = ConcurrentKeyspace::guard_position(&sorted_shards, per_key_shards[1]);

        let (src_table, dst_table) = if src_position < dst_position {
            let (left, right) = guards.split_at_mut(dst_position);
            (&mut *left[src_position].1, &mut *right[0].1)
        } else {
            let (left, right) = guards.split_at_mut(src_position);
            (&mut *right[0].1, &mut *left[dst_position].1)
        };

        let _ = remove_if_expired(src_table, old_key, now_nanos);
        if !src_table.contains_key(old_key) {
            return Err(b"-ERR no such key\r\n");
        }
        let _ = remove_if_expired(dst_table, &new_key, now_nanos);
        if nx && dst_table.contains_key(&new_key) {
            return Ok(false);
        }

        let (value, ttl) = src_table
            .remove_with_ttl(old_key)
            .expect("source key must exist after contains_key check");
        dst_table.remove(&new_key);
        if ttl != 0 && ttl > now_nanos {
            dst_table.insert_with_ttl(new_key, value, ttl);
        } else {
            dst_table.insert(new_key, value);
        }
        Ok(true)
    }

    pub(crate) fn scan_keys(
        &self,
        cursor: u64,
        pattern: Option<&[u8]>,
        count: usize,
        type_filter: Option<&[u8]>,
        now_nanos: u64,
    ) -> (u64, Vec<VortexKey>) {
        let (mut shard_index, mut slot_index) = decode_scan_cursor(cursor);
        let shard_count = self.num_shards();
        if shard_index >= shard_count {
            shard_index = 0;
            slot_index = 0;
        }

        let mut results = Vec::with_capacity(count.max(1));
        for current_shard in shard_index..shard_count {
            let guard = self.read_shard_by_index(current_shard);
            let start_slot = if current_shard == shard_index {
                slot_index
            } else {
                0
            };
            let next_slot = scan_table_slots(
                &guard,
                start_slot,
                pattern,
                count.max(1),
                type_filter,
                now_nanos,
                &mut results,
            );

            if results.len() >= count.max(1) {
                if next_slot < guard.total_slots() {
                    return (encode_scan_cursor(current_shard, next_slot), results);
                }
                if current_shard + 1 < shard_count {
                    return (encode_scan_cursor(current_shard + 1, 0), results);
                }
                return (0, results);
            }
        }

        (0, results)
    }

    pub(crate) fn keys_matching(&self, pattern: &[u8], now_nanos: u64) -> Vec<VortexKey> {
        let per_shard = self.scan_all_shards(|_shard_index, table| {
            collect_matching_keys(table, pattern, now_nanos)
        });
        let total = per_shard.iter().map(Vec::len).sum();
        let mut results = Vec::with_capacity(total);
        for mut shard_keys in per_shard {
            results.append(&mut shard_keys);
        }
        results
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

    pub(crate) fn copy_key(
        &self,
        src: &VortexKey,
        dst: VortexKey,
        replace: bool,
        now_nanos: u64,
    ) -> bool {
        let source_shard = self.shard_index(src.as_bytes());
        let destination_shard = self.shard_index(dst.as_bytes());

        if source_shard == destination_shard {
            let mut guard = self.write_shard_by_index(source_shard);
            return copy_within_table(&mut guard, src, dst, replace, now_nanos);
        }

        let key_refs = [src.as_bytes(), dst.as_bytes()];
        let (mut guards, sorted_shards, per_key_shards) = self.multi_write(&key_refs);
        let src_position = ConcurrentKeyspace::guard_position(&sorted_shards, per_key_shards[0]);
        let dst_position = ConcurrentKeyspace::guard_position(&sorted_shards, per_key_shards[1]);

        let (src_table, dst_table) = if src_position < dst_position {
            let (left, right) = guards.split_at_mut(dst_position);
            (&mut *left[src_position].1, &mut *right[0].1)
        } else {
            let (left, right) = guards.split_at_mut(src_position);
            (&mut *right[0].1, &mut *left[dst_position].1)
        };

        let _ = remove_if_expired(src_table, src, now_nanos);
        let (value_clone, ttl) = {
            let Some((value, ttl)) = src_table.get_with_ttl(src) else {
                return false;
            };
            if ttl != 0 && ttl <= now_nanos {
                return false;
            }
            (value.clone(), ttl)
        };

        let _ = remove_if_expired(dst_table, &dst, now_nanos);
        if !replace && dst_table.contains_key(&dst) {
            return false;
        }

        if ttl != 0 && ttl > now_nanos {
            dst_table.insert_with_ttl(dst, value_clone, ttl);
        } else {
            dst_table.insert(dst, value_clone);
        }
        true
    }

    pub(crate) fn cmd_dbsize(&self, now_nanos: u64) -> usize {
        let _ = now_nanos;
        self.dbsize()
    }

    pub(crate) fn cmd_flush_all(&self) {
        ConcurrentKeyspace::flush_all(self);
    }

    pub(crate) fn info_keyspace(&self, now_nanos: u64) -> (usize, usize) {
        let keys = self.dbsize();
        let expires = (0..self.num_shards())
            .map(|shard_index| {
                let guard = self.read_shard_by_index(shard_index);
                count_live_expires(&guard, now_nanos)
            })
            .sum();
        (keys, expires)
    }
}
