//! Keyspace command helpers — lock-topology and table-level operations.
//!
//! After P1.9 cleanup, command handlers call methods directly on
//! `ConcurrentKeyspace` (defined here via `impl ConcurrentKeyspace`).
//! No trait indirection, no dynamic dispatch.

use core::mem::size_of;

use bytes::Bytes;
use smallvec::SmallVec;
use vortex_common::value::InlineBytes;
use vortex_common::{VortexKey, VortexValue};
use vortex_proto::RespFrame;

use crate::SwissTable;
use crate::entry::Entry;
use crate::keyspace::{ConcurrentKeyspace, EvictedKeys, EvictionAdmissionError, MemoryReservation};

use super::pattern::glob_match;
use super::{
    AofRecord, AofRecords, CmdResult, ERR_NOT_FLOAT, ERR_NOT_INTEGER, ERR_OVERFLOW, ERR_WRONG_TYPE,
    ExecutedCommand,
};

const SCAN_CURSOR_SHARD_SHIFT: u32 = 32;
const DEFAULT_RANDOM_SEED: u64 = 0xDEAD_BEEF_CAFE_BABE;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TtlState {
    Missing,
    Persistent,
    Deadline(u64),
}

#[derive(Clone, Copy)]
struct MultiWriteLookup {
    pair_index: usize,
    shard_idx: usize,
    table_hash: u64,
}

#[derive(Debug)]
pub(crate) struct MutationOutcome<T> {
    pub(crate) value: T,
    pub(crate) aof_records: AofRecords,
    pub(crate) aof_lsn: Option<u64>,
}

impl<T> MutationOutcome<T> {
    #[inline(always)]
    pub(crate) fn new(value: T, aof_lsn: Option<u64>) -> Self {
        Self {
            value,
            aof_records: None,
            aof_lsn,
        }
    }

    #[inline(always)]
    pub(crate) fn with_aof_records(
        value: T,
        aof_records: AofRecords,
        aof_lsn: Option<u64>,
    ) -> Self {
        Self {
            value,
            aof_records,
            aof_lsn,
        }
    }

    #[inline(always)]
    pub(crate) fn map_value<U>(self, value: U) -> MutationOutcome<U> {
        MutationOutcome {
            value,
            aof_records: self.aof_records,
            aof_lsn: self.aof_lsn,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MutationError {
    pub(crate) response: &'static [u8],
    pub(crate) aof_records: AofRecords,
}

impl MutationError {
    #[inline(always)]
    pub(crate) fn new(response: &'static [u8]) -> Self {
        Self {
            response,
            aof_records: None,
        }
    }

    #[inline(always)]
    pub(crate) fn with_evictions(response: &'static [u8], evicted: EvictedKeys) -> Self {
        Self {
            response,
            aof_records: evicted_keys_to_aof_records(evicted),
        }
    }

    #[inline]
    pub(crate) fn into_executed(self) -> ExecutedCommand {
        ExecutedCommand::with_aof_records(CmdResult::Static(self.response), self.aof_records, None)
    }
}

impl From<&'static [u8]> for MutationError {
    #[inline(always)]
    fn from(response: &'static [u8]) -> Self {
        Self::new(response)
    }
}

impl From<EvictionAdmissionError> for MutationError {
    #[inline(always)]
    fn from(error: EvictionAdmissionError) -> Self {
        Self {
            response: error.response,
            aof_records: evicted_keys_to_aof_records(error.evicted),
        }
    }
}

pub(crate) type MutationResult<T> = Result<MutationOutcome<T>, MutationError>;

#[inline]
fn evicted_keys_to_aof_records(evicted: EvictedKeys) -> AofRecords {
    let evicted = evicted?;

    let mut records = Vec::with_capacity(evicted.len());
    for evicted in evicted {
        records.push(AofRecord {
            lsn: evicted.lsn,
            key: evicted.key,
        });
    }
    Some(records.into_boxed_slice())
}

#[inline]
fn mutation_outcome_with_evictions<T>(
    value: T,
    aof_lsn: Option<u64>,
    evicted: EvictedKeys,
) -> MutationOutcome<T> {
    MutationOutcome::with_aof_records(value, evicted_keys_to_aof_records(evicted), aof_lsn)
}

#[inline(always)]
fn stamp_entry_lsn(table: &mut SwissTable, key_bytes: &[u8], table_hash: u64, lsn: u64) {
    let stamped = table.set_lsn_version_prehashed(key_bytes, table_hash, lsn);
    debug_assert!(stamped, "live key must still exist when stamping LSN");
}

/// Result of a SET command with options (NX/XX/GET).
pub(crate) enum SetResult {
    /// SET succeeded (no GET flag).
    Ok,
    /// SET was not performed (NX/XX condition failed, no GET flag).
    NotSet,
    /// SET succeeded — returns old value (GET flag was set).
    OkGet(Option<VortexValue>),
    /// SET was not performed — returns current value (GET flag + NX/XX failed).
    NotSetGet(Option<VortexValue>),
}

/// Options for SET-style writes.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SetOptions {
    pub(crate) ttl_deadline: u64,
    pub(crate) nx: bool,
    pub(crate) xx: bool,
    pub(crate) get: bool,
    pub(crate) keepttl: bool,
}

#[derive(Clone, Copy)]
struct ExpiryTransition {
    had_ttl: bool,
    has_ttl_after: bool,
}

#[inline]
fn mget_value_to_frame(value: &VortexValue) -> RespFrame {
    match value {
        VortexValue::InlineString(inline) => {
            RespFrame::bulk_string(Bytes::copy_from_slice(inline.as_bytes()))
        }
        VortexValue::String(bytes) => RespFrame::bulk_string(bytes.clone()),
        VortexValue::Integer(number) => {
            let mut buffer = itoa::Buffer::new();
            let text = buffer.format(*number);
            RespFrame::bulk_string(Bytes::copy_from_slice(text.as_bytes()))
        }
        _ => RespFrame::null_bulk_string(),
    }
}

#[inline]
fn ttl_present(ttl_deadline: Option<u64>) -> bool {
    matches!(ttl_deadline, Some(deadline) if deadline != 0)
}

#[inline]
fn positive_delta(delta: isize) -> usize {
    delta.max(0) as usize
}

#[inline]
fn entry_memory_usage(key: &VortexKey, value_memory_usage: usize) -> usize {
    size_of::<Entry>() + key.memory_usage() + value_memory_usage
}

#[inline]
fn string_value_memory_usage(len: usize) -> usize {
    if len <= vortex_common::MAX_INLINE_VALUE_LEN {
        size_of::<InlineBytes>() + len
    } else {
        size_of::<Bytes>() + len
    }
}

#[inline]
fn integer_value_memory_usage() -> usize {
    size_of::<i64>()
}

#[inline]
fn projected_rewrite_delta(
    key: &VortexKey,
    existing_value: Option<&VortexValue>,
    new_value_memory_usage: usize,
) -> usize {
    match existing_value {
        Some(value) => {
            positive_delta(new_value_memory_usage as isize - value.memory_usage() as isize)
        }
        None => entry_memory_usage(key, new_value_memory_usage),
    }
}

fn projected_increment_delta(
    table: &SwissTable,
    key: &VortexKey,
    delta: i64,
    now_nanos: u64,
) -> Result<usize, &'static [u8]> {
    match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
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
            let _ = current.checked_add(delta).ok_or(ERR_OVERFLOW)?;
            Ok(projected_rewrite_delta(
                key,
                Some(value),
                integer_value_memory_usage(),
            ))
        }
        Some(_) | None => Ok(entry_memory_usage(key, integer_value_memory_usage())),
    }
}

fn projected_increment_by_float_delta(
    table: &SwissTable,
    key: &VortexKey,
    increment: f64,
    now_nanos: u64,
) -> Result<usize, &'static [u8]> {
    let current = match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => match value {
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
        Some(_) | None => 0.0,
    };

    let result = current + increment;
    if result.is_nan() || result.is_infinite() {
        return Err(ERR_NOT_FLOAT);
    }

    let mut buffer = ryu::Buffer::new();
    let text = buffer.format(result);
    let existing = match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
        _ => None,
    };

    Ok(projected_rewrite_delta(
        key,
        existing,
        string_value_memory_usage(text.len()),
    ))
}

fn projected_append_delta(
    table: &SwissTable,
    key: &VortexKey,
    append_bytes: &[u8],
    now_nanos: u64,
) -> Result<usize, &'static [u8]> {
    match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
            let current_len = match value {
                VortexValue::InlineString(_) | VortexValue::String(_) | VortexValue::Integer(_) => {
                    value.strlen()
                }
                _ => return Err(ERR_WRONG_TYPE),
            };
            Ok(projected_rewrite_delta(
                key,
                Some(value),
                string_value_memory_usage(current_len + append_bytes.len()),
            ))
        }
        Some(_) | None => Ok(entry_memory_usage(
            key,
            string_value_memory_usage(append_bytes.len()),
        )),
    }
}

fn projected_setrange_delta(
    table: &SwissTable,
    key: &VortexKey,
    offset: usize,
    new_bytes: &[u8],
    now_nanos: u64,
) -> Result<usize, &'static [u8]> {
    let required_len = offset + new_bytes.len();
    match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
            let current_len = match value {
                VortexValue::InlineString(_) | VortexValue::String(_) | VortexValue::Integer(_) => {
                    value.strlen()
                }
                _ => return Err(ERR_WRONG_TYPE),
            };
            Ok(projected_rewrite_delta(
                key,
                Some(value),
                string_value_memory_usage(current_len.max(required_len)),
            ))
        }
        Some(_) | None => Ok(entry_memory_usage(
            key,
            string_value_memory_usage(required_len),
        )),
    }
}

fn projected_copy_delta(
    source_value: Option<&VortexValue>,
    dst_table: &SwissTable,
    dst: &VortexKey,
    replace: bool,
    now_nanos: u64,
) -> usize {
    let Some(source_value) = source_value else {
        return 0;
    };

    let existing = match dst_table.get_with_ttl(dst) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
        _ => None,
    };
    if !replace && existing.is_some() {
        return 0;
    }

    projected_rewrite_delta(dst, existing, source_value.memory_usage())
}

fn projected_rename_delta(
    src_table: &SwissTable,
    old_key: &VortexKey,
    dst_table: &SwissTable,
    new_key: &VortexKey,
    now_nanos: u64,
    nx: bool,
) -> Result<usize, &'static [u8]> {
    if old_key == new_key {
        return Ok(0);
    }

    let Some((source_value, source_ttl)) = src_table.get_with_ttl(old_key) else {
        return Err(b"-ERR no such key\r\n");
    };
    if source_ttl != 0 && source_ttl <= now_nanos {
        return Err(b"-ERR no such key\r\n");
    }

    let existing_dst = match dst_table.get_with_ttl(new_key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
        _ => None,
    };
    if nx && existing_dst.is_some() {
        return Ok(0);
    }

    let moved_usage = entry_memory_usage(new_key, source_value.memory_usage());
    let removed_source_usage = entry_memory_usage(old_key, source_value.memory_usage());
    let removed_dst_usage = existing_dst
        .map(|value| entry_memory_usage(new_key, value.memory_usage()))
        .unwrap_or(0);

    Ok(positive_delta(
        moved_usage as isize - removed_source_usage as isize - removed_dst_usage as isize,
    ))
}

fn projected_set_write_delta(
    table: &SwissTable,
    key: &VortexKey,
    value: &VortexValue,
    options: SetOptions,
    now_nanos: u64,
) -> usize {
    let exists = matches!(
        table.get_with_ttl(key),
        Some((_, ttl)) if ttl == 0 || ttl > now_nanos
    );

    if options.nx && exists {
        return 0;
    }
    if options.xx && !exists {
        return 0;
    }

    positive_delta(table.projected_insert_delta(key, value))
}

fn build_multi_write_lookups<'a>(
    keyspace: &ConcurrentKeyspace,
    pairs: &'a [(VortexKey, VortexValue)],
) -> (SmallVec<[&'a [u8]; 16]>, SmallVec<[MultiWriteLookup; 16]>) {
    let mut key_refs: SmallVec<[&[u8]; 16]> = SmallVec::with_capacity(pairs.len());
    let mut lookups: SmallVec<[MultiWriteLookup; 16]> = SmallVec::with_capacity(pairs.len());

    for (pair_index, (key, _)) in pairs.iter().enumerate() {
        let key_bytes = key.as_bytes();
        key_refs.push(key_bytes);
        lookups.push(MultiWriteLookup {
            pair_index,
            shard_idx: keyspace.shard_index(key_bytes),
            table_hash: keyspace.table_hash_key(key_bytes),
        });
    }

    lookups.sort_unstable_by_key(|lookup| (lookup.shard_idx, lookup.pair_index));

    (key_refs, lookups)
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

fn take_live_value(
    table: &mut SwissTable,
    key: &VortexKey,
    now_nanos: u64,
) -> (Option<VortexValue>, bool) {
    let Some((value, ttl_deadline)) = table.remove_with_ttl(key) else {
        return (None, false);
    };

    let had_ttl = ttl_deadline != 0;
    if had_ttl && ttl_deadline <= now_nanos {
        return (None, true);
    }

    (Some(value), had_ttl)
}

fn delete_live_key_bytes(
    table: &mut SwissTable,
    key_bytes: &[u8],
    hash: u64,
    now_nanos: u64,
) -> (bool, bool) {
    let Some((value, ttl_deadline)) = table.remove_with_ttl_prehashed(key_bytes, hash) else {
        return (false, false);
    };
    drop(value);

    let had_ttl = ttl_deadline != 0;
    if had_ttl && ttl_deadline <= now_nanos {
        return (false, true);
    }

    (true, had_ttl)
}

fn set_with_options_on_table(
    table: &mut SwissTable,
    key: VortexKey,
    value: VortexValue,
    options: SetOptions,
    now_nanos: u64,
) -> (SetResult, bool) {
    let _ = remove_if_expired(table, &key, now_nanos);

    // Plain SET is the hot path in the benchmark workload. It does not need
    // an existence probe, previous value clone, or TTL carry-forward.
    if !options.nx && !options.xx && !options.get && !options.keepttl {
        if options.ttl_deadline != 0 {
            table.insert_with_ttl(key, value, options.ttl_deadline);
        } else {
            table.insert(key, value);
        }
        return (SetResult::Ok, options.ttl_deadline != 0);
    }

    let mut existing_for_notset_get = None;
    let mut existing_ttl = 0u64;
    let exists = match table.get_with_ttl(&key) {
        Some((current, ttl)) if ttl == 0 || ttl > now_nanos => {
            existing_ttl = ttl;
            if options.get && (options.nx || options.xx) {
                existing_for_notset_get = Some(current.clone());
            }
            true
        }
        _ => false,
    };

    if options.nx && exists {
        return if options.get {
            (
                SetResult::NotSetGet(existing_for_notset_get),
                existing_ttl != 0,
            )
        } else {
            (SetResult::NotSet, existing_ttl != 0)
        };
    }

    if options.xx && !exists {
        return if options.get {
            (SetResult::NotSetGet(None), false)
        } else {
            (SetResult::NotSet, false)
        };
    }

    let effective_ttl = if options.keepttl {
        existing_ttl
    } else {
        options.ttl_deadline
    };

    let previous = if effective_ttl != 0 {
        table.insert_with_ttl(key, value, effective_ttl)
    } else {
        table.insert(key, value)
    };

    if options.get {
        (SetResult::OkGet(previous), effective_ttl != 0)
    } else {
        (SetResult::Ok, effective_ttl != 0)
    }
}

fn increment_table_by(
    table: &mut SwissTable,
    key: VortexKey,
    delta: i64,
    lsn: u64,
    now_nanos: u64,
) -> Result<(i64, ExpiryTransition), &'static [u8]> {
    match table.get_with_ttl(&key) {
        Some((value, ttl)) => {
            let had_ttl = ttl != 0;
            if had_ttl && ttl <= now_nanos {
                table.remove(&key);
                let hash = table.hash_key_bytes(key.as_bytes());
                let _ = table.insert_no_ttl_prehashed_and_lsn(
                    key,
                    VortexValue::Integer(delta),
                    hash,
                    lsn,
                );
                return Ok((
                    delta,
                    ExpiryTransition {
                        had_ttl: true,
                        has_ttl_after: false,
                    },
                ));
            }

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
            let hash = table.hash_key_bytes(key.as_bytes());
            table
                .replace_value_preserving_ttl_prehashed_and_lsn(
                    key.as_bytes(),
                    VortexValue::Integer(result),
                    hash,
                    lsn,
                )
                .expect("live key must exist while replacing increment result");
            Ok((
                result,
                ExpiryTransition {
                    had_ttl,
                    has_ttl_after: had_ttl,
                },
            ))
        }
        None => {
            let hash = table.hash_key_bytes(key.as_bytes());
            let _ =
                table.insert_no_ttl_prehashed_and_lsn(key, VortexValue::Integer(delta), hash, lsn);
            Ok((
                delta,
                ExpiryTransition {
                    had_ttl: false,
                    has_ttl_after: false,
                },
            ))
        }
    }
}

fn increment_table_by_float(
    table: &mut SwissTable,
    key: VortexKey,
    increment: f64,
    lsn: u64,
    now_nanos: u64,
) -> Result<Bytes, &'static [u8]> {
    let current = match table.get_with_ttl(&key) {
        Some((value, ttl)) => {
            if ttl != 0 && ttl <= now_nanos {
                table.remove(&key);
                0.0
            } else {
                match value {
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
                }
            }
        }
        None => 0.0,
    };

    let result = current + increment;
    if result.is_nan() || result.is_infinite() {
        return Err(ERR_NOT_FLOAT);
    }

    let mut buffer = ryu::Buffer::new();
    let text = buffer.format(result);
    let bytes = Bytes::copy_from_slice(text.as_bytes());
    let _ = table.insert_and_lsn(key, VortexValue::from_bytes(bytes.as_ref()), lsn);
    Ok(bytes)
}

fn append_to_table(
    table: &mut SwissTable,
    key: VortexKey,
    append_bytes: &[u8],
    lsn: u64,
    now_nanos: u64,
) -> Result<usize, &'static [u8]> {
    let _ = remove_if_expired(table, &key, now_nanos);

    let hash = table.hash_key_bytes(key.as_bytes());
    if !table.contains_key_prehashed(key.as_bytes(), hash) {
        table.insert_new_prehashed_and_lsn(key, VortexValue::from_bytes(append_bytes), hash, lsn);
        return Ok(append_bytes.len());
    }

    let (length, new_value) = {
        let existing = table
            .get(&key)
            .expect("key must exist after contains_key_prehashed");
        match existing {
            VortexValue::InlineString(inline) => {
                let new_len = inline.len() + append_bytes.len();
                let mut next_inline = inline.clone();
                if next_inline.try_extend(append_bytes) {
                    (new_len, VortexValue::InlineString(next_inline))
                } else {
                    let mut combined = Vec::with_capacity(new_len);
                    combined.extend_from_slice(inline.as_bytes());
                    combined.extend_from_slice(append_bytes);
                    (new_len, VortexValue::String(Bytes::from(combined)))
                }
            }
            VortexValue::String(bytes) => {
                let new_len = bytes.len() + append_bytes.len();
                let mut combined = Vec::with_capacity(new_len);
                combined.extend_from_slice(bytes.as_ref());
                combined.extend_from_slice(append_bytes);
                let new_value = if new_len <= vortex_common::MAX_INLINE_VALUE_LEN {
                    VortexValue::InlineString(InlineBytes::from_slice(&combined))
                } else {
                    VortexValue::String(Bytes::from(combined))
                };
                let length = match &new_value {
                    VortexValue::InlineString(inline) => inline.len(),
                    VortexValue::String(bytes) => bytes.len(),
                    _ => 0,
                };
                (length, new_value)
            }
            VortexValue::Integer(number) => {
                let mut buffer = itoa::Buffer::new();
                let text = buffer.format(*number);
                let mut combined = Vec::with_capacity(text.len() + append_bytes.len());
                combined.extend_from_slice(text.as_bytes());
                combined.extend_from_slice(append_bytes);
                let new_value = VortexValue::from_bytes(&combined);
                let length = new_value.strlen();
                (length, new_value)
            }
            _ => return Err(ERR_WRONG_TYPE),
        }
    };
    table
        .replace_value_preserving_ttl_prehashed_and_lsn(key.as_bytes(), new_value, hash, lsn)
        .expect("live key must exist while replacing append result");
    Ok(length)
}

/// Compute GETRANGE on a borrowed value. Extracted so both the legacy table
/// path and the double-checked-locking concurrent path can share the logic.
fn getrange_from_value(
    value: &VortexValue,
    start: i64,
    end: i64,
) -> Result<Option<Bytes>, &'static [u8]> {
    fn range_bounds(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
        if len == 0 {
            return None;
        }

        let len_i64 = len as i64;
        let start_index = if start < 0 {
            (len_i64 + start).max(0) as usize
        } else {
            start.min(len_i64) as usize
        };
        let end_index = if end < 0 {
            (len_i64 + end).max(0) as usize
        } else {
            end.min(len_i64 - 1).max(0) as usize
        };

        if start_index > end_index || start_index >= len {
            None
        } else {
            Some((start_index, end_index.min(len - 1)))
        }
    }

    match value {
        VortexValue::InlineString(inline) => Ok(match range_bounds(inline.len(), start, end) {
            Some((start_index, end_index)) => Some(Bytes::copy_from_slice(
                &inline.as_bytes()[start_index..=end_index],
            )),
            None => Some(Bytes::new()),
        }),
        VortexValue::String(bytes) => Ok(match range_bounds(bytes.len(), start, end) {
            Some((start_index, end_index)) => Some(bytes.slice(start_index..=end_index)),
            None => Some(Bytes::new()),
        }),
        VortexValue::Integer(number) => {
            let mut buffer = itoa::Buffer::new();
            let text = buffer.format(*number);
            Ok(match range_bounds(text.len(), start, end) {
                Some((start_index, end_index)) => Some(Bytes::copy_from_slice(
                    &text.as_bytes()[start_index..=end_index],
                )),
                None => Some(Bytes::new()),
            })
        }
        _ => Err(ERR_WRONG_TYPE),
    }
}

fn setrange_in_table(
    table: &mut SwissTable,
    key: VortexKey,
    offset: usize,
    new_bytes: &[u8],
    lsn: u64,
    now_nanos: u64,
) -> Result<usize, &'static [u8]> {
    let _ = remove_if_expired(table, &key, now_nanos);

    let mut data = match table.get(&key) {
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
    let _ = table.insert_and_lsn(key, VortexValue::from_bytes(&data), lsn);
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
    #[inline]
    fn cleanup_expired_key(
        &self,
        shard_index: usize,
        table: &mut SwissTable,
        key: &VortexKey,
        now_nanos: u64,
    ) -> bool {
        let had_ttl = ttl_present(table.get_entry_ttl(key));
        let removed = remove_if_expired(table, key, now_nanos);
        if removed {
            self.update_expiry_count(shard_index, had_ttl, false);
            self.bump_watch_key(key);
        }
        removed
    }

    #[inline]
    fn cleanup_expired_prehashed(
        &self,
        shard_index: usize,
        table: &mut SwissTable,
        key_bytes: &[u8],
        hash: u64,
        now_nanos: u64,
    ) -> bool {
        let had_expired_ttl = matches!(
            table.get_with_ttl_prehashed(key_bytes, hash),
            Some((_, ttl)) if ttl != 0 && ttl <= now_nanos
        );
        if !had_expired_ttl {
            return false;
        }

        let _ = table.get_or_expire_prehashed(key_bytes, hash, now_nanos);
        let has_ttl = matches!(
            table.get_with_ttl_prehashed(key_bytes, hash),
            Some((_, ttl)) if ttl != 0
        );
        self.update_expiry_count(shard_index, true, has_ttl);
        self.bump_watch_key_bytes(key_bytes);
        true
    }

    pub(crate) fn get_value(&self, key: &VortexKey, now_nanos: u64) -> Option<VortexValue> {
        let shard_index = self.shard_index(key.as_bytes());
        let table_hash = self.table_hash_key(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                self.record_access_prehashed(&guard, key.as_bytes(), table_hash);
                Some(value.clone())
            }
            Some(_) => {
                // Expired — drop read lock, escalate to write lock, double-check.
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                self.cleanup_expired_key(shard_index, &mut wguard, key, now_nanos);
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
        now_nanos: u64,
    ) -> MutationResult<Option<VortexValue>> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let projected_delta = self
            .read_shard_by_index(shard_index)
            .projected_insert_delta(&key, &value);
        let (evicted, reservation) =
            self.ensure_memory_for(shard_index, positive_delta(projected_delta), now_nanos)?;
        let mut guard = self.write_shard_by_index(shard_index);
        let had_ttl = ttl_present(guard.get_entry_ttl(&key));
        let watched_key = self.watch_tracking_active().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        let previous = guard.insert_with_ttl_and_lsn(key, value, ttl_deadline_nanos, lsn);
        self.update_expiry_count(shard_index, had_ttl, true);
        self.record_frequency_hash(table_hash);
        if let Some(key) = watched_key {
            self.bump_watch_key(&key);
        }
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(previous, aof_lsn, evicted))
    }

    /// Fast path for plain `SET key value` (no options, no TTL).
    ///
    /// Eliminates three redundant table probes:
    /// - No `remove_if_expired` probe (insert overwrites unconditionally)
    /// - No `get_entry_ttl` pre-probe (fused into `insert_no_ttl`)
    /// - Single probe in `insert_no_ttl` returns old TTL status
    ///
    /// # Why skipping `remove_if_expired` is correct
    ///
    /// Plain SET unconditionally overwrites the key with TTL=0, so any
    /// expired value is replaced regardless. The expiry count is still
    /// correctly maintained via the `old_had_ttl` return value.
    #[inline]
    pub(crate) fn set_value_plain_bytes(
        &self,
        key_bytes: &[u8],
        value_bytes: &[u8],
        now_nanos: u64,
    ) -> MutationResult<()> {
        let features = self.mutation_features();
        if features != 0 {
            return self.set_value_plain(
                VortexKey::from(key_bytes),
                VortexValue::from_bytes(value_bytes),
                now_nanos,
            );
        }

        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let mut guard = self.write_shard_by_index(shard_index);
        let (lsn, _aof_lsn) = self.allocate_mutation_lsn();
        let old_had_ttl = if value_bytes.len() <= vortex_common::MAX_INLINE_VALUE_LEN {
            let value = VortexValue::from_bytes(value_bytes);
            let (_old, old_had_ttl) =
                guard.insert_no_ttl_bytes_prehashed_and_lsn(key_bytes, value, table_hash, lsn);
            old_had_ttl
        } else {
            guard.insert_no_ttl_raw_value_prehashed_and_lsn(key_bytes, value_bytes, table_hash, lsn)
        };
        self.update_expiry_count(shard_index, old_had_ttl, false);
        Ok(MutationOutcome::new((), None))
    }

    #[inline]
    pub(crate) fn set_value_plain(
        &self,
        key: VortexKey,
        value: VortexValue,
        now_nanos: u64,
    ) -> MutationResult<()> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        // Pre-hash for the table BEFORE acquiring the write lock.
        let table_hash = self.table_hash_key(key_bytes);
        let features = self.mutation_features();
        let (evicted, reservation) = if ConcurrentKeyspace::mutation_feature_maxmemory(features) {
            let eviction = self.eviction_config();
            if eviction.max_memory == 0 || self.replay_mode_active() {
                (None, MemoryReservation::new(self, 0))
            } else {
                let projected_delta = self
                    .read_shard_by_index(shard_index)
                    .projected_insert_delta_prehashed(&key, &value, table_hash);
                self.ensure_memory_for_snapshot(
                    shard_index,
                    positive_delta(projected_delta),
                    now_nanos,
                    eviction,
                )?
            }
        } else {
            (None, MemoryReservation::new(self, 0))
        };
        let mut guard = self.write_shard_by_index(shard_index);
        let watched_key = ConcurrentKeyspace::mutation_feature_watch(features).then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn_with_features(features);
        let (_old, old_had_ttl) =
            guard.insert_no_ttl_prehashed_and_lsn(key, value, table_hash, lsn);
        self.update_expiry_count(shard_index, old_had_ttl, false);
        if ConcurrentKeyspace::mutation_feature_maxmemory(features) {
            self.record_frequency_hash(table_hash);
        }
        if let Some(key) = watched_key {
            self.bump_watch_key(&key);
        }
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions((), aof_lsn, evicted))
    }
    pub(crate) fn set_value_with_options(
        &self,
        key: VortexKey,
        value: VortexValue,
        options: SetOptions,
        now_nanos: u64,
    ) -> MutationResult<SetResult> {
        let shard_index = self.shard_index(key.as_bytes());
        let projected_delta = projected_set_write_delta(
            &self.read_shard_by_index(shard_index),
            &key,
            &value,
            options,
            now_nanos,
        );
        let (evicted, reservation) =
            self.ensure_memory_for(shard_index, projected_delta, now_nanos)?;
        let mut guard = self.write_shard_by_index(shard_index);
        let had_ttl = ttl_present(guard.get_entry_ttl(&key));
        let (result, has_ttl) =
            set_with_options_on_table(&mut guard, key.clone(), value, options, now_nanos);
        self.update_expiry_count(shard_index, had_ttl, has_ttl);
        let changed = matches!(&result, SetResult::Ok | SetResult::OkGet(_));
        let aof_lsn = if changed {
            let key_bytes = key.as_bytes();
            let table_hash = self.table_hash_key(key_bytes);
            let (lsn, aof_lsn) = self.allocate_mutation_lsn();
            stamp_entry_lsn(&mut guard, key_bytes, table_hash, lsn);
            aof_lsn
        } else {
            None
        };
        if changed {
            self.record_frequency_hash(self.table_hash_key(key.as_bytes()));
            self.bump_watch_key(&key);
        }
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(result, aof_lsn, evicted))
    }

    pub(crate) fn mget_frames(&self, keys: &[&[u8]], now_nanos: u64) -> Vec<RespFrame> {
        if keys.is_empty() {
            return Vec::new();
        }

        #[derive(Clone, Copy)]
        struct KeyLookup {
            output_idx: usize,
            shard_idx: usize,
            hash: u64,
        }

        let mut lookups: SmallVec<[KeyLookup; 16]> = SmallVec::with_capacity(keys.len());
        for (output_idx, &key_bytes) in keys.iter().enumerate() {
            lookups.push(KeyLookup {
                output_idx,
                shard_idx: self.shard_index(key_bytes),
                hash: self.table_hash_key(key_bytes),
            });
        }
        lookups.sort_unstable_by_key(|lookup| lookup.shard_idx);

        let mut frames = Vec::with_capacity(keys.len());
        frames.resize_with(keys.len(), RespFrame::null_bulk_string);
        let mut expired: SmallVec<[KeyLookup; 8]> = SmallVec::new();

        // Process one shard at a time so the hot path avoids the extra vectors,
        // binary searches, and guard fan-out of the generic multi-read helper.
        let mut cursor = 0;
        while cursor < lookups.len() {
            let shard_idx = lookups[cursor].shard_idx;
            let group_start = cursor;
            while cursor < lookups.len() && lookups[cursor].shard_idx == shard_idx {
                cursor += 1;
            }

            let guard = self.read_shard_by_index(shard_idx);
            for lookup in &lookups[group_start..cursor] {
                guard.prefetch_group(lookup.hash);
            }

            for lookup in &lookups[group_start..cursor] {
                let key_bytes = keys[lookup.output_idx];
                match guard.get_with_ttl_prehashed(key_bytes, lookup.hash) {
                    Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                        self.record_access_prehashed(&guard, key_bytes, lookup.hash);
                        frames[lookup.output_idx] = mget_value_to_frame(value);
                    }
                    Some(_) => {
                        expired.push(*lookup);
                    }
                    None => {}
                }
            }
        }

        // `expired` is emitted in the same shard-group order as the read pass,
        // so we can batch lazy expiry cleanup with one write lock per shard.
        let mut expired_cursor = 0;
        while expired_cursor < expired.len() {
            let shard_idx = expired[expired_cursor].shard_idx;
            let mut wguard = self.write_shard_by_index(shard_idx);
            while expired_cursor < expired.len() && expired[expired_cursor].shard_idx == shard_idx {
                let lookup = expired[expired_cursor];
                let _ = self.cleanup_expired_prehashed(
                    shard_idx,
                    &mut wguard,
                    keys[lookup.output_idx],
                    lookup.hash,
                    now_nanos,
                );
                expired_cursor += 1;
            }
        }

        frames
    }

    pub(crate) fn mset_values(
        &self,
        pairs: Vec<(VortexKey, VortexValue)>,
        now_nanos: u64,
    ) -> MutationResult<()> {
        if pairs.is_empty() {
            return Ok(MutationOutcome::new((), None));
        }

        let projected_delta: usize = pairs
            .iter()
            .map(|(key, value)| {
                let shard_index = self.shard_index(key.as_bytes());
                let guard = self.read_shard_by_index(shard_index);
                positive_delta(guard.projected_insert_delta(key, value))
            })
            .sum();
        let (evicted, reservation) = self.ensure_memory_for(
            self.shard_index(pairs[0].0.as_bytes()),
            projected_delta,
            now_nanos,
        )?;

        let (key_refs, lookups) = build_multi_write_lookups(self, &pairs);
        let (mut guards, sorted_shards, _) = self.multi_write(&key_refs);
        drop(key_refs);
        let mut guard_pos = 0usize;
        let mut pairs = pairs.into_iter().map(Some).collect::<Vec<_>>();
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();

        for lookup in lookups {
            while sorted_shards[guard_pos] != lookup.shard_idx {
                guard_pos += 1;
            }
            let table = &mut *guards[guard_pos].1;
            let (key, value) = pairs[lookup.pair_index]
                .take()
                .expect("mset pair must be available exactly once");
            self.bump_watch_key(&key);
            let (_previous, old_had_ttl) =
                table.insert_no_ttl_prehashed_and_lsn(key, value, lookup.table_hash, lsn);
            self.update_expiry_count(lookup.shard_idx, old_had_ttl, false);
            self.record_frequency_hash(lookup.table_hash);
        }
        drop(guards);
        reservation.settle();
        Ok(mutation_outcome_with_evictions((), aof_lsn, evicted))
    }

    pub(crate) fn msetnx_values(
        &self,
        pairs: Vec<(VortexKey, VortexValue)>,
        now_nanos: u64,
    ) -> MutationResult<bool> {
        if pairs.is_empty() {
            return Ok(MutationOutcome::new(true, None));
        }

        let all_absent = pairs.iter().all(|(key, _)| {
            !matches!(
                self.read_shard_by_index(self.shard_index(key.as_bytes()))
                    .get_with_ttl(key),
                Some((_, ttl)) if ttl == 0 || ttl > now_nanos
            )
        });
        if !all_absent {
            return Ok(MutationOutcome::new(false, None));
        }

        let projected_delta: usize = pairs
            .iter()
            .map(|(key, value)| {
                let shard_index = self.shard_index(key.as_bytes());
                let guard = self.read_shard_by_index(shard_index);
                positive_delta(guard.projected_insert_delta(key, value))
            })
            .sum();
        let (evicted, reservation) = self.ensure_memory_for(
            self.shard_index(pairs[0].0.as_bytes()),
            projected_delta,
            now_nanos,
        )?;

        let (key_refs, lookups) = build_multi_write_lookups(self, &pairs);
        let (mut guards, sorted_shards, _) = self.multi_write(&key_refs);
        drop(key_refs);
        let mut guard_pos = 0usize;

        for lookup in &lookups {
            while sorted_shards[guard_pos] != lookup.shard_idx {
                guard_pos += 1;
            }
            let table = &mut *guards[guard_pos].1;
            let key_bytes = pairs[lookup.pair_index].0.as_bytes();
            let _ = self.cleanup_expired_prehashed(
                lookup.shard_idx,
                table,
                key_bytes,
                lookup.table_hash,
                now_nanos,
            );
            if table.contains_key_prehashed(key_bytes, lookup.table_hash) {
                drop(guards);
                reservation.settle();
                return Ok(mutation_outcome_with_evictions(false, None, evicted));
            }
        }

        let mut pairs = pairs.into_iter().map(Some).collect::<Vec<_>>();
        guard_pos = 0;
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        for lookup in lookups {
            while sorted_shards[guard_pos] != lookup.shard_idx {
                guard_pos += 1;
            }
            let table = &mut *guards[guard_pos].1;
            let (key, value) = pairs[lookup.pair_index]
                .take()
                .expect("msetnx pair must be available exactly once");
            self.bump_watch_key(&key);
            table.insert_new_prehashed_and_lsn(key, value, lookup.table_hash, lsn);
            self.record_frequency_hash(lookup.table_hash);
        }
        drop(guards);
        reservation.settle();

        Ok(mutation_outcome_with_evictions(true, aof_lsn, evicted))
    }

    pub(crate) fn remove_value(
        &self,
        key: &VortexKey,
        now_nanos: u64,
    ) -> MutationOutcome<Option<VortexValue>> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        let (removed, had_ttl) = take_live_value(&mut guard, key, now_nanos);
        self.update_expiry_count(shard_index, had_ttl, false);
        let changed = removed.is_some();
        if changed {
            self.bump_watch_key(key);
        }
        MutationOutcome::new(removed, changed.then(|| self.next_aof_lsn()).flatten())
    }

    pub(crate) fn delete_key_bytes(
        &self,
        key_bytes: &[u8],
        now_nanos: u64,
    ) -> MutationOutcome<bool> {
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let mut guard = self.write_shard_by_index(shard_index);
        let (deleted, had_ttl) =
            delete_live_key_bytes(&mut guard, key_bytes, table_hash, now_nanos);
        self.update_expiry_count(shard_index, had_ttl, false);
        if deleted {
            self.bump_watch_key_bytes(key_bytes);
        }
        MutationOutcome::new(deleted, deleted.then(|| self.next_aof_lsn()).flatten())
    }

    pub(crate) fn increment_by(
        &self,
        key: VortexKey,
        delta: i64,
        now_nanos: u64,
    ) -> MutationResult<i64> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta = projected_increment_delta(&read_guard, &key, delta, now_nanos)?;
        drop(read_guard);
        let (evicted, reservation) =
            self.ensure_memory_for(shard_index, projected_delta, now_nanos)?;
        let mut guard = self.write_shard_by_index(shard_index);
        let watched_key = self.watch_tracking_active().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        let (result, transition) = match increment_table_by(&mut guard, key, delta, lsn, now_nanos)
        {
            Ok(result) => result,
            Err(err) => return Err(MutationError::with_evictions(err, evicted)),
        };
        self.update_expiry_count(shard_index, transition.had_ttl, transition.has_ttl_after);
        self.record_frequency_hash(table_hash);
        if let Some(key) = watched_key {
            self.bump_watch_key(&key);
        }
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(result, aof_lsn, evicted))
    }

    pub(crate) fn increment_by_float(
        &self,
        key: VortexKey,
        increment: f64,
        now_nanos: u64,
    ) -> MutationResult<Bytes> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta =
            projected_increment_by_float_delta(&read_guard, &key, increment, now_nanos)?;
        drop(read_guard);
        let (evicted, reservation) =
            self.ensure_memory_for(shard_index, projected_delta, now_nanos)?;
        let mut guard = self.write_shard_by_index(shard_index);
        let ttl_deadline = guard.get_entry_ttl(&key);
        let watched_key = self.watch_tracking_active().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        let result = match increment_table_by_float(&mut guard, key, increment, lsn, now_nanos) {
            Ok(result) => result,
            Err(err) => return Err(MutationError::with_evictions(err, evicted)),
        };
        self.update_expiry_count(
            shard_index,
            ttl_present(ttl_deadline),
            matches!(ttl_deadline, Some(deadline) if deadline > now_nanos),
        );
        self.record_frequency_hash(table_hash);
        if let Some(key) = watched_key {
            self.bump_watch_key(&key);
        }
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(result, aof_lsn, evicted))
    }

    pub(crate) fn append_value(
        &self,
        key: VortexKey,
        append_bytes: &[u8],
        now_nanos: u64,
    ) -> MutationResult<usize> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta = projected_append_delta(&read_guard, &key, append_bytes, now_nanos)?;
        drop(read_guard);
        let (evicted, reservation) =
            self.ensure_memory_for(shard_index, projected_delta, now_nanos)?;
        let mut guard = self.write_shard_by_index(shard_index);
        let ttl_deadline = guard.get_entry_ttl(&key);
        let watched_key = self.watch_tracking_active().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        let length = match append_to_table(&mut guard, key, append_bytes, lsn, now_nanos) {
            Ok(length) => length,
            Err(err) => return Err(MutationError::with_evictions(err, evicted)),
        };
        self.update_expiry_count(
            shard_index,
            ttl_present(ttl_deadline),
            matches!(ttl_deadline, Some(deadline) if deadline > now_nanos),
        );
        self.record_frequency_hash(table_hash);
        if let Some(key) = watched_key {
            self.bump_watch_key(&key);
        }
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(length, aof_lsn, evicted))
    }

    pub(crate) fn strlen_value(
        &self,
        key: &VortexKey,
        now_nanos: u64,
    ) -> Result<Option<usize>, &'static [u8]> {
        let shard_index = self.shard_index(key.as_bytes());
        let table_hash = self.table_hash_key(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                self.record_access_prehashed(&guard, key.as_bytes(), table_hash);
                if !value.is_string() {
                    Err(ERR_WRONG_TYPE)
                } else {
                    Ok(Some(value.strlen()))
                }
            }
            Some(_) => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                self.cleanup_expired_key(shard_index, &mut wguard, key, now_nanos);
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
        let table_hash = self.table_hash_key(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                self.record_access_prehashed(&guard, key.as_bytes(), table_hash);
                // Live key — compute range while read guard is held.
                getrange_from_value(value, start, end)
            }
            Some(_) => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                self.cleanup_expired_key(shard_index, &mut wguard, key, now_nanos);
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
    ) -> MutationResult<usize> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta =
            projected_setrange_delta(&read_guard, &key, offset, new_bytes, now_nanos)?;
        drop(read_guard);
        let (evicted, reservation) =
            self.ensure_memory_for(shard_index, projected_delta, now_nanos)?;
        let mut guard = self.write_shard_by_index(shard_index);
        let had_ttl = ttl_present(guard.get_entry_ttl(&key));
        let ttl_probe_key = key.clone();
        let watched_key = self.watch_tracking_active().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        let length = match setrange_in_table(&mut guard, key, offset, new_bytes, lsn, now_nanos) {
            Ok(length) => length,
            Err(err) => return Err(MutationError::with_evictions(err, evicted)),
        };
        let has_ttl_after = ttl_present(guard.get_entry_ttl(&ttl_probe_key));
        self.update_expiry_count(shard_index, had_ttl, has_ttl_after);
        self.record_frequency_hash(table_hash);
        if let Some(key) = watched_key {
            self.bump_watch_key(&key);
        }
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(length, aof_lsn, evicted))
    }

    pub(crate) fn delete_keys(&self, keys: &[VortexKey], now_nanos: u64) -> MutationOutcome<i64> {
        if keys.is_empty() {
            return MutationOutcome::new(0, None);
        }
        if keys.len() == 1 {
            let removed = self.remove_value(&keys[0], now_nanos);
            let deleted = removed.value.is_some();
            return removed.map_value(i64::from(deleted));
        }

        let key_refs: Vec<&[u8]> = keys.iter().map(VortexKey::as_bytes).collect();
        let (mut guards, sorted_shards, per_key_shards) = self.multi_write(&key_refs);
        let mut deleted = 0i64;

        for (key, shard_index) in keys.iter().zip(per_key_shards.iter().copied()) {
            let position = ConcurrentKeyspace::guard_position(&sorted_shards, shard_index);
            let table = &mut *guards[position].1;
            let (removed, had_ttl) = take_live_value(table, key, now_nanos);
            if removed.is_some() {
                deleted += 1;
                self.bump_watch_key(key);
            }
            self.update_expiry_count(shard_index, had_ttl, false);
        }

        MutationOutcome::new(
            deleted,
            (deleted != 0).then(|| self.next_aof_lsn()).flatten(),
        )
    }

    pub(crate) fn count_existing(&self, keys: &[VortexKey], now_nanos: u64) -> i64 {
        if keys.is_empty() {
            return 0;
        }

        // Single-key fast path: skip multi_read machinery entirely.
        if keys.len() == 1 {
            let key = &keys[0];
            let shard_index = self.shard_index(key.as_bytes());
            let table_hash = self.table_hash_key(key.as_bytes());
            let guard = self.read_shard_by_index(shard_index);
            return match guard.get_with_ttl(key) {
                Some((_, ttl)) if ttl == 0 || ttl > now_nanos => {
                    self.record_access_prehashed(&guard, key.as_bytes(), table_hash);
                    1
                }
                Some(_) => {
                    drop(guard);
                    let mut wguard = self.write_shard_by_index(shard_index);
                    self.cleanup_expired_key(shard_index, &mut wguard, key, now_nanos);
                    0
                }
                None => 0,
            };
        }

        let key_refs: Vec<&[u8]> = keys.iter().map(VortexKey::as_bytes).collect();
        let (guards, sorted_shards, per_key_shards) = self.multi_read(&key_refs);
        let mut count = 0i64;
        let mut expired_indices: Vec<usize> = Vec::new();

        for (idx, (key, shard_index)) in keys.iter().zip(per_key_shards.iter().copied()).enumerate()
        {
            let position = ConcurrentKeyspace::guard_position(&sorted_shards, shard_index);
            match guards[position].1.get_with_ttl(key) {
                Some((_, ttl)) if ttl == 0 || ttl > now_nanos => {
                    self.record_access_prehashed(
                        &guards[position].1,
                        key.as_bytes(),
                        self.table_hash_key(key.as_bytes()),
                    );
                    count += 1;
                }
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
                self.cleanup_expired_key(shard_index, &mut wguard, &keys[idx], now_nanos);
            }
        }

        count
    }

    pub(crate) fn expire_key(
        &self,
        key: &VortexKey,
        deadline_nanos: u64,
        now_nanos: u64,
    ) -> MutationOutcome<bool> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        let _ = self.cleanup_expired_key(shard_index, &mut guard, key, now_nanos);
        let had_ttl = ttl_present(guard.get_entry_ttl(key));
        let updated = guard.set_entry_ttl(key, deadline_nanos);
        let aof_lsn = if updated {
            let key_bytes = key.as_bytes();
            let table_hash = self.table_hash_key(key_bytes);
            let (lsn, aof_lsn) = self.allocate_mutation_lsn();
            stamp_entry_lsn(&mut guard, key_bytes, table_hash, lsn);
            aof_lsn
        } else {
            None
        };
        self.update_expiry_count(shard_index, had_ttl, updated);
        if updated {
            self.bump_watch_key(key);
        }
        MutationOutcome::new(updated, aof_lsn)
    }

    pub(crate) fn persist_key(&self, key: &VortexKey) -> MutationOutcome<bool> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        let had_ttl = ttl_present(guard.get_entry_ttl(key));
        let updated = guard.clear_entry_ttl(key);
        let aof_lsn = if updated {
            let key_bytes = key.as_bytes();
            let table_hash = self.table_hash_key(key_bytes);
            let (lsn, aof_lsn) = self.allocate_mutation_lsn();
            stamp_entry_lsn(&mut guard, key_bytes, table_hash, lsn);
            aof_lsn
        } else {
            None
        };
        self.update_expiry_count(shard_index, had_ttl, false);
        if updated {
            self.bump_watch_key(key);
        }
        MutationOutcome::new(updated, aof_lsn)
    }

    pub(crate) fn ttl_state(&self, key: &VortexKey, now_nanos: u64) -> TtlState {
        self.ttl_state_bytes(key.as_bytes(), now_nanos)
    }

    pub(crate) fn ttl_state_bytes(&self, key_bytes: &[u8], now_nanos: u64) -> TtlState {
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl_prehashed(key_bytes, table_hash) {
            None => TtlState::Missing,
            Some((_, 0)) => {
                self.record_access_prehashed(&guard, key_bytes, table_hash);
                TtlState::Persistent
            }
            Some((_, deadline)) if deadline <= now_nanos => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                let _ = self.cleanup_expired_prehashed(
                    shard_index,
                    &mut wguard,
                    key_bytes,
                    table_hash,
                    now_nanos,
                );
                TtlState::Missing
            }
            Some((_, deadline)) => {
                self.record_access_prehashed(&guard, key_bytes, table_hash);
                TtlState::Deadline(deadline)
            }
        }
    }

    pub(crate) fn type_of_key(&self, key: &VortexKey, now_nanos: u64) -> Option<&'static str> {
        let shard_index = self.shard_index(key.as_bytes());
        let table_hash = self.table_hash_key(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                self.record_access_prehashed(&guard, key.as_bytes(), table_hash);
                Some(value.type_name())
            }
            Some(_) => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                self.cleanup_expired_key(shard_index, &mut wguard, key, now_nanos);
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
    ) -> MutationResult<bool> {
        let source_shard = self.shard_index(old_key.as_bytes());
        let destination_shard = self.shard_index(new_key.as_bytes());
        let new_hash = self.table_hash_key(new_key.as_bytes());

        if source_shard == destination_shard {
            let destination = new_key.clone();
            let mut guard = self.write_shard_by_index(source_shard);
            let _ = self.cleanup_expired_key(source_shard, &mut guard, old_key, now_nanos);
            if old_key != &destination {
                let _ = self.cleanup_expired_key(source_shard, &mut guard, &destination, now_nanos);
            }
            let old_had_ttl = ttl_present(guard.get_entry_ttl(old_key));
            let new_had_ttl = ttl_present(guard.get_entry_ttl(&destination));
            let same_key = old_key == &destination;
            let renamed = rename_within_table(&mut guard, old_key, new_key, now_nanos, nx);
            let old_has_ttl = ttl_present(guard.get_entry_ttl(old_key));
            let new_has_ttl = ttl_present(guard.get_entry_ttl(&destination));
            self.update_expiry_count(source_shard, old_had_ttl, old_has_ttl);
            if !same_key {
                self.update_expiry_count(source_shard, new_had_ttl, new_has_ttl);
            }
            let renamed = renamed?;
            let changed = renamed && !same_key;
            let aof_lsn = if changed {
                let (lsn, aof_lsn) = self.allocate_mutation_lsn();
                stamp_entry_lsn(&mut guard, destination.as_bytes(), new_hash, lsn);
                aof_lsn
            } else {
                None
            };
            if changed {
                self.record_frequency_hash(new_hash);
                self.bump_watch_key(old_key);
                self.bump_watch_key(&destination);
            }
            return Ok(MutationOutcome::new(renamed, aof_lsn));
        }

        let key_refs = [old_key.as_bytes(), new_key.as_bytes()];
        let (read_guards, read_sorted_shards, read_per_key_shards) = self.multi_read(&key_refs);
        let read_src_position =
            ConcurrentKeyspace::guard_position(&read_sorted_shards, read_per_key_shards[0]);
        let read_dst_position =
            ConcurrentKeyspace::guard_position(&read_sorted_shards, read_per_key_shards[1]);
        let projected_delta = projected_rename_delta(
            &read_guards[read_src_position].1,
            old_key,
            &read_guards[read_dst_position].1,
            &new_key,
            now_nanos,
            nx,
        )?;
        drop(read_guards);
        let (evicted, reservation) =
            self.ensure_memory_for(destination_shard, projected_delta, now_nanos)?;

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

        let _ = self.cleanup_expired_key(source_shard, src_table, old_key, now_nanos);
        let _ = self.cleanup_expired_key(destination_shard, dst_table, &new_key, now_nanos);
        let old_had_ttl = ttl_present(src_table.get_entry_ttl(old_key));
        let new_had_ttl = ttl_present(dst_table.get_entry_ttl(&new_key));
        if !src_table.contains_key(old_key) {
            return Err(MutationError::with_evictions(
                b"-ERR no such key\r\n",
                evicted,
            ));
        }
        if nx && dst_table.contains_key(&new_key) {
            drop(guards);
            reservation.settle();
            return Ok(mutation_outcome_with_evictions(false, None, evicted));
        }

        let destination_key = new_key.clone();
        let (value, ttl) = src_table
            .remove_with_ttl(old_key)
            .expect("source key must exist after contains_key check");
        dst_table.remove(&new_key);
        let watched_new_key = self.watch_tracking_active().then(|| new_key.clone());
        if ttl != 0 && ttl > now_nanos {
            dst_table.insert_with_ttl(new_key, value, ttl);
        } else {
            dst_table.insert(new_key, value);
        }
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        stamp_entry_lsn(dst_table, destination_key.as_bytes(), new_hash, lsn);
        self.update_expiry_count(source_shard, old_had_ttl, false);
        self.update_expiry_count(destination_shard, new_had_ttl, ttl != 0 && ttl > now_nanos);
        self.record_frequency_hash(new_hash);
        self.bump_watch_key(old_key);
        if let Some(key) = watched_new_key {
            self.bump_watch_key(&key);
        }
        drop(guards);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(true, aof_lsn, evicted))
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
    ) -> MutationResult<bool> {
        let source_shard = self.shard_index(src.as_bytes());
        let destination_shard = self.shard_index(dst.as_bytes());
        let dst_hash = self.table_hash_key(dst.as_bytes());

        if source_shard == destination_shard {
            let read_guard = self.read_shard_by_index(source_shard);
            let source_value = match read_guard.get_with_ttl(src) {
                Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
                _ => None,
            };
            let projected_delta =
                projected_copy_delta(source_value, &read_guard, &dst, replace, now_nanos);
            drop(read_guard);
            let (evicted, reservation) =
                self.ensure_memory_for(destination_shard, projected_delta, now_nanos)?;
            let destination = dst.clone();
            let mut guard = self.write_shard_by_index(source_shard);
            let _ = self.cleanup_expired_key(source_shard, &mut guard, src, now_nanos);
            if src != &destination {
                let _ = self.cleanup_expired_key(source_shard, &mut guard, &destination, now_nanos);
            }
            let dst_had_ttl = ttl_present(guard.get_entry_ttl(&destination));
            let copied = copy_within_table(&mut guard, src, dst, replace, now_nanos);
            let dst_has_ttl = ttl_present(guard.get_entry_ttl(&destination));
            self.update_expiry_count(source_shard, dst_had_ttl, dst_has_ttl);
            let aof_lsn = if copied {
                let (lsn, aof_lsn) = self.allocate_mutation_lsn();
                stamp_entry_lsn(&mut guard, destination.as_bytes(), dst_hash, lsn);
                aof_lsn
            } else {
                None
            };
            if copied {
                self.record_frequency_hash(dst_hash);
                self.bump_watch_key(&destination);
            }
            drop(guard);
            reservation.settle();
            return Ok(mutation_outcome_with_evictions(copied, aof_lsn, evicted));
        }

        let key_refs = [src.as_bytes(), dst.as_bytes()];
        let (read_guards, read_sorted_shards, read_per_key_shards) = self.multi_read(&key_refs);
        let read_src_position =
            ConcurrentKeyspace::guard_position(&read_sorted_shards, read_per_key_shards[0]);
        let read_dst_position =
            ConcurrentKeyspace::guard_position(&read_sorted_shards, read_per_key_shards[1]);
        let source_value = match read_guards[read_src_position].1.get_with_ttl(src) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
            _ => None,
        };
        let projected_delta = projected_copy_delta(
            source_value,
            &read_guards[read_dst_position].1,
            &dst,
            replace,
            now_nanos,
        );
        drop(read_guards);
        let (evicted, reservation) =
            self.ensure_memory_for(destination_shard, projected_delta, now_nanos)?;

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

        let _ = self.cleanup_expired_key(source_shard, src_table, src, now_nanos);
        let _ = self.cleanup_expired_key(destination_shard, dst_table, &dst, now_nanos);
        let dst_had_ttl = ttl_present(dst_table.get_entry_ttl(&dst));
        let (value_clone, ttl) = {
            let Some((value, ttl)) = src_table.get_with_ttl(src) else {
                // Source key vanished — reservation will auto-settle via Drop.
                return Ok(mutation_outcome_with_evictions(false, None, evicted));
            };
            if ttl != 0 && ttl <= now_nanos {
                return Ok(mutation_outcome_with_evictions(false, None, evicted));
            }
            (value.clone(), ttl)
        };

        let _ = remove_if_expired(dst_table, &dst, now_nanos);
        if !replace && dst_table.contains_key(&dst) {
            return Ok(mutation_outcome_with_evictions(false, None, evicted));
        }

        let watched_dst = self.watch_tracking_active().then(|| dst.clone());
        let destination = dst.clone();
        if ttl != 0 && ttl > now_nanos {
            dst_table.insert_with_ttl(dst, value_clone, ttl);
        } else {
            dst_table.insert(dst, value_clone);
        }
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        stamp_entry_lsn(dst_table, destination.as_bytes(), dst_hash, lsn);
        self.update_expiry_count(destination_shard, dst_had_ttl, ttl != 0 && ttl > now_nanos);
        self.record_frequency_hash(dst_hash);
        if let Some(key) = watched_dst {
            self.bump_watch_key(&key);
        }
        drop(guards);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(true, aof_lsn, evicted))
    }

    pub(crate) fn cmd_dbsize(&self, now_nanos: u64) -> usize {
        let (keys, _) = self.exact_keyspace_counts(now_nanos);
        keys
    }

    pub(crate) fn cmd_flush_all(&self) -> Option<u64> {
        self.flush_all_with_lsn()
    }

    pub(crate) fn info_keyspace(&self, now_nanos: u64) -> (usize, usize) {
        self.exact_keyspace_counts(now_nanos)
    }
}
