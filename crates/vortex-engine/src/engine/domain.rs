//! Engine-domain operations for command handlers.
//!
//! Command modules parse RESP frames and shape replies. This module owns the
//! zero-cost mutation/read coordination over `ConcurrentKeyspace`: shard locks,
//! memory admission, TTL transitions, WATCH invalidation, eviction effects, AOF
//! LSN stamping, and table-level mutation helpers. It uses only inherent impls
//! and free functions, so there is no trait-object or boxed-operation overhead.

use core::mem::size_of;
use std::collections::HashMap;

use bytes::Bytes;
use smallvec::SmallVec;
use vortex_common::value::InlineBytes;
use vortex_common::{VortexKey, VortexValue};

use crate::EvictionConfig;
use crate::SwissTable;
use crate::entry::Entry;
use crate::keyspace::{
    ConcurrentKeyspace, EvictedKey, EvictedKeys, EvictionAdmissionError, ExpiryTransition,
    MemoryReservation, PositiveDelta, ProjectedDelta, ShardPlan, ShardWriteGuard, ShardWriteGuards,
};
use crate::table::{BorrowedKey, MutationPolicy, RawValueBytes, TableHash};

use crate::commands::pattern::glob_match;
use crate::commands::{
    AofRecord, AofRecords, ERR_NOT_FLOAT, ERR_NOT_INTEGER, ERR_OVERFLOW, ERR_WRONG_TYPE,
};

const SCAN_CURSOR_SHARD_SHIFT: u32 = 32;
const DEFAULT_RANDOM_SEED: u64 = 0xDEAD_BEEF_CAFE_BABE;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TtlState {
    Missing,
    Persistent,
    Deadline(u64),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ExpireOptions {
    pub(crate) nx: bool,
    pub(crate) xx: bool,
    pub(crate) gt: bool,
    pub(crate) lt: bool,
}

impl ExpireOptions {
    #[inline(always)]
    fn permits(self, current_ttl: u64, deadline_nanos: u64) -> bool {
        if self.nx && current_ttl != 0 {
            return false;
        }
        if self.xx && current_ttl == 0 {
            return false;
        }
        if self.gt && current_ttl != 0 && deadline_nanos <= current_ttl {
            return false;
        }
        if self.lt && current_ttl != 0 && deadline_nanos >= current_ttl {
            return false;
        }
        true
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GetExOption {
    None,
    ExpireAt(u64),
    Persist,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct AofEffect {
    lsn: Option<u64>,
}

impl AofEffect {
    #[inline(always)]
    const fn none() -> Self {
        Self { lsn: None }
    }

    #[inline(always)]
    const fn lsn(lsn: Option<u64>) -> Self {
        Self { lsn }
    }

    #[inline(always)]
    const fn into_lsn(self) -> Option<u64> {
        self.lsn
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TtlEffect {
    shard_index: usize,
    transition: ExpiryTransition,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum WatchEffect<'a> {
    #[default]
    None,
    Key(&'a VortexKey),
    KeyBytes(&'a [u8]),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MutationEffects<'a> {
    ttl: Option<TtlEffect>,
    watch: WatchEffect<'a>,
    frequency: Option<TableHash>,
    aof: AofEffect,
}

impl<'a> MutationEffects<'a> {
    #[inline(always)]
    const fn none() -> Self {
        Self {
            ttl: None,
            watch: WatchEffect::None,
            frequency: None,
            aof: AofEffect::none(),
        }
    }

    #[inline(always)]
    const fn with_ttl(mut self, shard_index: usize, transition: ExpiryTransition) -> Self {
        self.ttl = Some(TtlEffect {
            shard_index,
            transition,
        });
        self
    }

    #[inline(always)]
    const fn with_watch_key(mut self, key: &'a VortexKey) -> Self {
        self.watch = WatchEffect::Key(key);
        self
    }

    #[inline(always)]
    const fn with_watch_key_bytes(mut self, key_bytes: &'a [u8]) -> Self {
        self.watch = WatchEffect::KeyBytes(key_bytes);
        self
    }

    #[inline(always)]
    fn with_watch_key_if(self, condition: bool, key: &'a VortexKey) -> Self {
        if condition {
            self.with_watch_key(key)
        } else {
            self
        }
    }

    #[inline(always)]
    fn with_optional_watch_key(self, key: Option<&'a VortexKey>) -> Self {
        match key {
            Some(key) => self.with_watch_key(key),
            None => self,
        }
    }

    #[inline(always)]
    const fn with_frequency(mut self, hash: TableHash) -> Self {
        self.frequency = Some(hash);
        self
    }

    #[inline(always)]
    const fn with_aof_lsn(mut self, lsn: Option<u64>) -> Self {
        self.aof = AofEffect::lsn(lsn);
        self
    }
}

#[derive(Clone, Copy)]
struct MultiWriteLookup {
    pair_index: usize,
    shard_idx: usize,
    table_hash: TableHash,
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
fn stamp_entry_lsn(table: &mut SwissTable, key_bytes: &[u8], table_hash: TableHash, lsn: u64) {
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

pub(crate) struct FloatIncrementResult {
    pub(crate) value: Bytes,
    pub(crate) ttl_after: TtlState,
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

#[inline]
fn ttl_present(ttl_deadline: Option<u64>) -> bool {
    matches!(ttl_deadline, Some(deadline) if deadline != 0)
}

#[inline]
fn positive_delta(delta: isize) -> PositiveDelta {
    ProjectedDelta::from_bytes(delta).positive()
}

#[inline]
fn admission_revalidation_active(keyspace: &ConcurrentKeyspace, snapshot: EvictionConfig) -> bool {
    snapshot.max_memory != 0 && !keyspace.replay_mode_active()
}

#[inline]
fn entry_memory_usage(key: &VortexKey, value_memory_usage: usize) -> usize {
    size_of::<Entry>() + key.memory_usage() + value_memory_usage
}

#[inline]
fn merge_evicted_keys(current: &mut EvictedKeys, additional: EvictedKeys) {
    match (current.take(), additional) {
        (None, None) => {}
        (existing @ Some(_), None) => *current = existing,
        (None, additional @ Some(_)) => *current = additional,
        (Some(existing), Some(additional)) => {
            let mut merged: Vec<EvictedKey> = existing.into_vec();
            merged.extend(additional.into_vec());
            *current = Some(merged.into_boxed_slice());
        }
    }
}

#[inline]
fn reserve_memory_from_snapshot<'a>(
    keyspace: &'a ConcurrentKeyspace,
    preferred_shard: usize,
    additional_bytes: PositiveDelta,
    now_nanos: u64,
    snapshot: EvictionConfig,
) -> Result<(EvictedKeys, MemoryReservation<'a>), EvictionAdmissionError> {
    if snapshot.max_memory == 0 || keyspace.replay_mode_active() || additional_bytes.is_zero() {
        return Ok((None, MemoryReservation::new(keyspace, 0)));
    }

    keyspace.ensure_memory_for_snapshot(
        preferred_shard,
        additional_bytes.bytes(),
        now_nanos,
        snapshot,
    )
}

fn deduplicate_last_write_pairs(
    pairs: Vec<(VortexKey, VortexValue)>,
) -> Vec<(VortexKey, VortexValue)> {
    let mut unique: Vec<(VortexKey, VortexValue)> = Vec::with_capacity(pairs.len());
    let mut positions: HashMap<VortexKey, usize> = HashMap::with_capacity(pairs.len());

    for (key, value) in pairs {
        if let Some(&position) = positions.get(&key) {
            unique[position].1 = value;
            continue;
        }

        positions.insert(key.clone(), unique.len());
        unique.push((key, value));
    }

    unique
}

struct ReservationState<'a> {
    snapshot: EvictionConfig,
    reservation: MemoryReservation<'a>,
    evicted: EvictedKeys,
}

#[derive(Clone, Copy)]
struct ReservationCoordinator<'a> {
    keyspace: &'a ConcurrentKeyspace,
    now_nanos: u64,
}

impl<'a> ReservationCoordinator<'a> {
    #[inline(always)]
    const fn new(keyspace: &'a ConcurrentKeyspace, now_nanos: u64) -> Self {
        Self {
            keyspace,
            now_nanos,
        }
    }

    #[inline]
    fn reserve(
        self,
        preferred_shard: usize,
        additional_bytes: PositiveDelta,
        snapshot: EvictionConfig,
    ) -> Result<ReservationState<'a>, EvictionAdmissionError> {
        let (evicted, reservation) = reserve_memory_from_snapshot(
            self.keyspace,
            preferred_shard,
            additional_bytes,
            self.now_nanos,
            snapshot,
        )?;
        Ok(ReservationState {
            snapshot,
            reservation,
            evicted,
        })
    }

    #[inline]
    fn admission_revalidation_active(self, snapshot: EvictionConfig) -> bool {
        admission_revalidation_active(self.keyspace, snapshot)
    }

    #[inline]
    fn acquire_single_shard<F>(
        self,
        shard_index: usize,
        state: ReservationState<'a>,
        hook_label: &'static str,
        admission_active: bool,
        required_delta: F,
    ) -> Result<(ShardWriteGuard<'a>, ReservationState<'a>), MutationError>
    where
        F: FnMut(&SwissTable) -> Result<PositiveDelta, &'static [u8]>,
    {
        if !admission_active {
            return Ok((self.keyspace.write_shard_by_index(shard_index), state));
        }

        self.acquire_single_shard_revalidated(shard_index, state, hook_label, required_delta)
    }

    #[inline]
    fn acquire_single_shard_revalidated<F>(
        self,
        shard_index: usize,
        state: ReservationState<'a>,
        hook_label: &'static str,
        mut required_delta: F,
    ) -> Result<(ShardWriteGuard<'a>, ReservationState<'a>), MutationError>
    where
        F: FnMut(&SwissTable) -> Result<PositiveDelta, &'static [u8]>,
    {
        let ReservationState {
            snapshot,
            mut reservation,
            mut evicted,
        } = state;

        maybe_pause_after_projection(hook_label);

        loop {
            let guard = self.keyspace.write_shard_by_index(shard_index);
            let required = match required_delta(&guard) {
                Ok(required) => required,
                Err(response) => return Err(MutationError::with_evictions(response, evicted)),
            };

            if required.bytes() <= reservation.reserved_bytes() {
                return Ok((
                    guard,
                    ReservationState {
                        snapshot,
                        reservation,
                        evicted,
                    },
                ));
            }

            let extra = PositiveDelta::from_bytes(required.bytes() - reservation.reserved_bytes());
            drop(guard);

            let (additional_evicted, additional_reservation) = match reserve_memory_from_snapshot(
                self.keyspace,
                shard_index,
                extra,
                self.now_nanos,
                snapshot,
            ) {
                Ok(result) => result,
                Err(error) => {
                    merge_evicted_keys(&mut evicted, error.evicted);
                    return Err(MutationError::with_evictions(error.response, evicted));
                }
            };
            merge_evicted_keys(&mut evicted, additional_evicted);
            reservation.absorb(additional_reservation);
        }
    }

    #[inline]
    fn acquire_multi_write<F>(
        self,
        key_refs: &[&[u8]],
        preferred_shard: usize,
        state: ReservationState<'a>,
        hook_label: &'static str,
        admission_active: bool,
        required_delta: F,
    ) -> Result<(ShardWriteGuards<'a>, ShardPlan, ReservationState<'a>), MutationError>
    where
        F: FnMut(&mut ShardWriteGuards<'a>, &ShardPlan) -> Result<PositiveDelta, &'static [u8]>,
    {
        if !admission_active {
            let (guards, plan) = self.keyspace.multi_write(key_refs);
            return Ok((guards, plan, state));
        }

        self.acquire_multi_write_revalidated(
            key_refs,
            preferred_shard,
            state,
            hook_label,
            required_delta,
        )
    }

    #[inline]
    fn acquire_multi_write_revalidated<F>(
        self,
        key_refs: &[&[u8]],
        preferred_shard: usize,
        state: ReservationState<'a>,
        hook_label: &'static str,
        mut required_delta: F,
    ) -> Result<(ShardWriteGuards<'a>, ShardPlan, ReservationState<'a>), MutationError>
    where
        F: FnMut(&mut ShardWriteGuards<'a>, &ShardPlan) -> Result<PositiveDelta, &'static [u8]>,
    {
        let ReservationState {
            snapshot,
            mut reservation,
            mut evicted,
        } = state;

        maybe_pause_after_projection(hook_label);

        loop {
            let (mut guards, plan) = self.keyspace.multi_write(key_refs);
            let required = match required_delta(&mut guards, &plan) {
                Ok(required) => required,
                Err(response) => return Err(MutationError::with_evictions(response, evicted)),
            };

            if required.bytes() <= reservation.reserved_bytes() {
                return Ok((
                    guards,
                    plan,
                    ReservationState {
                        snapshot,
                        reservation,
                        evicted,
                    },
                ));
            }

            let extra = PositiveDelta::from_bytes(required.bytes() - reservation.reserved_bytes());
            drop((guards, plan));

            let (additional_evicted, additional_reservation) = match reserve_memory_from_snapshot(
                self.keyspace,
                preferred_shard,
                extra,
                self.now_nanos,
                snapshot,
            ) {
                Ok(result) => result,
                Err(error) => {
                    merge_evicted_keys(&mut evicted, error.evicted);
                    return Err(MutationError::with_evictions(error.response, evicted));
                }
            };
            merge_evicted_keys(&mut evicted, additional_evicted);
            reservation.absorb(additional_reservation);
        }
    }
}

#[cfg(test)]
struct ProjectionAdmissionTestHook {
    label: &'static str,
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static PROJECTION_ADMISSION_TEST_HOOK: std::sync::Mutex<Option<ProjectionAdmissionTestHook>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
static PROJECTION_ADMISSION_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
fn install_projection_admission_test_hook(
    label: &'static str,
) -> (
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::SyncSender<()>,
) {
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let mut slot = PROJECTION_ADMISSION_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(
        slot.is_none(),
        "projection admission test hook already installed"
    );
    *slot = Some(ProjectionAdmissionTestHook {
        label,
        entered: entered_tx,
        release: release_rx,
    });
    (entered_rx, release_tx)
}

#[cfg(test)]
fn maybe_pause_after_projection(label: &'static str) {
    let hook = {
        let mut slot = PROJECTION_ADMISSION_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match slot.as_ref() {
            Some(hook) if hook.label == label => slot.take(),
            _ => None,
        }
    };

    if let Some(hook) = hook {
        hook.entered
            .send(())
            .expect("projection admission test hook receiver must stay alive");
        hook.release
            .recv()
            .expect("projection admission test release sender must stay alive");
    }
}

#[cfg(not(test))]
#[inline(always)]
fn maybe_pause_after_projection(_label: &'static str) {}

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
) -> PositiveDelta {
    match existing_value {
        Some(value) => {
            positive_delta(new_value_memory_usage as isize - value.memory_usage() as isize)
        }
        None => PositiveDelta::from_bytes(entry_memory_usage(key, new_value_memory_usage)),
    }
}

fn projected_increment_delta(
    table: &SwissTable,
    key: &VortexKey,
    delta: i64,
    now_nanos: u64,
) -> Result<PositiveDelta, &'static [u8]> {
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
        Some(_) | None => Ok(PositiveDelta::from_bytes(entry_memory_usage(
            key,
            integer_value_memory_usage(),
        ))),
    }
}

fn projected_increment_by_float_delta(
    table: &SwissTable,
    key: &VortexKey,
    increment: f64,
    now_nanos: u64,
) -> Result<PositiveDelta, &'static [u8]> {
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
) -> Result<PositiveDelta, &'static [u8]> {
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
        Some(_) | None => Ok(PositiveDelta::from_bytes(entry_memory_usage(
            key,
            string_value_memory_usage(append_bytes.len()),
        ))),
    }
}

fn projected_setrange_delta(
    table: &SwissTable,
    key: &VortexKey,
    offset: usize,
    new_bytes: &[u8],
    now_nanos: u64,
) -> Result<PositiveDelta, &'static [u8]> {
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
        Some(_) | None => Ok(PositiveDelta::from_bytes(entry_memory_usage(
            key,
            string_value_memory_usage(required_len),
        ))),
    }
}

fn projected_copy_delta(
    source_value: Option<&VortexValue>,
    dst_table: &SwissTable,
    dst: &VortexKey,
    replace: bool,
    now_nanos: u64,
) -> PositiveDelta {
    let Some(source_value) = source_value else {
        return PositiveDelta::zero();
    };

    let existing = match dst_table.get_with_ttl(dst) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
        _ => None,
    };
    if !replace && existing.is_some() {
        return PositiveDelta::zero();
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
) -> Result<PositiveDelta, &'static [u8]> {
    if old_key == new_key {
        return Ok(PositiveDelta::zero());
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
        return Ok(PositiveDelta::zero());
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
) -> PositiveDelta {
    let exists = matches!(
        table.get_with_ttl(key),
        Some((_, ttl)) if ttl == 0 || ttl > now_nanos
    );

    if options.nx && exists {
        return PositiveDelta::zero();
    }
    if options.xx && !exists {
        return PositiveDelta::zero();
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

fn msetnx_all_absent_locked(
    keyspace: &ConcurrentKeyspace,
    guards: &mut ShardWriteGuards<'_>,
    plan: &ShardPlan,
    pairs: &[(VortexKey, VortexValue)],
    lookups: &[MultiWriteLookup],
    now_nanos: u64,
) -> bool {
    for lookup in lookups {
        let guard_pos = plan.guard_index_for_key(lookup.pair_index).get();
        let table = &mut *guards[guard_pos].1;
        let key_bytes = pairs[lookup.pair_index].0.as_bytes();
        let _ = keyspace.cleanup_expired_prehashed(
            lookup.shard_idx,
            table,
            key_bytes,
            lookup.table_hash,
            now_nanos,
        );
        if table.contains_key_prehashed(key_bytes, lookup.table_hash) {
            return false;
        }
    }
    true
}

#[inline]
fn distinct_guard_tables_mut<'g>(
    guards: &'g mut ShardWriteGuards<'_>,
    first_position: usize,
    second_position: usize,
) -> (&'g mut SwissTable, &'g mut SwissTable) {
    debug_assert_ne!(
        first_position, second_position,
        "same-shard operations must use the single-shard path"
    );

    if first_position < second_position {
        let (left, right) = guards.split_at_mut(second_position);
        (&mut *left[first_position].1, &mut *right[0].1)
    } else {
        let (left, right) = guards.split_at_mut(first_position);
        (&mut *right[0].1, &mut *left[second_position].1)
    }
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
) -> (Option<VortexValue>, ExpiryTransition) {
    let Some((value, ttl_deadline)) = table.remove_with_ttl(key) else {
        return (None, ExpiryTransition::default());
    };

    let had_ttl = ttl_deadline != 0;
    if had_ttl && ttl_deadline <= now_nanos {
        return (None, ExpiryTransition::ttl_removed());
    }

    (Some(value), ExpiryTransition::remove(had_ttl))
}

fn delete_live_key_bytes(
    table: &mut SwissTable,
    key_bytes: &[u8],
    hash: TableHash,
    now_nanos: u64,
) -> (bool, ExpiryTransition) {
    let Some((value, ttl_deadline)) = table.remove_with_ttl_prehashed(key_bytes, hash) else {
        return (false, ExpiryTransition::default());
    };
    drop(value);

    let had_ttl = ttl_deadline != 0;
    if had_ttl && ttl_deadline <= now_nanos {
        return (false, ExpiryTransition::ttl_removed());
    }

    (true, ExpiryTransition::remove(had_ttl))
}

fn set_with_options_on_table(
    table: &mut SwissTable,
    key: VortexKey,
    value: VortexValue,
    options: SetOptions,
    now_nanos: u64,
) -> (SetResult, ExpiryTransition) {
    let had_ttl = ttl_present(table.get_entry_ttl(&key));
    let _ = remove_if_expired(table, &key, now_nanos);

    // Plain SET is the hot path in the benchmark workload. It does not need
    // an existence probe, previous value clone, or TTL carry-forward.
    if !options.nx && !options.xx && !options.get && !options.keepttl {
        if options.ttl_deadline != 0 {
            table.insert_with(key, value, options.ttl_deadline, None);
        } else {
            table.insert(key, value);
        }
        return (
            SetResult::Ok,
            ExpiryTransition::new(had_ttl, options.ttl_deadline != 0),
        );
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
                ExpiryTransition::new(had_ttl, existing_ttl != 0),
            )
        } else {
            (
                SetResult::NotSet,
                ExpiryTransition::new(had_ttl, existing_ttl != 0),
            )
        };
    }

    if options.xx && !exists {
        return if options.get {
            (
                SetResult::NotSetGet(None),
                ExpiryTransition::remove(had_ttl),
            )
        } else {
            (SetResult::NotSet, ExpiryTransition::remove(had_ttl))
        };
    }

    let effective_ttl = if options.keepttl {
        existing_ttl
    } else {
        options.ttl_deadline
    };

    let previous = if effective_ttl != 0 {
        table.insert_with(key, value, effective_ttl, None)
    } else {
        table.insert(key, value)
    };

    if options.get {
        (
            SetResult::OkGet(previous),
            ExpiryTransition::new(had_ttl, effective_ttl != 0),
        )
    } else {
        (
            SetResult::Ok,
            ExpiryTransition::new(had_ttl, effective_ttl != 0),
        )
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
                let hash = table.table_hash_key_bytes(key.as_bytes());
                let _ = table.mutate_prehashed(
                    key,
                    VortexValue::Integer(delta),
                    hash,
                    MutationPolicy::clear(Some(lsn)),
                );
                return Ok((delta, ExpiryTransition::ttl_removed()));
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
            let hash = table.table_hash_key_bytes(key.as_bytes());
            table
                .replace_prehashed(
                    key.as_bytes(),
                    VortexValue::Integer(result),
                    hash,
                    MutationPolicy::preserve_ttl(Some(lsn)),
                )
                .expect("live key must exist while replacing increment result");
            Ok((result, ExpiryTransition::new(had_ttl, had_ttl)))
        }
        None => {
            let hash = table.table_hash_key_bytes(key.as_bytes());
            let _ = table.mutate_prehashed(
                key,
                VortexValue::Integer(delta),
                hash,
                MutationPolicy::clear(Some(lsn)),
            );
            Ok((delta, ExpiryTransition::default()))
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
    let _ = table.insert_with_lsn(key, VortexValue::from_bytes(bytes.as_ref()), Some(lsn));
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

    let hash = table.table_hash_key_bytes(key.as_bytes());
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
        .replace_prehashed(
            key.as_bytes(),
            new_value,
            hash,
            MutationPolicy::preserve_ttl(Some(lsn)),
        )
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
    let _ = table.insert_with_lsn(key, VortexValue::from_bytes(&data), Some(lsn));
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
        table.insert_with(new_key, value, ttl, None);
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
        table.insert_with(dst, value_clone, ttl, None);
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
    #[inline(always)]
    fn commit_effects(&self, effects: MutationEffects<'_>) -> AofEffect {
        if let Some(ttl) = effects.ttl {
            self.apply_expiry_transition(ttl.shard_index, ttl.transition);
        }
        if let Some(hash) = effects.frequency {
            self.record_frequency_hash(hash);
        }
        match effects.watch {
            WatchEffect::None => {}
            WatchEffect::Key(key) => self.bump_watch_key(key),
            WatchEffect::KeyBytes(key_bytes) => self.bump_watch_key_bytes(key_bytes),
        }
        effects.aof
    }

    /// Read a string value and perform lazy expiry cleanup for GET-style commands.
    ///
    /// The caller-provided encoder runs while the read guard is held so command
    /// code can build a borrowed response without cloning `VortexValue`.
    #[inline]
    pub(crate) fn read_value_with<R, F>(&self, key_bytes: &[u8], now_nanos: u64, encode: F) -> R
    where
        F: FnOnce(Option<&VortexValue>) -> R,
    {
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl_prehashed(key_bytes, table_hash) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                self.record_access_prehashed(&guard, key_bytes, table_hash);
                encode(Some(value))
            }
            Some(_) => {
                drop(guard);
                let key = VortexKey::from_bytes(key_bytes);
                let mut wguard = self.write_shard_by_index(shard_index);
                let had_ttl = matches!(wguard.get_entry_ttl(&key), Some(ttl) if ttl != 0);
                if remove_if_expired(&mut wguard, &key, now_nanos) {
                    self.commit_effects(
                        MutationEffects::none()
                            .with_ttl(shard_index, ExpiryTransition::remove(had_ttl))
                            .with_watch_key(&key),
                    );
                }
                encode(None)
            }
            None => encode(None),
        }
    }

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
            self.commit_effects(
                MutationEffects::none()
                    .with_ttl(shard_index, ExpiryTransition::remove(had_ttl))
                    .with_watch_key(key),
            );
        }
        removed
    }

    #[inline]
    fn cleanup_expired_prehashed(
        &self,
        shard_index: usize,
        table: &mut SwissTable,
        key_bytes: &[u8],
        hash: TableHash,
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
        self.commit_effects(
            MutationEffects::none()
                .with_ttl(shard_index, ExpiryTransition::new(true, has_ttl))
                .with_watch_key_bytes(key_bytes),
        );
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

    pub(crate) fn get_value_with_expiry_option(
        &self,
        key: &VortexKey,
        option: GetExOption,
        now_nanos: u64,
    ) -> MutationOutcome<Option<VortexValue>> {
        if option == GetExOption::None {
            return MutationOutcome::new(self.get_value(key, now_nanos), None);
        }

        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let mut guard = self.write_shard_by_index(shard_index);
        let Some((value, ttl_deadline)) = guard.get_with_ttl(key) else {
            return MutationOutcome::new(None, None);
        };
        let had_ttl = ttl_deadline != 0;
        if had_ttl && ttl_deadline <= now_nanos {
            let removed = remove_if_expired(&mut guard, key, now_nanos);
            debug_assert!(removed, "expired GETEX key must be removable");
            self.commit_effects(
                MutationEffects::none()
                    .with_ttl(shard_index, ExpiryTransition::ttl_removed())
                    .with_watch_key(key),
            );
            return MutationOutcome::new(None, None);
        }

        let value = value.clone();
        self.record_access_prehashed(&guard, key_bytes, table_hash);

        let (changed, transition) = match option {
            GetExOption::None => unreachable!("GETEX none exits through get_value"),
            GetExOption::ExpireAt(deadline) if deadline <= now_nanos => {
                let removed = guard.remove(key).is_some();
                debug_assert!(removed, "live GETEX key must be removable");
                (removed, ExpiryTransition::remove(had_ttl))
            }
            GetExOption::ExpireAt(deadline) => {
                let updated = guard.set_entry_ttl(key, deadline);
                (updated, ExpiryTransition::new(had_ttl, updated))
            }
            GetExOption::Persist => {
                let updated = guard.clear_entry_ttl(key);
                (updated, ExpiryTransition::remove(had_ttl))
            }
        };

        let aof_lsn = if changed {
            match option {
                GetExOption::ExpireAt(deadline) if deadline <= now_nanos => self.next_aof_lsn(),
                _ => {
                    let (lsn, aof_lsn) = self.allocate_mutation_lsn();
                    stamp_entry_lsn(&mut guard, key_bytes, table_hash, lsn);
                    aof_lsn
                }
            }
        } else {
            None
        };
        let aof_lsn = self
            .commit_effects(
                MutationEffects::none()
                    .with_ttl(shard_index, transition)
                    .with_watch_key_if(changed, key)
                    .with_aof_lsn(aof_lsn),
            )
            .into_lsn();
        MutationOutcome::new(Some(value), aof_lsn)
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
        let eviction = self.eviction_config();
        let projected_delta = self
            .read_shard_by_index(shard_index)
            .projected_insert_delta(&key, &value);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(shard_index, positive_delta(projected_delta), eviction)?;
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "set_value_with_ttl",
            coordinator.admission_revalidation_active(eviction),
            |table| Ok(positive_delta(table.projected_insert_delta(&key, &value))),
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let had_ttl = ttl_present(guard.get_entry_ttl(&key));
        let watched_key = self.watch_tracking_active().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        let previous = guard.insert_with(key, value, ttl_deadline_nanos, Some(lsn));
        self.commit_effects(
            MutationEffects::none()
                .with_ttl(shard_index, ExpiryTransition::new(had_ttl, true))
                .with_frequency(table_hash)
                .with_optional_watch_key(watched_key.as_ref()),
        );
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
        if !features.is_empty() {
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
            guard
                .mutate_prehashed(
                    BorrowedKey(key_bytes),
                    value,
                    table_hash,
                    MutationPolicy::clear(Some(lsn)),
                )
                .had_ttl()
        } else {
            guard
                .mutate_prehashed(
                    BorrowedKey(key_bytes),
                    RawValueBytes(value_bytes),
                    table_hash,
                    MutationPolicy::clear(Some(lsn)),
                )
                .had_ttl()
        };
        self.commit_effects(
            MutationEffects::none().with_ttl(shard_index, ExpiryTransition::remove(old_had_ttl)),
        );
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
        let eviction = self.eviction_config();
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = if features.maxmemory() {
            let projected_delta = self
                .read_shard_by_index(shard_index)
                .projected_insert_delta_prehashed(&key, &value, table_hash);
            coordinator.reserve(shard_index, positive_delta(projected_delta), eviction)?
        } else {
            ReservationState {
                snapshot: eviction,
                reservation: MemoryReservation::new(self, 0),
                evicted: None,
            }
        };
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "set_value_plain",
            features.maxmemory() && coordinator.admission_revalidation_active(eviction),
            |table| {
                Ok(positive_delta(table.projected_insert_delta_prehashed(
                    &key, &value, table_hash,
                )))
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let watched_key = features.watch().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn_with_features(features);
        let old_had_ttl = guard
            .mutate_prehashed(key, value, table_hash, MutationPolicy::clear(Some(lsn)))
            .had_ttl();
        let mut effects = MutationEffects::none()
            .with_ttl(shard_index, ExpiryTransition::remove(old_had_ttl))
            .with_aof_lsn(aof_lsn)
            .with_optional_watch_key(watched_key.as_ref());
        if features.maxmemory() {
            effects = effects.with_frequency(table_hash);
        }
        let aof_lsn = self.commit_effects(effects).into_lsn();
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
        let eviction = self.eviction_config();
        let projected_delta = projected_set_write_delta(
            &self.read_shard_by_index(shard_index),
            &key,
            &value,
            options,
            now_nanos,
        );
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(shard_index, projected_delta, eviction)?;
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "set_value_with_options",
            coordinator.admission_revalidation_active(eviction),
            |table| {
                Ok(projected_set_write_delta(
                    table, &key, &value, options, now_nanos,
                ))
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let (result, transition) =
            set_with_options_on_table(&mut guard, key.clone(), value, options, now_nanos);
        let changed = matches!(&result, SetResult::Ok | SetResult::OkGet(_));
        let table_hash = self.table_hash_key(key.as_bytes());
        let aof_lsn = if changed {
            let key_bytes = key.as_bytes();
            let (lsn, aof_lsn) = self.allocate_mutation_lsn();
            stamp_entry_lsn(&mut guard, key_bytes, table_hash, lsn);
            aof_lsn
        } else {
            None
        };
        let mut effects = MutationEffects::none()
            .with_ttl(shard_index, transition)
            .with_aof_lsn(aof_lsn);
        if changed {
            effects = effects.with_frequency(table_hash).with_watch_key(&key);
        }
        let aof_lsn = self.commit_effects(effects).into_lsn();
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(result, aof_lsn, evicted))
    }

    pub(crate) fn mget_values_with<R, F, N>(
        &self,
        keys: &[&[u8]],
        now_nanos: u64,
        mut encode: F,
        mut nil: N,
    ) -> Vec<R>
    where
        F: FnMut(&VortexValue) -> R,
        N: FnMut() -> R,
    {
        if keys.is_empty() {
            return Vec::new();
        }

        #[derive(Clone, Copy)]
        struct KeyLookup {
            output_idx: usize,
            shard_idx: usize,
            hash: TableHash,
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

        let mut values = Vec::with_capacity(keys.len());
        values.resize_with(keys.len(), || None);
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
                guard.prefetch_group(lookup.hash.get());
            }

            for lookup in &lookups[group_start..cursor] {
                let key_bytes = keys[lookup.output_idx];
                match guard.get_with_ttl_prehashed(key_bytes, lookup.hash) {
                    Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                        self.record_access_prehashed(&guard, key_bytes, lookup.hash);
                        values[lookup.output_idx] = Some(encode(value));
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

        values
            .into_iter()
            .map(|value| match value {
                Some(value) => value,
                None => nil(),
            })
            .collect()
    }

    pub(crate) fn mset_values(
        &self,
        pairs: Vec<(VortexKey, VortexValue)>,
        now_nanos: u64,
    ) -> MutationResult<()> {
        if pairs.is_empty() {
            return Ok(MutationOutcome::new((), None));
        }

        let eviction = self.eviction_config();
        let pairs = if admission_revalidation_active(self, eviction) {
            deduplicate_last_write_pairs(pairs)
        } else {
            pairs
        };
        let preferred_shard = self.shard_index(pairs[0].0.as_bytes());
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let projected_delta = PositiveDelta::sum(pairs.iter().map(|(key, value)| {
            let shard_index = self.shard_index(key.as_bytes());
            let guard = self.read_shard_by_index(shard_index);
            positive_delta(guard.projected_insert_delta(key, value))
        }));
        let state = coordinator.reserve(preferred_shard, projected_delta, eviction)?;

        let (key_refs, lookups) = build_multi_write_lookups(self, &pairs);
        let (mut guards, plan, state) = coordinator.acquire_multi_write(
            &key_refs,
            preferred_shard,
            state,
            "mset_values",
            coordinator.admission_revalidation_active(eviction),
            |guards, plan| {
                Ok(PositiveDelta::sum(lookups.iter().map(|lookup| {
                    let guard_pos = plan.guard_index_for_key(lookup.pair_index).get();
                    let table = &*guards[guard_pos].1;
                    let (key, value) = &pairs[lookup.pair_index];
                    positive_delta(table.projected_insert_delta_prehashed(
                        key,
                        value,
                        lookup.table_hash,
                    ))
                })))
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        drop(key_refs);
        let mut pairs = pairs.into_iter().map(Some).collect::<Vec<_>>();
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();

        for lookup in lookups {
            let guard_pos = plan.guard_index_for_key(lookup.pair_index).get();
            let table = &mut *guards[guard_pos].1;
            let (key, value) = pairs[lookup.pair_index]
                .take()
                .expect("mset pair must be available exactly once");
            let watched_key = self.watch_tracking_active().then(|| key.clone());
            let old_had_ttl = table
                .mutate_prehashed(
                    key,
                    value,
                    lookup.table_hash,
                    MutationPolicy::clear(Some(lsn)),
                )
                .had_ttl();
            self.commit_effects(
                MutationEffects::none()
                    .with_ttl(lookup.shard_idx, ExpiryTransition::remove(old_had_ttl))
                    .with_frequency(lookup.table_hash)
                    .with_optional_watch_key(watched_key.as_ref()),
            );
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
        let pairs = deduplicate_last_write_pairs(pairs);
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

        let preferred_shard = self.shard_index(pairs[0].0.as_bytes());
        let eviction = self.eviction_config();
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let projected_delta = PositiveDelta::sum(pairs.iter().map(|(key, value)| {
            let shard_index = self.shard_index(key.as_bytes());
            let guard = self.read_shard_by_index(shard_index);
            positive_delta(guard.projected_insert_delta(key, value))
        }));
        let state = coordinator.reserve(preferred_shard, projected_delta, eviction)?;

        let (key_refs, lookups) = build_multi_write_lookups(self, &pairs);
        let (mut guards, plan, state) = coordinator.acquire_multi_write(
            &key_refs,
            preferred_shard,
            state,
            "msetnx_values",
            coordinator.admission_revalidation_active(eviction),
            |guards, plan| {
                if !msetnx_all_absent_locked(self, guards, plan, &pairs, &lookups, now_nanos) {
                    return Ok(PositiveDelta::zero());
                }
                Ok(PositiveDelta::sum(lookups.iter().map(|lookup| {
                    let guard_pos = plan.guard_index_for_key(lookup.pair_index).get();
                    let table = &*guards[guard_pos].1;
                    let (key, value) = &pairs[lookup.pair_index];
                    positive_delta(table.projected_insert_delta_prehashed(
                        key,
                        value,
                        lookup.table_hash,
                    ))
                })))
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        if !msetnx_all_absent_locked(self, &mut guards, &plan, &pairs, &lookups, now_nanos) {
            drop(guards);
            reservation.settle();
            return Ok(mutation_outcome_with_evictions(false, None, evicted));
        }
        drop(key_refs);

        let mut pairs = pairs.into_iter().map(Some).collect::<Vec<_>>();
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        for lookup in lookups {
            let guard_pos = plan.guard_index_for_key(lookup.pair_index).get();
            let table = &mut *guards[guard_pos].1;
            let (key, value) = pairs[lookup.pair_index]
                .take()
                .expect("msetnx pair must be available exactly once");
            let watched_key = self.watch_tracking_active().then(|| key.clone());
            table.insert_new_prehashed_and_lsn(key, value, lookup.table_hash, lsn);
            self.commit_effects(
                MutationEffects::none()
                    .with_frequency(lookup.table_hash)
                    .with_optional_watch_key(watched_key.as_ref()),
            );
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
        let (removed, transition) = take_live_value(&mut guard, key, now_nanos);
        let changed = removed.is_some();
        let aof_lsn = changed.then(|| self.next_aof_lsn()).flatten();
        let aof_lsn = self
            .commit_effects(
                MutationEffects::none()
                    .with_ttl(shard_index, transition)
                    .with_watch_key_if(changed, key)
                    .with_aof_lsn(aof_lsn),
            )
            .into_lsn();
        MutationOutcome::new(removed, aof_lsn)
    }

    pub(crate) fn delete_key_bytes(
        &self,
        key_bytes: &[u8],
        now_nanos: u64,
    ) -> MutationOutcome<bool> {
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let mut guard = self.write_shard_by_index(shard_index);
        let (deleted, transition) =
            delete_live_key_bytes(&mut guard, key_bytes, table_hash, now_nanos);
        let aof_lsn = deleted.then(|| self.next_aof_lsn()).flatten();
        let mut effects = MutationEffects::none()
            .with_ttl(shard_index, transition)
            .with_aof_lsn(aof_lsn);
        if deleted {
            effects = effects.with_watch_key_bytes(key_bytes);
        }
        let aof_lsn = self.commit_effects(effects).into_lsn();
        MutationOutcome::new(deleted, aof_lsn)
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
        let eviction = self.eviction_config();
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta = projected_increment_delta(&read_guard, &key, delta, now_nanos)?;
        drop(read_guard);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(shard_index, projected_delta, eviction)?;
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "increment_by",
            coordinator.admission_revalidation_active(eviction),
            |table| projected_increment_delta(table, &key, delta, now_nanos),
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let watched_key = self.watch_tracking_active().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        let (result, transition) = match increment_table_by(&mut guard, key, delta, lsn, now_nanos)
        {
            Ok(result) => result,
            Err(err) => return Err(MutationError::with_evictions(err, evicted)),
        };
        self.commit_effects(
            MutationEffects::none()
                .with_ttl(shard_index, transition)
                .with_frequency(table_hash)
                .with_optional_watch_key(watched_key.as_ref()),
        );
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(result, aof_lsn, evicted))
    }

    pub(crate) fn increment_by_float(
        &self,
        key: VortexKey,
        increment: f64,
        now_nanos: u64,
    ) -> MutationResult<FloatIncrementResult> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let eviction = self.eviction_config();
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta =
            projected_increment_by_float_delta(&read_guard, &key, increment, now_nanos)?;
        drop(read_guard);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(shard_index, projected_delta, eviction)?;
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "increment_by_float",
            coordinator.admission_revalidation_active(eviction),
            |table| projected_increment_by_float_delta(table, &key, increment, now_nanos),
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let ttl_deadline = guard.get_entry_ttl(&key);
        let watched_key = self.watch_tracking_active().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        let result = match increment_table_by_float(&mut guard, key, increment, lsn, now_nanos) {
            Ok(result) => result,
            Err(err) => return Err(MutationError::with_evictions(err, evicted)),
        };
        let ttl_after = match ttl_deadline {
            Some(deadline) if deadline > now_nanos => TtlState::Deadline(deadline),
            _ => TtlState::Persistent,
        };
        let mut effects = MutationEffects::none()
            .with_ttl(
                shard_index,
                ExpiryTransition::new(
                    ttl_present(ttl_deadline),
                    matches!(ttl_after, TtlState::Deadline(_)),
                ),
            )
            .with_frequency(table_hash);
        if let Some(key) = watched_key.as_ref() {
            effects = effects.with_watch_key(key);
        }
        self.commit_effects(effects);
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(
            FloatIncrementResult {
                value: result,
                ttl_after,
            },
            aof_lsn,
            evicted,
        ))
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
        let eviction = self.eviction_config();
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta = projected_append_delta(&read_guard, &key, append_bytes, now_nanos)?;
        drop(read_guard);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(shard_index, projected_delta, eviction)?;
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "append_value",
            coordinator.admission_revalidation_active(eviction),
            |table| projected_append_delta(table, &key, append_bytes, now_nanos),
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let ttl_deadline = guard.get_entry_ttl(&key);
        let watched_key = self.watch_tracking_active().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        let length = match append_to_table(&mut guard, key, append_bytes, lsn, now_nanos) {
            Ok(length) => length,
            Err(err) => return Err(MutationError::with_evictions(err, evicted)),
        };
        self.commit_effects(
            MutationEffects::none()
                .with_ttl(
                    shard_index,
                    ExpiryTransition::new(
                        ttl_present(ttl_deadline),
                        matches!(ttl_deadline, Some(deadline) if deadline > now_nanos),
                    ),
                )
                .with_frequency(table_hash)
                .with_optional_watch_key(watched_key.as_ref()),
        );
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
        let eviction = self.eviction_config();
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta =
            projected_setrange_delta(&read_guard, &key, offset, new_bytes, now_nanos)?;
        drop(read_guard);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(shard_index, projected_delta, eviction)?;
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "setrange_value",
            coordinator.admission_revalidation_active(eviction),
            |table| projected_setrange_delta(table, &key, offset, new_bytes, now_nanos),
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let had_ttl = ttl_present(guard.get_entry_ttl(&key));
        let ttl_probe_key = key.clone();
        let watched_key = self.watch_tracking_active().then(|| key.clone());
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        let length = match setrange_in_table(&mut guard, key, offset, new_bytes, lsn, now_nanos) {
            Ok(length) => length,
            Err(err) => return Err(MutationError::with_evictions(err, evicted)),
        };
        let has_ttl_after = ttl_present(guard.get_entry_ttl(&ttl_probe_key));
        self.commit_effects(
            MutationEffects::none()
                .with_ttl(shard_index, ExpiryTransition::new(had_ttl, has_ttl_after))
                .with_frequency(table_hash)
                .with_optional_watch_key(watched_key.as_ref()),
        );
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

        let key_refs: SmallVec<[&[u8]; 16]> = keys.iter().map(VortexKey::as_bytes).collect();
        let (mut guards, plan) = self.multi_write(&key_refs);
        let mut deleted = 0i64;

        for (idx, key) in keys.iter().enumerate() {
            let shard_index = plan.shard_for_key(idx).get();
            let position = plan.guard_index_for_key(idx).get();
            let table = &mut *guards[position].1;
            let (removed, transition) = take_live_value(table, key, now_nanos);
            let changed = removed.is_some();
            if removed.is_some() {
                deleted += 1;
            }
            self.commit_effects(
                MutationEffects::none()
                    .with_ttl(shard_index, transition)
                    .with_watch_key_if(changed, key),
            );
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

        let key_refs: SmallVec<[&[u8]; 16]> = keys.iter().map(VortexKey::as_bytes).collect();
        let (guards, plan) = self.multi_read(&key_refs);
        let mut count = 0i64;
        let mut expired_indices: Vec<usize> = Vec::new();

        for (idx, key) in keys.iter().enumerate() {
            let position = plan.guard_index_for_key(idx).get();
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
                let shard_index = plan.shard_for_key(idx).get();
                let mut wguard = self.write_shard_by_index(shard_index);
                self.cleanup_expired_key(shard_index, &mut wguard, &keys[idx], now_nanos);
            }
        }

        count
    }

    pub(crate) fn expire_key_with_options(
        &self,
        key: &VortexKey,
        deadline_nanos: u64,
        now_nanos: u64,
        options: ExpireOptions,
    ) -> MutationOutcome<bool> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);

        let _ = self.cleanup_expired_key(shard_index, &mut guard, key, now_nanos);
        let current_ttl = match guard.get_entry_ttl(key) {
            Some(deadline) => deadline,
            None => return MutationOutcome::new(false, None),
        };
        if !options.permits(current_ttl, deadline_nanos) {
            return MutationOutcome::new(false, None);
        }

        if deadline_nanos <= now_nanos {
            let (removed, transition) = take_live_value(&mut guard, key, now_nanos);
            let changed = removed.is_some();
            let aof_lsn = changed.then(|| self.next_aof_lsn()).flatten();
            let aof_lsn = self
                .commit_effects(
                    MutationEffects::none()
                        .with_ttl(shard_index, transition)
                        .with_watch_key_if(changed, key)
                        .with_aof_lsn(aof_lsn),
                )
                .into_lsn();
            return MutationOutcome::new(changed, aof_lsn);
        }

        let had_ttl = current_ttl != 0;
        let key_bytes = key.as_bytes();
        let table_hash = self.table_hash_key(key_bytes);
        let updated = guard.set_entry_ttl(key, deadline_nanos);
        let aof_lsn = if updated {
            let (lsn, aof_lsn) = self.allocate_mutation_lsn();
            stamp_entry_lsn(&mut guard, key_bytes, table_hash, lsn);
            aof_lsn
        } else {
            None
        };
        let aof_lsn = self
            .commit_effects(
                MutationEffects::none()
                    .with_ttl(shard_index, ExpiryTransition::new(had_ttl, updated))
                    .with_watch_key_if(updated, key)
                    .with_aof_lsn(aof_lsn),
            )
            .into_lsn();
        MutationOutcome::new(updated, aof_lsn)
    }

    pub(crate) fn persist_key(&self, key: &VortexKey, now_nanos: u64) -> MutationOutcome<bool> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);

        let Some(ttl_deadline) = guard.get_entry_ttl(key) else {
            return MutationOutcome::new(false, None);
        };
        if ttl_deadline != 0 && ttl_deadline <= now_nanos {
            let removed = remove_if_expired(&mut guard, key, now_nanos);
            debug_assert!(removed, "expired PERSIST key must be removable");
            self.commit_effects(
                MutationEffects::none()
                    .with_ttl(shard_index, ExpiryTransition::ttl_removed())
                    .with_watch_key(key),
            );
            return MutationOutcome::new(false, None);
        }

        let had_ttl = ttl_deadline != 0;
        if !had_ttl {
            return MutationOutcome::new(false, None);
        }

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
        let aof_lsn = self
            .commit_effects(
                MutationEffects::none()
                    .with_ttl(shard_index, ExpiryTransition::remove(had_ttl))
                    .with_watch_key_if(updated, key)
                    .with_aof_lsn(aof_lsn),
            )
            .into_lsn();
        MutationOutcome::new(updated, aof_lsn)
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
            let eviction = self.eviction_config();
            let read_guard = self.read_shard_by_index(source_shard);
            let projected_delta = projected_rename_delta(
                &read_guard,
                old_key,
                &read_guard,
                &destination,
                now_nanos,
                nx,
            )?;
            drop(read_guard);
            let coordinator = ReservationCoordinator::new(self, now_nanos);
            let state = coordinator.reserve(destination_shard, projected_delta, eviction)?;
            let (mut guard, state) = coordinator.acquire_single_shard(
                source_shard,
                state,
                "rename_key",
                coordinator.admission_revalidation_active(eviction),
                |table| projected_rename_delta(table, old_key, table, &destination, now_nanos, nx),
            )?;
            let ReservationState {
                reservation,
                evicted,
                ..
            } = state;
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
            self.commit_effects(MutationEffects::none().with_ttl(
                source_shard,
                ExpiryTransition::new(old_had_ttl, old_has_ttl),
            ));
            if !same_key {
                self.commit_effects(MutationEffects::none().with_ttl(
                    source_shard,
                    ExpiryTransition::new(new_had_ttl, new_has_ttl),
                ));
            }
            let renamed = match renamed {
                Ok(renamed) => renamed,
                Err(response) => return Err(MutationError::with_evictions(response, evicted)),
            };
            let changed = renamed && !same_key;
            let aof_lsn = if changed {
                let (lsn, aof_lsn) = self.allocate_mutation_lsn();
                stamp_entry_lsn(&mut guard, destination.as_bytes(), new_hash, lsn);
                aof_lsn
            } else {
                None
            };
            if changed {
                self.commit_effects(
                    MutationEffects::none()
                        .with_frequency(new_hash)
                        .with_watch_key(old_key)
                        .with_aof_lsn(aof_lsn),
                );
                self.commit_effects(MutationEffects::none().with_watch_key(&destination));
            }
            drop(guard);
            reservation.settle();
            return Ok(mutation_outcome_with_evictions(renamed, aof_lsn, evicted));
        }

        let key_refs = [old_key.as_bytes(), new_key.as_bytes()];
        let eviction = self.eviction_config();
        let (read_guards, read_plan) = self.multi_read(&key_refs);
        let read_src_position = read_plan.guard_index_for_key(0).get();
        let read_dst_position = read_plan.guard_index_for_key(1).get();
        let projected_delta = projected_rename_delta(
            &read_guards[read_src_position].1,
            old_key,
            &read_guards[read_dst_position].1,
            &new_key,
            now_nanos,
            nx,
        )?;
        drop(read_guards);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(destination_shard, projected_delta, eviction)?;
        let (mut guards, plan, state) = coordinator.acquire_multi_write(
            &key_refs,
            destination_shard,
            state,
            "rename_key",
            coordinator.admission_revalidation_active(eviction),
            |guards, plan| {
                let src_position = plan.guard_index_for_key(0).get();
                let dst_position = plan.guard_index_for_key(1).get();
                let (src_table, dst_table) =
                    distinct_guard_tables_mut(guards, src_position, dst_position);

                let _ = self.cleanup_expired_key(source_shard, src_table, old_key, now_nanos);
                let _ = self.cleanup_expired_key(destination_shard, dst_table, &new_key, now_nanos);
                projected_rename_delta(src_table, old_key, dst_table, &new_key, now_nanos, nx)
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let src_position = plan.guard_index_for_key(0).get();
        let dst_position = plan.guard_index_for_key(1).get();

        let (src_table, dst_table) =
            distinct_guard_tables_mut(&mut guards, src_position, dst_position);

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
            dst_table.insert_with(new_key, value, ttl, None);
        } else {
            dst_table.insert(new_key, value);
        }
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        stamp_entry_lsn(dst_table, destination_key.as_bytes(), new_hash, lsn);
        self.commit_effects(
            MutationEffects::none().with_ttl(source_shard, ExpiryTransition::remove(old_had_ttl)),
        );
        self.commit_effects(MutationEffects::none().with_ttl(
            destination_shard,
            ExpiryTransition::new(new_had_ttl, ttl != 0 && ttl > now_nanos),
        ));
        self.commit_effects(
            MutationEffects::none()
                .with_frequency(new_hash)
                .with_watch_key(old_key)
                .with_aof_lsn(aof_lsn),
        );
        if let Some(key) = watched_new_key {
            self.commit_effects(MutationEffects::none().with_watch_key(&key));
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
            let eviction = self.eviction_config();
            let source_value = match read_guard.get_with_ttl(src) {
                Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
                _ => None,
            };
            let projected_delta =
                projected_copy_delta(source_value, &read_guard, &dst, replace, now_nanos);
            drop(read_guard);
            let destination = dst.clone();
            let coordinator = ReservationCoordinator::new(self, now_nanos);
            let state = coordinator.reserve(destination_shard, projected_delta, eviction)?;
            let (mut guard, state) = coordinator.acquire_single_shard(
                source_shard,
                state,
                "copy_key",
                coordinator.admission_revalidation_active(eviction),
                |table| {
                    let source_value = match table.get_with_ttl(src) {
                        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
                        _ => None,
                    };
                    Ok(projected_copy_delta(
                        source_value,
                        table,
                        &dst,
                        replace,
                        now_nanos,
                    ))
                },
            )?;
            let ReservationState {
                reservation,
                evicted,
                ..
            } = state;
            let _ = self.cleanup_expired_key(source_shard, &mut guard, src, now_nanos);
            if src != &destination {
                let _ = self.cleanup_expired_key(source_shard, &mut guard, &destination, now_nanos);
            }
            let dst_had_ttl = ttl_present(guard.get_entry_ttl(&destination));
            let copied = copy_within_table(&mut guard, src, dst, replace, now_nanos);
            let dst_has_ttl = ttl_present(guard.get_entry_ttl(&destination));
            self.commit_effects(MutationEffects::none().with_ttl(
                source_shard,
                ExpiryTransition::new(dst_had_ttl, dst_has_ttl),
            ));
            let aof_lsn = if copied {
                let (lsn, aof_lsn) = self.allocate_mutation_lsn();
                stamp_entry_lsn(&mut guard, destination.as_bytes(), dst_hash, lsn);
                aof_lsn
            } else {
                None
            };
            if copied {
                self.commit_effects(
                    MutationEffects::none()
                        .with_frequency(dst_hash)
                        .with_watch_key(&destination)
                        .with_aof_lsn(aof_lsn),
                );
            }
            drop(guard);
            reservation.settle();
            return Ok(mutation_outcome_with_evictions(copied, aof_lsn, evicted));
        }

        let key_refs = [src.as_bytes(), dst.as_bytes()];
        let eviction = self.eviction_config();
        let (read_guards, read_plan) = self.multi_read(&key_refs);
        let read_src_position = read_plan.guard_index_for_key(0).get();
        let read_dst_position = read_plan.guard_index_for_key(1).get();
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
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(destination_shard, projected_delta, eviction)?;
        let (mut guards, plan, state) = coordinator.acquire_multi_write(
            &key_refs,
            destination_shard,
            state,
            "copy_key",
            coordinator.admission_revalidation_active(eviction),
            |guards, plan| {
                let src_position = plan.guard_index_for_key(0).get();
                let dst_position = plan.guard_index_for_key(1).get();
                let (src_table, dst_table) =
                    distinct_guard_tables_mut(guards, src_position, dst_position);

                let _ = self.cleanup_expired_key(source_shard, src_table, src, now_nanos);
                let _ = self.cleanup_expired_key(destination_shard, dst_table, &dst, now_nanos);
                let source_value = match src_table.get_with_ttl(src) {
                    Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
                    _ => None,
                };
                Ok(projected_copy_delta(
                    source_value,
                    dst_table,
                    &dst,
                    replace,
                    now_nanos,
                ))
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let src_position = plan.guard_index_for_key(0).get();
        let dst_position = plan.guard_index_for_key(1).get();

        let (src_table, dst_table) =
            distinct_guard_tables_mut(&mut guards, src_position, dst_position);

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
            dst_table.insert_with(dst, value_clone, ttl, None);
        } else {
            dst_table.insert(dst, value_clone);
        }
        let (lsn, aof_lsn) = self.allocate_mutation_lsn();
        stamp_entry_lsn(dst_table, destination.as_bytes(), dst_hash, lsn);
        let mut effects = MutationEffects::none()
            .with_ttl(
                destination_shard,
                ExpiryTransition::new(dst_had_ttl, ttl != 0 && ttl > now_nanos),
            )
            .with_frequency(dst_hash)
            .with_aof_lsn(aof_lsn);
        if let Some(key) = watched_dst.as_ref() {
            effects = effects.with_watch_key(key);
        }
        self.commit_effects(effects);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::EvictionPolicy;

    const TEST_SHARDS: usize = 64;

    fn fixed_key(label: &str, suffix: usize) -> VortexKey {
        VortexKey::from(format!("race:{label}:{suffix:03}"))
    }

    fn key_for_shard_with_len(
        keyspace: &ConcurrentKeyspace,
        target_shard: usize,
        total_len: usize,
        prefix: &str,
    ) -> VortexKey {
        for candidate in 0..200_000usize {
            let mut text = format!("{prefix}:{candidate:06}");
            if text.len() > total_len {
                continue;
            }
            while text.len() < total_len {
                text.push('x');
            }
            let key = VortexKey::from(text);
            if keyspace.shard_index(key.as_bytes()) == target_shard {
                return key;
            }
        }

        panic!("failed to find key for shard {target_shard} with length {total_len}");
    }

    fn value_of_len(len: usize, byte: u8) -> VortexValue {
        let bytes = vec![byte; len];
        VortexValue::from_bytes(&bytes)
    }

    fn insert_raw(keyspace: &ConcurrentKeyspace, key: VortexKey, value: VortexValue) {
        let key_bytes = key.as_bytes().to_vec();
        keyspace.write(&key_bytes, move |table| {
            table.insert(key.clone(), value.clone());
        });
    }

    fn configure_noeviction_at_current_usage(keyspace: &ConcurrentKeyspace) {
        keyspace.configure_eviction(keyspace.memory_used(), EvictionPolicy::NoEviction);
    }

    fn run_projection_race<T, F, G>(
        label: &'static str,
        keyspace: Arc<ConcurrentKeyspace>,
        writer: F,
        interleave: G,
    ) -> MutationResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<ConcurrentKeyspace>) -> MutationResult<T> + Send + 'static,
        G: FnOnce(&ConcurrentKeyspace),
    {
        let _test_lock = PROJECTION_ADMISSION_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (entered_rx, release_tx) = install_projection_admission_test_hook(label);
        let writer_keyspace = Arc::clone(&keyspace);
        let handle = thread::spawn(move || writer(writer_keyspace));

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("writer should pause after projection");
        interleave(&keyspace);
        release_tx
            .send(())
            .expect("writer release sender should stay alive");

        let result = handle.join().expect("writer thread should not panic");
        let mut slot = PROJECTION_ADMISSION_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        slot.take();
        result
    }

    fn assert_oom<T>(result: MutationResult<T>) {
        let error = match result {
            Ok(_) => panic!("mutation should fail with OOM after revalidation"),
            Err(error) => error,
        };
        assert_eq!(error.response, crate::commands::ERR_OOM);
    }

    #[test]
    fn set_value_plain_revalidates_after_delete_and_fill() {
        let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let key = fixed_key("set", 0);
        let filler = fixed_key("set", 1);
        let old_value = value_of_len(32, b'a');
        let new_value = value_of_len(32, b'b');

        insert_raw(&keyspace, key.clone(), old_value.clone());
        configure_noeviction_at_current_usage(&keyspace);

        let writer_key = key.clone();
        let writer_value = new_value.clone();
        let stale_key = key.clone();
        let stale_filler = filler.clone();
        let stale_value = old_value.clone();
        let result = run_projection_race(
            "set_value_plain",
            Arc::clone(&keyspace),
            move |keyspace| keyspace.set_value_plain(writer_key, writer_value, 0),
            move |keyspace| {
                assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
                insert_raw(keyspace, stale_filler, stale_value);
            },
        );

        assert_oom(result);
        assert!(keyspace.get_value(&key, 0).is_none());
        assert!(keyspace.get_value(&filler, 0).is_some());
    }

    #[test]
    fn append_value_revalidates_after_delete_and_fill() {
        let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let key = fixed_key("append", 0);
        let filler = fixed_key("append", 1);
        let old_value = value_of_len(32, b'c');

        insert_raw(&keyspace, key.clone(), old_value.clone());
        configure_noeviction_at_current_usage(&keyspace);

        let writer_key = key.clone();
        let stale_key = key.clone();
        let stale_filler = filler.clone();
        let stale_value = old_value.clone();
        let result = run_projection_race(
            "append_value",
            Arc::clone(&keyspace),
            move |keyspace| keyspace.append_value(writer_key, b"", 0),
            move |keyspace| {
                assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
                insert_raw(keyspace, stale_filler, stale_value);
            },
        );

        assert_oom(result);
        assert!(keyspace.get_value(&key, 0).is_none());
        assert!(keyspace.get_value(&filler, 0).is_some());
    }

    #[test]
    fn setrange_value_revalidates_after_delete_and_fill() {
        let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let key = fixed_key("setrange", 0);
        let filler = fixed_key("setrange", 1);
        let old_value = value_of_len(1, b'd');

        insert_raw(&keyspace, key.clone(), old_value.clone());
        configure_noeviction_at_current_usage(&keyspace);

        let writer_key = key.clone();
        let stale_key = key.clone();
        let stale_filler = filler.clone();
        let stale_value = old_value.clone();
        let result = run_projection_race(
            "setrange_value",
            Arc::clone(&keyspace),
            move |keyspace| keyspace.setrange_value(writer_key, 0, b"z", 0),
            move |keyspace| {
                assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
                insert_raw(keyspace, stale_filler, stale_value);
            },
        );

        assert_oom(result);
        assert!(keyspace.get_value(&key, 0).is_none());
        assert!(keyspace.get_value(&filler, 0).is_some());
    }

    #[test]
    fn increment_by_revalidates_after_delete_and_fill() {
        let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let key = fixed_key("incr", 0);
        let filler = fixed_key("incr", 1);
        let old_value = VortexValue::from(0_i64);

        insert_raw(&keyspace, key.clone(), old_value.clone());
        configure_noeviction_at_current_usage(&keyspace);

        let writer_key = key.clone();
        let stale_key = key.clone();
        let stale_filler = filler.clone();
        let stale_value = old_value.clone();
        let result = run_projection_race(
            "increment_by",
            Arc::clone(&keyspace),
            move |keyspace| keyspace.increment_by(writer_key, 0, 0),
            move |keyspace| {
                assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
                insert_raw(keyspace, stale_filler, stale_value);
            },
        );

        assert_oom(result);
        assert!(keyspace.get_value(&key, 0).is_none());
        assert!(keyspace.get_value(&filler, 0).is_some());
    }

    #[test]
    fn increment_by_float_revalidates_after_delete_and_fill() {
        let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let key = fixed_key("incrfloat", 0);
        let filler = fixed_key("incrfloat", 1);
        let old_value = VortexValue::from("0.0");

        insert_raw(&keyspace, key.clone(), old_value.clone());
        configure_noeviction_at_current_usage(&keyspace);

        let writer_key = key.clone();
        let stale_key = key.clone();
        let stale_filler = filler.clone();
        let stale_value = old_value.clone();
        let result = run_projection_race(
            "increment_by_float",
            Arc::clone(&keyspace),
            move |keyspace| keyspace.increment_by_float(writer_key, 0.0, 0),
            move |keyspace| {
                assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
                insert_raw(keyspace, stale_filler, stale_value);
            },
        );

        assert_oom(result);
        assert!(keyspace.get_value(&key, 0).is_none());
        assert!(keyspace.get_value(&filler, 0).is_some());
    }

    #[test]
    fn mset_values_revalidates_after_delete_and_fill() {
        let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let key = fixed_key("mset", 0);
        let filler = fixed_key("mset", 1);
        let old_value = value_of_len(32, b'm');
        let new_value = value_of_len(32, b'n');

        insert_raw(&keyspace, key.clone(), old_value.clone());
        configure_noeviction_at_current_usage(&keyspace);

        let writer_key = key.clone();
        let writer_value = new_value.clone();
        let stale_key = key.clone();
        let stale_filler = filler.clone();
        let stale_value = old_value.clone();
        let result = run_projection_race(
            "mset_values",
            Arc::clone(&keyspace),
            move |keyspace| keyspace.mset_values(vec![(writer_key, writer_value)], 0),
            move |keyspace| {
                assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
                insert_raw(keyspace, stale_filler, stale_value);
            },
        );

        assert_oom(result);
        assert!(keyspace.get_value(&key, 0).is_none());
        assert!(keyspace.get_value(&filler, 0).is_some());
    }

    #[test]
    fn mset_values_deduplicates_before_memory_admission() {
        let keyspace = ConcurrentKeyspace::new(TEST_SHARDS);
        let key = fixed_key("mset-duplicate", 0);
        let first_value = value_of_len(32, b'1');
        let second_value = value_of_len(32, b'2');
        let projected_delta = {
            let shard_index = keyspace.shard_index(key.as_bytes());
            let guard = keyspace.read_shard_by_index(shard_index);
            positive_delta(guard.projected_insert_delta(&key, &second_value)).bytes()
        };
        keyspace.configure_eviction(
            keyspace.memory_used() + projected_delta,
            EvictionPolicy::NoEviction,
        );

        let outcome = keyspace
            .mset_values(
                vec![
                    (key.clone(), first_value),
                    (key.clone(), second_value.clone()),
                ],
                0,
            )
            .expect("deduplicated MSET should fit maxmemory");

        assert_eq!(outcome.value, ());
        assert_eq!(keyspace.get_value(&key, 0), Some(second_value));
    }

    #[test]
    fn rename_key_revalidates_after_destination_delete_and_fill() {
        let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let old_key = key_for_shard_with_len(&keyspace, 0, 20, "rename-old");
        let new_key = key_for_shard_with_len(&keyspace, TEST_SHARDS - 1, 28, "rename-new");
        let filler_key = key_for_shard_with_len(&keyspace, 1, 28, "rename-fill");
        let value = value_of_len(16, b'r');

        insert_raw(&keyspace, old_key.clone(), value.clone());
        insert_raw(&keyspace, new_key.clone(), value.clone());
        configure_noeviction_at_current_usage(&keyspace);

        let writer_old = old_key.clone();
        let writer_new = new_key.clone();
        let stale_new = new_key.clone();
        let stale_filler = filler_key.clone();
        let stale_value = value.clone();
        let result = run_projection_race(
            "rename_key",
            Arc::clone(&keyspace),
            move |keyspace| keyspace.rename_key(&writer_old, writer_new, 0, false),
            move |keyspace| {
                assert!(keyspace.remove_value(&stale_new, 0).value.is_some());
                insert_raw(keyspace, stale_filler, stale_value);
            },
        );

        assert_oom(result);
        assert!(keyspace.get_value(&old_key, 0).is_some());
        assert!(keyspace.get_value(&new_key, 0).is_none());
        assert!(keyspace.get_value(&filler_key, 0).is_some());
    }

    #[test]
    fn rename_key_same_shard_respects_memory_admission() {
        let keyspace = ConcurrentKeyspace::new(TEST_SHARDS);
        let old_key = key_for_shard_with_len(&keyspace, 0, 32, "rename-same-old");
        let new_key = key_for_shard_with_len(&keyspace, 0, 56, "rename-same-new");
        let value = value_of_len(16, b's');

        insert_raw(&keyspace, old_key.clone(), value.clone());
        configure_noeviction_at_current_usage(&keyspace);

        let result = keyspace.rename_key(&old_key, new_key.clone(), 0, false);

        assert_oom(result);
        assert_eq!(keyspace.get_value(&old_key, 0), Some(value));
        assert!(keyspace.get_value(&new_key, 0).is_none());
    }

    #[test]
    fn copy_key_revalidates_after_destination_delete_and_fill() {
        let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let src = key_for_shard_with_len(&keyspace, 0, 20, "copy-src");
        let dst = key_for_shard_with_len(&keyspace, TEST_SHARDS - 1, 20, "copy-dst");
        let filler = key_for_shard_with_len(&keyspace, 1, 20, "copy-fill");
        let value = value_of_len(16, b'c');

        insert_raw(&keyspace, src.clone(), value.clone());
        insert_raw(&keyspace, dst.clone(), value.clone());
        configure_noeviction_at_current_usage(&keyspace);

        let writer_src = src.clone();
        let writer_dst = dst.clone();
        let stale_dst = dst.clone();
        let stale_filler = filler.clone();
        let stale_value = value.clone();
        let result = run_projection_race(
            "copy_key",
            Arc::clone(&keyspace),
            move |keyspace| keyspace.copy_key(&writer_src, writer_dst, true, 0),
            move |keyspace| {
                assert!(keyspace.remove_value(&stale_dst, 0).value.is_some());
                insert_raw(keyspace, stale_filler, stale_value);
            },
        );

        assert_oom(result);
        assert!(keyspace.get_value(&src, 0).is_some());
        assert!(keyspace.get_value(&dst, 0).is_none());
        assert!(keyspace.get_value(&filler, 0).is_some());
    }
}
