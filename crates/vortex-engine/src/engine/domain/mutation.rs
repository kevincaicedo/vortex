use super::*;

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
    pub(super) fn permits(self, current_ttl: u64, deadline_nanos: u64) -> bool {
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
pub(super) struct AofEffect {
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
    pub(super) const fn into_lsn(self) -> Option<u64> {
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
pub(super) struct MutationEffects<'a> {
    ttl: Option<TtlEffect>,
    watch: WatchEffect<'a>,
    frequency: Option<TableHash>,
    aof: AofEffect,
}

impl<'a> MutationEffects<'a> {
    #[inline(always)]
    pub(super) const fn none() -> Self {
        Self {
            ttl: None,
            watch: WatchEffect::None,
            frequency: None,
            aof: AofEffect::none(),
        }
    }

    #[inline(always)]
    pub(super) const fn with_ttl(
        mut self,
        shard_index: usize,
        transition: ExpiryTransition,
    ) -> Self {
        self.ttl = Some(TtlEffect {
            shard_index,
            transition,
        });
        self
    }

    #[inline(always)]
    pub(super) const fn with_watch_key(mut self, key: &'a VortexKey) -> Self {
        self.watch = WatchEffect::Key(key);
        self
    }

    #[inline(always)]
    pub(super) const fn with_watch_key_bytes(mut self, key_bytes: &'a [u8]) -> Self {
        self.watch = WatchEffect::KeyBytes(key_bytes);
        self
    }

    #[inline(always)]
    pub(super) fn with_watch_key_if(self, condition: bool, key: &'a VortexKey) -> Self {
        if condition {
            self.with_watch_key(key)
        } else {
            self
        }
    }

    #[inline(always)]
    pub(super) fn with_optional_watch_key(self, key: Option<&'a VortexKey>) -> Self {
        match key {
            Some(key) => self.with_watch_key(key),
            None => self,
        }
    }

    #[inline(always)]
    pub(super) const fn with_frequency(mut self, hash: TableHash) -> Self {
        self.frequency = Some(hash);
        self
    }

    #[inline(always)]
    pub(super) const fn with_aof_lsn(mut self, lsn: Option<u64>) -> Self {
        self.aof = AofEffect::lsn(lsn);
        self
    }
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
pub(super) fn mutation_outcome_with_evictions<T>(
    value: T,
    aof_lsn: Option<u64>,
    evicted: EvictedKeys,
) -> MutationOutcome<T> {
    MutationOutcome::with_aof_records(value, evicted_keys_to_aof_records(evicted), aof_lsn)
}

#[inline(always)]
pub(super) fn stamp_entry_lsn(
    table: &mut SwissTable,
    key_bytes: &[u8],
    table_hash: TableHash,
    lsn: u64,
) {
    let stamped = table.set_lsn_version_prehashed(key_bytes, table_hash, lsn);
    debug_assert!(stamped, "live key must still exist when stamping LSN");
}

#[inline(always)]
pub(super) fn stamp_entry_lsn_if(
    table: &mut SwissTable,
    key_bytes: &[u8],
    table_hash: TableHash,
    lsn: Option<u64>,
) {
    if let Some(lsn) = lsn {
        stamp_entry_lsn(table, key_bytes, table_hash, lsn);
    }
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
pub(super) fn ttl_present(ttl_deadline: Option<u64>) -> bool {
    matches!(ttl_deadline, Some(deadline) if deadline != 0)
}

#[inline]
pub(super) fn positive_delta(delta: isize) -> PositiveDelta {
    ProjectedDelta::from_bytes(delta).positive()
}

#[inline]
fn admission_revalidation_active(keyspace: &ConcurrentKeyspace, snapshot: EvictionConfig) -> bool {
    snapshot.max_memory != 0 && !keyspace.replay_mode_active()
}

#[inline]
pub(super) fn entry_memory_usage(key: &VortexKey, value_memory_usage: usize) -> usize {
    size_of::<Entry>() + key.memory_usage() + value_memory_usage
}

pub(super) fn projected_rewrite_delta(
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

pub(super) struct ReservationState<'a> {
    pub(super) snapshot: EvictionConfig,
    pub(super) reservation: MemoryReservation<'a>,
    pub(super) evicted: EvictedKeys,
}

impl<'a> ReservationState<'a> {
    #[inline(always)]
    pub(super) fn empty(keyspace: &'a ConcurrentKeyspace, snapshot: EvictionConfig) -> Self {
        Self {
            snapshot,
            reservation: MemoryReservation::new(keyspace, 0),
            evicted: None,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct ReservationCoordinator<'a> {
    keyspace: &'a ConcurrentKeyspace,
    now_nanos: u64,
}

impl<'a> ReservationCoordinator<'a> {
    #[inline(always)]
    pub(super) const fn new(keyspace: &'a ConcurrentKeyspace, now_nanos: u64) -> Self {
        Self {
            keyspace,
            now_nanos,
        }
    }

    #[inline]
    pub(super) fn reserve(
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
    pub(super) fn admission_revalidation_active(self, snapshot: EvictionConfig) -> bool {
        admission_revalidation_active(self.keyspace, snapshot)
    }

    #[inline]
    pub(super) fn acquire_single_shard<F>(
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
    pub(super) fn acquire_multi_write<F>(
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
pub(super) struct ProjectionAdmissionTestHook {
    label: &'static str,
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
pub(super) static PROJECTION_ADMISSION_TEST_HOOK: std::sync::Mutex<
    Option<ProjectionAdmissionTestHook>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(super) static PROJECTION_ADMISSION_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
pub(super) fn install_projection_admission_test_hook(
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
pub(super) fn remove_if_expired(table: &mut SwissTable, key: &VortexKey, now_nanos: u64) -> bool {
    match table.get_entry_ttl(key) {
        Some(deadline) if deadline != 0 && deadline <= now_nanos => {
            table.remove(key);
            true
        }
        _ => false,
    }
}

impl ConcurrentKeyspace {
    #[inline(always)]
    pub(super) fn commit_effects(&self, effects: MutationEffects<'_>) -> AofEffect {
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

    #[inline]
    pub(super) fn cleanup_expired_key(
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
    pub(super) fn cleanup_expired_prehashed(
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
}
