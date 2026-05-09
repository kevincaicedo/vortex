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
    lsn: Option<AofLsn>,
}

impl AofEffect {
    #[inline(always)]
    const fn none() -> Self {
        Self { lsn: None }
    }

    #[inline(always)]
    const fn lsn(lsn: Option<AofLsn>) -> Self {
        Self { lsn }
    }

    #[inline(always)]
    pub(super) const fn into_lsn(self) -> Option<AofLsn> {
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

impl WatchEffect<'_> {
    #[inline(always)]
    const fn is_none(self) -> bool {
        matches!(self, Self::None)
    }
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
    pub(super) fn with_watch_key(mut self, key: &'a VortexKey) -> Self {
        self.watch = WatchEffect::Key(key);
        self
    }

    #[inline(always)]
    pub(super) fn with_watch_key_bytes(mut self, key_bytes: &'a [u8]) -> Self {
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
    pub(super) const fn with_aof_lsn(mut self, lsn: Option<AofLsn>) -> Self {
        self.aof = AofEffect::lsn(lsn);
        self
    }

    #[inline(always)]
    pub(super) const fn is_empty(self) -> bool {
        let ttl_empty = match self.ttl {
            Some(ttl) => ttl.transition.is_noop(),
            None => true,
        };
        ttl_empty && self.watch.is_none() && self.frequency.is_none() && self.aof.lsn.is_none()
    }

    #[inline(always)]
    pub(super) const fn defer(self) -> DeferredEffects<'a> {
        DeferredEffects { effects: self }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
#[must_use = "deferred mutation effects must be published after shard guards drop"]
pub(super) struct DeferredEffects<'a> {
    effects: MutationEffects<'a>,
}

#[derive(Debug, Default, PartialEq, Eq)]
#[must_use = "owned deferred mutation effects must be published after shard guards drop"]
pub(super) struct OwnedDeferredEffects {
    ttl: Option<TtlEffect>,
    watch: Option<VortexKey>,
    frequency: Option<TableHash>,
    aof: AofEffect,
}

impl OwnedDeferredEffects {
    #[inline(always)]
    pub(super) fn from_owned_watch(
        effects: MutationEffects<'static>,
        watch: Option<VortexKey>,
    ) -> Self {
        debug_assert!(
            effects.watch.is_none(),
            "owned deferred effects must receive owned WATCH keys explicitly"
        );
        Self {
            ttl: effects.ttl,
            watch,
            frequency: effects.frequency,
            aof: effects.aof,
        }
    }

    #[inline(always)]
    pub(super) fn publish(self, keyspace: &ConcurrentKeyspace) -> AofEffect {
        if let Some(ttl) = self.ttl {
            keyspace.apply_expiry_transition(ttl.shard_index, ttl.transition);
        }
        if let Some(hash) = self.frequency {
            keyspace.record_frequency_hash(hash);
        }
        if let Some(key) = self.watch {
            keyspace.bump_watch_key(&key);
        }
        self.aof
    }
}

impl<'a> DeferredEffects<'a> {
    #[inline(always)]
    pub(super) fn publish(self, keyspace: &ConcurrentKeyspace) -> AofEffect {
        let MutationEffects {
            ttl,
            watch,
            frequency,
            aof,
        } = self.effects;

        if let Some(ttl) = ttl {
            keyspace.apply_expiry_transition(ttl.shard_index, ttl.transition);
        }
        if let Some(hash) = frequency {
            keyspace.record_frequency_hash(hash);
        }
        match watch {
            WatchEffect::None => {}
            WatchEffect::Key(key) => keyspace.bump_watch_key(key),
            WatchEffect::KeyBytes(key_bytes) => keyspace.bump_watch_key_bytes(key_bytes),
        }
        aof
    }
}

#[derive(Debug)]
pub(crate) struct MutationOutcome<T> {
    pub(crate) value: T,
    pub(crate) aof_records: AofRecords,
    pub(crate) aof_lsn: Option<AofLsn>,
}

impl<T> MutationOutcome<T> {
    #[inline(always)]
    pub(crate) fn new(value: T, aof_lsn: Option<AofLsn>) -> Self {
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
        aof_lsn: Option<AofLsn>,
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
    pub(crate) kind: MutationErrorKind,
    pub(crate) aof_records: AofRecords,
}

impl MutationError {
    #[inline(always)]
    pub(crate) fn new(kind: MutationErrorKind) -> Self {
        Self {
            kind,
            aof_records: None,
        }
    }

    #[inline(always)]
    pub(crate) fn with_evictions(kind: MutationErrorKind, evicted: EvictedKeys) -> Self {
        Self {
            kind,
            aof_records: evicted_keys_to_aof_records(evicted),
        }
    }
}

impl From<MutationErrorKind> for MutationError {
    #[inline(always)]
    fn from(kind: MutationErrorKind) -> Self {
        Self::new(kind)
    }
}

impl From<EvictionAdmissionError> for MutationError {
    #[inline(always)]
    fn from(error: EvictionAdmissionError) -> Self {
        Self {
            kind: error.kind,
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
    aof_lsn: Option<AofLsn>,
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
        F: FnMut(&SwissTable) -> Result<PositiveDelta, MutationErrorKind>,
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
        F: FnMut(&SwissTable) -> Result<PositiveDelta, MutationErrorKind>,
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
                Err(kind) => {
                    #[cfg(feature = "lock-profile")]
                    self.keyspace.record_lock_profile_revalidation_failure(
                        crate::keyspace::LockProfileClass::SingleKey,
                    );
                    return Err(MutationError::with_evictions(kind, evicted));
                }
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
            #[cfg(feature = "lock-profile")]
            self.keyspace
                .record_lock_profile_retry(crate::keyspace::LockProfileClass::SingleKey);

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
                    return Err(MutationError::with_evictions(error.kind, evicted));
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
        F: FnMut(&mut ShardWriteGuards<'a>, &ShardPlan) -> Result<PositiveDelta, MutationErrorKind>,
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
    pub(super) fn acquire_prehashed_multi_write<'k, F>(
        self,
        plan: &PrehashedShardPlan<'k>,
        preferred_shard: usize,
        state: ReservationState<'a>,
        hook_label: &'static str,
        admission_active: bool,
        required_delta: F,
    ) -> Result<(ShardWriteGuards<'a>, ReservationState<'a>), MutationError>
    where
        F: FnMut(
            &mut ShardWriteGuards<'a>,
            &PrehashedShardPlan<'k>,
        ) -> Result<PositiveDelta, MutationErrorKind>,
    {
        if !admission_active {
            let guards = self.keyspace.multi_write_prehashed(plan);
            return Ok((guards, state));
        }

        self.acquire_prehashed_multi_write_revalidated(
            plan,
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
        F: FnMut(&mut ShardWriteGuards<'a>, &ShardPlan) -> Result<PositiveDelta, MutationErrorKind>,
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
                Err(kind) => {
                    #[cfg(feature = "lock-profile")]
                    self.keyspace.record_lock_profile_revalidation_failure(
                        crate::keyspace::LockProfileClass::MultiKey,
                    );
                    return Err(MutationError::with_evictions(kind, evicted));
                }
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
            #[cfg(feature = "lock-profile")]
            self.keyspace
                .record_lock_profile_retry(crate::keyspace::LockProfileClass::MultiKey);

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
                    return Err(MutationError::with_evictions(error.kind, evicted));
                }
            };
            merge_evicted_keys(&mut evicted, additional_evicted);
            reservation.absorb(additional_reservation);
        }
    }

    #[inline]
    fn acquire_prehashed_multi_write_revalidated<'k, F>(
        self,
        plan: &PrehashedShardPlan<'k>,
        preferred_shard: usize,
        state: ReservationState<'a>,
        hook_label: &'static str,
        mut required_delta: F,
    ) -> Result<(ShardWriteGuards<'a>, ReservationState<'a>), MutationError>
    where
        F: FnMut(
            &mut ShardWriteGuards<'a>,
            &PrehashedShardPlan<'k>,
        ) -> Result<PositiveDelta, MutationErrorKind>,
    {
        let ReservationState {
            snapshot,
            mut reservation,
            mut evicted,
        } = state;

        maybe_pause_after_projection(hook_label);

        loop {
            let mut guards = self.keyspace.multi_write_prehashed(plan);
            let required = match required_delta(&mut guards, plan) {
                Ok(required) => required,
                Err(kind) => {
                    #[cfg(feature = "lock-profile")]
                    self.keyspace.record_lock_profile_revalidation_failure(
                        crate::keyspace::LockProfileClass::MultiKey,
                    );
                    return Err(MutationError::with_evictions(kind, evicted));
                }
            };

            if required.bytes() <= reservation.reserved_bytes() {
                return Ok((
                    guards,
                    ReservationState {
                        snapshot,
                        reservation,
                        evicted,
                    },
                ));
            }

            let extra = PositiveDelta::from_bytes(required.bytes() - reservation.reserved_bytes());
            drop(guards);
            #[cfg(feature = "lock-profile")]
            self.keyspace
                .record_lock_profile_retry(crate::keyspace::LockProfileClass::MultiKey);

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
                    return Err(MutationError::with_evictions(error.kind, evicted));
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
pub(super) struct OptimisticPrepareTestHook {
    label: &'static str,
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
pub(super) static OPTIMISTIC_PREPARE_TEST_HOOK: std::sync::Mutex<
    Option<OptimisticPrepareTestHook>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(super) static OPTIMISTIC_PREPARE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
thread_local! {
    static DEFERRED_EFFECT_PUBLISH_PAUSE_ENABLED: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) struct DeferredEffectPublishPauseScope;

#[cfg(test)]
impl Drop for DeferredEffectPublishPauseScope {
    fn drop(&mut self) {
        DEFERRED_EFFECT_PUBLISH_PAUSE_ENABLED.with(|enabled| enabled.set(false));
    }
}

#[cfg(test)]
pub(super) fn enable_deferred_effect_publish_pause_for_current_thread()
-> DeferredEffectPublishPauseScope {
    DEFERRED_EFFECT_PUBLISH_PAUSE_ENABLED.with(|enabled| enabled.set(true));
    DeferredEffectPublishPauseScope
}

#[cfg(test)]
pub(super) struct DeferredEffectPublishTestHook {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
pub(super) static DEFERRED_EFFECT_PUBLISH_TEST_HOOK: std::sync::Mutex<
    Option<DeferredEffectPublishTestHook>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(super) static DEFERRED_EFFECT_PUBLISH_TEST_LOCK: std::sync::Mutex<()> =
    std::sync::Mutex::new(());

#[cfg(test)]
pub(super) fn install_deferred_effect_publish_test_hook() -> (
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::SyncSender<()>,
) {
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let mut slot = DEFERRED_EFFECT_PUBLISH_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(
        slot.is_none(),
        "deferred effect publish test hook already installed"
    );
    *slot = Some(DeferredEffectPublishTestHook {
        entered: entered_tx,
        release: release_rx,
    });
    (entered_rx, release_tx)
}

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
pub(super) fn install_optimistic_prepare_test_hook(
    label: &'static str,
) -> (
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::SyncSender<()>,
) {
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let mut slot = OPTIMISTIC_PREPARE_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(
        slot.is_none(),
        "optimistic prepare test hook already installed"
    );
    *slot = Some(OptimisticPrepareTestHook {
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

#[cfg(test)]
pub(super) fn maybe_pause_after_optimistic_prepare(label: &'static str) {
    let hook = {
        let mut slot = OPTIMISTIC_PREPARE_TEST_HOOK
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
            .expect("optimistic prepare test hook receiver must stay alive");
        hook.release
            .recv()
            .expect("optimistic prepare test release sender must stay alive");
    }
}

#[cfg(not(test))]
#[inline(always)]
pub(super) fn maybe_pause_after_optimistic_prepare(_label: &'static str) {}

#[cfg(test)]
fn maybe_pause_before_deferred_effect_publish() {
    let enabled = DEFERRED_EFFECT_PUBLISH_PAUSE_ENABLED.with(|enabled| enabled.get());
    if !enabled {
        return;
    }

    let hook = {
        let mut slot = DEFERRED_EFFECT_PUBLISH_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        slot.take()
    };

    if let Some(hook) = hook {
        hook.entered
            .send(())
            .expect("deferred effect publish hook receiver must stay alive");
        hook.release
            .recv()
            .expect("deferred effect publish release sender must stay alive");
    }
}

#[cfg(not(test))]
#[inline(always)]
fn maybe_pause_before_deferred_effect_publish() {}

#[inline]
pub(super) fn remove_if_expired(table: &mut SwissTable, key: &VortexKey, now_nanos: u64) -> bool {
    let hash = table.table_hash_key_bytes(key.as_bytes());
    match table.slot_cursor_prehashed(key.as_bytes(), hash, now_nanos) {
        SlotCursor::Expired(expired) => expired.remove().is_some(),
        SlotCursor::Live(_) | SlotCursor::Vacant(_) => false,
    }
}

impl ConcurrentKeyspace {
    #[inline(always)]
    pub(super) fn publish_deferred_effects(&self, effects: DeferredEffects<'_>) -> AofEffect {
        #[cfg(feature = "lock-profile")]
        let _lock_profile =
            self.enter_lock_profile_scope(crate::keyspace::LockProfileClass::AofMetadata);
        maybe_pause_before_deferred_effect_publish();
        effects.publish(self)
    }

    #[inline(always)]
    pub(super) fn publish_owned_deferred_effects(
        &self,
        effects: OwnedDeferredEffects,
    ) -> AofEffect {
        #[cfg(feature = "lock-profile")]
        let _lock_profile =
            self.enter_lock_profile_scope(crate::keyspace::LockProfileClass::AofMetadata);
        maybe_pause_before_deferred_effect_publish();
        effects.publish(self)
    }

    #[inline(always)]
    pub(super) fn publish_optional_deferred_effects(&self, effects: Option<DeferredEffects<'_>>) {
        if let Some(effects) = effects {
            let _ = self.publish_deferred_effects(effects);
        }
    }

    #[inline]
    pub(super) fn cleanup_expired_key<'a>(
        &self,
        shard_index: usize,
        table: &mut SwissTable,
        key: &'a VortexKey,
        now_nanos: u64,
    ) -> Option<DeferredEffects<'a>> {
        let hash = table.table_hash_key_bytes(key.as_bytes());
        let removed = match table.slot_cursor_prehashed(key.as_bytes(), hash, now_nanos) {
            SlotCursor::Expired(expired) => expired.remove(),
            SlotCursor::Live(_) | SlotCursor::Vacant(_) => None,
        };
        removed.map(|removal| {
            MutationEffects::none()
                .with_ttl(shard_index, ExpiryTransition::remove(removal.old_had_ttl()))
                .with_watch_key(key)
                .defer()
        })
    }

    #[inline]
    pub(super) fn cleanup_expired_key_bytes_owned(
        &self,
        shard_index: usize,
        table: &mut SwissTable,
        key_bytes: &[u8],
        hash: TableHash,
        now_nanos: u64,
    ) -> Option<OwnedDeferredEffects> {
        let removed = match table.slot_cursor_prehashed(key_bytes, hash, now_nanos) {
            SlotCursor::Expired(expired) => expired.remove(),
            SlotCursor::Live(_) | SlotCursor::Vacant(_) => None,
        }?;
        Some(OwnedDeferredEffects::from_owned_watch(
            MutationEffects::none()
                .with_ttl(shard_index, ExpiryTransition::remove(removed.old_had_ttl())),
            Some(VortexKey::from_bytes(key_bytes)),
        ))
    }

    #[inline]
    pub(super) fn cleanup_expired_prehashed<'a>(
        &self,
        shard_index: usize,
        table: &mut SwissTable,
        key_bytes: &'a [u8],
        hash: TableHash,
        now_nanos: u64,
    ) -> Option<DeferredEffects<'a>> {
        let removed = match table.slot_cursor_prehashed(key_bytes, hash, now_nanos) {
            SlotCursor::Expired(expired) => expired.remove(),
            SlotCursor::Live(_) | SlotCursor::Vacant(_) => None,
        }?;
        Some(
            MutationEffects::none()
                .with_ttl(shard_index, ExpiryTransition::remove(removed.old_had_ttl()))
                .with_watch_key_bytes(key_bytes)
                .defer(),
        )
    }
}
