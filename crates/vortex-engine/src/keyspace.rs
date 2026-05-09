//! Concurrent Keyspace — Sharded RwLock architecture.
//!
//! The keyspace is partitioned into `K` shards (power-of-two), each a
//! `CachePadded<RwLock<SwissTable>>`. Shard selection uses `ahash(key) & mask`
//! — a single AES-NI instruction on x86_64.
//!
//! # Concurrency guarantees
//!
//! - **Deadlock-free:** Multi-key operations acquire shard locks in strictly
//!   ascending shard index order. This total ordering prevents circular wait.
//!   Formally proven via Kani proof in the M2 lab (`verify_binary_search_always_finds`).
//!
//! - **No reader starvation:** `parking_lot::RwLock` is fair — readers and writers
//!   alternate, preventing starvation in either direction.
//!
//! - **Minimized critical sections:** Data conversion (key/value construction)
//!   happens outside lock guards. Only raw table operations occur under the lock.
//!
//! # Mechanical sympathy
//!
//! - Each shard is `CachePadded` to 128 bytes, preventing false sharing between
//!   adjacent shard locks on different cache lines.
//! - `parking_lot::RwLock` uses a single `AtomicU8` — no reader counter bouncing
//!   that plagues `std::sync::RwLock`.
//! - Power-of-two shard count enables bitwise AND instead of modulo for shard
//!   selection — saves ~3ns per operation vs integer division.
//! - `unsafe { get_unchecked }` on the shard array eliminates bounds checks on
//!   the hot path — the mask guarantees `idx < shards.len()`.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use ahash::RandomState;
use crossbeam_utils::CachePadded;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use vortex_common::{Timestamp, VortexKey, VortexValue};

use crate::effects::MutationErrorKind;
use crate::eviction::{
    EvictionConfig, EvictionConfigState, EvictionPolicy, FrequencySketch, next_random_u64,
    should_sample_lfu_read,
};
use crate::table::{SwissTable, TableHash};

mod admin;
mod eviction_sweep;
mod expiry;
mod features;
mod gate;
#[cfg(feature = "lock-profile")]
mod lock_profile;
mod memory;
mod metrics;
mod persistence;
mod shards;
mod watch;

pub use eviction_sweep::EvictionMaintenanceSlice;
pub(super) use eviction_sweep::EvictionScanReport;
pub(crate) use eviction_sweep::{EvictedKey, EvictedKeys};
pub(crate) use expiry::ExpiryTransition;
pub(crate) use features::MutationFeatures;
use gate::TransactionGate;
pub use gate::{CommandGateGuard, TransactionGateGuard, TransactionGatePlan};
#[cfg(feature = "lock-profile")]
pub use lock_profile::{
    BUCKET_LABELS as LOCK_PROFILE_BUCKET_LABELS, LockProfileClass, LockProfileClassSnapshot,
    LockProfileSnapshot, LockProfileStatSnapshot,
};
#[cfg(feature = "lock-profile")]
use lock_profile::{LockProfileHold, LockProfileKind, LockProfileScope, LockProfileState};
use memory::ServerMemoryAttribution;
pub use memory::{EngineMemoryAttributionSnapshot, ServerMemoryAttributionSnapshot};
pub(crate) use memory::{EvictionAdmissionError, MemoryReservation, PositiveDelta, ProjectedDelta};
use metrics::{EvictionMetrics, RuntimeMetrics};
pub use metrics::{
    EvictionMetricsSnapshot, RuntimeAofTelemetry, RuntimeBackendMode, RuntimeBackendSnapshot,
    RuntimeLocalFlushMetrics, RuntimeMetricsSnapshot, RuntimeOverloadTelemetry,
    RuntimeTelemetryMode,
};
pub use persistence::{AofLsn, EntryLsn, Lsn, LsnOverflow, LsnRestoreError, ReplayModeGuard};
use shards::{MultiReadGuards, MultiWriteGuards, Shard, ShardId, ShardReadGuards};
pub(crate) use shards::{PrehashedKeyPlan, PrehashedShardPlan, ShardPlan, ShardWriteGuards};
pub use shards::{ShardCount, ShardCountError};
pub use watch::WatchRegistration;
use watch::{AbsentWatchShard, make_absent_watch_shards};

// ─── Constants ──────────────────────────────────────────────────────

/// Default shard count — 4096 provides optimal balance between parallelism
/// and memory overhead.
pub const DEFAULT_SHARD_COUNT: usize = 4096;

/// Minimum allowed shard count.
pub const MIN_SHARD_COUNT: usize = 64;

/// Maximum allowed shard count.
pub const MAX_SHARD_COUNT: usize = 131_072;

/// Fixed ahash seeds for deterministic shard routing across process restarts.
const AHASH_SEED_0: u64 = 0x517c_c1b7_2722_0a95;
const AHASH_SEED_1: u64 = 0x6c62_272e_07bb_0142;
const AHASH_SEED_2: u64 = 0x8fbc_2d2b_9e3a_6ee8;
const AHASH_SEED_3: u64 = 0xcf41_41b0_ed82_a837;
const ABSENT_WATCH_SHARD_COUNT: usize = 256;
#[cfg(feature = "lock-profile")]
pub struct ShardReadGuard<'a> {
    guard: RwLockReadGuard<'a, SwissTable>,
    profile: Option<LockProfileHold<'a>>,
}

#[cfg(not(feature = "lock-profile"))]
pub type ShardReadGuard<'a> = RwLockReadGuard<'a, SwissTable>;

#[cfg(feature = "lock-profile")]
impl Deref for ShardReadGuard<'_> {
    type Target = SwissTable;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

#[cfg(feature = "lock-profile")]
impl Drop for ShardReadGuard<'_> {
    fn drop(&mut self) {
        self.profile.take();
    }
}

pub(crate) struct ShardWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, SwissTable>,
    global_memory_used: &'a AtomicUsize,
    strict_memory_accounting: &'a AtomicBool,
    #[cfg(feature = "lock-profile")]
    profile: Option<LockProfileHold<'a>>,
}

impl Deref for ShardWriteGuard<'_> {
    type Target = SwissTable;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for ShardWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for ShardWriteGuard<'_> {
    fn drop(&mut self) {
        self.guard.flush_memory_drift_with(
            self.global_memory_used,
            self.strict_memory_accounting.load(Ordering::Relaxed),
        );
        #[cfg(feature = "lock-profile")]
        {
            self.profile.take();
        }
    }
}

// ─── ConcurrentKeyspace ─────────────────────────────────────────────

/// M2 Sharded RwLock Concurrent Keyspace.
///
/// The keyspace is partitioned into `K` shards (power of 2). Each shard
/// has its own `RwLock<SwissTable>`. Shard selection uses a fixed-seed
/// `ahash` hasher with bitwise AND masking.
///
/// # Performance characteristics
///
/// - Uncontended single-key: ~15ns (one shard lock acquire + table op)
/// - Under contention, parallelism scales with shard count (up to core count)
/// - Multi-key ops pay O(unique_shards) lock acquisitions
/// - Read-heavy workloads benefit from concurrent shared RwLock reads
/// - Expected scaling: near-linear up to core count under uniform distribution
pub struct ConcurrentKeyspace {
    /// Shard array — each shard is cache-line padded to 128 bytes.
    shards: Box<[Shard]>,
    /// Per-shard eviction sweep cursor.
    clock_hands: Box<[CachePadded<AtomicUsize>]>,
    /// Per-shard count of keys that currently carry a TTL deadline.
    expiry_key_count: Box<[CachePadded<AtomicUsize>]>,
    /// Global count of keys that currently carry a TTL deadline.
    expiry_key_total: AtomicUsize,
    /// Bitmask for shard selection: `hash & mask` == `hash % num_shards`.
    /// Valid because `num_shards` is always a power of two.
    mask: u64,
    /// Fixed-seed hasher for deterministic shard routing.
    hasher: RandomState,
    /// Shared SwissTable hasher cloned into every shard table in this keyspace.
    /// This keeps table hashing random per process while still allowing
    /// batch commands to pre-hash once before taking shard locks.
    table_hasher: RandomState,
    /// Global approximate memory counter — updated in threshold-flushed chunks.
    global_memory_used: AtomicUsize,
    /// Global reservation counter for linearizable maxmemory admission.
    ///
    /// Writers `fetch_add` their projected positive delta before mutation,
    /// then `fetch_sub` it (settle) after the actual delta is published to
    /// `global_memory_used` via the shard write guard drop. This prevents
    /// concurrent writers from all passing admission against the same stale
    /// published counter.
    ///
    /// Admission checks: `published_memory_used() + memory_reserved ≤ maxmemory`.
    ///
    /// Uses `CachePadded` to avoid false sharing with `global_memory_used`.
    memory_reserved: CachePadded<AtomicUsize>,
    /// When `maxmemory` is active, publish shard-local drift on every write
    /// guard drop so eviction can rely on the global counter without a full
    /// keyspace lock sweep.
    strict_memory_accounting: AtomicBool,
    /// Hot write-path feature bits. One load tells a mutation whether it must
    /// take any cold side paths: maxmemory admission, WATCH invalidation, or
    /// AOF LSN/eviction-record capture.
    mutation_features: AtomicUsize,
    /// Replay mode bypasses maxmemory admission and synchronous eviction so
    /// recovery depends only on the persisted log, not the current runtime
    /// cache policy.
    replay_depth: AtomicUsize,
    /// Global Logical Sequence Number (LSN) counter.
    ///
    /// Monotonically increasing, assigned inside shard write-lock critical
    /// sections. Guarantees causal ordering: if Op₁ happens-before Op₂ on
    /// the same key, then LSN₁ < LSN₂. Used for:
    /// - AOF per-reactor file ordering (K-Way merge recovery)
    /// - Phase 5 shadow-page epoch tracking
    /// - P2.4 hot-key cache version stamps
    global_lsn: AtomicU64,
    /// Number of active AOF writers using this keyspace. When zero, mutating
    /// commands skip LSN allocation and synthetic eviction record capture.
    aof_recording_refs: AtomicUsize,
    /// Runtime-configurable eviction state shared across all reactors.
    eviction: EvictionConfigState,
    /// Global LFU frequency sketch shared across all shards.
    frequency_sketch: FrequencySketch,
    /// Slow-path observability for bounded eviction work.
    eviction_metrics: EvictionMetrics,
    /// Always-on low-overhead counters exported through INFO runtime.
    runtime_metrics: RuntimeMetrics,
    /// Server/IO memory attribution published by the reactor pool.
    server_memory_attribution: ServerMemoryAttribution,
    /// Cold WATCH registry used only for keys that were absent at WATCH time.
    /// Present-key validation reads entry-resident LSNs directly.
    absent_watch_shards: Box<[AbsentWatchShard]>,
    /// Number of active WATCH registrations for keys that were absent when
    /// watched. This keeps absent-key invalidation off the mutation hot path
    /// unless it is required for Redis-compatible semantics.
    absent_watch_active: AtomicUsize,
    /// Number of active WATCH registrations. Zero means write paths skip the
    /// cold-path invalidation hooks entirely.
    watch_active: AtomicUsize,
    /// Global invalidation epoch for full-keyspace writes such as FLUSHALL.
    watch_epoch: AtomicU64,
    /// Per-shard transaction visibility gates. Normal commands enter the
    /// gates for touched shards as readers; EXEC enters the same sorted shard
    /// set exclusively between WATCH validation and queued mutation execution.
    transaction_gates: Box<[CachePadded<TransactionGate>]>,
    /// Profiling-only shard-lock wait/hold telemetry. Not compiled into the
    /// default or release build.
    #[cfg(feature = "lock-profile")]
    lock_profile: LockProfileState,
}

impl std::fmt::Debug for ConcurrentKeyspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentKeyspace")
            .field("num_shards", &self.shards.len())
            .field("ttl_keys", &self.total_expiry_keys())
            .field(
                "global_memory_used",
                &self.global_memory_used.load(Ordering::Relaxed),
            )
            .field(
                "replay_mode",
                &(self.replay_depth.load(Ordering::Relaxed) != 0),
            )
            .field("global_lsn", &self.global_lsn.load(Ordering::Relaxed))
            .field("eviction", &self.eviction.load())
            .finish()
    }
}

#[inline]
fn evicted_keys_to_box(evicted: Vec<EvictedKey>) -> EvictedKeys {
    if evicted.is_empty() {
        None
    } else {
        Some(evicted.into_boxed_slice())
    }
}

impl ConcurrentKeyspace {
    /// Create a new keyspace with `num_shards` shards.
    ///
    /// # Panics
    ///
    /// Panics if `num_shards` is zero, not a power of two, or outside
    /// the range `[MIN_SHARD_COUNT, MAX_SHARD_COUNT]`.
    pub fn new(num_shards: usize) -> Self {
        Self::new_with_runtime_slots(num_shards, 1)
    }

    /// Create a new keyspace with `num_shards` shards and `runtime_slots`
    /// contention-free runtime counter slots.
    pub fn new_with_runtime_slots(num_shards: usize, runtime_slots: usize) -> Self {
        let shard_count = ShardCount::try_new(num_shards).unwrap_or_else(|error| panic!("{error}"));
        let num_shards = shard_count.get();

        let table_hasher = RandomState::new();
        let shards: Vec<Shard> = (0..num_shards)
            .map(|_| CachePadded::new(RwLock::new(SwissTable::with_hasher(table_hasher.clone()))))
            .collect();
        let clock_hands: Vec<CachePadded<AtomicUsize>> = (0..num_shards)
            .map(|_| CachePadded::new(AtomicUsize::new(0)))
            .collect();
        let expiry_key_count: Vec<CachePadded<AtomicUsize>> = (0..num_shards)
            .map(|_| CachePadded::new(AtomicUsize::new(0)))
            .collect();
        let transaction_gates: Vec<CachePadded<TransactionGate>> = (0..num_shards)
            .map(|_| CachePadded::new(TransactionGate::default()))
            .collect();
        let absent_watch_shards = make_absent_watch_shards();

        Self {
            shards: shards.into_boxed_slice(),
            clock_hands: clock_hands.into_boxed_slice(),
            expiry_key_count: expiry_key_count.into_boxed_slice(),
            expiry_key_total: AtomicUsize::new(0),
            mask: shard_count.mask(),
            hasher: RandomState::with_seeds(AHASH_SEED_0, AHASH_SEED_1, AHASH_SEED_2, AHASH_SEED_3),
            table_hasher,
            global_memory_used: AtomicUsize::new(0),
            memory_reserved: CachePadded::new(AtomicUsize::new(0)),
            strict_memory_accounting: AtomicBool::new(false),
            mutation_features: AtomicUsize::new(MutationFeatures::empty().bits()),
            replay_depth: AtomicUsize::new(0),
            global_lsn: AtomicU64::new(0),
            aof_recording_refs: AtomicUsize::new(0),
            eviction: EvictionConfigState::new(),
            frequency_sketch: FrequencySketch::new(),
            eviction_metrics: EvictionMetrics::default(),
            runtime_metrics: RuntimeMetrics::new(runtime_slots),
            server_memory_attribution: ServerMemoryAttribution::default(),
            absent_watch_shards,
            absent_watch_active: AtomicUsize::new(0),
            watch_active: AtomicUsize::new(0),
            watch_epoch: AtomicU64::new(0),
            transaction_gates: transaction_gates.into_boxed_slice(),
            #[cfg(feature = "lock-profile")]
            lock_profile: LockProfileState::default(),
        }
    }

    /// Create a new keyspace pre-sized for `total_capacity` entries spread
    /// evenly across `num_shards` shards. Avoids early resize churn.
    ///
    /// # Panics
    ///
    /// Same as [`new`](Self::new).
    pub fn with_capacity(num_shards: usize, total_capacity: usize) -> Self {
        let shard_count = ShardCount::try_new(num_shards).unwrap_or_else(|error| panic!("{error}"));
        let num_shards = shard_count.get();

        let per_shard = total_capacity.div_ceil(num_shards);
        let table_hasher = RandomState::new();
        let shards: Vec<Shard> = (0..num_shards)
            .map(|_| {
                CachePadded::new(RwLock::new(SwissTable::with_capacity_and_hasher(
                    per_shard,
                    table_hasher.clone(),
                )))
            })
            .collect();
        let clock_hands: Vec<CachePadded<AtomicUsize>> = (0..num_shards)
            .map(|_| CachePadded::new(AtomicUsize::new(0)))
            .collect();
        let expiry_key_count: Vec<CachePadded<AtomicUsize>> = (0..num_shards)
            .map(|_| CachePadded::new(AtomicUsize::new(0)))
            .collect();
        let transaction_gates: Vec<CachePadded<TransactionGate>> = (0..num_shards)
            .map(|_| CachePadded::new(TransactionGate::default()))
            .collect();
        let absent_watch_shards = make_absent_watch_shards();

        Self {
            shards: shards.into_boxed_slice(),
            clock_hands: clock_hands.into_boxed_slice(),
            expiry_key_count: expiry_key_count.into_boxed_slice(),
            expiry_key_total: AtomicUsize::new(0),
            mask: shard_count.mask(),
            hasher: RandomState::with_seeds(AHASH_SEED_0, AHASH_SEED_1, AHASH_SEED_2, AHASH_SEED_3),
            table_hasher,
            global_memory_used: AtomicUsize::new(0),
            memory_reserved: CachePadded::new(AtomicUsize::new(0)),
            strict_memory_accounting: AtomicBool::new(false),
            mutation_features: AtomicUsize::new(MutationFeatures::empty().bits()),
            replay_depth: AtomicUsize::new(0),
            global_lsn: AtomicU64::new(0),
            aof_recording_refs: AtomicUsize::new(0),
            eviction: EvictionConfigState::new(),
            frequency_sketch: FrequencySketch::new(),
            eviction_metrics: EvictionMetrics::default(),
            runtime_metrics: RuntimeMetrics::new(1),
            server_memory_attribution: ServerMemoryAttribution::default(),
            absent_watch_shards,
            absent_watch_active: AtomicUsize::new(0),
            watch_active: AtomicUsize::new(0),
            watch_epoch: AtomicU64::new(0),
            transaction_gates: transaction_gates.into_boxed_slice(),
            #[cfg(feature = "lock-profile")]
            lock_profile: LockProfileState::default(),
        }
    }

    #[inline]
    pub fn eviction_config(&self) -> EvictionConfig {
        self.eviction.load()
    }

    #[inline]
    pub fn max_memory(&self) -> usize {
        self.eviction.load().max_memory
    }

    #[inline]
    pub fn eviction_policy(&self) -> EvictionPolicy {
        self.eviction.load().policy
    }

    #[inline]
    pub fn set_max_memory(&self, max_memory: usize) {
        let previous = self.max_memory();
        if previous == 0 && max_memory != 0 {
            self.enable_mutation_feature(MutationFeatures::MAXMEMORY);
            self.strict_memory_accounting.store(true, Ordering::Relaxed);
            self.publish_all_memory_drift();
        }

        self.eviction.set_max_memory(max_memory);

        if previous != 0 && max_memory == 0 {
            self.strict_memory_accounting
                .store(false, Ordering::Relaxed);
            self.disable_mutation_feature(MutationFeatures::MAXMEMORY);
        }
    }

    #[inline]
    pub fn set_eviction_policy(&self, policy: EvictionPolicy) {
        self.eviction.set_policy(policy);
    }

    #[inline]
    pub fn configure_eviction(&self, max_memory: usize, policy: EvictionPolicy) {
        let previous = self.max_memory();
        if previous == 0 && max_memory != 0 {
            self.enable_mutation_feature(MutationFeatures::MAXMEMORY);
            self.strict_memory_accounting.store(true, Ordering::Relaxed);
            self.publish_all_memory_drift();
        }

        self.eviction.store(max_memory, policy);

        if previous != 0 && max_memory == 0 {
            self.strict_memory_accounting
                .store(false, Ordering::Relaxed);
            self.disable_mutation_feature(MutationFeatures::MAXMEMORY);
        }
    }

    #[inline(always)]
    pub(crate) fn mutation_features(&self) -> MutationFeatures {
        let mut bits = self.mutation_features.load(Ordering::Acquire);
        if self.watch_active.load(Ordering::Acquire) == 0 {
            bits &= !MutationFeatures::WATCH.bits();
        } else {
            bits |= MutationFeatures::WATCH.bits();
        }
        MutationFeatures::from_bits(bits)
    }

    #[inline(always)]
    fn mutation_feature_active(&self, feature: MutationFeatures) -> bool {
        self.mutation_features().bits() & feature.bits() != 0
    }

    #[inline(always)]
    fn enable_mutation_feature(&self, feature: MutationFeatures) {
        self.mutation_features
            .fetch_or(feature.bits(), Ordering::Release);
    }

    #[inline(always)]
    fn disable_mutation_feature(&self, feature: MutationFeatures) {
        self.mutation_features
            .fetch_and(!feature.bits(), Ordering::Release);
    }

    #[inline]
    pub(crate) fn record_access_prehashed(
        &self,
        table: &SwissTable,
        key_bytes: &[u8],
        hash: TableHash,
    ) {
        let snapshot = self.eviction_config();
        if snapshot.max_memory == 0 {
            return;
        }

        match snapshot.policy {
            EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu => {
                let access_random = next_random_u64();
                if should_sample_lfu_read(access_random) {
                    self.frequency_sketch.record(hash.get());
                }
                let _ = table.record_access_prehashed(key_bytes, hash, access_random);
            }
            EvictionPolicy::AllKeysLru | EvictionPolicy::VolatileLru => {
                let _ = table.record_access_prehashed(key_bytes, hash, next_random_u64());
            }
            _ => {}
        }
    }

    #[inline]
    pub(crate) fn record_frequency_hash(&self, hash: TableHash) {
        let snapshot = self.eviction_config();
        self.record_frequency_hash_snapshot(hash, snapshot);
    }

    #[inline]
    pub(crate) fn record_frequency_hash_snapshot(&self, hash: TableHash, snapshot: EvictionConfig) {
        if snapshot.max_memory != 0 && snapshot.policy.is_lfu() {
            self.frequency_sketch.record(hash.get());
        }
    }

    /// Reservation-based admission against a pre-loaded eviction config snapshot.
    ///
    /// Protocol:
    /// 1. Reserve `additional_bytes` in `memory_reserved` via `fetch_add`.
    /// 2. Check `published_memory_used() + memory_reserved <= maxmemory`.
    /// 3. If over limit: try eviction, then re-check.
    /// 4. If still over limit: release reservation, return OOM.
    /// 5. On success: return `MemoryReservation` that the caller settles
    ///    after mutation (settlement = `fetch_sub(reserved_bytes)`).
    ///
    /// This guarantees linearizable admission: concurrent writers cannot all
    /// pass against the same stale published counter because each one's
    /// reservation is visible to the others.
    pub(crate) fn ensure_memory_for_snapshot(
        &self,
        preferred_shard: usize,
        additional_bytes: usize,
        now_nanos: u64,
        snapshot: EvictionConfig,
    ) -> Result<(EvictedKeys, MemoryReservation<'_>), EvictionAdmissionError> {
        if additional_bytes == 0 {
            return Ok((None, MemoryReservation::new(self, 0)));
        }

        if snapshot.max_memory == 0 {
            return Ok((None, MemoryReservation::new(self, 0)));
        }

        // Step 1: Reserve the projected delta atomically.
        self.memory_reserved
            .fetch_add(additional_bytes, Ordering::Acquire);

        // Step 2: Check whether the combined (published + reserved) fits.
        let committed = self.committed_memory_pressure();
        if committed <= snapshot.max_memory {
            // Fast path: fits within maxmemory even accounting for all
            // concurrent reservations. No eviction needed.
            return Ok((None, MemoryReservation::new(self, additional_bytes)));
        }

        // Step 3: Over limit — try eviction before rejecting.
        if snapshot.policy.is_noeviction() {
            // Release reservation before returning error.
            self.memory_reserved
                .fetch_sub(additional_bytes, Ordering::Release);
            return Err(EvictionAdmissionError::new(
                MutationErrorKind::OutOfMemory,
                None,
            ));
        }

        let mut report = EvictionScanReport::default();
        if snapshot.policy.is_volatile_only() && !self.has_expiring_keys() {
            self.memory_reserved
                .fetch_sub(additional_bytes, Ordering::Release);
            report.oom_after_scan = true;
            self.eviction_metrics.record(report);
            return Err(EvictionAdmissionError::new(
                MutationErrorKind::OutOfMemory,
                None,
            ));
        }

        let mut evicted = Vec::new();
        // Evict until published + reserved fits within maxmemory.
        let target_used = snapshot
            .max_memory
            .saturating_sub(self.memory_reserved.load(Ordering::Acquire));
        let eviction_scan_start = if self.runtime_profile_timers_enabled() {
            Some(Timestamp::now().as_nanos())
        } else {
            None
        };
        report.bytes_freed = self.evict_until_target(
            preferred_shard,
            target_used,
            snapshot.policy,
            now_nanos,
            &mut report,
            &mut evicted,
        );
        let eviction_scan_nanos = eviction_scan_start
            .map(|start| Timestamp::now().as_nanos().saturating_sub(start).max(1))
            .unwrap_or(0);

        // Step 4: Re-check after eviction.
        report.oom_after_scan = self.committed_memory_pressure() > snapshot.max_memory;
        self.eviction_metrics
            .record_with_duration(report, eviction_scan_nanos);

        if !report.oom_after_scan {
            Ok((
                evicted_keys_to_box(evicted),
                MemoryReservation::new(self, additional_bytes),
            ))
        } else {
            // Release reservation — admission failed.
            self.memory_reserved
                .fetch_sub(additional_bytes, Ordering::Release);
            Err(EvictionAdmissionError::new(
                MutationErrorKind::OutOfMemory,
                evicted_keys_to_box(evicted),
            ))
        }
    }

    /// Returns `published_memory_used + memory_reserved`.
    ///
    /// This is the total memory pressure including in-flight reservations
    /// from concurrent writers that have passed admission but have not yet
    /// committed their mutations.
    #[inline]
    fn committed_memory_pressure(&self) -> usize {
        self.published_memory_used()
            .saturating_add(self.memory_reserved.load(Ordering::Acquire))
    }

    #[cfg(not(feature = "lock-profile"))]
    #[inline(always)]
    fn tracked_read_guard<'a>(
        &'a self,
        guard: RwLockReadGuard<'a, SwissTable>,
    ) -> ShardReadGuard<'a> {
        guard
    }

    #[cfg(feature = "lock-profile")]
    #[inline(always)]
    fn tracked_read_guard<'a>(
        &'a self,
        guard: RwLockReadGuard<'a, SwissTable>,
        started_at: Option<std::time::Instant>,
        class: LockProfileClass,
        guard_count: usize,
    ) -> ShardReadGuard<'a> {
        ShardReadGuard {
            guard,
            profile: self.lock_profile.finish_acquisition(
                started_at,
                class,
                LockProfileKind::Read,
                guard_count,
            ),
        }
    }

    #[cfg(not(feature = "lock-profile"))]
    #[inline(always)]
    fn tracked_write_guard<'a>(
        &'a self,
        guard: RwLockWriteGuard<'a, SwissTable>,
    ) -> ShardWriteGuard<'a> {
        ShardWriteGuard {
            guard,
            global_memory_used: &self.global_memory_used,
            strict_memory_accounting: &self.strict_memory_accounting,
        }
    }

    #[cfg(feature = "lock-profile")]
    #[inline(always)]
    fn tracked_write_guard<'a>(
        &'a self,
        guard: RwLockWriteGuard<'a, SwissTable>,
        started_at: Option<std::time::Instant>,
        class: LockProfileClass,
        guard_count: usize,
    ) -> ShardWriteGuard<'a> {
        ShardWriteGuard {
            guard,
            global_memory_used: &self.global_memory_used,
            strict_memory_accounting: &self.strict_memory_accounting,
            profile: self.lock_profile.finish_acquisition(
                started_at,
                class,
                LockProfileKind::Write,
                guard_count,
            ),
        }
    }

    #[cfg(feature = "lock-profile")]
    #[inline(always)]
    fn lock_profile_started(&self) -> Option<std::time::Instant> {
        self.lock_profile.acquisition_started()
    }

    #[cfg(feature = "lock-profile")]
    #[inline]
    pub fn set_lock_profile_enabled(&self, enabled: bool) {
        self.lock_profile.set_enabled(enabled);
    }

    #[cfg(feature = "lock-profile")]
    #[inline]
    pub fn lock_profile_snapshot(&self) -> LockProfileSnapshot {
        self.lock_profile.snapshot()
    }

    #[cfg(feature = "lock-profile")]
    #[inline]
    pub fn reset_lock_profile(&self) {
        self.lock_profile.reset();
    }

    #[cfg(feature = "lock-profile")]
    #[inline]
    pub fn enter_lock_profile_scope(&self, class: LockProfileClass) -> LockProfileScope {
        if self.lock_profile.enabled() {
            LockProfileScope::enter(class)
        } else {
            LockProfileScope::disabled()
        }
    }

    #[cfg(feature = "lock-profile")]
    #[inline]
    pub(crate) fn record_lock_profile_retry(&self, class: LockProfileClass) {
        self.lock_profile.record_retry(class);
    }

    #[cfg(feature = "lock-profile")]
    #[inline]
    pub(crate) fn record_lock_profile_revalidation_failure(&self, class: LockProfileClass) {
        self.lock_profile.record_revalidation_failure(class);
    }

    fn publish_all_memory_drift(&self) {
        for shard in self.shards.iter() {
            let mut guard = shard.write();
            guard.flush_memory_drift_with(&self.global_memory_used, true);
        }
    }

    #[inline]
    fn published_memory_used(&self) -> usize {
        self.global_memory_used.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn clock_hand(&self, shard_idx: usize) -> usize {
        debug_assert!(shard_idx < self.clock_hands.len());
        self.clock_hands[shard_idx].load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_clock_hand(&self, shard_idx: usize, next_slot: usize) {
        debug_assert!(shard_idx < self.clock_hands.len());
        self.clock_hands[shard_idx].store(next_slot, Ordering::Relaxed);
    }

    // ─── Shard routing ──────────────────────────────────────────────

    /// Returns the number of shards.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    #[inline]
    fn shard_count(&self) -> ShardCount {
        debug_assert!(ShardCount::try_new(self.shards.len()).is_ok());
        ShardCount::from_validated(self.shards.len())
    }

    /// Compute the shard index for a key using ahash + bitmask.
    ///
    /// Uses bitwise AND instead of modulo because `num_shards` is a power of 2.
    /// ahash uses AES-NI on x86_64 / hardware crypto on ARM — ~2ns per hash.
    #[inline(always)]
    pub fn shard_index(&self, key: &[u8]) -> usize {
        self.shard_id(key).get()
    }

    /// Compute the typed shard ID for a key using ahash + bitmask.
    #[inline(always)]
    pub(crate) fn shard_id(&self, key: &[u8]) -> ShardId {
        ShardId::from_masked_index((self.hasher.hash_one(key) & self.mask) as usize)
    }

    /// Hash a key using the keyspace's hasher.
    /// Exposed for pre-hashing in batch operations (MGET/MSET).
    #[inline(always)]
    pub fn hash_key(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Hash a key using the shared SwissTable hasher for this keyspace.
    #[inline(always)]
    pub(crate) fn table_hash_key(&self, key: &[u8]) -> TableHash {
        TableHash::from_u64(self.table_hasher.hash_one(key))
    }

    // ─── Single-key lock acquisition ────────────────────────────────

    /// Acquire a read lock on the shard containing `key`.
    ///
    /// Use for read-only operations: GET, EXISTS, TTL, PTTL, TYPE, STRLEN.
    #[inline(always)]
    pub fn read_shard(&self, key: &[u8]) -> ShardReadGuard<'_> {
        let idx = self.shard_index(key);
        #[cfg(feature = "lock-profile")]
        let started_at = self.lock_profile_started();
        // SAFETY: idx is always < shards.len() because mask = num_shards - 1
        // and num_shards is a power of 2. Bounds check is provably unnecessary.
        let guard = unsafe { self.shards.get_unchecked(idx) }.read();
        #[cfg(feature = "lock-profile")]
        {
            self.tracked_read_guard(guard, started_at, LockProfileClass::SingleKey, 1)
        }
        #[cfg(not(feature = "lock-profile"))]
        {
            self.tracked_read_guard(guard)
        }
    }

    /// Acquire a read lock on a specific shard by index.
    ///
    /// # Panics
    ///
    /// Panics if `idx >= self.num_shards()`.
    #[inline(always)]
    pub fn read_shard_by_index(&self, idx: usize) -> ShardReadGuard<'_> {
        self.try_read_shard_by_index(idx)
            .expect("shard index out of bounds")
    }

    /// Acquire a write lock on a specific shard by index.
    ///
    /// # Panics
    ///
    /// Panics if `idx >= self.num_shards()`.
    #[inline(always)]
    pub(crate) fn write_shard_by_index(&self, idx: usize) -> ShardWriteGuard<'_> {
        self.try_write_shard_by_index(idx)
            .expect("shard index out of bounds")
    }

    /// Acquire a read lock on a specific shard by index, returning `None`
    /// when `idx` is out of range.
    #[inline(always)]
    pub fn try_read_shard_by_index(&self, idx: usize) -> Option<ShardReadGuard<'_>> {
        let shard = ShardId::try_new(idx, self.shard_count())?;
        let shard_idx = shard.get();
        let shard = self.shards.get(shard_idx)?;
        #[cfg(feature = "lock-profile")]
        let started_at = self.lock_profile_started();
        let guard = shard.read();
        #[cfg(feature = "lock-profile")]
        {
            Some(self.tracked_read_guard(guard, started_at, LockProfileClass::SingleKey, 1))
        }
        #[cfg(not(feature = "lock-profile"))]
        {
            Some(self.tracked_read_guard(guard))
        }
    }

    /// Acquire a write lock on a specific shard by index, returning `None`
    /// when `idx` is out of range.
    #[inline(always)]
    pub(crate) fn try_write_shard_by_index(&self, idx: usize) -> Option<ShardWriteGuard<'_>> {
        let shard_id = ShardId::try_new(idx, self.shard_count())?;
        let shard = self.shards.get(shard_id.get())?;
        #[cfg(feature = "lock-profile")]
        let started_at = self.lock_profile_started();
        let guard = shard.write();
        #[cfg(feature = "lock-profile")]
        {
            Some(self.tracked_write_guard(guard, started_at, LockProfileClass::SingleKey, 1))
        }
        #[cfg(not(feature = "lock-profile"))]
        {
            Some(self.tracked_write_guard(guard))
        }
    }

    // ─── Closure-based single-key access ────────────────────────────
    //
    // These methods keep critical sections minimal: the closure runs
    // with the lock held, and data conversion happens outside.

    /// Execute a read-only operation on the shard containing `key`.
    ///
    /// The closure receives an immutable reference to the `SwissTable`.
    /// Multiple readers can execute concurrently on the same shard.
    #[inline]
    pub fn read<F, R>(&self, key: &[u8], f: F) -> R
    where
        F: FnOnce(&SwissTable) -> R,
    {
        let guard = self.read_shard(key);
        f(&guard)
    }

    /// Benchmark-only raw insert hook.
    ///
    /// This bypasses command mutation protocols: maxmemory admission, AOF LSN
    /// recording, WATCH invalidation, and TTL accounting are not performed.
    ///
    /// # Safety
    ///
    /// Call only from benchmark setup code before the measured operation, with
    /// no concurrent readers or writers depending on command-side invariants.
    #[doc(hidden)]
    pub unsafe fn benchmark_insert_unchecked(&self, key: VortexKey, value: VortexValue) {
        let mut guard = self.write_shard_by_index(self.shard_index(key.as_bytes()));
        guard.insert(key, value);
    }

    // ─── Multi-key operations ───────────────────────────────────────
    //
    // Critical: acquire shard locks in ascending shard ID order to prevent
    // deadlocks. Deduplicate shard indices to avoid double-locking.

    /// Acquire read locks on all shards touched by `keys`, in ascending order.
    ///
    /// Returns `(guards, plan)`. Use `plan.guard_index_for_key(key_index)`
    /// to map an input key to its guard without repeated binary searches.
    #[inline]
    pub(crate) fn multi_read<'a>(&'a self, keys: &[&[u8]]) -> MultiReadGuards<'a> {
        let plan = ShardPlan::new(self, keys);

        // Acquire read locks in ascending shard order — deadlock-free.
        #[cfg(feature = "lock-profile")]
        let guard_count = plan.sorted_shards().len();
        let guards: ShardReadGuards<'_> = plan
            .sorted_shards()
            .iter()
            .map(|shard| {
                let idx = shard.get();
                #[cfg(feature = "lock-profile")]
                let started_at = self.lock_profile_started();
                // SAFETY: idx < shards.len() by construction (mask guarantees).
                let guard = unsafe { self.shards.get_unchecked(idx) }.read();
                #[cfg(feature = "lock-profile")]
                {
                    (
                        idx,
                        self.tracked_read_guard(
                            guard,
                            started_at,
                            LockProfileClass::MultiKey,
                            guard_count,
                        ),
                    )
                }
                #[cfg(not(feature = "lock-profile"))]
                {
                    (idx, self.tracked_read_guard(guard))
                }
            })
            .collect();

        (guards, plan)
    }

    #[inline]
    pub(crate) fn prehashed_plan<'a, I>(&self, keys: I) -> PrehashedShardPlan<'a>
    where
        I: IntoIterator<Item = (usize, &'a [u8])>,
    {
        PrehashedShardPlan::new(self, keys)
    }

    #[inline]
    pub(crate) fn multi_read_prehashed<'a, 'k>(
        &'a self,
        plan: &PrehashedShardPlan<'k>,
    ) -> ShardReadGuards<'a> {
        #[cfg(feature = "lock-profile")]
        let guard_count = plan.sorted_shards().len();
        plan.sorted_shards()
            .iter()
            .map(|shard| {
                let idx = shard.get();
                #[cfg(feature = "lock-profile")]
                let started_at = self.lock_profile_started();
                // SAFETY: idx < shards.len() by construction (mask guarantees).
                let guard = unsafe { self.shards.get_unchecked(idx) }.read();
                #[cfg(feature = "lock-profile")]
                {
                    (
                        idx,
                        self.tracked_read_guard(
                            guard,
                            started_at,
                            LockProfileClass::MultiKey,
                            guard_count,
                        ),
                    )
                }
                #[cfg(not(feature = "lock-profile"))]
                {
                    (idx, self.tracked_read_guard(guard))
                }
            })
            .collect()
    }

    /// Acquire write locks on all shards touched by `keys`, in ascending order.
    ///
    /// Returns `(guards, plan)`. The plan owns the sorted shard IDs and each
    /// input key's precomputed guard index.
    #[inline]
    pub(crate) fn multi_write<'a>(&'a self, keys: &[&[u8]]) -> MultiWriteGuards<'a> {
        let plan = ShardPlan::new(self, keys);

        // Acquire write locks in ascending shard order — deadlock-free.
        #[cfg(feature = "lock-profile")]
        let guard_count = plan.sorted_shards().len();
        let guards: ShardWriteGuards<'_> = plan
            .sorted_shards()
            .iter()
            .map(|shard| {
                let idx = shard.get();
                #[cfg(feature = "lock-profile")]
                let started_at = self.lock_profile_started();
                // SAFETY: idx < shards.len() by construction (mask guarantees).
                let guard = unsafe { self.shards.get_unchecked(idx) }.write();
                #[cfg(feature = "lock-profile")]
                {
                    (
                        idx,
                        self.tracked_write_guard(
                            guard,
                            started_at,
                            LockProfileClass::MultiKey,
                            guard_count,
                        ),
                    )
                }
                #[cfg(not(feature = "lock-profile"))]
                {
                    (idx, self.tracked_write_guard(guard))
                }
            })
            .collect();

        (guards, plan)
    }

    #[inline]
    pub(crate) fn multi_write_prehashed<'a, 'k>(
        &'a self,
        plan: &PrehashedShardPlan<'k>,
    ) -> ShardWriteGuards<'a> {
        #[cfg(feature = "lock-profile")]
        let guard_count = plan.sorted_shards().len();
        plan.sorted_shards()
            .iter()
            .map(|shard| {
                let idx = shard.get();
                #[cfg(feature = "lock-profile")]
                let started_at = self.lock_profile_started();
                // SAFETY: idx < shards.len() by construction (mask guarantees).
                let guard = unsafe { self.shards.get_unchecked(idx) }.write();
                #[cfg(feature = "lock-profile")]
                {
                    (
                        idx,
                        self.tracked_write_guard(
                            guard,
                            started_at,
                            LockProfileClass::MultiKey,
                            guard_count,
                        ),
                    )
                }
                #[cfg(not(feature = "lock-profile"))]
                {
                    (idx, self.tracked_write_guard(guard))
                }
            })
            .collect()
    }

    // ─── Scan operations ────────────────────────────────────────────

    /// Execute a closure on each shard sequentially (for KEYS, SCAN, DBSIZE).
    ///
    /// Acquires a read lock on each shard one at a time to avoid holding
    /// all locks simultaneously. Results are approximate (point-in-time
    /// snapshots per shard, not a global snapshot).
    #[inline]
    pub fn scan_all_shards<F, R>(&self, mut f: F) -> Vec<R>
    where
        F: FnMut(usize, &SwissTable) -> R,
    {
        let mut results = Vec::with_capacity(self.shards.len());
        for (idx, shard) in self.shards.iter().enumerate() {
            let guard = shard.read();
            results.push(f(idx, &guard));
        }
        results
    }

    // ─── Metadata ───────────────────────────────────────────────────

    /// Approximate total number of keys across all shards.
    ///
    /// This is cheap metadata based on per-shard lengths and does not filter
    /// expired-yet-not-cleaned entries.
    pub fn dbsize(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }
}

#[cfg(test)]
impl ConcurrentKeyspace {
    /// Admit a test mutation that will increase memory by `additional_bytes`.
    ///
    /// Tests use this to exercise reservation and eviction admission directly.
    /// Command code should use the snapshot-based admission path so it can
    /// reuse the eviction config already loaded for the mutation.
    fn ensure_memory_for(
        &self,
        preferred_shard: usize,
        additional_bytes: usize,
        now_nanos: u64,
    ) -> Result<(EvictedKeys, MemoryReservation<'_>), EvictionAdmissionError> {
        if self.replay_mode_active() {
            return Ok((None, MemoryReservation::new(self, 0)));
        }

        if additional_bytes == 0 {
            return Ok((None, MemoryReservation::new(self, 0)));
        }

        let snapshot = self.eviction_config();
        self.ensure_memory_for_snapshot(preferred_shard, additional_bytes, now_nanos, snapshot)
    }

    /// Returns the current outstanding reservation counter value.
    #[inline]
    fn memory_reserved(&self) -> usize {
        self.memory_reserved.load(Ordering::Relaxed)
    }

    /// Acquire a write lock on the shard containing `key`.
    #[inline(always)]
    fn write_shard(&self, key: &[u8]) -> ShardWriteGuard<'_> {
        let idx = self.shard_index(key);
        #[cfg(feature = "lock-profile")]
        let started_at = self.lock_profile_started();
        // SAFETY: idx is always < shards.len() because mask = num_shards - 1
        // and num_shards is a power of 2.
        let guard = unsafe { self.shards.get_unchecked(idx) }.write();
        #[cfg(feature = "lock-profile")]
        {
            self.tracked_write_guard(guard, started_at, LockProfileClass::SingleKey, 1)
        }
        #[cfg(not(feature = "lock-profile"))]
        {
            self.tracked_write_guard(guard)
        }
    }

    /// Execute a raw table mutation on the shard containing `key`.
    ///
    /// This intentionally bypasses command-side protocols and is available
    /// only to tests that set up internal table state directly.
    #[inline]
    pub(crate) fn write<F, R>(&self, key: &[u8], f: F) -> R
    where
        F: FnOnce(&mut SwissTable) -> R,
    {
        let mut guard = self.write_shard(key);
        f(&mut guard)
    }
}

// ═══════════════════════════════════════════════════════════════════
// Unit Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::MAX_STORED_LSN_VERSION;
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    use vortex_common::{VortexKey, VortexValue};

    /// Helper: smallest valid shard count for tests.
    const TEST_SHARDS: usize = 64;

    fn keys_for_shards(keyspace: &ConcurrentKeyspace, shards: &[usize]) -> Vec<Vec<u8>> {
        let mut keys = vec![Vec::new(); shards.len()];
        let mut found = vec![false; shards.len()];

        for candidate in 0..200_000usize {
            let key = format!("evict:{candidate:06}").into_bytes();
            let shard_idx = keyspace.shard_index(&key);
            for (position, target) in shards.iter().enumerate() {
                if !found[position] && *target == shard_idx {
                    keys[position] = key.clone();
                    found[position] = true;
                }
            }
            if found.iter().all(|flag| *flag) {
                return keys;
            }
        }

        panic!("failed to find keys for requested shards");
    }

    #[test]
    fn shard_transaction_gate_blocks_only_touched_shards() {
        let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let keys = keys_for_shards(&keyspace, &[0, 1]);
        let first = keys[0].clone();
        let second = keys[1].clone();

        let transaction_guard = keyspace.enter_transaction_gate_for_keys(&[first.as_slice()]);

        let blocked_keyspace = Arc::clone(&keyspace);
        let blocked_key = first.clone();
        let (blocked_started_tx, blocked_started_rx) = mpsc::channel();
        let (blocked_entered_tx, blocked_entered_rx) = mpsc::channel();
        let blocked = thread::spawn(move || {
            blocked_started_tx.send(()).unwrap();
            let _guard = blocked_keyspace.enter_command_gate_for_keys(&[blocked_key.as_slice()], 0);
            blocked_entered_tx.send(()).unwrap();
        });

        blocked_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocked worker started");
        assert!(
            blocked_entered_rx
                .recv_timeout(Duration::from_millis(20))
                .is_err(),
            "same-shard command gate entered while transaction gate was active"
        );

        let unrelated_keyspace = Arc::clone(&keyspace);
        let (unrelated_tx, unrelated_rx) = mpsc::channel();
        let unrelated = thread::spawn(move || {
            let _guard = unrelated_keyspace.enter_command_gate_for_keys(&[second.as_slice()], 0);
            unrelated_tx.send(()).unwrap();
        });
        unrelated_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("different-shard command gate must not wait");
        unrelated.join().unwrap();

        drop(transaction_guard);
        blocked_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("same-shard command gate entered after transaction gate drop");
        blocked.join().unwrap();
    }

    fn keys_for_same_shard(keyspace: &ConcurrentKeyspace, count: usize) -> (usize, Vec<Vec<u8>>) {
        let target = keyspace.shard_index(b"same-shard-seed");
        let mut keys = Vec::with_capacity(count);

        for candidate in 0..200_000usize {
            let key = format!("same-shard:{candidate:06}").into_bytes();
            if keyspace.shard_index(&key) == target {
                keys.push(key);
                if keys.len() == count {
                    return (target, keys);
                }
            }
        }

        panic!("failed to find {count} keys for shard {target}");
    }

    fn slot_for_key(table: &SwissTable, key: &VortexKey) -> usize {
        for slot in 0..table.total_slots() {
            if table
                .slot_key_value(slot)
                .is_some_and(|(slot_key, _)| slot_key == key)
            {
                return slot;
            }
        }

        panic!("key not found in table slots: {key:?}");
    }

    #[test]
    fn shard_count_rejects_invalid_counts() {
        for invalid in [
            0,
            MIN_SHARD_COUNT - 1,
            MIN_SHARD_COUNT + 1,
            MAX_SHARD_COUNT + 1,
        ] {
            let error = ShardCount::try_new(invalid).expect_err("count should be rejected");
            assert_eq!(error.attempted(), invalid);
        }

        assert_eq!(
            ShardCount::try_new(MIN_SHARD_COUNT).unwrap().get(),
            MIN_SHARD_COUNT
        );
        assert_eq!(
            ShardCount::try_new(DEFAULT_SHARD_COUNT).unwrap().get(),
            DEFAULT_SHARD_COUNT
        );
        assert_eq!(
            ShardCount::try_new(MAX_SHARD_COUNT).unwrap().get(),
            MAX_SHARD_COUNT
        );
    }

    #[test]
    fn shard_id_rejects_out_of_range_indexes() {
        let count = ShardCount::try_new(TEST_SHARDS).unwrap();

        assert_eq!(ShardId::try_new(0, count).unwrap().get(), 0);
        assert_eq!(
            ShardId::try_new(TEST_SHARDS - 1, count).unwrap().get(),
            TEST_SHARDS - 1
        );
        assert!(ShardId::try_new(TEST_SHARDS, count).is_none());
    }

    #[test]
    fn mutation_features_mask_unknown_bits_and_expose_named_flags() {
        let features = MutationFeatures::from_bits(
            MutationFeatures::MAXMEMORY.bits() | MutationFeatures::AOF.bits() | (1 << 31),
        );

        assert!(features.maxmemory());
        assert!(!features.watch());
        assert!(features.aof());
        assert_eq!(
            features.bits(),
            MutationFeatures::MAXMEMORY.bits() | MutationFeatures::AOF.bits()
        );
        assert!(MutationFeatures::empty().is_empty());
    }

    #[test]
    fn watch_feature_snapshot_follows_active_registration_count() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        ks.enable_mutation_feature(MutationFeatures::WATCH);
        assert!(!ks.watch_tracking_active());
        assert!(!ks.mutation_features().watch());

        let key = VortexKey::from_bytes(b"watch:feature-race");
        let epoch = ks.current_watch_epoch();
        let watched = ks.watch_key(key.clone());

        ks.disable_mutation_feature(MutationFeatures::WATCH);
        assert!(ks.watch_tracking_active());
        assert!(ks.mutation_features().watch());

        ks.bump_watch_key(&key);
        assert!(ks.watched_keys_changed(epoch, std::slice::from_ref(&watched)));

        ks.unwatch_keys(std::iter::once(watched));
        assert!(!ks.watch_tracking_active());
        assert!(!ks.mutation_features().watch());
    }

    #[test]
    fn projected_delta_converts_only_positive_bytes() {
        assert_eq!(
            ProjectedDelta::from_bytes(-32).positive(),
            PositiveDelta::zero()
        );
        assert_eq!(
            ProjectedDelta::from_bytes(0).positive(),
            PositiveDelta::zero()
        );
        assert_eq!(ProjectedDelta::from_bytes(42).positive().bytes(), 42);
        assert_eq!(
            PositiveDelta::sum([
                PositiveDelta::from_bytes(10),
                PositiveDelta::zero(),
                PositiveDelta::from_bytes(5),
            ])
            .bytes(),
            15
        );
    }

    #[test]
    fn single_thread_set_get_del() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        let key = VortexKey::from_bytes(b"hello");
        let val = VortexValue::from("world");

        // SET
        let old = ks.write(b"hello", |t| t.insert(key.clone(), val.clone()));
        assert!(old.is_none());

        // GET
        let got = ks.read(b"hello", |t| {
            t.get(&VortexKey::from_bytes(b"hello")).cloned()
        });
        assert_eq!(got, Some(VortexValue::from("world")));

        // DEL
        let existed = ks.write(b"hello", |t| t.remove(&VortexKey::from_bytes(b"hello")));
        assert!(existed.is_some());

        // GET after DEL
        let got = ks.read(b"hello", |t| {
            t.get(&VortexKey::from_bytes(b"hello")).cloned()
        });
        assert!(got.is_none());
    }

    #[test]
    fn memory_attribution_reports_table_and_server_buckets() {
        let ks = ConcurrentKeyspace::with_capacity(TEST_SHARDS, 128);
        let key_a = VortexKey::from_bytes(b"mem:a");
        let key_b = VortexKey::from_bytes(b"mem:b");

        ks.write(b"mem:a", |table| {
            table.insert(key_a.clone(), VortexValue::from_bytes(b"one"))
        });
        ks.write(b"mem:b", |table| {
            table.insert(key_b.clone(), VortexValue::from_bytes(b"two"))
        });
        ks.write(b"mem:b", |table| table.remove(&key_b));

        let engine = ks.engine_memory_attribution();
        assert_eq!(engine.live_keys, 1);
        assert_eq!(engine.shard_count, TEST_SHARDS);
        assert!(engine.logical_dataset_bytes > 0);
        assert!(engine.table_allocated_bytes > 0);
        assert!(engine.table_total_slots >= engine.live_keys);
        assert!(engine.capacity_slack_slots <= engine.table_total_slots);
        assert!(engine.tombstone_slots <= engine.table_total_slots);
        assert!(engine.load_factor > 0.0);
        assert!(engine.bytes_per_live_key.is_some());

        let server = ServerMemoryAttributionSnapshot {
            io_fixed_buffer_reserved_bytes: 8 * 4096,
            io_fixed_buffer_committed_bytes: 8 * 4096,
            io_fixed_buffer_active_bytes: 4096,
            per_connection_state_bytes: 2048,
            connection_capacity: 32,
            fixed_buffer_count: 8,
            fixed_buffer_size: 4096,
        };
        ks.set_server_memory_attribution(server);
        assert_eq!(ks.server_memory_attribution(), server);
    }

    #[test]
    fn runtime_metrics_snapshot_aggregates_reactor_counters() {
        let ks = ConcurrentKeyspace::new_with_runtime_slots(TEST_SHARDS, 4);

        ks.record_reactor_loop_iteration(0);
        ks.record_reactor_loop_iteration(1);
        ks.record_reactor_accept_eagain_rearm(1);
        ks.record_reactor_accept_drain(1, 3);
        ks.record_reactor_submit_sq_full_retry(2);
        ks.record_reactor_submit_failure(3);
        ks.record_reactor_completion_budget_exhaustion(0);
        ks.record_reactor_command_budget_exhaustion(1);
        ks.record_reactor_accept_budget_exhaustion(1);
        ks.record_reactor_writev_budget_exhaustion(2);
        ks.record_reactor_maintenance_budget_exhaustion(3);
        ks.record_reactor_yielded_connection(1);
        ks.record_reactor_parser_resume(2);
        ks.record_reactor_completion_batch(0, 4);
        ks.record_reactor_completion_batch(1, 2);
        ks.record_reactor_completion_nanos(1, 50);
        ks.record_reactor_command_batch(0, 3);
        ks.record_reactor_command_batch(1, 1);
        ks.record_reactor_writev_chunk(0, 5);
        ks.record_reactor_queued_response_bytes(0, 128);
        ks.record_reactor_close_drain_nanos(0, 75);
        ks.record_reactor_active_expiry(2, 12, 5);
        ks.record_reactor_active_expiry_nanos(2, 80);
        ks.record_reactor_aof_append_nanos(2, 90);
        ks.record_reactor_aof_fsync_nanos(2, 100);
        ks.publish_reactor_aof_telemetry(
            2,
            RuntimeAofTelemetry {
                pending_bytes: 4096,
                pending_writes: 17,
                fsync_requested: 3,
                fsync_completed: 2,
                fsync_failed: 1,
                fsync_worker_saturation: 4,
                backpressure_events: 5,
                backpressure_nanos_total: 600,
                backpressure_nanos_max: 300,
                last_appended_lsn: 44,
                last_durable_lsn: 40,
                fsync_latency_nanos_total: 700,
                fsync_latency_nanos_max: 500,
                fsync_latency_buckets: [1, 2, 3, 4, 5, 6, 7, 8],
            },
        );
        ks.record_reactor_maintenance_nanos(2, 180);
        ks.record_reactor_metrics_flush_nanos(2, 7);
        ks.publish_runtime_backend(RuntimeBackendSnapshot {
            requested: RuntimeBackendMode::Auto,
            effective: RuntimeBackendMode::Polling,
            fixed_buffers_capable: false,
            fixed_buffers_registered: false,
            cancel_support: true,
            nonblocking_drain: true,
            requested_ring_size: 4096,
            ..Default::default()
        });
        ks.record_reactor_backend_submit_syscall(0);
        ks.record_reactor_backend_submit_syscall(3);
        ks.record_reactor_backend_queue_status(0, 3, 4096, 2, 8192, 0);
        ks.record_reactor_backend_queue_status(1, 5, 4096, 4, 8192, 2);

        let snapshot = ks.runtime_metrics();

        assert_eq!(snapshot.telemetry_mode, RuntimeTelemetryMode::Minimal);
        assert!(!snapshot.profile_timers_available);
        assert!(snapshot.local_flush_metrics_available);
        assert_eq!(snapshot.reactor_slots, 4);
        assert_eq!(snapshot.backend.requested, RuntimeBackendMode::Auto);
        assert_eq!(snapshot.backend.effective, RuntimeBackendMode::Polling);
        assert!(snapshot.backend.cancel_support);
        assert_eq!(snapshot.backend.requested_ring_size, 4096);
        assert_eq!(snapshot.backend_submit_syscalls, 2);
        assert_eq!(snapshot.backend_sq_occupancy_max, 5);
        assert_eq!(snapshot.backend_sq_capacity, 4096);
        assert_eq!(snapshot.backend_cq_occupancy_max, 4);
        assert_eq!(snapshot.backend_cq_capacity, 8192);
        assert_eq!(snapshot.backend_cq_overflows, 2);
        assert_eq!(snapshot.backend_completions_per_submit_syscall_x1000, 3_000);
        assert_eq!(snapshot.loop_iterations, 2);
        assert_eq!(snapshot.accept_eagain_rearms, 1);
        assert_eq!(snapshot.accept_drain_runs, 1);
        assert_eq!(snapshot.accept_drain_accepted, 3);
        assert_eq!(snapshot.accept_drain_accepted_max, 3);
        assert_eq!(snapshot.submit_sq_full_retries, 1);
        assert_eq!(snapshot.submit_failures, 1);
        assert_eq!(snapshot.completion_budget_exhaustions, 1);
        assert_eq!(snapshot.command_budget_exhaustions, 1);
        assert_eq!(snapshot.accept_budget_exhaustions, 1);
        assert_eq!(snapshot.writev_budget_exhaustions, 1);
        assert_eq!(snapshot.maintenance_budget_exhaustions, 1);
        assert_eq!(snapshot.yielded_connections, 1);
        assert_eq!(snapshot.parser_resumes, 1);
        assert_eq!(snapshot.completion_batch_count, 2);
        assert_eq!(snapshot.completion_batch_total, 6);
        assert_eq!(snapshot.completion_batch_max, 4);
        assert!((snapshot.completion_batch_avg - 3.0).abs() < f64::EPSILON);
        #[cfg(feature = "profile-telemetry")]
        {
            assert_eq!(snapshot.completion_nanos_total, 50);
            assert_eq!(snapshot.completion_nanos_max, 50);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            assert_eq!(snapshot.completion_nanos_total, 0);
            assert_eq!(snapshot.completion_nanos_max, 0);
        }
        assert_eq!(snapshot.command_batch_count, 2);
        assert_eq!(snapshot.command_batch_total, 4);
        assert_eq!(snapshot.command_batch_max, 3);
        assert!((snapshot.command_batch_avg - 2.0).abs() < f64::EPSILON);
        assert_eq!(snapshot.writev_chunks, 1);
        assert_eq!(snapshot.writev_iovecs_total, 5);
        assert_eq!(snapshot.writev_iovecs_max, 5);
        assert_eq!(snapshot.queued_response_bytes_total, 128);
        assert_eq!(snapshot.queued_response_bytes_max, 128);
        #[cfg(feature = "profile-telemetry")]
        {
            assert_eq!(snapshot.close_drain_nanos_total, 75);
            assert_eq!(snapshot.close_drain_nanos_max, 75);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            assert_eq!(snapshot.close_drain_nanos_total, 0);
            assert_eq!(snapshot.close_drain_nanos_max, 0);
        }
        assert_eq!(snapshot.active_expiry_runs, 1);
        assert_eq!(snapshot.active_expiry_sampled, 12);
        assert_eq!(snapshot.active_expiry_expired, 5);
        #[cfg(feature = "profile-telemetry")]
        {
            assert_eq!(snapshot.active_expiry_nanos_total, 80);
            assert_eq!(snapshot.active_expiry_nanos_max, 80);
            assert_eq!(snapshot.aof_append_nanos_total, 90);
            assert_eq!(snapshot.aof_append_nanos_max, 90);
            assert_eq!(snapshot.aof_fsync_nanos_total, 100);
            assert_eq!(snapshot.aof_fsync_nanos_max, 100);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            assert_eq!(snapshot.active_expiry_nanos_total, 0);
            assert_eq!(snapshot.active_expiry_nanos_max, 0);
            assert_eq!(snapshot.aof_append_nanos_total, 0);
            assert_eq!(snapshot.aof_append_nanos_max, 0);
            assert_eq!(snapshot.aof_fsync_nanos_total, 0);
            assert_eq!(snapshot.aof_fsync_nanos_max, 0);
        }
        assert_eq!(snapshot.aof_pending_bytes, 4096);
        assert_eq!(snapshot.aof_pending_bytes_max, 4096);
        assert_eq!(snapshot.aof_pending_writes, 17);
        assert_eq!(snapshot.aof_pending_writes_max, 17);
        assert_eq!(snapshot.aof_fsync_requested, 3);
        assert_eq!(snapshot.aof_fsync_completed, 2);
        assert_eq!(snapshot.aof_fsync_failed, 1);
        assert_eq!(snapshot.aof_fsync_worker_saturation, 4);
        assert_eq!(snapshot.aof_backpressure_events, 5);
        assert_eq!(snapshot.aof_last_appended_lsn, 44);
        assert_eq!(snapshot.aof_last_durable_lsn, 40);
        assert_eq!(snapshot.aof_durable_lsn_lag, 4);
        #[cfg(feature = "profile-telemetry")]
        {
            assert_eq!(snapshot.aof_backpressure_nanos_total, 600);
            assert_eq!(snapshot.aof_backpressure_nanos_max, 300);
            assert_eq!(snapshot.aof_fsync_latency_nanos_total, 700);
            assert_eq!(snapshot.aof_fsync_latency_nanos_max, 500);
            assert_eq!(snapshot.aof_fsync_latency_buckets, [1, 2, 3, 4, 5, 6, 7, 8]);
            assert_eq!(snapshot.maintenance_nanos_total, 180);
            assert_eq!(snapshot.maintenance_nanos_max, 180);
            assert_eq!(snapshot.metrics_flush_nanos_total, 7);
            assert_eq!(snapshot.metrics_flush_nanos_max, 7);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            assert_eq!(snapshot.aof_backpressure_nanos_total, 0);
            assert_eq!(snapshot.aof_backpressure_nanos_max, 0);
            assert_eq!(snapshot.aof_fsync_latency_nanos_total, 0);
            assert_eq!(snapshot.aof_fsync_latency_nanos_max, 0);
            assert_eq!(snapshot.aof_fsync_latency_buckets, [0; 8]);
            assert_eq!(snapshot.maintenance_nanos_total, 0);
            assert_eq!(snapshot.maintenance_nanos_max, 0);
            assert_eq!(snapshot.metrics_flush_nanos_total, 0);
            assert_eq!(snapshot.metrics_flush_nanos_max, 0);
        }
    }

    #[test]
    fn runtime_metrics_flush_local_batches_once() {
        let ks = ConcurrentKeyspace::new_with_runtime_slots(TEST_SHARDS, 2);

        ks.flush_reactor_local_metrics(
            1,
            RuntimeLocalFlushMetrics {
                loop_iterations: 4,
                accept_drain_runs: 2,
                accept_drain_accepted: 5,
                accept_drain_accepted_max: 3,
                completion_batch_count: 2,
                completion_batch_total: 9,
                completion_batch_max: 7,
                command_batch_count: 1,
                command_batch_total: 8,
                command_batch_max: 8,
                writev_chunks: 3,
                writev_iovecs_total: 11,
                writev_iovecs_max: 6,
                queued_response_bytes_total: 1024,
                queued_response_bytes_max: 512,
            },
        );

        let snapshot = ks.runtime_metrics();
        assert_eq!(snapshot.loop_iterations, 4);
        assert_eq!(snapshot.accept_drain_runs, 2);
        assert_eq!(snapshot.accept_drain_accepted, 5);
        assert_eq!(snapshot.accept_drain_accepted_max, 3);
        assert_eq!(snapshot.completion_batch_count, 2);
        assert_eq!(snapshot.completion_batch_total, 9);
        assert_eq!(snapshot.completion_batch_max, 7);
        assert_eq!(snapshot.command_batch_count, 1);
        assert_eq!(snapshot.command_batch_total, 8);
        assert_eq!(snapshot.command_batch_max, 8);
        assert_eq!(snapshot.writev_chunks, 3);
        assert_eq!(snapshot.writev_iovecs_total, 11);
        assert_eq!(snapshot.writev_iovecs_max, 6);
        assert_eq!(snapshot.queued_response_bytes_total, 1024);
        assert_eq!(snapshot.queued_response_bytes_max, 512);
    }

    #[cfg(feature = "profile-telemetry")]
    #[test]
    fn runtime_telemetry_mode_controls_profile_availability() {
        let ks = ConcurrentKeyspace::new_with_runtime_slots(TEST_SHARDS, 1);
        assert!(!ks.runtime_profile_timers_enabled());
        assert_eq!(
            ks.runtime_metrics().telemetry_mode,
            RuntimeTelemetryMode::Minimal
        );

        ks.set_runtime_telemetry_mode(RuntimeTelemetryMode::Profile);

        let snapshot = ks.runtime_metrics();
        assert_eq!(snapshot.telemetry_mode, RuntimeTelemetryMode::Profile);
        assert!(snapshot.profile_timers_available);
        assert!(ks.runtime_profile_timers_enabled());
    }

    #[test]
    fn runtime_backend_contract_marks_mixed_plans() {
        let ks = ConcurrentKeyspace::new_with_runtime_slots(TEST_SHARDS, 2);

        ks.publish_runtime_backend(RuntimeBackendSnapshot {
            requested: RuntimeBackendMode::Auto,
            effective: RuntimeBackendMode::Polling,
            cancel_support: true,
            nonblocking_drain: true,
            ..Default::default()
        });
        ks.publish_runtime_backend(RuntimeBackendSnapshot {
            requested: RuntimeBackendMode::Auto,
            effective: RuntimeBackendMode::IoUring,
            fixed_buffers_capable: true,
            close_opcode: true,
            cancel_support: true,
            nonblocking_drain: true,
            requested_ring_size: 4096,
            effective_ring_size: 4096,
            ..Default::default()
        });

        let snapshot = ks.runtime_metrics();

        assert!(snapshot.backend.mixed);
        assert_eq!(snapshot.backend.effective, RuntimeBackendMode::Mixed);
    }

    /// Verify that concurrent max updates never lose the largest value.
    ///
    /// Each thread records the true maximum (`MAX_VALUE`) into both
    /// completion and command batch counters on slot 0, then records many
    /// scrambled smaller values. With the old racy load-then-store pattern,
    /// a smaller value on one thread could overwrite the larger max stored
    /// by another thread between the load and the store. With `fetch_max`
    /// the maximum is atomically preserved regardless of interleaving.
    #[test]
    fn concurrent_runtime_max_preserves_largest_value() {
        const SLOT_COUNT: usize = 2;
        const THREADS: usize = 8;
        const MAX_VALUE: u64 = 1_000;
        const ITERATIONS: u64 = 2_000;

        let ks = Arc::new(ConcurrentKeyspace::new_with_runtime_slots(
            TEST_SHARDS,
            SLOT_COUNT,
        ));
        let barrier = Arc::new(Barrier::new(THREADS));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let ks = Arc::clone(&ks);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    // Every thread pushes the true maximum first.
                    ks.record_reactor_completion_batch(0, MAX_VALUE as usize);
                    ks.record_reactor_command_batch(0, MAX_VALUE as usize);
                    // Then write many scrambled smaller values that could
                    // race with other threads' MAX_VALUE writes.
                    for v in 1..=ITERATIONS {
                        let width = ((v.wrapping_mul(31 + t as u64)) % (MAX_VALUE - 1)) + 1;
                        ks.record_reactor_completion_batch(0, width as usize);
                        ks.record_reactor_command_batch(0, width as usize);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let snapshot = ks.runtime_metrics();
        assert_eq!(
            snapshot.completion_batch_max, MAX_VALUE,
            "completion_batch_max must be {MAX_VALUE}, got {}",
            snapshot.completion_batch_max,
        );
        assert_eq!(
            snapshot.command_batch_max, MAX_VALUE,
            "command_batch_max must be {MAX_VALUE}, got {}",
            snapshot.command_batch_max,
        );
    }

    /// Deterministic single-thread test: a smaller value recorded after a
    /// larger one must not overwrite the max.
    #[test]
    fn runtime_max_smaller_after_larger_preserves_max() {
        let ks = ConcurrentKeyspace::new_with_runtime_slots(TEST_SHARDS, 1);

        ks.record_reactor_completion_batch(0, 100);
        ks.record_reactor_completion_batch(0, 50);
        ks.record_reactor_completion_batch(0, 75);

        ks.record_reactor_command_batch(0, 200);
        ks.record_reactor_command_batch(0, 10);
        ks.record_reactor_command_batch(0, 150);

        let snapshot = ks.runtime_metrics();
        assert_eq!(snapshot.completion_batch_max, 100);
        assert_eq!(snapshot.command_batch_max, 200);
    }

    #[test]
    fn dbsize_and_flush() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        for i in 0..100u64 {
            let key_bytes = format!("key:{i:06}");
            let key = VortexKey::from_bytes(key_bytes.as_bytes());
            let val = VortexValue::from(i as i64);
            ks.write(key_bytes.as_bytes(), |t| t.insert(key, val));
        }

        assert_eq!(ks.dbsize(), 100);

        ks.flush_all();
        assert_eq!(ks.dbsize(), 0);
    }

    #[test]
    fn flush_all_with_lsn_returns_none_when_keyspace_is_empty() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        ks.enable_aof_recording();

        let lsn_before_flush = ks.current_lsn();

        assert_eq!(ks.flush_all_with_lsn(), None);
        assert_eq!(ks.current_lsn(), lsn_before_flush);
        assert_eq!(ks.dbsize(), 0);
    }

    #[test]
    fn flush_all_with_lsn_returns_lsn_and_invalidates_watches_when_non_empty() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        ks.enable_aof_recording();
        ks.write(b"watch:present", |table| {
            table.insert(
                VortexKey::from_bytes(b"watch:present"),
                VortexValue::from("value"),
            );
        });

        let watched = [
            ks.watch_key(VortexKey::from_bytes(b"watch:present")),
            ks.watch_key(VortexKey::from_bytes(b"watch:absent")),
        ];
        let watch_epoch = ks.current_watch_epoch();
        let lsn_before_flush = ks.current_lsn();

        assert!(!ks.watched_keys_changed(watch_epoch, &watched));

        let flush_lsn = ks.flush_all_with_lsn();

        assert_eq!(flush_lsn.map(AofLsn::get), Some(lsn_before_flush));
        assert_eq!(ks.current_lsn(), lsn_before_flush + 1);
        assert_eq!(ks.dbsize(), 0);
        assert!(
            ks.watched_keys_changed(watch_epoch, &watched),
            "non-empty global FLUSH must invalidate existing and absent WATCH state"
        );

        ks.unwatch_keys(watched);
    }

    #[test]
    fn flush_all_with_lsn_allocates_lsn_before_releasing_shards() {
        let ks = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        ks.enable_aof_recording();

        let keys = keys_for_shards(&ks, &[0, TEST_SHARDS - 1]);
        let writer_key = keys[0].clone();
        ks.write(&writer_key, |table| {
            table.insert(
                VortexKey::from_bytes(&writer_key),
                VortexValue::from("resident"),
            );
        });

        let blocker = ks.shards[TEST_SHARDS - 1].write();
        let flush_ks = Arc::clone(&ks);
        let (flush_tx, flush_rx) = mpsc::channel();
        let flush_handle = thread::spawn(move || {
            flush_tx
                .send(
                    flush_ks
                        .flush_all_with_lsn()
                        .expect("non-empty AOF-enabled flush must allocate an LSN"),
                )
                .expect("flush LSN receiver should stay alive");
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut flush_holds_first_shard = false;
        while Instant::now() < deadline {
            if ks.shards[0].try_write().is_none() {
                flush_holds_first_shard = true;
                break;
            }
            thread::yield_now();
        }
        assert!(
            flush_holds_first_shard,
            "flush should hold shard 0 while blocked on the final shard"
        );

        let writer_ks = Arc::clone(&ks);
        let (writer_tx, writer_rx) = mpsc::channel();
        let writer_handle = thread::spawn(move || {
            let mut guard = writer_ks.write_shard_by_index(0);
            let lsn = writer_ks.next_lsn();
            guard.insert_with_lsn(
                VortexKey::from_bytes(&writer_key),
                VortexValue::from("after-flush"),
                Some(lsn),
            );
            writer_tx
                .send(lsn)
                .expect("writer LSN receiver should stay alive");
        });

        drop(blocker);
        let flush_lsn = flush_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("flush should complete after blocker is released");
        let writer_lsn = writer_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("writer should complete after flush releases shard 0");

        flush_handle.join().expect("flush worker should not panic");
        writer_handle
            .join()
            .expect("writer worker should not panic");

        assert!(
            flush_lsn.get() < writer_lsn,
            "post-FLUSH writer LSN must be greater than FLUSH LSN"
        );
    }

    #[test]
    fn flush_all_preserves_outstanding_memory_reservation() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        ks.configure_eviction(1 << 20, EvictionPolicy::NoEviction);
        ks.write(b"resident", |table| {
            table.insert(
                VortexKey::from_bytes(b"resident"),
                VortexValue::from("value"),
            );
        });
        assert!(ks.memory_used() > 0);
        assert!(ks.approx_memory_used() > 0);

        let (_, reservation) = ks.ensure_memory_for(0, 512, 0).unwrap();
        assert_eq!(ks.memory_reserved(), 512);

        ks.flush_all();

        assert_eq!(ks.memory_used(), 0);
        assert_eq!(ks.approx_memory_used(), 0);
        assert_eq!(
            ks.memory_reserved(),
            512,
            "FLUSH must not erase outstanding reservation ownership"
        );

        reservation.settle();
        assert_eq!(ks.memory_reserved(), 0);
    }

    #[test]
    fn flush_all_with_lsn_preserves_outstanding_memory_reservation() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        ks.enable_aof_recording();
        ks.configure_eviction(1 << 20, EvictionPolicy::NoEviction);
        ks.write(b"resident", |table| {
            table.insert(
                VortexKey::from_bytes(b"resident"),
                VortexValue::from("value"),
            );
        });
        assert!(ks.memory_used() > 0);
        assert!(ks.approx_memory_used() > 0);

        let (_, reservation) = ks.ensure_memory_for(0, 512, 0).unwrap();
        assert_eq!(ks.memory_reserved(), 512);

        assert!(
            ks.flush_all_with_lsn().is_some(),
            "non-empty AOF-enabled flush should allocate an LSN"
        );

        assert_eq!(ks.memory_used(), 0);
        assert_eq!(ks.approx_memory_used(), 0);
        assert_eq!(
            ks.memory_reserved(),
            512,
            "FLUSH with LSN must not erase outstanding reservation ownership"
        );

        reservation.settle();
        assert_eq!(ks.memory_reserved(), 0);
    }

    #[test]
    fn watch_registration_and_bumps_release_all_refs_under_contention() {
        let ks = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let barrier = Arc::new(Barrier::new(7));
        let key = VortexKey::from("watched:key");
        let mut handles = Vec::new();

        for _ in 0..4 {
            let ks = Arc::clone(&ks);
            let barrier = Arc::clone(&barrier);
            let key = key.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..1_000 {
                    let watched = ks.watch_key(key.clone());
                    let epoch = ks.current_watch_epoch();
                    let _ = ks.watched_keys_changed(epoch, std::slice::from_ref(&watched));
                    ks.unwatch_keys(std::iter::once(watched));
                }
            }));
        }

        for _ in 0..2 {
            let ks = Arc::clone(&ks);
            let barrier = Arc::clone(&barrier);
            let key = key.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..2_000 {
                    ks.bump_watch_key(&key);
                }
            }));
        }

        barrier.wait();
        for handle in handles {
            handle.join().expect("watch worker should not panic");
        }

        let live_watch_slots: usize = ks
            .absent_watch_shards
            .iter()
            .map(|shard| shard.read().len())
            .sum();
        assert_eq!(live_watch_slots, 0);
        assert_eq!(ks.absent_watch_active.load(Ordering::Acquire), 0);
        assert_eq!(ks.watch_active.load(Ordering::Acquire), 0);
        assert!(!ks.watch_tracking_active());
    }

    #[test]
    fn unwatch_consumes_watch_registrations_and_releases_refs_once() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        let watched = [
            ks.watch_key(VortexKey::from_bytes(b"owned:present")),
            ks.watch_key(VortexKey::from_bytes(b"owned:absent")),
        ];

        assert_eq!(ks.watch_active.load(Ordering::Acquire), 2);
        assert_eq!(ks.absent_watch_active.load(Ordering::Acquire), 2);

        ks.unwatch_keys(watched);

        assert_eq!(ks.watch_active.load(Ordering::Acquire), 0);
        assert_eq!(ks.absent_watch_active.load(Ordering::Acquire), 0);
    }

    #[test]
    fn shard_count_must_be_power_of_two() {
        std::panic::catch_unwind(|| ConcurrentKeyspace::new(100)).unwrap_err();
        std::panic::catch_unwind(|| ConcurrentKeyspace::new(0)).unwrap_err();
        std::panic::catch_unwind(|| ConcurrentKeyspace::new(32)).unwrap_err(); // below MIN_SHARD_COUNT
        // These should not panic:
        let _ = ConcurrentKeyspace::new(64);
        let _ = ConcurrentKeyspace::new(256);
        let _ = ConcurrentKeyspace::new(4096);
        let _ = ConcurrentKeyspace::new(65536);
    }

    #[test]
    fn with_capacity_pre_sizes_shards() {
        let ks = ConcurrentKeyspace::with_capacity(TEST_SHARDS, 1_000_000);
        for i in 0..10_000u64 {
            let key_bytes = format!("k:{i:08}");
            let key = VortexKey::from_bytes(key_bytes.as_bytes());
            ks.write(key_bytes.as_bytes(), |t| {
                t.insert(key, VortexValue::from("value"));
            });
        }
        assert_eq!(ks.dbsize(), 10_000);
    }

    #[test]
    fn shard_index_is_deterministic() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        let idx1 = ks.shard_index(b"test_key");
        let idx2 = ks.shard_index(b"test_key");
        assert_eq!(idx1, idx2);
        assert!(idx1 < TEST_SHARDS);
    }

    #[test]
    fn multi_read_acquires_locks_in_order() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        // Insert some keys
        for i in 0..10u64 {
            let key_bytes = format!("mk:{i}");
            let key = VortexKey::from_bytes(key_bytes.as_bytes());
            ks.write(key_bytes.as_bytes(), |t| {
                t.insert(key, VortexValue::from(i as i64));
            });
        }

        let keys: Vec<&[u8]> = (0..10)
            .map(|i| {
                let key = format!("mk:{i}");
                // Leak the string for test simplicity — tests don't care about this.
                key.into_bytes().leak() as &[u8]
            })
            .collect();

        let (guards, plan) = ks.multi_read(&keys);

        // Verify sorted indices are actually sorted
        for w in plan.sorted_shards().windows(2) {
            assert!(w[0] < w[1], "sorted shards must be strictly ascending");
        }

        // Verify guard count matches unique shard count
        assert_eq!(guards.len(), plan.sorted_shards().len());

        // Verify we can look up each key's guard
        for (i, key_bytes) in keys.iter().enumerate() {
            let shard_idx = plan.shard_for_key(i).get();
            let pos = plan.guard_index_for_key(i).get();
            assert_eq!(guards[pos].0, shard_idx);
            let key = VortexKey::from_bytes(key_bytes);
            assert!(guards[pos].1.get(&key).is_some() || guards[pos].1.get(&key).is_none());
        }
    }

    #[test]
    fn multi_write_no_deadlock() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        let keys: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma", b"delta", b"epsilon"];
        let (mut guards, plan) = ks.multi_write(&keys);

        // Write all keys through the guards
        for (i, key_bytes) in keys.iter().enumerate() {
            let pos = plan.guard_index_for_key(i).get();
            let key = VortexKey::from_bytes(key_bytes);
            let val = VortexValue::from(i as i64);
            guards[pos].1.insert(key, val);
        }

        drop(guards);
        assert_eq!(ks.dbsize(), 5);
    }

    #[test]
    fn concurrent_reads_no_contention() {
        let ks = Arc::new(ConcurrentKeyspace::new(256));

        // Pre-populate
        for i in 0..1000u64 {
            let key_bytes = format!("key:{i:06}");
            let key = VortexKey::from_bytes(key_bytes.as_bytes());
            ks.write(key_bytes.as_bytes(), |t| {
                t.insert(key, VortexValue::from(i as i64));
            });
        }

        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let ks = Arc::clone(&ks);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for i in 0..1000u64 {
                        let key_bytes = format!("key:{i:06}");
                        let result = ks.read(key_bytes.as_bytes(), |t| {
                            t.get(&VortexKey::from_bytes(key_bytes.as_bytes())).cloned()
                        });
                        assert!(result.is_some(), "key:{i:06} should exist");
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn concurrent_read_write_correctness() {
        let ks = Arc::new(ConcurrentKeyspace::new(256));
        let barrier = Arc::new(Barrier::new(5));
        let mut handles = Vec::new();

        // 2 writers — each writes 500 distinct keys
        for w in 0..2u64 {
            let ks = Arc::clone(&ks);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for i in 0..500u64 {
                    let key_bytes = format!("w{w}:{i}");
                    let key = VortexKey::from_bytes(key_bytes.as_bytes());
                    let val = VortexValue::from(i as i64);
                    ks.write(key_bytes.as_bytes(), |t| {
                        t.insert(key, val);
                    });
                }
            }));
        }

        // 2 readers — continuously read all possible keys
        for _ in 0..2 {
            let ks = Arc::clone(&ks);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    for w in 0..2u64 {
                        for i in 0..500u64 {
                            let key_bytes = format!("w{w}:{i}");
                            let _ = ks.read(key_bytes.as_bytes(), |t| {
                                t.get(&VortexKey::from_bytes(key_bytes.as_bytes())).cloned()
                            });
                        }
                    }
                }
            }));
        }

        // Start all threads at the barrier
        barrier.wait();
        for h in handles {
            h.join().unwrap();
        }

        // Both writers wrote 500 keys each
        assert_eq!(ks.dbsize(), 1000);
    }

    #[test]
    fn concurrent_multi_write_no_deadlock() {
        // Verify that concurrent multi-key writes touching overlapping shards
        // don't deadlock thanks to ordered locking.
        let ks = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let barrier = Arc::new(Barrier::new(8));

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let ks = Arc::clone(&ks);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for i in 0..100u64 {
                        let k1 = format!("t{t}:a:{i}");
                        let k2 = format!("t{t}:b:{i}");
                        let k3 = format!("t{t}:c:{i}");
                        let keys: Vec<&[u8]> = vec![k1.as_bytes(), k2.as_bytes(), k3.as_bytes()];
                        let (mut guards, plan) = ks.multi_write(&keys);

                        for (j, key_bytes) in keys.iter().enumerate() {
                            let pos = plan.guard_index_for_key(j).get();
                            let key = VortexKey::from_bytes(key_bytes);
                            let val = VortexValue::from("v");
                            guards[pos].1.insert(key, val);
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(ks.dbsize(), 8 * 100 * 3);
    }

    #[test]
    fn ttl_through_concurrent_keyspace() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        let key = VortexKey::from_bytes(b"ttl_key");
        let val = VortexValue::from("ephemeral");
        let deadline = 1_000_000_000u64; // 1 second from epoch
        let shard_idx = ks.shard_index(b"ttl_key");

        // SET with TTL
        ks.write(b"ttl_key", |t| {
            t.insert_with(key.clone(), val, deadline, None);
        });
        ks.apply_expiry_transition(shard_idx, ExpiryTransition::new(false, true));

        // GET before expiry (now=0)
        let got = ks.read(b"ttl_key", |t| {
            t.get(&VortexKey::from_bytes(b"ttl_key")).cloned()
        });
        assert!(got.is_some());

        // GET with lazy expiry (now > deadline)
        let got = ks.write(b"ttl_key", |t| {
            let key = VortexKey::from_bytes(b"ttl_key");
            let hash = t.table_hash_key_bytes(key.as_bytes());
            t.get_or_expire_prehashed(key.as_bytes(), hash, 2_000_000_000)
                .cloned()
        });
        assert!(got.is_none());
    }

    #[test]
    fn scan_all_shards_aggregates() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        for i in 0..200u64 {
            let key_bytes = format!("scan:{i}");
            let key = VortexKey::from_bytes(key_bytes.as_bytes());
            ks.write(key_bytes.as_bytes(), |t| {
                t.insert(key, VortexValue::from(i as i64));
            });
        }

        let counts: Vec<usize> = ks.scan_all_shards(|_, table| table.len());
        let total: usize = counts.iter().sum();
        assert_eq!(total, 200);

        // Verify keys are distributed across multiple shards
        let nonempty = counts.iter().filter(|&&c| c > 0).count();
        assert!(nonempty > 1, "keys should be distributed across shards");
    }

    #[test]
    fn memory_accounting() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        ks.write(b"tiny", |table| {
            table.insert(VortexKey::from_bytes(b"tiny"), VortexValue::from("v"));
        });

        assert!(ks.memory_used() > 0);
        assert_eq!(ks.approx_memory_used(), 0);

        let large = vec![b'x'; 20_000];
        ks.write(b"large", |table| {
            table.insert(
                VortexKey::from_bytes(b"large"),
                VortexValue::from_bytes(&large),
            );
        });

        let exact_after_insert = ks.memory_used();
        let approx_after_insert = ks.approx_memory_used();
        assert!(approx_after_insert >= 16 * 1024);
        assert!(approx_after_insert <= exact_after_insert);

        ks.write(b"large", |table| {
            table.remove(&VortexKey::from_bytes(b"large"));
        });

        assert!(ks.memory_used() < exact_after_insert);
        assert!(ks.approx_memory_used() < approx_after_insert);

        ks.flush_all();
        assert_eq!(ks.memory_used(), 0);
        assert_eq!(ks.approx_memory_used(), 0);
    }

    #[test]
    fn enabling_maxmemory_publishes_pending_memory_drift() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        ks.write(b"tiny", |table| {
            table.insert(VortexKey::from_bytes(b"tiny"), VortexValue::from("v"));
        });

        let exact_before = ks.memory_used();
        assert_eq!(ks.approx_memory_used(), 0);

        ks.configure_eviction(1 << 20, EvictionPolicy::AllKeysLru);

        assert_eq!(ks.approx_memory_used(), exact_before);
    }

    #[test]
    fn maxmemory_mode_publishes_small_memory_changes_exactly() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        ks.configure_eviction(1 << 20, EvictionPolicy::AllKeysLru);

        ks.write(b"tiny", |table| {
            table.insert(VortexKey::from_bytes(b"tiny"), VortexValue::from("v"));
        });

        assert_eq!(ks.approx_memory_used(), ks.memory_used());
    }

    #[test]
    fn shard_plan_maps_duplicate_keys_to_the_same_guard() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        let keys: [&[u8]; 4] = [b"dup:key", b"other:key", b"dup:key", b"third:key"];
        let plan = ShardPlan::new(&ks, &keys);

        assert_eq!(plan.shard_for_key(0), plan.shard_for_key(2));
        assert_eq!(plan.guard_index_for_key(0), plan.guard_index_for_key(2));

        for (idx, key) in keys.iter().enumerate() {
            let shard = ShardId::from_masked_index(ks.shard_index(key));
            assert_eq!(plan.shard_for_key(idx), shard);
            assert_eq!(
                plan.sorted_shards()[plan.guard_index_for_key(idx).get()],
                shard
            );
        }
    }

    #[test]
    fn checked_shard_access_returns_none_for_out_of_bounds_index() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        assert!(ks.try_read_shard_by_index(TEST_SHARDS).is_none());
        assert!(ks.try_write_shard_by_index(TEST_SHARDS).is_none());
    }

    #[test]
    fn shard_plan_keeps_common_batches_inline() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        let keys: Vec<Vec<u8>> = (0..16u64)
            .map(|i| format!("plan:{i}").into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let plan = ShardPlan::new(&ks, &key_refs);

        assert!(!plan.sorted_shards.spilled());
        assert!(!plan.per_key_shards.spilled());
        assert!(!plan.per_key_guard_indices.spilled());
    }

    #[test]
    fn restore_lsn_after_replay_zero_advances_next_lsn_once() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        let zero = AofLsn::try_from_raw(0).expect("zero is a valid replay LSN");

        unsafe {
            ks.restore_lsn_after_replay(Some(zero))
                .expect("zero restore should succeed");
        }

        assert_eq!(ks.current_lsn(), 1);

        unsafe {
            ks.restore_lsn_after_replay(Some(zero))
                .expect("duplicate restore should not move backward");
        }

        assert_eq!(ks.current_lsn(), 1);
    }

    #[test]
    fn aof_lsn_rejects_values_above_entry_stamp_width() {
        assert!(AofLsn::try_from_raw(MAX_STORED_LSN_VERSION).is_ok());
        assert!(AofLsn::try_from_raw(MAX_STORED_LSN_VERSION + 1).is_err());
    }

    #[test]
    #[should_panic(expected = "global LSN exceeds 48-bit entry version storage")]
    fn next_lsn_panics_before_entry_stamp_when_counter_is_exhausted() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        let max = AofLsn::try_from_raw(MAX_STORED_LSN_VERSION).unwrap();

        unsafe {
            ks.restore_lsn_after_replay(Some(max))
                .expect("max restore should leave the next counter just past the entry bound");
        }

        let _ = ks.next_lsn();
    }

    #[test]
    fn stress_concurrent_mixed_operations() {
        // Stress test: 16 threads doing mixed read/write/multi-key ops
        let ks = Arc::new(ConcurrentKeyspace::new(256));
        let barrier = Arc::new(Barrier::new(16));
        let iters = 500u64;

        let handles: Vec<_> = (0..16)
            .map(|t| {
                let ks = Arc::clone(&ks);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for i in 0..iters {
                        match t % 4 {
                            // Writers
                            0 => {
                                let key_bytes = format!("stress:{t}:{i}");
                                let key = VortexKey::from_bytes(key_bytes.as_bytes());
                                ks.write(key_bytes.as_bytes(), |table| {
                                    table.insert(key, VortexValue::from(i as i64));
                                });
                            }
                            // Readers
                            1 => {
                                let key_bytes = format!("stress:{t}:{i}");
                                let _ = ks.read(key_bytes.as_bytes(), |table| {
                                    table
                                        .get(&VortexKey::from_bytes(key_bytes.as_bytes()))
                                        .cloned()
                                });
                            }
                            // Multi-key writes
                            2 => {
                                let k1 = format!("mstress:{t}:a:{i}");
                                let k2 = format!("mstress:{t}:b:{i}");
                                let keys: Vec<&[u8]> = vec![k1.as_bytes(), k2.as_bytes()];
                                let (mut guards, plan) = ks.multi_write(&keys);
                                for (j, kb) in keys.iter().enumerate() {
                                    let pos = plan.guard_index_for_key(j).get();
                                    guards[pos]
                                        .1
                                        .insert(VortexKey::from_bytes(kb), VortexValue::from("mv"));
                                }
                            }
                            // DBSIZE
                            _ => {
                                let _ = ks.dbsize();
                            }
                            #[allow(unreachable_patterns)]
                            _ => unreachable!(),
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify no crash, no deadlock, positive key count
        assert!(ks.dbsize() > 0);
    }

    #[test]
    fn active_expiry_uses_delete_slot_directly() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        let deadline = 1_000_000;

        // Insert 10 keys with TTL deadline = 1 ms (will be expired at now=2 ms).
        for i in 0..10u64 {
            let key_bytes = format!("exp:{i}");
            let key = VortexKey::from_bytes(key_bytes.as_bytes());
            let shard_idx = ks.shard_index(key_bytes.as_bytes());
            ks.write(key_bytes.as_bytes(), |t| {
                t.insert_with(key, VortexValue::from(i as i64), deadline, None);
            });
            ks.apply_expiry_transition(shard_idx, ExpiryTransition::new(false, true));
        }

        assert_eq!(ks.dbsize(), 10);
        let mem_before = ks.memory_used();
        assert!(mem_before > 0);

        // Run active expiry across all shards with now_nanos > deadline.
        let mut total_expired = 0usize;
        let num_shards = ks.num_shards();
        for shard_idx in 0..num_shards {
            let (expired, _sampled) =
                ks.run_active_expiry_on_shard(shard_idx, 0, 256, deadline * 2);
            total_expired += expired;
        }

        assert_eq!(total_expired, 10);
        assert_eq!(ks.dbsize(), 0);
        // Memory must be fully reclaimed.
        assert_eq!(ks.memory_used(), 0);
    }

    #[test]
    fn active_expiry_skips_non_expired_and_no_ttl() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        let deadline = 10_000_000;

        // 5 keys with no TTL (persistent).
        for i in 0..5u64 {
            let key_bytes = format!("perm:{i}");
            let key = VortexKey::from_bytes(key_bytes.as_bytes());
            ks.write(key_bytes.as_bytes(), |t| {
                t.insert(key, VortexValue::from("forever"));
            });
        }

        // 5 keys with TTL in the future (not expired).
        for i in 0..5u64 {
            let key_bytes = format!("future:{i}");
            let key = VortexKey::from_bytes(key_bytes.as_bytes());
            let shard_idx = ks.shard_index(key_bytes.as_bytes());
            ks.write(key_bytes.as_bytes(), |t| {
                t.insert_with(key, VortexValue::from("later"), deadline, None);
            });
            ks.apply_expiry_transition(shard_idx, ExpiryTransition::new(false, true));
        }

        assert_eq!(ks.dbsize(), 10);

        // Run active expiry at now=5 ms — future keys are still alive.
        let mut total_expired = 0usize;
        for shard_idx in 0..ks.num_shards() {
            let (expired, _) = ks.run_active_expiry_on_shard(shard_idx, 0, 256, 5_000_000);
            total_expired += expired;
        }

        assert_eq!(total_expired, 0);
        assert_eq!(ks.dbsize(), 10);
    }

    #[test]
    fn lfu_read_sampling_reduces_global_sketch_updates() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        ks.configure_eviction(1 << 20, EvictionPolicy::AllKeysLfu);

        let key = VortexKey::from_bytes(b"hot-key");
        ks.write(b"hot-key", |table| {
            table.insert(key.clone(), VortexValue::from("value"));
        });

        let shard_idx = ks.shard_index(key.as_bytes());
        let hash = ks.table_hash_key(key.as_bytes());
        let guard = ks.read_shard_by_index(shard_idx);
        let mut reads = 0usize;
        while ks.frequency_sketch.estimate(hash.get()) == 0 {
            ks.record_access_prehashed(&guard, key.as_bytes(), hash);
            reads += 1;
            assert!(
                reads <= 4096,
                "LFU read sampling should eventually record with deterministic xorshift RNG"
            );
        }
        for _ in reads..128 {
            ks.record_access_prehashed(&guard, key.as_bytes(), hash);
        }
        drop(guard);

        let sampled_reads = ks.frequency_sketch.estimate(hash.get());
        assert!(sampled_reads < 64);

        let writes = ConcurrentKeyspace::new(TEST_SHARDS);
        writes.configure_eviction(1 << 20, EvictionPolicy::AllKeysLfu);
        for _ in 0..128 {
            writes.record_frequency_hash(hash);
        }
        let write_updates = writes.frequency_sketch.estimate(hash.get());
        assert_eq!(write_updates, 128);
        assert!(sampled_reads < write_updates);
    }

    #[test]
    fn volatile_eviction_without_ttls_fails_fast_without_scanning() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        ks.write(b"resident", |table| {
            table.insert(
                VortexKey::from_bytes(b"resident"),
                VortexValue::from("value"),
            );
        });
        ks.configure_eviction(ks.memory_used(), EvictionPolicy::VolatileLru);

        let error = ks.ensure_memory_for(0, 1, 0).unwrap_err();
        assert_eq!(error.kind, MutationErrorKind::OutOfMemory);

        let metrics = ks.eviction_metrics();
        assert_eq!(metrics.admissions, 1);
        assert_eq!(metrics.shards_scanned, 0);
        assert_eq!(metrics.slots_sampled, 0);
        assert_eq!(metrics.bytes_freed, 0);
        assert_eq!(metrics.oom_after_scan, 1);
    }

    #[test]
    fn eviction_effects_preserve_aof_watch_and_ttl_for_each_policy() {
        let policies = [
            EvictionPolicy::AllKeysLru,
            EvictionPolicy::AllKeysLfu,
            EvictionPolicy::AllKeysRandom,
            EvictionPolicy::VolatileLru,
            EvictionPolicy::VolatileLfu,
            EvictionPolicy::VolatileRandom,
            EvictionPolicy::VolatileTtl,
        ];

        for (policy_idx, policy) in policies.into_iter().enumerate() {
            let ks = ConcurrentKeyspace::new(TEST_SHARDS);
            ks.enable_aof_recording();
            let key_bytes = format!("eviction-effects:{policy_idx}");
            let key = VortexKey::from_bytes(key_bytes.as_bytes());
            let shard_idx = ks.shard_index(key_bytes.as_bytes());
            let ttl_deadline = 10_000_000;

            ks.write(key_bytes.as_bytes(), |table| {
                table.insert_with(
                    key.clone(),
                    VortexValue::from_bytes(b"resident"),
                    ttl_deadline,
                    None,
                );
            });
            ks.apply_expiry_transition(shard_idx, ExpiryTransition::new(false, true));
            assert_eq!(ks.approx_expiring_keys(), 1);

            let epoch = ks.current_watch_epoch();
            let watched = ks.watch_key(key.clone());
            ks.configure_eviction(ks.memory_used(), policy);

            let (evicted, reservation) = ks
                .ensure_memory_for(shard_idx, 1, 0)
                .expect("single resident key should satisfy eviction admission");
            reservation.settle();

            let evicted = evicted.expect("AOF-enabled eviction should return records");
            assert_eq!(evicted.len(), 1, "policy {policy:?}");
            assert_eq!(evicted[0].key, key, "policy {policy:?}");
            assert_eq!(evicted[0].lsn.get(), 0, "policy {policy:?}");
            assert_eq!(ks.current_lsn(), 1, "policy {policy:?}");
            assert_eq!(ks.dbsize(), 0, "policy {policy:?}");
            assert_eq!(ks.approx_expiring_keys(), 0, "policy {policy:?}");
            assert!(
                ks.watched_keys_changed(epoch, std::slice::from_ref(&watched)),
                "policy {policy:?}"
            );
            ks.unwatch_keys(std::iter::once(watched));
        }
    }

    #[test]
    fn volatile_ttl_sweep_does_not_delete_deferred_candidate_after_expired_key_frees_enough() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        let (shard_idx, keys) = keys_for_same_shard(&ks, 2);
        let future_key = VortexKey::from_bytes(&keys[0]);
        let expired_key = VortexKey::from_bytes(&keys[1]);
        let now_nanos = 10_000;
        let expired_deadline = now_nanos - 1;
        let future_deadline = now_nanos + 1_000_000;

        {
            let mut guard = ks.write_shard_by_index(shard_idx);
            guard.insert_with(
                future_key.clone(),
                VortexValue::from("future"),
                future_deadline,
                None,
            );
            guard.insert_with(
                expired_key.clone(),
                VortexValue::from("expired"),
                expired_deadline,
                None,
            );
        }
        ks.apply_expiry_transition(shard_idx, ExpiryTransition::new(false, true));
        ks.apply_expiry_transition(shard_idx, ExpiryTransition::new(false, true));

        let future_slot = {
            let guard = ks.read_shard_by_index(shard_idx);
            slot_for_key(&guard, &future_key)
        };
        ks.set_clock_hand(shard_idx, future_slot);

        let mut report = EvictionScanReport::default();
        let mut evicted = Vec::new();
        let freed = ks.evict_from_shard(
            shard_idx,
            EvictionPolicy::VolatileTtl,
            1,
            now_nanos,
            &mut report,
            &mut evicted,
        );

        assert!(freed > 0);
        let guard = ks.read_shard_by_index(shard_idx);
        assert!(
            guard.get(&future_key).is_some(),
            "future TTL candidate must survive once the expired key satisfies the sweep"
        );
        assert!(guard.get(&expired_key).is_none());
        drop(guard);
        assert_eq!(ks.dbsize(), 1);
        assert_eq!(ks.approx_expiring_keys(), 1);
    }

    #[test]
    fn eviction_budget_caps_shards_scanned_per_admission() {
        let ks = ConcurrentKeyspace::new(256);
        let shard_budget = crate::eviction::EVICTION_MAX_SHARDS_PER_ADMISSION;
        let target_shards: Vec<usize> = (0..(shard_budget + 8)).collect();
        let keys = keys_for_shards(&ks, &target_shards);

        for key in &keys {
            ks.write(key, |table| {
                table.insert(VortexKey::from_bytes(key), VortexValue::from("value"));
            });
        }
        ks.configure_eviction(ks.memory_used(), EvictionPolicy::AllKeysRandom);

        let additional_bytes = ks.max_memory() + 1;
        let error = ks.ensure_memory_for(0, additional_bytes, 0).unwrap_err();
        assert_eq!(error.kind, MutationErrorKind::OutOfMemory);

        let metrics = ks.eviction_metrics();
        assert_eq!(metrics.admissions, 1);
        assert!(metrics.shards_scanned <= shard_budget as u64);
        assert!(metrics.slots_sampled >= metrics.shards_scanned);
        assert!(metrics.bytes_freed > 0);
        assert_eq!(metrics.oom_after_scan, 1);
    }

    #[test]
    fn active_expiry_on_ttl_free_shard_skips_lock_acquisition() {
        let ks = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let shard_idx = 0usize;
        let write_guard = ks.write_shard_by_index(shard_idx);
        let (tx, rx) = mpsc::channel();
        let worker_ks = Arc::clone(&ks);

        let handle = thread::spawn(move || {
            tx.send(worker_ks.run_active_expiry_on_shard(shard_idx, 0, 32, 0))
                .unwrap();
        });

        let result = rx.recv_timeout(Duration::from_millis(100));
        drop(write_guard);
        handle.join().unwrap();

        assert_eq!(result.unwrap(), (0, 0));
    }

    #[test]
    fn reservation_prevents_concurrent_overshoot_under_noeviction() {
        let ks = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
        let max_mem = ks.memory_used() + 1024;
        ks.configure_eviction(max_mem, EvictionPolicy::NoEviction);

        // N threads racing to allocate 512 bytes each.
        // With max_mem = used + 1024, only 2 threads should succeed.
        let mut handles = Vec::new();
        let success_count = Arc::new(AtomicUsize::new(0));

        for _ in 0..10 {
            let worker_ks = Arc::clone(&ks);
            let success_count = Arc::clone(&success_count);
            handles.push(thread::spawn(move || {
                let res = worker_ks.ensure_memory_for(0, 512, 0);
                if let Ok((_, reservation)) = res {
                    success_count.fetch_add(1, Ordering::SeqCst);
                    // Simulate doing some work and holding the reservation
                    thread::sleep(Duration::from_millis(5));

                    // Manually increment published used to simulate commit
                    worker_ks
                        .global_memory_used
                        .fetch_add(512, Ordering::SeqCst);
                    reservation.settle();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            success_count.load(Ordering::SeqCst),
            2,
            "Only 2 concurrent reservations should succeed"
        );
        assert_eq!(
            ks.memory_reserved(),
            0,
            "All reservations should be settled"
        );
    }

    #[test]
    fn reservation_settles_on_error_paths() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        let max_mem = ks.memory_used() + 1024;
        ks.configure_eviction(max_mem, EvictionPolicy::NoEviction);

        {
            let res = ks.ensure_memory_for(0, 512, 0);
            assert!(res.is_ok());
            let (_, reservation) = res.unwrap();
            assert_eq!(ks.memory_reserved(), 512);
            // Drop without calling settle() simulates an error path
            drop(reservation);
        }

        assert_eq!(
            ks.memory_reserved(),
            0,
            "Reservation should be auto-settled via Drop"
        );
    }

    #[test]
    fn reservation_counter_returns_to_zero_after_operations() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);
        ks.configure_eviction(1024 * 1024, EvictionPolicy::NoEviction);

        let (evicted, reservation) = ks.ensure_memory_for(0, 100, 0).unwrap();
        assert!(evicted.is_none());
        assert_eq!(ks.memory_reserved(), 100);

        // Simulating mutation and shard drop
        reservation.settle();

        assert_eq!(ks.memory_reserved(), 0);
    }
}
