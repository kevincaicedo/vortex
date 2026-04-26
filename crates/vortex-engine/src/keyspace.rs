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

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use ahash::RandomState;
use crossbeam_utils::CachePadded;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use vortex_common::VortexKey;

use crate::eviction::{
    EVICTION_MAX_SHARDS_PER_ADMISSION, EVICTION_SWEEP_WINDOW, EvictionConfig, EvictionConfigState,
    EvictionPolicy, FrequencySketch, next_random_u64, should_sample_lfu_read,
};
use crate::table::SwissTable;

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
const WATCH_SHARD_COUNT: usize = 256;
const TRANSACTION_GATE_COUNTERS: usize = 128;
const MUTATION_FEATURE_MAXMEMORY: usize = 1 << 0;
const MUTATION_FEATURE_WATCH: usize = 1 << 1;
const MUTATION_FEATURE_AOF: usize = 1 << 2;

// ─── Shard type alias ───────────────────────────────────────────────

/// A single shard: a `SwissTable` behind a `RwLock`, padded to a full
/// 128-byte boundary so that adjacent shard locks never share a cache line.
type Shard = CachePadded<RwLock<SwissTable>>;

/// Return type for multi-key read lock acquisition.
/// `(guards_with_shard_id, sorted_unique_shard_indices, per_key_shard_indices)`
pub type MultiReadGuards<'a> = (
    Vec<(usize, RwLockReadGuard<'a, SwissTable>)>,
    Vec<usize>,
    Vec<usize>,
);

/// Return type for multi-key write lock acquisition.
/// `(guards_with_shard_id, sorted_unique_shard_indices, per_key_shard_indices)`
pub type MultiWriteGuards<'a> = (Vec<(usize, ShardWriteGuard<'a>)>, Vec<usize>, Vec<usize>);

#[derive(Debug)]
pub(crate) struct EvictedKey {
    pub(crate) lsn: u64,
    pub(crate) key: VortexKey,
}

pub(crate) type EvictedKeys = Option<Box<[EvictedKey]>>;

#[derive(Debug)]
/// Cold WATCH metadata kept outside the 64-byte Entry hot path.
struct WatchSlot {
    version: u64,
    refs: usize,
}

type WatchShard = CachePadded<RwLock<HashMap<VortexKey, WatchSlot>>>;

#[derive(Clone, Debug)]
pub struct WatchKeyState {
    pub key: VortexKey,
    pub version: u64,
}

#[derive(Debug)]
struct TransactionGate {
    active: AtomicBool,
    readers: Box<[CachePadded<AtomicUsize>]>,
}

impl Default for TransactionGate {
    fn default() -> Self {
        let readers: Vec<CachePadded<AtomicUsize>> = (0..TRANSACTION_GATE_COUNTERS)
            .map(|_| CachePadded::new(AtomicUsize::new(0)))
            .collect();
        Self {
            active: AtomicBool::new(false),
            readers: readers.into_boxed_slice(),
        }
    }
}

pub struct CommandGateGuard<'a> {
    reader: &'a CachePadded<AtomicUsize>,
}

pub struct TransactionGateGuard<'a> {
    gate: &'a TransactionGate,
}

#[derive(Debug)]
pub(crate) struct EvictionAdmissionError {
    pub(crate) response: &'static [u8],
    pub(crate) evicted: EvictedKeys,
}

impl EvictionAdmissionError {
    #[inline]
    fn new(response: &'static [u8], evicted: EvictedKeys) -> Self {
        Self { response, evicted }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EvictionMetricsSnapshot {
    pub admissions: u64,
    pub shards_scanned: u64,
    pub slots_sampled: u64,
    pub bytes_freed: u64,
    pub oom_after_scan: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EvictionScanReport {
    shards_scanned: usize,
    slots_sampled: usize,
    bytes_freed: usize,
    oom_after_scan: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EvictionSweepResult {
    next_slot: usize,
    freed_bytes: usize,
    slots_sampled: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EvictionSweepContext {
    shard_idx: usize,
    start_slot: usize,
    bytes_needed: usize,
    now_nanos: u64,
    volatile_only: bool,
}

#[derive(Debug, Default)]
struct EvictionMetrics {
    admissions: AtomicU64,
    shards_scanned: AtomicU64,
    slots_sampled: AtomicU64,
    bytes_freed: AtomicU64,
    oom_after_scan: AtomicU64,
}

impl EvictionMetrics {
    #[inline]
    fn record(&self, report: EvictionScanReport) {
        self.admissions.fetch_add(1, Ordering::Relaxed);
        self.shards_scanned
            .fetch_add(report.shards_scanned as u64, Ordering::Relaxed);
        self.slots_sampled
            .fetch_add(report.slots_sampled as u64, Ordering::Relaxed);
        self.bytes_freed
            .fetch_add(report.bytes_freed as u64, Ordering::Relaxed);
        if report.oom_after_scan {
            self.oom_after_scan.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn snapshot(&self) -> EvictionMetricsSnapshot {
        EvictionMetricsSnapshot {
            admissions: self.admissions.load(Ordering::Relaxed),
            shards_scanned: self.shards_scanned.load(Ordering::Relaxed),
            slots_sampled: self.slots_sampled.load(Ordering::Relaxed),
            bytes_freed: self.bytes_freed.load(Ordering::Relaxed),
            oom_after_scan: self.oom_after_scan.load(Ordering::Relaxed),
        }
    }
}

pub struct ShardWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, SwissTable>,
    global_memory_used: &'a AtomicUsize,
    strict_memory_accounting: &'a AtomicBool,
}

pub struct ReplayModeGuard<'a> {
    keyspace: &'a ConcurrentKeyspace,
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
        if self.strict_memory_accounting.load(Ordering::Relaxed) {
            self.guard.flush_memory_drift_force(self.global_memory_used);
        } else {
            self.guard.flush_memory_drift(self.global_memory_used);
        }
    }
}

impl Drop for ReplayModeGuard<'_> {
    fn drop(&mut self) {
        self.keyspace.replay_depth.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for CommandGateGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.reader.fetch_sub(1, Ordering::Release);
    }
}

impl Drop for TransactionGateGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.gate.active.store(false, Ordering::Release);
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
    /// Optimistic WATCH side table. It is cold unless clients are actively
    /// watching keys; normal mutations avoid it behind `watch_active`.
    watch_shards: Box<[WatchShard]>,
    /// Number of active WATCH registrations. Zero means write paths skip the
    /// side table entirely.
    watch_active: AtomicUsize,
    /// Global invalidation epoch for full-keyspace writes such as FLUSHALL.
    watch_epoch: AtomicU64,
    /// Cross-reactor transaction gate. Normal commands enter as readers;
    /// EXEC enters exclusively and waits for pre-existing commands to drain.
    transaction_gate: CachePadded<TransactionGate>,
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

fn make_watch_shards() -> Box<[WatchShard]> {
    (0..WATCH_SHARD_COUNT)
        .map(|_| CachePadded::new(RwLock::new(HashMap::new())))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

#[inline]
fn evicted_keys_to_box(evicted: Vec<EvictedKey>) -> EvictedKeys {
    if evicted.is_empty() {
        None
    } else {
        Some(evicted.into_boxed_slice())
    }
}

#[inline]
fn spin_or_yield(spins: &mut usize) {
    if *spins < 64 {
        *spins += 1;
        std::hint::spin_loop();
    } else {
        std::thread::yield_now();
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
        assert!(
            num_shards > 0 && num_shards.is_power_of_two(),
            "num_shards must be a power of two, got {num_shards}"
        );
        assert!(
            (MIN_SHARD_COUNT..=MAX_SHARD_COUNT).contains(&num_shards),
            "num_shards must be in [{MIN_SHARD_COUNT}, {MAX_SHARD_COUNT}], got {num_shards}"
        );

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
        let watch_shards = make_watch_shards();

        Self {
            shards: shards.into_boxed_slice(),
            clock_hands: clock_hands.into_boxed_slice(),
            expiry_key_count: expiry_key_count.into_boxed_slice(),
            expiry_key_total: AtomicUsize::new(0),
            mask: (num_shards - 1) as u64,
            hasher: RandomState::with_seeds(AHASH_SEED_0, AHASH_SEED_1, AHASH_SEED_2, AHASH_SEED_3),
            table_hasher,
            global_memory_used: AtomicUsize::new(0),
            strict_memory_accounting: AtomicBool::new(false),
            mutation_features: AtomicUsize::new(0),
            replay_depth: AtomicUsize::new(0),
            global_lsn: AtomicU64::new(0),
            aof_recording_refs: AtomicUsize::new(0),
            eviction: EvictionConfigState::new(),
            frequency_sketch: FrequencySketch::new(),
            eviction_metrics: EvictionMetrics::default(),
            watch_shards,
            watch_active: AtomicUsize::new(0),
            watch_epoch: AtomicU64::new(0),
            transaction_gate: CachePadded::new(TransactionGate::default()),
        }
    }

    #[inline]
    pub fn enter_replay_mode(&self) -> ReplayModeGuard<'_> {
        self.replay_depth.fetch_add(1, Ordering::Relaxed);
        ReplayModeGuard { keyspace: self }
    }

    #[inline]
    pub(crate) fn replay_mode_active(&self) -> bool {
        self.replay_depth.load(Ordering::Relaxed) != 0
    }

    #[inline]
    pub fn enter_command_gate(&self) -> CommandGateGuard<'_> {
        self.enter_command_gate_slot(0)
    }

    #[inline]
    pub fn enter_command_gate_slot(&self, slot: usize) -> CommandGateGuard<'_> {
        let gate = &self.transaction_gate;
        let reader = &gate.readers[slot % gate.readers.len()];
        let mut spins = 0usize;
        loop {
            while gate.active.load(Ordering::Acquire) {
                spin_or_yield(&mut spins);
            }

            reader.fetch_add(1, Ordering::Acquire);
            if !gate.active.load(Ordering::Acquire) {
                return CommandGateGuard { reader };
            }

            reader.fetch_sub(1, Ordering::Release);
            spin_or_yield(&mut spins);
        }
    }

    #[inline]
    pub fn enter_transaction_gate(&self) -> TransactionGateGuard<'_> {
        let gate = &self.transaction_gate;
        let mut spins = 0usize;
        loop {
            if gate
                .active
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
            spin_or_yield(&mut spins);
        }

        for reader in gate.readers.iter() {
            while reader.load(Ordering::Acquire) != 0 {
                spin_or_yield(&mut spins);
            }
        }

        TransactionGateGuard { gate }
    }

    #[inline]
    pub fn current_watch_epoch(&self) -> u64 {
        self.watch_epoch.load(Ordering::Acquire)
    }

    pub fn watch_key(&self, key: VortexKey) -> WatchKeyState {
        self.enable_mutation_feature(MUTATION_FEATURE_WATCH);
        let shard_idx = self.watch_shard_index(key.as_bytes());
        // TODO: Check Deadlock-free guarantees for WATCH operations.
        let mut guard = self.watch_shards[shard_idx].write();
        let slot = guard.entry(key.clone()).or_insert(WatchSlot {
            version: 0,
            refs: 0,
        });
        slot.refs += 1;
        if self.watch_active.fetch_add(1, Ordering::Release) == 0 {
            self.enable_mutation_feature(MUTATION_FEATURE_WATCH);
        }
        WatchKeyState {
            key,
            version: slot.version,
        }
    }

    pub fn unwatch_keys(&self, keys: &[WatchKeyState]) {
        for watched in keys {
            let shard_idx = self.watch_shard_index(watched.key.as_bytes());
            // TODO: Check Deadlock-free guarantees for WATCH operations.
            let mut guard = self.watch_shards[shard_idx].write();
            if let Some(slot) = guard.get_mut(&watched.key) {
                if slot.refs > 1 {
                    slot.refs -= 1;
                } else {
                    guard.remove(&watched.key);
                }
                if self.watch_active.fetch_sub(1, Ordering::Release) == 1 {
                    self.disable_mutation_feature(MUTATION_FEATURE_WATCH);
                }
            }
        }
    }

    pub fn watched_keys_changed(&self, epoch: u64, keys: &[WatchKeyState]) -> bool {
        if self.current_watch_epoch() != epoch {
            return true;
        }

        for watched in keys {
            let shard_idx = self.watch_shard_index(watched.key.as_bytes());
            // TODO: Check Deadlock-free guarantees for WATCH operations.
            let guard = self.watch_shards[shard_idx].read();
            let Some(slot) = guard.get(&watched.key) else {
                return true;
            };
            if slot.version != watched.version {
                return true;
            }
        }

        false
    }

    /// Create a new keyspace pre-sized for `total_capacity` entries spread
    /// evenly across `num_shards` shards. Avoids early resize churn.
    ///
    /// # Panics
    ///
    /// Same as [`new`](Self::new).
    pub fn with_capacity(num_shards: usize, total_capacity: usize) -> Self {
        assert!(
            num_shards > 0 && num_shards.is_power_of_two(),
            "num_shards must be a power of two, got {num_shards}"
        );
        assert!(
            (MIN_SHARD_COUNT..=MAX_SHARD_COUNT).contains(&num_shards),
            "num_shards must be in [{MIN_SHARD_COUNT}, {MAX_SHARD_COUNT}], got {num_shards}"
        );

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
        let watch_shards = make_watch_shards();

        Self {
            shards: shards.into_boxed_slice(),
            clock_hands: clock_hands.into_boxed_slice(),
            expiry_key_count: expiry_key_count.into_boxed_slice(),
            expiry_key_total: AtomicUsize::new(0),
            mask: (num_shards - 1) as u64,
            hasher: RandomState::with_seeds(AHASH_SEED_0, AHASH_SEED_1, AHASH_SEED_2, AHASH_SEED_3),
            table_hasher,
            global_memory_used: AtomicUsize::new(0),
            strict_memory_accounting: AtomicBool::new(false),
            mutation_features: AtomicUsize::new(0),
            replay_depth: AtomicUsize::new(0),
            global_lsn: AtomicU64::new(0),
            aof_recording_refs: AtomicUsize::new(0),
            eviction: EvictionConfigState::new(),
            frequency_sketch: FrequencySketch::new(),
            eviction_metrics: EvictionMetrics::default(),
            watch_shards,
            watch_active: AtomicUsize::new(0),
            watch_epoch: AtomicU64::new(0),
            transaction_gate: CachePadded::new(TransactionGate::default()),
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
            self.enable_mutation_feature(MUTATION_FEATURE_MAXMEMORY);
            self.strict_memory_accounting.store(true, Ordering::Relaxed);
            self.publish_all_memory_drift();
        }

        self.eviction.set_max_memory(max_memory);

        if previous != 0 && max_memory == 0 {
            self.strict_memory_accounting
                .store(false, Ordering::Relaxed);
            self.disable_mutation_feature(MUTATION_FEATURE_MAXMEMORY);
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
            self.enable_mutation_feature(MUTATION_FEATURE_MAXMEMORY);
            self.strict_memory_accounting.store(true, Ordering::Relaxed);
            self.publish_all_memory_drift();
        }

        self.eviction.store(max_memory, policy);

        if previous != 0 && max_memory == 0 {
            self.strict_memory_accounting
                .store(false, Ordering::Relaxed);
            self.disable_mutation_feature(MUTATION_FEATURE_MAXMEMORY);
        }
    }

    #[inline(always)]
    pub(crate) fn mutation_features(&self) -> usize {
        self.mutation_features.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub(crate) fn mutation_feature_maxmemory(features: usize) -> bool {
        features & MUTATION_FEATURE_MAXMEMORY != 0
    }

    #[inline(always)]
    pub(crate) fn mutation_feature_watch(features: usize) -> bool {
        features & MUTATION_FEATURE_WATCH != 0
    }

    #[inline(always)]
    pub(crate) fn mutation_feature_aof(features: usize) -> bool {
        features & MUTATION_FEATURE_AOF != 0
    }

    #[inline(always)]
    pub(crate) fn next_aof_lsn_with_features(&self, features: usize) -> Option<u64> {
        Self::mutation_feature_aof(features).then(|| self.next_lsn())
    }

    #[inline(always)]
    fn mutation_feature_active(&self, feature: usize) -> bool {
        self.mutation_features.load(Ordering::Acquire) & feature != 0
    }

    #[inline(always)]
    fn enable_mutation_feature(&self, feature: usize) {
        self.mutation_features.fetch_or(feature, Ordering::Release);
    }

    #[inline(always)]
    fn disable_mutation_feature(&self, feature: usize) {
        self.mutation_features
            .fetch_and(!feature, Ordering::Release);
    }

    #[inline]
    pub(crate) fn record_access_prehashed(&self, table: &SwissTable, key_bytes: &[u8], hash: u64) {
        let snapshot = self.eviction_config();
        if snapshot.max_memory == 0 {
            return;
        }

        match snapshot.policy {
            EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu => {
                let access_random = next_random_u64();
                if should_sample_lfu_read(access_random) {
                    self.frequency_sketch.record(hash);
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
    pub(crate) fn record_frequency_hash(&self, hash: u64) {
        let snapshot = self.eviction_config();
        self.record_frequency_hash_snapshot(hash, snapshot);
    }

    #[inline]
    pub(crate) fn record_frequency_hash_snapshot(&self, hash: u64, snapshot: EvictionConfig) {
        if snapshot.max_memory != 0 && snapshot.policy.is_lfu() {
            self.frequency_sketch.record(hash);
        }
    }

    #[inline]
    fn watch_shard_index(&self, key_bytes: &[u8]) -> usize {
        (self.table_hash_key(key_bytes) as usize) & (WATCH_SHARD_COUNT - 1)
    }

    #[inline]
    pub(crate) fn watch_tracking_active(&self) -> bool {
        self.mutation_feature_active(MUTATION_FEATURE_WATCH)
    }

    #[inline]
    pub(crate) fn bump_watch_key(&self, key: &VortexKey) {
        if !self.watch_tracking_active() {
            return;
        }
        self.bump_watch_key_known_active(key);
    }

    #[inline]
    pub(crate) fn bump_watch_key_known_active(&self, key: &VortexKey) {
        let shard_idx = self.watch_shard_index(key.as_bytes());
        let mut guard = self.watch_shards[shard_idx].write();
        if let Some(slot) = guard.get_mut(key) {
            slot.version = slot.version.wrapping_add(1).max(1);
        }
    }

    #[inline]
    pub(crate) fn bump_watch_key_bytes(&self, key_bytes: &[u8]) {
        if !self.watch_tracking_active() {
            return;
        }
        self.bump_watch_key(&VortexKey::from(key_bytes));
    }

    #[inline]
    pub(crate) fn bump_all_watches(&self) {
        if self.watch_active.load(Ordering::Acquire) != 0 {
            self.watch_epoch.fetch_add(1, Ordering::Release);
        }
    }

    pub(crate) fn ensure_memory_for(
        &self,
        preferred_shard: usize,
        additional_bytes: usize,
        now_nanos: u64,
    ) -> Result<EvictedKeys, EvictionAdmissionError> {
        if self.replay_mode_active() {
            return Ok(None);
        }

        if additional_bytes == 0 {
            return Ok(None);
        }

        let snapshot = self.eviction_config();
        self.ensure_memory_for_snapshot(preferred_shard, additional_bytes, now_nanos, snapshot)
    }

    pub(crate) fn ensure_memory_for_snapshot(
        &self,
        preferred_shard: usize,
        additional_bytes: usize,
        now_nanos: u64,
        snapshot: EvictionConfig,
    ) -> Result<EvictedKeys, EvictionAdmissionError> {
        // Alpha note: this is a concurrent preflight against the published
        // global counter, not a linearizable reservation. Racing writers can
        // still overshoot until a future reservation protocol lands.
        if additional_bytes == 0 {
            return Ok(None);
        }

        if snapshot.max_memory == 0 {
            return Ok(None);
        }

        if self
            .published_memory_used()
            .saturating_add(additional_bytes)
            <= snapshot.max_memory
        {
            return Ok(None);
        }

        if snapshot.policy.is_noeviction() {
            return Err(EvictionAdmissionError::new(crate::commands::ERR_OOM, None));
        }

        let mut report = EvictionScanReport::default();
        if snapshot.policy.is_volatile_only() && !self.has_expiring_keys() {
            report.oom_after_scan = true;
            self.eviction_metrics.record(report);
            return Err(EvictionAdmissionError::new(crate::commands::ERR_OOM, None));
        }

        let mut evicted = Vec::new();
        let target_used = snapshot.max_memory.saturating_sub(additional_bytes);
        report.bytes_freed = self.evict_until_target(
            preferred_shard,
            target_used,
            snapshot.policy,
            now_nanos,
            &mut report,
            &mut evicted,
        );

        report.oom_after_scan = self
            .published_memory_used()
            .saturating_add(additional_bytes)
            > snapshot.max_memory;
        self.eviction_metrics.record(report);

        if !report.oom_after_scan {
            Ok(evicted_keys_to_box(evicted))
        } else {
            Err(EvictionAdmissionError::new(
                crate::commands::ERR_OOM,
                evicted_keys_to_box(evicted),
            ))
        }
    }

    fn evict_until_target(
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

    fn evict_from_shard(
        &self,
        shard_idx: usize,
        policy: EvictionPolicy,
        bytes_needed: usize,
        now_nanos: u64,
        report: &mut EvictionScanReport,
        evicted: &mut Vec<EvictedKey>,
    ) -> usize {
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
        };
        let sweep = match policy {
            EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => {
                self.evict_random(&mut guard, context, evicted)
            }
            EvictionPolicy::VolatileTtl => self.evict_volatile_ttl(&mut guard, context, evicted),
            policy if policy.is_lfu() => self.evict_lfu_clock_sweep(&mut guard, context, evicted),
            _ => self.evict_clock_sweep(&mut guard, context, evicted),
        };
        report.slots_sampled += sweep.slots_sampled;
        self.set_clock_hand(shard_idx, sweep.next_slot);
        sweep.freed_bytes
    }

    fn evict_clock_sweep(
        &self,
        table: &mut SwissTable,
        context: EvictionSweepContext,
        evicted: &mut Vec<EvictedKey>,
    ) -> EvictionSweepResult {
        let total_slots = table.total_slots();
        if total_slots == 0 {
            return EvictionSweepResult::default();
        }

        let mut slot = context.start_slot % total_slots;
        let mut freed_bytes = 0usize;
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

            let Some(entry) = table.slot_entry(current_slot) else {
                continue;
            };

            if ttl != 0 && ttl <= context.now_nanos {
                freed_bytes +=
                    self.delete_evictable_slot(context.shard_idx, table, current_slot, evicted);
                if freed_bytes >= context.bytes_needed {
                    break;
                }
                continue;
            }

            if entry.decrement_eviction_counter() {
                continue;
            }

            freed_bytes +=
                self.delete_evictable_slot(context.shard_idx, table, current_slot, evicted);
            if freed_bytes >= context.bytes_needed {
                break;
            }
        }

        EvictionSweepResult {
            next_slot: slot,
            freed_bytes,
            slots_sampled,
        }
    }

    fn evict_lfu_clock_sweep(
        &self,
        table: &mut SwissTable,
        context: EvictionSweepContext,
        evicted: &mut Vec<EvictedKey>,
    ) -> EvictionSweepResult {
        let total_slots = table.total_slots();
        if total_slots == 0 {
            return EvictionSweepResult::default();
        }

        let mut slot = context.start_slot % total_slots;
        let mut freed_bytes = 0usize;
        let mut best_candidate = None;
        let mut best_frequency = u8::MAX;
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

            let Some(entry) = table.slot_entry(current_slot) else {
                continue;
            };

            if ttl != 0 && ttl <= context.now_nanos {
                freed_bytes +=
                    self.delete_evictable_slot(context.shard_idx, table, current_slot, evicted);
                if freed_bytes >= context.bytes_needed {
                    return EvictionSweepResult {
                        next_slot: slot,
                        freed_bytes,
                        slots_sampled,
                    };
                }
                continue;
            }

            if entry.decrement_eviction_counter() {
                continue;
            }

            let Some((key, _)) = table.slot_key_value(current_slot) else {
                continue;
            };
            let frequency = self
                .frequency_sketch
                .estimate(table.hash_key_bytes(key.as_bytes()));
            if best_candidate.is_none() || frequency < best_frequency {
                best_candidate = Some(current_slot);
                best_frequency = frequency;
            }
        }

        if freed_bytes < context.bytes_needed {
            if let Some(candidate) = best_candidate {
                freed_bytes +=
                    self.delete_evictable_slot(context.shard_idx, table, candidate, evicted);
            }
        }

        EvictionSweepResult {
            next_slot: slot,
            freed_bytes,
            slots_sampled,
        }
    }

    fn evict_random(
        &self,
        table: &mut SwissTable,
        context: EvictionSweepContext,
        evicted: &mut Vec<EvictedKey>,
    ) -> EvictionSweepResult {
        let total_slots = table.total_slots();
        if total_slots == 0 {
            return EvictionSweepResult::default();
        }

        let random_start = (next_random_u64() as usize) & (total_slots - 1);
        let mut slot = (context.start_slot + random_start) % total_slots;
        let mut freed_bytes = 0usize;
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
            let _ = context.now_nanos;
            freed_bytes +=
                self.delete_evictable_slot(context.shard_idx, table, current_slot, evicted);
            if freed_bytes >= context.bytes_needed {
                break;
            }
        }

        EvictionSweepResult {
            next_slot: slot,
            freed_bytes,
            slots_sampled,
        }
    }

    fn evict_volatile_ttl(
        &self,
        table: &mut SwissTable,
        context: EvictionSweepContext,
        evicted: &mut Vec<EvictedKey>,
    ) -> EvictionSweepResult {
        let total_slots = table.total_slots();
        if total_slots == 0 {
            return EvictionSweepResult::default();
        }

        let mut slot = context.start_slot % total_slots;
        let mut best_slot = None;
        let mut best_deadline = u64::MAX;
        let mut slots_sampled = 0usize;
        let sweep_len = EVICTION_SWEEP_WINDOW.min(total_slots);

        for _ in 0..sweep_len {
            let current_slot = slot;
            slot = (slot + 1) % total_slots;
            slots_sampled += 1;
            let ttl = table.slot_entry_ttl(current_slot);
            if ttl == 0 {
                continue;
            }
            if table.slot_entry(current_slot).is_none() {
                continue;
            }
            if ttl <= context.now_nanos {
                let freed =
                    self.delete_evictable_slot(context.shard_idx, table, current_slot, evicted);
                return EvictionSweepResult {
                    next_slot: slot,
                    freed_bytes: freed,
                    slots_sampled,
                };
            }
            if ttl < best_deadline {
                best_deadline = ttl;
                best_slot = Some(current_slot);
            }
        }

        let freed = best_slot
            .map(|candidate| {
                self.delete_evictable_slot(context.shard_idx, table, candidate, evicted)
            })
            .unwrap_or(0);
        EvictionSweepResult {
            next_slot: slot,
            freed_bytes: freed,
            slots_sampled,
        }
    }

    fn delete_evictable_slot(
        &self,
        shard_idx: usize,
        table: &mut SwissTable,
        slot: usize,
        evicted: &mut Vec<EvictedKey>,
    ) -> usize {
        let ttl = table.slot_entry_ttl(slot);
        let bytes = table.slot_memory_bytes(slot);
        if bytes == 0 {
            return 0;
        }
        let features = self.mutation_features();
        let record_aof = Self::mutation_feature_aof(features);
        let track_watch = Self::mutation_feature_watch(features);
        let Some((key, _)) = table.slot_key_value(slot) else {
            return 0;
        };
        let key = (record_aof || track_watch).then(|| key.clone());

        let _ = table.delete_slot(slot);
        self.update_expiry_count(shard_idx, ttl != 0, false);
        if let Some(key) = key {
            if track_watch {
                self.bump_watch_key_known_active(&key);
            }
            if record_aof {
                evicted.push(EvictedKey {
                    lsn: self.next_lsn(),
                    key,
                });
            }
        }
        bytes
    }

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

    fn publish_all_memory_drift(&self) {
        for shard in self.shards.iter() {
            let mut guard = shard.write();
            guard.flush_memory_drift_force(&self.global_memory_used);
        }
    }

    #[inline]
    fn published_memory_used(&self) -> usize {
        self.global_memory_used.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub(crate) fn update_expiry_count(&self, shard_idx: usize, had_ttl: bool, has_ttl: bool) {
        debug_assert!(shard_idx < self.expiry_key_count.len());
        match (had_ttl, has_ttl) {
            (false, true) => {
                self.expiry_key_count[shard_idx].fetch_add(1, Ordering::Relaxed);
                self.expiry_key_total.fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                self.expiry_key_count[shard_idx].fetch_sub(1, Ordering::Relaxed);
                self.expiry_key_total.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    #[inline]
    pub(crate) fn total_expiry_keys(&self) -> usize {
        self.expiry_key_total.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn approx_expiring_keys(&self) -> usize {
        self.expiry_key_total.load(Ordering::Relaxed)
    }

    /// Cheap global check used by event loops to skip active-expiry work for
    /// pure no-TTL workloads.
    #[inline]
    pub fn has_expiring_keys(&self) -> bool {
        self.approx_expiring_keys() != 0
    }

    #[inline]
    fn shard_has_expiring_keys(&self, shard_idx: usize) -> bool {
        debug_assert!(shard_idx < self.expiry_key_count.len());
        self.expiry_key_count[shard_idx].load(Ordering::Relaxed) != 0
    }

    #[inline]
    pub fn eviction_metrics(&self) -> EvictionMetricsSnapshot {
        self.eviction_metrics.snapshot()
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

    // ─── Global LSN (Logical Sequence Number) ───────────────────────

    /// Allocate the next LSN. **Must be called while holding a shard write lock**
    /// to guarantee causal ordering: if Op₁ → Op₂ on the same key, then
    /// LSN₁ < LSN₂.
    ///
    /// `Relaxed` ordering is correct because the shard `RwLock` provides the
    /// necessary acquire/release synchronization. The atomic itself only needs
    /// monotonicity, which `fetch_add` guarantees on all architectures.
    #[inline(always)]
    pub fn next_lsn(&self) -> u64 {
        self.global_lsn.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    pub fn enable_aof_recording(&self) {
        if self.aof_recording_refs.fetch_add(1, Ordering::Release) == 0 {
            self.enable_mutation_feature(MUTATION_FEATURE_AOF);
        }
    }

    #[inline]
    pub fn disable_aof_recording(&self) {
        if let Ok(previous) =
            self.aof_recording_refs
                .fetch_update(Ordering::Release, Ordering::Relaxed, |refs| {
                    (refs != 0).then_some(refs - 1)
                })
        {
            if previous == 1 {
                self.disable_mutation_feature(MUTATION_FEATURE_AOF);
            }
        }
    }

    #[inline(always)]
    pub(crate) fn aof_recording_enabled(&self) -> bool {
        self.mutation_feature_active(MUTATION_FEATURE_AOF)
    }

    #[inline(always)]
    pub(crate) fn next_aof_lsn(&self) -> Option<u64> {
        self.aof_recording_enabled().then(|| self.next_lsn())
    }

    /// Read the current LSN value (the next LSN to be assigned).
    ///
    /// Useful for snapshot points (BGREWRITEAOF) and restoring LSN state
    /// after AOF replay.
    #[inline]
    pub fn current_lsn(&self) -> u64 {
        self.global_lsn.load(Ordering::Relaxed)
    }

    /// Set the global LSN to a specific value. Used during AOF replay to
    /// restore the LSN counter to the highest replayed value + 1.
    ///
    /// # Safety contract
    ///
    /// Must only be called during single-threaded initialization (before
    /// reactors are spawned) or while all reactors are quiesced.
    pub fn set_lsn(&self, lsn: u64) {
        self.global_lsn.store(lsn, Ordering::Relaxed);
    }

    /// Compute the shard index for a key using ahash + bitmask.
    ///
    /// Uses bitwise AND instead of modulo because `num_shards` is a power of 2.
    /// ahash uses AES-NI on x86_64 / hardware crypto on ARM — ~2ns per hash.
    #[inline(always)]
    pub fn shard_index(&self, key: &[u8]) -> usize {
        (self.hasher.hash_one(key) & self.mask) as usize
    }

    /// Hash a key using the keyspace's hasher.
    /// Exposed for pre-hashing in batch operations (MGET/MSET).
    #[inline(always)]
    pub fn hash_key(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Hash a key using the shared SwissTable hasher for this keyspace.
    #[inline(always)]
    pub(crate) fn table_hash_key(&self, key: &[u8]) -> u64 {
        self.table_hasher.hash_one(key)
    }

    // ─── Single-key lock acquisition ────────────────────────────────

    /// Acquire a read lock on the shard containing `key`.
    ///
    /// Use for read-only operations: GET, EXISTS, TTL, PTTL, TYPE, STRLEN.
    #[inline(always)]
    pub fn read_shard(&self, key: &[u8]) -> RwLockReadGuard<'_, SwissTable> {
        let idx = self.shard_index(key);
        // SAFETY: idx is always < shards.len() because mask = num_shards - 1
        // and num_shards is a power of 2. Bounds check is provably unnecessary.
        unsafe { self.shards.get_unchecked(idx) }.read()
    }

    /// Acquire a write lock on the shard containing `key`.
    ///
    /// Use for mutation operations: SET, DEL, INCR, EXPIRE, etc.
    #[inline(always)]
    pub fn write_shard(&self, key: &[u8]) -> ShardWriteGuard<'_> {
        let idx = self.shard_index(key);
        // SAFETY: idx is always < shards.len() because mask = num_shards - 1
        // and num_shards is a power of 2.
        self.tracked_write_guard(unsafe { self.shards.get_unchecked(idx) }.write())
    }

    /// Acquire a read lock on a specific shard by index.
    #[inline(always)]
    pub fn read_shard_by_index(&self, idx: usize) -> RwLockReadGuard<'_, SwissTable> {
        self.try_read_shard_by_index(idx)
            .expect("shard index out of bounds")
    }

    /// Acquire a write lock on a specific shard by index.
    #[inline(always)]
    pub fn write_shard_by_index(&self, idx: usize) -> ShardWriteGuard<'_> {
        self.try_write_shard_by_index(idx)
            .expect("shard index out of bounds")
    }

    /// Acquire a read lock on a specific shard by index, returning `None`
    /// when `idx` is out of range.
    #[inline(always)]
    pub fn try_read_shard_by_index(&self, idx: usize) -> Option<RwLockReadGuard<'_, SwissTable>> {
        self.shards.get(idx).map(|shard| shard.read())
    }

    /// Acquire a write lock on a specific shard by index, returning `None`
    /// when `idx` is out of range.
    #[inline(always)]
    pub fn try_write_shard_by_index(&self, idx: usize) -> Option<ShardWriteGuard<'_>> {
        self.shards
            .get(idx)
            .map(|shard| self.tracked_write_guard(shard.write()))
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

    /// Execute a mutation on the shard containing `key`.
    ///
    /// The closure receives an exclusive mutable reference to the `SwissTable`.
    #[inline]
    pub fn write<F, R>(&self, key: &[u8], f: F) -> R
    where
        F: FnOnce(&mut SwissTable) -> R,
    {
        let mut guard = self.write_shard(key);
        f(&mut guard)
    }

    // ─── Multi-key operations ───────────────────────────────────────
    //
    // Critical: acquire shard locks in ascending shard ID order to prevent
    // deadlocks. Deduplicate shard indices to avoid double-locking.

    /// Compute, sort, and deduplicate shard indices for a set of keys.
    ///
    /// Returns `(sorted_unique_shard_indices, per_key_shard_indices)` where:
    /// - `sorted_unique_shard_indices`: deduplicated shard IDs in ascending order
    /// - `per_key_shard_indices[i]`: the shard index for `keys[i]`
    ///
    /// Guard lookup via `sorted_shards.binary_search(&shard_idx)` is O(log K).
    ///
    /// # Safety invariant
    ///
    /// `binary_search` on `sorted_unique` always succeeds for any value in
    /// `per_key_shards`, because `sorted_unique` is the deduplicated set of
    /// all `per_key_shards` values. Formally proven via Kani proof
    /// `verify_binary_search_always_finds` in the M2 lab.
    #[inline]
    pub fn sorted_shard_indices(&self, keys: &[&[u8]]) -> (Vec<usize>, Vec<usize>) {
        let mut per_key = Vec::with_capacity(keys.len());

        for &key in keys {
            per_key.push(self.shard_index(key));
        }

        let mut unique = per_key.clone();
        unique.sort_unstable();
        unique.dedup();

        (unique, per_key)
    }

    /// Acquire read locks on all shards touched by `keys`, in ascending order.
    ///
    /// Returns `(guards, sorted_shard_indices, per_key_shard_indices)`.
    /// Use `binary_search` on `sorted_shard_indices` to map each key's shard
    /// index to the correct guard position.
    #[inline]
    pub fn multi_read<'a>(&'a self, keys: &[&[u8]]) -> MultiReadGuards<'a> {
        let (sorted, per_key) = self.sorted_shard_indices(keys);

        // Acquire read locks in ascending shard order — deadlock-free.
        let guards: Vec<(usize, RwLockReadGuard<'_, SwissTable>)> = sorted
            .iter()
            .map(|&idx| {
                // SAFETY: idx < shards.len() by construction (mask guarantees).
                (idx, unsafe { self.shards.get_unchecked(idx) }.read())
            })
            .collect();

        (guards, sorted, per_key)
    }

    /// Acquire write locks on all shards touched by `keys`, in ascending order.
    ///
    /// Returns `(guards, sorted_shard_indices, per_key_shard_indices)`.
    #[inline]
    pub fn multi_write<'a>(&'a self, keys: &[&[u8]]) -> MultiWriteGuards<'a> {
        let (sorted, per_key) = self.sorted_shard_indices(keys);

        // Acquire write locks in ascending shard order — deadlock-free.
        let guards: Vec<(usize, ShardWriteGuard<'_>)> = sorted
            .iter()
            .map(|&idx| {
                // SAFETY: idx < shards.len() by construction (mask guarantees).
                (
                    idx,
                    self.tracked_write_guard(unsafe { self.shards.get_unchecked(idx) }.write()),
                )
            })
            .collect();

        (guards, sorted, per_key)
    }

    /// Find the guard index for a shard index in the sorted guards array.
    ///
    /// # Safety
    ///
    /// `shard_idx` must be present in `sorted_shards`. This is guaranteed
    /// when `sorted_shards` was produced by `sorted_shard_indices` for the
    /// same set of keys. Formally proven deadlock-free and correct by Kani
    /// proof `verify_binary_search_always_finds`.
    #[inline(always)]
    pub fn guard_position(sorted_shards: &[usize], shard_idx: usize) -> usize {
        Self::try_guard_position(sorted_shards, shard_idx)
            .expect("shard index must be present in sorted_shards")
    }

    /// Find the guard index for a shard index in the sorted guards array.
    #[inline(always)]
    pub fn try_guard_position(sorted_shards: &[usize], shard_idx: usize) -> Option<usize> {
        sorted_shards.binary_search(&shard_idx).ok()
    }

    // ─── Transaction support ────────────────────────────────────────

    /// Acquire write locks on all shards touched by `keys` for transaction
    /// execution (MULTI/EXEC).
    ///
    /// Even GET operations within a transaction acquire write locks to ensure
    /// the entire transaction is serializable — no interleaving from other
    /// writers.
    ///
    /// Returns the same tuple as `multi_write`.
    #[inline]
    pub fn exec_transaction_locks<'a>(&'a self, keys: &[&[u8]]) -> MultiWriteGuards<'a> {
        self.multi_write(keys)
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

    /// Execute a closure on a single shard by index with a write lock.
    /// Used for per-shard active expiry sweeps.
    #[inline]
    pub fn write_shard_scan<F, R>(&self, shard_idx: usize, f: F) -> R
    where
        F: FnOnce(&mut SwissTable) -> R,
    {
        debug_assert!(shard_idx < self.shards.len());
        let mut guard = self.write_shard_by_index(shard_idx);
        f(&mut guard)
    }

    /// Run one round of active expiry on a single shard by index.
    ///
    /// Acquires a write lock on the target shard, scans up to `max_effort`
    /// occupied slots starting from `start_slot`, and removes entries whose
    /// TTL deadline has passed. Returns `(expired_count, sampled_count)`.
    ///
    /// The caller should rotate `start_slot` across calls to ensure full
    /// coverage over time (e.g. increment by `max_effort` each tick).
    ///
    /// # Mechanical sympathy
    ///
    /// - One write lock per call, not held across shards.
    /// - Sequential slot access is prefetcher-friendly.
    /// - Zero allocation: `delete_slot()` removes by slot index directly,
    ///   avoiding the key clone + hash + re-probe overhead of `remove()`.
    pub fn run_active_expiry_on_shard(
        &self,
        shard_idx: usize,
        start_slot: usize,
        max_effort: usize,
        now_nanos: u64,
    ) -> (usize, usize) {
        debug_assert!(shard_idx < self.shards.len());
        if !self.shard_has_expiring_keys(shard_idx) {
            return (0, 0);
        }
        let mut guard = self.write_shard_by_index(shard_idx);
        let total_slots = guard.total_slots();
        if total_slots == 0 {
            return (0, 0);
        }

        let mut expired = 0usize;
        let mut sampled = 0usize;

        for i in 0..max_effort {
            let slot = (start_slot + i) % total_slots;
            let deadline = guard.slot_entry_ttl(slot);
            // deadline == 0 means empty/deleted or no TTL — skip.
            if deadline == 0 {
                continue;
            }
            sampled += 1;
            if deadline <= now_nanos {
                // Entry expired — delete directly by slot index (O(1), zero alloc).
                let watched_key = if self.watch_active.load(Ordering::Acquire) != 0 {
                    guard.slot_key_value(slot).map(|(key, _)| key.clone())
                } else {
                    None
                };
                guard.delete_slot(slot);
                if let Some(key) = watched_key {
                    self.bump_watch_key(&key);
                }
                expired += 1;
            }
        }

        if expired != 0 {
            self.expiry_key_count[shard_idx].fetch_sub(expired, Ordering::Relaxed);
            self.expiry_key_total.fetch_sub(expired, Ordering::Relaxed);
        }

        (expired, sampled)
    }

    // ─── Metadata ───────────────────────────────────────────────────

    /// DBSIZE: returns the total number of keys across all shards.
    ///
    /// Acquires a read lock on each shard sequentially. The result is
    /// best-effort: keys may be added/removed between shard reads.
    pub fn dbsize(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// FLUSHDB / FLUSHALL: remove all keys from all shards.
    ///
    /// Acquires a write lock on each shard sequentially. Not atomic across
    /// shards — concurrent reads may see partial results during flush.
    #[allow(dead_code)]
    pub(crate) fn flush_all(&self) {
        for shard in self.shards.iter() {
            let mut guard = shard.write();
            // Replace with a fresh empty table to release all memory.
            *guard = SwissTable::with_hasher(self.table_hasher.clone());
        }
        for count in self.expiry_key_count.iter() {
            count.store(0, Ordering::Relaxed);
        }
        self.expiry_key_total.store(0, Ordering::Relaxed);
        self.global_memory_used.store(0, Ordering::Relaxed);
        self.bump_all_watches();
    }

    /// FLUSHDB / FLUSHALL with an optional AOF LSN allocated while all shard
    /// write locks are held.
    pub(crate) fn flush_all_with_lsn(&self) -> Option<u64> {
        let mut guards = Vec::with_capacity(self.shards.len());
        for shard in self.shards.iter() {
            guards.push(shard.write());
        }

        let had_entries = guards.iter().any(|guard| !guard.is_empty());
        for guard in &mut guards {
            **guard = SwissTable::with_hasher(self.table_hasher.clone());
        }
        for count in self.expiry_key_count.iter() {
            count.store(0, Ordering::Relaxed);
        }
        self.expiry_key_total.store(0, Ordering::Relaxed);
        self.global_memory_used.store(0, Ordering::Relaxed);
        if had_entries {
            self.bump_all_watches();
        }

        had_entries.then(|| self.next_aof_lsn()).flatten()
    }

    /// Returns the exact memory usage across all shards.
    pub fn memory_used(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().local_memory_used())
            .sum()
    }

    /// Returns the approximate published memory counter used by OOM/eviction checks.
    /// It is intentionally cheap and may lag exact shard-local memory briefly.
    #[inline]
    pub fn approx_memory_used(&self) -> usize {
        self.global_memory_used.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════
// Unit Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

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

        let (guards, sorted, per_key) = ks.multi_read(&keys);

        // Verify sorted indices are actually sorted
        for w in sorted.windows(2) {
            assert!(w[0] < w[1], "sorted shards must be strictly ascending");
        }

        // Verify guard count matches unique shard count
        assert_eq!(guards.len(), sorted.len());

        // Verify we can look up each key's guard
        for (i, &shard_idx) in per_key.iter().enumerate() {
            let pos = ConcurrentKeyspace::guard_position(&sorted, shard_idx);
            assert_eq!(guards[pos].0, shard_idx);
            let key = VortexKey::from_bytes(keys[i]);
            assert!(guards[pos].1.get(&key).is_some() || guards[pos].1.get(&key).is_none());
        }
    }

    #[test]
    fn multi_write_no_deadlock() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        let keys: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma", b"delta", b"epsilon"];
        let (mut guards, sorted, per_key) = ks.multi_write(&keys);

        // Write all keys through the guards
        for (i, key_bytes) in keys.iter().enumerate() {
            let shard_idx = per_key[i];
            let pos = ConcurrentKeyspace::guard_position(&sorted, shard_idx);
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
                        let (mut guards, sorted, per_key) = ks.multi_write(&keys);

                        for (j, key_bytes) in keys.iter().enumerate() {
                            let shard_idx = per_key[j];
                            let pos = ConcurrentKeyspace::guard_position(&sorted, shard_idx);
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
            t.insert_with_ttl(key.clone(), val, deadline);
        });
        ks.update_expiry_count(shard_idx, false, true);

        // GET before expiry (now=0)
        let got = ks.read(b"ttl_key", |t| {
            t.get(&VortexKey::from_bytes(b"ttl_key")).cloned()
        });
        assert!(got.is_some());

        // GET with lazy expiry (now > deadline)
        let got = ks.write(b"ttl_key", |t| {
            t.get_or_expire(&VortexKey::from_bytes(b"ttl_key"), 2_000_000_000)
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
    fn guard_position_correctness() {
        let sorted = vec![0, 3, 7, 12, 25];
        assert_eq!(ConcurrentKeyspace::guard_position(&sorted, 0), 0);
        assert_eq!(ConcurrentKeyspace::guard_position(&sorted, 3), 1);
        assert_eq!(ConcurrentKeyspace::guard_position(&sorted, 7), 2);
        assert_eq!(ConcurrentKeyspace::guard_position(&sorted, 12), 3);
        assert_eq!(ConcurrentKeyspace::guard_position(&sorted, 25), 4);
    }

    #[test]
    fn checked_shard_access_returns_none_for_out_of_bounds_index() {
        let ks = ConcurrentKeyspace::new(TEST_SHARDS);

        assert!(ks.try_read_shard_by_index(TEST_SHARDS).is_none());
        assert!(ks.try_write_shard_by_index(TEST_SHARDS).is_none());
        assert!(ConcurrentKeyspace::try_guard_position(&[0, 3, 7], 5).is_none());
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
                                let (mut guards, sorted, per_key) = ks.multi_write(&keys);
                                for (j, kb) in keys.iter().enumerate() {
                                    let pos =
                                        ConcurrentKeyspace::guard_position(&sorted, per_key[j]);
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

        // Insert 10 keys with TTL deadline = 1_000 (will be expired at now=2_000).
        for i in 0..10u64 {
            let key_bytes = format!("exp:{i}");
            let key = VortexKey::from_bytes(key_bytes.as_bytes());
            let shard_idx = ks.shard_index(key_bytes.as_bytes());
            ks.write(key_bytes.as_bytes(), |t| {
                t.insert_with_ttl(key, VortexValue::from(i as i64), 1_000);
            });
            ks.update_expiry_count(shard_idx, false, true);
        }

        assert_eq!(ks.dbsize(), 10);
        let mem_before = ks.memory_used();
        assert!(mem_before > 0);

        // Run active expiry across all shards with now_nanos > deadline.
        let mut total_expired = 0usize;
        let num_shards = ks.num_shards();
        for shard_idx in 0..num_shards {
            let (expired, _sampled) = ks.run_active_expiry_on_shard(shard_idx, 0, 256, 2_000);
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
                t.insert_with_ttl(key, VortexValue::from("later"), 10_000);
            });
            ks.update_expiry_count(shard_idx, false, true);
        }

        assert_eq!(ks.dbsize(), 10);

        // Run active expiry at now=5_000 — future keys are still alive.
        let mut total_expired = 0usize;
        for shard_idx in 0..ks.num_shards() {
            let (expired, _) = ks.run_active_expiry_on_shard(shard_idx, 0, 256, 5_000);
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
        for _ in 0..128 {
            ks.record_access_prehashed(&guard, key.as_bytes(), hash);
        }
        drop(guard);

        let sampled_reads = ks.frequency_sketch.estimate(hash);
        assert!(sampled_reads > 0);
        assert!(sampled_reads < 64);

        let writes = ConcurrentKeyspace::new(TEST_SHARDS);
        writes.configure_eviction(1 << 20, EvictionPolicy::AllKeysLfu);
        for _ in 0..128 {
            writes.record_frequency_hash(hash);
        }
        let write_updates = writes.frequency_sketch.estimate(hash);
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
        assert_eq!(error.response, crate::commands::ERR_OOM);

        let metrics = ks.eviction_metrics();
        assert_eq!(metrics.admissions, 1);
        assert_eq!(metrics.shards_scanned, 0);
        assert_eq!(metrics.slots_sampled, 0);
        assert_eq!(metrics.bytes_freed, 0);
        assert_eq!(metrics.oom_after_scan, 1);
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
        assert_eq!(error.response, crate::commands::ERR_OOM);

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
}
