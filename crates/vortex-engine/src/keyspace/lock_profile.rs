//! Profiling-only shard lock wait/hold telemetry.
//!
//! This module is compiled only with the `lock-profile` feature. Default and
//! release builds do not include the counters, clock reads, TLS scope, or guard
//! wrappers.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

const CLASS_COUNT: usize = 7;
const BUCKET_COUNT: usize = 10;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(usize)]
pub enum LockProfileClass {
    SingleKey = 0,
    MultiKey = 1,
    Transaction = 2,
    ExpiryCleanup = 3,
    Eviction = 4,
    AofMetadata = 5,
    #[default]
    Unclassified = 6,
}

impl LockProfileClass {
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SingleKey => "single_key",
            Self::MultiKey => "multi_key",
            Self::Transaction => "transaction",
            Self::ExpiryCleanup => "expiry_cleanup",
            Self::Eviction => "eviction",
            Self::AofMetadata => "aof_metadata",
            Self::Unclassified => "unclassified",
        }
    }

    #[inline]
    const fn from_index(index: usize) -> Self {
        match index {
            0 => Self::SingleKey,
            1 => Self::MultiKey,
            2 => Self::Transaction,
            3 => Self::ExpiryCleanup,
            4 => Self::Eviction,
            5 => Self::AofMetadata,
            _ => Self::Unclassified,
        }
    }

    #[inline]
    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockProfileKind {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LockProfileStatSnapshot {
    pub acquisitions: u64,
    pub wait_nanos_total: u64,
    pub wait_nanos_max: u64,
    pub hold_nanos_total: u64,
    pub hold_nanos_max: u64,
    pub wait_buckets: [u64; BUCKET_COUNT],
    pub hold_buckets: [u64; BUCKET_COUNT],
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LockProfileClassSnapshot {
    pub class: LockProfileClass,
    pub read: LockProfileStatSnapshot,
    pub write: LockProfileStatSnapshot,
    pub guard_count_total: u64,
    pub multi_guard_count_total: u64,
    pub retries: u64,
    pub revalidation_failures: u64,
}

#[derive(Clone, Debug)]
pub struct LockProfileSnapshot {
    pub enabled: bool,
    pub classes: [LockProfileClassSnapshot; CLASS_COUNT],
}

pub struct LockProfileScope {
    previous: Option<LockProfileClass>,
}

#[derive(Debug)]
pub(super) struct LockProfileState {
    enabled: AtomicBool,
    classes: [LockClassCounters; CLASS_COUNT],
}

#[derive(Debug)]
struct LockClassCounters {
    read: LockCounters,
    write: LockCounters,
    guard_count_total: AtomicU64,
    multi_guard_count_total: AtomicU64,
    retries: AtomicU64,
    revalidation_failures: AtomicU64,
}

#[derive(Debug)]
struct LockCounters {
    acquisitions: AtomicU64,
    wait_nanos_total: AtomicU64,
    wait_nanos_max: AtomicU64,
    hold_nanos_total: AtomicU64,
    hold_nanos_max: AtomicU64,
    wait_buckets: [AtomicU64; BUCKET_COUNT],
    hold_buckets: [AtomicU64; BUCKET_COUNT],
}

pub(super) struct LockProfileHold<'a> {
    state: &'a LockProfileState,
    class: LockProfileClass,
    kind: LockProfileKind,
    acquired_at: Instant,
}

thread_local! {
    static CURRENT_CLASS: Cell<LockProfileClass> = const { Cell::new(LockProfileClass::Unclassified) };
}

impl Default for LockProfileState {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            classes: std::array::from_fn(|_| LockClassCounters::default()),
        }
    }
}

impl Default for LockClassCounters {
    fn default() -> Self {
        Self {
            read: LockCounters::default(),
            write: LockCounters::default(),
            guard_count_total: AtomicU64::new(0),
            multi_guard_count_total: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            revalidation_failures: AtomicU64::new(0),
        }
    }
}

impl Default for LockCounters {
    fn default() -> Self {
        Self {
            acquisitions: AtomicU64::new(0),
            wait_nanos_total: AtomicU64::new(0),
            wait_nanos_max: AtomicU64::new(0),
            hold_nanos_total: AtomicU64::new(0),
            hold_nanos_max: AtomicU64::new(0),
            wait_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            hold_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl LockProfileState {
    #[inline]
    pub(super) fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Release);
    }

    #[inline]
    pub(super) fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    #[inline]
    pub(super) fn acquisition_started(&self) -> Option<Instant> {
        self.enabled().then(Instant::now)
    }

    #[inline]
    pub(super) fn finish_acquisition(
        &self,
        started_at: Option<Instant>,
        default_class: LockProfileClass,
        kind: LockProfileKind,
        guard_count: usize,
    ) -> Option<LockProfileHold<'_>> {
        let started_at = started_at?;
        let acquired_at = Instant::now();
        let class = current_class_or(default_class);
        let wait_nanos = elapsed_nanos(started_at, acquired_at);
        self.record_wait(class, kind, wait_nanos, guard_count);
        Some(LockProfileHold {
            state: self,
            class,
            kind,
            acquired_at,
        })
    }

    #[inline]
    pub(super) fn record_retry(&self, class: LockProfileClass) {
        if self.enabled() {
            self.classes[class.index()]
                .retries
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(super) fn record_revalidation_failure(&self, class: LockProfileClass) {
        if self.enabled() {
            self.classes[class.index()]
                .revalidation_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn snapshot(&self) -> LockProfileSnapshot {
        LockProfileSnapshot {
            enabled: self.enabled(),
            classes: std::array::from_fn(|index| {
                let class = LockProfileClass::from_index(index);
                let counters = &self.classes[index];
                LockProfileClassSnapshot {
                    class,
                    read: counters.read.snapshot(),
                    write: counters.write.snapshot(),
                    guard_count_total: counters.guard_count_total.load(Ordering::Relaxed),
                    multi_guard_count_total: counters
                        .multi_guard_count_total
                        .load(Ordering::Relaxed),
                    retries: counters.retries.load(Ordering::Relaxed),
                    revalidation_failures: counters.revalidation_failures.load(Ordering::Relaxed),
                }
            }),
        }
    }

    pub(super) fn reset(&self) {
        for counters in &self.classes {
            counters.reset();
        }
    }

    #[inline]
    fn record_wait(
        &self,
        class: LockProfileClass,
        kind: LockProfileKind,
        nanos: u64,
        guard_count: usize,
    ) {
        let counters = &self.classes[class.index()];
        let lock = counters.lock(kind);
        lock.record_wait(nanos);
        counters
            .guard_count_total
            .fetch_add(guard_count as u64, Ordering::Relaxed);
        if guard_count > 1 {
            counters
                .multi_guard_count_total
                .fetch_add(guard_count as u64, Ordering::Relaxed);
        }
    }

    #[inline]
    fn record_hold(&self, class: LockProfileClass, kind: LockProfileKind, nanos: u64) {
        self.classes[class.index()].lock(kind).record_hold(nanos);
    }
}

impl LockProfileScope {
    #[inline]
    pub fn enter(class: LockProfileClass) -> Self {
        let previous = CURRENT_CLASS.with(|current| {
            let previous = current.get();
            current.set(class);
            previous
        });
        Self {
            previous: Some(previous),
        }
    }

    #[inline]
    pub fn disabled() -> Self {
        Self { previous: None }
    }
}

impl LockProfileSnapshot {
    pub fn to_json(&self) -> String {
        let mut json = String::with_capacity(4096);
        json.push_str("{\n");
        json.push_str(&format!("  \"enabled\": {},\n", self.enabled));
        json.push_str("  \"bucket_labels\": [");
        for (index, label) in BUCKET_LABELS.iter().enumerate() {
            if index != 0 {
                json.push_str(", ");
            }
            json.push('"');
            json.push_str(label);
            json.push('"');
        }
        json.push_str("],\n");
        json.push_str("  \"classes\": [\n");
        for (index, class) in self.classes.iter().enumerate() {
            let trailing = if index + 1 == self.classes.len() {
                ""
            } else {
                ","
            };
            json.push_str(&class.to_json(4));
            json.push_str(trailing);
            json.push('\n');
        }
        json.push_str("  ]\n");
        json.push_str("}\n");
        json
    }
}

impl LockProfileClassSnapshot {
    fn to_json(self, indent: usize) -> String {
        let pad = " ".repeat(indent);
        let inner = " ".repeat(indent + 2);
        let mut json = String::with_capacity(512);
        json.push_str(&format!("{pad}{{\n"));
        json.push_str(&format!("{inner}\"class\": \"{}\",\n", self.class.as_str()));
        json.push_str(&format!(
            "{inner}\"guard_count_total\": {},\n",
            self.guard_count_total
        ));
        json.push_str(&format!(
            "{inner}\"multi_guard_count_total\": {},\n",
            self.multi_guard_count_total
        ));
        json.push_str(&format!("{inner}\"retries\": {},\n", self.retries));
        json.push_str(&format!(
            "{inner}\"revalidation_failures\": {},\n",
            self.revalidation_failures
        ));
        json.push_str(&format!("{inner}\"read\": {},\n", self.read.to_json()));
        json.push_str(&format!("{inner}\"write\": {}\n", self.write.to_json()));
        json.push_str(&format!("{pad}}}"));
        json
    }
}

impl LockProfileStatSnapshot {
    fn to_json(self) -> String {
        format!(
            "{{\"acquisitions\": {}, \"wait_nanos_total\": {}, \"wait_nanos_max\": {}, \"hold_nanos_total\": {}, \"hold_nanos_max\": {}, \"wait_buckets\": {}, \"hold_buckets\": {}}}",
            self.acquisitions,
            self.wait_nanos_total,
            self.wait_nanos_max,
            self.hold_nanos_total,
            self.hold_nanos_max,
            array_to_json(&self.wait_buckets),
            array_to_json(&self.hold_buckets)
        )
    }
}

impl Drop for LockProfileScope {
    #[inline]
    fn drop(&mut self) {
        if let Some(previous) = self.previous {
            CURRENT_CLASS.with(|current| current.set(previous));
        }
    }
}

impl Drop for LockProfileHold<'_> {
    #[inline]
    fn drop(&mut self) {
        self.state.record_hold(
            self.class,
            self.kind,
            elapsed_nanos(self.acquired_at, Instant::now()),
        );
    }
}

impl LockClassCounters {
    #[inline]
    fn lock(&self, kind: LockProfileKind) -> &LockCounters {
        match kind {
            LockProfileKind::Read => &self.read,
            LockProfileKind::Write => &self.write,
        }
    }

    fn reset(&self) {
        self.read.reset();
        self.write.reset();
        self.guard_count_total.store(0, Ordering::Relaxed);
        self.multi_guard_count_total.store(0, Ordering::Relaxed);
        self.retries.store(0, Ordering::Relaxed);
        self.revalidation_failures.store(0, Ordering::Relaxed);
    }
}

impl LockCounters {
    #[inline]
    fn record_wait(&self, nanos: u64) {
        self.acquisitions.fetch_add(1, Ordering::Relaxed);
        self.wait_nanos_total.fetch_add(nanos, Ordering::Relaxed);
        fetch_max(&self.wait_nanos_max, nanos);
        self.wait_buckets[bucket_index(nanos)].fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn record_hold(&self, nanos: u64) {
        self.hold_nanos_total.fetch_add(nanos, Ordering::Relaxed);
        fetch_max(&self.hold_nanos_max, nanos);
        self.hold_buckets[bucket_index(nanos)].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LockProfileStatSnapshot {
        LockProfileStatSnapshot {
            acquisitions: self.acquisitions.load(Ordering::Relaxed),
            wait_nanos_total: self.wait_nanos_total.load(Ordering::Relaxed),
            wait_nanos_max: self.wait_nanos_max.load(Ordering::Relaxed),
            hold_nanos_total: self.hold_nanos_total.load(Ordering::Relaxed),
            hold_nanos_max: self.hold_nanos_max.load(Ordering::Relaxed),
            wait_buckets: std::array::from_fn(|idx| self.wait_buckets[idx].load(Ordering::Relaxed)),
            hold_buckets: std::array::from_fn(|idx| self.hold_buckets[idx].load(Ordering::Relaxed)),
        }
    }

    fn reset(&self) {
        self.acquisitions.store(0, Ordering::Relaxed);
        self.wait_nanos_total.store(0, Ordering::Relaxed);
        self.wait_nanos_max.store(0, Ordering::Relaxed);
        self.hold_nanos_total.store(0, Ordering::Relaxed);
        self.hold_nanos_max.store(0, Ordering::Relaxed);
        for bucket in &self.wait_buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        for bucket in &self.hold_buckets {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

#[inline]
fn current_class_or(default_class: LockProfileClass) -> LockProfileClass {
    CURRENT_CLASS.with(|current| {
        let class = current.get();
        if class == LockProfileClass::Unclassified {
            default_class
        } else {
            class
        }
    })
}

#[inline]
fn elapsed_nanos(start: Instant, end: Instant) -> u64 {
    end.saturating_duration_since(start)
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

#[inline]
fn bucket_index(nanos: u64) -> usize {
    match nanos {
        0..=100 => 0,
        101..=500 => 1,
        501..=1_000 => 2,
        1_001..=5_000 => 3,
        5_001..=10_000 => 4,
        10_001..=50_000 => 5,
        50_001..=100_000 => 6,
        100_001..=500_000 => 7,
        500_001..=1_000_000 => 8,
        _ => 9,
    }
}

#[inline]
fn fetch_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

fn array_to_json(values: &[u64; BUCKET_COUNT]) -> String {
    let mut json = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            json.push_str(", ");
        }
        json.push_str(&value.to_string());
    }
    json.push(']');
    json
}

pub const BUCKET_LABELS: [&str; BUCKET_COUNT] = [
    "le_100ns", "le_500ns", "le_1us", "le_5us", "le_10us", "le_50us", "le_100us", "le_500us",
    "le_1ms", "gt_1ms",
];
