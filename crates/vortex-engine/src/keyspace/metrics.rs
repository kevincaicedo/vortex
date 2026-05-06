use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_utils::CachePadded;
use vortex_sync::ShardedCounter;

use super::{ConcurrentKeyspace, EvictionScanReport};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EvictionMetricsSnapshot {
    pub admissions: u64,
    pub shards_scanned: u64,
    pub slots_sampled: u64,
    pub bytes_freed: u64,
    pub oom_after_scan: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RuntimeMetricsSnapshot {
    pub reactor_slots: usize,
    pub loop_iterations: u64,
    pub accept_eagain_rearms: u64,
    pub submit_sq_full_retries: u64,
    pub submit_failures: u64,
    pub completion_batch_count: u64,
    pub completion_batch_total: u64,
    pub completion_batch_max: u64,
    pub completion_batch_avg: f64,
    pub command_batch_count: u64,
    pub command_batch_total: u64,
    pub command_batch_max: u64,
    pub command_batch_avg: f64,
    pub active_expiry_runs: u64,
    pub active_expiry_sampled: u64,
    pub active_expiry_expired: u64,
    pub eviction_admissions: u64,
    pub eviction_shards_scanned: u64,
    pub eviction_slots_sampled: u64,
    pub eviction_bytes_freed: u64,
    pub eviction_oom_after_scan: u64,
}

#[derive(Debug, Default)]
pub(super) struct EvictionMetrics {
    admissions: AtomicU64,
    shards_scanned: AtomicU64,
    slots_sampled: AtomicU64,
    bytes_freed: AtomicU64,
    oom_after_scan: AtomicU64,
}

impl EvictionMetrics {
    #[inline]
    pub(super) fn record(&self, report: EvictionScanReport) {
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
    pub(super) fn snapshot(&self) -> EvictionMetricsSnapshot {
        EvictionMetricsSnapshot {
            admissions: self.admissions.load(Ordering::Relaxed),
            shards_scanned: self.shards_scanned.load(Ordering::Relaxed),
            slots_sampled: self.slots_sampled.load(Ordering::Relaxed),
            bytes_freed: self.bytes_freed.load(Ordering::Relaxed),
            oom_after_scan: self.oom_after_scan.load(Ordering::Relaxed),
        }
    }
}

pub(super) struct RuntimeMetrics {
    loop_iterations: ShardedCounter,
    accept_eagain_rearms: ShardedCounter,
    submit_sq_full_retries: ShardedCounter,
    submit_failures: ShardedCounter,
    completion_batch_count: ShardedCounter,
    completion_batch_total: ShardedCounter,
    command_batch_count: ShardedCounter,
    command_batch_total: ShardedCounter,
    active_expiry_runs: ShardedCounter,
    active_expiry_sampled: ShardedCounter,
    active_expiry_expired: ShardedCounter,
    completion_batch_max: Box<[CachePadded<AtomicU64>]>,
    command_batch_max: Box<[CachePadded<AtomicU64>]>,
}

impl RuntimeMetrics {
    pub(super) fn new(num_slots: usize) -> Self {
        let slot_count = num_slots.max(1);
        Self {
            loop_iterations: ShardedCounter::new(slot_count),
            accept_eagain_rearms: ShardedCounter::new(slot_count),
            submit_sq_full_retries: ShardedCounter::new(slot_count),
            submit_failures: ShardedCounter::new(slot_count),
            completion_batch_count: ShardedCounter::new(slot_count),
            completion_batch_total: ShardedCounter::new(slot_count),
            command_batch_count: ShardedCounter::new(slot_count),
            command_batch_total: ShardedCounter::new(slot_count),
            active_expiry_runs: ShardedCounter::new(slot_count),
            active_expiry_sampled: ShardedCounter::new(slot_count),
            active_expiry_expired: ShardedCounter::new(slot_count),
            completion_batch_max: make_runtime_max_slots(slot_count),
            command_batch_max: make_runtime_max_slots(slot_count),
        }
    }

    pub(super) fn slot_count(&self) -> usize {
        self.completion_batch_max.len()
    }

    #[inline]
    pub(super) fn record_loop_iteration(&self, slot: usize) {
        self.loop_iterations.increment(slot);
    }

    #[inline]
    pub(super) fn record_accept_eagain_rearm(&self, slot: usize) {
        self.accept_eagain_rearms.increment(slot);
    }

    #[inline]
    pub(super) fn record_submit_sq_full_retry(&self, slot: usize) {
        self.submit_sq_full_retries.increment(slot);
    }

    #[inline]
    pub(super) fn record_submit_failure(&self, slot: usize) {
        self.submit_failures.increment(slot);
    }

    #[inline]
    pub(super) fn record_completion_batch(&self, slot: usize, width: usize) {
        if width == 0 {
            return;
        }
        self.completion_batch_count.increment(slot);
        self.completion_batch_total.add(slot, width as u64);
        update_runtime_slot_max(&self.completion_batch_max, slot, width as u64);
    }

    #[inline]
    pub(super) fn record_command_batch(&self, slot: usize, width: usize) {
        if width == 0 {
            return;
        }
        self.command_batch_count.increment(slot);
        self.command_batch_total.add(slot, width as u64);
        update_runtime_slot_max(&self.command_batch_max, slot, width as u64);
    }

    #[inline]
    pub(super) fn record_active_expiry(&self, slot: usize, sampled: usize, expired: usize) {
        self.active_expiry_runs.increment(slot);
        if sampled != 0 {
            self.active_expiry_sampled.add(slot, sampled as u64);
        }
        if expired != 0 {
            self.active_expiry_expired.add(slot, expired as u64);
        }
    }

    #[inline]
    pub(super) fn snapshot(&self, eviction: EvictionMetricsSnapshot) -> RuntimeMetricsSnapshot {
        let completion_batch_count = self.completion_batch_count.total();
        let completion_batch_total = self.completion_batch_total.total();
        let command_batch_count = self.command_batch_count.total();
        let command_batch_total = self.command_batch_total.total();

        RuntimeMetricsSnapshot {
            reactor_slots: self.slot_count(),
            loop_iterations: self.loop_iterations.total(),
            accept_eagain_rearms: self.accept_eagain_rearms.total(),
            submit_sq_full_retries: self.submit_sq_full_retries.total(),
            submit_failures: self.submit_failures.total(),
            completion_batch_count,
            completion_batch_total,
            completion_batch_max: runtime_slot_max(&self.completion_batch_max),
            completion_batch_avg: avg_counter(completion_batch_total, completion_batch_count),
            command_batch_count,
            command_batch_total,
            command_batch_max: runtime_slot_max(&self.command_batch_max),
            command_batch_avg: avg_counter(command_batch_total, command_batch_count),
            active_expiry_runs: self.active_expiry_runs.total(),
            active_expiry_sampled: self.active_expiry_sampled.total(),
            active_expiry_expired: self.active_expiry_expired.total(),
            eviction_admissions: eviction.admissions,
            eviction_shards_scanned: eviction.shards_scanned,
            eviction_slots_sampled: eviction.slots_sampled,
            eviction_bytes_freed: eviction.bytes_freed,
            eviction_oom_after_scan: eviction.oom_after_scan,
        }
    }
}

fn make_runtime_max_slots(num_slots: usize) -> Box<[CachePadded<AtomicU64>]> {
    (0..num_slots)
        .map(|_| CachePadded::new(AtomicU64::new(0)))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

#[inline]
fn update_runtime_slot_max(slots: &[CachePadded<AtomicU64>], slot: usize, value: u64) {
    if let Some(current) = slots.get(slot) {
        current.fetch_max(value, Ordering::Relaxed);
    }
}

#[inline]
fn runtime_slot_max(slots: &[CachePadded<AtomicU64>]) -> u64 {
    slots
        .iter()
        .map(|slot| slot.load(Ordering::Relaxed))
        .max()
        .unwrap_or(0)
}

#[inline]
fn avg_counter(total: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total as f64 / count as f64
    }
}

impl ConcurrentKeyspace {
    #[inline]
    pub fn eviction_metrics(&self) -> EvictionMetricsSnapshot {
        self.eviction_metrics.snapshot()
    }

    #[inline]
    pub fn runtime_metrics(&self) -> RuntimeMetricsSnapshot {
        self.runtime_metrics
            .snapshot(self.eviction_metrics.snapshot())
    }

    #[inline(always)]
    pub fn record_reactor_loop_iteration(&self, reactor_id: usize) {
        self.runtime_metrics.record_loop_iteration(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_accept_eagain_rearm(&self, reactor_id: usize) {
        self.runtime_metrics.record_accept_eagain_rearm(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_submit_sq_full_retry(&self, reactor_id: usize) {
        self.runtime_metrics.record_submit_sq_full_retry(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_submit_failure(&self, reactor_id: usize) {
        self.runtime_metrics.record_submit_failure(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_completion_batch(&self, reactor_id: usize, width: usize) {
        self.runtime_metrics
            .record_completion_batch(reactor_id, width);
    }

    #[inline(always)]
    pub fn record_reactor_command_batch(&self, reactor_id: usize, width: usize) {
        self.runtime_metrics.record_command_batch(reactor_id, width);
    }

    #[inline(always)]
    pub fn record_reactor_active_expiry(&self, reactor_id: usize, sampled: usize, expired: usize) {
        self.runtime_metrics
            .record_active_expiry(reactor_id, sampled, expired);
    }
}
