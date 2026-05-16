use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use crossbeam_utils::CachePadded;
use vortex_sync::ShardedCounter;

use super::{ConcurrentKeyspace, EvictionScanReport};

pub const RUNTIME_AOF_FSYNC_LATENCY_BUCKETS: usize = 8;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EvictionMetricsSnapshot {
    pub admissions: u64,
    pub shards_scanned: u64,
    pub slots_sampled: u64,
    pub bytes_freed: u64,
    pub oom_after_scan: u64,
    pub scan_nanos_total: u64,
    pub scan_nanos_max: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RuntimeBackendMode {
    #[default]
    Unknown,
    Auto,
    Polling,
    IoUring,
    Test,
    Mixed,
}

impl RuntimeBackendMode {
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Auto => "auto",
            Self::Polling => "polling",
            Self::IoUring => "io_uring",
            Self::Test => "test",
            Self::Mixed => "mixed",
        }
    }

    #[inline]
    const fn code(self) -> u8 {
        match self {
            Self::Unknown => 0,
            Self::Auto => 1,
            Self::Polling => 2,
            Self::IoUring => 3,
            Self::Test => 4,
            Self::Mixed => 5,
        }
    }

    #[inline]
    const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::Auto,
            2 => Self::Polling,
            3 => Self::IoUring,
            4 => Self::Test,
            5 => Self::Mixed,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RuntimeTelemetryMode {
    #[default]
    Minimal,
    #[cfg(feature = "profile-telemetry")]
    Profile,
}

impl RuntimeTelemetryMode {
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Minimal => "minimal",
            #[cfg(feature = "profile-telemetry")]
            Self::Profile => "profile",
        }
    }

    #[inline]
    const fn code(self) -> u8 {
        match self {
            Self::Minimal => 0,
            #[cfg(feature = "profile-telemetry")]
            Self::Profile => 1,
        }
    }

    #[inline]
    const fn from_code(code: u8) -> Self {
        #[cfg(feature = "profile-telemetry")]
        match code {
            1 => Self::Profile,
            _ => Self::Minimal,
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            let _ = code;
            Self::Minimal
        }
    }

    #[inline]
    pub const fn profile_timers_enabled(self) -> bool {
        #[cfg(feature = "profile-telemetry")]
        {
            matches!(self, Self::Profile)
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            let _ = self;
            false
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeBackendSnapshot {
    pub requested: RuntimeBackendMode,
    pub effective: RuntimeBackendMode,
    pub mixed: bool,
    pub fixed_buffers_capable: bool,
    pub fixed_buffers_registered: bool,
    pub sqpoll: bool,
    pub multishot_accept: bool,
    pub accept4: bool,
    pub close_opcode: bool,
    pub cancel_support: bool,
    pub nonblocking_drain: bool,
    pub requested_ring_size: u64,
    pub effective_ring_size: u64,
}

impl RuntimeBackendSnapshot {
    #[inline]
    fn same_contract(self, other: Self) -> bool {
        self.requested == other.requested
            && self.effective == other.effective
            && self.fixed_buffers_capable == other.fixed_buffers_capable
            && self.fixed_buffers_registered == other.fixed_buffers_registered
            && self.sqpoll == other.sqpoll
            && self.multishot_accept == other.multishot_accept
            && self.accept4 == other.accept4
            && self.close_opcode == other.close_opcode
            && self.cancel_support == other.cancel_support
            && self.nonblocking_drain == other.nonblocking_drain
            && self.requested_ring_size == other.requested_ring_size
            && self.effective_ring_size == other.effective_ring_size
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeAofTelemetry {
    pub pending_bytes: u64,
    pub pending_writes: u64,
    pub fsync_requested: u64,
    pub fsync_completed: u64,
    pub fsync_failed: u64,
    pub fsync_worker_saturation: u64,
    pub backpressure_events: u64,
    pub backpressure_nanos_total: u64,
    pub backpressure_nanos_max: u64,
    pub last_appended_lsn: u64,
    pub last_durable_lsn: u64,
    pub fsync_latency_nanos_total: u64,
    pub fsync_latency_nanos_max: u64,
    pub fsync_latency_buckets: [u64; RUNTIME_AOF_FSYNC_LATENCY_BUCKETS],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeOverloadTelemetry {
    pub pending_response_bytes: u64,
    pub pending_response_bytes_peak: u64,
    pub parser_accumulator_bytes: u64,
    pub parser_accumulator_bytes_peak: u64,
    pub writev_backlog_bytes: u64,
    pub writev_backlog_bytes_peak: u64,
    pub aof_pending_bytes: u64,
    pub maintenance_debt: u64,
    pub maintenance_debt_peak: u64,
    pub read_disabled_connections: u64,
    pub read_disabled_connections_peak: u64,
    pub deferred_commands: u64,
    pub deferred_commands_peak: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeSharedNothingTelemetry {
    pub accepted_remote: u64,
    pub remote_backpressure: u64,
    pub reply_backpressure: u64,
    pub deferred_replies: u64,
    pub wakeups_sent: u64,
    pub wakeup_failures: u64,
    pub aggregates_accepted: u64,
    pub aggregate_width_max: u64,
    pub txns_accepted: u64,
    pub txn_prepare_messages: u64,
    pub txn_commit_messages: u64,
    pub txn_abort_messages: u64,
    pub txn_condition_aborts: u64,
    pub txn_conflict_aborts: u64,
    pub prepared_key_waits: u64,
    pub prepared_key_retries: u64,
    pub prepared_key_wait_nanos_total: u64,
    pub prepared_key_wait_nanos_max: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeLocalFlushMetrics {
    pub loop_iterations: u64,
    pub accept_drain_runs: u64,
    pub accept_drain_accepted: u64,
    pub accept_drain_accepted_max: u64,
    pub completion_batch_count: u64,
    pub completion_batch_total: u64,
    pub completion_batch_max: u64,
    pub command_batch_count: u64,
    pub command_batch_total: u64,
    pub command_batch_max: u64,
    pub writev_chunks: u64,
    pub writev_iovecs_total: u64,
    pub writev_iovecs_max: u64,
    pub queued_response_bytes_total: u64,
    pub queued_response_bytes_max: u64,
}

impl RuntimeLocalFlushMetrics {
    #[inline]
    pub const fn is_empty(self) -> bool {
        self.loop_iterations == 0
            && self.accept_drain_runs == 0
            && self.accept_drain_accepted == 0
            && self.completion_batch_count == 0
            && self.command_batch_count == 0
            && self.writev_chunks == 0
            && self.queued_response_bytes_total == 0
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RuntimeMetricsSnapshot {
    pub telemetry_mode: RuntimeTelemetryMode,
    pub profile_timers_available: bool,
    pub local_flush_metrics_available: bool,
    pub reactor_slots: usize,
    pub backend: RuntimeBackendSnapshot,
    pub loop_iterations: u64,
    pub accept_eagain_rearms: u64,
    pub accept_drain_runs: u64,
    pub accept_drain_accepted: u64,
    pub accept_drain_accepted_max: u64,
    pub backend_submit_syscalls: u64,
    pub backend_sq_occupancy_max: u64,
    pub backend_sq_capacity: u64,
    pub backend_cq_occupancy_max: u64,
    pub backend_cq_capacity: u64,
    pub backend_cq_overflows: u64,
    pub backend_completions_per_submit_syscall_x1000: u64,
    pub submit_sq_full_retries: u64,
    pub submit_failures: u64,
    pub completion_budget_exhaustions: u64,
    pub command_budget_exhaustions: u64,
    pub accept_budget_exhaustions: u64,
    pub writev_budget_exhaustions: u64,
    pub maintenance_budget_exhaustions: u64,
    pub yielded_connections: u64,
    pub parser_resumes: u64,
    pub completion_batch_count: u64,
    pub completion_batch_total: u64,
    pub completion_batch_max: u64,
    pub completion_batch_avg: f64,
    pub completion_nanos_total: u64,
    pub completion_nanos_max: u64,
    pub command_batch_count: u64,
    pub command_batch_total: u64,
    pub command_batch_max: u64,
    pub command_batch_avg: f64,
    pub writev_chunks: u64,
    pub writev_iovecs_total: u64,
    pub writev_iovecs_max: u64,
    pub queued_response_bytes_total: u64,
    pub queued_response_bytes_max: u64,
    pub client_retained_bytes: u64,
    pub client_retained_bytes_max: u64,
    pub client_retained_bytes_peak: u64,
    pub request_cap_exceeded: u64,
    pub response_cap_exceeded: u64,
    pub multi_queue_command_cap_exceeded: u64,
    pub multi_queue_bytes_cap_exceeded: u64,
    pub watch_cap_exceeded: u64,
    pub writev_chunk_cap_exceeded: u64,
    pub overload_accept_throttled: u64,
    pub overload_accept_resumed: u64,
    pub overload_read_disabled: u64,
    pub overload_read_resumed: u64,
    pub overload_command_deferred: u64,
    pub overload_command_resumed: u64,
    pub overload_connections_dropped: u64,
    pub overload_pending_response_bytes: u64,
    pub overload_pending_response_bytes_peak: u64,
    pub overload_parser_accumulator_bytes: u64,
    pub overload_parser_accumulator_bytes_peak: u64,
    pub overload_writev_backlog_bytes: u64,
    pub overload_writev_backlog_bytes_peak: u64,
    pub overload_aof_pending_bytes: u64,
    pub overload_aof_pending_bytes_peak: u64,
    pub overload_maintenance_debt: u64,
    pub overload_maintenance_debt_peak: u64,
    pub overload_read_disabled_connections: u64,
    pub overload_read_disabled_connections_peak: u64,
    pub overload_deferred_commands: u64,
    pub overload_deferred_commands_peak: u64,
    pub shared_nothing_accepted_remote: u64,
    pub shared_nothing_remote_backpressure: u64,
    pub shared_nothing_reply_backpressure: u64,
    pub shared_nothing_deferred_replies: u64,
    pub shared_nothing_wakeups_sent: u64,
    pub shared_nothing_wakeup_failures: u64,
    pub shared_nothing_aggregates_accepted: u64,
    pub shared_nothing_aggregate_width_max: u64,
    pub shared_nothing_txns_accepted: u64,
    pub shared_nothing_txn_prepare_messages: u64,
    pub shared_nothing_txn_commit_messages: u64,
    pub shared_nothing_txn_abort_messages: u64,
    pub shared_nothing_txn_condition_aborts: u64,
    pub shared_nothing_txn_conflict_aborts: u64,
    pub shared_nothing_prepared_key_waits: u64,
    pub shared_nothing_prepared_key_retries: u64,
    pub shared_nothing_prepared_key_wait_nanos_total: u64,
    pub shared_nothing_prepared_key_wait_nanos_max: u64,
    pub close_drain_nanos_total: u64,
    pub close_drain_nanos_max: u64,
    pub active_expiry_runs: u64,
    pub active_expiry_sampled: u64,
    pub active_expiry_expired: u64,
    pub active_expiry_nanos_total: u64,
    pub active_expiry_nanos_max: u64,
    pub aof_append_nanos_total: u64,
    pub aof_append_nanos_max: u64,
    pub aof_fsync_nanos_total: u64,
    pub aof_fsync_nanos_max: u64,
    pub aof_pending_bytes: u64,
    pub aof_pending_bytes_max: u64,
    pub aof_pending_writes: u64,
    pub aof_pending_writes_max: u64,
    pub aof_fsync_requested: u64,
    pub aof_fsync_completed: u64,
    pub aof_fsync_failed: u64,
    pub aof_fsync_worker_saturation: u64,
    pub aof_backpressure_events: u64,
    pub aof_backpressure_nanos_total: u64,
    pub aof_backpressure_nanos_max: u64,
    pub aof_last_appended_lsn: u64,
    pub aof_last_durable_lsn: u64,
    pub aof_durable_lsn_lag: u64,
    pub aof_fsync_latency_nanos_total: u64,
    pub aof_fsync_latency_nanos_max: u64,
    pub aof_fsync_latency_buckets: [u64; RUNTIME_AOF_FSYNC_LATENCY_BUCKETS],
    pub maintenance_nanos_total: u64,
    pub maintenance_nanos_max: u64,
    pub metrics_flush_nanos_total: u64,
    pub metrics_flush_nanos_max: u64,
    pub eviction_admissions: u64,
    pub eviction_shards_scanned: u64,
    pub eviction_slots_sampled: u64,
    pub eviction_bytes_freed: u64,
    pub eviction_oom_after_scan: u64,
    pub eviction_nanos_total: u64,
    pub eviction_nanos_max: u64,
}

#[derive(Debug, Default)]
pub(super) struct EvictionMetrics {
    admissions: AtomicU64,
    shards_scanned: AtomicU64,
    slots_sampled: AtomicU64,
    bytes_freed: AtomicU64,
    oom_after_scan: AtomicU64,
    #[cfg(feature = "profile-telemetry")]
    scan_nanos_total: AtomicU64,
    #[cfg(feature = "profile-telemetry")]
    scan_nanos_max: AtomicU64,
}

impl EvictionMetrics {
    #[inline]
    pub(super) fn record(&self, report: EvictionScanReport) {
        self.record_with_duration(report, 0);
    }

    #[inline]
    pub(super) fn record_with_duration(&self, report: EvictionScanReport, scan_nanos: u64) {
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
        #[cfg(feature = "profile-telemetry")]
        if scan_nanos != 0 {
            self.scan_nanos_total
                .fetch_add(scan_nanos, Ordering::Relaxed);
            self.scan_nanos_max.fetch_max(scan_nanos, Ordering::Relaxed);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        let _ = scan_nanos;
    }

    #[inline]
    pub(super) fn snapshot(&self) -> EvictionMetricsSnapshot {
        EvictionMetricsSnapshot {
            admissions: self.admissions.load(Ordering::Relaxed),
            shards_scanned: self.shards_scanned.load(Ordering::Relaxed),
            slots_sampled: self.slots_sampled.load(Ordering::Relaxed),
            bytes_freed: self.bytes_freed.load(Ordering::Relaxed),
            oom_after_scan: self.oom_after_scan.load(Ordering::Relaxed),
            scan_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.scan_nanos_total.load(Ordering::Relaxed)
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            scan_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.scan_nanos_max.load(Ordering::Relaxed)
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
        }
    }
}

pub(super) struct RuntimeMetrics {
    telemetry_mode: AtomicU8,
    backend_requested: AtomicU8,
    backend_effective: AtomicU8,
    backend_plan_mixed: AtomicBool,
    backend_fixed_buffers_capable: AtomicBool,
    backend_fixed_buffers_registered: AtomicBool,
    backend_sqpoll: AtomicBool,
    backend_multishot_accept: AtomicBool,
    backend_accept4: AtomicBool,
    backend_close_opcode: AtomicBool,
    backend_cancel_support: AtomicBool,
    backend_nonblocking_drain: AtomicBool,
    backend_requested_ring_size: AtomicU64,
    backend_effective_ring_size: AtomicU64,
    backend_submit_syscalls: ShardedCounter,
    backend_cq_overflows: ShardedCounter,
    loop_iterations: ShardedCounter,
    accept_eagain_rearms: ShardedCounter,
    accept_drain_runs: ShardedCounter,
    accept_drain_accepted: ShardedCounter,
    submit_sq_full_retries: ShardedCounter,
    submit_failures: ShardedCounter,
    completion_budget_exhaustions: ShardedCounter,
    command_budget_exhaustions: ShardedCounter,
    accept_budget_exhaustions: ShardedCounter,
    writev_budget_exhaustions: ShardedCounter,
    maintenance_budget_exhaustions: ShardedCounter,
    yielded_connections: ShardedCounter,
    parser_resumes: ShardedCounter,
    completion_batch_count: ShardedCounter,
    completion_batch_total: ShardedCounter,
    #[cfg(feature = "profile-telemetry")]
    completion_nanos_total: ShardedCounter,
    command_batch_count: ShardedCounter,
    command_batch_total: ShardedCounter,
    writev_chunks: ShardedCounter,
    writev_iovecs_total: ShardedCounter,
    queued_response_bytes_total: ShardedCounter,
    request_cap_exceeded: ShardedCounter,
    response_cap_exceeded: ShardedCounter,
    multi_queue_command_cap_exceeded: ShardedCounter,
    multi_queue_bytes_cap_exceeded: ShardedCounter,
    watch_cap_exceeded: ShardedCounter,
    writev_chunk_cap_exceeded: ShardedCounter,
    overload_accept_throttled: ShardedCounter,
    overload_accept_resumed: ShardedCounter,
    overload_read_disabled: ShardedCounter,
    overload_read_resumed: ShardedCounter,
    overload_command_deferred: ShardedCounter,
    overload_command_resumed: ShardedCounter,
    overload_connections_dropped: ShardedCounter,
    #[cfg(feature = "profile-telemetry")]
    close_drain_nanos_total: ShardedCounter,
    active_expiry_runs: ShardedCounter,
    active_expiry_sampled: ShardedCounter,
    active_expiry_expired: ShardedCounter,
    #[cfg(feature = "profile-telemetry")]
    active_expiry_nanos_total: ShardedCounter,
    #[cfg(feature = "profile-telemetry")]
    aof_append_nanos_total: ShardedCounter,
    #[cfg(feature = "profile-telemetry")]
    aof_fsync_nanos_total: ShardedCounter,
    aof_pending_bytes: RuntimeGaugeSlots,
    aof_pending_writes: RuntimeGaugeSlots,
    aof_fsync_requested: RuntimeGaugeSlots,
    aof_fsync_completed: RuntimeGaugeSlots,
    aof_fsync_failed: RuntimeGaugeSlots,
    aof_fsync_worker_saturation: RuntimeGaugeSlots,
    aof_backpressure_events: RuntimeGaugeSlots,
    #[cfg(feature = "profile-telemetry")]
    aof_backpressure_nanos_total: RuntimeGaugeSlots,
    aof_last_appended_lsn: RuntimeGaugeSlots,
    aof_last_durable_lsn: RuntimeGaugeSlots,
    #[cfg(feature = "profile-telemetry")]
    aof_fsync_latency_nanos_total: RuntimeGaugeSlots,
    #[cfg(feature = "profile-telemetry")]
    aof_fsync_latency_buckets: [RuntimeGaugeSlots; RUNTIME_AOF_FSYNC_LATENCY_BUCKETS],
    client_retained_bytes: RuntimeGaugeSlots,
    client_retained_bytes_max: RuntimeGaugeSlots,
    overload_pending_response_bytes: RuntimeGaugeSlots,
    overload_parser_accumulator_bytes: RuntimeGaugeSlots,
    overload_writev_backlog_bytes: RuntimeGaugeSlots,
    overload_aof_pending_bytes: RuntimeGaugeSlots,
    overload_maintenance_debt: RuntimeGaugeSlots,
    overload_read_disabled_connections: RuntimeGaugeSlots,
    overload_deferred_commands: RuntimeGaugeSlots,
    shared_nothing_accepted_remote: RuntimeGaugeSlots,
    shared_nothing_remote_backpressure: RuntimeGaugeSlots,
    shared_nothing_reply_backpressure: RuntimeGaugeSlots,
    shared_nothing_deferred_replies: RuntimeGaugeSlots,
    shared_nothing_wakeups_sent: RuntimeGaugeSlots,
    shared_nothing_wakeup_failures: RuntimeGaugeSlots,
    shared_nothing_aggregates_accepted: RuntimeGaugeSlots,
    shared_nothing_aggregate_width_max: RuntimeGaugeSlots,
    shared_nothing_txns_accepted: RuntimeGaugeSlots,
    shared_nothing_txn_prepare_messages: RuntimeGaugeSlots,
    shared_nothing_txn_commit_messages: RuntimeGaugeSlots,
    shared_nothing_txn_abort_messages: RuntimeGaugeSlots,
    shared_nothing_txn_condition_aborts: RuntimeGaugeSlots,
    shared_nothing_txn_conflict_aborts: RuntimeGaugeSlots,
    shared_nothing_prepared_key_waits: RuntimeGaugeSlots,
    shared_nothing_prepared_key_retries: RuntimeGaugeSlots,
    shared_nothing_prepared_key_wait_nanos_total: RuntimeGaugeSlots,
    shared_nothing_prepared_key_wait_nanos_max: RuntimeMaxSlots,
    #[cfg(feature = "profile-telemetry")]
    maintenance_nanos_total: ShardedCounter,
    #[cfg(feature = "profile-telemetry")]
    metrics_flush_nanos_total: ShardedCounter,
    completion_batch_max: RuntimeMaxSlots,
    backend_sq_occupancy_max: RuntimeMaxSlots,
    backend_sq_capacity: RuntimeMaxSlots,
    backend_cq_occupancy_max: RuntimeMaxSlots,
    backend_cq_capacity: RuntimeMaxSlots,
    command_batch_max: RuntimeMaxSlots,
    accept_drain_accepted_max: RuntimeMaxSlots,
    #[cfg(feature = "profile-telemetry")]
    completion_nanos_max: RuntimeMaxSlots,
    writev_iovecs_max: RuntimeMaxSlots,
    queued_response_bytes_max: RuntimeMaxSlots,
    client_retained_bytes_peak: RuntimeMaxSlots,
    overload_pending_response_bytes_peak: RuntimeMaxSlots,
    overload_parser_accumulator_bytes_peak: RuntimeMaxSlots,
    overload_writev_backlog_bytes_peak: RuntimeMaxSlots,
    overload_aof_pending_bytes_peak: RuntimeMaxSlots,
    overload_maintenance_debt_peak: RuntimeMaxSlots,
    overload_read_disabled_connections_peak: RuntimeMaxSlots,
    overload_deferred_commands_peak: RuntimeMaxSlots,
    #[cfg(feature = "profile-telemetry")]
    close_drain_nanos_max: RuntimeMaxSlots,
    #[cfg(feature = "profile-telemetry")]
    active_expiry_nanos_max: RuntimeMaxSlots,
    #[cfg(feature = "profile-telemetry")]
    aof_append_nanos_max: RuntimeMaxSlots,
    #[cfg(feature = "profile-telemetry")]
    aof_fsync_nanos_max: RuntimeMaxSlots,
    aof_pending_bytes_max: RuntimeMaxSlots,
    aof_pending_writes_max: RuntimeMaxSlots,
    #[cfg(feature = "profile-telemetry")]
    aof_backpressure_nanos_max: RuntimeMaxSlots,
    #[cfg(feature = "profile-telemetry")]
    aof_fsync_latency_nanos_max: RuntimeMaxSlots,
    #[cfg(feature = "profile-telemetry")]
    maintenance_nanos_max: RuntimeMaxSlots,
    #[cfg(feature = "profile-telemetry")]
    metrics_flush_nanos_max: RuntimeMaxSlots,
}

impl RuntimeMetrics {
    pub(super) fn new(num_slots: usize) -> Self {
        let slot_count = num_slots.max(1);
        Self {
            telemetry_mode: AtomicU8::new(RuntimeTelemetryMode::Minimal.code()),
            backend_requested: AtomicU8::new(RuntimeBackendMode::Unknown.code()),
            backend_effective: AtomicU8::new(RuntimeBackendMode::Unknown.code()),
            backend_plan_mixed: AtomicBool::new(false),
            backend_fixed_buffers_capable: AtomicBool::new(false),
            backend_fixed_buffers_registered: AtomicBool::new(false),
            backend_sqpoll: AtomicBool::new(false),
            backend_multishot_accept: AtomicBool::new(false),
            backend_accept4: AtomicBool::new(false),
            backend_close_opcode: AtomicBool::new(false),
            backend_cancel_support: AtomicBool::new(false),
            backend_nonblocking_drain: AtomicBool::new(false),
            backend_requested_ring_size: AtomicU64::new(0),
            backend_effective_ring_size: AtomicU64::new(0),
            backend_submit_syscalls: ShardedCounter::new(slot_count),
            backend_cq_overflows: ShardedCounter::new(slot_count),
            loop_iterations: ShardedCounter::new(slot_count),
            accept_eagain_rearms: ShardedCounter::new(slot_count),
            accept_drain_runs: ShardedCounter::new(slot_count),
            accept_drain_accepted: ShardedCounter::new(slot_count),
            submit_sq_full_retries: ShardedCounter::new(slot_count),
            submit_failures: ShardedCounter::new(slot_count),
            completion_budget_exhaustions: ShardedCounter::new(slot_count),
            command_budget_exhaustions: ShardedCounter::new(slot_count),
            accept_budget_exhaustions: ShardedCounter::new(slot_count),
            writev_budget_exhaustions: ShardedCounter::new(slot_count),
            maintenance_budget_exhaustions: ShardedCounter::new(slot_count),
            yielded_connections: ShardedCounter::new(slot_count),
            parser_resumes: ShardedCounter::new(slot_count),
            completion_batch_count: ShardedCounter::new(slot_count),
            completion_batch_total: ShardedCounter::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            completion_nanos_total: ShardedCounter::new(slot_count),
            command_batch_count: ShardedCounter::new(slot_count),
            command_batch_total: ShardedCounter::new(slot_count),
            writev_chunks: ShardedCounter::new(slot_count),
            writev_iovecs_total: ShardedCounter::new(slot_count),
            queued_response_bytes_total: ShardedCounter::new(slot_count),
            request_cap_exceeded: ShardedCounter::new(slot_count),
            response_cap_exceeded: ShardedCounter::new(slot_count),
            multi_queue_command_cap_exceeded: ShardedCounter::new(slot_count),
            multi_queue_bytes_cap_exceeded: ShardedCounter::new(slot_count),
            watch_cap_exceeded: ShardedCounter::new(slot_count),
            writev_chunk_cap_exceeded: ShardedCounter::new(slot_count),
            overload_accept_throttled: ShardedCounter::new(slot_count),
            overload_accept_resumed: ShardedCounter::new(slot_count),
            overload_read_disabled: ShardedCounter::new(slot_count),
            overload_read_resumed: ShardedCounter::new(slot_count),
            overload_command_deferred: ShardedCounter::new(slot_count),
            overload_command_resumed: ShardedCounter::new(slot_count),
            overload_connections_dropped: ShardedCounter::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            close_drain_nanos_total: ShardedCounter::new(slot_count),
            active_expiry_runs: ShardedCounter::new(slot_count),
            active_expiry_sampled: ShardedCounter::new(slot_count),
            active_expiry_expired: ShardedCounter::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            active_expiry_nanos_total: ShardedCounter::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            aof_append_nanos_total: ShardedCounter::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            aof_fsync_nanos_total: ShardedCounter::new(slot_count),
            aof_pending_bytes: RuntimeGaugeSlots::new(slot_count),
            aof_pending_writes: RuntimeGaugeSlots::new(slot_count),
            aof_fsync_requested: RuntimeGaugeSlots::new(slot_count),
            aof_fsync_completed: RuntimeGaugeSlots::new(slot_count),
            aof_fsync_failed: RuntimeGaugeSlots::new(slot_count),
            aof_fsync_worker_saturation: RuntimeGaugeSlots::new(slot_count),
            aof_backpressure_events: RuntimeGaugeSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            aof_backpressure_nanos_total: RuntimeGaugeSlots::new(slot_count),
            aof_last_appended_lsn: RuntimeGaugeSlots::new(slot_count),
            aof_last_durable_lsn: RuntimeGaugeSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            aof_fsync_latency_nanos_total: RuntimeGaugeSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            aof_fsync_latency_buckets: std::array::from_fn(|_| RuntimeGaugeSlots::new(slot_count)),
            client_retained_bytes: RuntimeGaugeSlots::new(slot_count),
            client_retained_bytes_max: RuntimeGaugeSlots::new(slot_count),
            overload_pending_response_bytes: RuntimeGaugeSlots::new(slot_count),
            overload_parser_accumulator_bytes: RuntimeGaugeSlots::new(slot_count),
            overload_writev_backlog_bytes: RuntimeGaugeSlots::new(slot_count),
            overload_aof_pending_bytes: RuntimeGaugeSlots::new(slot_count),
            overload_maintenance_debt: RuntimeGaugeSlots::new(slot_count),
            overload_read_disabled_connections: RuntimeGaugeSlots::new(slot_count),
            overload_deferred_commands: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_accepted_remote: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_remote_backpressure: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_reply_backpressure: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_deferred_replies: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_wakeups_sent: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_wakeup_failures: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_aggregates_accepted: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_aggregate_width_max: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_txns_accepted: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_txn_prepare_messages: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_txn_commit_messages: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_txn_abort_messages: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_txn_condition_aborts: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_txn_conflict_aborts: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_prepared_key_waits: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_prepared_key_retries: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_prepared_key_wait_nanos_total: RuntimeGaugeSlots::new(slot_count),
            shared_nothing_prepared_key_wait_nanos_max: RuntimeMaxSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            maintenance_nanos_total: ShardedCounter::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            metrics_flush_nanos_total: ShardedCounter::new(slot_count),
            completion_batch_max: RuntimeMaxSlots::new(slot_count),
            backend_sq_occupancy_max: RuntimeMaxSlots::new(slot_count),
            backend_sq_capacity: RuntimeMaxSlots::new(slot_count),
            backend_cq_occupancy_max: RuntimeMaxSlots::new(slot_count),
            backend_cq_capacity: RuntimeMaxSlots::new(slot_count),
            command_batch_max: RuntimeMaxSlots::new(slot_count),
            accept_drain_accepted_max: RuntimeMaxSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            completion_nanos_max: RuntimeMaxSlots::new(slot_count),
            writev_iovecs_max: RuntimeMaxSlots::new(slot_count),
            queued_response_bytes_max: RuntimeMaxSlots::new(slot_count),
            client_retained_bytes_peak: RuntimeMaxSlots::new(slot_count),
            overload_pending_response_bytes_peak: RuntimeMaxSlots::new(slot_count),
            overload_parser_accumulator_bytes_peak: RuntimeMaxSlots::new(slot_count),
            overload_writev_backlog_bytes_peak: RuntimeMaxSlots::new(slot_count),
            overload_aof_pending_bytes_peak: RuntimeMaxSlots::new(slot_count),
            overload_maintenance_debt_peak: RuntimeMaxSlots::new(slot_count),
            overload_read_disabled_connections_peak: RuntimeMaxSlots::new(slot_count),
            overload_deferred_commands_peak: RuntimeMaxSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            close_drain_nanos_max: RuntimeMaxSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            active_expiry_nanos_max: RuntimeMaxSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            aof_append_nanos_max: RuntimeMaxSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            aof_fsync_nanos_max: RuntimeMaxSlots::new(slot_count),
            aof_pending_bytes_max: RuntimeMaxSlots::new(slot_count),
            aof_pending_writes_max: RuntimeMaxSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            aof_backpressure_nanos_max: RuntimeMaxSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            aof_fsync_latency_nanos_max: RuntimeMaxSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            maintenance_nanos_max: RuntimeMaxSlots::new(slot_count),
            #[cfg(feature = "profile-telemetry")]
            metrics_flush_nanos_max: RuntimeMaxSlots::new(slot_count),
        }
    }

    pub(super) fn slot_count(&self) -> usize {
        self.completion_batch_max.len()
    }

    #[inline]
    pub(super) fn set_telemetry_mode(&self, mode: RuntimeTelemetryMode) {
        self.telemetry_mode.store(mode.code(), Ordering::Relaxed);
    }

    #[inline]
    pub(super) fn telemetry_mode(&self) -> RuntimeTelemetryMode {
        RuntimeTelemetryMode::from_code(self.telemetry_mode.load(Ordering::Relaxed))
    }

    #[inline]
    pub(super) fn publish_backend(&self, snapshot: RuntimeBackendSnapshot) {
        let current = self.backend_snapshot();
        if current.requested != RuntimeBackendMode::Unknown && !current.same_contract(snapshot) {
            self.backend_plan_mixed.store(true, Ordering::Relaxed);
            self.backend_effective
                .store(RuntimeBackendMode::Mixed.code(), Ordering::Relaxed);
            return;
        }

        self.backend_requested
            .store(snapshot.requested.code(), Ordering::Relaxed);
        self.backend_effective
            .store(snapshot.effective.code(), Ordering::Relaxed);
        self.backend_plan_mixed
            .store(snapshot.mixed, Ordering::Relaxed);
        self.backend_fixed_buffers_capable
            .store(snapshot.fixed_buffers_capable, Ordering::Relaxed);
        self.backend_fixed_buffers_registered
            .store(snapshot.fixed_buffers_registered, Ordering::Relaxed);
        self.backend_sqpoll
            .store(snapshot.sqpoll, Ordering::Relaxed);
        self.backend_multishot_accept
            .store(snapshot.multishot_accept, Ordering::Relaxed);
        self.backend_accept4
            .store(snapshot.accept4, Ordering::Relaxed);
        self.backend_close_opcode
            .store(snapshot.close_opcode, Ordering::Relaxed);
        self.backend_cancel_support
            .store(snapshot.cancel_support, Ordering::Relaxed);
        self.backend_nonblocking_drain
            .store(snapshot.nonblocking_drain, Ordering::Relaxed);
        self.backend_requested_ring_size
            .store(snapshot.requested_ring_size, Ordering::Relaxed);
        self.backend_effective_ring_size
            .store(snapshot.effective_ring_size, Ordering::Relaxed);
    }

    #[inline]
    fn backend_snapshot(&self) -> RuntimeBackendSnapshot {
        RuntimeBackendSnapshot {
            requested: RuntimeBackendMode::from_code(
                self.backend_requested.load(Ordering::Relaxed),
            ),
            effective: RuntimeBackendMode::from_code(
                self.backend_effective.load(Ordering::Relaxed),
            ),
            mixed: self.backend_plan_mixed.load(Ordering::Relaxed),
            fixed_buffers_capable: self.backend_fixed_buffers_capable.load(Ordering::Relaxed),
            fixed_buffers_registered: self
                .backend_fixed_buffers_registered
                .load(Ordering::Relaxed),
            sqpoll: self.backend_sqpoll.load(Ordering::Relaxed),
            multishot_accept: self.backend_multishot_accept.load(Ordering::Relaxed),
            accept4: self.backend_accept4.load(Ordering::Relaxed),
            close_opcode: self.backend_close_opcode.load(Ordering::Relaxed),
            cancel_support: self.backend_cancel_support.load(Ordering::Relaxed),
            nonblocking_drain: self.backend_nonblocking_drain.load(Ordering::Relaxed),
            requested_ring_size: self.backend_requested_ring_size.load(Ordering::Relaxed),
            effective_ring_size: self.backend_effective_ring_size.load(Ordering::Relaxed),
        }
    }

    #[inline]
    pub(super) fn record_backend_submit_syscall(&self, slot: usize) {
        self.backend_submit_syscalls.increment(slot);
    }

    #[inline]
    pub(super) fn record_backend_queue_status(
        &self,
        slot: usize,
        sq_occupancy: u64,
        sq_capacity: u64,
        cq_occupancy: u64,
        cq_capacity: u64,
        cq_overflow_delta: u64,
    ) {
        if sq_occupancy != 0 {
            self.backend_sq_occupancy_max.record(slot, sq_occupancy);
        }
        if sq_capacity != 0 {
            self.backend_sq_capacity.record(slot, sq_capacity);
        }
        if cq_occupancy != 0 {
            self.backend_cq_occupancy_max.record(slot, cq_occupancy);
        }
        if cq_capacity != 0 {
            self.backend_cq_capacity.record(slot, cq_capacity);
        }
        if cq_overflow_delta != 0 {
            self.backend_cq_overflows.add(slot, cq_overflow_delta);
        }
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
    pub(super) fn record_accept_drain(&self, slot: usize, accepted: usize) {
        self.accept_drain_runs.increment(slot);
        if accepted != 0 {
            self.accept_drain_accepted.add(slot, accepted as u64);
            self.accept_drain_accepted_max.record(slot, accepted as u64);
        }
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
    pub(super) fn record_completion_budget_exhaustion(&self, slot: usize) {
        self.completion_budget_exhaustions.increment(slot);
    }

    #[inline]
    pub(super) fn record_command_budget_exhaustion(&self, slot: usize) {
        self.command_budget_exhaustions.increment(slot);
    }

    #[inline]
    pub(super) fn record_accept_budget_exhaustion(&self, slot: usize) {
        self.accept_budget_exhaustions.increment(slot);
    }

    #[inline]
    pub(super) fn record_writev_budget_exhaustion(&self, slot: usize) {
        self.writev_budget_exhaustions.increment(slot);
    }

    #[inline]
    pub(super) fn record_maintenance_budget_exhaustion(&self, slot: usize) {
        self.maintenance_budget_exhaustions.increment(slot);
    }

    #[inline]
    pub(super) fn record_yielded_connection(&self, slot: usize) {
        self.yielded_connections.increment(slot);
    }

    #[inline]
    pub(super) fn record_parser_resume(&self, slot: usize) {
        self.parser_resumes.increment(slot);
    }

    #[inline]
    pub(super) fn record_completion_batch(&self, slot: usize, width: usize) {
        if width == 0 {
            return;
        }
        self.completion_batch_count.increment(slot);
        self.completion_batch_total.add(slot, width as u64);
        self.completion_batch_max.record(slot, width as u64);
    }

    #[inline]
    pub(super) fn record_completion_nanos(&self, slot: usize, nanos: u64) {
        #[cfg(feature = "profile-telemetry")]
        {
            if nanos == 0 {
                return;
            }
            self.completion_nanos_total.add(slot, nanos);
            self.completion_nanos_max.record(slot, nanos);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            let _ = (slot, nanos);
        }
    }

    #[inline]
    pub(super) fn record_command_batch(&self, slot: usize, width: usize) {
        if width == 0 {
            return;
        }
        self.command_batch_count.increment(slot);
        self.command_batch_total.add(slot, width as u64);
        self.command_batch_max.record(slot, width as u64);
    }

    #[inline]
    pub(super) fn record_writev_chunk(&self, slot: usize, iovecs: usize) {
        self.writev_chunks.increment(slot);
        if iovecs != 0 {
            self.writev_iovecs_total.add(slot, iovecs as u64);
            self.writev_iovecs_max.record(slot, iovecs as u64);
        }
    }

    #[inline]
    pub(super) fn record_queued_response_bytes(&self, slot: usize, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.queued_response_bytes_total.add(slot, bytes as u64);
        self.queued_response_bytes_max.record(slot, bytes as u64);
    }

    #[inline]
    pub(super) fn flush_local_metrics(&self, slot: usize, metrics: RuntimeLocalFlushMetrics) {
        if metrics.is_empty() {
            return;
        }
        if metrics.loop_iterations != 0 {
            self.loop_iterations.add(slot, metrics.loop_iterations);
        }
        if metrics.accept_drain_runs != 0 {
            self.accept_drain_runs.add(slot, metrics.accept_drain_runs);
        }
        if metrics.accept_drain_accepted != 0 {
            self.accept_drain_accepted
                .add(slot, metrics.accept_drain_accepted);
            self.accept_drain_accepted_max
                .record(slot, metrics.accept_drain_accepted_max);
        }
        if metrics.completion_batch_count != 0 {
            self.completion_batch_count
                .add(slot, metrics.completion_batch_count);
            self.completion_batch_total
                .add(slot, metrics.completion_batch_total);
            self.completion_batch_max
                .record(slot, metrics.completion_batch_max);
        }
        if metrics.command_batch_count != 0 {
            self.command_batch_count
                .add(slot, metrics.command_batch_count);
            self.command_batch_total
                .add(slot, metrics.command_batch_total);
            self.command_batch_max
                .record(slot, metrics.command_batch_max);
        }
        if metrics.writev_chunks != 0 {
            self.writev_chunks.add(slot, metrics.writev_chunks);
            self.writev_iovecs_total
                .add(slot, metrics.writev_iovecs_total);
            self.writev_iovecs_max
                .record(slot, metrics.writev_iovecs_max);
        }
        if metrics.queued_response_bytes_total != 0 {
            self.queued_response_bytes_total
                .add(slot, metrics.queued_response_bytes_total);
            self.queued_response_bytes_max
                .record(slot, metrics.queued_response_bytes_max);
        }
    }

    #[inline]
    pub(super) fn publish_client_retained_bytes(
        &self,
        slot: usize,
        total_bytes: usize,
        max_connection_bytes: usize,
    ) {
        self.client_retained_bytes.store(slot, total_bytes as u64);
        self.client_retained_bytes_max
            .store(slot, max_connection_bytes as u64);
        self.client_retained_bytes_peak
            .record(slot, max_connection_bytes as u64);
    }

    #[inline]
    pub(super) fn record_request_cap_exceeded(&self, slot: usize) {
        self.request_cap_exceeded.increment(slot);
    }

    #[inline]
    pub(super) fn record_response_cap_exceeded(&self, slot: usize) {
        self.response_cap_exceeded.increment(slot);
    }

    #[inline]
    pub(super) fn record_multi_queue_command_cap_exceeded(&self, slot: usize) {
        self.multi_queue_command_cap_exceeded.increment(slot);
    }

    #[inline]
    pub(super) fn record_multi_queue_bytes_cap_exceeded(&self, slot: usize) {
        self.multi_queue_bytes_cap_exceeded.increment(slot);
    }

    #[inline]
    pub(super) fn record_watch_cap_exceeded(&self, slot: usize) {
        self.watch_cap_exceeded.increment(slot);
    }

    #[inline]
    pub(super) fn record_writev_chunk_cap_exceeded(&self, slot: usize) {
        self.writev_chunk_cap_exceeded.increment(slot);
    }

    #[inline]
    pub(super) fn record_overload_accept_throttled(&self, slot: usize) {
        self.overload_accept_throttled.increment(slot);
    }

    #[inline]
    pub(super) fn record_overload_accept_resumed(&self, slot: usize) {
        self.overload_accept_resumed.increment(slot);
    }

    #[inline]
    pub(super) fn record_overload_read_disabled(&self, slot: usize) {
        self.overload_read_disabled.increment(slot);
    }

    #[inline]
    pub(super) fn record_overload_read_resumed(&self, slot: usize) {
        self.overload_read_resumed.increment(slot);
    }

    #[inline]
    pub(super) fn record_overload_command_deferred(&self, slot: usize) {
        self.overload_command_deferred.increment(slot);
    }

    #[inline]
    pub(super) fn record_overload_command_resumed(&self, slot: usize) {
        self.overload_command_resumed.increment(slot);
    }

    #[inline]
    pub(super) fn record_overload_connection_dropped(&self, slot: usize) {
        self.overload_connections_dropped.increment(slot);
    }

    #[inline]
    pub(super) fn publish_overload_telemetry(
        &self,
        slot: usize,
        telemetry: RuntimeOverloadTelemetry,
    ) {
        self.overload_pending_response_bytes
            .store(slot, telemetry.pending_response_bytes);
        self.overload_pending_response_bytes_peak
            .record(slot, telemetry.pending_response_bytes_peak);
        self.overload_parser_accumulator_bytes
            .store(slot, telemetry.parser_accumulator_bytes);
        self.overload_parser_accumulator_bytes_peak
            .record(slot, telemetry.parser_accumulator_bytes_peak);
        self.overload_writev_backlog_bytes
            .store(slot, telemetry.writev_backlog_bytes);
        self.overload_writev_backlog_bytes_peak
            .record(slot, telemetry.writev_backlog_bytes_peak);
        self.overload_aof_pending_bytes
            .store(slot, telemetry.aof_pending_bytes);
        self.overload_aof_pending_bytes_peak
            .record(slot, telemetry.aof_pending_bytes);
        self.overload_maintenance_debt
            .store(slot, telemetry.maintenance_debt);
        self.overload_maintenance_debt_peak
            .record(slot, telemetry.maintenance_debt_peak);
        self.overload_read_disabled_connections
            .store(slot, telemetry.read_disabled_connections);
        self.overload_read_disabled_connections_peak
            .record(slot, telemetry.read_disabled_connections_peak);
        self.overload_deferred_commands
            .store(slot, telemetry.deferred_commands);
        self.overload_deferred_commands_peak
            .record(slot, telemetry.deferred_commands_peak);
    }

    #[inline]
    pub(super) fn publish_shared_nothing_telemetry(
        &self,
        slot: usize,
        telemetry: RuntimeSharedNothingTelemetry,
    ) {
        self.shared_nothing_accepted_remote
            .store(slot, telemetry.accepted_remote);
        self.shared_nothing_remote_backpressure
            .store(slot, telemetry.remote_backpressure);
        self.shared_nothing_reply_backpressure
            .store(slot, telemetry.reply_backpressure);
        self.shared_nothing_deferred_replies
            .store(slot, telemetry.deferred_replies);
        self.shared_nothing_wakeups_sent
            .store(slot, telemetry.wakeups_sent);
        self.shared_nothing_wakeup_failures
            .store(slot, telemetry.wakeup_failures);
        self.shared_nothing_aggregates_accepted
            .store(slot, telemetry.aggregates_accepted);
        self.shared_nothing_aggregate_width_max
            .store(slot, telemetry.aggregate_width_max);
        self.shared_nothing_txns_accepted
            .store(slot, telemetry.txns_accepted);
        self.shared_nothing_txn_prepare_messages
            .store(slot, telemetry.txn_prepare_messages);
        self.shared_nothing_txn_commit_messages
            .store(slot, telemetry.txn_commit_messages);
        self.shared_nothing_txn_abort_messages
            .store(slot, telemetry.txn_abort_messages);
        self.shared_nothing_txn_condition_aborts
            .store(slot, telemetry.txn_condition_aborts);
        self.shared_nothing_txn_conflict_aborts
            .store(slot, telemetry.txn_conflict_aborts);
        self.shared_nothing_prepared_key_waits
            .store(slot, telemetry.prepared_key_waits);
        self.shared_nothing_prepared_key_retries
            .store(slot, telemetry.prepared_key_retries);
        self.shared_nothing_prepared_key_wait_nanos_total
            .store(slot, telemetry.prepared_key_wait_nanos_total);
        self.shared_nothing_prepared_key_wait_nanos_max
            .record(slot, telemetry.prepared_key_wait_nanos_max);
    }

    #[inline]
    pub(super) fn record_close_drain_nanos(&self, slot: usize, nanos: u64) {
        #[cfg(feature = "profile-telemetry")]
        {
            if nanos == 0 {
                return;
            }
            self.close_drain_nanos_total.add(slot, nanos);
            self.close_drain_nanos_max.record(slot, nanos);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            let _ = (slot, nanos);
        }
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
    pub(super) fn record_active_expiry_nanos(&self, slot: usize, nanos: u64) {
        #[cfg(feature = "profile-telemetry")]
        {
            if nanos == 0 {
                return;
            }
            self.active_expiry_nanos_total.add(slot, nanos);
            self.active_expiry_nanos_max.record(slot, nanos);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            let _ = (slot, nanos);
        }
    }

    #[inline]
    pub(super) fn record_aof_append_nanos(&self, slot: usize, nanos: u64) {
        #[cfg(feature = "profile-telemetry")]
        {
            if nanos == 0 {
                return;
            }
            self.aof_append_nanos_total.add(slot, nanos);
            self.aof_append_nanos_max.record(slot, nanos);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            let _ = (slot, nanos);
        }
    }

    #[inline]
    pub(super) fn record_aof_fsync_nanos(&self, slot: usize, nanos: u64) {
        #[cfg(feature = "profile-telemetry")]
        {
            if nanos == 0 {
                return;
            }
            self.aof_fsync_nanos_total.add(slot, nanos);
            self.aof_fsync_nanos_max.record(slot, nanos);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            let _ = (slot, nanos);
        }
    }

    #[inline]
    pub(super) fn publish_aof_telemetry(&self, slot: usize, telemetry: RuntimeAofTelemetry) {
        self.aof_pending_bytes.store(slot, telemetry.pending_bytes);
        self.aof_pending_writes
            .store(slot, telemetry.pending_writes);
        self.aof_pending_bytes_max
            .record(slot, telemetry.pending_bytes);
        self.aof_pending_writes_max
            .record(slot, telemetry.pending_writes);
        self.aof_fsync_requested
            .store(slot, telemetry.fsync_requested);
        self.aof_fsync_completed
            .store(slot, telemetry.fsync_completed);
        self.aof_fsync_failed.store(slot, telemetry.fsync_failed);
        self.aof_fsync_worker_saturation
            .store(slot, telemetry.fsync_worker_saturation);
        self.aof_backpressure_events
            .store(slot, telemetry.backpressure_events);
        #[cfg(feature = "profile-telemetry")]
        {
            self.aof_backpressure_nanos_total
                .store(slot, telemetry.backpressure_nanos_total);
            self.aof_backpressure_nanos_max
                .record(slot, telemetry.backpressure_nanos_max);
        }
        self.aof_last_appended_lsn
            .store(slot, telemetry.last_appended_lsn);
        self.aof_last_durable_lsn
            .store(slot, telemetry.last_durable_lsn);
        #[cfg(feature = "profile-telemetry")]
        {
            self.aof_fsync_latency_nanos_total
                .store(slot, telemetry.fsync_latency_nanos_total);
            self.aof_fsync_latency_nanos_max
                .record(slot, telemetry.fsync_latency_nanos_max);
            for (idx, bucket) in telemetry.fsync_latency_buckets.iter().enumerate() {
                self.aof_fsync_latency_buckets[idx].store(slot, *bucket);
            }
        }
    }

    #[inline]
    pub(super) fn record_maintenance_nanos(&self, slot: usize, nanos: u64) {
        #[cfg(feature = "profile-telemetry")]
        {
            if nanos == 0 {
                return;
            }
            self.maintenance_nanos_total.add(slot, nanos);
            self.maintenance_nanos_max.record(slot, nanos);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            let _ = (slot, nanos);
        }
    }

    #[inline]
    pub(super) fn record_metrics_flush_nanos(&self, slot: usize, nanos: u64) {
        #[cfg(feature = "profile-telemetry")]
        {
            if nanos == 0 {
                return;
            }
            self.metrics_flush_nanos_total.add(slot, nanos);
            self.metrics_flush_nanos_max.record(slot, nanos);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            let _ = (slot, nanos);
        }
    }

    #[inline]
    pub(super) fn snapshot(&self, eviction: EvictionMetricsSnapshot) -> RuntimeMetricsSnapshot {
        let telemetry_mode = self.telemetry_mode();
        let completion_batch_count = self.completion_batch_count.total();
        let completion_batch_total = self.completion_batch_total.total();
        let command_batch_count = self.command_batch_count.total();
        let command_batch_total = self.command_batch_total.total();

        RuntimeMetricsSnapshot {
            telemetry_mode,
            profile_timers_available: telemetry_mode.profile_timers_enabled(),
            local_flush_metrics_available: true,
            reactor_slots: self.slot_count(),
            backend: self.backend_snapshot(),
            loop_iterations: self.loop_iterations.total(),
            accept_eagain_rearms: self.accept_eagain_rearms.total(),
            accept_drain_runs: self.accept_drain_runs.total(),
            accept_drain_accepted: self.accept_drain_accepted.total(),
            accept_drain_accepted_max: self.accept_drain_accepted_max.max(),
            backend_submit_syscalls: self.backend_submit_syscalls.total(),
            backend_sq_occupancy_max: self.backend_sq_occupancy_max.max(),
            backend_sq_capacity: self.backend_sq_capacity.max(),
            backend_cq_occupancy_max: self.backend_cq_occupancy_max.max(),
            backend_cq_capacity: self.backend_cq_capacity.max(),
            backend_cq_overflows: self.backend_cq_overflows.total(),
            backend_completions_per_submit_syscall_x1000: avg_counter_x1000(
                completion_batch_total,
                self.backend_submit_syscalls.total(),
            ),
            submit_sq_full_retries: self.submit_sq_full_retries.total(),
            submit_failures: self.submit_failures.total(),
            completion_budget_exhaustions: self.completion_budget_exhaustions.total(),
            command_budget_exhaustions: self.command_budget_exhaustions.total(),
            accept_budget_exhaustions: self.accept_budget_exhaustions.total(),
            writev_budget_exhaustions: self.writev_budget_exhaustions.total(),
            maintenance_budget_exhaustions: self.maintenance_budget_exhaustions.total(),
            yielded_connections: self.yielded_connections.total(),
            parser_resumes: self.parser_resumes.total(),
            completion_batch_count,
            completion_batch_total,
            completion_batch_max: self.completion_batch_max.max(),
            completion_batch_avg: avg_counter(completion_batch_total, completion_batch_count),
            completion_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.completion_nanos_total.total()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            completion_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.completion_nanos_max.max()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            command_batch_count,
            command_batch_total,
            command_batch_max: self.command_batch_max.max(),
            command_batch_avg: avg_counter(command_batch_total, command_batch_count),
            writev_chunks: self.writev_chunks.total(),
            writev_iovecs_total: self.writev_iovecs_total.total(),
            writev_iovecs_max: self.writev_iovecs_max.max(),
            queued_response_bytes_total: self.queued_response_bytes_total.total(),
            queued_response_bytes_max: self.queued_response_bytes_max.max(),
            client_retained_bytes: self.client_retained_bytes.sum(),
            client_retained_bytes_max: self.client_retained_bytes_max.max(),
            client_retained_bytes_peak: self.client_retained_bytes_peak.max(),
            request_cap_exceeded: self.request_cap_exceeded.total(),
            response_cap_exceeded: self.response_cap_exceeded.total(),
            multi_queue_command_cap_exceeded: self.multi_queue_command_cap_exceeded.total(),
            multi_queue_bytes_cap_exceeded: self.multi_queue_bytes_cap_exceeded.total(),
            watch_cap_exceeded: self.watch_cap_exceeded.total(),
            writev_chunk_cap_exceeded: self.writev_chunk_cap_exceeded.total(),
            overload_accept_throttled: self.overload_accept_throttled.total(),
            overload_accept_resumed: self.overload_accept_resumed.total(),
            overload_read_disabled: self.overload_read_disabled.total(),
            overload_read_resumed: self.overload_read_resumed.total(),
            overload_command_deferred: self.overload_command_deferred.total(),
            overload_command_resumed: self.overload_command_resumed.total(),
            overload_connections_dropped: self.overload_connections_dropped.total(),
            overload_pending_response_bytes: self.overload_pending_response_bytes.sum(),
            overload_pending_response_bytes_peak: self.overload_pending_response_bytes_peak.max(),
            overload_parser_accumulator_bytes: self.overload_parser_accumulator_bytes.sum(),
            overload_parser_accumulator_bytes_peak: self
                .overload_parser_accumulator_bytes_peak
                .max(),
            overload_writev_backlog_bytes: self.overload_writev_backlog_bytes.sum(),
            overload_writev_backlog_bytes_peak: self.overload_writev_backlog_bytes_peak.max(),
            overload_aof_pending_bytes: self.overload_aof_pending_bytes.sum(),
            overload_aof_pending_bytes_peak: self.overload_aof_pending_bytes_peak.max(),
            overload_maintenance_debt: self.overload_maintenance_debt.sum(),
            overload_maintenance_debt_peak: self.overload_maintenance_debt_peak.max(),
            overload_read_disabled_connections: self.overload_read_disabled_connections.sum(),
            overload_read_disabled_connections_peak: self
                .overload_read_disabled_connections_peak
                .max(),
            overload_deferred_commands: self.overload_deferred_commands.sum(),
            overload_deferred_commands_peak: self.overload_deferred_commands_peak.max(),
            shared_nothing_accepted_remote: self.shared_nothing_accepted_remote.sum(),
            shared_nothing_remote_backpressure: self.shared_nothing_remote_backpressure.sum(),
            shared_nothing_reply_backpressure: self.shared_nothing_reply_backpressure.sum(),
            shared_nothing_deferred_replies: self.shared_nothing_deferred_replies.sum(),
            shared_nothing_wakeups_sent: self.shared_nothing_wakeups_sent.sum(),
            shared_nothing_wakeup_failures: self.shared_nothing_wakeup_failures.sum(),
            shared_nothing_aggregates_accepted: self.shared_nothing_aggregates_accepted.sum(),
            shared_nothing_aggregate_width_max: self.shared_nothing_aggregate_width_max.max(),
            shared_nothing_txns_accepted: self.shared_nothing_txns_accepted.sum(),
            shared_nothing_txn_prepare_messages: self.shared_nothing_txn_prepare_messages.sum(),
            shared_nothing_txn_commit_messages: self.shared_nothing_txn_commit_messages.sum(),
            shared_nothing_txn_abort_messages: self.shared_nothing_txn_abort_messages.sum(),
            shared_nothing_txn_condition_aborts: self.shared_nothing_txn_condition_aborts.sum(),
            shared_nothing_txn_conflict_aborts: self.shared_nothing_txn_conflict_aborts.sum(),
            shared_nothing_prepared_key_waits: self.shared_nothing_prepared_key_waits.sum(),
            shared_nothing_prepared_key_retries: self.shared_nothing_prepared_key_retries.sum(),
            shared_nothing_prepared_key_wait_nanos_total: self
                .shared_nothing_prepared_key_wait_nanos_total
                .sum(),
            shared_nothing_prepared_key_wait_nanos_max: self
                .shared_nothing_prepared_key_wait_nanos_max
                .max(),
            close_drain_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.close_drain_nanos_total.total()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            close_drain_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.close_drain_nanos_max.max()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            active_expiry_runs: self.active_expiry_runs.total(),
            active_expiry_sampled: self.active_expiry_sampled.total(),
            active_expiry_expired: self.active_expiry_expired.total(),
            active_expiry_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.active_expiry_nanos_total.total()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            active_expiry_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.active_expiry_nanos_max.max()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            aof_append_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.aof_append_nanos_total.total()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            aof_append_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.aof_append_nanos_max.max()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            aof_fsync_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.aof_fsync_nanos_total.total()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            aof_fsync_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.aof_fsync_nanos_max.max()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            aof_pending_bytes: self.aof_pending_bytes.sum(),
            aof_pending_bytes_max: self.aof_pending_bytes_max.max(),
            aof_pending_writes: self.aof_pending_writes.sum(),
            aof_pending_writes_max: self.aof_pending_writes_max.max(),
            aof_fsync_requested: self.aof_fsync_requested.sum(),
            aof_fsync_completed: self.aof_fsync_completed.sum(),
            aof_fsync_failed: self.aof_fsync_failed.sum(),
            aof_fsync_worker_saturation: self.aof_fsync_worker_saturation.sum(),
            aof_backpressure_events: self.aof_backpressure_events.sum(),
            aof_backpressure_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.aof_backpressure_nanos_total.sum()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            aof_backpressure_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.aof_backpressure_nanos_max.max()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            aof_last_appended_lsn: self.aof_last_appended_lsn.max(),
            aof_last_durable_lsn: self.aof_last_durable_lsn.max(),
            aof_durable_lsn_lag: self
                .aof_last_appended_lsn
                .max()
                .saturating_sub(self.aof_last_durable_lsn.max()),
            aof_fsync_latency_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.aof_fsync_latency_nanos_total.sum()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            aof_fsync_latency_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.aof_fsync_latency_nanos_max.max()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            aof_fsync_latency_buckets: {
                #[cfg(feature = "profile-telemetry")]
                {
                    std::array::from_fn(|idx| self.aof_fsync_latency_buckets[idx].sum())
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    [0; RUNTIME_AOF_FSYNC_LATENCY_BUCKETS]
                }
            },
            maintenance_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.maintenance_nanos_total.total()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            maintenance_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.maintenance_nanos_max.max()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            metrics_flush_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.metrics_flush_nanos_total.total()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            metrics_flush_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.metrics_flush_nanos_max.max()
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            eviction_admissions: eviction.admissions,
            eviction_shards_scanned: eviction.shards_scanned,
            eviction_slots_sampled: eviction.slots_sampled,
            eviction_bytes_freed: eviction.bytes_freed,
            eviction_oom_after_scan: eviction.oom_after_scan,
            eviction_nanos_total: eviction.scan_nanos_total,
            eviction_nanos_max: eviction.scan_nanos_max,
        }
    }
}

struct RuntimeGaugeSlots {
    slots: Box<[CachePadded<AtomicU64>]>,
}

impl RuntimeGaugeSlots {
    fn new(num_slots: usize) -> Self {
        let slots = (0..num_slots)
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { slots }
    }

    #[inline]
    fn store(&self, slot: usize, value: u64) {
        if let Some(current) = self.slots.get(slot) {
            current.store(value, Ordering::Relaxed);
        }
    }

    #[inline]
    fn sum(&self) -> u64 {
        self.slots
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .sum()
    }

    #[inline]
    fn max(&self) -> u64 {
        self.slots
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0)
    }
}

struct RuntimeMaxSlots {
    slots: Box<[CachePadded<AtomicU64>]>,
}

impl RuntimeMaxSlots {
    fn new(num_slots: usize) -> Self {
        let slots = (0..num_slots)
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { slots }
    }

    #[inline]
    fn len(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    fn record(&self, slot: usize, value: u64) {
        if let Some(current) = self.slots.get(slot) {
            current.fetch_max(value, Ordering::Relaxed);
        }
    }

    #[inline]
    fn max(&self) -> u64 {
        self.slots
            .iter()
            .map(|slot| slot.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0)
    }
}

#[inline]
fn avg_counter(total: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total as f64 / count as f64
    }
}

#[inline]
fn avg_counter_x1000(total: u64, count: u64) -> u64 {
    if count == 0 {
        0
    } else {
        total.saturating_mul(1_000) / count
    }
}

impl ConcurrentKeyspace {
    #[inline]
    pub fn eviction_metrics(&self) -> EvictionMetricsSnapshot {
        self.eviction_metrics.snapshot()
    }

    #[inline]
    pub fn set_runtime_telemetry_mode(&self, mode: RuntimeTelemetryMode) {
        self.runtime_metrics.set_telemetry_mode(mode);
    }

    #[inline]
    pub fn runtime_telemetry_mode(&self) -> RuntimeTelemetryMode {
        self.runtime_metrics.telemetry_mode()
    }

    #[inline]
    pub fn runtime_profile_timers_enabled(&self) -> bool {
        self.runtime_telemetry_mode().profile_timers_enabled()
    }

    #[inline]
    pub fn runtime_metrics(&self) -> RuntimeMetricsSnapshot {
        self.runtime_metrics
            .snapshot(self.eviction_metrics.snapshot())
    }

    #[inline]
    pub fn publish_runtime_backend(&self, snapshot: RuntimeBackendSnapshot) {
        self.runtime_metrics.publish_backend(snapshot);
    }

    #[inline(always)]
    pub fn record_reactor_backend_submit_syscall(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_backend_submit_syscall(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_backend_queue_status(
        &self,
        reactor_id: usize,
        sq_occupancy: u64,
        sq_capacity: u64,
        cq_occupancy: u64,
        cq_capacity: u64,
        cq_overflow_delta: u64,
    ) {
        self.runtime_metrics.record_backend_queue_status(
            reactor_id,
            sq_occupancy,
            sq_capacity,
            cq_occupancy,
            cq_capacity,
            cq_overflow_delta,
        );
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
    pub fn record_reactor_accept_drain(&self, reactor_id: usize, accepted: usize) {
        self.runtime_metrics
            .record_accept_drain(reactor_id, accepted);
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
    pub fn record_reactor_completion_budget_exhaustion(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_completion_budget_exhaustion(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_command_budget_exhaustion(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_command_budget_exhaustion(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_accept_budget_exhaustion(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_accept_budget_exhaustion(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_writev_budget_exhaustion(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_writev_budget_exhaustion(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_maintenance_budget_exhaustion(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_maintenance_budget_exhaustion(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_yielded_connection(&self, reactor_id: usize) {
        self.runtime_metrics.record_yielded_connection(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_parser_resume(&self, reactor_id: usize) {
        self.runtime_metrics.record_parser_resume(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_completion_batch(&self, reactor_id: usize, width: usize) {
        self.runtime_metrics
            .record_completion_batch(reactor_id, width);
    }

    #[inline(always)]
    pub fn record_reactor_completion_nanos(&self, reactor_id: usize, nanos: u64) {
        self.runtime_metrics
            .record_completion_nanos(reactor_id, nanos);
    }

    #[inline(always)]
    pub fn record_reactor_command_batch(&self, reactor_id: usize, width: usize) {
        self.runtime_metrics.record_command_batch(reactor_id, width);
    }

    #[inline(always)]
    pub fn record_reactor_writev_chunk(&self, reactor_id: usize, iovecs: usize) {
        self.runtime_metrics.record_writev_chunk(reactor_id, iovecs);
    }

    #[inline(always)]
    pub fn record_reactor_queued_response_bytes(&self, reactor_id: usize, bytes: usize) {
        self.runtime_metrics
            .record_queued_response_bytes(reactor_id, bytes);
    }

    #[inline(always)]
    pub fn flush_reactor_local_metrics(
        &self,
        reactor_id: usize,
        metrics: RuntimeLocalFlushMetrics,
    ) {
        self.runtime_metrics
            .flush_local_metrics(reactor_id, metrics);
    }

    #[inline(always)]
    pub fn publish_reactor_client_retained_bytes(
        &self,
        reactor_id: usize,
        total_bytes: usize,
        max_connection_bytes: usize,
    ) {
        self.runtime_metrics.publish_client_retained_bytes(
            reactor_id,
            total_bytes,
            max_connection_bytes,
        );
    }

    #[inline(always)]
    pub fn record_reactor_request_cap_exceeded(&self, reactor_id: usize) {
        self.runtime_metrics.record_request_cap_exceeded(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_response_cap_exceeded(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_response_cap_exceeded(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_multi_queue_command_cap_exceeded(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_multi_queue_command_cap_exceeded(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_multi_queue_bytes_cap_exceeded(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_multi_queue_bytes_cap_exceeded(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_watch_cap_exceeded(&self, reactor_id: usize) {
        self.runtime_metrics.record_watch_cap_exceeded(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_writev_chunk_cap_exceeded(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_writev_chunk_cap_exceeded(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_overload_accept_throttled(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_overload_accept_throttled(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_overload_accept_resumed(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_overload_accept_resumed(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_overload_read_disabled(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_overload_read_disabled(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_overload_read_resumed(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_overload_read_resumed(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_overload_command_deferred(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_overload_command_deferred(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_overload_command_resumed(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_overload_command_resumed(reactor_id);
    }

    #[inline(always)]
    pub fn record_reactor_overload_connection_dropped(&self, reactor_id: usize) {
        self.runtime_metrics
            .record_overload_connection_dropped(reactor_id);
    }

    #[inline(always)]
    pub fn publish_reactor_overload_telemetry(
        &self,
        reactor_id: usize,
        telemetry: RuntimeOverloadTelemetry,
    ) {
        self.runtime_metrics
            .publish_overload_telemetry(reactor_id, telemetry);
    }

    #[inline(always)]
    pub fn publish_reactor_shared_nothing_telemetry(
        &self,
        reactor_id: usize,
        telemetry: RuntimeSharedNothingTelemetry,
    ) {
        self.runtime_metrics
            .publish_shared_nothing_telemetry(reactor_id, telemetry);
    }

    #[inline(always)]
    pub fn record_reactor_close_drain_nanos(&self, reactor_id: usize, nanos: u64) {
        self.runtime_metrics
            .record_close_drain_nanos(reactor_id, nanos);
    }

    #[inline(always)]
    pub fn record_reactor_active_expiry(&self, reactor_id: usize, sampled: usize, expired: usize) {
        self.runtime_metrics
            .record_active_expiry(reactor_id, sampled, expired);
    }

    #[inline(always)]
    pub fn record_reactor_active_expiry_nanos(&self, reactor_id: usize, nanos: u64) {
        self.runtime_metrics
            .record_active_expiry_nanos(reactor_id, nanos);
    }

    #[inline(always)]
    pub fn record_reactor_aof_append_nanos(&self, reactor_id: usize, nanos: u64) {
        self.runtime_metrics
            .record_aof_append_nanos(reactor_id, nanos);
    }

    #[inline(always)]
    pub fn record_reactor_aof_fsync_nanos(&self, reactor_id: usize, nanos: u64) {
        self.runtime_metrics
            .record_aof_fsync_nanos(reactor_id, nanos);
    }

    #[inline(always)]
    pub fn publish_reactor_aof_telemetry(&self, reactor_id: usize, telemetry: RuntimeAofTelemetry) {
        self.runtime_metrics
            .publish_aof_telemetry(reactor_id, telemetry);
    }

    #[inline(always)]
    pub fn record_reactor_maintenance_nanos(&self, reactor_id: usize, nanos: u64) {
        self.runtime_metrics
            .record_maintenance_nanos(reactor_id, nanos);
    }

    #[inline(always)]
    pub fn record_reactor_metrics_flush_nanos(&self, reactor_id: usize, nanos: u64) {
        self.runtime_metrics
            .record_metrics_flush_nanos(reactor_id, nanos);
    }
}
