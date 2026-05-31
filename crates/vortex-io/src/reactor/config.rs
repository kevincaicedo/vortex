use super::*;

pub struct ReactorConfig {
    /// Address to bind the listener on.
    pub bind_addr: std::net::SocketAddr,
    /// Max number of client connections per reactor.
    pub max_connections: usize,
    /// Read buffer size in bytes.
    pub buffer_size: usize,
    /// Max bytes retained for one in-flight request or pipeline.
    pub max_request_bytes: usize,
    /// Per-connection retained-memory caps for parser, response, transaction,
    /// WATCH, and deferred writev state.
    pub connection_caps: ConnectionMemoryCaps,
    /// Reactor-local admission thresholds for overload backpressure.
    pub overload_policy: ReactorOverloadPolicy,
    /// Number of pre-allocated I/O buffers.
    pub buffer_count: usize,
    /// io_uring fixed-buffer registration policy.
    pub fixed_buffer_registration: FixedBufferRegistrationMode,
    /// Idle connection timeout in seconds (0 = disabled).
    pub connection_timeout: u32,
    /// AOF persistence configuration (None = disabled).
    pub aof_config: Option<AofConfig>,
    /// I/O backend selection.
    pub io_backend: IoBackendMode,
    /// io_uring submission queue size.
    pub ring_size: u32,
    /// SQPOLL idle timeout in milliseconds (0 = disabled).
    pub sqpoll_idle_ms: u32,
    /// Per-activation reactor work budgets.
    pub budgets: ReactorBudgets,
    /// Runtime telemetry policy. Minimal is the release/default target;
    /// Standard samples hot-path reactor-local diagnostics; Profile enables
    /// exact diagnostics plus timestamped phase timers for benchmark/profiler runs.
    pub telemetry_mode: RuntimeTelemetryMode,
    /// Effective local diagnostic sample rate. Zero disables local telemetry
    /// for the cheapest minimal path.
    pub telemetry_local_sample_rate: u32,
    /// Cold metrics publication interval in monotonic nanoseconds.
    pub telemetry_flush_interval_nanos: u64,
}

/// Per-connection retained-memory limits enforced by the reactor.
///
/// These caps are production safety rails, not profiling instrumentation. They
/// bound memory retained outside the keyspace so one slow reader, malicious
/// request, or deeply pipelined client cannot create unbounded server-side
/// growth. Cap-exceeded counters are cold-path signals for operators and
/// benchmark reports; the command path only pays the integer checks needed to
/// enforce the limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionMemoryCaps {
    /// Max bytes retained for an incomplete request accumulator.
    pub max_parser_accumulator_bytes: usize,
    /// Max serialized response bytes queued for one connection.
    pub max_pending_response_bytes: usize,
    /// Max commands queued inside MULTI for one connection.
    pub max_multi_queue_commands: usize,
    /// Max serialized command bytes queued inside MULTI for one connection.
    pub max_multi_queue_bytes: usize,
    /// Max WATCH registrations retained by one connection.
    pub max_watch_registrations: usize,
    /// Max deferred writev submit chunks retained for one response batch.
    pub max_writev_chunks: usize,
}

impl Default for ConnectionMemoryCaps {
    fn default() -> Self {
        Self {
            max_parser_accumulator_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_pending_response_bytes: 64 * 1024 * 1024,
            max_multi_queue_commands: MAX_TRANSACTION_COMMANDS,
            max_multi_queue_bytes: 16 * 1024 * 1024,
            max_watch_registrations: 1024,
            max_writev_chunks: 64,
        }
    }
}

/// Reactor-local overload thresholds.
///
/// These thresholds are production admission controls, not profiler-only
/// instrumentation. They intentionally stay reactor-local: a reactor pauses its
/// own accepts, read re-arms, or write-command execution when local backlog
/// crosses a limit. The hot path only performs local integer comparisons;
/// shared telemetry is updated on actual throttle/defer/drop events or during
/// the cold metrics flush.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReactorOverloadPolicy {
    /// Stop accepting new connections when active connections reach this
    /// percent of the reactor-local connection capacity.
    pub accept_throttle_connection_percent: u8,
    /// Disable new read submissions when reactor queued response bytes reach
    /// this threshold.
    pub read_disable_pending_response_bytes: usize,
    /// Disable new read submissions when parser accumulator bytes retained by
    /// this reactor reach this threshold.
    pub read_disable_parser_accumulator_bytes: usize,
    /// Defer AOF-bearing write commands and throttle accepts when pending AOF
    /// durable-work bytes reach this threshold.
    pub aof_pending_bytes: u64,
    /// Disable new read submissions when outstanding writev backlog bytes reach
    /// this threshold.
    pub writev_backlog_bytes: usize,
    /// Defer foreground command execution and throttle accepts when close,
    /// timer, expiry, eviction, or fsync maintenance debt reaches this many
    /// reactor-local units.
    pub maintenance_debt: usize,
}

impl Default for ReactorOverloadPolicy {
    fn default() -> Self {
        Self {
            accept_throttle_connection_percent: 95,
            read_disable_pending_response_bytes: 256 * 1024 * 1024,
            read_disable_parser_accumulator_bytes: 256 * 1024 * 1024,
            aof_pending_bytes: DEFAULT_EVERYSEC_MAX_PENDING_BYTES,
            writev_backlog_bytes: 256 * 1024 * 1024,
            maintenance_debt: 1024,
        }
    }
}

/// AOF configuration passed to each reactor.
#[derive(Clone, Debug)]
pub struct AofConfig {
    /// Base path for AOF files. Each reactor appends its shard ID.
    pub path: std::path::PathBuf,
    /// Fsync policy.
    pub fsync_policy: vortex_persist::aof::AofFsyncPolicy,
    /// Max unsynced bytes before `everysec` applies backpressure.
    pub max_pending_fsync_bytes: u64,
}

pub(super) struct AofWriterSlot {
    pub(super) epoch: AofEpoch,
    pub(super) writer: AofFileWriter,
}

impl AofWriterSlot {
    pub(super) fn new(epoch: AofEpoch, writer: AofFileWriter) -> Self {
        Self { epoch, writer }
    }
}

pub(super) fn open_aof_writer(
    path: &std::path::Path,
    writer_id: AofReactorId,
    config: &AofConfig,
    telemetry_mode: RuntimeTelemetryMode,
) -> io::Result<AofFileWriter> {
    let mut writer = AofFileWriter::open(path, writer_id, config.fsync_policy)?;
    writer.set_max_pending_fsync_bytes(config.max_pending_fsync_bytes);
    #[cfg(feature = "profile-telemetry")]
    writer.set_profile_telemetry(telemetry_mode.profile_timers_enabled());
    #[cfg(not(feature = "profile-telemetry"))]
    let _ = telemetry_mode;
    Ok(writer)
}

#[derive(Clone)]
pub(crate) struct AofRuntime {
    pub(super) coordinator: Arc<AofCoordinator>,
    pub(super) startup_epoch: Option<AofEpoch>,
}

impl AofRuntime {
    pub(crate) fn new(coordinator: Arc<AofCoordinator>, startup_epoch: Option<AofEpoch>) -> Self {
        Self {
            coordinator,
            startup_epoch,
        }
    }
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:6379".parse().expect("valid default addr"),
            max_connections: 1024,
            buffer_size: DEFAULT_BUF_SIZE,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            connection_caps: ConnectionMemoryCaps::default(),
            overload_policy: ReactorOverloadPolicy::default(),
            buffer_count: 2048,
            fixed_buffer_registration: FixedBufferRegistrationMode::Auto,
            connection_timeout: 300,
            aof_config: None,
            io_backend: IoBackendMode::Auto,
            ring_size: 4096,
            sqpoll_idle_ms: 0,
            budgets: ReactorBudgets::default(),
            telemetry_mode: RuntimeTelemetryMode::Minimal,
            telemetry_local_sample_rate: 0,
            telemetry_flush_interval_nanos: METRICS_FLUSH_INTERVAL_NANOS,
        }
    }
}

pub(super) fn try_make_uring_backend(config: &ReactorConfig) -> std::io::Result<Backend> {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        Ok(Backend::Uring(crate::backend::IoUringBackend::new(
            config.ring_size,
            config.sqpoll_idle_ms,
        )?))
    }

    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    {
        let _ = config;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "io_uring backend is not available in this build",
        ))
    }
}

pub(super) fn make_backend(config: &ReactorConfig) -> std::io::Result<(BackendPlan, Backend)> {
    fn planned(requested: IoBackendMode, backend: Backend) -> (BackendPlan, Backend) {
        (
            BackendPlan {
                requested,
                effective: backend.kind(),
                capabilities: backend.capabilities(),
            },
            backend,
        )
    }

    match config.io_backend {
        IoBackendMode::Polling => {
            let backend = Backend::Polling(PollingBackend::new()?);
            Ok(planned(config.io_backend, backend))
        }
        IoBackendMode::Uring => {
            let backend = try_make_uring_backend(config)?;
            Ok(planned(config.io_backend, backend))
        }
        IoBackendMode::Auto => match try_make_uring_backend(config) {
            Ok(backend) => Ok(planned(config.io_backend, backend)),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    ring_size = config.ring_size,
                    sqpoll_idle_ms = config.sqpoll_idle_ms,
                    "io_uring backend unavailable, falling back to polling"
                );
                let backend = Backend::Polling(PollingBackend::new()?);
                Ok(planned(config.io_backend, backend))
            }
        },
    }
}

#[inline]
pub(super) fn runtime_requested_backend(mode: IoBackendMode) -> RuntimeBackendMode {
    match mode {
        IoBackendMode::Auto => RuntimeBackendMode::Auto,
        IoBackendMode::Uring => RuntimeBackendMode::IoUring,
        IoBackendMode::Polling => RuntimeBackendMode::Polling,
    }
}

#[inline]
pub(super) fn runtime_effective_backend(kind: BackendKind) -> RuntimeBackendMode {
    match kind {
        BackendKind::Polling => RuntimeBackendMode::Polling,
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        BackendKind::Uring => RuntimeBackendMode::IoUring,
        #[cfg(test)]
        BackendKind::Test => RuntimeBackendMode::Test,
    }
}

#[inline]
pub(super) fn backend_plan_for(config: &ReactorConfig, backend: &Backend) -> BackendPlan {
    BackendPlan {
        requested: config.io_backend,
        effective: backend.kind(),
        capabilities: backend.capabilities(),
    }
}

#[inline]
pub(super) fn backend_flush_counts_submit_syscall(plan: BackendPlan) -> bool {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        plan.effective == BackendKind::Uring && plan.capabilities.sqpoll
    }
    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    {
        let _ = plan;
        false
    }
}

#[inline]
pub(super) fn backend_completions_count_submit_syscall(plan: BackendPlan) -> bool {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        plan.effective == BackendKind::Uring && !plan.capabilities.sqpoll
    }
    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    {
        let _ = plan;
        false
    }
}

#[inline]
pub(super) fn backend_drain_cq_counts_submit_syscall(plan: BackendPlan) -> bool {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        plan.effective == BackendKind::Uring
    }
    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    {
        let _ = plan;
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FixedBufferPolicy {
    Register,
    Disabled,
}
