//! Single-threaded event loop reactor.
//!
//! Each reactor is pinned to a CPU core and owns its own I/O backend,
//! connection slab, buffer pool, timer wheel, and buffer state.

use std::collections::VecDeque;
use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;

use crate::aof::{
    AofCoordinator, AofEpoch, reactor_aof_writer_path, runtime_reconfigure_unsupported,
};
use crate::backend::{
    Backend, BackendCapabilities, BackendKind, BackendPlan, BackendQueueStatus, CancelResult,
    Completion, CompletionToken, ConnFd, DecodedCompletionToken, FixedBufferId, IovecBatch,
    ListenerFd, OpType, PollingBackend, ReadLease, SubmitError, WriteLease, encode_token,
};
use crate::connection::{ConnectionFlags, ConnectionMeta, ConnectionSlab};
use crate::pool::{FixedBufferRegistrationMode, IoBackendMode};
use crate::shutdown::ShutdownCoordinator;
use crate::timer::{ExpiredTimer, TimerWheel};
use vortex_common::{Timestamp, current_unix_time_nanos};
use vortex_engine::commands::{
    AofCommitEffect, CmdResult, CommandClock, arg_bytes, arg_count, key_from_bytes,
};
use vortex_engine::keyspace::{
    AofLsn, DEFAULT_SHARD_COUNT, RuntimeAofTelemetry, RuntimeBackendMode, RuntimeBackendSnapshot,
    RuntimeLocalFlushMetrics, RuntimeOverloadTelemetry, RuntimeTelemetryMode, TransactionGatePlan,
    WatchRegistration,
};
use vortex_engine::{
    CommandExecutionScope, ConcurrentKeyspace, EvictionPolicy, SharedKeyspaceExecutor,
};
use vortex_memory::{ArenaAllocator, BufferPool};
#[cfg(test)]
use vortex_persist::aof::AofCommitPoint;
use vortex_persist::aof::writer::{AofFileWriter, DEFAULT_EVERYSEC_MAX_PENDING_BYTES};
use vortex_persist::aof::{
    AOF_TRANSACTION_BATCH_COMMAND, AofAppendOutcome, AofDurabilityRequirement, AofReactorId,
    AofRecordBytes, AofTelemetrySnapshot,
};
use vortex_proto::{
    BorrowedRespTape, CommandFlags, CommandRouter, DispatchResult, FrameRef, IovecWriter,
    ParseError, RespFrame, RespSerializer, TapeEntry, uppercase_inplace,
};

mod accept;
mod admission;
mod aof;
mod command_scope;
mod config;
mod dispatch;
mod event_loop;
mod inflight;
mod maintenance;
mod read_path;
mod shutdown;
mod state;
#[cfg(test)]
mod tests;
mod transaction;
mod types;
mod write_path;

pub(crate) use self::config::AofRuntime;
pub use self::config::{AofConfig, ConnectionMemoryCaps, ReactorConfig, ReactorOverloadPolicy};
pub use self::types::{
    AcceptBudget, CommandBudget, CompletionBudget, MaintenanceBudget, ReactorBudgets, TimeBudget,
    WritevBudget,
};

use self::admission::ReactorOverloadState;
use self::command_scope::command_keyspace_gate_scope;
use self::config::{
    AofWriterSlot, FixedBufferPolicy, backend_completions_count_submit_syscall,
    backend_drain_cq_counts_submit_syscall, backend_flush_counts_submit_syscall, backend_plan_for,
    make_backend, open_aof_writer, runtime_effective_backend, runtime_requested_backend,
};
use self::inflight::InflightSet;
#[cfg(test)]
use self::types::append_resp_frame;
use self::types::{
    CommandAccumulator, CommandResponse, CommandSliceBudget, CommandSliceOutcome, MaintenanceClass,
    MaintenanceRun, MaintenanceScheduler, QueuedCommandResult, ReactorLocalMetrics, SliceBudget,
    TransactionAofBatch, TransactionState, append_cmd_result, config_pair_response,
    push_resp_array_len, push_resp_bulk_string,
};
use self::write_path::PendingWritev;

/// Default read buffer size per connection (16 KB).
const DEFAULT_BUF_SIZE: usize = 16_384;
/// Default maximum bytes retained for one in-flight request or pipeline.
const DEFAULT_MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

const DEFAULT_COMPLETION_BUDGET: CompletionBudget = CompletionBudget::new_unchecked(256);
const DEFAULT_COMMAND_BUDGET: CommandBudget = CommandBudget::new_unchecked(1024);
const DEFAULT_ACCEPT_BUDGET: AcceptBudget = AcceptBudget::new_unchecked(64);
const DEFAULT_WRITEV_BUDGET: WritevBudget = WritevBudget::new_unchecked(IovecBatch::MAX_SEGMENTS);
const DEFAULT_MAINTENANCE_BUDGET: MaintenanceBudget = MaintenanceBudget::new_unchecked(4);
const COMMAND_TIME_CHECK_GRANULARITY: usize = 64;
const ACTIVE_EXPIRY_INTERVAL_NANOS: u64 = 1_000_000;
const ACTIVE_EXPIRY_MAX_EFFORT: usize = 20;
const METRICS_FLUSH_INTERVAL_NANOS: u64 = 100_000_000;

/// Pre-computed RESP error for unknown commands.
static RESP_ERR_UNKNOWN: &[u8] = b"-ERR unknown command\r\n";
static RESP_ERR_WRONG_ARGC: &[u8] = b"-ERR wrong number of arguments for command\r\n";
/// Pre-computed RESP error for protocol failures.
static RESP_ERR_PROTOCOL: &[u8] = b"-ERR protocol error\r\n";
static RESP_ERR_AOF_MISCONF: &[u8] =
    b"-MISCONF AOF persistence error; write commands are disabled until the issue is resolved\r\n";
static RESP_ERR_BGREWRITEAOF_DISABLED: &[u8] =
    b"-ERR BGREWRITEAOF is disabled for the alpha release\r\n";
static RESP_ERR_CONFIG_SET_MAXMEMORY: &[u8] = b"-ERR invalid argument for CONFIG SET maxmemory\r\n";
static RESP_ERR_CONFIG_SET_POLICY: &[u8] =
    b"-ERR invalid argument for CONFIG SET maxmemory-policy\r\n";
static RESP_ERR_CONFIG_SET_APPENDONLY_MULTI: &[u8] = b"-ERR CONFIG SET appendonly is disabled in multi-reactor alpha mode; configure appendonly at startup\r\n";
static RESP_ERR_REQUEST_TOO_LARGE: &[u8] = b"-ERR request too large\r\n";
static RESP_ERR_RESPONSE_TOO_LARGE: &[u8] = b"-ERR response memory limit exceeded\r\n";

/// Responses smaller than this threshold are copied into the write buffer.
/// Larger responses use scatter-gather `writev` to avoid contiguous copies.
#[allow(dead_code)] // Infrastructure for Phase 2.5 engine command integration.
const WRITEV_THRESHOLD: usize = 256;
const MAX_TRANSACTION_COMMANDS: usize = 128;

static RESP_QUEUED: &[u8] = b"+QUEUED\r\n";
static RESP_NULL_ARRAY: &[u8] = b"*-1\r\n";
static RESP_ERR_EXEC_WITHOUT_MULTI: &[u8] = b"-ERR EXEC without MULTI\r\n";
static RESP_ERR_DISCARD_WITHOUT_MULTI: &[u8] = b"-ERR DISCARD without MULTI\r\n";
static RESP_ERR_NESTED_MULTI: &[u8] = b"-ERR MULTI calls can not be nested\r\n";
static RESP_ERR_WATCH_INSIDE_MULTI: &[u8] = b"-ERR WATCH inside MULTI is not allowed\r\n";
static RESP_ERR_EXECABORT: &[u8] =
    b"-EXECABORT Transaction discarded because of previous errors.\r\n";
static RESP_ERR_TX_QUEUE_FULL: &[u8] = b"-ERR transaction queue limit exceeded\r\n";
static RESP_ERR_TX_QUEUE_BYTES: &[u8] = b"-ERR transaction queue memory limit exceeded\r\n";
static RESP_ERR_WATCH_LIMIT: &[u8] = b"-ERR watch registration limit exceeded\r\n";

/// Single-threaded event loop reactor.
pub struct Reactor {
    /// Reactor ID (typically matches the CPU core index).
    pub id: usize,
    /// I/O backend (polling or io_uring).
    backend: Backend,
    /// Whether backend flush may issue an io_uring submit syscall.
    backend_flush_counts_submit_syscall: bool,
    /// Whether completion wait may issue an io_uring submit syscall.
    backend_completions_count_submit_syscall: bool,
    /// Whether non-blocking CQ drain may submit SQEs after progress.
    backend_drain_cq_counts_submit_syscall: bool,
    /// Connection slab.
    connections: ConnectionSlab,
    /// Listening socket file descriptor.
    listener_fd: RawFd,
    /// mmap-backed buffer pool for connection I/O buffers.
    ///
    /// Each accepted connection leases two buffers (read + write) via
    /// `lease_index()`. Buffers are registered with the kernel for
    /// zero-copy `ReadFixed`/`WriteFixed` operations on io_uring.
    buffer_pool: BufferPool,
    /// True only after startup validates and registers io_uring fixed buffers.
    fixed_buffers_enabled: bool,
    /// Per-slot generation counters: indexed by slab token.
    /// 24-bit effective range (masked with `0xFF_FFFF`). Incremented each
    /// time a slot is reused, preventing stale CQE processing.
    generations: Vec<u32>,
    /// Reusable completions drain buffer.
    cqe_buf: Vec<Completion>,
    /// Completion overflow kept in reactor order after a budgeted slice yields.
    pending_completions: VecDeque<Completion>,
    /// Reactor-local high-frequency runtime counters flushed on the cold
    /// metrics maintenance cadence.
    local_metrics: ReactorLocalMetrics,
    /// Malformed backend completion tokens dropped by this reactor.
    invalid_completion_tokens: u64,
    /// Well-formed connection completions dropped because no matching
    /// operation is in flight for the current slot generation.
    unexpected_completion_tokens: u64,
    /// Hierarchical timing wheel for connection idle timeouts.
    timer_wheel: TimerWheel,
    /// Monotonic nanosecond epoch captured at reactor start.
    start_nanos: u64,
    /// Cached current time in seconds since `start_nanos`.
    now_secs: u32,
    /// Reusable buffer for expired timer entries.
    expired_buf: Vec<ExpiredTimer>,
    /// Timer expirations deferred after a maintenance budget is exhausted.
    pending_expired_timers: VecDeque<ExpiredTimer>,
    /// Closing connections whose terminal CQEs have arrived and whose resource
    /// release is deferred to the bounded close-drain maintenance class.
    pending_close_finalization: VecDeque<usize>,
    /// Idle connection timeout in seconds (0 = disabled).
    connection_timeout: u32,
    /// Whether the reactor is running.
    running: bool,
    /// Shared shutdown coordinator.
    coordinator: Arc<ShutdownCoordinator>,
    /// Whether we're in drain mode (no new accepts).
    draining: bool,
    /// Configuration.
    config: ReactorConfig,
    /// Per-iteration bump allocator for transient response building.
    arena: ArenaAllocator,
    /// Per-connection scatter-gather write state. Each slot owns the RESP
    /// frames, serializer scratch, and raw iovec array for any in-flight
    /// WRITEV operation so io_uring can safely complete asynchronously.
    writev_states: Vec<PendingWritev>,
    /// Per-connection in-flight I/O ownership. Buffers must not be released
    /// until all tracked ops have completed or been canceled.
    inflight_ops: Vec<InflightSet>,
    /// Monotonic close-start timestamps for measuring close-drain duration.
    close_started_nanos: Vec<u64>,
    /// Per-slot close-finalization queue membership guard.
    close_finalization_pending: Vec<bool>,
    /// Per-connection MULTI/WATCH state. Kept outside `ConnectionMeta` so the
    /// hot connection cache line stays fixed at 64 bytes.
    transaction_states: Vec<TransactionState>,
    /// Per-connection owned parser bytes for requests that outgrow the fixed
    /// read buffer or survive a budget yield.
    command_accumulators: Vec<CommandAccumulator>,
    /// Command dispatch router with PHF lookup.
    command_router: CommandRouter,
    /// Concrete command executor for the alpha shared-keyspace topology.
    command_executor: SharedKeyspaceExecutor,
    /// Shared concurrent keyspace — all reactors operate on the same data.
    keyspace: Arc<ConcurrentKeyspace>,
    /// Cached monotonic timestamp (nanoseconds) for the current event-loop iteration.
    /// Avoids re-reading the clock for every command in a batch.
    cached_nanos: u64,
    /// Cached Unix wall-clock timestamp (nanoseconds) paired with `cached_nanos`.
    cached_unix_nanos: u64,
    /// Next time at which the background active-expiry sweep should run.
    /// Active expiry is opportunistic cleanup; lazy expiry on key access
    /// remains the correctness path for expired reads.
    next_active_expiry_nanos: u64,
    /// Next time at which cold runtime telemetry should be republished.
    next_metrics_flush_nanos: u64,
    /// AOF writer (None if persistence disabled).
    /// Per-reactor AOF writer. Each reactor owns its own file, avoiding
    /// cross-reactor synchronization on the I/O hot path. Global ordering
    /// is provided by the LSN prefix (from `ConcurrentKeyspace::next_lsn()`)
    /// and K-Way merge on replay.
    aof_writer: Option<AofWriterSlot>,
    /// Reusable scratch buffer for serializing RESP frames to AOF.
    /// 4 KB is enough for any single command (max key 512 bytes + value + overhead).
    aof_scratch: Vec<u8>,
    /// Pool-owned AOF mode and write-stop coordinator.
    aof_coordinator: Arc<AofCoordinator>,
    #[cfg(test)]
    aof_append_fail_after: Option<usize>,
    #[cfg(test)]
    aof_fsync_fail_after: Option<usize>,
    /// Active expiry: current shard index being swept by this reactor.
    /// Each reactor sweeps different shards in round-robin fashion to
    /// distribute expiry load across the pool.
    expiry_shard_cursor: usize,
    /// Active expiry: current slot offset within the current shard.
    expiry_slot_cursor: usize,
    /// Eviction maintenance: current shard index for pressure slices.
    eviction_shard_cursor: usize,
    /// Round-robin scheduler for bounded maintenance classes.
    maintenance_scheduler: MaintenanceScheduler,
    /// Reactor-local admission and overload backpressure state.
    overload: ReactorOverloadState,
    /// Reusable RESP parser tape entries. Kept reactor-local so command
    /// parsing does not allocate on every read batch.
    parse_entries: Vec<TapeEntry>,
}

impl Drop for Reactor {
    fn drop(&mut self) {
        self.close_listener_fd();
    }
}
