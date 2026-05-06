//! Single-threaded event loop reactor.
//!
//! Each reactor is pinned to a CPU core and owns its own I/O backend,
//! connection slab, buffer pool, timer wheel, and buffer state.

use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::backend::{
    ACCEPT_CONN_ID, Completion, IoBackend, OpType, PollingBackend, decode_token, encode_token,
};
use crate::connection::{ConnectionFlags, ConnectionMeta, ConnectionSlab, ConnectionState};
use crate::pool::IoBackendMode;
use crate::shutdown::ShutdownCoordinator;
use crate::timer::{ExpiredTimer, TimerWheel};
use vortex_common::{Timestamp, current_unix_time_nanos};
use vortex_engine::commands::{
    CmdResult, CommandClock, arg_bytes, arg_count, execute_command, key_from_bytes,
};
use vortex_engine::keyspace::{DEFAULT_SHARD_COUNT, WatchRegistration};
use vortex_engine::{ConcurrentKeyspace, EvictionPolicy};
use vortex_memory::{ArenaAllocator, BufferPool};
use vortex_persist::aof::writer::AofFileWriter;
use vortex_proto::{
    BorrowedRespTape, CommandFlags, CommandMeta, CommandRouter, DispatchResult, FrameRef,
    IovecWriter, ParseError, RespFrame, RespSerializer, TapeEntry, uppercase_inplace,
};

/// Default read buffer size per connection (16 KB).
const DEFAULT_BUF_SIZE: usize = 16_384;

/// Maximum completions processed per iteration.
const MAX_COMPLETIONS: usize = 256;
/// Maximum extra sockets drained after one accept readiness notification.
const ACCEPT_DRAIN_BUDGET: usize = 512;

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

/// Command response type — determines serialization strategy.
#[allow(dead_code)] // Frame variant is infrastructure for Phase 2.5 engine commands.
#[derive(Debug)]
enum CommandResponse {
    /// Pre-computed static response (PONG, OK, ERR).
    /// Emitted as a zero-copy `writev` segment.
    Static(&'static [u8]),
    /// Tiny dynamic response already serialized into inline RESP bytes.
    Inline(vortex_engine::commands::InlineResp),
    /// Dynamic RESP frame requiring serialization.
    /// Serialized into `writev` segments owned by [`PendingWritev`].
    Frame(RespFrame),
    /// Owned serialized RESP bytes.
    Owned(Box<[u8]>),
}

#[derive(Default)]
struct TransactionState {
    queueing: bool,
    dirty: bool,
    queued: Vec<Box<[u8]>>,
    watched: Vec<WatchRegistration>,
    watch_epoch: u64,
}

impl TransactionState {
    #[inline]
    fn reset_multi(&mut self) {
        self.queueing = false;
        self.dirty = false;
        self.queued.clear();
    }

    #[inline]
    fn reset_all(&mut self) {
        self.reset_multi();
        debug_assert!(
            self.watched.is_empty(),
            "WATCH registrations must be released through clear_watches"
        );
        self.watch_epoch = 0;
    }
}

fn config_pair_response(param: &'static [u8], value: Vec<u8>) -> (CommandResponse, bool) {
    (
        CommandResponse::Frame(RespFrame::Array(Some(vec![
            RespFrame::BulkString(Some(bytes::Bytes::from_static(param))),
            RespFrame::BulkString(Some(bytes::Bytes::from(value))),
        ]))),
        false,
    )
}

#[inline]
fn push_resp_array_len(buf: &mut Vec<u8>, len: usize) {
    buf.push(b'*');
    push_decimal(buf, len);
    buf.extend_from_slice(b"\r\n");
}

#[inline]
fn push_resp_bulk_string(buf: &mut Vec<u8>, value: &[u8]) {
    buf.push(b'$');
    push_decimal(buf, value.len());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(value);
    buf.extend_from_slice(b"\r\n");
}

#[inline]
fn push_decimal<T: itoa::Integer>(buf: &mut Vec<u8>, value: T) {
    let mut digits = itoa::Buffer::new();
    buf.extend_from_slice(digits.format(value).as_bytes());
}

fn append_cmd_result(buf: &mut Vec<u8>, result: CmdResult) {
    match result {
        CmdResult::Static(bytes) => buf.extend_from_slice(bytes),
        CmdResult::Inline(inline) => buf.extend_from_slice(inline.as_bytes()),
        CmdResult::Resp(frame) => append_resp_frame(buf, &frame),
    }
}

fn append_resp_frame(buf: &mut Vec<u8>, frame: &RespFrame) {
    let start = buf.len();
    let mut len = 128usize;
    loop {
        buf.resize(start + len, 0);
        if let Some(written) = RespSerializer::serialize_to_slice(frame, &mut buf[start..]) {
            buf.truncate(start + written);
            return;
        }
        len = len.saturating_mul(2);
    }
}

#[derive(Default)]
pub(crate) struct AofFatalState {
    failed: AtomicBool,
}

impl AofFatalState {
    #[inline]
    fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }

    fn mark_failed(&self, reactor_id: usize, context: &'static str, error: &io::Error) {
        let was_failed = self.failed.swap(true, Ordering::Relaxed);
        if was_failed {
            tracing::debug!(
                reactor_id,
                context,
                error = %error,
                "AOF fatal state already set"
            );
        } else {
            tracing::error!(
                reactor_id,
                context,
                error = %error,
                "AOF persistence failed; rejecting future writes"
            );
        }
    }
}

struct PendingWritev {
    writer: IovecWriter,
    frames: Vec<RespFrame>,
    owned: Vec<Box<[u8]>>,
    iovecs: Vec<libc::iovec>,
    iov_start: usize,
    total_len: usize,
}

impl PendingWritev {
    fn new() -> Self {
        Self {
            writer: IovecWriter::new(),
            frames: Vec::new(),
            owned: Vec::new(),
            iovecs: Vec::new(),
            iov_start: 0,
            total_len: 0,
        }
    }

    fn clear(&mut self) {
        self.writer.clear();
        self.frames.clear();
        self.owned.clear();
        self.iovecs.clear();
        self.iov_start = 0;
        self.total_len = 0;
    }

    fn push_static(&mut self, buf: &'static [u8]) {
        self.writer.push_static(buf);
    }

    fn push_inline(&mut self, buf: &[u8]) {
        self.writer.push_scratch(buf);
    }

    fn push_owned(&mut self, buf: Box<[u8]>) {
        if buf.is_empty() {
            return;
        }
        self.owned.push(buf);
        let idx = self.owned.len() - 1;
        self.writer.push_bytes(self.owned[idx].as_ref());
    }

    fn push_frame(&mut self, frame: RespFrame) {
        self.frames.push(frame);
        let idx = self.frames.len() - 1;
        RespSerializer::serialize_to_iovecs(&self.frames[idx], &mut self.writer);
    }

    fn finalize(&mut self) {
        self.total_len = self.writer.total_len();
        self.iovecs = self.writer.as_raw_iovecs();
        self.iov_start = 0;
    }

    fn remaining_len(&self) -> usize {
        self.total_len
    }

    fn remaining_iovecs(&self) -> &[libc::iovec] {
        &self.iovecs[self.iov_start..]
    }

    fn advance(&mut self, mut bytes_written: usize) {
        while bytes_written > 0 && self.iov_start < self.iovecs.len() {
            let iov = &mut self.iovecs[self.iov_start];
            if bytes_written < iov.iov_len {
                // SAFETY: advancing within the currently outstanding iovec segment.
                iov.iov_base =
                    unsafe { (iov.iov_base as *mut u8).add(bytes_written) }.cast::<libc::c_void>();
                iov.iov_len -= bytes_written;
                self.total_len -= bytes_written;
                return;
            }

            let consumed = iov.iov_len;
            bytes_written -= consumed;
            self.total_len -= consumed;
            self.iov_start += 1;
        }
    }
}

#[derive(Clone, Copy, Default)]
struct InflightOps {
    read: bool,
    write: bool,
    writev: bool,
    close: bool,
}

impl InflightOps {
    #[inline]
    fn mark_submitted(&mut self, op: OpType) {
        match op {
            OpType::Read => self.read = true,
            OpType::Write => self.write = true,
            OpType::Writev => self.writev = true,
            OpType::Close => self.close = true,
            OpType::Accept => {}
        }
    }

    #[inline]
    fn mark_completed(&mut self, op: OpType) {
        match op {
            OpType::Read => self.read = false,
            OpType::Write => self.write = false,
            OpType::Writev => self.writev = false,
            OpType::Close => self.close = false,
            OpType::Accept => {}
        }
    }

    #[inline]
    fn has_any(&self) -> bool {
        self.read || self.write || self.writev || self.close
    }
}

/// Configuration for a single reactor.
pub struct ReactorConfig {
    /// Address to bind the listener on.
    pub bind_addr: std::net::SocketAddr,
    /// Max number of client connections per reactor.
    pub max_connections: usize,
    /// Read buffer size in bytes.
    pub buffer_size: usize,
    /// Number of pre-allocated I/O buffers.
    pub buffer_count: usize,
    /// Idle connection timeout in seconds (0 = disabled).
    pub connection_timeout: u32,
    /// AOF persistence configuration (None = disabled).
    pub aof_config: Option<AofConfig>,
    /// I/O backend selection.
    pub io_backend: IoBackendMode,
    /// io_uring submission queue size.
    pub ring_size: u32,
    /// SQPOLL idle timeout in milliseconds.
    pub sqpoll_idle_ms: u32,
}

/// AOF configuration passed to each reactor.
#[derive(Clone, Debug)]
pub struct AofConfig {
    /// Base path for AOF files. Each reactor appends its shard ID.
    pub path: std::path::PathBuf,
    /// Fsync policy.
    pub fsync_policy: vortex_persist::aof::AofFsyncPolicy,
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:6379".parse().expect("valid default addr"),
            max_connections: 1024,
            buffer_size: DEFAULT_BUF_SIZE,
            buffer_count: 2048,
            connection_timeout: 300,
            aof_config: None,
            io_backend: IoBackendMode::Auto,
            ring_size: 4096,
            sqpoll_idle_ms: 1000,
        }
    }
}

fn try_make_uring_backend(config: &ReactorConfig) -> std::io::Result<Box<dyn IoBackend>> {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        Ok(Box::new(crate::backend::IoUringBackend::new(
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

fn make_backend(config: &ReactorConfig) -> std::io::Result<Box<dyn IoBackend>> {
    match config.io_backend {
        IoBackendMode::Polling => Ok(Box::new(PollingBackend::new()?)),
        IoBackendMode::Uring => try_make_uring_backend(config),
        IoBackendMode::Auto => match try_make_uring_backend(config) {
            Ok(backend) => Ok(backend),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    ring_size = config.ring_size,
                    sqpoll_idle_ms = config.sqpoll_idle_ms,
                    "io_uring backend unavailable, falling back to polling"
                );
                Ok(Box::new(PollingBackend::new()?))
            }
        },
    }
}

/// Single-threaded event loop reactor.
pub struct Reactor {
    /// Reactor ID (typically matches the CPU core index).
    pub id: usize,
    /// I/O backend (polling or io_uring).
    backend: Box<dyn IoBackend>,
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
    /// Per-slot generation counters: indexed by slab token.
    /// 24-bit effective range (masked with `0xFF_FFFF`). Incremented each
    /// time a slot is reused, preventing stale CQE processing.
    generations: Vec<u32>,
    /// Reusable completions drain buffer.
    cqe_buf: Vec<Completion>,
    /// Hierarchical timing wheel for connection idle timeouts.
    timer_wheel: TimerWheel,
    /// Monotonic nanosecond epoch captured at reactor start.
    start_nanos: u64,
    /// Cached current time in seconds since `start_nanos`.
    now_secs: u32,
    /// Reusable buffer for expired timer entries.
    expired_buf: Vec<ExpiredTimer>,
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
    inflight_ops: Vec<InflightOps>,
    /// Per-connection MULTI/WATCH state. Kept outside `ConnectionMeta` so the
    /// hot connection cache line stays fixed at 64 bytes.
    transaction_states: Vec<TransactionState>,
    /// Command dispatch router with PHF lookup.
    command_router: CommandRouter,
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
    /// AOF writer (None if persistence disabled).
    /// Per-reactor AOF writer. Each reactor owns its own file, avoiding
    /// cross-reactor synchronization on the I/O hot path. Global ordering
    /// is provided by the LSN prefix (from `ConcurrentKeyspace::next_lsn()`)
    /// and K-Way merge on replay.
    aof_writer: Option<AofFileWriter>,
    /// Reusable scratch buffer for serializing RESP frames to AOF.
    /// 4 KB is enough for any single command (max key 512 bytes + value + overhead).
    aof_scratch: Vec<u8>,
    /// Shared durable-write health state. Once persistence fails, reactors
    /// reject future write commands before mutating the keyspace.
    aof_fatal_state: Arc<AofFatalState>,
    /// Active expiry: current shard index being swept by this reactor.
    /// Each reactor sweeps different shards in round-robin fashion to
    /// distribute expiry load across the pool.
    expiry_shard_cursor: usize,
    /// Active expiry: current slot offset within the current shard.
    expiry_slot_cursor: usize,
    /// Reusable RESP parser tape entries. Kept reactor-local so command
    /// parsing does not allocate on every read batch.
    parse_entries: Vec<TapeEntry>,
}

impl Reactor {
    #[inline]
    fn has_exact_arity(frame: &FrameRef<'_>, expected: usize) -> bool {
        frame
            .element_count()
            .is_some_and(|argc| argc as usize == expected)
    }

    #[inline]
    fn has_min_arity(frame: &FrameRef<'_>, minimum: usize) -> bool {
        frame
            .element_count()
            .is_some_and(|argc| argc as usize >= minimum)
    }

    #[inline]
    fn mark_transaction_dirty(&mut self, conn_id: usize) {
        if conn_id < self.transaction_states.len() {
            self.transaction_states[conn_id].dirty = true;
        }
    }

    pub(crate) fn min_buffer_count_for_connections(max_connections: usize) -> io::Result<usize> {
        Ok(max_connections)
    }

    fn validate_buffer_configuration(
        max_connections: usize,
        buffer_count: usize,
    ) -> io::Result<()> {
        if buffer_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer_count must be greater than zero",
            ));
        }

        let min_buffer_count = Self::min_buffer_count_for_connections(max_connections)?;
        if buffer_count < min_buffer_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "buffer_count ({buffer_count}) must be at least max_connections ({min_buffer_count})"
                ),
            ));
        }

        Ok(())
    }

    /// Creates a new reactor with the polling backend and a fresh keyspace.
    ///
    /// This constructor creates its own `ConcurrentKeyspace`. For production
    /// use, prefer [`with_shared_keyspace`](Self::with_shared_keyspace)
    /// which accepts a pool-owned `Arc<ConcurrentKeyspace>`.
    pub fn new(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
    ) -> std::io::Result<Self> {
        let backend = make_backend(&config)?;
        let keyspace = Arc::new(ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT));
        let aof_fatal_state = Arc::new(AofFatalState::default());
        Self::with_keyspace_and_backend(
            id,
            config,
            coordinator,
            keyspace,
            backend,
            aof_fatal_state,
            1,
        )
    }

    /// Creates a new reactor with a specific backend and a fresh keyspace.
    pub fn with_backend(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
        backend: Box<dyn IoBackend>,
    ) -> std::io::Result<Self> {
        let keyspace = Arc::new(ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT));
        let aof_fatal_state = Arc::new(AofFatalState::default());
        Self::with_keyspace_and_backend(
            id,
            config,
            coordinator,
            keyspace,
            backend,
            aof_fatal_state,
            1,
        )
    }

    /// Creates a new reactor with a shared keyspace (used by `ReactorPool`).
    ///
    /// The keyspace is already populated (e.g. from AOF replay) before
    /// the reactor starts accepting connections.
    /// Creates a new reactor with a shared keyspace (used by `ReactorPool`).
    ///
    /// The keyspace is already populated (e.g. from AOF replay) before
    /// the reactor starts accepting connections.
    ///
    /// `num_reactors` controls active-expiry cursor staggering so each
    /// reactor sweeps a distinct shard region, avoiding duplicate work.
    pub fn with_shared_keyspace(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
        keyspace: Arc<ConcurrentKeyspace>,
        num_reactors: usize,
    ) -> std::io::Result<Self> {
        let aof_fatal_state = Arc::new(AofFatalState::default());
        Self::with_shared_keyspace_and_aof_state(
            id,
            config,
            coordinator,
            keyspace,
            aof_fatal_state,
            num_reactors,
        )
    }

    pub(crate) fn with_shared_keyspace_and_aof_state(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
        keyspace: Arc<ConcurrentKeyspace>,
        aof_fatal_state: Arc<AofFatalState>,
        num_reactors: usize,
    ) -> std::io::Result<Self> {
        let backend = make_backend(&config)?;
        Self::with_keyspace_and_backend(
            id,
            config,
            coordinator,
            keyspace,
            backend,
            aof_fatal_state,
            num_reactors,
        )
    }

    /// Internal constructor — all public constructors delegate here.
    pub(crate) fn with_keyspace_and_backend(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
        keyspace: Arc<ConcurrentKeyspace>,
        backend: Box<dyn IoBackend>,
        aof_fatal_state: Arc<AofFatalState>,
        num_reactors: usize,
    ) -> std::io::Result<Self> {
        Self::validate_buffer_configuration(config.max_connections, config.buffer_count)?;

        // Create the listener socket with SO_REUSEPORT + SO_REUSEADDR + SO_INCOMING_CPU.
        let listener_fd = crate::accept::create_listener(config.bind_addr, Some(id))?;

        let max_conn = config.max_connections;
        let connection_timeout = config.connection_timeout;

        // `buffer_count` is validated up-front and remains authoritative here.
        let buffer_pool = BufferPool::new(config.buffer_count, config.buffer_size);

        // Register the fixed buffers with the io_uring kernel if applicable.
        // For polling backends this is a no-op.
        let iovecs = buffer_pool.as_iovecs();
        if let Err(e) = backend.register_buffers(&iovecs) {
            tracing::warn!(error = %e, "failed to register fixed buffers (non-fatal on polling)");
        }

        // Initialize AOF writer if persistence is enabled.
        // NOTE: AOF replay is now handled at the pool level (before reactor creation).
        // The reactor only opens the writer for appending new mutations.
        let aof_writer = if let Some(ref aof_cfg) = config.aof_config {
            let aof_path = if id == 0 {
                aof_cfg.path.clone()
            } else {
                let stem = aof_cfg
                    .path
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy();
                let ext = aof_cfg
                    .path
                    .extension()
                    .unwrap_or_default()
                    .to_string_lossy();
                aof_cfg
                    .path
                    .with_file_name(format!("{stem}-shard{id}.{ext}"))
            };

            match AofFileWriter::open(&aof_path, id as u16, aof_cfg.fsync_policy) {
                Ok(w) => {
                    tracing::info!(
                        reactor_id = id,
                        path = %aof_path.display(),
                        policy = ?aof_cfg.fsync_policy,
                        "AOF writer initialized"
                    );
                    Some(w)
                }
                Err(e) => {
                    tracing::error!(
                        reactor_id = id,
                        error = %e,
                        "failed to open AOF file — persistence disabled for this reactor"
                    );
                    None
                }
            }
        } else {
            None
        };
        if aof_writer.is_some() {
            keyspace.enable_aof_recording();
        }

        // Stagger the starting shard cursor by reactor ID so each reactor
        // sweeps a distinct region, distributing expiry work evenly.
        // With K shards and N reactors, spacing = K/N.
        let expiry_shard_cursor = id * (keyspace.num_shards() / num_reactors.max(1)).max(1);

        let cached_nanos = Timestamp::now().as_nanos();
        let cached_unix_nanos = current_unix_time_nanos();

        Ok(Self {
            id,
            backend,
            connections: ConnectionSlab::with_capacity(max_conn),
            listener_fd,
            buffer_pool,
            generations: vec![0u32; max_conn],
            cqe_buf: Vec::with_capacity(MAX_COMPLETIONS),
            timer_wheel: TimerWheel::new(max_conn),
            start_nanos: cached_nanos,
            now_secs: 0,
            expired_buf: Vec::with_capacity(64),
            connection_timeout,
            running: false,
            coordinator,
            draining: false,
            config,
            arena: ArenaAllocator::new(vortex_memory::arena::DEFAULT_ARENA_CAPACITY),
            writev_states: std::iter::repeat_with(PendingWritev::new)
                .take(max_conn)
                .collect(),
            inflight_ops: vec![InflightOps::default(); max_conn],
            transaction_states: std::iter::repeat_with(TransactionState::default)
                .take(max_conn)
                .collect(),
            command_router: CommandRouter::new(),
            keyspace,
            cached_nanos,
            cached_unix_nanos,
            next_active_expiry_nanos: cached_nanos,
            aof_writer,
            aof_scratch: vec![0u8; 4096],
            aof_fatal_state,
            expiry_shard_cursor,
            expiry_slot_cursor: 0,
            parse_entries: Vec::with_capacity(64),
        })
    }

    /// Runs the reactor event loop. Blocks until shutdown.
    pub fn run(&mut self) {
        self.running = true;
        self.start_nanos = Timestamp::now().as_nanos();
        tracing::info!(reactor_id = self.id, "reactor starting");

        // Submit initial accept.
        if let Err(e) = self.submit_accept_rearm() {
            tracing::error!(error = %e, "failed to submit initial accept");
            return;
        }

        loop {
            // Cache monotonic time once per iteration and derive the seconds view from it.
            self.cached_nanos = Timestamp::now().as_nanos();
            self.cached_unix_nanos = current_unix_time_nanos();
            self.now_secs = ((self.cached_nanos - self.start_nanos) / 1_000_000_000) as u32;
            self.keyspace.record_reactor_loop_iteration(self.id);

            // 1. Flush pending submissions.
            if let Err(e) = self.backend.flush() {
                tracing::error!(error = %e, "backend flush failed");
                break;
            }

            // 2. Drain completions.
            self.cqe_buf.clear();
            match self.backend.completions(&mut self.cqe_buf) {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(error = %e, "backend completions failed");
                    break;
                }
            }
            if !self.cqe_buf.is_empty() {
                self.keyspace
                    .record_reactor_completion_batch(self.id, self.cqe_buf.len());
            }

            // 3. Process each completion.
            // Zero-alloc: temporarily take the buffer, process, then restore.
            let mut completions = std::mem::take(&mut self.cqe_buf);
            for cqe in &completions {
                let (conn_id, cgen, op) = decode_token(cqe.token);
                match op {
                    OpType::Accept => self.handle_accept(cqe),
                    _ => {
                        // Validate generation for non-accept operations.
                        // Stale CQEs from closed/reused slots are silently discarded.
                        let valid = self.connections.get(conn_id).is_some_and(|_| {
                            self.generations.get(conn_id).copied().unwrap_or(0) == cgen
                        });
                        if !valid {
                            continue;
                        }
                        match op {
                            OpType::Read => self.handle_read(conn_id, cqe),
                            OpType::Write | OpType::Writev => self.handle_write(conn_id, op, cqe),
                            OpType::Close => self.handle_close(conn_id),
                            OpType::Accept => unreachable!(),
                        }
                    }
                }
            }
            completions.clear();
            self.cqe_buf = completions;

            // 3b. Fast-path: process newly submitted ops (e.g., writes after
            //     reads) immediately, collapsing read→process→write into a
            //     single event-loop iteration instead of two.
            //     Uses drain_cq() for a non-blocking CQ peek — no syscall on
            //     io_uring, avoiding a third io_uring_enter per iteration.
            self.cqe_buf.clear();
            if self.backend.drain_cq(&mut self.cqe_buf).is_ok() && !self.cqe_buf.is_empty() {
                self.keyspace
                    .record_reactor_completion_batch(self.id, self.cqe_buf.len());
                let mut fast_completions = std::mem::take(&mut self.cqe_buf);
                for cqe in &fast_completions {
                    let (conn_id, cgen, op) = decode_token(cqe.token);
                    match op {
                        OpType::Accept => self.handle_accept(cqe),
                        _ => {
                            let valid = self.connections.get(conn_id).is_some_and(|_| {
                                self.generations.get(conn_id).copied().unwrap_or(0) == cgen
                            });
                            if !valid {
                                continue;
                            }
                            match op {
                                OpType::Read => self.handle_read(conn_id, cqe),
                                OpType::Write | OpType::Writev => {
                                    self.handle_write(conn_id, op, cqe);
                                }
                                OpType::Close => self.handle_close(conn_id),
                                OpType::Accept => unreachable!(),
                            }
                        }
                    }
                }
                fast_completions.clear();
                self.cqe_buf = fast_completions;
            }

            // 4. Tick timer wheel — expire idle connections.
            if self.connection_timeout > 0 {
                self.tick_timers();
            }

            // 5. Active expiry sweep — staggered deterministic shard cursors.
            // Each reactor sweeps different shards in round-robin, staggered
            // by reactor_id * (K / N), distributing load across the pool.
            // Re-sweep if >25% of sampled entries were expired (max 3 iters).
            // Throttle the sweep cadence so no-TTL workloads do not pay a
            // write-lock tax on every event-loop iteration.
            {
                let now = self.cached_nanos;
                if now >= self.next_active_expiry_nanos {
                    const ACTIVE_EXPIRY_INTERVAL_NANOS: u64 = 1_000_000;
                    const MAX_EXPIRY_ITERS: usize = 3;
                    const MAX_EFFORT: usize = 20;
                    self.next_active_expiry_nanos =
                        now.saturating_add(ACTIVE_EXPIRY_INTERVAL_NANOS);
                    if self.keyspace.has_expiring_keys() {
                        let num_shards = self.keyspace.num_shards();
                        for _ in 0..MAX_EXPIRY_ITERS {
                            let shard_idx = self.expiry_shard_cursor % num_shards;
                            let (expired, sampled) = self.keyspace.run_active_expiry_on_shard(
                                shard_idx,
                                self.expiry_slot_cursor,
                                MAX_EFFORT,
                                now,
                            );
                            self.keyspace
                                .record_reactor_active_expiry(self.id, sampled, expired);
                            // Advance cursors for next sweep.
                            self.expiry_slot_cursor =
                                self.expiry_slot_cursor.wrapping_add(MAX_EFFORT);
                            self.expiry_shard_cursor = self.expiry_shard_cursor.wrapping_add(1);
                            if sampled == 0 || expired * 4 <= sampled {
                                break;
                            }
                        }
                    }
                }
            }

            // 5.6 AOF periodic fsync — for `everysec` policy, flush to disk
            // at most once per second. No-op for `always` (synced inline)
            // or `no` (OS-managed). Cheap: just checks a timestamp.
            if let Some(ref mut w) = self.aof_writer {
                if let Err(error) = w.maybe_fsync() {
                    self.aof_fatal_state
                        .mark_failed(self.id, "maybe_fsync", &error);
                }
            }

            // 6. Check for force-kill (immediate exit, no flushing).
            if self.coordinator.is_force_kill() {
                tracing::warn!(
                    reactor_id = self.id,
                    "force-kill received, exiting immediately"
                );
                break;
            }

            // 7. Check for graceful shutdown.
            if self.coordinator.is_draining() && !self.draining {
                self.enter_drain_mode();
            }

            if self.draining && self.connections.is_empty() {
                tracing::info!(reactor_id = self.id, "drain complete, exiting");
                break;
            }

            // 8. Reset the per-iteration arena allocator.
            self.arena.reset();
        }

        // Flush and sync AOF before tearing down I/O.
        if let Some(ref mut w) = self.aof_writer {
            if let Err(e) = w.flush_and_sync() {
                tracing::error!(reactor_id = self.id, error = %e, "AOF final flush failed");
            } else {
                tracing::info!(reactor_id = self.id, "AOF flushed and synced on shutdown");
            }
        }

        // Cancel all in-flight I/O and drain completions before dropping the
        // BufferPool. This prevents use-after-unmap when MmapRegion drops.
        self.drain_inflight_io();

        self.running = false;

        // Signal the coordinator that this reactor is done.
        self.coordinator.reactor_finished(self.id);

        // Close the listener fd.
        // SAFETY: listener_fd is a valid fd we own.
        unsafe {
            libc::close(self.listener_fd);
        }
        tracing::info!(reactor_id = self.id, "reactor stopped");
    }

    /// Signals the reactor to stop (initiates graceful shutdown).
    pub fn stop(&self) {
        self.coordinator.initiate();
    }

    /// Returns whether the reactor is currently running.
    pub fn is_running(&self) -> bool {
        self.running
    }

    // ── Accept handler ─────────────────────────────────────────────

    fn handle_accept(&mut self, cqe: &Completion) {
        if cqe.result < 0 {
            let errno = -cqe.result;
            // EAGAIN/EWOULDBLOCK is normal for non-blocking accept — retry.
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                self.keyspace.record_reactor_accept_eagain_rearm(self.id);
                if !self.draining {
                    let _ = self.submit_accept_rearm();
                }
                return;
            }
            tracing::warn!(errno, "accept failed");
            if !self.draining {
                let _ = self.submit_accept_rearm();
            }
            return;
        }

        self.handle_accepted_fd(cqe.result);
        self.drain_ready_accepts();

        // Re-arm accept (unless draining).
        if !self.draining {
            let _ = self.submit_accept_rearm();
        }
    }

    fn drain_ready_accepts(&mut self) {
        for _ in 0..ACCEPT_DRAIN_BUDGET {
            let result = unsafe {
                let mut addr: libc::sockaddr_storage = std::mem::zeroed();
                let mut addr_len: libc::socklen_t =
                    std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                libc::accept(
                    self.listener_fd,
                    &mut addr as *mut libc::sockaddr_storage as *mut libc::sockaddr,
                    &mut addr_len,
                )
            };

            if result >= 0 {
                self.handle_accepted_fd(result);
                continue;
            }

            let err = std::io::Error::last_os_error();
            if err.kind() != std::io::ErrorKind::WouldBlock {
                tracing::warn!(errno = err.raw_os_error().unwrap_or(1), "accept failed");
            }
            break;
        }
    }

    fn handle_accepted_fd(&mut self, new_fd: RawFd) {
        unsafe {
            let nodelay: libc::c_int = 1;
            libc::setsockopt(
                new_fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &nodelay as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            let flags = libc::fcntl(new_fd, libc::F_GETFL);
            libc::fcntl(new_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        if self.connections.len() >= self.config.max_connections {
            tracing::warn!("max connections reached, rejecting");
            unsafe {
                libc::close(new_fd);
            }
            return;
        }

        let read_idx = match self.buffer_pool.lease_index() {
            Some(idx) => idx,
            None => {
                tracing::warn!("buffer pool exhausted (read), rejecting connection");
                unsafe {
                    libc::close(new_fd);
                }
                return;
            }
        };

        let mut meta = ConnectionMeta::new(new_fd, read_idx as u32);
        meta.last_active = self.now_secs;
        meta.read_buf_offset = read_idx as u32;
        meta.read_buf_len = 0;
        meta.write_buf_len = 0;
        let conn_id = self.connections.insert(meta);

        if conn_id < self.generations.len() {
            self.generations[conn_id] = self.generations[conn_id].wrapping_add(1) & 0xFF_FFFF;
        }
        let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
        self.writev_states[conn_id].clear();
        self.inflight_ops[conn_id] = InflightOps::default();
        self.transaction_states[conn_id].reset_all();

        tracing::debug!(
            reactor_id = self.id,
            conn_id,
            fd = new_fd,
            cgen,
            read_idx,
            "connection accepted"
        );

        if self.connection_timeout > 0 {
            let deadline = self.now_secs + self.connection_timeout;
            let entry = self.timer_wheel.schedule(conn_id, cgen, deadline);
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.timer_slot = entry;
            }
        }

        self.submit_read_for(conn_id, new_fd);
    }

    // ── Read handler ───────────────────────────────────────────────

    fn handle_read(&mut self, conn_id: usize, cqe: &Completion) {
        self.mark_inflight_completed(conn_id, OpType::Read);

        // Ignore late completions for connections being torn down.
        if let Some(c) = self.connections.get(conn_id) {
            if c.is_closing() {
                self.maybe_finalize_close(conn_id);
                return;
            }
        }

        if cqe.result <= 0 {
            // EOF or error — close connection.
            if cqe.result == 0 {
                tracing::debug!(conn_id, "client disconnected (EOF)");
            } else {
                tracing::debug!(conn_id, errno = -cqe.result, "read error");
            }
            self.close_connection(conn_id);
            return;
        }

        let bytes_read = cqe.result as usize;
        let fd = match self.connections.get(conn_id) {
            Some(c) => c.fd,
            None => return, // Connection already removed.
        };

        // Advance read cursor in ConnectionMeta.
        if let Some(c) = self.connections.get_mut(conn_id) {
            c.read_buf_len += bytes_read as u32;
        }

        // Transition to Active state and update last_active.
        if let Some(c) = self.connections.get_mut(conn_id) {
            if c.state() == ConnectionState::New {
                let _ = c.transition(ConnectionState::Active);
            }
            c.last_active = self.now_secs;
        }

        // Attempt to parse and process commands from the read buffer.
        self.process_commands(conn_id, fd);
    }

    /// Parse RESP frames from the read buffer and generate responses.
    ///
    /// Responses are accumulated into [`PendingWritev`] and submitted via
    /// `writev` / `IORING_OP_WRITEV`. This keeps request parsing on a single
    /// fixed read buffer while the response path owns its payloads separately.
    fn process_commands(&mut self, conn_id: usize, fd: RawFd) {
        let (read_idx, cursor) = match self.connections.get(conn_id) {
            Some(c) => (c.read_buf_offset as usize, c.read_buf_len as usize),
            None => return,
        };

        if cursor == 0 {
            self.submit_read_for(conn_id, fd);
            return;
        }

        self.writev_states[conn_id].clear();

        let read_ptr = self.buffer_pool.ptr(read_idx);

        // SAFETY: read_ptr points to a reactor-owned mmap-backed region that is
        // valid for `cursor` bytes. Access is single-threaded on the reactor.
        let read_slice = unsafe { std::slice::from_raw_parts(read_ptr, cursor) };

        let mut offset = 0;
        let mut close_after_write = false;
        let mut parse_entries = std::mem::take(&mut self.parse_entries);

        while offset < cursor {
            match BorrowedRespTape::parse_pipeline_into(
                &read_slice[offset..cursor],
                &mut parse_entries,
            ) {
                Ok(tape) => {
                    let batch_end = offset + tape.consumed();
                    let mut batch_width = 0usize;
                    for frame in tape.iter() {
                        batch_width += 1;
                        let (response, should_close) = self.dispatch_command(conn_id, &frame);
                        if should_close {
                            close_after_write = true;
                        }
                        match response {
                            CommandResponse::Static(buf) => {
                                self.writev_states[conn_id].push_static(buf)
                            }
                            CommandResponse::Inline(inline) => {
                                self.writev_states[conn_id].push_inline(inline.as_bytes());
                            }
                            CommandResponse::Owned(bytes) => {
                                self.writev_states[conn_id].push_owned(bytes);
                            }
                            CommandResponse::Frame(resp_frame) => {
                                self.writev_states[conn_id].push_frame(resp_frame);
                            }
                        }
                    }
                    if batch_width != 0 {
                        self.keyspace
                            .record_reactor_command_batch(self.id, batch_width);
                    }

                    if close_after_write {
                        break;
                    }

                    offset = batch_end;
                }
                Err(ParseError::NeedMoreData) => break,
                Err(
                    ParseError::FrameTooLarge
                    | ParseError::NestingTooDeep
                    | ParseError::InvalidFrame,
                ) => {
                    self.writev_states[conn_id].push_static(RESP_ERR_PROTOCOL);
                    close_after_write = true;
                    offset = cursor;
                    break;
                }
            }
        }

        // Shift unconsumed data to the front of the read buffer.
        if offset > 0 {
            let remaining = cursor - offset;
            // SAFETY: Source and dest are within the same mmap buffer. The
            // regions may overlap, so we use `copy` (memmove) not `copy_nonoverlapping`.
            unsafe {
                std::ptr::copy(read_ptr.add(offset), read_ptr, remaining);
            }
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.read_buf_len = remaining as u32;
            }
        }

        if close_after_write {
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.flags |= ConnectionFlags::CLOSE_AFTER_WRITE;
            }
        }

        parse_entries.clear();
        self.parse_entries = parse_entries;

        self.writev_states[conn_id].finalize();
        let total = self.writev_states[conn_id].remaining_len();
        if total > 0 {
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.write_buf_len = total as u32;
            }
            let (iov_ptr, iov_count) = {
                let remaining_iovecs = self.writev_states[conn_id].remaining_iovecs();
                (remaining_iovecs.as_ptr(), remaining_iovecs.len())
            };
            let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
            let token = encode_token(conn_id, cgen, OpType::Writev);
            if self
                .submit_backend_op("writev", |backend| {
                    backend.submit_writev(fd, iov_ptr, iov_count, token)
                })
                .is_err()
            {
                self.writev_states[conn_id].clear();
                self.close_connection(conn_id);
            } else {
                self.mark_inflight_submitted(conn_id, OpType::Writev);
            }
        } else if close_after_write {
            self.close_connection(conn_id);
        } else if self.draining {
            // Drain mode: no complete command and nothing to write — close.
            self.close_connection(conn_id);
        } else {
            // No complete command yet — re-arm read.
            self.writev_states[conn_id].clear();
            self.submit_read_for(conn_id, fd);
        }
    }

    /// UNWATCH
    ///
    /// Clears every key watched by the connection.
    ///
    /// Big-O: `O(W)`, where `W` is watched keys for this connection.
    ///
    /// Compatibility: Returns `OK` even when no keys are watched. `EXEC`,
    /// `DISCARD`, and connection close also call this path.
    ///
    /// Notes: Watch removals touch only the watch side table and do not take
    /// keyspace shard write locks.
    #[inline]
    fn clear_watches(&mut self, conn_id: usize) {
        if conn_id >= self.transaction_states.len() {
            return;
        }
        let watched = std::mem::take(&mut self.transaction_states[conn_id].watched);
        self.transaction_states[conn_id].watch_epoch = 0;
        if !watched.is_empty() {
            self.keyspace.unwatch_keys(watched);
        }
    }

    #[inline]
    fn clear_transaction_state(&mut self, conn_id: usize) {
        if conn_id >= self.transaction_states.len() {
            return;
        }
        self.clear_watches(conn_id);
        self.transaction_states[conn_id].reset_all();
    }

    /// MULTI
    ///
    /// Starts transaction queueing for one connection.
    ///
    /// Big-O: `O(Q)` only when stale queued commands must be cleared from a
    /// previous dirty state; the normal path is `O(1)`.
    ///
    /// Compatibility: Returns `OK` and queues later commands until `EXEC` or
    /// `DISCARD`. Nested `MULTI` is rejected and dirties the transaction in
    /// `dispatch_command`.
    ///
    /// Notes: The state is reactor-local to avoid adding transaction branches
    /// or connection lookups to the keyspace hot path.
    #[inline]
    fn begin_transaction(&mut self, conn_id: usize) -> (CommandResponse, bool) {
        if conn_id < self.transaction_states.len() {
            let tx = &mut self.transaction_states[conn_id];
            tx.queueing = true;
            tx.dirty = false;
            tx.queued.clear();
        }
        (
            CommandResponse::Static(vortex_engine::commands::RESP_OK),
            false,
        )
    }

    /// DISCARD
    ///
    /// Drops queued commands, exits transaction mode, and clears WATCH state.
    ///
    /// Big-O: `O(Q + W)`, where `Q` is queued commands and `W` is watched keys.
    ///
    /// Compatibility: Returns `OK` inside MULTI and `ERR DISCARD without MULTI`
    /// outside MULTI.
    ///
    /// Notes: Because queued commands were not executed, DISCARD never appends
    /// their payloads to AOF.
    #[inline]
    fn discard_transaction(&mut self, conn_id: usize) -> (CommandResponse, bool) {
        self.clear_transaction_state(conn_id);
        (
            CommandResponse::Static(vortex_engine::commands::RESP_OK),
            false,
        )
    }

    fn normalized_command_name(frame: &FrameRef<'_>) -> Option<([u8; 32], usize)> {
        let cmd_name = frame.command_name()?;
        let len = cmd_name.len();
        if len == 0 || len > 32 {
            return None;
        }
        let mut upper = [0u8; 32];
        upper[..len].copy_from_slice(cmd_name);
        uppercase_inplace(&mut upper[..len]);
        Some((upper, len))
    }

    #[inline]
    fn command_requires_keyspace_gate(&self, meta: &CommandMeta) -> bool {
        meta.flags.contains(CommandFlags::WRITE) || meta.flags.contains(CommandFlags::READ)
    }

    fn frame_to_owned_resp(&mut self, frame: &FrameRef<'_>) -> io::Result<Box<[u8]>> {
        let written = loop {
            if let Some(written) = frame.write_resp_to(&mut self.aof_scratch) {
                break written;
            }

            let next_len = self
                .aof_scratch
                .len()
                .max(1)
                .checked_mul(2)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "RESP frame exceeded scratch buffer growth limit",
                    )
                })?;
            self.aof_scratch.resize(next_len, 0);
        };
        Ok(self.aof_scratch[..written].to_vec().into_boxed_slice())
    }

    fn queue_transaction_command(
        &mut self,
        conn_id: usize,
        frame: &FrameRef<'_>,
    ) -> CommandResponse {
        if self.transaction_states[conn_id].queued.len() >= MAX_TRANSACTION_COMMANDS {
            self.transaction_states[conn_id].dirty = true;
            return CommandResponse::Static(RESP_ERR_TX_QUEUE_FULL);
        }

        match self.command_router.dispatch(frame) {
            DispatchResult::Dispatch { meta, .. } => {
                if self.aof_writer.is_some()
                    && self.aof_fatal_state.is_failed()
                    && meta.flags.contains(CommandFlags::WRITE)
                {
                    self.transaction_states[conn_id].dirty = true;
                    return CommandResponse::Static(RESP_ERR_AOF_MISCONF);
                }
            }
            DispatchResult::WrongArity { .. } => {
                self.transaction_states[conn_id].dirty = true;
                return CommandResponse::Static(RESP_ERR_WRONG_ARGC);
            }
            DispatchResult::UnknownCommand => {
                self.transaction_states[conn_id].dirty = true;
                return CommandResponse::Static(RESP_ERR_UNKNOWN);
            }
        }

        match self.frame_to_owned_resp(frame) {
            Ok(payload) => {
                self.transaction_states[conn_id].queued.push(payload);
                CommandResponse::Static(RESP_QUEUED)
            }
            Err(_) => {
                self.transaction_states[conn_id].dirty = true;
                CommandResponse::Static(RESP_ERR_PROTOCOL)
            }
        }
    }

    /// WATCH key [key ...]
    ///
    /// Registers optimistic CAS watches for the current connection. `EXEC`
    /// aborts with a nil array if any watched key changes before execution.
    ///
    /// Big-O: `O(K + D)`, where `K` is the number of requested keys and `D` is
    /// the current watched-key count for duplicate suppression.
    ///
    /// Compatibility: Requires at least one key; `WATCH` inside MULTI is
    /// rejected in `dispatch_command` and dirties the transaction.
    ///
    /// Notes: The first WATCH snapshots the global watch epoch. Writes stay on
    /// the fast path when no watches exist because keyspace mutation only
    /// consults the watch side table while the watch feature bit is active.
    fn handle_watch(&mut self, conn_id: usize, frame: &FrameRef<'_>) -> CommandResponse {
        let argc = match frame.element_count() {
            Some(argc) if argc >= 2 => argc as usize,
            _ => return CommandResponse::Static(RESP_ERR_WRONG_ARGC),
        };

        let mut keys = Vec::with_capacity(argc - 1);
        let Some(mut children) = frame.children() else {
            return CommandResponse::Static(RESP_ERR_WRONG_ARGC);
        };
        let _ = children.next();
        for child in children {
            let Some(key_bytes) = child.as_bytes() else {
                return CommandResponse::Static(RESP_ERR_WRONG_ARGC);
            };
            keys.push(key_from_bytes(key_bytes));
        }

        if self.transaction_states[conn_id].watched.is_empty() {
            self.transaction_states[conn_id].watch_epoch = self.keyspace.current_watch_epoch();
        }

        for key in keys {
            if self.transaction_states[conn_id]
                .watched
                .iter()
                .any(|watched| watched.key() == &key)
            {
                continue;
            }
            let watched = self.keyspace.watch_key(key);
            self.transaction_states[conn_id].watched.push(watched);
        }

        CommandResponse::Static(vortex_engine::commands::RESP_OK)
    }

    fn queued_payload_has_write(&mut self, payload: &[u8]) -> bool {
        let Ok(tape) = BorrowedRespTape::parse_pipeline(payload) else {
            return false;
        };
        let Some(frame) = tape.iter().next() else {
            return false;
        };
        match self.command_router.dispatch(&frame) {
            DispatchResult::Dispatch { meta, .. } => meta.flags.contains(CommandFlags::WRITE),
            _ => false,
        }
    }

    fn queued_transaction_has_write(&mut self, queued: &[Box<[u8]>]) -> bool {
        queued
            .iter()
            .any(|payload| self.queued_payload_has_write(payload.as_ref()))
    }

    /// EXEC
    ///
    /// Atomically validates WATCH state and executes queued commands for one
    /// connection, returning one RESP array with each command reply.
    ///
    /// Big-O: `O(W + Q * C)`, where `W` is watched keys, `Q` is queued commands,
    /// and `C` is the cost of each command. Response construction is linear in
    /// the serialized replies.
    ///
    /// Compatibility: Queue-time errors cause `EXECABORT`; runtime command
    /// errors are returned inside the EXEC array and do not stop later queued
    /// commands. A watched-key conflict returns a nil array.
    ///
    /// Notes: The transaction gate excludes concurrent keyspace mutations
    /// between watch validation and queued command execution. AOF records are
    /// appended per executed write only after the command mutates successfully.
    fn execute_transaction(&mut self, conn_id: usize) -> (CommandResponse, bool) {
        if conn_id >= self.transaction_states.len() || !self.transaction_states[conn_id].queueing {
            return (CommandResponse::Static(RESP_ERR_EXEC_WITHOUT_MULTI), false);
        }

        if self.transaction_states[conn_id].dirty {
            self.clear_transaction_state(conn_id);
            return (CommandResponse::Static(RESP_ERR_EXECABORT), false);
        }

        let queued = std::mem::take(&mut self.transaction_states[conn_id].queued);
        self.transaction_states[conn_id].queueing = false;
        self.transaction_states[conn_id].dirty = false;

        if self.aof_writer.is_some()
            && self.aof_fatal_state.is_failed()
            && self.queued_transaction_has_write(&queued)
        {
            self.clear_watches(conn_id);
            return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
        }

        let keyspace = Arc::clone(&self.keyspace);
        let _transaction_gate = keyspace.enter_transaction_gate();

        let watched_changed = {
            let tx = &self.transaction_states[conn_id];
            !tx.watched.is_empty()
                && keyspace.watched_keys_changed(tx.watch_epoch, tx.watched.as_slice())
        };
        if watched_changed {
            self.clear_watches(conn_id);
            return (CommandResponse::Static(RESP_NULL_ARRAY), false);
        }

        self.clear_watches(conn_id);

        let mut response = Vec::with_capacity(32 + queued.len() * 8);
        push_resp_array_len(&mut response, queued.len());

        for payload in queued {
            if let Err(error) = self.execute_queued_payload(payload.as_ref(), &mut response) {
                self.aof_fatal_state
                    .mark_failed(self.id, "append_transaction", &error);
                return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
            }
        }

        (CommandResponse::Owned(response.into_boxed_slice()), false)
    }

    fn execute_queued_payload(&mut self, payload: &[u8], response: &mut Vec<u8>) -> io::Result<()> {
        let tape = BorrowedRespTape::parse_pipeline(payload)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "queued RESP parse failed"))?;
        let frame = tape
            .iter()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty queued command"))?;

        let dispatch = self.command_router.dispatch(&frame);
        match dispatch {
            DispatchResult::Dispatch { name, .. } => {
                let clock = CommandClock::new(self.cached_nanos, self.cached_unix_nanos);
                match execute_command(&self.keyspace, name, &frame, clock) {
                    Some(executed) => {
                        let vortex_engine::commands::ExecutedCommand {
                            response: command_response,
                            aof_records,
                            aof_lsn,
                            aof_payload,
                        } = executed;
                        if let Some(records) = aof_records {
                            for record in records {
                                self.append_eviction_aof_record(record.lsn, record.key.as_bytes())?;
                            }
                        }
                        if aof_lsn != vortex_engine::commands::NO_AOF_LSN {
                            let payload = aof_payload.as_deref().unwrap_or(payload);
                            self.append_aof_payload(aof_lsn, payload)?;
                        }
                        append_cmd_result(response, command_response);
                    }
                    None => response.extend_from_slice(b"-ERR command not yet implemented\r\n"),
                }
            }
            DispatchResult::WrongArity { .. } => response.extend_from_slice(RESP_ERR_WRONG_ARGC),
            DispatchResult::UnknownCommand => response.extend_from_slice(RESP_ERR_UNKNOWN),
        }

        Ok(())
    }

    /// Dispatch a parsed tape frame through the engine.
    ///
    /// Uses O(1) perfect-hash lookup with SWAR uppercase normalization,
    /// then routes to `vortex_engine::commands::execute_command()` against
    /// the shared `ConcurrentKeyspace`.
    /// Returns `(CommandResponse, should_close)` — the bool signals QUIT.
    fn dispatch_command(
        &mut self,
        conn_id: usize,
        frame: &FrameRef<'_>,
    ) -> (CommandResponse, bool) {
        let Some((upper, len)) = Self::normalized_command_name(frame) else {
            return (CommandResponse::Static(RESP_ERR_UNKNOWN), false);
        };
        let command_name = &upper[..len];

        if conn_id < self.transaction_states.len() && self.transaction_states[conn_id].queueing {
            match command_name {
                b"MULTI" => {
                    self.mark_transaction_dirty(conn_id);
                    if !Self::has_exact_arity(frame, 1) {
                        return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                    }
                    return (CommandResponse::Static(RESP_ERR_NESTED_MULTI), false);
                }
                b"EXEC" => {
                    if !Self::has_exact_arity(frame, 1) {
                        self.mark_transaction_dirty(conn_id);
                        return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                    }
                    return self.execute_transaction(conn_id);
                }
                b"DISCARD" => {
                    if !Self::has_exact_arity(frame, 1) {
                        self.mark_transaction_dirty(conn_id);
                        return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                    }
                    return self.discard_transaction(conn_id);
                }
                b"WATCH" => {
                    self.mark_transaction_dirty(conn_id);
                    if !Self::has_min_arity(frame, 2) {
                        return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                    }
                    return (CommandResponse::Static(RESP_ERR_WATCH_INSIDE_MULTI), false);
                }
                _ => return (self.queue_transaction_command(conn_id, frame), false),
            }
        }

        match command_name {
            b"MULTI" => {
                if !Self::has_exact_arity(frame, 1) {
                    return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                }
                return self.begin_transaction(conn_id);
            }
            b"EXEC" => {
                if !Self::has_exact_arity(frame, 1) {
                    return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                }
                return (CommandResponse::Static(RESP_ERR_EXEC_WITHOUT_MULTI), false);
            }
            b"DISCARD" => {
                if !Self::has_exact_arity(frame, 1) {
                    return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                }
                return (
                    CommandResponse::Static(RESP_ERR_DISCARD_WITHOUT_MULTI),
                    false,
                );
            }
            b"WATCH" => {
                if !Self::has_min_arity(frame, 2) {
                    return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                }
                return (self.handle_watch(conn_id, frame), false);
            }
            b"UNWATCH" => {
                if !Self::has_exact_arity(frame, 1) {
                    return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                }
                self.clear_watches(conn_id);
                return (
                    CommandResponse::Static(vortex_engine::commands::RESP_OK),
                    false,
                );
            }
            b"BGREWRITEAOF" => return self.handle_bgrewriteaof(),
            b"CONFIG" => {
                if let Some(resp) = self.handle_config(frame) {
                    return resp;
                }
            }
            _ => {}
        }

        match CommandRouter::dispatch_normalized(frame, command_name) {
            DispatchResult::Dispatch { meta, name, .. } => {
                if self.aof_writer.is_some()
                    && self.aof_fatal_state.is_failed()
                    && meta.flags.contains(CommandFlags::WRITE)
                {
                    return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
                }

                let clock = CommandClock::new(self.cached_nanos, self.cached_unix_nanos);
                let executed = if self.command_requires_keyspace_gate(meta) {
                    let _command_gate = self.keyspace.enter_command_gate_slot(self.id);
                    execute_command(&self.keyspace, name, frame, clock)
                } else {
                    execute_command(&self.keyspace, name, frame, clock)
                };
                match executed {
                    Some(executed) => {
                        let vortex_engine::commands::ExecutedCommand {
                            response,
                            aof_records,
                            aof_lsn,
                            aof_payload,
                        } = executed;
                        if let Some(records) = aof_records {
                            for record in records {
                                if let Err(error) = self
                                    .append_eviction_aof_record(record.lsn, record.key.as_bytes())
                                {
                                    self.aof_fatal_state.mark_failed(
                                        self.id,
                                        "append_eviction_record",
                                        &error,
                                    );
                                    return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
                                }
                            }
                        }
                        if aof_lsn != vortex_engine::commands::NO_AOF_LSN {
                            if let Err(error) =
                                self.append_to_aof(aof_lsn, frame, aof_payload.as_deref())
                            {
                                self.aof_fatal_state.mark_failed(
                                    self.id,
                                    "append_with_lsn",
                                    &error,
                                );
                                return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
                            }
                        }
                        match response {
                            CmdResult::Static(buf) => {
                                let close = meta.name == "QUIT";
                                (CommandResponse::Static(buf), close)
                            }
                            CmdResult::Inline(inline) => (CommandResponse::Inline(inline), false),
                            CmdResult::Resp(f) => (CommandResponse::Frame(f), false),
                        }
                    }
                    None => {
                        // Engine doesn't handle this command — shouldn't happen
                        // since all PHF commands are wired, but handle gracefully.
                        (
                            CommandResponse::Static(b"-ERR command not yet implemented\r\n"),
                            false,
                        )
                    }
                }
            }
            DispatchResult::WrongArity { .. } => {
                (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false)
            }
            DispatchResult::UnknownCommand => (CommandResponse::Static(RESP_ERR_UNKNOWN), false),
        }
    }

    /// Append a mutation command to the AOF file with its global LSN.
    ///
    /// Serializes the frame's RESP encoding into the scratch buffer, then
    /// writes it with the LSN prefix to the AOF. This is the only AOF
    /// hot-path code.
    #[inline]
    fn append_eviction_aof_record(&mut self, lsn: u64, key: &[u8]) -> io::Result<()> {
        if let Some(ref mut writer) = self.aof_writer {
            self.aof_scratch.clear();
            push_resp_array_len(&mut self.aof_scratch, 2);
            push_resp_bulk_string(&mut self.aof_scratch, b"DEL");
            push_resp_bulk_string(&mut self.aof_scratch, key);
            writer.append_with_lsn(lsn, &self.aof_scratch)?;
            if self.aof_scratch.len() < 4096 {
                self.aof_scratch.resize(4096, 0);
            }
        }
        Ok(())
    }

    #[inline]
    fn append_aof_payload(&mut self, lsn: u64, payload: &[u8]) -> io::Result<()> {
        if let Some(ref mut writer) = self.aof_writer {
            writer.append_with_lsn(lsn, payload)?;
        }
        Ok(())
    }

    #[inline]
    fn append_to_aof(
        &mut self,
        lsn: u64,
        frame: &FrameRef<'_>,
        aof_payload: Option<&[u8]>,
    ) -> io::Result<()> {
        if let Some(ref mut writer) = self.aof_writer {
            let payload = if let Some(payload) = aof_payload {
                payload
            } else {
                let written = loop {
                    if let Some(written) = frame.write_resp_to(&mut self.aof_scratch) {
                        break written;
                    }

                    let next_len =
                        self.aof_scratch
                            .len()
                            .max(1)
                            .checked_mul(2)
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "AOF frame exceeded scratch buffer growth limit",
                                )
                            })?;
                    self.aof_scratch.resize(next_len, 0);
                };
                &self.aof_scratch[..written]
            };

            writer.append_with_lsn(lsn, payload)?;
        }

        Ok(())
    }

    fn handle_bgrewriteaof(&mut self) -> (CommandResponse, bool) {
        (
            CommandResponse::Static(RESP_ERR_BGREWRITEAOF_DISABLED),
            false,
        )
    }

    /// Handle CONFIG subcommands relevant to AOF.
    ///
    /// Returns `Some(response)` for handled subcommands, `None` to fall through
    /// to the engine for other CONFIG operations.
    fn handle_config(&mut self, frame: &FrameRef<'_>) -> Option<(CommandResponse, bool)> {
        // CONFIG GET/SET require at least 3 args: CONFIG <subcmd> <param>
        let argc = arg_count(frame);
        if argc < 3 {
            return None;
        }

        let subcmd = arg_bytes(frame, 1)?;
        let param = arg_bytes(frame, 2)?;

        // Normalize subcmd to uppercase for comparison.
        let subcmd_upper: Vec<u8> = subcmd.iter().map(|b| b.to_ascii_uppercase()).collect();

        match subcmd_upper.as_slice() {
            b"GET" => {
                let param_lower: Vec<u8> = param.iter().map(|b| b.to_ascii_lowercase()).collect();
                match param_lower.as_slice() {
                    b"appendonly" => Some(config_pair_response(
                        b"appendonly",
                        if self.aof_writer.is_some() {
                            b"yes".to_vec()
                        } else {
                            b"no".to_vec()
                        },
                    )),
                    b"appendfsync" => Some(config_pair_response(
                        b"appendfsync",
                        match &self.config.aof_config {
                            Some(cfg) => match cfg.fsync_policy {
                                vortex_persist::aof::AofFsyncPolicy::Always => b"always".to_vec(),
                                vortex_persist::aof::AofFsyncPolicy::Everysec => {
                                    b"everysec".to_vec()
                                }
                                vortex_persist::aof::AofFsyncPolicy::No => b"no".to_vec(),
                            },
                            None => b"everysec".to_vec(),
                        },
                    )),
                    b"maxmemory" => Some(config_pair_response(
                        b"maxmemory",
                        self.keyspace.max_memory().to_string().into_bytes(),
                    )),
                    b"maxmemory-policy" => Some(config_pair_response(
                        b"maxmemory-policy",
                        self.keyspace.eviction_policy().as_str().as_bytes().to_vec(),
                    )),
                    _ => None, // Fall through to engine.
                }
            }
            b"SET" => {
                if argc < 4 {
                    return Some((
                        CommandResponse::Static(
                            b"-ERR wrong number of arguments for CONFIG SET\r\n",
                        ),
                        false,
                    ));
                }
                let param_lower: Vec<u8> = param.iter().map(|b| b.to_ascii_lowercase()).collect();
                let value = arg_bytes(frame, 3)?;
                match param_lower.as_slice() {
                    b"appendonly" => {
                        let val_lower: Vec<u8> =
                            value.iter().map(|b| b.to_ascii_lowercase()).collect();
                        match val_lower.as_slice() {
                            b"yes" => {
                                if self.aof_writer.is_some() {
                                    return Some((CommandResponse::Static(b"+OK\r\n"), false));
                                }
                                let aof_cfg = match &self.config.aof_config {
                                    Some(cfg) => cfg.clone(),
                                    None => AofConfig {
                                        path: std::path::PathBuf::from("vortex.aof"),
                                        fsync_policy: vortex_persist::aof::AofFsyncPolicy::Everysec,
                                    },
                                };
                                let aof_path = if self.id == 0 {
                                    aof_cfg.path.clone()
                                } else {
                                    let stem = aof_cfg
                                        .path
                                        .file_stem()
                                        .unwrap_or_default()
                                        .to_string_lossy();
                                    let ext = aof_cfg
                                        .path
                                        .extension()
                                        .unwrap_or_default()
                                        .to_string_lossy();
                                    aof_cfg
                                        .path
                                        .with_file_name(format!("{stem}-shard{}.{ext}", self.id))
                                };
                                match AofFileWriter::open(
                                    &aof_path,
                                    self.id as u16,
                                    aof_cfg.fsync_policy,
                                ) {
                                    Ok(w) => {
                                        self.aof_writer = Some(w);
                                        self.keyspace.enable_aof_recording();
                                        self.config.aof_config = Some(aof_cfg);
                                        tracing::info!(
                                            reactor_id = self.id,
                                            "AOF enabled via CONFIG SET"
                                        );
                                        Some((CommandResponse::Static(b"+OK\r\n"), false))
                                    }
                                    Err(e) => Some((
                                        CommandResponse::Frame(RespFrame::Error(
                                            format!("ERR failed to enable AOF: {e}").into(),
                                        )),
                                        false,
                                    )),
                                }
                            }
                            b"no" => {
                                if let Some(ref mut w) = self.aof_writer {
                                    let _ = w.flush_and_sync();
                                    self.keyspace.disable_aof_recording();
                                }
                                self.aof_writer = None;
                                tracing::info!(reactor_id = self.id, "AOF disabled via CONFIG SET");
                                Some((CommandResponse::Static(b"+OK\r\n"), false))
                            }
                            _ => Some((
                                CommandResponse::Static(
                                    b"-ERR invalid argument for CONFIG SET appendonly\r\n",
                                ),
                                false,
                            )),
                        }
                    }
                    b"maxmemory" => {
                        let Ok(raw_value) = std::str::from_utf8(value) else {
                            return Some((
                                CommandResponse::Static(RESP_ERR_CONFIG_SET_MAXMEMORY),
                                false,
                            ));
                        };
                        let Ok(max_memory) = raw_value.parse::<usize>() else {
                            return Some((
                                CommandResponse::Static(RESP_ERR_CONFIG_SET_MAXMEMORY),
                                false,
                            ));
                        };
                        self.keyspace.set_max_memory(max_memory);
                        Some((CommandResponse::Static(b"+OK\r\n"), false))
                    }
                    b"maxmemory-policy" => {
                        let Some(policy) = EvictionPolicy::parse_bytes(value) else {
                            return Some((
                                CommandResponse::Static(RESP_ERR_CONFIG_SET_POLICY),
                                false,
                            ));
                        };
                        self.keyspace.set_eviction_policy(policy);
                        Some((CommandResponse::Static(b"+OK\r\n"), false))
                    }
                    _ => None, // Fall through to engine.
                }
            }
            _ => None, // RESETSTAT, REWRITE, etc. — fall through.
        }
    }

    // ── Write handler ──────────────────────────────────────────────

    fn handle_write(&mut self, conn_id: usize, op: OpType, cqe: &Completion) {
        self.mark_inflight_completed(conn_id, op);

        // Ignore late completions for connections being torn down.
        if let Some(c) = self.connections.get(conn_id) {
            if c.is_closing() {
                if let Some(conn) = self.connections.get_mut(conn_id) {
                    conn.write_buf_len = 0;
                }
                if matches!(op, OpType::Writev) {
                    self.writev_states[conn_id].clear();
                }
                self.maybe_finalize_close(conn_id);
                return;
            }
        }

        if cqe.result < 0 {
            tracing::debug!(conn_id, errno = -cqe.result, "write error");
            self.close_connection(conn_id);
            return;
        }

        let (fd, write_idx, total) = match self.connections.get(conn_id) {
            Some(c) => (c.fd, c.write_buf_offset as usize, c.write_buf_len as usize),
            None => return,
        };

        let bytes_written = cqe.result as usize;

        if bytes_written < total {
            let remaining = total - bytes_written;
            match op {
                OpType::Write => {
                    // Partial write — shift unwritten bytes to front and resubmit.
                    let write_ptr = self.buffer_pool.ptr(write_idx);
                    // SAFETY: Both source and dest are within the same mmap buffer.
                    unsafe {
                        std::ptr::copy(write_ptr.add(bytes_written), write_ptr, remaining);
                    }
                    if let Some(c) = self.connections.get_mut(conn_id) {
                        c.write_buf_len = remaining as u32;
                    }
                    let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
                    let token = encode_token(conn_id, cgen, OpType::Write);
                    if self
                        .submit_backend_op("write_fixed_partial", |backend| {
                            backend.submit_write_fixed(
                                fd,
                                write_ptr as *const u8,
                                remaining,
                                write_idx as u16,
                                token,
                            )
                        })
                        .is_ok()
                    {
                        self.mark_inflight_submitted(conn_id, OpType::Write);
                    } else {
                        self.close_connection(conn_id);
                    }
                }
                OpType::Writev => {
                    self.writev_states[conn_id].advance(bytes_written);
                    if let Some(c) = self.connections.get_mut(conn_id) {
                        c.write_buf_len = remaining as u32;
                    }
                    let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
                    let token = encode_token(conn_id, cgen, OpType::Writev);
                    let (iov_ptr, iov_count) = {
                        let remaining_iovecs = self.writev_states[conn_id].remaining_iovecs();
                        (remaining_iovecs.as_ptr(), remaining_iovecs.len())
                    };
                    if self
                        .submit_backend_op("writev_partial", |backend| {
                            backend.submit_writev(fd, iov_ptr, iov_count, token)
                        })
                        .is_err()
                    {
                        self.writev_states[conn_id].clear();
                        self.close_connection(conn_id);
                    } else {
                        self.mark_inflight_submitted(conn_id, OpType::Writev);
                    }
                }
                _ => unreachable!(),
            }
        } else if self.draining
            || self
                .connections
                .get(conn_id)
                .is_some_and(|c| (c.flags & ConnectionFlags::CLOSE_AFTER_WRITE) != 0)
        {
            // Drain mode: writes flushed — close connection, don't re-arm read.
            if matches!(op, OpType::Writev) {
                self.writev_states[conn_id].clear();
            }
            self.close_connection(conn_id);
        } else {
            // Write complete — clear write length and re-arm read.
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.write_buf_len = 0;
            }
            if matches!(op, OpType::Writev) {
                self.writev_states[conn_id].clear();
            }
            self.submit_read_for(conn_id, fd);
        }
    }

    // ── Close handler ──────────────────────────────────────────────

    fn handle_close(&mut self, conn_id: usize) {
        self.mark_inflight_completed(conn_id, OpType::Close);
        self.maybe_finalize_close(conn_id);
    }

    // ── Helpers ────────────────────────────────────────────────────

    fn submit_accept_rearm(&mut self) -> io::Result<()> {
        let token = encode_token(ACCEPT_CONN_ID, 0, OpType::Accept);
        let listener_fd = self.listener_fd;
        self.submit_backend_op("accept", |backend| {
            backend.submit_accept(listener_fd, token)
        })
    }

    fn submit_backend_op<F>(&mut self, op_name: &'static str, mut submit: F) -> io::Result<()>
    where
        F: FnMut(&mut dyn IoBackend) -> io::Result<()>,
    {
        match submit(self.backend.as_mut()) {
            Ok(()) => Ok(()),
            Err(error)
                if error.kind() == io::ErrorKind::Other && error.to_string() == "SQ full" =>
            {
                self.keyspace.record_reactor_submit_sq_full_retry(self.id);
                if let Err(flush_error) = self.backend.flush() {
                    self.keyspace.record_reactor_submit_failure(self.id);
                    tracing::warn!(op = op_name, error = %flush_error, "submit retry flush failed");
                    return Err(flush_error);
                }

                match submit(self.backend.as_mut()) {
                    Ok(()) => Ok(()),
                    Err(retry_error) => {
                        self.keyspace.record_reactor_submit_failure(self.id);
                        tracing::warn!(op = op_name, error = %retry_error, "submit retry failed");
                        Err(retry_error)
                    }
                }
            }
            Err(error) => {
                self.keyspace.record_reactor_submit_failure(self.id);
                tracing::warn!(op = op_name, error = %error, "submit failed");
                Err(error)
            }
        }
    }

    /// Submit a read SQE for the given connection using the fixed buffer pool.
    fn submit_read_for(&mut self, conn_id: usize, fd: RawFd) {
        let (read_idx, cursor) = match self.connections.get(conn_id) {
            Some(c) => (c.read_buf_offset as usize, c.read_buf_len as usize),
            None => return,
        };

        let buf_size = self.buffer_pool.buffer_size();
        let remaining = buf_size - cursor;

        if remaining == 0 {
            // Buffer full — apply TCP backpressure. Don't re-arm the read SQE
            // until the parser has consumed bytes. If we reach this point after
            // parsing (i.e., the command itself is larger than the buffer), the
            // connection is unsalvageable — close it.
            tracing::warn!(
                conn_id,
                buf_size,
                "read buffer full, closing connection (backpressure)"
            );
            self.close_connection(conn_id);
            return;
        }

        // SAFETY: read_idx is a valid pool index leased for this connection.
        // cursor bytes have been written; we read into the remainder.
        let buf_ptr = unsafe { self.buffer_pool.ptr(read_idx).add(cursor) };
        let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
        let token = encode_token(conn_id, cgen, OpType::Read);
        if self
            .submit_backend_op("read_fixed", |backend| {
                backend.submit_read_fixed(fd, buf_ptr, remaining, read_idx as u16, token)
            })
            .is_ok()
        {
            self.mark_inflight_submitted(conn_id, OpType::Read);
        } else {
            self.close_connection(conn_id);
        }
    }

    /// Initiate a connection close.
    fn close_connection(&mut self, conn_id: usize) {
        let (fd, timer_slot) = match self.connections.get(conn_id) {
            Some(c) if !c.is_closing() => (c.fd, c.timer_slot),
            _ => return, // Already closing or not found — prevent double-close.
        };

        // Cancel pending idle timer.
        self.timer_wheel.cancel(timer_slot);

        // Transition to Closing.
        if let Some(c) = self.connections.get_mut(conn_id) {
            let _ = c.transition(ConnectionState::Closing);
        }

        let inflight = self.inflight_ops.get(conn_id).copied().unwrap_or_default();

        let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
        if inflight.read {
            let _ = self
                .backend
                .submit_cancel(encode_token(conn_id, cgen, OpType::Read));
        }
        if inflight.write {
            let _ = self
                .backend
                .submit_cancel(encode_token(conn_id, cgen, OpType::Write));
        }
        if inflight.writev {
            let _ = self
                .backend
                .submit_cancel(encode_token(conn_id, cgen, OpType::Writev));
        }

        let token = encode_token(conn_id, cgen, OpType::Close);
        if self.backend.submit_close(fd, token).is_ok() {
            self.mark_inflight_submitted(conn_id, OpType::Close);
        } else {
            tracing::warn!(conn_id, "submit_close failed, falling back to direct close");
            // SAFETY: fd is owned by this connection and must be closed exactly once.
            unsafe {
                libc::close(fd);
            }
            self.maybe_finalize_close(conn_id);
        }
    }

    #[inline]
    fn mark_inflight_submitted(&mut self, conn_id: usize, op: OpType) {
        if let Some(inflight) = self.inflight_ops.get_mut(conn_id) {
            inflight.mark_submitted(op);
        }
    }

    #[inline]
    fn mark_inflight_completed(&mut self, conn_id: usize, op: OpType) {
        if let Some(inflight) = self.inflight_ops.get_mut(conn_id) {
            inflight.mark_completed(op);
        }
    }

    fn maybe_finalize_close(&mut self, conn_id: usize) {
        let Some(connection) = self.connections.get(conn_id) else {
            return;
        };
        if !connection.is_closing() {
            return;
        }
        if self
            .inflight_ops
            .get(conn_id)
            .is_some_and(InflightOps::has_any)
        {
            return;
        }

        let read_idx = connection.read_buf_offset as usize;
        let write_idx = connection.write_buf_offset as usize;
        self.buffer_pool.release_index(read_idx);
        if write_idx != read_idx {
            self.buffer_pool.release_index(write_idx);
        }
        if conn_id < self.writev_states.len() {
            self.writev_states[conn_id].clear();
        }
        if conn_id < self.inflight_ops.len() {
            self.inflight_ops[conn_id] = InflightOps::default();
        }
        self.clear_transaction_state(conn_id);

        self.connections.remove(conn_id);
        tracing::debug!(reactor_id = self.id, conn_id, "connection closed");
    }

    // ── Timer management ───────────────────────────────────────────

    /// Advance the timer wheel to `self.now_secs` and close or reschedule
    /// expired connections.
    fn tick_timers(&mut self) {
        while self.timer_wheel.current_tick() <= self.now_secs {
            self.expired_buf.clear();
            self.timer_wheel.tick(&mut self.expired_buf);

            // Process expired entries — temporarily take the buffer so we can
            // call &mut self methods.
            let expired = std::mem::take(&mut self.expired_buf);
            for exp in &expired {
                self.handle_expired_timer(exp.conn_id, exp.generation);
            }
            self.expired_buf = expired;
        }
    }

    /// Handle a single expired timer entry.
    fn handle_expired_timer(&mut self, conn_id: usize, timer_gen: u32) {
        // Validate generation — skip stale entries where the slab slot was
        // reused by a different connection.
        let current_gen = self.generations.get(conn_id).copied().unwrap_or(0);
        if current_gen != timer_gen {
            return;
        }

        let (last_active, is_closing) = match self.connections.get(conn_id) {
            Some(c) => (c.last_active, c.is_closing()),
            None => return,
        };

        if is_closing {
            return;
        }

        let idle = self.now_secs.saturating_sub(last_active);
        if idle >= self.connection_timeout {
            tracing::debug!(conn_id, idle, "closing idle connection");
            self.close_connection(conn_id);
        } else {
            // Connection was active since the timer was scheduled — reschedule
            // from last_active rather than now to avoid timer drift.
            let new_deadline = last_active + self.connection_timeout;
            let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
            let entry = self.timer_wheel.schedule(conn_id, cgen, new_deadline);
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.timer_slot = entry;
            }
        }
    }

    /// Enter drain mode: stop accepting new connections and gracefully close
    /// existing ones (flush pending writes first).
    fn enter_drain_mode(&mut self) {
        self.draining = true;
        tracing::info!(reactor_id = self.id, "entering drain mode");

        // Graceful drain: flush connections with pending writes, close the rest.
        let conn_ids: Vec<usize> = self.connections.ids().collect();
        for conn_id in conn_ids {
            let (fd, write_len, write_idx, is_closing) = match self.connections.get(conn_id) {
                Some(c) => (
                    c.fd,
                    c.write_buf_len as usize,
                    c.write_buf_offset as usize,
                    c.is_closing(),
                ),
                None => continue,
            };

            if is_closing {
                continue;
            }

            if write_len > 0 {
                let _ = (fd, write_idx);
                if let Some(c) = self.connections.get_mut(conn_id) {
                    c.flags |= ConnectionFlags::CLOSE_AFTER_WRITE;
                }
            } else {
                // No pending writes — close immediately.
                self.close_connection(conn_id);
            }
        }
    }

    /// Returns the number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    // ── Graceful I/O drain ─────────────────────────────────────────

    /// Cancel all in-flight I/O operations and drain the completion queue.
    ///
    /// Must be called before the `BufferPool` is dropped to prevent the
    /// kernel from DMA-ing into unmapped memory (`use-after-unmap`).
    fn drain_inflight_io(&mut self) {
        // Submit AsyncCancel for every active connection's possible in-flight ops.
        let conn_ids: Vec<usize> = self.connections.ids().collect();
        for &conn_id in &conn_ids {
            let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
            // Cancel any pending read.
            let read_token = encode_token(conn_id, cgen, OpType::Read);
            let _ = self.backend.submit_cancel(read_token);
            // Cancel any pending write.
            let write_token = encode_token(conn_id, cgen, OpType::Write);
            let _ = self.backend.submit_cancel(write_token);
            let writev_token = encode_token(conn_id, cgen, OpType::Writev);
            let _ = self.backend.submit_cancel(writev_token);
        }

        // Also cancel the accept token.
        let accept_token = encode_token(ACCEPT_CONN_ID, 0, OpType::Accept);
        let _ = self.backend.submit_cancel(accept_token);

        // Flush cancellations and drain remaining completions.
        let _ = self.backend.flush();
        self.cqe_buf.clear();
        let _ = self.backend.completions(&mut self.cqe_buf);
        self.cqe_buf.clear();

        // Release all connection buffers back to the pool.
        for &conn_id in &conn_ids {
            if let Some(c) = self.connections.get(conn_id) {
                self.buffer_pool.release_index(c.read_buf_offset as usize);
                if c.write_buf_offset != c.read_buf_offset {
                    self.buffer_pool.release_index(c.write_buf_offset as usize);
                }
            }
            if conn_id < self.writev_states.len() {
                self.writev_states[conn_id].clear();
            }
        }
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        if self.aof_writer.is_some() {
            self.keyspace.disable_aof_recording();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shutdown::ShutdownCoordinator;
    use std::collections::VecDeque;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use vortex_persist::aof::{AofFsyncPolicy, reader::AofReader, writer::AofFileWriter};
    use vortex_proto::RespTape;

    static RESP_PONG: &[u8] = b"+PONG\r\n";

    fn test_reactor() -> Reactor {
        let config = ReactorConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };
        Reactor::new(0, config, Arc::new(ShutdownCoordinator::new(1))).unwrap()
    }

    #[derive(Default)]
    struct MockBackendState {
        cancels: Vec<u64>,
        closes: Vec<u64>,
        completions: VecDeque<Completion>,
        flushes: usize,
        fail_next_submit_sq_full: bool,
        fail_retry_submit_sq_full: bool,
    }

    struct MockBackend {
        state: Arc<Mutex<MockBackendState>>,
    }

    impl MockBackend {
        fn maybe_fail_submit(&self) -> std::io::Result<()> {
            let mut state = self.state.lock().unwrap();
            if state.fail_next_submit_sq_full {
                state.fail_next_submit_sq_full = false;
                return Err(io::Error::other("SQ full"));
            }
            if state.fail_retry_submit_sq_full {
                state.fail_retry_submit_sq_full = false;
                return Err(io::Error::other("SQ full"));
            }
            Ok(())
        }
    }

    impl IoBackend for MockBackend {
        fn submit_accept(&mut self, _listener_fd: RawFd, _token: u64) -> std::io::Result<()> {
            self.maybe_fail_submit()?;
            Ok(())
        }

        fn submit_read(
            &mut self,
            _fd: RawFd,
            _buf_ptr: *mut u8,
            _buf_len: usize,
            _token: u64,
        ) -> std::io::Result<()> {
            self.maybe_fail_submit()?;
            Ok(())
        }

        fn submit_write(
            &mut self,
            _fd: RawFd,
            _buf_ptr: *const u8,
            _buf_len: usize,
            _token: u64,
        ) -> std::io::Result<()> {
            self.maybe_fail_submit()?;
            Ok(())
        }

        fn submit_writev(
            &mut self,
            _fd: RawFd,
            _iovecs: *const libc::iovec,
            _iov_count: usize,
            _token: u64,
        ) -> std::io::Result<()> {
            self.maybe_fail_submit()?;
            Ok(())
        }

        fn submit_cancel(&mut self, token: u64) -> std::io::Result<()> {
            self.state.lock().unwrap().cancels.push(token);
            Ok(())
        }

        fn submit_close(&mut self, _fd: RawFd, token: u64) -> std::io::Result<()> {
            self.state.lock().unwrap().closes.push(token);
            Ok(())
        }

        fn flush(&mut self) -> std::io::Result<usize> {
            self.state.lock().unwrap().flushes += 1;
            Ok(0)
        }

        fn completions(&mut self, out: &mut Vec<Completion>) -> std::io::Result<usize> {
            let mut state = self.state.lock().unwrap();
            let start = out.len();
            while let Some(cqe) = state.completions.pop_front() {
                out.push(cqe);
            }
            Ok(out.len() - start)
        }

        fn drain_cq(&mut self, out: &mut Vec<Completion>) -> std::io::Result<usize> {
            self.completions(out)
        }
    }

    fn test_reactor_with_backend(state: Arc<Mutex<MockBackendState>>) -> Reactor {
        let config = ReactorConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };
        Reactor::with_keyspace_and_backend(
            0,
            config,
            Arc::new(ShutdownCoordinator::new(1)),
            Arc::new(ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT)),
            Box::new(MockBackend { state }),
            Arc::new(AofFatalState::default()),
            1,
        )
        .unwrap()
    }

    fn temp_aof_path(suffix: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut path = std::env::temp_dir();
        path.push(format!(
            "vortex-reactor-test-{}-{}-{suffix}.aof",
            std::process::id(),
            COUNTER.fetch_add(1, AtomicOrdering::Relaxed),
        ));
        path
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
    }

    fn handle_config_wire(reactor: &mut Reactor, wire: &[u8]) -> (CommandResponse, bool) {
        let tape = RespTape::parse_pipeline(wire).expect("valid RESP");
        let frame = tape.iter().next().expect("at least one frame");
        reactor.handle_config(&frame).expect("handled config")
    }

    fn dispatch_reactor_wire_on(
        reactor: &mut Reactor,
        conn_id: usize,
        wire: &[u8],
    ) -> (CommandResponse, bool) {
        let tape = RespTape::parse_pipeline(wire).expect("valid RESP");
        let frame = tape.iter().next().expect("at least one frame");
        reactor.dispatch_command(conn_id, &frame)
    }

    fn dispatch_reactor_wire(reactor: &mut Reactor, wire: &[u8]) -> (CommandResponse, bool) {
        dispatch_reactor_wire_on(reactor, 0, wire)
    }

    fn response_bytes(resp: CommandResponse) -> Vec<u8> {
        match resp {
            CommandResponse::Static(bytes) => bytes.to_vec(),
            CommandResponse::Inline(inline) => inline.as_bytes().to_vec(),
            CommandResponse::Owned(bytes) => bytes.into_vec(),
            CommandResponse::Frame(frame) => {
                let mut out = Vec::new();
                append_resp_frame(&mut out, &frame);
                out
            }
        }
    }

    fn expect_config_pair(resp: CommandResponse, expected_name: &[u8], expected_value: &[u8]) {
        match resp {
            CommandResponse::Frame(RespFrame::Array(Some(items))) => {
                assert_eq!(items.len(), 2);
                assert_eq!(bulk_bytes(&items[0]), expected_name);
                assert_eq!(bulk_bytes(&items[1]), expected_value);
            }
            other => panic!("expected config pair response, got {other:?}"),
        }
    }

    fn bulk_bytes(frame: &RespFrame) -> &[u8] {
        match frame {
            RespFrame::BulkString(Some(bytes)) => bytes.as_ref(),
            other => panic!("expected bulk string, got {other:?}"),
        }
    }

    /// Helper: parse a single RESP wire command, route through engine dispatch.
    fn dispatch_wire(wire: &[u8]) -> (CommandResponse, bool) {
        let tape = RespTape::parse_pipeline(wire).expect("valid RESP");
        let frame = tape.iter().next().expect("at least one frame");
        let mut router = CommandRouter::new();
        let keyspace = ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT);
        let now = Timestamp::now().as_nanos();
        match router.dispatch(&frame) {
            DispatchResult::Dispatch { meta, name, .. } => {
                match execute_command(&keyspace, name, &frame, now) {
                    Some(executed) => match executed.response {
                        CmdResult::Static(buf) => {
                            let close = meta.name == "QUIT";
                            (CommandResponse::Static(buf), close)
                        }
                        CmdResult::Inline(inline) => (CommandResponse::Inline(inline), false),
                        CmdResult::Resp(f) => (CommandResponse::Frame(f), false),
                    },
                    None => (
                        CommandResponse::Static(b"-ERR command not yet implemented\r\n"),
                        false,
                    ),
                }
            }
            DispatchResult::WrongArity { .. } => {
                (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false)
            }
            DispatchResult::UnknownCommand => (CommandResponse::Static(RESP_ERR_UNKNOWN), false),
        }
    }

    #[test]
    fn dispatch_ping() {
        let (resp, close) = dispatch_wire(b"*1\r\n$4\r\nPING\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_PONG));
        assert!(!close);
    }

    #[test]
    fn dispatch_unknown() {
        // FOOBAR is truly unknown
        let (resp, _) = dispatch_wire(b"*1\r\n$6\r\nFOOBAR\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_ERR_UNKNOWN));
    }

    #[test]
    fn dispatch_set_returns_ok() {
        // SET key value → now goes through the engine, returns +OK
        let (resp, _) = dispatch_wire(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
    }

    #[test]
    fn command_gate_is_skipped_for_non_keyspace_commands() {
        let reactor = test_reactor();
        let get = vortex_proto::command::lookup_command("GET").expect("GET metadata");
        let set = vortex_proto::command::lookup_command("SET").expect("SET metadata");
        let ping = vortex_proto::command::lookup_command("PING").expect("PING metadata");
        let dbsize = vortex_proto::command::lookup_command("DBSIZE").expect("DBSIZE metadata");

        assert!(reactor.command_requires_keyspace_gate(get));
        assert!(reactor.command_requires_keyspace_gate(dbsize));
        assert!(!reactor.command_requires_keyspace_gate(ping));
        assert!(reactor.command_requires_keyspace_gate(set));
    }

    #[test]
    fn read_commands_gate_even_without_expiry() {
        let reactor = test_reactor();
        let get = vortex_proto::command::lookup_command("GET").expect("GET metadata");

        assert!(reactor.command_requires_keyspace_gate(get));
    }

    #[test]
    fn multi_exec_queues_and_returns_array() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*2\r\n+OK\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn watch_aborts_when_other_connection_modifies_key() {
        let mut reactor = test_reactor();

        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            1,
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$5\r\nother\r\n",
        );
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*-1\r\n");

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        assert_eq!(response_bytes(resp), b"$-1\r\n");
    }

    #[test]
    fn watch_missing_key_aborts_after_set_delete_churn() {
        let mut reactor = test_reactor();

        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$7\r\nmissing\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            1,
            b"*3\r\n$3\r\nSET\r\n$7\r\nmissing\r\n$5\r\nvalue\r\n",
        );
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 1, b"*2\r\n$3\r\nDEL\r\n$7\r\nmissing\r\n");
        assert_eq!(response_bytes(resp), b":1\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*-1\r\n");
    }

    #[test]
    fn watch_aborts_after_flushall() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        );
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 1, b"*1\r\n$8\r\nFLUSHALL\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*-1\r\n");
    }

    #[test]
    fn watch_aborts_after_pexpire_only_mutation() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*3\r\n$3\r\nSET\r\n$9\r\nwatch:ttl\r\n$2\r\nv1\r\n",
        );
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$9\r\nwatch:ttl\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            1,
            b"*3\r\n$7\r\nPEXPIRE\r\n$9\r\nwatch:ttl\r\n$5\r\n60000\r\n",
        );
        assert_eq!(response_bytes(resp), b":1\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*-1\r\n");
    }

    #[test]
    fn watch_aborts_after_persist_only_mutation() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*5\r\n$3\r\nSET\r\n$13\r\nwatch:persist\r\n$2\r\nv1\r\n$2\r\nEX\r\n$2\r\n60\r\n",
        );
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*2\r\n$5\r\nWATCH\r\n$13\r\nwatch:persist\r\n",
        );
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            1,
            b"*2\r\n$7\r\nPERSIST\r\n$13\r\nwatch:persist\r\n",
        );
        assert_eq!(response_bytes(resp), b":1\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*-1\r\n");
    }

    #[test]
    fn watch_aborts_when_read_lazily_expires_watched_key() {
        let mut reactor = test_reactor();
        reactor.cached_nanos = 0;
        reactor.cached_unix_nanos = 0;

        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            1,
            b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n",
        );
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        reactor.cached_nanos = 200_000_000;
        reactor.cached_unix_nanos = 200_000_000;
        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 1, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"$-1\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$6\r\nqueued\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*-1\r\n");

        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
        assert_eq!(response_bytes(resp), b"$-1\r\n");
    }

    #[test]
    fn exec_appends_queued_writes_to_aof() {
        let path = temp_aof_path("transaction-aof");
        let mut reactor = test_reactor();
        reactor.aof_writer = Some(AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap());
        reactor.keyspace.enable_aof_recording();

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$3\r\nSET\r\n$5\r\ntxkey\r\n$5\r\nvalue\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*1\r\n+OK\r\n");
        reactor
            .aof_writer
            .as_mut()
            .expect("AOF writer")
            .flush_buffer()
            .unwrap();

        let replayed = ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT);
        let stats = AofReader::new(&path)
            .replay_into_keyspace(&replayed)
            .unwrap();
        assert_eq!(stats.commands_replayed, 1);

        let tape =
            RespTape::parse_pipeline(b"*2\r\n$3\r\nGET\r\n$5\r\ntxkey\r\n").expect("valid RESP");
        let frame = tape.iter().next().unwrap();
        let result = execute_command(
            &replayed,
            b"GET",
            &frame,
            CommandClock::new(Timestamp::now().as_nanos(), current_unix_time_nanos()),
        )
        .unwrap();
        assert!(
            matches!(result.response, CmdResult::Inline(inline) if inline.payload() == b"value")
        );

        cleanup(&path);
    }

    #[test]
    fn discard_clears_transaction_and_watches() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$7\r\nDISCARD\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"$-1\r\n");
    }

    #[test]
    fn multi_wrong_arity_does_not_enter_transaction() {
        let mut reactor = test_reactor();

        let (resp, _) =
            dispatch_reactor_wire(&mut reactor, b"*2\r\n$5\r\nMULTI\r\n$5\r\nextra\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);

        let (resp, _) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        );
        assert_eq!(response_bytes(resp), b"+OK\r\n");

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_EXEC_WITHOUT_MULTI);
    }

    #[test]
    fn queue_time_wrong_arity_aborts_exec_without_mutating() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
        let (resp, _) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$4\r\nINCR\r\n$3\r\nfoo\r\n$5\r\nextra\r\n",
        );
        assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_EXECABORT);

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"$-1\r\n");
    }

    #[test]
    fn nested_multi_marks_transaction_dirty() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_NESTED_MULTI);
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_EXECABORT);
    }

    #[test]
    fn watch_inside_multi_marks_transaction_dirty() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_WATCH_INSIDE_MULTI);
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_EXECABORT);
    }

    #[test]
    fn exec_wrong_arity_inside_multi_marks_transaction_dirty() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$4\r\nEXEC\r\n$5\r\nextra\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_EXECABORT);
    }

    #[test]
    fn exec_empty_transaction_returns_empty_array() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*0\r\n");
    }

    #[test]
    fn exec_runtime_error_does_not_abort_later_commands() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$3\r\nSET\r\n$3\r\nnum\r\n$5\r\nnoint\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n");
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
        let (resp, _) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$3\r\nSET\r\n$5\r\nafter\r\n$2\r\nok\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(
            response_bytes(resp),
            b"*3\r\n+OK\r\n-ERR value is not an integer or out of range\r\n+OK\r\n"
        );

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$5\r\nafter\r\n");
        assert_eq!(response_bytes(resp), b"$2\r\nok\r\n");
    }

    #[test]
    fn watch_aborts_on_same_connection_pre_multi_write() {
        let mut reactor = test_reactor();

        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$4\r\nself\r\n",
        );
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");

        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*-1\r\n");
    }

    #[test]
    fn unwatch_clears_watches_before_conflicting_write() {
        let mut reactor = test_reactor();

        let (resp, _) =
            dispatch_reactor_wire_on(&mut reactor, 0, b"*2\r\n$5\r\nWATCH\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$7\r\nUNWATCH\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            1,
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$5\r\nother\r\n",
        );
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$5\r\nMULTI\r\n");
        assert_eq!(response_bytes(resp), b"+OK\r\n");
        let (resp, _) = dispatch_reactor_wire_on(
            &mut reactor,
            0,
            b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$2\r\nok\r\n",
        );
        assert_eq!(response_bytes(resp), b"+QUEUED\r\n");
        let (resp, _) = dispatch_reactor_wire_on(&mut reactor, 0, b"*1\r\n$4\r\nEXEC\r\n");
        assert_eq!(response_bytes(resp), b"*1\r\n+OK\r\n");
    }

    #[test]
    fn watch_and_unwatch_wrong_arity_are_rejected() {
        let mut reactor = test_reactor();

        let (resp, _) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$5\r\nWATCH\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);

        let (resp, _) =
            dispatch_reactor_wire(&mut reactor, b"*2\r\n$7\r\nUNWATCH\r\n$3\r\nfoo\r\n");
        assert_eq!(response_bytes(resp), RESP_ERR_WRONG_ARGC);
    }

    #[test]
    fn fatal_aof_state_rejects_writes_before_mutation() {
        let path = temp_aof_path("fatal-write-reject");
        let mut reactor = test_reactor();
        reactor.aof_writer = Some(AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap());
        reactor.keyspace.enable_aof_recording();
        reactor.aof_fatal_state.mark_failed(
            reactor.id,
            "test",
            &std::io::Error::other("forced AOF failure"),
        );

        let (resp, close) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        );
        assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_ERR_AOF_MISCONF));
        assert!(!close);

        let (read_resp, _) =
            dispatch_reactor_wire(&mut reactor, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert!(matches!(read_resp, CommandResponse::Static(b) if b == b"$-1\r\n"));

        cleanup(&path);
    }

    #[test]
    fn dispatch_ping_lowercase() {
        let (resp, _) = dispatch_wire(b"*1\r\n$4\r\nping\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_PONG));
    }

    #[test]
    fn dispatch_attribute_wrapped_ping() {
        // Attribute-wrapped frames: |1\r\n+meta\r\n+value\r\n*1\r\n$4\r\nPING\r\n
        // The CommandRouter correctly extracts PING from inside the attribute
        // envelope. The engine receives the outer (attribute) FrameRef, whose
        // element_count() differs from a plain array — cmd_ping may treat the
        // attribute child as a message arg and return a bulk string instead of
        // the static +PONG. Verify routing succeeds (no error / no panic).
        let (resp, close) = dispatch_wire(b"|1\r\n+meta\r\n+value\r\n*1\r\n$4\r\nPING\r\n");
        assert!(!close);
        // Acceptable outcomes: static PONG or a bulk-string echo of the attribute child.
        match resp {
            CommandResponse::Static(b) => assert_eq!(b, RESP_PONG),
            CommandResponse::Inline(_) | CommandResponse::Frame(_) | CommandResponse::Owned(_) => {
                /* bulk string from attribute child — acceptable */
            }
        }
    }

    #[test]
    fn dispatch_wrong_arity() {
        // GET with no key (only 1 element, arity=2)
        let (resp, _) = dispatch_wire(b"*1\r\n$3\r\nGET\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b.starts_with(b"-ERR wrong number")));
    }

    #[test]
    fn dispatch_quit_signals_close() {
        let (resp, close) = dispatch_wire(b"*1\r\n$4\r\nQUIT\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
        assert!(close);
    }

    #[test]
    fn dispatch_uses_monotonic_clock_for_relative_expiry() {
        let mut reactor = test_reactor();
        let monotonic_now = 5 * 1_000_000_000;
        let unix_now = 4_102_444_800 * 1_000_000_000;
        reactor.cached_nanos = monotonic_now;
        reactor.cached_unix_nanos = unix_now;

        let (resp, close) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$3\r\nSET\r\n$7\r\nsession\r\n$5\r\ntoken\r\n",
        );
        assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
        assert!(!close);

        let (resp, close) = dispatch_reactor_wire(
            &mut reactor,
            b"*3\r\n$7\r\nPEXPIRE\r\n$7\r\nsession\r\n$1\r\n1\r\n",
        );
        assert!(matches!(resp, CommandResponse::Static(b) if b == b":1\r\n"));
        assert!(!close);

        let tape =
            RespTape::parse_pipeline(b"*2\r\n$3\r\nGET\r\n$7\r\nsession\r\n").expect("valid RESP");
        let frame = tape.iter().next().expect("at least one frame");
        let expired = execute_command(
            &reactor.keyspace,
            b"GET",
            &frame,
            CommandClock::new(monotonic_now + 2_000_000, unix_now + 2_000_000),
        )
        .expect("GET is implemented");
        assert!(matches!(expired.response, CmdResult::Static(b) if b == b"$-1\r\n"));
    }

    #[test]
    fn config_get_reads_runtime_eviction_state() {
        let mut reactor = test_reactor();
        reactor.keyspace.set_max_memory(4096);
        reactor
            .keyspace
            .set_eviction_policy(EvictionPolicy::VolatileTtl);

        let (resp, close) = handle_config_wire(
            &mut reactor,
            b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\nmaxmemory\r\n",
        );
        assert!(!close);
        expect_config_pair(resp, b"maxmemory", b"4096");

        let (resp, close) = handle_config_wire(
            &mut reactor,
            b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$16\r\nmaxmemory-policy\r\n",
        );
        assert!(!close);
        expect_config_pair(resp, b"maxmemory-policy", b"volatile-ttl");
    }

    #[test]
    fn config_set_updates_runtime_eviction_state() {
        let mut reactor = test_reactor();

        let (resp, close) = handle_config_wire(
            &mut reactor,
            b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$9\r\nmaxmemory\r\n$4\r\n8192\r\n",
        );
        assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
        assert!(!close);
        assert_eq!(reactor.keyspace.max_memory(), 8192);

        let (resp, close) = handle_config_wire(
            &mut reactor,
            b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$16\r\nmaxmemory-policy\r\n$15\r\nvolatile-random\r\n",
        );
        assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
        assert!(!close);
        assert_eq!(
            reactor.keyspace.eviction_policy(),
            EvictionPolicy::VolatileRandom
        );
    }

    #[test]
    fn config_set_accepts_lfu_policy() {
        let mut reactor = test_reactor();
        reactor
            .keyspace
            .set_eviction_policy(EvictionPolicy::NoEviction);

        let (resp, close) = handle_config_wire(
            &mut reactor,
            b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$16\r\nmaxmemory-policy\r\n$11\r\nallkeys-lfu\r\n",
        );
        assert!(matches!(resp, CommandResponse::Static(b) if b == b"+OK\r\n"));
        assert!(!close);
        assert_eq!(
            reactor.keyspace.eviction_policy(),
            EvictionPolicy::AllKeysLfu
        );
    }

    #[test]
    fn reactor_rejects_buffer_count_below_connection_minimum() {
        let config = ReactorConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 4,
            buffer_count: 3,
            ..Default::default()
        };

        let error = match Reactor::new(0, config, Arc::new(ShutdownCoordinator::new(1))) {
            Ok(_) => panic!("reactor creation should fail when buffer_count < max_connections"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("buffer_count"));
    }

    #[test]
    fn bgrewriteaof_is_disabled_for_alpha() {
        let mut reactor = test_reactor();

        let (resp, close) = dispatch_reactor_wire(&mut reactor, b"*1\r\n$12\r\nBGREWRITEAOF\r\n");

        assert!(!close);
        assert_eq!(response_bytes(resp), RESP_ERR_BGREWRITEAOF_DISABLED);
    }

    #[test]
    fn pending_writev_advances_across_segments() {
        let mut pending = PendingWritev::new();
        pending.push_static(b"abc");
        pending.push_static(b"defg");
        pending.finalize();

        pending.advance(4);

        assert_eq!(pending.remaining_len(), 3);
        let remaining = pending.remaining_iovecs();
        assert_eq!(remaining.len(), 1);
        // SAFETY: remaining iovec points to static storage used in the test.
        let bytes = unsafe {
            std::slice::from_raw_parts(remaining[0].iov_base.cast::<u8>(), remaining[0].iov_len)
        };
        assert_eq!(bytes, b"efg");
    }

    #[test]
    fn close_waits_for_inflight_io_before_releasing_buffers() {
        let state = Arc::new(Mutex::new(MockBackendState::default()));
        let mut reactor = test_reactor_with_backend(state.clone());

        let read_idx = reactor.buffer_pool.lease_index().unwrap();
        let write_idx = reactor.buffer_pool.lease_index().unwrap();
        let mut meta = ConnectionMeta::new(123, 0);
        meta.read_buf_offset = read_idx as u32;
        meta.write_buf_offset = write_idx as u32;
        let conn_id = reactor.connections.insert(meta);
        reactor.generations[conn_id] = 7;
        reactor.writev_states[conn_id].push_static(b"pending");
        reactor.writev_states[conn_id].finalize();
        reactor.inflight_ops[conn_id].read = true;
        reactor.inflight_ops[conn_id].writev = true;

        reactor.close_connection(conn_id);

        let cgen = reactor.generations[conn_id];
        let read_token = encode_token(conn_id, cgen, OpType::Read);
        let writev_token = encode_token(conn_id, cgen, OpType::Writev);
        let close_token = encode_token(conn_id, cgen, OpType::Close);

        {
            let state = state.lock().unwrap();
            assert_eq!(state.cancels, vec![read_token, writev_token]);
            assert_eq!(state.closes, vec![close_token]);
        }
        assert!(reactor.connections.get(conn_id).unwrap().is_closing());
        assert_eq!(reactor.buffer_pool.outstanding(), 2);

        reactor.handle_close(conn_id);
        assert!(reactor.connections.get(conn_id).is_some());
        assert_eq!(reactor.buffer_pool.outstanding(), 2);

        reactor.handle_read(
            conn_id,
            &Completion {
                token: read_token,
                result: -libc::ECANCELED,
                flags: 0,
            },
        );
        assert!(reactor.connections.get(conn_id).is_some());
        assert_eq!(reactor.buffer_pool.outstanding(), 2);

        reactor.handle_write(
            conn_id,
            OpType::Writev,
            &Completion {
                token: writev_token,
                result: -libc::ECANCELED,
                flags: 0,
            },
        );

        assert!(reactor.connections.get(conn_id).is_none());
        assert_eq!(reactor.buffer_pool.outstanding(), 0);
        assert_eq!(reactor.writev_states[conn_id].remaining_len(), 0);
    }

    #[test]
    fn submit_read_sq_full_flushes_and_retries_once() {
        let state = Arc::new(Mutex::new(MockBackendState {
            fail_next_submit_sq_full: true,
            ..Default::default()
        }));
        let mut reactor = test_reactor_with_backend(state.clone());

        let read_idx = reactor.buffer_pool.lease_index().unwrap();
        let write_idx = reactor.buffer_pool.lease_index().unwrap();
        let mut meta = ConnectionMeta::new(123, 0);
        meta.read_buf_offset = read_idx as u32;
        meta.write_buf_offset = write_idx as u32;
        let conn_id = reactor.connections.insert(meta);

        reactor.submit_read_for(conn_id, 123);

        assert!(reactor.inflight_ops[conn_id].read);
        assert_eq!(state.lock().unwrap().flushes, 1);

        let runtime = reactor.keyspace.runtime_metrics();
        assert_eq!(runtime.submit_sq_full_retries, 1);
        assert_eq!(runtime.submit_failures, 0);
    }

    #[test]
    fn submit_read_sq_full_retry_failure_closes_connection() {
        let state = Arc::new(Mutex::new(MockBackendState {
            fail_next_submit_sq_full: true,
            fail_retry_submit_sq_full: true,
            ..Default::default()
        }));
        let mut reactor = test_reactor_with_backend(state.clone());

        let read_idx = reactor.buffer_pool.lease_index().unwrap();
        let write_idx = reactor.buffer_pool.lease_index().unwrap();
        let mut meta = ConnectionMeta::new(123, 0);
        meta.read_buf_offset = read_idx as u32;
        meta.write_buf_offset = write_idx as u32;
        let conn_id = reactor.connections.insert(meta);
        reactor.generations[conn_id] = 11;

        reactor.submit_read_for(conn_id, 123);

        assert!(reactor.connections.get(conn_id).unwrap().is_closing());
        assert_eq!(state.lock().unwrap().flushes, 1);

        let runtime = reactor.keyspace.runtime_metrics();
        assert_eq!(runtime.submit_sq_full_retries, 1);
        assert_eq!(runtime.submit_failures, 1);
    }
}
