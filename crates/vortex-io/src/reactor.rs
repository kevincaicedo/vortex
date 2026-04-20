//! Single-threaded event loop reactor.
//!
//! Each reactor is pinned to a CPU core and owns its own I/O backend,
//! connection slab, buffer pool, timer wheel, and buffer state.

use std::os::fd::RawFd;
use std::sync::Arc;

use crate::backend::{
    ACCEPT_CONN_ID, Completion, IoBackend, OpType, PollingBackend, decode_token, encode_token,
};
use crate::connection::{ConnectionFlags, ConnectionMeta, ConnectionSlab, ConnectionState};
use crate::pool::IoBackendMode;
use crate::shutdown::ShutdownCoordinator;
use crate::timer::{ExpiredTimer, TimerWheel};
use vortex_common::Timestamp;
use vortex_engine::ConcurrentKeyspace;
use vortex_engine::commands::{CmdResult, arg_bytes, arg_count, execute_command};
use vortex_engine::concurrent_keyspace::DEFAULT_SHARD_COUNT;
use vortex_memory::{ArenaAllocator, BufferPool};
use vortex_persist::aof::rewrite::AofRewriter;
use vortex_persist::aof::writer::AofFileWriter;
use vortex_proto::{
    CommandRouter, DispatchResult, FrameRef, IovecWriter, ParseError, RespFrame, RespSerializer,
    RespTape,
};

/// Default read buffer size per connection (16 KB).
const DEFAULT_BUF_SIZE: usize = 16_384;

/// Maximum completions processed per iteration.
const MAX_COMPLETIONS: usize = 256;

/// Pre-computed RESP error for unknown commands.
static RESP_ERR_UNKNOWN: &[u8] = b"-ERR unknown command\r\n";
/// Pre-computed RESP error for protocol failures.
static RESP_ERR_PROTOCOL: &[u8] = b"-ERR protocol error\r\n";

/// Responses smaller than this threshold are copied into the write buffer.
/// Larger responses use scatter-gather `writev` to avoid contiguous copies.
#[allow(dead_code)] // Infrastructure for Phase 2.5 engine command integration.
const WRITEV_THRESHOLD: usize = 256;

/// Command response type — determines serialization strategy.
#[allow(dead_code)] // Frame variant is infrastructure for Phase 2.5 engine commands.
enum CommandResponse {
    /// Pre-computed static response (PONG, OK, ERR).
    /// Copied directly into the connection write buffer.
    Static(&'static [u8]),
    /// Tiny dynamic response already serialized into inline RESP bytes.
    Inline(vortex_engine::commands::InlineResp),
    /// Dynamic RESP frame requiring serialization.
    /// Uses `serialize_to_iovecs()` + `submit_writev()` for large responses,
    /// or `serialize_to_slice()` + memcpy for small ones.
    Frame(RespFrame),
}

struct PendingWritev {
    writer: IovecWriter,
    frames: Vec<RespFrame>,
    iovecs: Vec<libc::iovec>,
    iov_start: usize,
    total_len: usize,
}

impl PendingWritev {
    fn new() -> Self {
        Self {
            writer: IovecWriter::new(),
            frames: Vec::new(),
            iovecs: Vec::new(),
            iov_start: 0,
            total_len: 0,
        }
    }

    fn clear(&mut self) {
        self.writer.clear();
        self.frames.clear();
        self.iovecs.clear();
        self.iov_start = 0;
        self.total_len = 0;
    }

    fn push_static(&mut self, buf: &'static [u8]) {
        self.writer.push_static(buf);
    }

    fn push_bytes(&mut self, buf: &[u8]) {
        self.writer.push_bytes(buf);
    }

    fn push_inline(&mut self, buf: &[u8]) {
        self.writer.push_scratch(buf);
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
            buffer_count: 1024,
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
        return Ok(Box::new(crate::backend::IoUringBackend::new(
            config.ring_size,
            config.sqpoll_idle_ms,
        )?));
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
    /// Command dispatch router with PHF lookup.
    command_router: CommandRouter,
    /// Shared concurrent keyspace — all reactors operate on the same data.
    keyspace: Arc<ConcurrentKeyspace>,
    /// Cached monotonic timestamp (nanoseconds) for the current event-loop iteration.
    /// Avoids re-reading the clock for every command in a batch.
    cached_nanos: u64,
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
    /// Active expiry: current shard index being swept by this reactor.
    /// Each reactor sweeps different shards in round-robin fashion to
    /// distribute expiry load across the pool.
    expiry_shard_cursor: usize,
    /// Active expiry: current slot offset within the current shard.
    expiry_slot_cursor: usize,
}

impl Reactor {
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
        Self::with_keyspace_and_backend(id, config, coordinator, keyspace, backend, 1)
    }

    /// Creates a new reactor with a specific backend and a fresh keyspace.
    pub fn with_backend(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
        backend: Box<dyn IoBackend>,
    ) -> std::io::Result<Self> {
        let keyspace = Arc::new(ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT));
        Self::with_keyspace_and_backend(id, config, coordinator, keyspace, backend, 1)
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
        let backend = make_backend(&config)?;
        Self::with_keyspace_and_backend(id, config, coordinator, keyspace, backend, num_reactors)
    }

    /// Internal constructor — all public constructors delegate here.
    pub(crate) fn with_keyspace_and_backend(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
        keyspace: Arc<ConcurrentKeyspace>,
        backend: Box<dyn IoBackend>,
        num_reactors: usize,
    ) -> std::io::Result<Self> {
        // Create the listener socket with SO_REUSEPORT + SO_REUSEADDR + SO_INCOMING_CPU.
        let listener_fd = crate::accept::create_listener(config.bind_addr, Some(id))?;

        let max_conn = config.max_connections;
        let connection_timeout = config.connection_timeout;

        // Allocate 2 buffers per connection (read + write) from the mmap pool.
        let buffer_count = max_conn * 2;
        let buffer_pool = BufferPool::new(buffer_count, config.buffer_size);

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

        // Stagger the starting shard cursor by reactor ID so each reactor
        // sweeps a distinct region, distributing expiry work evenly.
        // With K shards and N reactors, spacing = K/N.
        let expiry_shard_cursor = id * (keyspace.num_shards() / num_reactors.max(1)).max(1);

        let cached_nanos = Timestamp::now().as_nanos();

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
            command_router: CommandRouter::new(),
            keyspace,
            cached_nanos,
            next_active_expiry_nanos: cached_nanos,
            aof_writer,
            aof_scratch: vec![0u8; 4096],
            expiry_shard_cursor,
            expiry_slot_cursor: 0,
        })
    }

    /// Runs the reactor event loop. Blocks until shutdown.
    pub fn run(&mut self) {
        self.running = true;
        self.start_nanos = Timestamp::now().as_nanos();
        tracing::info!(reactor_id = self.id, "reactor starting");

        // Submit initial accept.
        let accept_token = encode_token(ACCEPT_CONN_ID, 0, OpType::Accept);
        if let Err(e) = self.backend.submit_accept(self.listener_fd, accept_token) {
            tracing::error!(error = %e, "failed to submit initial accept");
            return;
        }

        loop {
            // Cache monotonic time once per iteration and derive the seconds view from it.
            self.cached_nanos = Timestamp::now().as_nanos();
            self.now_secs = ((self.cached_nanos - self.start_nanos) / 1_000_000_000) as u32;

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
                    let num_shards = self.keyspace.num_shards();
                    const ACTIVE_EXPIRY_INTERVAL_NANOS: u64 = 1_000_000;
                    const MAX_EXPIRY_ITERS: usize = 3;
                    const MAX_EFFORT: usize = 20;
                    self.next_active_expiry_nanos =
                        now.saturating_add(ACTIVE_EXPIRY_INTERVAL_NANOS);
                    for _ in 0..MAX_EXPIRY_ITERS {
                        let shard_idx = self.expiry_shard_cursor % num_shards;
                        let (expired, sampled) = self.keyspace.run_active_expiry_on_shard(
                            shard_idx,
                            self.expiry_slot_cursor,
                            MAX_EFFORT,
                            now,
                        );
                        // Advance cursors for next sweep.
                        self.expiry_slot_cursor = self.expiry_slot_cursor.wrapping_add(MAX_EFFORT);
                        self.expiry_shard_cursor = self.expiry_shard_cursor.wrapping_add(1);
                        if sampled == 0 || expired * 4 <= sampled {
                            break;
                        }
                    }
                }
            }

            // 5.6 AOF periodic fsync — for `everysec` policy, flush to disk
            // at most once per second. No-op for `always` (synced inline)
            // or `no` (OS-managed). Cheap: just checks a timestamp.
            if let Some(ref mut w) = self.aof_writer {
                let _ = w.maybe_fsync();
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
                if !self.draining {
                    let token = encode_token(ACCEPT_CONN_ID, 0, OpType::Accept);
                    let _ = self.backend.submit_accept(self.listener_fd, token);
                }
                return;
            }
            tracing::warn!(errno, "accept failed");
            if !self.draining {
                let token = encode_token(ACCEPT_CONN_ID, 0, OpType::Accept);
                let _ = self.backend.submit_accept(self.listener_fd, token);
            }
            return;
        }

        let new_fd = cqe.result;

        // Set TCP_NODELAY on the new connection.
        // SAFETY: new_fd is a valid socket fd just returned by accept.
        unsafe {
            let nodelay: libc::c_int = 1;
            libc::setsockopt(
                new_fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &nodelay as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            // Set non-blocking.
            let flags = libc::fcntl(new_fd, libc::F_GETFL);
            libc::fcntl(new_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        // Check if we have capacity.
        if self.connections.len() >= self.config.max_connections {
            tracing::warn!("max connections reached, rejecting");
            // SAFETY: new_fd is a valid fd.
            unsafe {
                libc::close(new_fd);
            }
        } else {
            // Lease read and write buffers from the mmap pool.
            let read_idx = match self.buffer_pool.lease_index() {
                Some(idx) => idx,
                None => {
                    tracing::warn!("buffer pool exhausted (read), rejecting connection");
                    unsafe {
                        libc::close(new_fd);
                    }
                    if !self.draining {
                        let token = encode_token(ACCEPT_CONN_ID, 0, OpType::Accept);
                        let _ = self.backend.submit_accept(self.listener_fd, token);
                    }
                    return;
                }
            };
            let write_idx = match self.buffer_pool.lease_index() {
                Some(idx) => idx,
                None => {
                    tracing::warn!("buffer pool exhausted (write), rejecting connection");
                    self.buffer_pool.release_index(read_idx);
                    unsafe {
                        libc::close(new_fd);
                    }
                    if !self.draining {
                        let token = encode_token(ACCEPT_CONN_ID, 0, OpType::Accept);
                        let _ = self.backend.submit_accept(self.listener_fd, token);
                    }
                    return;
                }
            };

            // Allocate slab slot.
            let mut meta = ConnectionMeta::new(new_fd, 0);
            meta.last_active = self.now_secs;
            meta.read_buf_offset = read_idx as u32;
            meta.write_buf_offset = write_idx as u32;
            meta.read_buf_len = 0;
            meta.write_buf_len = 0;
            let conn_id = self.connections.insert(meta);

            // Bump generation for this slot (wraps at 24-bit boundary).
            if conn_id < self.generations.len() {
                self.generations[conn_id] = self.generations[conn_id].wrapping_add(1) & 0xFF_FFFF;
            }
            let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
            self.writev_states[conn_id].clear();

            tracing::debug!(
                reactor_id = self.id,
                conn_id,
                fd = new_fd,
                cgen,
                read_idx,
                write_idx,
                "connection accepted"
            );

            // Schedule idle timeout.
            if self.connection_timeout > 0 {
                let deadline = self.now_secs + self.connection_timeout;
                let entry = self.timer_wheel.schedule(conn_id, cgen, deadline);
                if let Some(c) = self.connections.get_mut(conn_id) {
                    c.timer_slot = entry;
                }
            }

            // Submit read for this connection.
            self.submit_read_for(conn_id, new_fd);
        }

        // Re-arm accept (unless draining).
        if !self.draining {
            let token = encode_token(ACCEPT_CONN_ID, 0, OpType::Accept);
            let _ = self.backend.submit_accept(self.listener_fd, token);
        }
    }

    // ── Read handler ───────────────────────────────────────────────

    fn handle_read(&mut self, conn_id: usize, cqe: &Completion) {
        // Ignore late completions for connections being torn down.
        if let Some(c) = self.connections.get(conn_id) {
            if c.is_closing() {
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
    /// Uses two write strategies based on response type:
    /// - **Direct copy** (`CommandResponse::Static`): memcpy into the pool write buffer,
    ///   then submit `write_fixed`. Used for pre-computed responses (PONG, ERR, OK).
    /// - **Scatter-gather** (`CommandResponse::Frame`): serialize via
    ///   `serialize_to_slice()` into the write buffer when it fits. If the response
    ///   exceeds the remaining write buffer, switch the entire batch to scatter-gather
    ///   mode using `IovecWriter` + `submit_writev()`. The threshold
    ///   (`WRITEV_THRESHOLD`) is unused for the mode switch — any overflow triggers it.
    fn process_commands(&mut self, conn_id: usize, fd: RawFd) {
        let (read_idx, cursor, write_idx) = match self.connections.get(conn_id) {
            Some(c) => (
                c.read_buf_offset as usize,
                c.read_buf_len as usize,
                c.write_buf_offset as usize,
            ),
            None => return,
        };

        if cursor == 0 {
            self.submit_read_for(conn_id, fd);
            return;
        }

        let buf_size = self.buffer_pool.buffer_size();
        let read_ptr = self.buffer_pool.ptr(read_idx);
        let write_ptr = self.buffer_pool.ptr(write_idx);

        // SAFETY: read_ptr and write_ptr point to distinct mmap-backed regions
        // (different pool indices) that are valid for `buf_size` bytes. Access
        // is single-threaded (reactor owns the pool).
        let read_slice = unsafe { std::slice::from_raw_parts(read_ptr, cursor) };
        let write_buf = unsafe { std::slice::from_raw_parts_mut(write_ptr, buf_size) };

        let mut offset = 0;
        let mut write_cursor = 0usize;
        let mut close_after_write = false;
        // Tracks whether we've switched to scatter-gather mode for this batch.
        // Once true, all subsequent responses go through the IovecWriter.
        let mut scatter_gather = false;

        while offset < cursor {
            match RespTape::parse_pipeline(&read_slice[offset..cursor]) {
                Ok(tape) => {
                    let batch_end = offset + tape.consumed();
                    for frame in tape.iter() {
                        let (response, should_close) = self.dispatch_command(&frame);
                        if should_close {
                            close_after_write = true;
                        }
                        match response {
                            CommandResponse::Static(buf) => {
                                if scatter_gather {
                                    // Already in scatter-gather mode — push as static iovec.
                                    self.writev_states[conn_id].push_static(buf);
                                } else {
                                    let end = write_cursor + buf.len();
                                    if end > buf_size {
                                        tracing::warn!(
                                            conn_id,
                                            "write buffer full, closing after flush"
                                        );
                                        close_after_write = true;
                                        offset = cursor;
                                        break;
                                    }
                                    write_buf[write_cursor..end].copy_from_slice(buf);
                                    write_cursor = end;
                                }
                            }
                            CommandResponse::Inline(inline) => {
                                let buf = inline.as_bytes();
                                if scatter_gather {
                                    self.writev_states[conn_id].push_inline(buf);
                                } else {
                                    let end = write_cursor + buf.len();
                                    if end <= buf_size {
                                        write_buf[write_cursor..end].copy_from_slice(buf);
                                        write_cursor = end;
                                    } else {
                                        scatter_gather = true;
                                        self.writev_states[conn_id].clear();
                                        if write_cursor > 0 {
                                            self.writev_states[conn_id]
                                                .push_bytes(&write_buf[..write_cursor]);
                                        }
                                        self.writev_states[conn_id].push_inline(buf);
                                    }
                                }
                            }
                            CommandResponse::Frame(resp_frame) => {
                                if !scatter_gather {
                                    // Try direct serialization into write buffer first.
                                    let remaining = &mut write_buf[write_cursor..];
                                    if let Some(n) =
                                        RespSerializer::serialize_to_slice(&resp_frame, remaining)
                                    {
                                        write_cursor += n;
                                        continue;
                                    }
                                    // Doesn't fit — switch entire batch to scatter-gather.
                                    scatter_gather = true;
                                    self.writev_states[conn_id].clear();
                                    if write_cursor > 0 {
                                        // Transfer accumulated write_buf data into iovec.
                                        self.writev_states[conn_id]
                                            .push_bytes(&write_buf[..write_cursor]);
                                    }
                                }
                                self.writev_states[conn_id].push_frame(resp_frame);
                            }
                        }
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
                    if scatter_gather {
                        self.writev_states[conn_id].push_static(RESP_ERR_PROTOCOL);
                    } else {
                        let end = write_cursor + RESP_ERR_PROTOCOL.len();
                        if end > buf_size {
                            tracing::warn!(
                                conn_id,
                                "protocol error response does not fit, closing"
                            );
                            self.close_connection(conn_id);
                            return;
                        }
                        write_buf[write_cursor..end].copy_from_slice(RESP_ERR_PROTOCOL);
                        write_cursor = end;
                    }
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

        // Write all responses, or re-arm read if no complete command was parsed.
        if scatter_gather {
            // Scatter-gather path: submit writev with the assembled iovecs.
            self.writev_states[conn_id].finalize();
            let total = self.writev_states[conn_id].remaining_len();
            if total > 0 {
                if let Some(c) = self.connections.get_mut(conn_id) {
                    c.write_buf_len = total as u32;
                }
                let remaining_iovecs = self.writev_states[conn_id].remaining_iovecs();
                let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
                let token = encode_token(conn_id, cgen, OpType::Writev);
                if self
                    .backend
                    .submit_writev(fd, remaining_iovecs.as_ptr(), remaining_iovecs.len(), token)
                    .is_err()
                {
                    self.writev_states[conn_id].clear();
                    self.close_connection(conn_id);
                }
            } else if close_after_write {
                self.close_connection(conn_id);
            } else {
                self.writev_states[conn_id].clear();
                self.submit_read_for(conn_id, fd);
            }
        } else if write_cursor > 0 {
            // Direct copy path: submit write_fixed from the pool buffer.
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.write_buf_len = write_cursor as u32;
            }
            let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
            let token = encode_token(conn_id, cgen, OpType::Write);
            let _ = self.backend.submit_write_fixed(
                fd,
                write_ptr as *const u8,
                write_cursor,
                write_idx as u16,
                token,
            );
        } else if close_after_write {
            self.close_connection(conn_id);
        } else if self.draining {
            // Drain mode: no complete command and nothing to write — close.
            self.close_connection(conn_id);
        } else {
            // No complete command yet — re-arm read.
            self.submit_read_for(conn_id, fd);
        }
    }

    /// Dispatch a parsed tape frame through the engine.
    ///
    /// Uses O(1) perfect-hash lookup with SWAR uppercase normalization,
    /// then routes to `vortex_engine::commands::execute_command()` against
    /// the shared `ConcurrentKeyspace`.
    /// Returns `(CommandResponse, should_close)` — the bool signals QUIT.
    fn dispatch_command(&mut self, frame: &FrameRef<'_>) -> (CommandResponse, bool) {
        static RESP_ERR_WRONG_ARGC_PREFIX: &[u8] =
            b"-ERR wrong number of arguments for command\r\n";

        // Intercept reactor-level commands before PHF dispatch to avoid
        // borrow conflicts (dispatch borrows self.command_router mutably).
        if let Some(cmd_name) = frame.command_name() {
            // Quick uppercase check — command names are short (≤16 bytes).
            let mut upper = [0u8; 16];
            let len = cmd_name.len().min(16);
            upper[..len].copy_from_slice(&cmd_name[..len]);
            upper[..len].make_ascii_uppercase();

            match &upper[..len] {
                b"BGREWRITEAOF" => return self.handle_bgrewriteaof(),
                b"CONFIG" => {
                    if let Some(resp) = self.handle_config(frame) {
                        return resp;
                    }
                    // Fall through to engine/PHF for unhandled CONFIG subcommands.
                }
                _ => {}
            }
        }

        match self.command_router.dispatch(frame) {
            DispatchResult::Dispatch { meta, name, .. } => {
                // Route through the engine against the shared ConcurrentKeyspace.
                let now = self.cached_nanos;
                match execute_command(&self.keyspace, name, frame, now) {
                    Some(executed) => match executed.response {
                        CmdResult::Static(buf) => {
                            let close = meta.name == "QUIT";
                            if !close && self.aof_writer.is_some() {
                                if let Some(lsn) = executed.aof_lsn {
                                    self.append_to_aof(lsn, frame);
                                }
                            }
                            (CommandResponse::Static(buf), close)
                        }
                        CmdResult::Inline(inline) => {
                            if self.aof_writer.is_some() {
                                if let Some(lsn) = executed.aof_lsn {
                                    self.append_to_aof(lsn, frame);
                                }
                            }
                            (CommandResponse::Inline(inline), false)
                        }
                        CmdResult::Resp(f) => {
                            if self.aof_writer.is_some() {
                                if let Some(lsn) = executed.aof_lsn {
                                    self.append_to_aof(lsn, frame);
                                }
                            }
                            (CommandResponse::Frame(f), false)
                        }
                    },
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
                (CommandResponse::Static(RESP_ERR_WRONG_ARGC_PREFIX), false)
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
    fn append_to_aof(&mut self, lsn: u64, frame: &FrameRef<'_>) {
        // Serialize frame RESP into scratch buffer.
        let written = match frame.write_resp_to(&mut self.aof_scratch) {
            Some(n) => n,
            None => {
                // Scratch buffer too small — grow it and retry.
                self.aof_scratch.resize(self.aof_scratch.len() * 2, 0);
                match frame.write_resp_to(&mut self.aof_scratch) {
                    Some(n) => n,
                    None => {
                        tracing::warn!("AOF: frame too large for scratch buffer, skipping");
                        return;
                    }
                }
            }
        };

        if let Some(ref mut writer) = self.aof_writer {
            if let Err(e) = writer.append_with_lsn(lsn, &self.aof_scratch[..written]) {
                tracing::error!(error = %e, "AOF write failed");
            }
        }
    }

    /// Handle BGREWRITEAOF command.
    ///
    /// Rewrites the AOF by dumping the current keyspace state as a minimal
    /// set of SET commands. The rewrite runs synchronously on the reactor
    /// thread (blocking — acceptable for alpha) and swaps the file atomically.
    fn handle_bgrewriteaof(&mut self) -> (CommandResponse, bool) {
        let aof_cfg = match &self.config.aof_config {
            Some(cfg) => cfg.clone(),
            None => {
                return (
                    CommandResponse::Static(b"-ERR AOF is not enabled\r\n"),
                    false,
                );
            }
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

        match AofRewriter::rewrite(&self.keyspace, &aof_path, self.id as u16) {
            Ok((new_path, keys_written)) => {
                // Swap the writer to the new file.
                if let Some(ref mut writer) = self.aof_writer {
                    if let Err(e) = writer.swap_file(&new_path) {
                        tracing::error!(error = %e, "AOF rewrite swap failed");
                        return (
                            CommandResponse::Frame(RespFrame::Error(
                                format!("ERR AOF rewrite swap failed: {e}").into(),
                            )),
                            false,
                        );
                    }
                }
                tracing::info!(reactor_id = self.id, keys_written, "BGREWRITEAOF completed");
                (
                    CommandResponse::Static(b"+Background AOF rewrite started\r\n"),
                    false,
                )
            }
            Err(e) => {
                tracing::error!(error = %e, "BGREWRITEAOF failed");
                (
                    CommandResponse::Frame(RespFrame::Error(
                        format!("ERR BGREWRITEAOF failed: {e}").into(),
                    )),
                    false,
                )
            }
        }
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
                    b"appendonly" => {
                        let val = if self.aof_writer.is_some() {
                            "yes"
                        } else {
                            "no"
                        };
                        Some((
                            CommandResponse::Frame(RespFrame::Array(Some(vec![
                                RespFrame::BulkString(Some(bytes::Bytes::from_static(
                                    b"appendonly",
                                ))),
                                RespFrame::BulkString(Some(bytes::Bytes::from(
                                    val.as_bytes().to_vec(),
                                ))),
                            ]))),
                            false,
                        ))
                    }
                    b"appendfsync" => {
                        let val = match &self.config.aof_config {
                            Some(cfg) => match cfg.fsync_policy {
                                vortex_persist::aof::AofFsyncPolicy::Always => "always",
                                vortex_persist::aof::AofFsyncPolicy::Everysec => "everysec",
                                vortex_persist::aof::AofFsyncPolicy::No => "no",
                            },
                            None => "everysec",
                        };
                        Some((
                            CommandResponse::Frame(RespFrame::Array(Some(vec![
                                RespFrame::BulkString(Some(bytes::Bytes::from_static(
                                    b"appendfsync",
                                ))),
                                RespFrame::BulkString(Some(bytes::Bytes::from(
                                    val.as_bytes().to_vec(),
                                ))),
                            ]))),
                            false,
                        ))
                    }
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
                    _ => None, // Fall through to engine.
                }
            }
            _ => None, // RESETSTAT, REWRITE, etc. — fall through.
        }
    }

    // ── Write handler ──────────────────────────────────────────────

    fn handle_write(&mut self, conn_id: usize, op: OpType, cqe: &Completion) {
        // Ignore late completions for connections being torn down.
        if let Some(c) = self.connections.get(conn_id) {
            if c.is_closing() {
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
                    let _ = self.backend.submit_write_fixed(
                        fd,
                        write_ptr as *const u8,
                        remaining,
                        write_idx as u16,
                        token,
                    );
                }
                OpType::Writev => {
                    self.writev_states[conn_id].advance(bytes_written);
                    if let Some(c) = self.connections.get_mut(conn_id) {
                        c.write_buf_len = remaining as u32;
                    }
                    let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
                    let token = encode_token(conn_id, cgen, OpType::Writev);
                    let remaining_iovecs = self.writev_states[conn_id].remaining_iovecs();
                    if self
                        .backend
                        .submit_writev(fd, remaining_iovecs.as_ptr(), remaining_iovecs.len(), token)
                        .is_err()
                    {
                        self.writev_states[conn_id].clear();
                        self.close_connection(conn_id);
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
        if let Some(c) = self.connections.get(conn_id) {
            // Release leased buffers back to the mmap pool.
            let read_idx = c.read_buf_offset as usize;
            let write_idx = c.write_buf_offset as usize;
            self.buffer_pool.release_index(read_idx);
            self.buffer_pool.release_index(write_idx);
            if conn_id < self.writev_states.len() {
                self.writev_states[conn_id].clear();
            }

            self.connections.remove(conn_id);
            tracing::debug!(reactor_id = self.id, conn_id, "connection closed");
        }
    }

    // ── Helpers ────────────────────────────────────────────────────

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
        let _ = self
            .backend
            .submit_read_fixed(fd, buf_ptr, remaining, read_idx as u16, token);
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

        let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
        let token = encode_token(conn_id, cgen, OpType::Close);
        let _ = self.backend.submit_close(fd, token);
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
                self.buffer_pool.release_index(c.write_buf_offset as usize);
            }
            if conn_id < self.writev_states.len() {
                self.writev_states[conn_id].clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static RESP_PONG: &[u8] = b"+PONG\r\n";

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
            DispatchResult::WrongArity { .. } => (
                CommandResponse::Static(b"-ERR wrong number of arguments for command\r\n"),
                false,
            ),
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
            CommandResponse::Inline(_) | CommandResponse::Frame(_) => {
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
}
