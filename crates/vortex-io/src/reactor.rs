//! Single-threaded event loop reactor.
//!
//! Each reactor is pinned to a CPU core and owns its own I/O backend,
//! connection slab, buffer pool, timer wheel, and buffer state.

use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::Instant;

use crate::backend::{
    ACCEPT_CONN_ID, Completion, IoBackend, OpType, PollingBackend, decode_token, encode_token,
};
use crate::connection::{ConnectionFlags, ConnectionMeta, ConnectionSlab, ConnectionState};
use crate::pool::{CrossChannel, CrossMessage};
use crate::shutdown::ShutdownCoordinator;
use crate::timer::{ExpiredTimer, TimerWheel};
use vortex_memory::{ArenaAllocator, BufferPool};
use vortex_proto::{
    CommandRouter, DispatchResult, FrameRef, IovecWriter, ParseError, RespFrame, RespSerializer,
    RespTape,
};

/// Default read buffer size per connection (16 KB).
const DEFAULT_BUF_SIZE: usize = 16_384;

/// Maximum completions processed per iteration.
const MAX_COMPLETIONS: usize = 256;

/// Pre-computed RESP response for PONG.
static RESP_PONG: &[u8] = b"+PONG\r\n";
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
    /// Dynamic RESP frame requiring serialization.
    /// Uses `serialize_to_iovecs()` + `submit_writev()` for large responses,
    /// or `serialize_to_slice()` + memcpy for small ones.
    Frame(RespFrame),
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
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:6379".parse().expect("valid default addr"),
            max_connections: 1024,
            buffer_size: DEFAULT_BUF_SIZE,
            buffer_count: 1024,
            connection_timeout: 300,
        }
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
    /// Wall-clock epoch: `Instant` captured at reactor start.
    start_time: Instant,
    /// Cached current time in seconds since `start_time`.
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
    /// Incoming cross-reactor SPSC channels.
    cross_rx: Vec<CrossChannel>,
    /// Per-iteration bump allocator for transient response building.
    arena: ArenaAllocator,
    /// Reusable scatter-gather writer for iovec responses.
    iovec_writer: IovecWriter,
    /// Command dispatch router with PHF lookup.
    command_router: CommandRouter,
}

impl Reactor {
    /// Creates a new reactor with the polling backend.
    pub fn new(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
    ) -> std::io::Result<Self> {
        let backend = Box::new(PollingBackend::new()?);
        Self::with_backend(id, config, coordinator, backend)
    }

    /// Creates a new reactor with a specific backend.
    pub fn with_backend(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
        backend: Box<dyn IoBackend>,
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

        Ok(Self {
            id,
            backend,
            connections: ConnectionSlab::with_capacity(max_conn),
            listener_fd,
            buffer_pool,
            generations: vec![0u32; max_conn],
            cqe_buf: Vec::with_capacity(MAX_COMPLETIONS),
            timer_wheel: TimerWheel::new(max_conn),
            start_time: Instant::now(),
            now_secs: 0,
            expired_buf: Vec::with_capacity(64),
            connection_timeout,
            running: false,
            coordinator,
            draining: false,
            config,
            cross_rx: Vec::new(),
            arena: ArenaAllocator::new(vortex_memory::arena::DEFAULT_ARENA_CAPACITY),
            iovec_writer: IovecWriter::new(),
            command_router: CommandRouter::new(),
        })
    }

    /// Runs the reactor event loop. Blocks until shutdown.
    pub fn run(&mut self) {
        self.running = true;
        self.start_time = Instant::now();
        tracing::info!(reactor_id = self.id, "reactor starting");

        // Submit initial accept.
        let accept_token = encode_token(ACCEPT_CONN_ID, 0, OpType::Accept);
        if let Err(e) = self.backend.submit_accept(self.listener_fd, accept_token) {
            tracing::error!(error = %e, "failed to submit initial accept");
            return;
        }

        loop {
            // Cache wall-clock time once per iteration.
            self.now_secs = self.start_time.elapsed().as_secs() as u32;

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
                            OpType::Write | OpType::Writev => self.handle_write(conn_id, cqe),
                            OpType::Close => self.handle_close(conn_id),
                            OpType::Accept => unreachable!(),
                        }
                    }
                }
            }
            completions.clear();
            self.cqe_buf = completions;

            // 4. Tick timer wheel — expire idle connections.
            if self.connection_timeout > 0 {
                self.tick_timers();
            }

            // 5. Drain cross-reactor message queues.
            self.drain_cross_reactor_queue();

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

    /// Set the incoming cross-reactor SPSC channels.
    ///
    /// Called by [`ReactorPool`] before `run()` to install the messaging mesh.
    pub fn set_cross_channels(&mut self, channels: Vec<CrossChannel>) {
        self.cross_rx = channels;
    }

    // ── Cross-reactor messaging ────────────────────────────────────

    /// Drain all incoming cross-reactor SPSC channels (non-blocking).
    fn drain_cross_reactor_queue(&mut self) {
        let mut shutdown_requested = false;
        for channel in &self.cross_rx {
            while let Some(msg) = channel.pop() {
                match msg {
                    CrossMessage::Shutdown => {
                        shutdown_requested = true;
                    }
                    CrossMessage::Ping => {
                        tracing::debug!(reactor_id = self.id, "cross-reactor ping received");
                    }
                }
            }
        }
        if shutdown_requested && !self.draining {
            tracing::info!(reactor_id = self.id, "cross-reactor shutdown received");
            self.enter_drain_mode();
        }
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
                        let response = self.dispatch_command(&frame);
                        match response {
                            CommandResponse::Static(buf) => {
                                if scatter_gather {
                                    // Already in scatter-gather mode — push as static iovec.
                                    self.iovec_writer.push_static(buf);
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
                            CommandResponse::Frame(ref resp_frame) => {
                                if !scatter_gather {
                                    // Try direct serialization into write buffer first.
                                    let remaining = &mut write_buf[write_cursor..];
                                    if let Some(n) =
                                        RespSerializer::serialize_to_slice(resp_frame, remaining)
                                    {
                                        write_cursor += n;
                                        continue;
                                    }
                                    // Doesn't fit — switch entire batch to scatter-gather.
                                    scatter_gather = true;
                                    self.iovec_writer.clear();
                                    if write_cursor > 0 {
                                        // Transfer accumulated write_buf data into iovec.
                                        // SAFETY: write_buf is pool-allocated and remains
                                        // valid for the connection lifetime. The pointer
                                        // stored by push_bytes is consumed before the
                                        // buffer is released or reused.
                                        self.iovec_writer.push_bytes(&write_buf[..write_cursor]);
                                    }
                                }
                                RespSerializer::serialize_to_iovecs(
                                    resp_frame,
                                    &mut self.iovec_writer,
                                );
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
                        self.iovec_writer.push_static(RESP_ERR_PROTOCOL);
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
            let total = self.iovec_writer.total_len();
            if total > 0 {
                if let Some(c) = self.connections.get_mut(conn_id) {
                    c.write_buf_len = total as u32;
                }
                let iovecs = self.iovec_writer.as_raw_iovecs();
                let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
                let token = encode_token(conn_id, cgen, OpType::Writev);
                // NOTE: For the polling backend, writev completes synchronously
                // so the iovec Vec lifetime is sufficient. For io_uring, the
                // iovecs must remain valid until the CQE is reaped — a pinned
                // iovec buffer on the Reactor would be needed for production
                // io_uring scatter-gather.
                let _ = self
                    .backend
                    .submit_writev(fd, iovecs.as_ptr(), iovecs.len(), token);
            } else if close_after_write {
                self.close_connection(conn_id);
            } else {
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

    /// Dispatch a parsed tape frame via PHF command router.
    ///
    /// Uses O(1) perfect-hash lookup with SWAR uppercase normalization.
    /// Handles connection-level commands (PING, QUIT, ECHO, COMMAND, INFO)
    /// directly; all other resolved commands return an unimplemented stub.
    fn dispatch_command(&mut self, frame: &FrameRef<'_>) -> CommandResponse {
        static RESP_ERR_WRONG_ARGC_PREFIX: &[u8] =
            b"-ERR wrong number of arguments for command\r\n";

        match self.command_router.dispatch(frame) {
            DispatchResult::Dispatch { meta, .. } => match meta.name {
                "PING" => CommandResponse::Static(RESP_PONG),
                "QUIT" => CommandResponse::Static(b"+OK\r\n"),
                "ECHO" => {
                    // ECHO message — return the message as a bulk string.
                    if let Some(mut children) = frame.children() {
                        children.next(); // skip command name
                        if let Some(arg) = children.next() {
                            if let Some(bytes) = arg.as_bytes() {
                                return CommandResponse::Frame(RespFrame::bulk_string(
                                    bytes::Bytes::copy_from_slice(bytes),
                                ));
                            }
                        }
                    }
                    CommandResponse::Static(b"$-1\r\n")
                }
                "COMMAND" => {
                    // Minimal COMMAND stub — return empty array.
                    CommandResponse::Static(b"*0\r\n")
                }
                "INFO" => {
                    // Minimal INFO stub.
                    CommandResponse::Static(b"$11\r\n# VortexDB\r\n")
                }
                _ => {
                    // Command recognized but not yet implemented.
                    CommandResponse::Static(b"-ERR command not yet implemented\r\n")
                }
            },
            DispatchResult::WrongArity { .. } => {
                CommandResponse::Static(RESP_ERR_WRONG_ARGC_PREFIX)
            }
            DispatchResult::UnknownCommand => CommandResponse::Static(RESP_ERR_UNKNOWN),
        }
    }

    // ── Write handler ──────────────────────────────────────────────

    fn handle_write(&mut self, conn_id: usize, cqe: &Completion) {
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
            // Partial write — shift unwritten bytes to front and resubmit.
            let write_ptr = self.buffer_pool.ptr(write_idx);
            let remaining = total - bytes_written;
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
        } else if self.draining
            || self
                .connections
                .get(conn_id)
                .is_some_and(|c| (c.flags & ConnectionFlags::CLOSE_AFTER_WRITE) != 0)
        {
            // Drain mode: writes flushed — close connection, don't re-arm read.
            self.close_connection(conn_id);
        } else {
            // Write complete — clear write length and re-arm read.
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.write_buf_len = 0;
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
                // Write data pending — let the write handler close after flush.
                let write_ptr = self.buffer_pool.ptr(write_idx) as *const u8;
                let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
                let token = encode_token(conn_id, cgen, OpType::Write);
                let _ = self.backend.submit_write_fixed(
                    fd,
                    write_ptr,
                    write_len,
                    write_idx as u16,
                    token,
                );
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: parse a single RESP wire command and dispatch via CommandRouter.
    fn dispatch_wire(wire: &[u8]) -> CommandResponse {
        let tape = RespTape::parse_pipeline(wire).expect("valid RESP");
        let frame = tape.iter().next().expect("at least one frame");
        let mut router = CommandRouter::new();
        match router.dispatch(&frame) {
            DispatchResult::Dispatch { meta, .. } => match meta.name {
                "PING" => CommandResponse::Static(RESP_PONG),
                "QUIT" => CommandResponse::Static(b"+OK\r\n"),
                _ => CommandResponse::Static(b"-ERR command not yet implemented\r\n"),
            },
            DispatchResult::WrongArity { .. } => {
                CommandResponse::Static(b"-ERR wrong number of arguments for command\r\n")
            }
            DispatchResult::UnknownCommand => CommandResponse::Static(RESP_ERR_UNKNOWN),
        }
    }

    #[test]
    fn dispatch_ping() {
        let resp = dispatch_wire(b"*1\r\n$4\r\nPING\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_PONG));
    }

    #[test]
    fn dispatch_unknown() {
        // FOOBAR is truly unknown
        let resp = dispatch_wire(b"*1\r\n$6\r\nFOOBAR\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_ERR_UNKNOWN));
    }

    #[test]
    fn dispatch_set_recognized() {
        // SET key value → recognized but not yet implemented
        let resp = dispatch_wire(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert!(
            matches!(resp, CommandResponse::Static(b) if b == b"-ERR command not yet implemented\r\n")
        );
    }

    #[test]
    fn dispatch_ping_lowercase() {
        let resp = dispatch_wire(b"*1\r\n$4\r\nping\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_PONG));
    }

    #[test]
    fn dispatch_attribute_wrapped_ping() {
        // |1\r\n+meta\r\n+value\r\n*1\r\n$4\r\nPING\r\n
        let resp = dispatch_wire(b"|1\r\n+meta\r\n+value\r\n*1\r\n$4\r\nPING\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b == RESP_PONG));
    }

    #[test]
    fn dispatch_wrong_arity() {
        // GET with no key (only 1 element, arity=2)
        let resp = dispatch_wire(b"*1\r\n$3\r\nGET\r\n");
        assert!(matches!(resp, CommandResponse::Static(b) if b.starts_with(b"-ERR wrong number")));
    }
}
