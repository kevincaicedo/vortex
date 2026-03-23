//! Single-threaded event loop reactor.
//!
//! Each reactor is pinned to a CPU core and owns its own I/O backend,
//! connection slab, buffer pool, timer wheel, and buffer state.

use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;

use crate::backend::{
    decode_token, encode_token, Completion, IoBackend, OpType, PollingBackend, ACCEPT_CONN_ID,
};
use crate::connection::{ConnectionMeta, ConnectionSlab, ConnectionState};
use crate::pool::{CrossChannel, CrossMessage};
use crate::timer::{ExpiredTimer, TimerWheel};
use vortex_memory::BufferPool;
use vortex_proto::{RespFrame, RespParser};

/// Default read buffer size per connection (16 KB).
const DEFAULT_BUF_SIZE: usize = 16_384;

/// Maximum completions processed per iteration.
const MAX_COMPLETIONS: usize = 256;

/// Pre-computed RESP response for PONG.
static RESP_PONG: &[u8] = b"+PONG\r\n";
/// Pre-computed RESP error for unknown commands.
static RESP_ERR_UNKNOWN: &[u8] = b"-ERR unknown command\r\n";

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
    /// Buffer pool for connection read/write buffers.
    /// TODO(Phase 1.5): Upgrade to mmap-backed page-aligned buffers with io_uring
    /// fixed buffer registration.
    #[allow(dead_code)]
    buffer_pool: BufferPool,
    /// Per-slot generation counters: indexed by slab token.
    /// Incremented each time a slot is reused, preventing stale CQE processing.
    generations: Vec<u8>,
    /// Per-connection read buffers: indexed by slab token.
    read_bufs: Vec<Vec<u8>>,
    /// Per-connection cursor: bytes currently in the read buffer.
    read_cursors: Vec<usize>,
    /// Per-connection write buffers.
    write_bufs: Vec<Vec<u8>>,
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
    /// Shared shutdown flag.
    shutdown: Arc<AtomicBool>,
    /// Whether we're in drain mode (no new accepts).
    draining: bool,
    /// Configuration.
    config: ReactorConfig,
    /// Incoming cross-reactor SPSC channels.
    cross_rx: Vec<CrossChannel>,
}

impl Reactor {
    /// Creates a new reactor with the polling backend.
    pub fn new(id: usize, config: ReactorConfig, shutdown: Arc<AtomicBool>) -> std::io::Result<Self> {
        let backend = Box::new(PollingBackend::new()?);
        Self::with_backend(id, config, shutdown, backend)
    }

    /// Creates a new reactor with a specific backend.
    pub fn with_backend(
        id: usize,
        config: ReactorConfig,
        shutdown: Arc<AtomicBool>,
        backend: Box<dyn IoBackend>,
    ) -> std::io::Result<Self> {
        // Create the listener socket with SO_REUSEPORT + SO_REUSEADDR + SO_INCOMING_CPU.
        let listener_fd = crate::accept::create_listener(config.bind_addr, Some(id))?;

        let max_conn = config.max_connections;
        let buf_size = config.buffer_size;
        let connection_timeout = config.connection_timeout;

        let buffer_pool = BufferPool::new(config.buffer_count, config.buffer_size);

        Ok(Self {
            id,
            backend,
            connections: ConnectionSlab::with_capacity(max_conn),
            listener_fd,
            buffer_pool,
            generations: vec![0u8; max_conn],
            read_bufs: (0..max_conn).map(|_| vec![0u8; buf_size]).collect(),
            read_cursors: vec![0; max_conn],
            write_bufs: (0..max_conn).map(|_| Vec::with_capacity(4096)).collect(),
            cqe_buf: Vec::with_capacity(MAX_COMPLETIONS),
            timer_wheel: TimerWheel::new(max_conn),
            start_time: Instant::now(),
            now_secs: 0,
            expired_buf: Vec::with_capacity(64),
            connection_timeout,
            running: false,
            shutdown,
            draining: false,
            config,
            cross_rx: Vec::new(),
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
                        let valid = self
                            .connections
                            .get(conn_id)
                            .is_some_and(|_| {
                                self.generations.get(conn_id).copied().unwrap_or(0) == cgen
                            });
                        if !valid {
                            continue;
                        }
                        match op {
                            OpType::Read => self.handle_read(conn_id, cqe),
                            OpType::Write => self.handle_write(conn_id, cqe),
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

            // 6. Check shutdown.
            if self.shutdown.load(Ordering::Relaxed) && !self.draining {
                self.enter_drain_mode();
            }

            if self.draining && self.connections.is_empty() {
                tracing::info!(reactor_id = self.id, "drain complete, exiting");
                break;
            }
        }

        self.running = false;
        // Close the listener fd.
        // SAFETY: listener_fd is a valid fd we own.
        unsafe {
            libc::close(self.listener_fd);
        }
        tracing::info!(reactor_id = self.id, "reactor stopped");
    }

    /// Signals the reactor to stop (sets the shared shutdown flag).
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
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
            unsafe { libc::close(new_fd); }
        } else {
            // Allocate slab slot.
            let mut meta = ConnectionMeta::new(new_fd, 0);
            meta.last_active = self.now_secs;
            let conn_id = self.connections.insert(meta);

            // Bump generation for this slot (wraps on overflow).
            if conn_id < self.generations.len() {
                self.generations[conn_id] = self.generations[conn_id].wrapping_add(1);
            }
            let cgen = self.generations.get(conn_id).copied().unwrap_or(0);

            // Set the buffer offsets (buffer index == slab token).
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.read_buf_offset = conn_id as u32;
                c.write_buf_offset = conn_id as u32;
            }

            tracing::debug!(reactor_id = self.id, conn_id, fd = new_fd, cgen, "connection accepted");

            // Reset read cursor.
            if conn_id < self.read_cursors.len() {
                self.read_cursors[conn_id] = 0;
            }

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

        // Advance read cursor.
        if conn_id < self.read_cursors.len() {
            self.read_cursors[conn_id] += bytes_read;
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
    fn process_commands(&mut self, conn_id: usize, fd: RawFd) {
        let cursor = match self.read_cursors.get(conn_id) {
            Some(&c) => c,
            None => return,
        };

        if cursor == 0
            || conn_id >= self.read_bufs.len()
            || conn_id >= self.write_bufs.len()
        {
            self.submit_read_for(conn_id, fd);
            return;
        }

        let mut offset = 0;
        self.write_bufs[conn_id].clear();

        // Parse and dispatch in a single pass — write responses directly to
        // the write buffer, avoiding an intermediate Vec allocation.
        while offset < cursor {
            // Temporary borrow of read_bufs is released after parse returns
            // (RespFrame contains owned Bytes, not references into the buffer).
            let parse_result =
                RespParser::parse(&self.read_bufs[conn_id][offset..cursor]);
            match parse_result {
                Ok((frame, consumed)) => {
                    offset += consumed;
                    let response = self.dispatch_command(&frame);
                    self.write_bufs[conn_id].extend_from_slice(response);
                }
                Err(_) => {
                    // Need more data — shift unconsumed bytes and read more.
                    break;
                }
            }
        }

        // Shift unconsumed data to the front of the read buffer.
        if offset > 0 && conn_id < self.read_bufs.len() {
            let remaining = cursor - offset;
            self.read_bufs[conn_id].copy_within(offset..cursor, 0);
            self.read_cursors[conn_id] = remaining;
        }

        // Write all responses, or re-arm read if no complete command was parsed.
        if !self.write_bufs[conn_id].is_empty() {
            let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
            let buf_ptr = self.write_bufs[conn_id].as_ptr();
            let buf_len = self.write_bufs[conn_id].len();
            let token = encode_token(conn_id, cgen, OpType::Write);
            let _ = self.backend.submit_write(fd, buf_ptr, buf_len, token);
        } else {
            // No complete command yet — re-arm read.
            self.submit_read_for(conn_id, fd);
        }
    }

    /// Dispatch a parsed RESP frame to a command handler.
    /// Phase 1 only handles PING/QUIT.
    /// TODO(Phase 2): Route commands to the vortex-engine shard via command dispatch table.
    fn dispatch_command(&self, frame: &RespFrame) -> &'static [u8] {
        match frame {
            RespFrame::Array(Some(frames)) if !frames.is_empty() => {
                let cmd = Self::extract_command_name(&frames[0]);
                match cmd {
                    Some(name) if name.eq_ignore_ascii_case(b"PING") => RESP_PONG,
                    Some(name) if name.eq_ignore_ascii_case(b"QUIT") => {
                        b"+OK\r\n"
                    }
                    _ => RESP_ERR_UNKNOWN,
                }
            }
            // Inline PING (parsed as array by the inline parser).
            _ => RESP_ERR_UNKNOWN,
        }
    }

    /// Extract the command name bytes from a RESP frame.
    fn extract_command_name(frame: &RespFrame) -> Option<&Bytes> {
        match frame {
            RespFrame::BulkString(Some(data)) => Some(data),
            RespFrame::SimpleString(data) => Some(data),
            _ => None,
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

        let fd = match self.connections.get(conn_id) {
            Some(c) => c.fd,
            None => return,
        };

        let bytes_written = cqe.result as usize;
        let total = if conn_id < self.write_bufs.len() {
            self.write_bufs[conn_id].len()
        } else {
            0
        };

        if bytes_written < total {
            // Partial write — submit the remainder.
            if conn_id < self.write_bufs.len() {
                // Shift unwritten bytes to the front.
                self.write_bufs[conn_id].drain(..bytes_written);
                let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
                let buf_ptr = self.write_bufs[conn_id].as_ptr();
                let buf_len = self.write_bufs[conn_id].len();
                let token = encode_token(conn_id, cgen, OpType::Write);
                let _ = self.backend.submit_write(fd, buf_ptr, buf_len, token);
            }
        } else {
            // Write complete — check if this was a QUIT response.
            // For now, re-arm read.
            self.submit_read_for(conn_id, fd);
        }
    }

    // ── Close handler ──────────────────────────────────────────────

    fn handle_close(&mut self, conn_id: usize) {
        if self.connections.get(conn_id).is_some() {
            self.connections.remove(conn_id);
            tracing::debug!(reactor_id = self.id, conn_id, "connection closed");
        }
    }

    // ── Helpers ────────────────────────────────────────────────────

    /// Submit a read SQE for the given connection.
    fn submit_read_for(&mut self, conn_id: usize, fd: RawFd) {
        if conn_id >= self.read_bufs.len() {
            return;
        }
        let cursor = self.read_cursors[conn_id];
        let buf_len = self.read_bufs[conn_id].len();
        let buf_remaining = buf_len - cursor;
        if buf_remaining == 0 {
            // Buffer full — attempt to grow by doubling (up to 1 MB cap).
            const MAX_READ_BUF: usize = 1 << 20; // 1 MB
            if buf_len < MAX_READ_BUF {
                let new_size = (buf_len * 2).min(MAX_READ_BUF);
                self.read_bufs[conn_id].resize(new_size, 0);
                tracing::debug!(conn_id, old = buf_len, new = new_size, "read buffer grown");
            } else {
                // Already at max — apply backpressure: respond with an error
                // and close the connection to protect server memory.
                tracing::warn!(conn_id, "read buffer at maximum capacity, closing connection");
                if conn_id < self.write_bufs.len() {
                    self.write_bufs[conn_id].clear();
                    self.write_bufs[conn_id].extend_from_slice(b"-ERR read buffer overflow\r\n");
                    let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
                    let buf_ptr = self.write_bufs[conn_id].as_ptr();
                    let buf_len = self.write_bufs[conn_id].len();
                    let token = encode_token(conn_id, cgen, OpType::Write);
                    let _ = self.backend.submit_write(fd, buf_ptr, buf_len, token);
                }
                // Close after the error write completes (the write handler
                // will re-arm read, which will hit close_connection on the
                // next cycle since we're transitioning to Closing).
                self.close_connection(conn_id);
                return;
            }
        }
        let buf_ptr = unsafe { self.read_bufs[conn_id].as_mut_ptr().add(cursor) };
        let buf_remaining = self.read_bufs[conn_id].len() - cursor;
        let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
        let token = encode_token(conn_id, cgen, OpType::Read);
        let _ = self.backend.submit_read(fd, buf_ptr, buf_remaining, token);
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
    fn handle_expired_timer(&mut self, conn_id: usize, timer_gen: u8) {
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

    /// Enter drain mode: stop accepting, close all connections.
    fn enter_drain_mode(&mut self) {
        self.draining = true;
        tracing::info!(reactor_id = self.id, "entering drain mode");

        // Close all active connections.
        let conn_ids: Vec<usize> = self.connections.ids().collect();
        for conn_id in conn_ids {
            self.close_connection(conn_id);
        }
    }

    /// Returns the number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_ping() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let config = ReactorConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid addr"),
            ..ReactorConfig::default()
        };
        let reactor = Reactor::new(0, config, shutdown).expect("reactor creation");

        let frame = RespFrame::Array(Some(vec![RespFrame::BulkString(Some(
            Bytes::from_static(b"PING"),
        ))]));

        let resp = reactor.dispatch_command(&frame);
        assert_eq!(resp, RESP_PONG);
    }

    #[test]
    fn dispatch_unknown() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let config = ReactorConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid addr"),
            ..ReactorConfig::default()
        };
        let reactor = Reactor::new(0, config, shutdown).expect("reactor creation");

        let frame = RespFrame::Array(Some(vec![RespFrame::BulkString(Some(
            Bytes::from_static(b"SET"),
        ))]));

        let resp = reactor.dispatch_command(&frame);
        assert_eq!(resp, RESP_ERR_UNKNOWN);
    }

    #[test]
    fn dispatch_ping_lowercase() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let config = ReactorConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid addr"),
            ..ReactorConfig::default()
        };
        let reactor = Reactor::new(0, config, shutdown).expect("reactor creation");

        let frame = RespFrame::Array(Some(vec![RespFrame::BulkString(Some(
            Bytes::from_static(b"ping"),
        ))]));

        let resp = reactor.dispatch_command(&frame);
        assert_eq!(resp, RESP_PONG);
    }
}
