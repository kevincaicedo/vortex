//! Cross-platform polling backend using the `polling` crate.
//!
//! Wraps epoll (Linux), kqueue (macOS/BSD). Emulates
//! io_uring's completion-based model by performing I/O inline on poll events
//! and generating synthetic [`Completion`] structs.
//!
//! ## Optimisation
//!
//! * **Edge-triggered persistent interest** – fds are registered with
//!   `PollMode::Edge` (kqueue `EV_CLEAR`, epoll `EPOLLET`) so that read
//!   interest persists across events. On read `EAGAIN`, re-registration is
//!   skipped when interest is already active, eliminating a `kevent`/`epoll_ctl`
//!   syscall per read cycle (~20% CPU reduction measured via profiling).
//! * **Write-readiness registration** – on `EAGAIN` from `write`/`writev`,
//!   the fd is registered for write-readiness with the poller instead of
//!   blindly re-trying every iteration (busy-poll elimination).
//! * **Immediate retry after poll** – when `poller.wait()` returns readiness
//!   events, all pending ops are retried in the *same* `completions()` call
//!   instead of deferring to the next reactor iteration.
//! * **O(1) fd tracking** – per-fd interest flags in a flat `Vec<u8>` indexed
//!   by raw fd, replacing `HashSet` to eliminate hashing overhead.
//! * **1 ms poll timeout** – matches io_uring's wait strategy and keeps tail
//!   latency low during idle periods.

use std::collections::VecDeque;
use std::io;
use std::os::fd::{BorrowedFd, RawFd};

use polling::{Event, Events, PollMode, Poller};

use super::{Completion, IoBackend, OpType, decode_token};

/// Interest flag: fd is registered for read-readiness.
const INTEREST_READABLE: u8 = 1;
/// Interest flag: fd is registered for write-readiness.
const INTEREST_WRITABLE: u8 = 2;

/// Pending operation queued for execution on the next `flush()`/`completions()`.
enum PendingOp {
    Accept {
        listener_fd: RawFd,
        token: u64,
    },
    Read {
        fd: RawFd,
        buf_ptr: *mut u8,
        buf_len: usize,
        token: u64,
    },
    Write {
        fd: RawFd,
        buf_ptr: *const u8,
        buf_len: usize,
        token: u64,
    },
    Writev {
        fd: RawFd,
        iovecs: *const libc::iovec,
        iov_count: usize,
        token: u64,
    },
    Close {
        fd: RawFd,
        token: u64,
    },
}

// SAFETY: The raw pointers in PendingOp point to buffers owned by the reactor
// on the same thread. PollingBackend is single-threaded (thread-per-core).
unsafe impl Send for PendingOp {}

struct ArmedRead {
    fd: RawFd,
    buf_ptr: *mut u8,
    buf_len: usize,
    token: u64,
}

// SAFETY: ArmedRead stores reactor-owned raw buffer pointers and is only used
// by the single-threaded polling backend.
unsafe impl Send for ArmedRead {}

enum ArmedWrite {
    Write {
        fd: RawFd,
        buf_ptr: *const u8,
        buf_len: usize,
        token: u64,
    },
    Writev {
        fd: RawFd,
        iovecs: *const libc::iovec,
        iov_count: usize,
        token: u64,
    },
}

// SAFETY: ArmedWrite stores reactor-owned raw buffer pointers and is only used
// by the single-threaded polling backend.
unsafe impl Send for ArmedWrite {}

/// Cross-platform I/O backend using the `polling` crate.
pub struct PollingBackend {
    poller: Poller,
    events: Events,
    pending: VecDeque<PendingOp>,
    armed_accept: Option<(RawFd, u64)>,
    armed_reads: Vec<Option<ArmedRead>>,
    armed_writes: Vec<Option<ArmedWrite>>,
    ready: VecDeque<Completion>,
    ready_accept_tokens: Vec<u64>,
    ready_read_tokens: Vec<u64>,
    ready_write_tokens: Vec<u64>,
    /// Per-fd interest flags indexed by `RawFd`. 0 = not registered.
    /// Bit 0 (`INTEREST_READABLE`): registered for read-readiness.
    /// Bit 1 (`INTEREST_WRITABLE`): registered for write-readiness.
    registered: Vec<u8>,
}

impl PollingBackend {
    /// Creates a new polling backend.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            poller: Poller::new()?,
            events: Events::new(),
            pending: VecDeque::with_capacity(256),
            armed_accept: None,
            armed_reads: Vec::new(),
            armed_writes: Vec::new(),
            ready: VecDeque::with_capacity(64),
            ready_accept_tokens: Vec::with_capacity(4),
            ready_read_tokens: Vec::with_capacity(64),
            ready_write_tokens: Vec::with_capacity(64),
            registered: vec![0u8; 64],
        })
    }

    /// Ensure the registered vec can hold the given fd index.
    #[inline]
    fn ensure_capacity(&mut self, fd: RawFd) {
        let idx = fd as usize;
        if idx >= self.registered.len() {
            self.registered.resize(idx + 1, 0);
        }
    }

    #[inline]
    fn ensure_read_capacity(&mut self, conn_id: usize) {
        if conn_id >= self.armed_reads.len() {
            self.armed_reads.resize_with(conn_id + 1, || None);
        }
    }

    #[inline]
    fn ensure_write_capacity(&mut self, conn_id: usize) {
        if conn_id >= self.armed_writes.len() {
            self.armed_writes.resize_with(conn_id + 1, || None);
        }
    }

    /// Register (or re-arm) interest for `fd` with the poller using
    /// edge-triggered mode (`EV_CLEAR` on kqueue, `EPOLLET` on epoll).
    fn register_interest(&mut self, fd: RawFd, event: Event) {
        self.ensure_capacity(fd);
        // SAFETY: fd is a valid, open file descriptor owned by the reactor.
        let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
        if self.registered[fd as usize] != 0 {
            let _ = self
                .poller
                .modify_with_mode(borrowed, event, PollMode::Edge);
        } else {
            unsafe {
                self.poller
                    .add_with_mode(&borrowed, event, PollMode::Edge)
                    .unwrap_or_else(|_| {
                        let _ = self
                            .poller
                            .modify_with_mode(borrowed, event, PollMode::Edge);
                    });
            }
        }
        // Update tracked interest flags.
        let mut flags = 0u8;
        if event.readable {
            flags |= INTEREST_READABLE;
        }
        if event.writable {
            flags |= INTEREST_WRITABLE;
        }
        self.registered[fd as usize] = flags;
    }

    /// Process all currently-pending ops, appending completions to `out`.
    fn drain_pending(&mut self, out: &mut Vec<Completion>) {
        let n_pending = self.pending.len();
        for _ in 0..n_pending {
            if let Some(op) = self.pending.pop_front() {
                match op {
                    PendingOp::Accept { listener_fd, token } => {
                        self.do_accept(listener_fd, token, out);
                    }
                    PendingOp::Read {
                        fd,
                        buf_ptr,
                        buf_len,
                        token,
                    } => {
                        self.do_read(fd, buf_ptr, buf_len, token, out);
                    }
                    PendingOp::Write {
                        fd,
                        buf_ptr,
                        buf_len,
                        token,
                    } => {
                        self.do_write(fd, buf_ptr, buf_len, token, out);
                    }
                    PendingOp::Writev {
                        fd,
                        iovecs,
                        iov_count,
                        token,
                    } => {
                        self.do_writev(fd, iovecs, iov_count, token, out);
                    }
                    PendingOp::Close { fd, token } => {
                        self.do_close(fd, token, out);
                    }
                }
            }
        }
    }

    fn rearm_ready_ops(&mut self) {
        self.ready_accept_tokens.clear();
        self.ready_read_tokens.clear();
        self.ready_write_tokens.clear();
        for event in self.events.iter() {
            let token = event.key as u64;
            let (conn_id, _, op) = decode_token(token);
            if event.readable && matches!(op, OpType::Accept) {
                self.ready_accept_tokens.push(token);
            }
            if event.readable && matches!(op, OpType::Read) && conn_id < self.armed_reads.len() {
                self.ready_read_tokens.push(token);
            }
            if event.writable
                && matches!(op, OpType::Write | OpType::Writev)
                && conn_id < self.armed_writes.len()
            {
                self.ready_write_tokens.push(token);
            }
        }

        for token in self.ready_accept_tokens.drain(..) {
            let Some((listener_fd, accept_token)) = self.armed_accept.take() else {
                continue;
            };
            if accept_token == token {
                self.pending.push_back(PendingOp::Accept {
                    listener_fd,
                    token: accept_token,
                });
            } else {
                self.armed_accept = Some((listener_fd, accept_token));
            }
        }

        for token in self.ready_read_tokens.drain(..) {
            let (conn_id, _, _) = decode_token(token);
            let Some(read) = self.armed_reads.get_mut(conn_id).and_then(Option::take) else {
                continue;
            };
            if read.token != token {
                continue;
            }
            self.pending.push_back(PendingOp::Read {
                fd: read.fd,
                buf_ptr: read.buf_ptr,
                buf_len: read.buf_len,
                token: read.token,
            });
        }

        for token in self.ready_write_tokens.drain(..) {
            let (conn_id, _, _) = decode_token(token);
            let Some(write) = self.armed_writes.get_mut(conn_id).and_then(Option::take) else {
                continue;
            };
            match write {
                ArmedWrite::Write {
                    fd,
                    buf_ptr,
                    buf_len,
                    token: write_token,
                } if write_token == token => {
                    self.pending.push_back(PendingOp::Write {
                        fd,
                        buf_ptr,
                        buf_len,
                        token: write_token,
                    });
                }
                ArmedWrite::Writev {
                    fd,
                    iovecs,
                    iov_count,
                    token: write_token,
                } if write_token == token => {
                    self.pending.push_back(PendingOp::Writev {
                        fd,
                        iovecs,
                        iov_count,
                        token: write_token,
                    });
                }
                stale => {
                    self.armed_writes[conn_id] = Some(stale);
                }
            }
        }
    }

    #[inline]
    fn push_canceled_completion(&mut self, token: u64) {
        self.ready.push_back(Completion {
            token,
            result: -libc::ECANCELED,
            flags: 0,
        });
    }

    fn drain_ready(&mut self, out: &mut Vec<Completion>) -> usize {
        let start = out.len();
        while let Some(completion) = self.ready.pop_front() {
            out.push(completion);
        }
        out.len() - start
    }

    fn cancel_pending_token(&mut self, token: u64) -> bool {
        let mut canceled = false;
        let pending_len = self.pending.len();
        for _ in 0..pending_len {
            let Some(op) = self.pending.pop_front() else {
                break;
            };
            if pending_op_token(&op) == token {
                canceled = true;
                self.push_canceled_completion(token);
            } else {
                self.pending.push_back(op);
            }
        }

        let (conn_id, _, op) = decode_token(token);
        if matches!(op, OpType::Read)
            && conn_id < self.armed_reads.len()
            && self.armed_reads[conn_id]
                .as_ref()
                .is_some_and(|read| read.token == token)
        {
            self.armed_reads[conn_id] = None;
            canceled = true;
            self.push_canceled_completion(token);
        }

        if matches!(op, OpType::Write | OpType::Writev)
            && conn_id < self.armed_writes.len()
            && self.armed_writes[conn_id]
                .as_ref()
                .is_some_and(|write| armed_write_token(write) == token)
        {
            self.armed_writes[conn_id] = None;
            canceled = true;
            self.push_canceled_completion(token);
        }

        canceled
    }

    fn cancel_pending_fd(&mut self, fd: RawFd) {
        let mut canceled_tokens = Vec::new();
        let pending_len = self.pending.len();
        for _ in 0..pending_len {
            let Some(op) = self.pending.pop_front() else {
                break;
            };
            if pending_op_fd(&op) == Some(fd) {
                canceled_tokens.push(pending_op_token(&op));
            } else {
                self.pending.push_back(op);
            }
        }

        for armed in &mut self.armed_reads {
            if armed.as_ref().is_some_and(|read| read.fd == fd) {
                canceled_tokens.push(armed.as_ref().map(|read| read.token).unwrap_or(0));
                *armed = None;
            }
        }

        for armed in &mut self.armed_writes {
            if armed
                .as_ref()
                .is_some_and(|write| armed_write_fd(write) == fd)
            {
                canceled_tokens.push(armed.as_ref().map(armed_write_token).unwrap_or(0));
                *armed = None;
            }
        }

        for token in canceled_tokens {
            self.push_canceled_completion(token);
        }
    }
}

impl IoBackend for PollingBackend {
    fn submit_accept(&mut self, listener_fd: RawFd, token: u64) -> io::Result<()> {
        self.pending
            .push_back(PendingOp::Accept { listener_fd, token });
        Ok(())
    }

    fn submit_read(
        &mut self,
        fd: RawFd,
        buf_ptr: *mut u8,
        buf_len: usize,
        token: u64,
    ) -> io::Result<()> {
        self.pending.push_back(PendingOp::Read {
            fd,
            buf_ptr,
            buf_len,
            token,
        });
        Ok(())
    }

    fn submit_write(
        &mut self,
        fd: RawFd,
        buf_ptr: *const u8,
        buf_len: usize,
        token: u64,
    ) -> io::Result<()> {
        self.pending.push_back(PendingOp::Write {
            fd,
            buf_ptr,
            buf_len,
            token,
        });
        Ok(())
    }

    fn submit_close(&mut self, fd: RawFd, token: u64) -> io::Result<()> {
        self.pending.push_back(PendingOp::Close { fd, token });
        Ok(())
    }

    fn submit_writev(
        &mut self,
        fd: RawFd,
        iovecs: *const libc::iovec,
        iov_count: usize,
        token: u64,
    ) -> io::Result<()> {
        self.pending.push_back(PendingOp::Writev {
            fd,
            iovecs,
            iov_count,
            token,
        });
        Ok(())
    }

    fn submit_cancel(&mut self, token: u64) -> io::Result<()> {
        let _ = self.cancel_pending_token(token);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<usize> {
        // The polling backend doesn't batch — ops are processed in completions().
        Ok(0)
    }

    fn completions(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        let start = out.len();

        if self.drain_ready(out) > 0 {
            return Ok(out.len() - start);
        }

        // First pass: attempt all pending operations.
        self.drain_pending(out);
        self.drain_ready(out);

        // Hot path: if we produced completions, return immediately.
        if out.len() > start {
            return Ok(out.len() - start);
        }

        // No completions — poll for readiness with a short timeout so the
        // reactor can tick timers and check shutdown.
        self.events.clear();
        self.poller
            .wait(&mut self.events, Some(std::time::Duration::from_millis(1)))?;

        // Immediately retry pending ops now that readiness has been signalled.
        // This avoids deferring to the next reactor iteration.
        if !self.events.is_empty() {
            self.rearm_ready_ops();
            self.drain_pending(out);
            self.drain_ready(out);
        }

        Ok(out.len() - start)
    }

    fn drain_cq(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        let start = out.len();

        self.drain_ready(out);
        if out.len() == start {
            // Non-blocking progress pass for operations submitted while the
            // reactor was processing completions. This makes polling mode
            // match the reactor fast-path contract without entering poll().
            self.drain_pending(out);
            self.drain_ready(out);
        }

        Ok(out.len() - start)
    }
}

impl PollingBackend {
    fn do_accept(&mut self, listener_fd: RawFd, token: u64, out: &mut Vec<Completion>) {
        // SAFETY: listener_fd is a valid socket fd managed by the reactor.
        let result = unsafe {
            let mut addr: libc::sockaddr_storage = std::mem::zeroed();
            let mut addr_len: libc::socklen_t =
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            libc::accept(
                listener_fd,
                &mut addr as *mut libc::sockaddr_storage as *mut libc::sockaddr,
                &mut addr_len,
            )
        };

        if result >= 0 {
            // Set non-blocking on the new fd.
            // SAFETY: result is a valid fd just returned by accept().
            unsafe {
                let flags = libc::fcntl(result, libc::F_GETFL);
                libc::fcntl(result, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
            out.push(Completion {
                token,
                result: result as i32,
                flags: 0,
            });
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                // Not ready — register listener for read-readiness and re-queue.
                // Edge-triggered: skip if already registered.
                self.ensure_capacity(listener_fd);
                if (self.registered[listener_fd as usize] & INTEREST_READABLE) == 0 {
                    self.register_interest(listener_fd, Event::readable(token as usize));
                }
                self.armed_accept = Some((listener_fd, token));
            } else {
                let errno = err.raw_os_error().unwrap_or(1);
                out.push(Completion {
                    token,
                    result: -errno,
                    flags: 0,
                });
            }
        }
    }

    fn do_read(
        &mut self,
        fd: RawFd,
        buf_ptr: *mut u8,
        buf_len: usize,
        token: u64,
        out: &mut Vec<Completion>,
    ) {
        // Read-loop: drain all available data from the kernel socket buffer
        // in a single pass to reduce syscall overhead. With edge-triggered
        // polling we must drain until EAGAIN anyway, so this is correct.
        let mut total: usize = 0;
        let mut ptr = buf_ptr;
        let mut remaining = buf_len;

        loop {
            // SAFETY: ptr is a valid buffer owned by the reactor (same thread),
            // fd is a valid socket fd.
            let n = unsafe { libc::read(fd, ptr.cast::<libc::c_void>(), remaining) };

            if n > 0 {
                let bytes = n as usize;
                total += bytes;
                // SAFETY: advancing within the same buffer allocation.
                ptr = unsafe { ptr.add(bytes) };
                remaining -= bytes;
                if remaining == 0 {
                    break; // Buffer full — stop reading.
                }
                continue; // More buffer space — try to read more.
            } else if n == 0 {
                // EOF — report any accumulated data first; the next read
                // will observe the EOF again.
                if total > 0 {
                    break;
                }
                out.push(Completion {
                    token,
                    result: 0,
                    flags: 0,
                });
                return;
            } else {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    break; // Kernel buffer drained.
                }
                // Real error — report accumulated data first if any.
                if total > 0 {
                    break;
                }
                let errno = err.raw_os_error().unwrap_or(1);
                out.push(Completion {
                    token,
                    result: -errno,
                    flags: 0,
                });
                return;
            }
        }

        if total > 0 {
            out.push(Completion {
                token,
                result: total as i32,
                flags: 0,
            });
        } else {
            // EAGAIN with no data — register for readiness and re-queue.
            // Edge-triggered: only register if not already registered for
            // readable. With PollMode::Edge (EV_CLEAR), the interest
            // persists across events, so re-registration is unnecessary
            // and would waste a kevent/epoll_ctl syscall.
            self.ensure_capacity(fd);
            if (self.registered[fd as usize] & INTEREST_READABLE) == 0 {
                self.register_interest(fd, Event::readable(token as usize));
            }
            let (conn_id, _, op) = decode_token(token);
            debug_assert!(matches!(op, OpType::Read));
            self.ensure_read_capacity(conn_id);
            self.armed_reads[conn_id] = Some(ArmedRead {
                fd,
                buf_ptr,
                buf_len,
                token,
            });
        }
    }

    fn do_write(
        &mut self,
        fd: RawFd,
        buf_ptr: *const u8,
        buf_len: usize,
        token: u64,
        out: &mut Vec<Completion>,
    ) {
        // SAFETY: buf_ptr is a valid buffer owned by the reactor (same thread),
        // fd is a valid socket fd.
        let n = unsafe { libc::write(fd, buf_ptr.cast::<libc::c_void>(), buf_len) };

        if n >= 0 {
            out.push(Completion {
                token,
                result: n as i32,
                flags: 0,
            });
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                // Register for write-readiness instead of busy-polling.
                // Note: this replaces readable interest via the polling crate's
                // modify semantics; the readable flag is cleared so the next
                // read EAGAIN will re-register it.
                self.register_interest(fd, Event::writable(token as usize));
                let (conn_id, _, _) = decode_token(token);
                self.ensure_write_capacity(conn_id);
                self.armed_writes[conn_id] = Some(ArmedWrite::Write {
                    fd,
                    buf_ptr,
                    buf_len,
                    token,
                });
                return;
            }
            let errno = err.raw_os_error().unwrap_or(1);
            out.push(Completion {
                token,
                result: -errno,
                flags: 0,
            });
        }
    }

    fn do_writev(
        &mut self,
        fd: RawFd,
        iovecs: *const libc::iovec,
        iov_count: usize,
        token: u64,
        out: &mut Vec<Completion>,
    ) {
        // SAFETY: iovecs points to a valid array of iov_count iovec structs
        // on the reactor thread. fd is a valid socket fd.
        let n = unsafe { libc::writev(fd, iovecs, iov_count as libc::c_int) };

        if n >= 0 {
            out.push(Completion {
                token,
                result: n as i32,
                flags: 0,
            });
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                // Register for write-readiness instead of busy-polling.
                self.register_interest(fd, Event::writable(token as usize));
                let (conn_id, _, _) = decode_token(token);
                self.ensure_write_capacity(conn_id);
                self.armed_writes[conn_id] = Some(ArmedWrite::Writev {
                    fd,
                    iovecs,
                    iov_count,
                    token,
                });
                return;
            }
            let errno = err.raw_os_error().unwrap_or(1);
            out.push(Completion {
                token,
                result: -errno,
                flags: 0,
            });
        }
    }

    fn do_close(&mut self, fd: RawFd, token: u64, out: &mut Vec<Completion>) {
        let (conn_id, _, _) = decode_token(token);
        if conn_id < self.armed_reads.len() {
            if let Some(read) = self.armed_reads[conn_id].take() {
                self.push_canceled_completion(read.token);
            }
        }
        self.cancel_pending_fd(fd);

        // Remove from poller if registered.
        let fd_idx = fd as usize;
        if fd_idx < self.registered.len() && self.registered[fd_idx] != 0 {
            // SAFETY: fd was previously registered with the poller.
            let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
            let _ = self.poller.delete(borrowed);
            self.registered[fd_idx] = 0;
        }

        // SAFETY: fd is a valid socket fd owned by the reactor.
        let result = unsafe { libc::close(fd) };

        out.push(Completion {
            token,
            result,
            flags: 0,
        });
    }
}

#[inline]
fn pending_op_token(op: &PendingOp) -> u64 {
    match op {
        PendingOp::Accept { token, .. }
        | PendingOp::Read { token, .. }
        | PendingOp::Write { token, .. }
        | PendingOp::Writev { token, .. }
        | PendingOp::Close { token, .. } => *token,
    }
}

#[inline]
fn pending_op_fd(op: &PendingOp) -> Option<RawFd> {
    match op {
        PendingOp::Read { fd, .. }
        | PendingOp::Write { fd, .. }
        | PendingOp::Writev { fd, .. }
        | PendingOp::Close { fd, .. } => Some(*fd),
        PendingOp::Accept { .. } => None,
    }
}

#[inline]
fn armed_write_token(write: &ArmedWrite) -> u64 {
    match write {
        ArmedWrite::Write { token, .. } | ArmedWrite::Writev { token, .. } => *token,
    }
}

#[inline]
fn armed_write_fd(write: &ArmedWrite) -> RawFd {
    match write {
        ArmedWrite::Write { fd, .. } | ArmedWrite::Writev { fd, .. } => *fd,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::encode_token;

    #[test]
    fn cancel_pending_writev_yields_ecanceled_completion() {
        let mut backend = PollingBackend::new().unwrap();
        let token = encode_token(3, 9, OpType::Writev);
        let bytes = b"hello";
        let iovecs = [libc::iovec {
            iov_base: bytes.as_ptr() as *mut libc::c_void,
            iov_len: bytes.len(),
        }];

        backend
            .submit_writev(99, iovecs.as_ptr(), iovecs.len(), token)
            .unwrap();
        backend.submit_cancel(token).unwrap();

        let mut out = Vec::new();
        assert_eq!(backend.drain_cq(&mut out).unwrap(), 1);
        assert_eq!(out[0].token, token);
        assert_eq!(out[0].result, -libc::ECANCELED);
        assert!(backend.pending.is_empty());
    }

    #[test]
    fn cancel_armed_write_yields_ecanceled_completion() {
        let mut backend = PollingBackend::new().unwrap();
        let token = encode_token(3, 9, OpType::Write);
        let bytes = b"hello";

        backend.ensure_write_capacity(3);
        backend.armed_writes[3] = Some(ArmedWrite::Write {
            fd: 99,
            buf_ptr: bytes.as_ptr(),
            buf_len: bytes.len(),
            token,
        });

        let mut out = Vec::new();
        assert_eq!(backend.drain_cq(&mut out).unwrap(), 0);
        assert!(backend.armed_writes[3].is_some());

        backend.submit_cancel(token).unwrap();
        assert_eq!(backend.drain_cq(&mut out).unwrap(), 1);
        assert_eq!(out[0].token, token);
        assert_eq!(out[0].result, -libc::ECANCELED);
        assert!(backend.armed_writes[3].is_none());
    }

    #[test]
    fn accept_eagain_arms_listener_without_busy_pending_retry() {
        use std::os::fd::IntoRawFd;

        let mut backend = PollingBackend::new().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let fd = listener.into_raw_fd();
        let token = encode_token(crate::backend::ACCEPT_CONN_ID, 0, OpType::Accept);

        let mut out = Vec::new();
        backend.do_accept(fd, token, &mut out);

        assert!(out.is_empty());
        assert!(backend.pending.is_empty());
        assert_eq!(backend.armed_accept, Some((fd, token)));

        if fd as usize >= backend.registered.len() || backend.registered[fd as usize] != 0 {
            // SAFETY: fd was registered by this test's backend.
            let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
            let _ = backend.poller.delete(borrowed);
        }
        // SAFETY: fd was obtained from into_raw_fd above and is still open.
        unsafe {
            libc::close(fd);
        }
    }

    #[test]
    fn close_purges_pending_fd_ops_and_armed_reads() {
        let mut backend = PollingBackend::new().unwrap();
        let mut fds = [0; 2];
        // SAFETY: `pipe` initialises both fds on success.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(rc, 0);
        let fd = fds[0];
        let peer_fd = fds[1];

        let read_token = encode_token(1, 5, OpType::Read);
        let write_token = encode_token(1, 5, OpType::Write);
        let writev_token = encode_token(1, 5, OpType::Writev);
        let armed_write_token = encode_token(2, 5, OpType::Write);
        let close_token = encode_token(1, 5, OpType::Close);

        let write_bytes = [0u8; 8];
        let writev_bytes = [1u8; 8];
        let iovecs = [libc::iovec {
            iov_base: writev_bytes.as_ptr() as *mut libc::c_void,
            iov_len: writev_bytes.len(),
        }];

        backend.pending.push_back(PendingOp::Write {
            fd,
            buf_ptr: write_bytes.as_ptr(),
            buf_len: write_bytes.len(),
            token: write_token,
        });
        backend.pending.push_back(PendingOp::Writev {
            fd,
            iovecs: iovecs.as_ptr(),
            iov_count: iovecs.len(),
            token: writev_token,
        });
        backend.ensure_read_capacity(1);
        backend.armed_reads[1] = Some(ArmedRead {
            fd,
            buf_ptr: std::ptr::null_mut(),
            buf_len: 0,
            token: read_token,
        });
        backend.ensure_write_capacity(2);
        backend.armed_writes[2] = Some(ArmedWrite::Write {
            fd,
            buf_ptr: write_bytes.as_ptr(),
            buf_len: write_bytes.len(),
            token: armed_write_token,
        });

        let mut close_out = Vec::new();
        backend.do_close(fd, close_token, &mut close_out);
        assert_eq!(close_out.len(), 1);
        assert_eq!(close_out[0].token, close_token);

        let mut canceled = Vec::new();
        assert_eq!(backend.drain_cq(&mut canceled).unwrap(), 4);
        let canceled_tokens: Vec<u64> = canceled.into_iter().map(|c| c.token).collect();
        assert_eq!(
            canceled_tokens,
            vec![read_token, write_token, writev_token, armed_write_token]
        );
        assert!(backend.pending.is_empty());
        assert!(backend.armed_reads[1].is_none());
        assert!(backend.armed_writes[2].is_none());

        // SAFETY: `peer_fd` is the other end of the pipe opened by this test.
        unsafe {
            libc::close(peer_fd);
        }
    }
}
