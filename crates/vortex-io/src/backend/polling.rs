//! Cross-platform polling backend using the `polling` crate.
//!
//! Wraps epoll (Linux), kqueue (macOS/BSD). Emulates
//! io_uring's completion-based model by performing I/O inline on poll events
//! and generating synthetic [`Completion`] structs.
//!
//! ## Optimisation
//!
//! * **Write-readiness registration** – on `EAGAIN` from `write`/`writev`,
//!   the fd is registered for write-readiness with the poller instead of
//!   blindly re-trying every iteration (busy-poll elimination).
//! * **Immediate retry after poll** – when `poller.wait()` returns readiness
//!   events, all pending ops are retried in the *same* `completions()` call
//!   instead of deferring to the next reactor iteration.
//! * **O(1) fd tracking** – registered fds are kept in a `HashSet` instead
//!   of a `Vec`, eliminating linear scans on every `EAGAIN`.
//! * **1 ms poll timeout** – matches io_uring's wait strategy and keeps tail
//!   latency low during idle periods.

use std::collections::{HashSet, VecDeque};
use std::io;
use std::os::fd::{BorrowedFd, RawFd};

use polling::{Event, Events, Poller};

use super::{Completion, IoBackend};

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

/// Cross-platform I/O backend using the `polling` crate.
pub struct PollingBackend {
    poller: Poller,
    events: Events,
    pending: VecDeque<PendingOp>,
    /// File descriptors currently registered with the poller (any interest).
    registered: HashSet<RawFd>,
}

impl PollingBackend {
    /// Creates a new polling backend.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            poller: Poller::new()?,
            events: Events::new(),
            pending: VecDeque::with_capacity(256),
            registered: HashSet::with_capacity(64),
        })
    }

    /// Register (or re-arm) interest for `fd` with the poller.
    fn register_interest(&mut self, fd: RawFd, event: Event) {
        // SAFETY: fd is a valid, open file descriptor owned by the reactor.
        let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
        if self.registered.contains(&fd) {
            let _ = self.poller.modify(borrowed, event);
        } else {
            unsafe {
                self.poller.add(&borrowed, event).unwrap_or_else(|_| {
                    let _ = self.poller.modify(borrowed, event);
                });
            }
            self.registered.insert(fd);
        }
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

    fn flush(&mut self) -> io::Result<usize> {
        // The polling backend doesn't batch — ops are processed in completions().
        Ok(0)
    }

    fn try_completions(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        let start = out.len();

        self.drain_pending(out);
        if out.len() > start {
            return Ok(out.len() - start);
        }

        self.events.clear();
        self.poller
            .wait(&mut self.events, Some(std::time::Duration::ZERO))?;
        if !self.events.is_empty() {
            self.drain_pending(out);
        }

        Ok(out.len() - start)
    }

    fn completions(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        let start = out.len();

        // First pass: attempt all pending operations.
        self.drain_pending(out);

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
            self.drain_pending(out);
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
                self.register_interest(listener_fd, Event::readable(token as usize));
                self.pending
                    .push_back(PendingOp::Accept { listener_fd, token });
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
        // SAFETY: buf_ptr is a valid buffer owned by the reactor (same thread),
        // fd is a valid socket fd.
        let n = unsafe { libc::read(fd, buf_ptr.cast::<libc::c_void>(), buf_len) };

        if n > 0 {
            out.push(Completion {
                token,
                result: n as i32,
                flags: 0,
            });
        } else if n == 0 {
            // EOF
            out.push(Completion {
                token,
                result: 0,
                flags: 0,
            });
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                // Not ready — register for read-readiness and retry later.
                self.register_interest(fd, Event::readable(token as usize));
                self.pending.push_back(PendingOp::Read {
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
                self.register_interest(fd, Event::writable(token as usize));
                self.pending.push_back(PendingOp::Write {
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
                self.pending.push_back(PendingOp::Writev {
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
        // Remove from poller if registered.
        if self.registered.remove(&fd) {
            // SAFETY: fd was previously registered with the poller.
            let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
            let _ = self.poller.delete(borrowed);
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
