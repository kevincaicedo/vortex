//! Cross-platform polling backend using the `polling` crate.
//!
//! Wraps epoll (Linux), kqueue (macOS/BSD). Emulates
//! io_uring's completion-based model by performing I/O inline on poll events
//! and generating synthetic [`Completion`] structs.

use std::collections::VecDeque;
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
    /// File descriptors registered with the poller for read interest.
    registered_read: Vec<RawFd>,
}

impl PollingBackend {
    /// Creates a new polling backend.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            poller: Poller::new()?,
            events: Events::new(),
            pending: VecDeque::with_capacity(256),
            registered_read: Vec::new(),
        })
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

    fn flush(&mut self) -> io::Result<usize> {
        // The polling backend doesn't batch — ops are processed in completions().
        Ok(0)
    }

    fn completions(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        let start = out.len();

        // Process only the operations that were pending at the start of this
        // call. Operations re-queued during processing (e.g., on EAGAIN) will
        // be picked up on the next call after the poller signals readiness.
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
                    PendingOp::Close { fd, token } => {
                        self.do_close(fd, token, out);
                    }
                }
            }
        }

        // If no completions yet, poll for events with a short timeout so the
        // reactor can tick timers and check shutdown.
        if out.len() == start {
            self.events.clear();
            self.poller
                .wait(&mut self.events, Some(std::time::Duration::from_millis(10)))?;

            // Events indicate readiness, not completion. The re-queued pending
            // ops (from EAGAIN paths) will pick up the ready I/O next iteration.
            // No synthetic completions are generated from events.
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
                // Not ready — register listener with poller and re-queue.
                let is_new = !self.registered_read.contains(&listener_fd);
                let borrowed = unsafe { BorrowedFd::borrow_raw(listener_fd) };
                if is_new {
                    unsafe {
                        self.poller
                            .add(&borrowed, Event::readable(token as usize))
                            .unwrap_or_else(|_| {
                                let _ = self
                                    .poller
                                    .modify(borrowed, Event::readable(token as usize));
                            });
                    }
                    self.registered_read.push(listener_fd);
                } else {
                    let _ = self
                        .poller
                        .modify(borrowed, Event::readable(token as usize));
                }
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
                // Not ready yet — register with poller and retry later.
                let is_new = !self.registered_read.contains(&fd);
                // SAFETY: fd is a valid, open file descriptor owned by the reactor.
                let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
                if is_new {
                    unsafe {
                        self.poller
                            .add(&borrowed, Event::readable(token as usize))
                            .unwrap_or_else(|_| {
                                let _ = self
                                    .poller
                                    .modify(borrowed, Event::readable(token as usize));
                            });
                    }
                    self.registered_read.push(fd);
                } else {
                    let _ = self
                        .poller
                        .modify(borrowed, Event::readable(token as usize));
                }
                // Re-queue the read for when the fd becomes readable.
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
                // Re-queue for later.
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

    fn do_close(&mut self, fd: RawFd, token: u64, out: &mut Vec<Completion>) {
        // Remove from poller if registered.
        if let Some(pos) = self.registered_read.iter().position(|&f| f == fd) {
            // SAFETY: fd was previously registered with the poller.
            let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
            let _ = self.poller.delete(borrowed);
            self.registered_read.swap_remove(pos);
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
