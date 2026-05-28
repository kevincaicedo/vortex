//! Linux io_uring backend.
//!
//! Uses the `io-uring` crate for zero-syscall I/O via submission/completion
//! queues. Supports `IORING_SETUP_SQPOLL` behind the `sqpoll` feature flag.
//!
//! This module is only compiled on Linux (`#[cfg(target_os = "linux")]`).

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use io_uring::types::{SubmitArgs, Timespec};
use io_uring::{IoUring, opcode, types::Fd};

use super::{
    BackendDriver, BackendQueueStatus, Completion, CompletionToken, ConnFd, IovecBatch, ListenerFd,
    ReadLease, SubmitError, WriteLease, preserve_reaped_completion_count,
};

/// io_uring-based I/O backend for Linux.
pub struct IoUringBackend {
    ring: IoUring,
    fixed_buffers_registered: AtomicBool,
    sqpoll_enabled: bool,
    last_cq_overflow: u32,
}

impl IoUringBackend {
    /// Creates a new io_uring backend.
    ///
    /// `ring_size` is the number of SQEs (must be power of two).
    /// If `sqpoll` feature is enabled and `sqpoll_idle_ms > 0`, enables SQPOLL mode.
    pub fn new(ring_size: u32, sqpoll_idle_ms: u32) -> io::Result<Self> {
        #[cfg(not(feature = "sqpoll"))]
        let _ = sqpoll_idle_ms;

        #[cfg(feature = "sqpoll")]
        let mut builder = IoUring::builder();
        #[cfg(not(feature = "sqpoll"))]
        let builder = IoUring::builder();

        #[cfg(feature = "sqpoll")]
        let sqpoll_enabled = sqpoll_idle_ms > 0;
        #[cfg(not(feature = "sqpoll"))]
        let sqpoll_enabled = false;

        #[cfg(feature = "sqpoll")]
        if sqpoll_idle_ms > 0 {
            builder.setup_sqpoll(sqpoll_idle_ms);
        }

        let ring = builder.build(ring_size)?;
        Ok(Self {
            ring,
            fixed_buffers_registered: AtomicBool::new(false),
            sqpoll_enabled,
            last_cq_overflow: 0,
        })
    }

    #[inline]
    pub(crate) const fn sqpoll_enabled(&self) -> bool {
        self.sqpoll_enabled
    }
}

impl BackendDriver for IoUringBackend {
    fn submit_accept(
        &mut self,
        listener_fd: ListenerFd,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        let accept = opcode::Accept::new(
            Fd(listener_fd.raw()),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
        .flags(libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC)
        .build()
        .user_data(token.raw());

        // SAFETY: The SQE is valid, listener_fd is a bound listening socket,
        // and accepted descriptors inherit nonblocking/CLOEXEC atomically.
        unsafe {
            self.ring
                .submission()
                .push(&accept)
                .map_err(|_| SubmitError::QueueFull)?;
        }
        Ok(())
    }

    fn submit_read(&mut self, lease: ReadLease, token: CompletionToken) -> Result<(), SubmitError> {
        if self.fixed_buffers_registered.load(Ordering::Relaxed) {
            if let Some(buf_index) = lease.fixed() {
                let read = opcode::ReadFixed::new(
                    Fd(lease.fd().raw()),
                    lease.ptr(),
                    lease.len() as u32,
                    buf_index.raw(),
                )
                .build()
                .user_data(token.raw());

                // SAFETY: The SQE is valid, fd is an open socket, and the
                // typed lease points to a registered fixed buffer that remains
                // valid until the CQE is reaped.
                unsafe {
                    self.ring
                        .submission()
                        .push(&read)
                        .map_err(|_| SubmitError::QueueFull)?;
                }
                return Ok(());
            }
        }

        let read = opcode::Read::new(Fd(lease.fd().raw()), lease.ptr(), lease.len() as u32)
            .build()
            .user_data(token.raw());

        // SAFETY: The SQE is valid, fd is an open socket, and the typed lease
        // points to reactor-owned memory that remains valid until the CQE is
        // reaped.
        unsafe {
            self.ring
                .submission()
                .push(&read)
                .map_err(|_| SubmitError::QueueFull)?;
        }
        Ok(())
    }

    fn submit_write(
        &mut self,
        lease: WriteLease,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        if self.fixed_buffers_registered.load(Ordering::Relaxed) {
            if let Some(buf_index) = lease.fixed() {
                let write = opcode::WriteFixed::new(
                    Fd(lease.fd().raw()),
                    lease.ptr(),
                    lease.len() as u32,
                    buf_index.raw(),
                )
                .build()
                .user_data(token.raw());

                // SAFETY: Valid SQE, fd is open, and the typed lease points to
                // a registered fixed buffer identified by buf_index.
                unsafe {
                    self.ring
                        .submission()
                        .push(&write)
                        .map_err(|_| SubmitError::QueueFull)?;
                }
                return Ok(());
            }
        }

        let write = opcode::Write::new(Fd(lease.fd().raw()), lease.ptr(), lease.len() as u32)
            .build()
            .user_data(token.raw());

        // SAFETY: Valid SQE, fd is open, and the typed lease points to memory
        // that remains valid until the CQE is reaped.
        unsafe {
            self.ring
                .submission()
                .push(&write)
                .map_err(|_| SubmitError::QueueFull)?;
        }
        Ok(())
    }

    fn submit_close(&mut self, fd: ConnFd, token: CompletionToken) -> Result<(), SubmitError> {
        let close = opcode::Close::new(Fd(fd.raw()))
            .build()
            .user_data(token.raw());

        // SAFETY: Valid SQE, fd is an open file descriptor.
        unsafe {
            self.ring
                .submission()
                .push(&close)
                .map_err(|_| SubmitError::QueueFull)?;
        }
        Ok(())
    }

    fn submit_cancel(
        &mut self,
        target: CompletionToken,
        cancel_token: CompletionToken,
    ) -> Result<(), SubmitError> {
        let cancel = opcode::AsyncCancel::new(target.raw())
            .build()
            .user_data(cancel_token.raw());

        // SAFETY: Valid SQE. AsyncCancel targets the in-flight SQE with
        // the matching user_data token. The cancel SQE itself carries a
        // distinct token so its completion cannot decode as the target op.
        unsafe {
            self.ring
                .submission()
                .push(&cancel)
                .map_err(|_| SubmitError::QueueFull)?;
        }
        Ok(())
    }

    fn submit_writev(
        &mut self,
        batch: IovecBatch,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        let writev = opcode::Writev::new(
            Fd(batch.fd().raw()),
            batch.ptr().cast(),
            batch.count() as u32,
        )
        .build()
        .user_data(token.raw());

        // SAFETY: Valid SQE, fd is an open socket, and the typed batch points
        // to valid iovec structs whose backing memory remains valid until the
        // CQE is reaped.
        unsafe {
            self.ring
                .submission()
                .push(&writev)
                .map_err(|_| SubmitError::QueueFull)?;
        }
        Ok(())
    }

    fn register_buffers(&self, iovecs: &[libc::iovec]) -> Result<(), SubmitError> {
        // SAFETY: iovecs point to valid, pinned memory that will outlive the
        // io_uring instance. The buffers are owned by the BufferPool.
        let result = unsafe {
            self.ring
                .submitter()
                .register_buffers(iovecs)
                .map_err(|error| SubmitError::Backend(io::Error::other(error)))
        };

        self.fixed_buffers_registered
            .store(result.is_ok(), Ordering::Relaxed);
        result
    }

    fn flush(&mut self) -> Result<usize, SubmitError> {
        if self.sqpoll_enabled {
            // SQPOLL mode: call submit() to sync the SQ tail pointer and
            // wake the kernel poll thread if it has gone idle. Without this,
            // SQEs pushed after the thread parks would never be seen.
            let n = self.ring.submit().map_err(SubmitError::Backend)?;
            return Ok(n);
        }

        // Non-SQPOLL: submission is deferred to completions() so both submit
        // and wait happen in a single io_uring_enter syscall.
        Ok(0)
    }

    fn completions(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        let start = out.len();

        if self.sqpoll_enabled {
            // SQPOLL mode: the kernel thread submits SQEs for us (we
            // already synced the SQ in flush()). Drain completions with
            // adaptive backoff: spin → yield → park.
            let mut backoff = 0u32;
            loop {
                let cq = self.ring.completion();
                let mut got_any = false;
                for cqe in cq {
                    out.push(Completion {
                        token: CompletionToken::from_raw(cqe.user_data()),
                        result: cqe.result(),
                        flags: cqe.flags(),
                    });
                    got_any = true;
                }
                if got_any || backoff >= 120 {
                    break;
                }
                if backoff < 10 {
                    std::hint::spin_loop();
                } else if backoff < 110 {
                    std::thread::yield_now();
                } else {
                    std::thread::park_timeout(std::time::Duration::from_micros(1));
                }
                backoff += 1;
            }
            return Ok(out.len() - start);
        }

        // Combined submit + wait: submit_with_args atomically submits all
        // pending SQEs and waits for at least 1 CQE with a timeout.
        let timeout = Timespec::from(Duration::from_millis(1));
        let args = SubmitArgs::new().timespec(&timeout);
        if let Err(error) = self.ring.submitter().submit_with_args(1, &args) {
            // ETIME means the bounded wait produced no CQE; it is an empty
            // completion drain, not a failed submission.
            if error.raw_os_error() != Some(libc::ETIME) {
                return Err(error);
            }
        }

        let cq = self.ring.completion();
        for cqe in cq {
            out.push(Completion {
                token: CompletionToken::from_raw(cqe.user_data()),
                result: cqe.result(),
                flags: cqe.flags(),
            });
        }

        Ok(out.len() - start)
    }

    fn drain_cq(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        let start = out.len();

        // Non-blocking CQ peek — no syscall. Just read whatever the kernel
        // has already placed in the completion ring.
        let cq = self.ring.completion();
        for cqe in cq {
            out.push(Completion {
                token: CompletionToken::from_raw(cqe.user_data()),
                result: cqe.result(),
                flags: cqe.flags(),
            });
        }

        // If we got completions, also submit any pending SQEs that were
        // queued during processing (e.g. write SQEs from handle_read).
        if out.len() > start {
            // Non-SQPOLL explicitly submits pending SQEs. SQPOLL also needs
            // a tail sync to wake the kernel poll thread if it has gone idle.
            return preserve_reaped_completion_count(start, out.len(), self.ring.submit());
        }

        Ok(0)
    }

    fn queue_status(&mut self) -> BackendQueueStatus {
        let (sq_occupancy, sq_capacity) = {
            let sq = self.ring.submission();
            (sq.len() as u64, sq.capacity() as u64)
        };
        let (cq_occupancy, cq_capacity, cq_overflow) = {
            let cq = self.ring.completion();
            (cq.len() as u64, cq.capacity() as u64, cq.overflow())
        };
        let cq_overflow_delta = cq_overflow.wrapping_sub(self.last_cq_overflow) as u64;
        self.last_cq_overflow = cq_overflow;

        BackendQueueStatus {
            sq_occupancy,
            sq_capacity,
            cq_occupancy,
            cq_capacity,
            cq_overflow_delta,
        }
    }
}
