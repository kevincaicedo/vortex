//! Linux io_uring backend.
//!
//! Uses the `io-uring` crate for zero-syscall I/O via submission/completion
//! queues. Supports `IORING_SETUP_SQPOLL` behind the `sqpoll` feature flag.
//!
//! This module is only compiled on Linux (`#[cfg(target_os = "linux")]`).

use std::io;
use std::os::fd::RawFd;

#[cfg(not(feature = "sqpoll"))]
use std::time::Duration;

use io_uring::{IoUring, opcode, types::Fd};

#[cfg(not(feature = "sqpoll"))]
use io_uring::types::{SubmitArgs, Timespec};

use super::{Completion, IoBackend};

/// io_uring-based I/O backend for Linux.
pub struct IoUringBackend {
    ring: IoUring,
}

impl IoUringBackend {
    /// Creates a new io_uring backend.
    ///
    /// `ring_size` is the number of SQEs (must be power of two).
    /// If `sqpoll` feature is enabled and `sqpoll_idle_ms > 0`, enables SQPOLL mode.
    pub fn new(ring_size: u32, _sqpoll_idle_ms: u32) -> io::Result<Self> {
        #[cfg(feature = "sqpoll")]
        let mut builder = IoUring::builder();
        #[cfg(not(feature = "sqpoll"))]
        let builder = IoUring::builder();

        #[cfg(feature = "sqpoll")]
        if _sqpoll_idle_ms > 0 {
            builder.setup_sqpoll(_sqpoll_idle_ms);
        }

        let ring = builder.build(ring_size)?;
        Ok(Self { ring })
    }
}

impl IoBackend for IoUringBackend {
    fn submit_accept(&mut self, listener_fd: RawFd, token: u64) -> io::Result<()> {
        let accept =
            opcode::Accept::new(Fd(listener_fd), std::ptr::null_mut(), std::ptr::null_mut())
                .build()
                .user_data(token);

        // SAFETY: The SQE is valid and the listener_fd is a bound, listening socket.
        unsafe {
            self.ring
                .submission()
                .push(&accept)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn submit_read(
        &mut self,
        fd: RawFd,
        buf_ptr: *mut u8,
        buf_len: usize,
        token: u64,
    ) -> io::Result<()> {
        let read = opcode::Read::new(Fd(fd), buf_ptr, buf_len as u32)
            .build()
            .user_data(token);

        // SAFETY: The SQE is valid, fd is an open socket, and buf_ptr points to
        // a buffer that will remain valid until the CQE is reaped.
        unsafe {
            self.ring
                .submission()
                .push(&read)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn submit_write(
        &mut self,
        fd: RawFd,
        buf_ptr: *const u8,
        buf_len: usize,
        token: u64,
    ) -> io::Result<()> {
        let write = opcode::Write::new(Fd(fd), buf_ptr, buf_len as u32)
            .build()
            .user_data(token);

        // SAFETY: Valid SQE, fd is open, buf_ptr is valid for the duration.
        unsafe {
            self.ring
                .submission()
                .push(&write)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn submit_close(&mut self, fd: RawFd, token: u64) -> io::Result<()> {
        let close = opcode::Close::new(Fd(fd)).build().user_data(token);

        // SAFETY: Valid SQE, fd is an open file descriptor.
        unsafe {
            self.ring
                .submission()
                .push(&close)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn submit_read_fixed(
        &mut self,
        fd: RawFd,
        buf_ptr: *mut u8,
        buf_len: usize,
        buf_index: u16,
        token: u64,
    ) -> io::Result<()> {
        let read = opcode::ReadFixed::new(Fd(fd), buf_ptr, buf_len as u32, buf_index)
            .build()
            .user_data(token);

        // SAFETY: The SQE is valid, fd is an open socket, and buf_ptr points
        // to a registered fixed buffer that will remain valid until the CQE is
        // reaped. buf_index identifies the registered buffer region.
        unsafe {
            self.ring
                .submission()
                .push(&read)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn submit_write_fixed(
        &mut self,
        fd: RawFd,
        buf_ptr: *const u8,
        buf_len: usize,
        buf_index: u16,
        token: u64,
    ) -> io::Result<()> {
        let write = opcode::WriteFixed::new(Fd(fd), buf_ptr, buf_len as u32, buf_index)
            .build()
            .user_data(token);

        // SAFETY: Valid SQE, fd is open, buf_ptr is within a registered fixed
        // buffer identified by buf_index.
        unsafe {
            self.ring
                .submission()
                .push(&write)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn submit_cancel(&mut self, token: u64) -> io::Result<()> {
        let cancel = opcode::AsyncCancel::new(token).build();

        // SAFETY: Valid SQE. AsyncCancel targets the in-flight SQE with
        // the matching user_data token.
        unsafe {
            self.ring
                .submission()
                .push(&cancel)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn submit_writev(
        &mut self,
        fd: RawFd,
        iovecs: *const libc::iovec,
        iov_count: usize,
        token: u64,
    ) -> io::Result<()> {
        let writev = opcode::Writev::new(Fd(fd), iovecs.cast(), iov_count as u32)
            .build()
            .user_data(token);

        // SAFETY: Valid SQE, fd is an open socket, iovecs points to a valid
        // array of iov_count iovec structs whose backing memory will remain
        // valid until the CQE is reaped.
        unsafe {
            self.ring
                .submission()
                .push(&writev)
                .map_err(|_| io::Error::other("SQ full"))?;
        }
        Ok(())
    }

    fn register_buffers(&self, iovecs: &[libc::iovec]) -> io::Result<()> {
        // SAFETY: iovecs point to valid, pinned memory that will outlive the
        // io_uring instance. The buffers are owned by the BufferPool.
        unsafe {
            self.ring
                .submitter()
                .register_buffers(iovecs)
                .map_err(io::Error::other)
        }
    }

    fn flush(&mut self) -> io::Result<usize> {
        #[cfg(feature = "sqpoll")]
        {
            // SQPOLL mode: call submit() to sync the SQ tail pointer and
            // wake the kernel poll thread if it has gone idle. Without this,
            // SQEs pushed after the thread parks would never be seen.
            let n = self.ring.submit()?;
            Ok(n)
        }
        #[cfg(not(feature = "sqpoll"))]
        {
            // Non-SQPOLL: submission is deferred to completions() so both
            // submit and wait happen in a single io_uring_enter syscall.
            Ok(0)
        }
    }

    fn completions(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        let start = out.len();

        #[cfg(feature = "sqpoll")]
        {
            // SQPOLL mode: the kernel thread submits SQEs for us (we
            // already synced the SQ in flush()). Drain completions with
            // adaptive backoff: spin → yield → park.
            let mut backoff = 0u32;
            loop {
                let cq = self.ring.completion();
                let mut got_any = false;
                for cqe in cq {
                    out.push(Completion {
                        token: cqe.user_data(),
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
        }

        #[cfg(not(feature = "sqpoll"))]
        {
            // Combined submit + wait: submit_with_args atomically submits
            // all pending SQEs and waits for at least 1 CQE with a timeout.
            // This replaces the previous pattern of ring.submit() in flush()
            // followed by submit_with_args(1, ...) in completions() — two
            // io_uring_enter syscalls per iteration reduced to one.
            let timeout = Timespec::from(Duration::from_millis(1));
            let args = SubmitArgs::new().timespec(&timeout);
            match self.ring.submitter().submit_with_args(1, &args) {
                Ok(_) | Err(_) => {}
            }

            let cq = self.ring.completion();
            for cqe in cq {
                out.push(Completion {
                    token: cqe.user_data(),
                    result: cqe.result(),
                    flags: cqe.flags(),
                });
            }
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
                token: cqe.user_data(),
                result: cqe.result(),
                flags: cqe.flags(),
            });
        }

        // If we got completions, also submit any pending SQEs that were
        // queued during processing (e.g. write SQEs from handle_read).
        #[cfg(not(feature = "sqpoll"))]
        if out.len() > start {
            // Non-SQPOLL: explicit submit needed.
            let _ = self.ring.submit()?;
        }
        #[cfg(feature = "sqpoll")]
        if out.len() > start {
            // SQPOLL: sync SQ tail to wake kernel thread if needed.
            let _ = self.ring.submit()?;
        }

        Ok(out.len() - start)
    }
}
