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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CqOverflowCounter(u32);

impl CqOverflowCounter {
    #[inline]
    const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline]
    fn delta_since(self, previous: Self) -> u64 {
        self.0.wrapping_sub(previous.0) as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObservedQueueStatus {
    sq_occupancy: u64,
    sq_capacity: u64,
    cq_occupancy: u64,
    cq_capacity: u64,
    cq_overflow: CqOverflowCounter,
}

impl ObservedQueueStatus {
    #[inline]
    const fn new(
        sq_occupancy: u64,
        sq_capacity: u64,
        cq_occupancy: u64,
        cq_capacity: u64,
        cq_overflow: u32,
    ) -> Self {
        Self {
            sq_occupancy,
            sq_capacity,
            cq_occupancy,
            cq_capacity,
            cq_overflow: CqOverflowCounter::new(cq_overflow),
        }
    }
}

#[derive(Debug, Default)]
struct CqOverflowTracker {
    last: CqOverflowCounter,
}

impl CqOverflowTracker {
    #[inline]
    fn queue_status(&mut self, observed: ObservedQueueStatus) -> BackendQueueStatus {
        let cq_overflow_delta = observed.cq_overflow.delta_since(self.last);
        self.last = observed.cq_overflow;

        BackendQueueStatus {
            sq_occupancy: observed.sq_occupancy,
            sq_capacity: observed.sq_capacity,
            cq_occupancy: observed.cq_occupancy,
            cq_capacity: observed.cq_capacity,
            cq_overflow_delta,
        }
    }
}

/// io_uring-based I/O backend for Linux.
pub struct IoUringBackend {
    ring: IoUring,
    fixed_buffers_registered: AtomicBool,
    sqpoll_enabled: bool,
    cq_overflow: CqOverflowTracker,
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
            cq_overflow: CqOverflowTracker::default(),
        })
    }

    #[inline]
    pub(crate) const fn sqpoll_enabled(&self) -> bool {
        self.sqpoll_enabled
    }

    #[inline]
    fn queue_status_from_observed(&mut self, observed: ObservedQueueStatus) -> BackendQueueStatus {
        self.cq_overflow.queue_status(observed)
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

        self.queue_status_from_observed(ObservedQueueStatus::new(
            sq_occupancy,
            sq_capacity,
            cq_occupancy,
            cq_capacity,
            cq_overflow,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::time::{Duration, Instant};

    use crate::backend::{
        BackendDriver, Completion, CompletionToken, ConnFd, DecodedCompletionToken, IovecBatch,
        OpType, ReadLease, SubmitError,
    };

    fn native_uring_required() -> bool {
        std::env::var_os("VORTEX_REQUIRE_IO_URING_TESTS").is_some()
    }

    fn skippable_uring_error(error: &io::Error) -> bool {
        matches!(
            error.kind(),
            io::ErrorKind::Unsupported | io::ErrorKind::PermissionDenied
        ) || error.to_string().contains("Operation not permitted")
    }

    fn new_test_backend(ring_size: u32) -> Option<IoUringBackend> {
        match IoUringBackend::new(ring_size, 0) {
            Ok(backend) => Some(backend),
            Err(error) if skippable_uring_error(&error) && !native_uring_required() => {
                eprintln!("skipping native io_uring backend test: {error}");
                None
            }
            Err(error) => panic!("native io_uring backend startup failed: {error}"),
        }
    }

    fn drain_until_tokens(
        backend: &mut IoUringBackend,
        expected: &[CompletionToken],
    ) -> Vec<Completion> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut completions = Vec::new();

        while Instant::now() < deadline {
            backend
                .completions(&mut completions)
                .expect("native completion drain");
            if expected
                .iter()
                .all(|token| completions.iter().any(|cqe| cqe.token == *token))
            {
                return completions;
            }
        }

        panic!(
            "timed out waiting for native completions; expected={:?}, got={:?}",
            expected, completions
        );
    }

    fn fill_socket_send_buffer(stream: &mut UnixStream) {
        let chunk = [0x5Au8; 8192];
        let mut wrote_any = false;
        loop {
            match stream.write(&chunk) {
                Ok(0) => panic!("socket write returned zero while filling send buffer"),
                Ok(_) => wrote_any = true,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    assert!(wrote_any, "send buffer should accept at least one write");
                    return;
                }
                Err(error) => panic!("failed to fill socket send buffer: {error}"),
            }
        }
    }

    #[test]
    fn native_submit_close_reports_queue_full_when_sq_is_full() {
        let Some(mut backend) = new_test_backend(2) else {
            return;
        };

        let mut saw_queue_full = false;
        for sequence in 0..128 {
            let token = CompletionToken::from_raw(0x10_000 + sequence);
            match backend.submit_close(ConnFd::new(-1), token) {
                Ok(()) => {}
                Err(SubmitError::QueueFull) => {
                    saw_queue_full = true;
                    break;
                }
                Err(error) => panic!("unexpected native submit_close error: {error}"),
            }
        }

        assert!(
            saw_queue_full,
            "native io_uring close submissions should report QueueFull before overwriting SQ state"
        );
    }

    #[test]
    fn native_queue_status_reports_submission_pressure() {
        let Some(mut backend) = new_test_backend(2) else {
            return;
        };

        backend
            .submit_close(ConnFd::new(-1), CompletionToken::from_raw(0x20_000))
            .expect("first close SQE should fit");
        let status = backend.queue_status();

        assert!(status.has_capacity(), "native queue status has capacity");
        assert!(status.sq_capacity > 0, "SQ capacity should be published");
        assert!(
            status.sq_occupancy > 0,
            "pending native SQE should be visible as submission pressure"
        );
    }

    #[test]
    fn native_queue_status_fault_injects_cq_overflow_delta() {
        let Some(mut backend) = new_test_backend(2) else {
            return;
        };

        let first = backend.queue_status_from_observed(ObservedQueueStatus::new(0, 2, 2, 4, 7));
        assert_eq!(first.cq_occupancy, 2);
        assert_eq!(first.cq_capacity, 4);
        assert_eq!(first.cq_overflow_delta, 7);

        let second = backend.queue_status_from_observed(ObservedQueueStatus::new(1, 2, 3, 4, 9));
        assert_eq!(second.sq_occupancy, 1);
        assert_eq!(second.cq_overflow_delta, 2);

        let stable = backend.queue_status_from_observed(ObservedQueueStatus::new(0, 2, 0, 4, 9));
        assert_eq!(stable.cq_overflow_delta, 0);

        let _ =
            backend.queue_status_from_observed(ObservedQueueStatus::new(0, 2, 0, 4, u32::MAX - 1));
        let wrapped = backend.queue_status_from_observed(ObservedQueueStatus::new(0, 2, 0, 4, 3));
        assert_eq!(
            wrapped.cq_overflow_delta, 5,
            "overflow delta should survive the kernel's u32 counter wrap"
        );
    }

    #[test]
    fn native_cancel_race_uses_distinct_cancel_and_target_completions() {
        let Some(mut backend) = new_test_backend(8) else {
            return;
        };
        let (read_socket, mut write_socket) = UnixStream::pair().expect("socketpair");
        read_socket
            .set_nonblocking(true)
            .expect("set read socket nonblocking");
        write_socket
            .set_nonblocking(true)
            .expect("set write socket nonblocking");

        let mut read_buf = [0u8; 1];
        let read_token = CompletionToken::conn(7, 3, OpType::Read).expect("read token");
        let cancel_token = CompletionToken::cancel(read_token).expect("cancel token");
        let read_lease = unsafe {
            // SAFETY: `read_buf` stays live until both native completions are
            // drained below, and the socket fd remains owned by `read_socket`.
            ReadLease::new(
                ConnFd::new(read_socket.as_raw_fd()),
                read_buf.as_mut_ptr(),
                read_buf.len(),
                None,
            )
        }
        .expect("read lease");

        backend
            .submit_read(read_lease, read_token)
            .expect("submit native read");
        backend
            .submit_cancel(read_token, cancel_token)
            .expect("submit native cancel");
        write_socket.write_all(b"x").expect("write race byte");

        let completions = drain_until_tokens(&mut backend, &[read_token, cancel_token]);
        let read_completion = completions
            .iter()
            .find(|cqe| cqe.token == read_token)
            .expect("read completion");
        let cancel_completion = completions
            .iter()
            .find(|cqe| cqe.token == cancel_token)
            .expect("cancel completion");

        assert!(matches!(
            read_token.decode(),
            Ok(DecodedCompletionToken::Conn {
                id: 7,
                generation: 3,
                op: OpType::Read,
            })
        ));
        assert!(matches!(
            cancel_token.decode(),
            Ok(DecodedCompletionToken::Cancel { target }) if target == read_token
        ));
        assert_ne!(
            cancel_completion.token, read_completion.token,
            "native cancel CQE must not reuse the target operation token"
        );
        assert!(
            read_completion.result >= 0 || read_completion.result == -libc::ECANCELED,
            "read target should complete or be canceled, got {}",
            read_completion.result
        );
        assert!(
            cancel_completion.result >= 0
                || cancel_completion.result == -libc::ENOENT
                || cancel_completion.result == -libc::EALREADY,
            "cancel result should represent a legal native race outcome, got {}",
            cancel_completion.result
        );
    }

    #[test]
    fn native_writev_pressure_reports_short_or_eagain_completion() {
        let Some(mut backend) = new_test_backend(8) else {
            return;
        };
        let (mut write_socket, mut read_socket) = UnixStream::pair().expect("socketpair");
        write_socket
            .set_nonblocking(true)
            .expect("set write socket nonblocking");
        read_socket
            .set_nonblocking(true)
            .expect("set read socket nonblocking");

        fill_socket_send_buffer(&mut write_socket);

        let payload = vec![0xA5u8; 1024 * 1024];
        let iov = libc::iovec {
            iov_base: payload.as_ptr().cast_mut().cast(),
            iov_len: payload.len(),
        };
        let writev_token = CompletionToken::conn(9, 4, OpType::Writev).expect("writev token");
        let batch = unsafe {
            // SAFETY: `iov` and `payload` stay live until the native writev
            // completion is drained below, and the fd remains owned by
            // `write_socket`.
            IovecBatch::new(ConnFd::new(write_socket.as_raw_fd()), &iov, 1)
        }
        .expect("writev batch");

        backend
            .submit_writev(batch, writev_token)
            .expect("submit native writev");

        let deadline = Instant::now() + Duration::from_secs(2);
        let mut completions = Vec::new();
        let mut drain = [0u8; 8192];
        let mut drained_after_submit = 0usize;
        let writev_result = loop {
            backend
                .completions(&mut completions)
                .expect("native completion drain");
            if let Some(cqe) = completions.iter().find(|cqe| cqe.token == writev_token) {
                break cqe.result;
            }
            if Instant::now() >= deadline {
                panic!("timed out waiting for native pressured writev completion");
            }
            if drained_after_submit < 256 * 1024 {
                match read_socket.read(&mut drain) {
                    Ok(0) => panic!("peer socket closed while draining pressure"),
                    Ok(n) => drained_after_submit += n,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(error) => panic!("peer drain failed: {error}"),
                }
            } else {
                std::thread::yield_now();
            }
        };

        assert!(
            writev_result == -libc::EAGAIN
                || (writev_result > 0 && (writev_result as usize) < payload.len()),
            "native writev under send-buffer pressure should report EAGAIN or a short write, got {} for {} bytes",
            writev_result,
            payload.len()
        );
    }
}
