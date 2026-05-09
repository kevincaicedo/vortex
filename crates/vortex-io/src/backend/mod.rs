//! Crate-private I/O backend contracts for the reactor event loop.
//!
//! Backends expose typed submission objects to the reactor and keep raw async
//! pointers inside this module boundary. Both backends produce [`Completion`]
//! events that the reactor processes uniformly.

pub(crate) mod polling;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub(crate) mod uring;

pub(crate) use polling::PollingBackend;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub(crate) use uring::IoUringBackend;

use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::os::fd::RawFd;
use std::ptr::NonNull;

use crate::pool::IoBackendMode;

const ACCEPT_TOKEN_RAW: u64 = 0;
const CANCEL_TOKEN_BIT: u64 = 1 << 63;
const OP_MASK: u64 = 0xFF;
const GENERATION_MASK: u64 = 0xFF_FFFF;
const MAX_TOKEN_CONN_ID: usize = 0x7FFF_FFFF;

/// Operation type encoded in connection completion tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpType {
    Read = 1,
    Write = 2,
    Close = 3,
    /// Scatter-gather write (writev / io_uring WRITEV).
    Writev = 4,
}

impl OpType {
    /// Decodes an operation from the low 8 bits of a connection token.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Read),
            2 => Some(Self::Write),
            3 => Some(Self::Close),
            4 => Some(Self::Writev),
            _ => None,
        }
    }

    #[inline]
    fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Identifies one backend completion without exposing raw `user_data` bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct CompletionToken(u64);

impl CompletionToken {
    /// Returns the accept token used for listener completions.
    #[inline]
    pub const fn accept() -> Self {
        Self(ACCEPT_TOKEN_RAW)
    }

    /// Wraps raw backend `user_data` bits for later fallible decoding.
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw backend `user_data` bits.
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Creates a token for a connection operation.
    ///
    /// The generation is truncated to the 24 bits stored in the token layout,
    /// matching the existing slab-generation wrap policy.
    ///
    /// # Errors
    ///
    /// Returns [`TokenEncodeError::ConnIdTooLarge`] if `conn_id` cannot fit
    /// below the reserved cancel bit.
    #[inline]
    pub fn conn(conn_id: usize, generation: u32, op: OpType) -> Result<Self, TokenEncodeError> {
        if conn_id > MAX_TOKEN_CONN_ID {
            return Err(TokenEncodeError::ConnIdTooLarge { conn_id });
        }

        Ok(Self(
            ((conn_id as u64) << 32)
                | (((generation as u64) & GENERATION_MASK) << 8)
                | u64::from(op.as_u8()),
        ))
    }

    /// Creates a distinct cancel-request token for `target`.
    ///
    /// The returned token is used as the cancel SQE `user_data`; the target
    /// token remains the kernel cancellation key.
    ///
    /// # Errors
    ///
    /// Returns an error if `target` is itself a cancel token or cannot decode
    /// as an accept or connection operation.
    #[inline]
    pub fn cancel(target: CompletionToken) -> Result<Self, TokenEncodeError> {
        match target.decode() {
            Ok(DecodedCompletionToken::Accept | DecodedCompletionToken::Conn { .. }) => {
                Ok(Self(target.raw() | CANCEL_TOKEN_BIT))
            }
            Ok(DecodedCompletionToken::Cancel { .. }) => Err(TokenEncodeError::NestedCancel),
            Err(_) => Err(TokenEncodeError::MalformedCancelTarget {
                target: target.raw(),
            }),
        }
    }

    /// Decodes this token into the completion kind owned by the reactor.
    ///
    /// # Errors
    ///
    /// Returns [`TokenDecodeError`] when reserved accept bits, unknown operation
    /// bits, or malformed cancel targets are present.
    #[inline]
    pub fn decode(self) -> Result<DecodedCompletionToken, TokenDecodeError> {
        decode_token(self)
    }
}

/// The concrete backend selected for a reactor.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Backend {
    /// Cross-platform polling backend.
    Polling(PollingBackend),
    /// Linux io_uring backend.
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    Uring(IoUringBackend),
    /// Test-only driver used to exercise reactor retry/error handling without
    /// reintroducing production dynamic dispatch.
    #[cfg(test)]
    Test(Box<dyn BackendDriver>),
}

impl Backend {
    /// Returns the backend kind used for reporting and debugging.
    #[inline]
    pub(crate) const fn kind(&self) -> BackendKind {
        match self {
            Self::Polling(_) => BackendKind::Polling,
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(_) => BackendKind::Uring,
            #[cfg(test)]
            Self::Test(_) => BackendKind::Test,
        }
    }

    /// Returns static capabilities for the selected backend.
    #[inline]
    pub(crate) fn capabilities(&self) -> BackendCapabilities {
        match self {
            Self::Polling(_) => BackendCapabilities::polling(),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => BackendCapabilities::uring(backend.sqpoll_enabled()),
            #[cfg(test)]
            Self::Test(_) => BackendCapabilities::test(),
        }
    }

    /// Submit an accept operation on the listening socket.
    #[inline]
    pub(crate) fn submit_accept(
        &mut self,
        listener_fd: ListenerFd,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        match self {
            Self::Polling(backend) => backend.submit_accept(listener_fd, token),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.submit_accept(listener_fd, token),
            #[cfg(test)]
            Self::Test(backend) => backend.submit_accept(listener_fd, token),
        }
    }

    /// Submit a read operation.
    #[inline]
    pub(crate) fn submit_read(
        &mut self,
        lease: ReadLease,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        match self {
            Self::Polling(backend) => backend.submit_read(lease, token),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.submit_read(lease, token),
            #[cfg(test)]
            Self::Test(backend) => backend.submit_read(lease, token),
        }
    }

    /// Submit a write operation.
    #[inline]
    pub(crate) fn submit_write(
        &mut self,
        lease: WriteLease,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        match self {
            Self::Polling(backend) => backend.submit_write(lease, token),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.submit_write(lease, token),
            #[cfg(test)]
            Self::Test(backend) => backend.submit_write(lease, token),
        }
    }

    /// Submit a scatter-gather write operation.
    #[inline]
    pub(crate) fn submit_writev(
        &mut self,
        batch: IovecBatch,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        match self {
            Self::Polling(backend) => backend.submit_writev(batch, token),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.submit_writev(batch, token),
            #[cfg(test)]
            Self::Test(backend) => backend.submit_writev(batch, token),
        }
    }

    /// Submit async cancellation of an in-flight operation.
    #[inline]
    pub(crate) fn submit_cancel(
        &mut self,
        target: CompletionToken,
        cancel: CompletionToken,
    ) -> Result<(), SubmitError> {
        match self {
            Self::Polling(backend) => backend.submit_cancel(target, cancel),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.submit_cancel(target, cancel),
            #[cfg(test)]
            Self::Test(backend) => backend.submit_cancel(target, cancel),
        }
    }

    /// Register fixed buffers with the backend.
    #[inline]
    pub(crate) fn register_buffers(&self, iovecs: &[libc::iovec]) -> Result<(), SubmitError> {
        match self {
            Self::Polling(backend) => backend.register_buffers(iovecs),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.register_buffers(iovecs),
            #[cfg(test)]
            Self::Test(backend) => backend.register_buffers(iovecs),
        }
    }

    /// Submit a close operation.
    #[inline]
    pub(crate) fn submit_close(
        &mut self,
        fd: ConnFd,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        match self {
            Self::Polling(backend) => backend.submit_close(fd, token),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.submit_close(fd, token),
            #[cfg(test)]
            Self::Test(backend) => backend.submit_close(fd, token),
        }
    }

    /// Flush pending submissions.
    #[inline]
    pub(crate) fn flush(&mut self) -> Result<usize, SubmitError> {
        match self {
            Self::Polling(backend) => backend.flush(),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.flush(),
            #[cfg(test)]
            Self::Test(backend) => backend.flush(),
        }
    }

    /// Drain completions, potentially blocking briefly.
    #[inline]
    pub(crate) fn completions(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        match self {
            Self::Polling(backend) => backend.completions(out),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.completions(out),
            #[cfg(test)]
            Self::Test(backend) => backend.completions(out),
        }
    }

    /// Non-blocking completion drain.
    #[inline]
    pub(crate) fn drain_cq(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        match self {
            Self::Polling(backend) => backend.drain_cq(out),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.drain_cq(out),
            #[cfg(test)]
            Self::Test(backend) => backend.drain_cq(out),
        }
    }

    /// Returns backend queue state for runtime diagnostics.
    #[inline]
    pub(crate) fn queue_status(&mut self) -> BackendQueueStatus {
        match self {
            Self::Polling(backend) => backend.queue_status(),
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            Self::Uring(backend) => backend.queue_status(),
            #[cfg(test)]
            Self::Test(backend) => backend.queue_status(),
        }
    }

    /// Wraps a test backend without affecting production dispatch.
    #[cfg(test)]
    pub(crate) fn test(backend: impl BackendDriver + 'static) -> Self {
        Self::Test(Box::new(backend))
    }
}

/// Runtime backend kind after `Auto` fallback has been resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackendKind {
    /// Cross-platform polling backend.
    Polling,
    /// Linux io_uring backend.
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    Uring,
    /// Test-only backend.
    #[cfg(test)]
    Test,
}

/// Backend selection result for one reactor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackendPlan {
    /// Configured mode before fallback.
    pub(crate) requested: IoBackendMode,
    /// Effective backend used by the reactor.
    pub(crate) effective: BackendKind,
    /// Effective backend capabilities.
    pub(crate) capabilities: BackendCapabilities,
}

/// Runtime queue state exported by concrete backends.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BackendQueueStatus {
    /// Pending SQEs visible to the backend at the sample point.
    pub(crate) sq_occupancy: u64,
    /// Submission queue capacity.
    pub(crate) sq_capacity: u64,
    /// Pending CQEs visible to the backend at the sample point.
    pub(crate) cq_occupancy: u64,
    /// Completion queue capacity.
    pub(crate) cq_capacity: u64,
    /// New CQ overflow events since the previous backend sample.
    pub(crate) cq_overflow_delta: u64,
}

impl BackendQueueStatus {
    /// Returns true when the status carries static queue dimensions.
    #[inline]
    pub(crate) const fn has_capacity(self) -> bool {
        self.sq_capacity != 0 || self.cq_capacity != 0
    }

    /// Returns true when this sample should update runtime pressure counters.
    #[inline]
    pub(crate) const fn has_runtime_signal(self) -> bool {
        self.sq_occupancy != 0 || self.cq_occupancy != 0 || self.cq_overflow_delta != 0
    }
}

/// Static capability bits for a selected backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackendCapabilities {
    /// Backend can use registered fixed buffers for read/write submissions.
    pub(crate) fixed_buffers: bool,
    /// Backend is running with an io_uring SQPOLL kernel thread.
    pub(crate) sqpoll: bool,
    /// Backend accepts multiple sockets from one armed accept operation.
    pub(crate) multishot_accept: bool,
    /// Backend accepts sockets with accept4 flags instead of accept + fcntl.
    pub(crate) accept4: bool,
    /// Backend closes file descriptors through a completion-producing opcode.
    pub(crate) close_opcode: bool,
    /// Backend accepts cancellation requests.
    pub(crate) async_cancel: bool,
    /// Backend supports a non-blocking completion drain.
    pub(crate) nonblocking_drain: bool,
}

impl BackendCapabilities {
    const fn polling() -> Self {
        Self {
            fixed_buffers: false,
            sqpoll: false,
            multishot_accept: false,
            accept4: cfg!(any(target_os = "linux", target_os = "android")),
            close_opcode: false,
            async_cancel: true,
            nonblocking_drain: true,
        }
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    const fn uring(sqpoll: bool) -> Self {
        Self {
            fixed_buffers: true,
            sqpoll,
            multishot_accept: false,
            accept4: false,
            close_opcode: true,
            async_cancel: true,
            nonblocking_drain: true,
        }
    }

    #[cfg(test)]
    const fn test() -> Self {
        Self {
            fixed_buffers: false,
            sqpoll: false,
            multishot_accept: false,
            accept4: false,
            close_opcode: false,
            async_cancel: true,
            nonblocking_drain: true,
        }
    }
}

/// Socket file descriptor for a connected client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnFd(RawFd);

impl ConnFd {
    /// Wraps a raw connection fd owned by the reactor connection state.
    #[inline]
    pub(crate) const fn new(fd: RawFd) -> Self {
        Self(fd)
    }

    /// Returns the raw fd for syscalls.
    #[inline]
    pub(crate) const fn raw(self) -> RawFd {
        self.0
    }
}

/// Socket file descriptor for the listening socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ListenerFd(RawFd);

impl ListenerFd {
    /// Wraps the reactor-owned listener fd.
    #[inline]
    pub(crate) const fn new(fd: RawFd) -> Self {
        Self(fd)
    }

    /// Returns the raw fd for syscalls.
    #[inline]
    pub(crate) const fn raw(self) -> RawFd {
        self.0
    }
}

/// io_uring fixed-buffer id checked before narrowing to `u16`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(all(target_os = "linux", feature = "io-uring")), allow(dead_code))]
pub(crate) struct FixedBufferId(u16);

impl FixedBufferId {
    /// Largest fixed-buffer id representable by io_uring's `buf_index`.
    pub(crate) const MAX_INDEX: usize = u16::MAX as usize;
    /// Maximum registered-buffer count addressable by `buf_index`.
    pub(crate) const MAX_BUFFER_COUNT: usize = Self::MAX_INDEX + 1;

    /// Creates a fixed-buffer id from a buffer-pool index.
    #[inline]
    pub(crate) fn new(index: usize) -> Result<Self, SubmitError> {
        let id = u16::try_from(index)
            .map_err(|_| SubmitError::Unsupported("fixed buffer index exceeds u16"))?;
        Ok(Self(id))
    }

    /// Validates a buffer-pool length before fixed-buffer registration.
    #[inline]
    pub(crate) fn validate_pool_len(count: usize) -> Result<(), SubmitError> {
        if count <= Self::MAX_BUFFER_COUNT {
            Ok(())
        } else {
            Err(SubmitError::Unsupported(
                "fixed buffer count exceeds io_uring buf_index range",
            ))
        }
    }

    /// Returns the id passed to io_uring fixed-buffer opcodes.
    #[inline]
    #[cfg_attr(not(all(target_os = "linux", feature = "io-uring")), allow(dead_code))]
    pub(crate) const fn raw(self) -> u16 {
        self.0
    }
}

/// Reactor-owned read buffer lease for one submitted operation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReadLease {
    fd: ConnFd,
    ptr: NonNull<u8>,
    len: usize,
    #[cfg_attr(not(all(target_os = "linux", feature = "io-uring")), allow(dead_code))]
    fixed: Option<FixedBufferId>,
}

impl ReadLease {
    /// Creates a read lease for an async backend submission.
    ///
    /// # Safety
    ///
    /// `ptr..ptr+len` must identify writable memory owned by the reactor and
    /// remain valid until the completion for `token` is handled. If `fixed` is
    /// present, it must name the registered buffer containing the range.
    #[inline]
    pub(crate) unsafe fn new(
        fd: ConnFd,
        ptr: *mut u8,
        len: usize,
        fixed: Option<FixedBufferId>,
    ) -> Result<Self, SubmitError> {
        let ptr = NonNull::new(ptr).ok_or(SubmitError::Unsupported("null read buffer"))?;
        Ok(Self {
            fd,
            ptr,
            len,
            fixed,
        })
    }

    #[inline]
    pub(crate) const fn fd(self) -> ConnFd {
        self.fd
    }

    #[inline]
    pub(crate) const fn ptr(self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub(crate) const fn len(self) -> usize {
        self.len
    }

    #[inline]
    #[cfg_attr(not(all(target_os = "linux", feature = "io-uring")), allow(dead_code))]
    pub(crate) const fn fixed(self) -> Option<FixedBufferId> {
        self.fixed
    }
}

/// Reactor-owned write buffer lease for one submitted operation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WriteLease {
    fd: ConnFd,
    ptr: NonNull<u8>,
    len: usize,
    #[cfg_attr(not(all(target_os = "linux", feature = "io-uring")), allow(dead_code))]
    fixed: Option<FixedBufferId>,
    _shared: PhantomData<*const u8>,
}

impl WriteLease {
    /// Creates a write lease for an async backend submission.
    ///
    /// # Safety
    ///
    /// `ptr..ptr+len` must identify readable memory owned by the reactor and
    /// remain valid until the completion for `token` is handled. If `fixed` is
    /// present, it must name the registered buffer containing the range.
    #[inline]
    pub(crate) unsafe fn new(
        fd: ConnFd,
        ptr: *const u8,
        len: usize,
        fixed: Option<FixedBufferId>,
    ) -> Result<Self, SubmitError> {
        let ptr =
            NonNull::new(ptr.cast_mut()).ok_or(SubmitError::Unsupported("null write buffer"))?;
        Ok(Self {
            fd,
            ptr,
            len,
            fixed,
            _shared: PhantomData,
        })
    }

    #[inline]
    pub(crate) const fn fd(self) -> ConnFd {
        self.fd
    }

    #[inline]
    pub(crate) const fn ptr(self) -> *const u8 {
        self.ptr.as_ptr().cast_const()
    }

    #[inline]
    pub(crate) const fn len(self) -> usize {
        self.len
    }

    #[inline]
    #[cfg_attr(not(all(target_os = "linux", feature = "io-uring")), allow(dead_code))]
    pub(crate) const fn fixed(self) -> Option<FixedBufferId> {
        self.fixed
    }
}

/// Reactor-owned scatter-gather write batch for one submitted operation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IovecBatch {
    fd: ConnFd,
    iovecs: NonNull<libc::iovec>,
    count: usize,
}

impl IovecBatch {
    /// Conservative Linux/macOS `IOV_MAX` used before backend submission.
    pub(crate) const MAX_SEGMENTS: usize = 1024;

    /// Creates an async `writev` batch.
    ///
    /// # Safety
    ///
    /// `iovecs` must point to `count` valid `iovec` entries, and both the
    /// iovec array and every referenced response buffer must remain valid until
    /// the completion for `token` is handled.
    #[inline]
    pub(crate) unsafe fn new(
        fd: ConnFd,
        iovecs: *const libc::iovec,
        count: usize,
    ) -> Result<Self, SubmitError> {
        if count == 0 {
            return Err(SubmitError::Unsupported("empty iovec batch"));
        }
        if count > Self::MAX_SEGMENTS {
            return Err(SubmitError::Unsupported("iovec batch exceeds IOV_MAX"));
        }
        let iovecs =
            NonNull::new(iovecs.cast_mut()).ok_or(SubmitError::Unsupported("null iovec batch"))?;
        Ok(Self { fd, iovecs, count })
    }

    #[inline]
    pub(crate) const fn fd(self) -> ConnFd {
        self.fd
    }

    #[inline]
    pub(crate) const fn ptr(self) -> *const libc::iovec {
        self.iovecs.as_ptr().cast_const()
    }

    #[inline]
    pub(crate) const fn count(self) -> usize {
        self.count
    }
}

/// Error returned by backend submission paths.
#[derive(Debug)]
pub(crate) enum SubmitError {
    /// The backend submission queue has no free slot right now.
    #[cfg_attr(
        not(any(test, all(target_os = "linux", feature = "io-uring"))),
        allow(dead_code)
    )]
    QueueFull,
    /// The selected backend or current state does not support the operation.
    Unsupported(&'static str),
    /// The OS or backend returned an ordinary I/O error.
    Backend(io::Error),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull => f.write_str("submission queue full"),
            Self::Unsupported(reason) => write!(f, "unsupported backend operation: {reason}"),
            Self::Backend(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for SubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Backend(error) => Some(error),
            Self::QueueFull | Self::Unsupported(_) => None,
        }
    }
}

impl From<io::Error> for SubmitError {
    #[inline]
    fn from(error: io::Error) -> Self {
        Self::Backend(error)
    }
}

/// The typed completion represented by a [`CompletionToken`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodedCompletionToken {
    /// Listener accept completion.
    Accept,
    /// Cancel request completion for a target operation.
    Cancel {
        /// Operation token passed to the backend cancellation primitive.
        target: CompletionToken,
    },
    /// Connection-bound operation completion.
    Conn {
        /// Slab connection id.
        id: usize,
        /// 24-bit generation snapshot used to reject stale completions.
        generation: u32,
        /// Connection operation kind.
        op: OpType,
    },
}

/// Error returned when raw backend completion bits are not a valid token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenDecodeError {
    /// A non-zero token used reserved accept operation bits.
    ReservedAcceptPayload {
        /// Raw token that failed decoding.
        token: u64,
    },
    /// The operation bits are not assigned to any supported operation.
    UnknownOp {
        /// Raw token that failed decoding.
        token: u64,
        /// Unknown low-byte operation value.
        op: u8,
    },
    /// A cancel token targeted malformed raw bits.
    MalformedCancelTarget {
        /// Raw cancel token that failed decoding.
        token: u64,
        /// Raw target bits embedded in the cancel token.
        target: u64,
    },
}

/// Error returned when a typed token cannot be encoded in the 64-bit layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenEncodeError {
    /// The connection id would overlap the reserved cancel bit.
    ConnIdTooLarge {
        /// Connection id that did not fit.
        conn_id: usize,
    },
    /// Cancel request tokens cannot themselves be canceled.
    NestedCancel,
    /// The target token does not decode to an accept or connection operation.
    MalformedCancelTarget {
        /// Raw malformed target bits.
        target: u64,
    },
}

/// Result state for a cancel request completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelResult {
    /// The backend accepted the cancel and the original operation should later
    /// complete terminally, usually with `-ECANCELED`.
    Canceled,
    /// The reactor already observed the target operation's terminal completion.
    AlreadyTerminal,
    /// The backend could not find the target while the reactor still tracks it.
    NotFound,
    /// The backend reported an error unrelated to the normal cancel race.
    BackendError {
        /// Positive errno value derived from the backend completion result.
        errno: i32,
    },
}

impl CancelResult {
    /// Classifies a backend cancel request completion.
    #[inline]
    pub fn from_completion_result(result: i32, target_inflight: bool) -> Self {
        if result >= 0 {
            return Self::Canceled;
        }

        if result == -libc::ENOENT {
            return if target_inflight {
                Self::NotFound
            } else {
                Self::AlreadyTerminal
            };
        }

        Self::BackendError {
            errno: result.saturating_neg(),
        }
    }
}

/// Encodes a connection token, generation counter, and operation type.
///
/// Layout (64-bit):
/// ```text
///  63 62     32 31        8 7       0
/// +---+--------+-----------+---------+
/// | C | conn_id| generation| op_type |
/// | 0 |(31 bit)|  (24 bit) | (8 bit) |
/// +---+--------+-----------+---------+
/// ```
///
/// Bit 63 is reserved for cancel request completions. Raw zero is reserved for
/// [`DecodedCompletionToken::Accept`].
///
/// The 24-bit generation counter prevents stale CQE processing after a slab
/// slot is reused. At 100K ops/sec per slot, wrap-around takes about 168
/// seconds instead of about 2.6ms with the old 8-bit counter.
#[inline]
pub fn encode_token(
    conn_id: usize,
    generation: u32,
    op: OpType,
) -> Result<CompletionToken, TokenEncodeError> {
    CompletionToken::conn(conn_id, generation, op)
}

/// Decodes a token into its typed completion variant.
#[inline]
pub fn decode_token(token: CompletionToken) -> Result<DecodedCompletionToken, TokenDecodeError> {
    let raw = token.raw();
    if (raw & CANCEL_TOKEN_BIT) != 0 {
        let target = CompletionToken::from_raw(raw & !CANCEL_TOKEN_BIT);
        return match decode_token(target) {
            Ok(DecodedCompletionToken::Accept | DecodedCompletionToken::Conn { .. }) => {
                Ok(DecodedCompletionToken::Cancel { target })
            }
            Ok(DecodedCompletionToken::Cancel { .. }) => unreachable!("cancel bit was cleared"),
            Err(_) => Err(TokenDecodeError::MalformedCancelTarget {
                token: raw,
                target: target.raw(),
            }),
        };
    }

    if raw == ACCEPT_TOKEN_RAW {
        return Ok(DecodedCompletionToken::Accept);
    }

    let op_bits = (raw & OP_MASK) as u8;
    if op_bits == 0 {
        return Err(TokenDecodeError::ReservedAcceptPayload { token: raw });
    }

    let Some(op) = OpType::from_u8(op_bits) else {
        return Err(TokenDecodeError::UnknownOp {
            token: raw,
            op: op_bits,
        });
    };
    let generation = ((raw >> 8) & GENERATION_MASK) as u32;
    let id = (raw >> 32) as usize;

    Ok(DecodedCompletionToken::Conn { id, generation, op })
}

/// A completion event from the I/O backend.
#[derive(Debug, Clone)]
pub struct Completion {
    /// Typed wrapper around backend `user_data`.
    pub token: CompletionToken,
    /// Result code (bytes transferred, or negative errno on error).
    pub result: i32,
    /// Backend-specific flags.
    #[allow(dead_code)]
    pub flags: u32,
}

/// Crate-private backend driver contract implemented by concrete backends.
///
/// All operations are **submission-based**: the caller submits SQEs and later
/// retrieves completions. This matches io_uring's native model; the polling
/// backend emulates it by performing I/O inline and generating synthetic
/// completions.
pub(crate) trait BackendDriver {
    /// Submit an accept operation on the listening socket.
    fn submit_accept(
        &mut self,
        listener_fd: ListenerFd,
        token: CompletionToken,
    ) -> Result<(), SubmitError>;

    /// Submit a read operation into a reactor-owned buffer lease.
    fn submit_read(&mut self, lease: ReadLease, token: CompletionToken) -> Result<(), SubmitError>;

    /// Submit a write operation from a reactor-owned buffer lease.
    fn submit_write(
        &mut self,
        lease: WriteLease,
        token: CompletionToken,
    ) -> Result<(), SubmitError>;

    /// Submit a scatter-gather write from a reactor-owned iovec batch.
    fn submit_writev(
        &mut self,
        batch: IovecBatch,
        token: CompletionToken,
    ) -> Result<(), SubmitError>;

    /// Submit async cancellation of an in-flight operation.
    ///
    /// `target` is the operation being canceled. `cancel` is the distinct
    /// request token that will identify the cancel request completion.
    fn submit_cancel(
        &mut self,
        _target: CompletionToken,
        _cancel: CompletionToken,
    ) -> Result<(), SubmitError> {
        Ok(())
    }

    /// Register fixed buffers with the kernel (io_uring: `register_buffers`).
    ///
    /// No-op for non-uring backends.
    fn register_buffers(&self, _iovecs: &[libc::iovec]) -> Result<(), SubmitError> {
        Ok(())
    }

    /// Submit a close operation on a file descriptor.
    fn submit_close(&mut self, fd: ConnFd, token: CompletionToken) -> Result<(), SubmitError>;

    /// Flush all pending submissions to the kernel (io_uring: `submit()`).
    /// Returns the number of SQEs submitted.
    fn flush(&mut self) -> Result<usize, SubmitError>;

    /// Drain available completions into `out`. Returns the count of completions.
    ///
    /// This may block briefly (up to ~1 ms) if no completions are ready.
    /// For io_uring this combines submit + wait into a single syscall.
    fn completions(&mut self, out: &mut Vec<Completion>) -> io::Result<usize>;

    /// Non-blocking drain of the completion queue.
    ///
    /// Appends any immediately-available CQEs to `out` without issuing a
    /// syscall (io_uring) or blocking (polling). Used by the reactor's
    /// fast-path to collapse read->process->write into one iteration.
    fn drain_cq(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        // Default: delegate to completions(). Backends that can do a cheaper
        // non-blocking peek should override.
        self.completions(out)
    }

    /// Returns backend queue status. Non-queue backends return zeroes.
    fn queue_status(&mut self) -> BackendQueueStatus {
        BackendQueueStatus::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_roundtrip() {
        let conn_id = 42;
        let cgen = 7;
        let op = OpType::Read;
        let token = encode_token(conn_id, cgen, op).unwrap();
        assert_eq!(
            decode_token(token).unwrap(),
            DecodedCompletionToken::Conn {
                id: conn_id,
                generation: cgen,
                op,
            }
        );
    }

    #[test]
    fn token_accept_roundtrip() {
        let token = CompletionToken::accept();
        assert_eq!(decode_token(token).unwrap(), DecodedCompletionToken::Accept);
    }

    #[test]
    fn token_large_conn_id() {
        let conn_id = 100_000;
        let cgen = 0xFF_FFFF_u32;
        let op = OpType::Write;
        let token = encode_token(conn_id, cgen, op).unwrap();
        assert_eq!(
            decode_token(token).unwrap(),
            DecodedCompletionToken::Conn {
                id: conn_id,
                generation: cgen,
                op,
            }
        );
    }

    #[test]
    fn token_generation_wraps() {
        let conn_id = 5;
        let cgen: u32 = 0xFF_FFFF;
        let next_gen = cgen.wrapping_add(1) & 0xFF_FFFF;
        let token = encode_token(conn_id, next_gen, OpType::Read).unwrap();
        assert_eq!(
            decode_token(token).unwrap(),
            DecodedCompletionToken::Conn {
                id: conn_id,
                generation: 0,
                op: OpType::Read,
            }
        );
    }

    #[test]
    fn token_rejects_conn_id_that_overlaps_cancel_bit() {
        assert_eq!(
            encode_token(usize::MAX, 42, OpType::Close),
            Err(TokenEncodeError::ConnIdTooLarge {
                conn_id: usize::MAX,
            })
        );
    }

    #[test]
    fn malformed_reserved_accept_bits_do_not_decode_as_accept() {
        let token = CompletionToken::from_raw(1 << 32);
        assert_eq!(
            decode_token(token),
            Err(TokenDecodeError::ReservedAcceptPayload { token: 1 << 32 })
        );
    }

    #[test]
    fn malformed_unknown_op_bits_do_not_decode_as_accept() {
        let token = CompletionToken::from_raw(0xFE);
        assert_eq!(
            decode_token(token),
            Err(TokenDecodeError::UnknownOp {
                token: 0xFE,
                op: 0xFE,
            })
        );
    }

    #[test]
    fn cancel_token_decodes_to_target_and_is_distinct() {
        let target = encode_token(7, 11, OpType::Writev).unwrap();
        let cancel = CompletionToken::cancel(target).unwrap();
        assert_ne!(cancel, target);
        assert_eq!(
            decode_token(cancel).unwrap(),
            DecodedCompletionToken::Cancel { target }
        );
        assert_eq!(
            decode_token(target).unwrap(),
            DecodedCompletionToken::Conn {
                id: 7,
                generation: 11,
                op: OpType::Writev,
            }
        );
    }

    #[test]
    fn cancel_with_malformed_target_fails_decode() {
        let target = CompletionToken::from_raw(0xFE);
        let cancel = CompletionToken::from_raw(target.raw() | CANCEL_TOKEN_BIT);
        assert_eq!(
            decode_token(cancel),
            Err(TokenDecodeError::MalformedCancelTarget {
                token: cancel.raw(),
                target: target.raw(),
            })
        );
    }

    #[test]
    fn cancel_result_distinguishes_success_not_found_and_already_terminal() {
        assert_eq!(
            CancelResult::from_completion_result(0, true),
            CancelResult::Canceled
        );
        assert_eq!(
            CancelResult::from_completion_result(-libc::ENOENT, true),
            CancelResult::NotFound
        );
        assert_eq!(
            CancelResult::from_completion_result(-libc::ENOENT, false),
            CancelResult::AlreadyTerminal
        );
        assert_eq!(
            CancelResult::from_completion_result(-libc::EIO, true),
            CancelResult::BackendError { errno: libc::EIO }
        );
    }

    #[test]
    fn completion_token_is_zero_cost_u64_wrapper() {
        assert_eq!(
            std::mem::size_of::<CompletionToken>(),
            std::mem::size_of::<u64>()
        );
        assert_eq!(
            std::mem::align_of::<CompletionToken>(),
            std::mem::align_of::<u64>()
        );
    }

    #[test]
    fn fixed_buffer_id_rejects_overflow_before_u16_narrowing() {
        assert!(FixedBufferId::new(u16::MAX as usize).is_ok());
        assert!(matches!(
            FixedBufferId::new(u16::MAX as usize + 1),
            Err(SubmitError::Unsupported(_))
        ));
        assert!(FixedBufferId::validate_pool_len(FixedBufferId::MAX_BUFFER_COUNT).is_ok());
        assert!(matches!(
            FixedBufferId::validate_pool_len(FixedBufferId::MAX_BUFFER_COUNT + 1),
            Err(SubmitError::Unsupported(_))
        ));
    }

    #[test]
    fn typed_submit_wrappers_reject_null_or_empty_memory() {
        assert!(matches!(
            // SAFETY: The test deliberately passes a null pointer to verify
            // that the typed boundary rejects it before backend submission.
            unsafe { ReadLease::new(ConnFd::new(1), std::ptr::null_mut(), 1, None) },
            Err(SubmitError::Unsupported(_))
        ));
        assert!(matches!(
            // SAFETY: The test deliberately passes a null pointer to verify
            // that the typed boundary rejects it before backend submission.
            unsafe { WriteLease::new(ConnFd::new(1), std::ptr::null(), 1, None) },
            Err(SubmitError::Unsupported(_))
        ));

        let iovecs = [libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        }];
        assert!(matches!(
            // SAFETY: The pointer is valid, but an empty batch is not a valid
            // async writev submission.
            unsafe { IovecBatch::new(ConnFd::new(1), iovecs.as_ptr(), 0) },
            Err(SubmitError::Unsupported(_))
        ));

        let oversized_iovecs = vec![
            libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            };
            IovecBatch::MAX_SEGMENTS + 1
        ];
        assert!(matches!(
            // SAFETY: The pointer is valid for the test allocation, but the
            // count exceeds the backend writev segment limit.
            unsafe {
                IovecBatch::new(
                    ConnFd::new(1),
                    oversized_iovecs.as_ptr(),
                    oversized_iovecs.len(),
                )
            },
            Err(SubmitError::Unsupported(_))
        ));
    }
}
