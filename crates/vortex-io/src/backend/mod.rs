//! I/O backend abstraction for the reactor event loop.
//!
//! The [`IoBackend`] trait provides a completion-based I/O interface that
//! abstracts over io_uring (Linux) and polling/kqueue/epoll (cross-platform).
//! Both backends produce [`Completion`] events that the reactor processes
//! uniformly.

pub mod polling;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub mod uring;

pub use polling::PollingBackend;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use uring::IoUringBackend;

use std::os::fd::RawFd;

/// Operation type encoded in completion tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpType {
    Accept = 0,
    Read = 1,
    Write = 2,
    Close = 3,
    /// Scatter-gather write (writev / io_uring WRITEV).
    Writev = 4,
}

impl OpType {
    /// Decode from the low 8 bits of a token.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Accept),
            1 => Some(Self::Read),
            2 => Some(Self::Write),
            3 => Some(Self::Close),
            4 => Some(Self::Writev),
            _ => None,
        }
    }
}

/// Encode a connection token, generation counter, and operation type into a single u64.
///
/// Layout (64-bit):
/// ```text
///  63        32 31        8 7       0
/// ┌───────────┬───────────┬─────────┐
/// │  conn_id  │ generation│ op_type │
/// │  (32 bit) │  (24 bit) │ (8 bit) │
/// └───────────┴───────────┴─────────┘
/// ```
///
/// The 24-bit generation counter prevents stale CQE processing after a slab
/// slot is reused. At 100K ops/sec per slot, wrap-around takes ~168 seconds
/// instead of ~2.6ms with the old 8-bit counter.
#[inline]
pub fn encode_token(conn_id: usize, generation: u32, op: OpType) -> u64 {
    ((conn_id as u64) << 32) | (((generation & 0xFF_FFFF) as u64) << 8) | (op as u64)
}

/// Decode a token into (connection_id, generation, operation_type).
#[inline]
pub fn decode_token(token: u64) -> (usize, u32, OpType) {
    let op = OpType::from_u8((token & 0xFF) as u8).unwrap_or(OpType::Accept);
    let generation = ((token >> 8) & 0xFF_FFFF) as u32;
    let conn_id = (token >> 32) as usize;
    (conn_id, generation, op)
}

/// A completion event from the I/O backend.
#[derive(Debug, Clone)]
pub struct Completion {
    /// Encoded token: `(conn_id << 8) | op_type`.
    pub token: u64,
    /// Result code (bytes transferred, or negative errno on error).
    pub result: i32,
    /// Backend-specific flags.
    pub flags: u32,
}

/// Abstraction over io_uring and polling I/O backends.
///
/// All operations are **submission-based**: the caller submits SQEs and later
/// retrieves completions. This matches io_uring's native model; the polling
/// backend emulates it by performing I/O inline and generating synthetic
/// completions.
pub trait IoBackend {
    /// Submit an accept operation on the listening socket.
    fn submit_accept(&mut self, listener_fd: RawFd, token: u64) -> std::io::Result<()>;

    /// Submit a read operation into the buffer at `buf_ptr` with capacity `buf_len`.
    fn submit_read(
        &mut self,
        fd: RawFd,
        buf_ptr: *mut u8,
        buf_len: usize,
        token: u64,
    ) -> std::io::Result<()>;

    /// Submit a write operation from the buffer at `buf_ptr` with length `buf_len`.
    fn submit_write(
        &mut self,
        fd: RawFd,
        buf_ptr: *const u8,
        buf_len: usize,
        token: u64,
    ) -> std::io::Result<()>;

    /// Submit a read into a pre-registered fixed buffer (io_uring: `ReadFixed`).
    ///
    /// `buf_index` is the index in the buffer array registered with
    /// `register_buffers()`. The `buf_ptr` and `buf_len` specify the region
    /// within that registered buffer. For polling backends this falls back
    /// to a regular read.
    fn submit_read_fixed(
        &mut self,
        fd: RawFd,
        buf_ptr: *mut u8,
        buf_len: usize,
        buf_index: u16,
        token: u64,
    ) -> std::io::Result<()> {
        // Default: fall back to regular read (polling backends).
        let _ = buf_index;
        self.submit_read(fd, buf_ptr, buf_len, token)
    }

    /// Submit a write from a pre-registered fixed buffer (io_uring: `WriteFixed`).
    ///
    /// For polling backends this falls back to a regular write.
    fn submit_write_fixed(
        &mut self,
        fd: RawFd,
        buf_ptr: *const u8,
        buf_len: usize,
        buf_index: u16,
        token: u64,
    ) -> std::io::Result<()> {
        // Default: fall back to regular write (polling backends).
        let _ = buf_index;
        self.submit_write(fd, buf_ptr, buf_len, token)
    }

    /// Submit a scatter-gather write from an array of `iovec` segments.
    ///
    /// For `IoUringBackend`: builds `opcode::Writev` SQE.
    /// For `PollingBackend`: calls `libc::writev` inline.
    ///
    /// The completion result is bytes written (positive) or negative errno.
    /// The caller must ensure the iovec memory outlives the operation.
    fn submit_writev(
        &mut self,
        fd: RawFd,
        iovecs: *const libc::iovec,
        iov_count: usize,
        token: u64,
    ) -> std::io::Result<()>;

    /// Submit async cancellation of an in-flight operation identified by its
    /// user-data token (io_uring: `AsyncCancel`).
    ///
    /// For polling backends this is a no-op since operations complete inline.
    fn submit_cancel(&mut self, _token: u64) -> std::io::Result<()> {
        Ok(())
    }

    /// Register fixed buffers with the kernel (io_uring: `register_buffers`).
    ///
    /// No-op for non-uring backends.
    fn register_buffers(&self, _iovecs: &[libc::iovec]) -> std::io::Result<()> {
        Ok(())
    }

    /// Submit a close operation on a file descriptor.
    fn submit_close(&mut self, fd: RawFd, token: u64) -> std::io::Result<()>;

    /// Flush all pending submissions to the kernel (io_uring: `submit()`).
    /// Returns the number of SQEs submitted.
    fn flush(&mut self) -> std::io::Result<usize>;

    /// Drain available completions into `out`. Returns the count of completions.
    fn completions(&mut self, out: &mut Vec<Completion>) -> std::io::Result<usize>;
}

/// Sentinel connection ID used for accept operations (not tied to a connection).
pub const ACCEPT_CONN_ID: usize = 0x00FF_FFFF;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_roundtrip() {
        let conn_id = 42;
        let cgen = 7;
        let op = OpType::Read;
        let token = encode_token(conn_id, cgen, op);
        let (decoded_id, decoded_gen, decoded_op) = decode_token(token);
        assert_eq!(decoded_id, conn_id);
        assert_eq!(decoded_gen, cgen);
        assert_eq!(decoded_op, op);
    }

    #[test]
    fn token_accept_sentinel() {
        let token = encode_token(ACCEPT_CONN_ID, 0, OpType::Accept);
        let (id, cgen, op) = decode_token(token);
        assert_eq!(id, ACCEPT_CONN_ID);
        assert_eq!(cgen, 0);
        assert_eq!(op, OpType::Accept);
    }

    #[test]
    fn token_large_conn_id() {
        let conn_id = 100_000;
        let cgen = 0xFF_FFFF_u32; // max 24-bit generation
        let op = OpType::Write;
        let token = encode_token(conn_id, cgen, op);
        let (decoded_id, decoded_gen, decoded_op) = decode_token(token);
        assert_eq!(decoded_id, conn_id);
        assert_eq!(decoded_gen, cgen);
        assert_eq!(decoded_op, op);
    }

    #[test]
    fn token_generation_wraps() {
        let conn_id = 5;
        let cgen: u32 = 0xFF_FFFF; // max 24-bit
        // Wrapping add and mask to 24 bits
        let next_gen = cgen.wrapping_add(1) & 0xFF_FFFF;
        let token = encode_token(conn_id, next_gen, OpType::Read);
        let (decoded_id, decoded_gen, _) = decode_token(token);
        assert_eq!(decoded_id, conn_id);
        assert_eq!(decoded_gen, 0); // Wrapped around from 24-bit max
    }

    #[test]
    fn token_32bit_conn_id() {
        // Test that conn_id uses full 32-bit range.
        let conn_id = u32::MAX as usize;
        let cgen = 42_u32;
        let op = OpType::Close;
        let token = encode_token(conn_id, cgen, op);
        let (decoded_id, decoded_gen, decoded_op) = decode_token(token);
        assert_eq!(decoded_id, conn_id);
        assert_eq!(decoded_gen, cgen);
        assert_eq!(decoded_op, op);
    }
}
