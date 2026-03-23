//! I/O backend abstraction for the reactor event loop.
//!
//! The [`IoBackend`] trait provides a completion-based I/O interface that
//! abstracts over io_uring (Linux) and polling/kqueue/epoll (cross-platform).
//! Both backends produce [`Completion`] events that the reactor processes
//! uniformly.

pub mod polling;

#[cfg(target_os = "linux")]
pub mod uring;

pub use polling::PollingBackend;

#[cfg(target_os = "linux")]
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
}

impl OpType {
    /// Decode from the low 8 bits of a token.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Accept),
            1 => Some(Self::Read),
            2 => Some(Self::Write),
            3 => Some(Self::Close),
            _ => None,
        }
    }
}

/// Encode a connection token, generation counter, and operation type into a single u64.
///
/// Layout: `(conn_id << 16) | (generation << 8) | op_type`
///
/// The generation counter prevents stale CQE processing after a slab slot is
/// reused by a new connection while io_uring operations for the old connection
/// are still in flight.
#[inline]
pub fn encode_token(conn_id: usize, generation: u8, op: OpType) -> u64 {
    ((conn_id as u64) << 16) | ((generation as u64) << 8) | (op as u64)
}

/// Decode a token into (connection_id, generation, operation_type).
#[inline]
pub fn decode_token(token: u64) -> (usize, u8, OpType) {
    let op = OpType::from_u8((token & 0xFF) as u8).unwrap_or(OpType::Accept);
    let generation = ((token >> 8) & 0xFF) as u8;
    let conn_id = (token >> 16) as usize;
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
        let cgen = 255;
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
        let cgen: u8 = 255;
        let token = encode_token(conn_id, cgen.wrapping_add(1), OpType::Read);
        let (decoded_id, decoded_gen, _) = decode_token(token);
        assert_eq!(decoded_id, conn_id);
        assert_eq!(decoded_gen, 0); // Wrapped around
    }
}
