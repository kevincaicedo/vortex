//! Connection command handlers.
//!
//! Implements Redis connection-category commands whose behavior is either
//! stateless or whose state is owned by the reactor.

use vortex_proto::{FrameRef, RespFrame};

use super::{CmdResult, CommandArgs, RESP_OK};
use crate::ConcurrentKeyspace;

static RESP_PONG: &[u8] = b"+PONG\r\n";
static ERR_DB_INDEX: &[u8] = b"-ERR DB index is out of range\r\n";

/// PING [message]
///
/// Returns `PONG` when called without a message, otherwise returns the message
/// as a bulk string.
///
/// Big-O: `O(1)` without a message, `O(M)` with a message of `M` bytes because
/// the response owns a copy of the payload.
///
/// Compatibility: Redis-compatible for the request/response forms supported
/// by RESP2. Pub/Sub-mode PING is owned by the reactor when Pub/Sub is added.
///
/// Notes: The no-argument hot path returns static bytes and does not touch the
/// keyspace or allocate.
#[inline]
pub fn cmd_ping(
    _keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    let Some(args) = CommandArgs::collect(frame) else {
        return CmdResult::Static(RESP_PONG);
    };
    if args.len() <= 1 {
        return CmdResult::Static(RESP_PONG);
    }
    if let Some(msg) = args.get(1) {
        CmdResult::Resp(RespFrame::bulk_string(bytes::Bytes::copy_from_slice(msg)))
    } else {
        CmdResult::Static(RESP_PONG)
    }
}

/// ECHO message
///
/// Returns the provided message as a bulk string.
///
/// Big-O: `O(M)` for message length `M`.
///
/// Compatibility: Redis-compatible when the command router has validated the
/// exact arity. Malformed direct engine calls return a nil bulk string.
///
/// Notes: This command is intentionally keyspace-free; it is safe to execute
/// while write traffic is blocked by persistence failure.
#[inline]
pub fn cmd_echo(
    _keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    if let Some(msg) = CommandArgs::collect(frame).and_then(|args| args.get(1)) {
        CmdResult::Resp(RespFrame::bulk_string(bytes::Bytes::copy_from_slice(msg)))
    } else {
        CmdResult::Static(b"$-1\r\n")
    }
}

/// QUIT
///
/// Returns `OK`; the reactor closes the connection after writing the reply.
///
/// Big-O: `O(1)`.
///
/// Compatibility: Redis-compatible RESP2 behavior for an explicit QUIT.
///
/// Notes: Closing the socket must stay in the reactor because only the reactor
/// owns file descriptors and pending write buffers.
#[inline]
pub fn cmd_quit(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    CmdResult::Static(RESP_OK)
}

/// SELECT index
///
/// Selects the logical database for the connection.
///
/// Big-O: `O(1)`.
///
/// Compatibility: Vortex alpha exposes a single logical database. `SELECT 0`
/// returns `OK`; any other index returns Redis-compatible
/// `ERR DB index is out of range`.
///
/// Notes: The one-DB design avoids per-command database indirection on the hot
/// path. Multi-DB support would require a connection-local DB id and either a
/// keyspace array or DB id in the key hash domain.
#[inline]
pub fn cmd_select(
    _keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    if let Some(idx_bytes) = CommandArgs::collect(frame).and_then(|args| args.get(1)) {
        if idx_bytes == b"0" {
            return CmdResult::Static(RESP_OK);
        }
    }
    CmdResult::Static(ERR_DB_INDEX)
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::commands::test_harness::TestHarness;
    use vortex_proto::RespTape;

    fn make_resp(parts: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            buf.extend_from_slice(part);
            buf.extend_from_slice(b"\r\n");
        }
        buf
    }

    fn exec(h: &TestHarness, parts: &[&[u8]]) -> CmdResult {
        let wire = make_resp(parts);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().unwrap();
        let name_upper: Vec<u8> = parts[0].iter().map(|b| b.to_ascii_uppercase()).collect();
        crate::commands::execute_command(&h.keyspace, &name_upper, &frame, 0)
            .expect("command should be recognized")
            .response
    }

    fn assert_static(r: &CmdResult, expected: &[u8]) {
        match r {
            CmdResult::Static(s) => assert_eq!(*s, expected, "static mismatch"),
            CmdResult::Inline(_) => panic!("expected Static, got Inline"),
            CmdResult::Resp(f) => panic!("expected Static, got Resp: {f:?}"),
        }
    }

    #[test]
    fn ping_no_args() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"PING"]);
        assert_static(&r, RESP_PONG);
    }

    #[test]
    fn ping_with_message() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"PING", b"hello"]);
        match &r {
            CmdResult::Resp(RespFrame::BulkString(Some(b))) => {
                assert_eq!(b.as_ref(), b"hello" as &[u8]);
            }
            other => panic!("expected BulkString, got {other:?}"),
        }
    }

    #[test]
    fn echo_returns_message() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"ECHO", b"world"]);
        match &r {
            CmdResult::Resp(RespFrame::BulkString(Some(b))) => {
                assert_eq!(b.as_ref(), b"world" as &[u8]);
            }
            other => panic!("expected BulkString, got {other:?}"),
        }
    }

    #[test]
    fn quit_returns_ok() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"QUIT"]);
        assert_static(&r, RESP_OK);
    }

    #[test]
    fn select_zero_ok() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"SELECT", b"0"]);
        assert_static(&r, RESP_OK);
    }

    #[test]
    fn select_nonzero_error() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"SELECT", b"1"]);
        assert_static(&r, ERR_DB_INDEX);
    }
}
