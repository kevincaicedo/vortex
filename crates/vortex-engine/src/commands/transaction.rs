//! Transaction command boundary.
//!
//! Redis transactions are connection-scoped: `MULTI` flips a per-client
//! queueing bit, `WATCH` records per-client optimistic read sets, and `EXEC`
//! drains the queue for that client. The reactor owns that state because it is
//! already partitioned by connection. Keeping it out of the shared keyspace
//! avoids global locks on the normal GET/SET path and keeps watch bookkeeping
//! cold when no clients use transactions.
//!
//! This module contains only the stateless RESP semantics that are valid when
//! the engine is invoked without an active connection transaction. The
//! connection-stateful implementation lives in `vortex-io::reactor`.

use vortex_proto::FrameRef;

use super::{CmdResult, RESP_OK};
use crate::ConcurrentKeyspace;

/// EXEC
///
/// Executes all commands queued after `MULTI` and returns an array containing
/// each command reply in queue order.
///
/// Big-O: `O(N + W)` in the reactor, where `N` is the number of queued
/// commands and `W` is the number of watched keys that must be checked before
/// execution. This stateless engine fallback is `O(1)`.
///
/// Compatibility: Redis-compatible outside an active transaction: returns
/// `ERR EXEC without MULTI`. Successful execution, queue-time `EXECABORT`,
/// runtime errors inside the reply array, and WATCH aborts are implemented in
/// the reactor because they require per-connection state.
///
/// Notes: The reactor executes `EXEC` under the transaction gate so no other
/// command can interleave between the WATCH validation point and the queued
/// mutations. This function must not start or drain a transaction by itself.
#[inline]
pub fn cmd_exec(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    CmdResult::Static(b"-ERR EXEC without MULTI\r\n")
}

/// DISCARD
///
/// Flushes the queued commands for the current transaction, exits `MULTI`
/// mode, and clears watched keys for the connection.
///
/// Big-O: `O(N + W)` in the reactor, where `N` is queued commands and `W` is
/// watched keys. This stateless engine fallback is `O(1)`.
///
/// Compatibility: Redis-compatible outside an active transaction: returns
/// `ERR DISCARD without MULTI`. The successful in-transaction path is
/// connection-scoped and handled by the reactor.
///
/// Notes: `DISCARD` never executes queued commands, so it must not emit AOF
/// records for the discarded payloads.
#[inline]
pub fn cmd_discard(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    CmdResult::Static(b"-ERR DISCARD without MULTI\r\n")
}

/// UNWATCH
///
/// Clears all keys watched by the current connection.
///
/// Big-O: `O(W)` in the reactor, where `W` is the number of watched keys for
/// the connection. This stateless engine fallback is `O(1)`.
///
/// Compatibility: Redis-compatible with no active WATCH registrations:
/// returns `OK`. When watches exist, the reactor removes each registration and
/// returns the same `OK` reply.
///
/// Notes: `EXEC`, successful `DISCARD`, and connection close also clear
/// watches. The no-watch case is intentionally a static response so the common
/// compatibility probe allocates nothing.
#[inline]
pub fn cmd_unwatch(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    CmdResult::Static(RESP_OK)
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
    fn exec_without_multi_is_redis_error() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"EXEC"]);
        assert_static(&r, b"-ERR EXEC without MULTI\r\n");
    }

    #[test]
    fn discard_without_multi_is_redis_error() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"DISCARD"]);
        assert_static(&r, b"-ERR DISCARD without MULTI\r\n");
    }

    #[test]
    fn unwatch_without_watches_is_ok() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"UNWATCH"]);
        assert_static(&r, RESP_OK);
    }

    #[test]
    fn multi_and_watch_are_reactor_only() {
        let h = TestHarness::new();
        let multi = make_resp(&[b"MULTI"]);
        let tape = RespTape::parse_pipeline(&multi).expect("valid RESP");
        let frame = tape.iter().next().unwrap();
        assert!(crate::commands::execute_command(&h.keyspace, b"MULTI", &frame, 0).is_none());

        let watch = make_resp(&[b"WATCH", b"k"]);
        let tape = RespTape::parse_pipeline(&watch).expect("valid RESP");
        let frame = tape.iter().next().unwrap();
        assert!(crate::commands::execute_command(&h.keyspace, b"WATCH", &frame, 0).is_none());
    }
}
