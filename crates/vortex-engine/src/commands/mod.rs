//! Command handler dispatch for VortexDB engine.
//!
//! Each command is a free function `cmd_xxx(keyspace, args, now_nanos) -> CmdResult`
//! dispatched via a static match on the command name. No trait objects, no
//! dynamic dispatch — the compiler inlines the entire chain.

pub(crate) mod context;
pub(crate) mod key;
pub(crate) mod pattern;
pub(crate) mod server;
pub(crate) mod string;

use vortex_common::{VortexKey, VortexValue};
use vortex_proto::{FrameRef, RespFrame};

use crate::ConcurrentKeyspace;

/// Nanoseconds per second.
pub const NS_PER_SEC: u64 = 1_000_000_000;
/// Nanoseconds per millisecond.
pub const NS_PER_MS: u64 = 1_000_000;

/// The result of executing a command.
///
/// `Static` avoids allocation entirely for pre-computed wire bytes.
/// `Resp` wraps a `RespFrame` for dynamic responses.
#[derive(Debug)]
pub enum CmdResult {
    /// Pre-computed static RESP bytes — written directly to the wire.
    Static(&'static [u8]),
    /// Dynamic RESP frame requiring serialization.
    Resp(RespFrame),
}

// Pre-computed RESP constants for zero-allocation hot paths.
pub static RESP_OK: &[u8] = b"+OK\r\n";
pub static RESP_NIL: &[u8] = b"$-1\r\n";
pub static RESP_ZERO: &[u8] = b":0\r\n";
pub static RESP_ONE: &[u8] = b":1\r\n";
pub static RESP_NEG_ONE: &[u8] = b":-1\r\n";
pub static RESP_NEG_TWO: &[u8] = b":-2\r\n";
pub static RESP_EMPTY_BULK: &[u8] = b"$0\r\n\r\n";
pub static RESP_EMPTY_ARRAY: &[u8] = b"*0\r\n";

// Error responses.
pub static ERR_WRONG_TYPE: &[u8] =
    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
pub static ERR_SYNTAX: &[u8] = b"-ERR syntax error\r\n";
pub static ERR_NOT_INTEGER: &[u8] = b"-ERR value is not an integer or out of range\r\n";
pub static ERR_NOT_FLOAT: &[u8] = b"-ERR value is not a valid float\r\n";
pub static ERR_OVERFLOW: &[u8] = b"-ERR increment or decrement would overflow\r\n";
pub static ERR_BIT_OFFSET: &[u8] = b"-ERR bit offset is not an integer or out of range\r\n";

/// Execute a command against the shared concurrent keyspace.
///
/// `name` must be an uppercase ASCII command name (already normalized by
/// the CommandRouter). Returns `None` if the command is unknown to the
/// engine (connection-level commands like PING/QUIT are handled by the
/// reactor directly).
#[inline]
pub fn execute_command(
    keyspace: &ConcurrentKeyspace,
    name: &[u8],
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> Option<CmdResult> {
    match name {
        b"GET" => Some(string::cmd_get(keyspace, frame, now_nanos)),
        b"SET" => Some(string::cmd_set(keyspace, frame, now_nanos)),
        b"INCR" => Some(string::cmd_incr(keyspace, frame, now_nanos)),
        b"DECR" => Some(string::cmd_decr(keyspace, frame, now_nanos)),
        b"INCRBY" => Some(string::cmd_incrby(keyspace, frame, now_nanos)),
        b"DECRBY" => Some(string::cmd_decrby(keyspace, frame, now_nanos)),
        b"INCRBYFLOAT" => Some(string::cmd_incrbyfloat(keyspace, frame, now_nanos)),
        b"MGET" => Some(string::cmd_mget(keyspace, frame, now_nanos)),
        b"MSET" => Some(string::cmd_mset(keyspace, frame, now_nanos)),
        b"MSETNX" => Some(string::cmd_msetnx(keyspace, frame, now_nanos)),
        b"SETNX" => Some(string::cmd_setnx(keyspace, frame, now_nanos)),
        b"SETEX" => Some(string::cmd_setex(keyspace, frame, now_nanos)),
        b"PSETEX" => Some(string::cmd_psetex(keyspace, frame, now_nanos)),
        b"GETSET" => Some(string::cmd_getset(keyspace, frame, now_nanos)),
        b"GETDEL" => Some(string::cmd_getdel(keyspace, frame, now_nanos)),
        b"GETEX" => Some(string::cmd_getex(keyspace, frame, now_nanos)),
        b"APPEND" => Some(string::cmd_append(keyspace, frame, now_nanos)),
        b"STRLEN" => Some(string::cmd_strlen(keyspace, frame, now_nanos)),
        b"GETRANGE" => Some(string::cmd_getrange(keyspace, frame, now_nanos)),
        b"SETRANGE" => Some(string::cmd_setrange(keyspace, frame, now_nanos)),
        b"DEL" => Some(key::cmd_del(keyspace, frame, now_nanos)),
        b"UNLINK" => Some(key::cmd_unlink(keyspace, frame, now_nanos)),
        b"EXISTS" => Some(key::cmd_exists(keyspace, frame, now_nanos)),
        b"EXPIRE" => Some(key::cmd_expire(keyspace, frame, now_nanos)),
        b"PEXPIRE" => Some(key::cmd_pexpire(keyspace, frame, now_nanos)),
        b"EXPIREAT" => Some(key::cmd_expireat(keyspace, frame, now_nanos)),
        b"PEXPIREAT" => Some(key::cmd_pexpireat(keyspace, frame, now_nanos)),
        b"PERSIST" => Some(key::cmd_persist(keyspace, frame, now_nanos)),
        b"TTL" => Some(key::cmd_ttl(keyspace, frame, now_nanos)),
        b"PTTL" => Some(key::cmd_pttl(keyspace, frame, now_nanos)),
        b"EXPIRETIME" => Some(key::cmd_expiretime(keyspace, frame, now_nanos)),
        b"PEXPIRETIME" => Some(key::cmd_pexpiretime(keyspace, frame, now_nanos)),
        b"TYPE" => Some(key::cmd_type(keyspace, frame, now_nanos)),
        b"RENAME" => Some(key::cmd_rename(keyspace, frame, now_nanos)),
        b"RENAMENX" => Some(key::cmd_renamenx(keyspace, frame, now_nanos)),
        b"KEYS" => Some(key::cmd_keys(keyspace, frame, now_nanos)),
        b"SCAN" => Some(key::cmd_scan(keyspace, frame, now_nanos)),
        b"RANDOMKEY" => Some(key::cmd_randomkey(keyspace, frame, now_nanos)),
        b"TOUCH" => Some(key::cmd_touch(keyspace, frame, now_nanos)),
        b"COPY" => Some(key::cmd_copy(keyspace, frame, now_nanos)),
        b"PING" => Some(server::cmd_ping(keyspace, frame, now_nanos)),
        b"ECHO" => Some(server::cmd_echo(keyspace, frame, now_nanos)),
        b"QUIT" => Some(server::cmd_quit(keyspace, frame, now_nanos)),
        b"DBSIZE" => Some(server::cmd_dbsize(keyspace, frame, now_nanos)),
        b"FLUSHDB" => Some(server::cmd_flushdb(keyspace, frame, now_nanos)),
        b"FLUSHALL" => Some(server::cmd_flushall(keyspace, frame, now_nanos)),
        b"INFO" => Some(server::cmd_info(keyspace, frame, now_nanos)),
        b"COMMAND" => Some(server::cmd_command(keyspace, frame, now_nanos)),
        b"SELECT" => Some(server::cmd_select(keyspace, frame, now_nanos)),
        b"TIME" => Some(server::cmd_time(keyspace, frame, now_nanos)),
        b"MULTI" => Some(server::cmd_multi(keyspace, frame, now_nanos)),
        b"EXEC" => Some(server::cmd_exec(keyspace, frame, now_nanos)),
        b"DISCARD" => Some(server::cmd_discard(keyspace, frame, now_nanos)),
        b"WATCH" => Some(server::cmd_watch(keyspace, frame, now_nanos)),
        b"UNWATCH" => Some(server::cmd_unwatch(keyspace, frame, now_nanos)),
        _ => None,
    }
}

// ── Argument extraction helpers ─────────────────────────────────────────

/// Extract the Nth child's bytes from a FrameRef (0-indexed).
/// Child 0 is the command name.
#[inline]
pub fn arg_bytes<'a>(frame: &FrameRef<'a>, index: usize) -> Option<&'a [u8]> {
    let mut children = frame.children()?;
    for _ in 0..index {
        children.next()?;
    }
    children.next()?.as_bytes()
}

/// Count the number of children in a frame (including command name).
#[inline]
pub fn arg_count(frame: &FrameRef<'_>) -> usize {
    frame.element_count().unwrap_or(0) as usize
}

/// Parse a child as an i64.
#[inline]
pub fn arg_i64(frame: &FrameRef<'_>, index: usize) -> Option<i64> {
    let bytes = arg_bytes(frame, index)?;
    parse_i64(bytes)
}

/// Parse bytes as i64 (fast path for small numbers).
#[inline]
pub fn parse_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else if bytes[0] == b'+' {
        (false, &bytes[1..])
    } else {
        (false, bytes)
    };
    if digits.is_empty() {
        return None;
    }
    let mut n: i64 = 0;
    if neg {
        for &b in digits {
            if !b.is_ascii_digit() {
                return None;
            }
            n = n.checked_mul(10)?;
            n = n.checked_sub((b - b'0') as i64)?;
        }
        Some(n)
    } else {
        for &b in digits {
            if !b.is_ascii_digit() {
                return None;
            }
            n = n.checked_mul(10)?;
            n = n.checked_add((b - b'0') as i64)?;
        }
        Some(n)
    }
}

/// Build a `VortexKey` from a byte slice.
#[inline]
pub fn key_from_bytes(bytes: &[u8]) -> VortexKey {
    VortexKey::from(bytes)
}

/// Build a `VortexValue` from a byte slice, using inline storage when possible.
/// Also tries to detect integers for zero-allocation integer encoding.
#[inline]
pub fn value_from_bytes(bytes: &[u8]) -> VortexValue {
    // Try integer encoding first (Redis stores "42" as Integer(42)).
    if !bytes.is_empty() && bytes.len() <= 20 {
        if let Some(n) = parse_i64(bytes) {
            return VortexValue::Integer(n);
        }
    }
    VortexValue::from_bytes(bytes)
}

/// Serialize a VortexValue to a RESP bulk string CmdResult.
#[inline]
pub fn value_to_resp(val: &VortexValue) -> CmdResult {
    match val {
        VortexValue::InlineString(ib) => CmdResult::Resp(RespFrame::bulk_string(
            bytes::Bytes::copy_from_slice(ib.as_bytes()),
        )),
        VortexValue::String(b) => CmdResult::Resp(RespFrame::bulk_string(b.clone())),
        VortexValue::Integer(n) => {
            // Serialize integer as its string representation (Redis behavior).
            let mut buf = itoa::Buffer::new();
            let s = buf.format(*n);
            CmdResult::Resp(RespFrame::bulk_string(bytes::Bytes::copy_from_slice(
                s.as_bytes(),
            )))
        }
        _ => CmdResult::Static(ERR_WRONG_TYPE),
    }
}

/// Serialize an owned VortexValue to a RESP bulk string CmdResult.
#[inline]
pub fn owned_value_to_resp(val: VortexValue) -> CmdResult {
    match val {
        VortexValue::InlineString(ib) => CmdResult::Resp(RespFrame::bulk_string(
            bytes::Bytes::copy_from_slice(ib.as_bytes()),
        )),
        VortexValue::String(b) => CmdResult::Resp(RespFrame::bulk_string(b)),
        VortexValue::Integer(n) => {
            let mut buf = itoa::Buffer::new();
            let s = buf.format(n);
            CmdResult::Resp(RespFrame::bulk_string(bytes::Bytes::copy_from_slice(
                s.as_bytes(),
            )))
        }
        _ => CmdResult::Static(ERR_WRONG_TYPE),
    }
}

/// Integer response.
#[inline]
pub fn int_resp(n: i64) -> CmdResult {
    CmdResult::Resp(RespFrame::integer(n))
}

#[cfg(test)]
pub(crate) mod test_harness {
    use crate::concurrent_keyspace::ConcurrentKeyspace;
    use vortex_common::{VortexKey, VortexValue};

    pub struct TestHarness {
        pub keyspace: ConcurrentKeyspace,
    }

    impl TestHarness {
        pub fn new() -> Self {
            Self {
                keyspace: ConcurrentKeyspace::new(64),
            }
        }

        pub fn set(&self, key: VortexKey, value: VortexValue) {
            let idx = self.keyspace.shard_index(key.as_bytes());
            self.keyspace.write_shard_by_index(idx).insert(key, value);
        }

        pub fn set_with_ttl(&self, key: VortexKey, value: VortexValue, ttl_deadline: u64) {
            let idx = self.keyspace.shard_index(key.as_bytes());
            self.keyspace
                .write_shard_by_index(idx)
                .insert_with_ttl(key, value, ttl_deadline);
        }

        pub fn get(&self, key: &VortexKey, now: u64) -> Option<VortexValue> {
            let idx = self.keyspace.shard_index(key.as_bytes());
            let mut guard = self.keyspace.write_shard_by_index(idx);
            guard.get_or_expire(key, now).cloned()
        }

        pub fn exists(&self, key: &VortexKey, now: u64) -> bool {
            self.get(key, now).is_some()
        }

        pub fn len(&self) -> usize {
            self.keyspace.dbsize()
        }
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    #[test]
    fn parse_i64_valid() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"42"), Some(42));
        assert_eq!(parse_i64(b"-1"), Some(-1));
        assert_eq!(parse_i64(b"+100"), Some(100));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
    }

    #[test]
    fn parse_i64_invalid() {
        assert_eq!(parse_i64(b""), None);
        assert_eq!(parse_i64(b"abc"), None);
        assert_eq!(parse_i64(b"12.5"), None);
        assert_eq!(parse_i64(b"-"), None);
        // Overflow:
        assert_eq!(parse_i64(b"9223372036854775808"), None);
    }

    #[test]
    fn value_from_bytes_integer() {
        assert_eq!(value_from_bytes(b"42"), VortexValue::Integer(42));
        assert_eq!(value_from_bytes(b"-1"), VortexValue::Integer(-1));
        assert_eq!(value_from_bytes(b"0"), VortexValue::Integer(0));
    }

    #[test]
    fn value_from_bytes_string() {
        let val = value_from_bytes(b"hello");
        assert!(matches!(val, VortexValue::InlineString(_)));
    }
}
