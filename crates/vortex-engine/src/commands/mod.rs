//! Command handler dispatch for VortexDB engine.
//!
//! Each command is a free function `cmd_xxx(shard, args, now_nanos) -> CmdResult`
//! dispatched via a static match on the command name. No trait objects, no
//! dynamic dispatch — the compiler inlines the entire chain.

pub mod key;
pub mod server;
pub mod string;

use vortex_common::{VortexKey, VortexValue};
use vortex_proto::{FrameRef, RespFrame};

use crate::Shard;

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

/// Execute a command by name.
///
/// `name` must be an uppercase ASCII command name (already normalized by
/// the CommandRouter). Returns `None` if the command is unknown to the
/// engine (connection-level commands like PING/QUIT are handled by the
/// reactor directly).
#[inline]
pub fn execute_command(
    shard: &mut Shard,
    name: &[u8],
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> Option<CmdResult> {
    // String commands — ordered by frequency in typical workloads.
    match name {
        b"GET" => Some(string::cmd_get(shard, frame, now_nanos)),
        b"SET" => Some(string::cmd_set(shard, frame, now_nanos)),
        b"INCR" => Some(string::cmd_incr(shard, frame, now_nanos)),
        b"DECR" => Some(string::cmd_decr(shard, frame, now_nanos)),
        b"INCRBY" => Some(string::cmd_incrby(shard, frame, now_nanos)),
        b"DECRBY" => Some(string::cmd_decrby(shard, frame, now_nanos)),
        b"INCRBYFLOAT" => Some(string::cmd_incrbyfloat(shard, frame, now_nanos)),
        b"MGET" => Some(string::cmd_mget(shard, frame, now_nanos)),
        b"MSET" => Some(string::cmd_mset(shard, frame, now_nanos)),
        b"MSETNX" => Some(string::cmd_msetnx(shard, frame, now_nanos)),
        b"SETNX" => Some(string::cmd_setnx(shard, frame, now_nanos)),
        b"SETEX" => Some(string::cmd_setex(shard, frame, now_nanos)),
        b"PSETEX" => Some(string::cmd_psetex(shard, frame, now_nanos)),
        b"GETSET" => Some(string::cmd_getset(shard, frame, now_nanos)),
        b"GETDEL" => Some(string::cmd_getdel(shard, frame, now_nanos)),
        b"GETEX" => Some(string::cmd_getex(shard, frame, now_nanos)),
        b"APPEND" => Some(string::cmd_append(shard, frame, now_nanos)),
        b"STRLEN" => Some(string::cmd_strlen(shard, frame, now_nanos)),
        b"GETRANGE" => Some(string::cmd_getrange(shard, frame, now_nanos)),
        b"SETRANGE" => Some(string::cmd_setrange(shard, frame, now_nanos)),
        // Key management commands.
        b"DEL" => Some(key::cmd_del(shard, frame, now_nanos)),
        b"UNLINK" => Some(key::cmd_unlink(shard, frame, now_nanos)),
        b"EXISTS" => Some(key::cmd_exists(shard, frame, now_nanos)),
        b"EXPIRE" => Some(key::cmd_expire(shard, frame, now_nanos)),
        b"PEXPIRE" => Some(key::cmd_pexpire(shard, frame, now_nanos)),
        b"EXPIREAT" => Some(key::cmd_expireat(shard, frame, now_nanos)),
        b"PEXPIREAT" => Some(key::cmd_pexpireat(shard, frame, now_nanos)),
        b"PERSIST" => Some(key::cmd_persist(shard, frame, now_nanos)),
        b"TTL" => Some(key::cmd_ttl(shard, frame, now_nanos)),
        b"PTTL" => Some(key::cmd_pttl(shard, frame, now_nanos)),
        b"EXPIRETIME" => Some(key::cmd_expiretime(shard, frame, now_nanos)),
        b"PEXPIRETIME" => Some(key::cmd_pexpiretime(shard, frame, now_nanos)),
        b"TYPE" => Some(key::cmd_type(shard, frame, now_nanos)),
        b"RENAME" => Some(key::cmd_rename(shard, frame, now_nanos)),
        b"RENAMENX" => Some(key::cmd_renamenx(shard, frame, now_nanos)),
        b"KEYS" => Some(key::cmd_keys(shard, frame, now_nanos)),
        b"SCAN" => Some(key::cmd_scan(shard, frame, now_nanos)),
        b"RANDOMKEY" => Some(key::cmd_randomkey(shard, frame, now_nanos)),
        b"TOUCH" => Some(key::cmd_touch(shard, frame, now_nanos)),
        b"COPY" => Some(key::cmd_copy(shard, frame, now_nanos)),
        // Server & connection commands.
        b"PING" => Some(server::cmd_ping(shard, frame, now_nanos)),
        b"ECHO" => Some(server::cmd_echo(shard, frame, now_nanos)),
        b"QUIT" => Some(server::cmd_quit(shard, frame, now_nanos)),
        b"DBSIZE" => Some(server::cmd_dbsize(shard, frame, now_nanos)),
        b"FLUSHDB" => Some(server::cmd_flushdb(shard, frame, now_nanos)),
        b"FLUSHALL" => Some(server::cmd_flushall(shard, frame, now_nanos)),
        b"INFO" => Some(server::cmd_info(shard, frame, now_nanos)),
        b"COMMAND" => Some(server::cmd_command(shard, frame, now_nanos)),
        b"SELECT" => Some(server::cmd_select(shard, frame, now_nanos)),
        b"TIME" => Some(server::cmd_time(shard, frame, now_nanos)),
        b"MULTI" => Some(server::cmd_multi(shard, frame, now_nanos)),
        b"EXEC" => Some(server::cmd_exec(shard, frame, now_nanos)),
        b"DISCARD" => Some(server::cmd_discard(shard, frame, now_nanos)),
        b"WATCH" => Some(server::cmd_watch(shard, frame, now_nanos)),
        b"UNWATCH" => Some(server::cmd_unwatch(shard, frame, now_nanos)),
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
