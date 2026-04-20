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

use smallvec::SmallVec;
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
    /// Pre-serialized inline RESP bytes for tiny dynamic replies.
    Inline(InlineResp),
    /// Dynamic RESP frame requiring serialization.
    Resp(RespFrame),
}

#[derive(Debug, Clone, Copy)]
pub struct InlineResp {
    len: u8,
    buf: [u8; 32],
}

impl InlineResp {
    #[inline]
    pub fn bulk_from_payload(payload: &[u8]) -> Self {
        debug_assert!(
            payload.len() <= 23,
            "InlineResp only supports tiny bulk strings"
        );

        let mut buf = [0u8; 32];
        let mut cursor = 0usize;
        buf[cursor] = b'$';
        cursor += 1;

        let len = payload.len();
        if len >= 10 {
            buf[cursor] = b'0' + (len / 10) as u8;
            cursor += 1;
        }
        buf[cursor] = b'0' + (len % 10) as u8;
        cursor += 1;
        buf[cursor] = b'\r';
        cursor += 1;
        buf[cursor] = b'\n';
        cursor += 1;
        buf[cursor..cursor + payload.len()].copy_from_slice(payload);
        cursor += payload.len();
        buf[cursor] = b'\r';
        cursor += 1;
        buf[cursor] = b'\n';
        cursor += 1;

        Self {
            len: cursor as u8,
            buf,
        }
    }

    #[inline]
    pub fn bulk_from_i64(n: i64) -> Self {
        let mut itoa_buf = itoa::Buffer::new();
        let text = itoa_buf.format(n);
        Self::bulk_from_payload(text.as_bytes())
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }

    #[inline]
    pub fn payload(&self) -> &[u8] {
        let total = self.len as usize;
        let payload_start = if self.buf[2] == b'\r' { 4 } else { 5 };
        &self.buf[payload_start..total - 2]
    }
}

#[derive(Debug)]
pub struct ExecutedCommand {
    pub response: CmdResult,
    pub aof_lsn: Option<u64>,
}

impl ExecutedCommand {
    #[inline]
    pub const fn with_aof_lsn(response: CmdResult, aof_lsn: Option<u64>) -> Self {
        Self { response, aof_lsn }
    }
}

impl From<CmdResult> for ExecutedCommand {
    #[inline]
    fn from(response: CmdResult) -> Self {
        Self {
            response,
            aof_lsn: None,
        }
    }
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
) -> Option<ExecutedCommand> {
    match name {
        b"GET" => Some(string::cmd_get(keyspace, frame, now_nanos).into()),
        b"SET" => Some(string::cmd_set(keyspace, frame, now_nanos).into()),
        b"INCR" => Some(string::cmd_incr(keyspace, frame, now_nanos).into()),
        b"DECR" => Some(string::cmd_decr(keyspace, frame, now_nanos).into()),
        b"INCRBY" => Some(string::cmd_incrby(keyspace, frame, now_nanos).into()),
        b"DECRBY" => Some(string::cmd_decrby(keyspace, frame, now_nanos).into()),
        b"INCRBYFLOAT" => Some(string::cmd_incrbyfloat(keyspace, frame, now_nanos).into()),
        b"MGET" => Some(string::cmd_mget(keyspace, frame, now_nanos).into()),
        b"MSET" => Some(string::cmd_mset(keyspace, frame, now_nanos).into()),
        b"MSETNX" => Some(string::cmd_msetnx(keyspace, frame, now_nanos).into()),
        b"SETNX" => Some(string::cmd_setnx(keyspace, frame, now_nanos).into()),
        b"SETEX" => Some(string::cmd_setex(keyspace, frame, now_nanos).into()),
        b"PSETEX" => Some(string::cmd_psetex(keyspace, frame, now_nanos).into()),
        b"GETSET" => Some(string::cmd_getset(keyspace, frame, now_nanos).into()),
        b"GETDEL" => Some(string::cmd_getdel(keyspace, frame, now_nanos).into()),
        b"GETEX" => Some(string::cmd_getex(keyspace, frame, now_nanos).into()),
        b"APPEND" => Some(string::cmd_append(keyspace, frame, now_nanos).into()),
        b"STRLEN" => Some(string::cmd_strlen(keyspace, frame, now_nanos).into()),
        b"GETRANGE" => Some(string::cmd_getrange(keyspace, frame, now_nanos).into()),
        b"SETRANGE" => Some(string::cmd_setrange(keyspace, frame, now_nanos).into()),
        b"DEL" => Some(key::cmd_del(keyspace, frame, now_nanos).into()),
        b"UNLINK" => Some(key::cmd_unlink(keyspace, frame, now_nanos).into()),
        b"EXISTS" => Some(key::cmd_exists(keyspace, frame, now_nanos).into()),
        b"EXPIRE" => Some(key::cmd_expire(keyspace, frame, now_nanos).into()),
        b"PEXPIRE" => Some(key::cmd_pexpire(keyspace, frame, now_nanos).into()),
        b"EXPIREAT" => Some(key::cmd_expireat(keyspace, frame, now_nanos).into()),
        b"PEXPIREAT" => Some(key::cmd_pexpireat(keyspace, frame, now_nanos).into()),
        b"PERSIST" => Some(key::cmd_persist(keyspace, frame, now_nanos).into()),
        b"TTL" => Some(key::cmd_ttl(keyspace, frame, now_nanos).into()),
        b"PTTL" => Some(key::cmd_pttl(keyspace, frame, now_nanos).into()),
        b"EXPIRETIME" => Some(key::cmd_expiretime(keyspace, frame, now_nanos).into()),
        b"PEXPIRETIME" => Some(key::cmd_pexpiretime(keyspace, frame, now_nanos).into()),
        b"TYPE" => Some(key::cmd_type(keyspace, frame, now_nanos).into()),
        b"RENAME" => Some(key::cmd_rename(keyspace, frame, now_nanos).into()),
        b"RENAMENX" => Some(key::cmd_renamenx(keyspace, frame, now_nanos).into()),
        b"KEYS" => Some(key::cmd_keys(keyspace, frame, now_nanos).into()),
        b"SCAN" => Some(key::cmd_scan(keyspace, frame, now_nanos).into()),
        b"RANDOMKEY" => Some(key::cmd_randomkey(keyspace, frame, now_nanos).into()),
        b"TOUCH" => Some(key::cmd_touch(keyspace, frame, now_nanos).into()),
        b"COPY" => Some(key::cmd_copy(keyspace, frame, now_nanos).into()),
        b"PING" => Some(server::cmd_ping(keyspace, frame, now_nanos).into()),
        b"ECHO" => Some(server::cmd_echo(keyspace, frame, now_nanos).into()),
        b"QUIT" => Some(server::cmd_quit(keyspace, frame, now_nanos).into()),
        b"DBSIZE" => Some(server::cmd_dbsize(keyspace, frame, now_nanos).into()),
        b"FLUSHDB" => Some(server::cmd_flushdb(keyspace, frame, now_nanos).into()),
        b"FLUSHALL" => Some(server::cmd_flushall(keyspace, frame, now_nanos).into()),
        b"INFO" => Some(server::cmd_info(keyspace, frame, now_nanos).into()),
        b"COMMAND" => Some(server::cmd_command(keyspace, frame, now_nanos).into()),
        b"SELECT" => Some(server::cmd_select(keyspace, frame, now_nanos).into()),
        b"TIME" => Some(server::cmd_time(keyspace, frame, now_nanos).into()),
        b"MULTI" => Some(server::cmd_multi(keyspace, frame, now_nanos).into()),
        b"EXEC" => Some(server::cmd_exec(keyspace, frame, now_nanos).into()),
        b"DISCARD" => Some(server::cmd_discard(keyspace, frame, now_nanos).into()),
        b"WATCH" => Some(server::cmd_watch(keyspace, frame, now_nanos).into()),
        b"UNWATCH" => Some(server::cmd_unwatch(keyspace, frame, now_nanos).into()),
        _ => None,
    }
}

// ── Argument extraction helpers ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CommandArgs<'a> {
    args: SmallVec<[&'a [u8]; 8]>,
}

impl<'a> CommandArgs<'a> {
    #[inline]
    pub fn collect(frame: &FrameRef<'a>) -> Option<Self> {
        let mut children = frame.children()?;
        let mut args = SmallVec::with_capacity(frame.element_count()? as usize);
        while let Some(child) = children.next() {
            args.push(child.as_bytes()?);
        }
        Some(Self { args })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.args.len()
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&'a [u8]> {
        self.args.get(index).copied()
    }

    #[inline]
    pub fn i64(&self, index: usize) -> Option<i64> {
        parse_i64(self.get(index)?)
    }

    #[inline]
    pub fn iter_from(&self, start: usize) -> impl Iterator<Item = &'a [u8]> + '_ {
        self.args.iter().skip(start).copied()
    }
}

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
        VortexValue::InlineString(ib) => {
            CmdResult::Inline(InlineResp::bulk_from_payload(ib.as_bytes()))
        }
        VortexValue::String(b) => CmdResult::Resp(RespFrame::bulk_string(b.clone())),
        VortexValue::Integer(n) => CmdResult::Inline(InlineResp::bulk_from_i64(*n)),
        _ => CmdResult::Static(ERR_WRONG_TYPE),
    }
}

/// Serialize an owned VortexValue to a RESP bulk string CmdResult.
#[inline]
pub fn owned_value_to_resp(val: VortexValue) -> CmdResult {
    match val {
        VortexValue::InlineString(ib) => {
            CmdResult::Inline(InlineResp::bulk_from_payload(ib.as_bytes()))
        }
        VortexValue::String(b) => CmdResult::Resp(RespFrame::bulk_string(b)),
        VortexValue::Integer(n) => CmdResult::Inline(InlineResp::bulk_from_i64(n)),
        _ => CmdResult::Static(ERR_WRONG_TYPE),
    }
}

/// Integer response — uses pre-computed static bytes for common values.
#[inline]
pub fn int_resp(n: i64) -> CmdResult {
    match n {
        0 => CmdResult::Static(RESP_ZERO),
        1 => CmdResult::Static(RESP_ONE),
        -1 => CmdResult::Static(RESP_NEG_ONE),
        -2 => CmdResult::Static(RESP_NEG_TWO),
        _ => CmdResult::Resp(RespFrame::integer(n)),
    }
}

#[cfg(test)]
pub(crate) mod test_harness {
    use crate::commands::context::SetOptions;
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
            let _ = self
                .keyspace
                .set_value_with_options(key, value, SetOptions::default(), 0);
        }

        pub fn set_with_ttl(&self, key: VortexKey, value: VortexValue, ttl_deadline: u64) {
            let _ = self.keyspace.set_value_with_options(
                key,
                value,
                SetOptions {
                    ttl_deadline,
                    ..SetOptions::default()
                },
                0,
            );
        }

        pub fn get(&self, key: &VortexKey, now: u64) -> Option<VortexValue> {
            self.keyspace.get_value(key, now)
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
