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
use vortex_common::{
    VortexKey, VortexValue,
    absolute_unix_nanos_to_deadline_nanos as common_absolute_unix_nanos_to_deadline_nanos,
    current_unix_time_nanos,
    deadline_nanos_to_absolute_unix_nanos as common_deadline_nanos_to_absolute_unix_nanos,
};
use vortex_proto::{FrameRef, RespFrame};

use crate::ConcurrentKeyspace;

/// Nanoseconds per second.
pub const NS_PER_SEC: u64 = 1_000_000_000;
/// Nanoseconds per millisecond.
pub const NS_PER_MS: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, Default)]
pub struct CommandClock {
    pub monotonic_nanos: u64,
    pub unix_nanos: u64,
}

impl CommandClock {
    #[inline]
    pub const fn new(monotonic_nanos: u64, unix_nanos: u64) -> Self {
        Self {
            monotonic_nanos,
            unix_nanos,
        }
    }
}

impl From<u64> for CommandClock {
    #[inline]
    fn from(now_nanos: u64) -> Self {
        Self::new(now_nanos, now_nanos)
    }
}

#[inline]
pub(crate) fn resolve_unix_time_now_nanos(unix_now_nanos: u64) -> u64 {
    if unix_now_nanos == 0 {
        current_unix_time_nanos()
    } else {
        unix_now_nanos
    }
}

#[inline]
fn resolve_deadline_clock_now_nanos(now_nanos: u64, unix_now_nanos: u64) -> u64 {
    if unix_now_nanos == 0 {
        now_nanos
    } else {
        unix_now_nanos
    }
}

#[inline]
pub(crate) fn absolute_unix_nanos_to_deadline_nanos(
    absolute_unix_nanos: u64,
    now_nanos: u64,
    unix_now_nanos: u64,
) -> u64 {
    common_absolute_unix_nanos_to_deadline_nanos(
        absolute_unix_nanos,
        now_nanos,
        resolve_deadline_clock_now_nanos(now_nanos, unix_now_nanos),
    )
}

#[inline]
pub(crate) fn deadline_nanos_to_absolute_unix_nanos(
    deadline_nanos: u64,
    now_nanos: u64,
    unix_now_nanos: u64,
) -> u64 {
    common_deadline_nanos_to_absolute_unix_nanos(
        deadline_nanos,
        now_nanos,
        resolve_deadline_clock_now_nanos(now_nanos, unix_now_nanos),
    )
}

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

impl CmdResult {
    #[inline]
    pub fn is_error(&self) -> bool {
        match self {
            Self::Static(buf) => buf.first() == Some(&b'-'),
            Self::Inline(inline) => inline.as_bytes().first() == Some(&b'-'),
            Self::Resp(frame) => matches!(frame, RespFrame::Error(_)),
        }
    }
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
pub struct AofRecord {
    pub lsn: u64,
    pub key: VortexKey,
}

pub type AofRecords = Option<Box<[AofRecord]>>;
pub const NO_AOF_LSN: u64 = u64::MAX;

#[inline]
fn encode_aof_lsn(aof_lsn: Option<u64>) -> u64 {
    aof_lsn.unwrap_or(NO_AOF_LSN)
}

#[derive(Debug)]
pub struct ExecutedCommand {
    pub response: CmdResult,
    pub aof_records: AofRecords,
    pub aof_lsn: u64,
    pub aof_payload: Option<Box<[u8]>>,
}

impl ExecutedCommand {
    #[inline]
    pub fn with_aof_lsn(response: CmdResult, aof_lsn: Option<u64>) -> Self {
        Self {
            response,
            aof_records: None,
            aof_lsn: encode_aof_lsn(aof_lsn),
            aof_payload: None,
        }
    }

    #[inline]
    pub fn with_optional_aof_payload(
        response: CmdResult,
        aof_lsn: Option<u64>,
        aof_payload: Option<Box<[u8]>>,
    ) -> Self {
        Self::with_optional_aof_payload_and_records(response, None, aof_lsn, aof_payload)
    }

    #[inline]
    pub fn with_aof_records(
        response: CmdResult,
        aof_records: AofRecords,
        aof_lsn: Option<u64>,
    ) -> Self {
        Self::with_optional_aof_payload_and_records(response, aof_records, aof_lsn, None)
    }

    #[inline]
    pub fn with_optional_aof_payload_and_records(
        response: CmdResult,
        aof_records: AofRecords,
        aof_lsn: Option<u64>,
        aof_payload: Option<Box<[u8]>>,
    ) -> Self {
        Self {
            response,
            aof_records,
            aof_lsn: encode_aof_lsn(aof_lsn),
            aof_payload,
        }
    }

    #[inline]
    pub fn aof_lsn(&self) -> Option<u64> {
        (self.aof_lsn != NO_AOF_LSN).then_some(self.aof_lsn)
    }
}

impl From<CmdResult> for ExecutedCommand {
    #[inline]
    fn from(response: CmdResult) -> Self {
        Self {
            response,
            aof_records: None,
            aof_lsn: NO_AOF_LSN,
            aof_payload: None,
        }
    }
}

#[inline]
fn encode_resp_bulk_command(parts: &[&[u8]]) -> Box<[u8]> {
    let mut buf = Vec::with_capacity(16 + parts.iter().map(|part| part.len() + 16).sum::<usize>());

    push_resp_array_len(&mut buf, parts.len());
    for part in parts {
        push_resp_bulk_string(&mut buf, part);
    }

    buf.into_boxed_slice()
}

#[inline]
fn push_resp_array_len(buf: &mut Vec<u8>, len: usize) {
    buf.push(b'*');
    push_decimal(buf, len);
    buf.extend_from_slice(b"\r\n");
}

#[inline]
fn push_resp_bulk_string(buf: &mut Vec<u8>, value: &[u8]) {
    buf.push(b'$');
    push_decimal(buf, value.len());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(value);
    buf.extend_from_slice(b"\r\n");
}

#[inline]
fn push_decimal<T: itoa::Integer>(buf: &mut Vec<u8>, value: T) {
    let mut digits = itoa::Buffer::new();
    buf.extend_from_slice(digits.format(value).as_bytes());
}

#[inline]
pub(crate) fn encode_aof_set_pxat(
    key_bytes: &[u8],
    value_bytes: &[u8],
    absolute_deadline_ms: u64,
) -> Box<[u8]> {
    let mut deadline = itoa::Buffer::new();
    encode_resp_bulk_command(&[
        b"SET",
        key_bytes,
        value_bytes,
        b"PXAT",
        deadline.format(absolute_deadline_ms).as_bytes(),
    ])
}

#[inline]
pub(crate) fn encode_aof_pexpireat(key_bytes: &[u8], absolute_deadline_ms: u64) -> Box<[u8]> {
    let mut deadline = itoa::Buffer::new();
    encode_resp_bulk_command(&[
        b"PEXPIREAT",
        key_bytes,
        deadline.format(absolute_deadline_ms).as_bytes(),
    ])
}

#[inline]
pub(crate) fn encode_aof_persist(key_bytes: &[u8]) -> Box<[u8]> {
    encode_resp_bulk_command(&[b"PERSIST", key_bytes])
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
pub static ERR_OOM: &[u8] = b"-OOM command not allowed when used memory > 'maxmemory'.\r\n";

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
    clock: impl Into<CommandClock>,
) -> Option<ExecutedCommand> {
    let clock = clock.into();
    let now_nanos = clock.monotonic_nanos;
    let unix_now_nanos = clock.unix_nanos;

    match name {
        b"GET" => Some(string::cmd_get(keyspace, frame, now_nanos).into()),
        b"SET" => Some(string::cmd_set_with_clock(
            keyspace,
            frame,
            now_nanos,
            unix_now_nanos,
        )),
        b"INCR" => Some(string::cmd_incr(keyspace, frame, now_nanos)),
        b"DECR" => Some(string::cmd_decr(keyspace, frame, now_nanos)),
        b"INCRBY" => Some(string::cmd_incrby(keyspace, frame, now_nanos)),
        b"DECRBY" => Some(string::cmd_decrby(keyspace, frame, now_nanos)),
        b"INCRBYFLOAT" => Some(string::cmd_incrbyfloat(keyspace, frame, now_nanos)),
        b"MGET" => Some(string::cmd_mget(keyspace, frame, now_nanos).into()),
        b"MSET" => Some(string::cmd_mset(keyspace, frame, now_nanos)),
        b"MSETNX" => Some(string::cmd_msetnx(keyspace, frame, now_nanos)),
        b"SETNX" => Some(string::cmd_setnx(keyspace, frame, now_nanos)),
        b"SETEX" => Some(string::cmd_setex_with_clock(
            keyspace,
            frame,
            now_nanos,
            unix_now_nanos,
        )),
        b"PSETEX" => Some(string::cmd_psetex_with_clock(
            keyspace,
            frame,
            now_nanos,
            unix_now_nanos,
        )),
        b"GETSET" => Some(string::cmd_getset(keyspace, frame, now_nanos)),
        b"GETDEL" => Some(string::cmd_getdel(keyspace, frame, now_nanos)),
        b"GETEX" => Some(string::cmd_getex_with_clock(
            keyspace,
            frame,
            now_nanos,
            unix_now_nanos,
        )),
        b"APPEND" => Some(string::cmd_append(keyspace, frame, now_nanos)),
        b"STRLEN" => Some(string::cmd_strlen(keyspace, frame, now_nanos).into()),
        b"GETRANGE" => Some(string::cmd_getrange(keyspace, frame, now_nanos).into()),
        b"SETRANGE" => Some(string::cmd_setrange(keyspace, frame, now_nanos)),
        b"DEL" => Some(key::cmd_del(keyspace, frame, now_nanos)),
        b"UNLINK" => Some(key::cmd_unlink(keyspace, frame, now_nanos)),
        b"EXISTS" => Some(key::cmd_exists(keyspace, frame, now_nanos).into()),
        b"EXPIRE" => Some(key::cmd_expire_with_clock(
            keyspace,
            frame,
            now_nanos,
            unix_now_nanos,
        )),
        b"PEXPIRE" => Some(key::cmd_pexpire_with_clock(
            keyspace,
            frame,
            now_nanos,
            unix_now_nanos,
        )),
        b"EXPIREAT" => Some(key::cmd_expireat_with_clock(
            keyspace,
            frame,
            now_nanos,
            unix_now_nanos,
        )),
        b"PEXPIREAT" => Some(key::cmd_pexpireat_with_clock(
            keyspace,
            frame,
            now_nanos,
            unix_now_nanos,
        )),
        b"PERSIST" => Some(key::cmd_persist(keyspace, frame, now_nanos)),
        b"TTL" => Some(key::cmd_ttl(keyspace, frame, now_nanos).into()),
        b"PTTL" => Some(key::cmd_pttl(keyspace, frame, now_nanos).into()),
        b"EXPIRETIME" => {
            Some(key::cmd_expiretime_with_clock(keyspace, frame, now_nanos, unix_now_nanos).into())
        }
        b"PEXPIRETIME" => {
            Some(key::cmd_pexpiretime_with_clock(keyspace, frame, now_nanos, unix_now_nanos).into())
        }
        b"TYPE" => Some(key::cmd_type(keyspace, frame, now_nanos).into()),
        b"RENAME" => Some(key::cmd_rename(keyspace, frame, now_nanos)),
        b"RENAMENX" => Some(key::cmd_renamenx(keyspace, frame, now_nanos)),
        b"KEYS" => Some(key::cmd_keys(keyspace, frame, now_nanos).into()),
        b"SCAN" => Some(key::cmd_scan(keyspace, frame, now_nanos).into()),
        b"RANDOMKEY" => Some(key::cmd_randomkey(keyspace, frame, now_nanos).into()),
        b"TOUCH" => Some(key::cmd_touch(keyspace, frame, now_nanos).into()),
        b"COPY" => Some(key::cmd_copy(keyspace, frame, now_nanos)),
        b"PING" => Some(server::cmd_ping(keyspace, frame, now_nanos).into()),
        b"ECHO" => Some(server::cmd_echo(keyspace, frame, now_nanos).into()),
        b"QUIT" => Some(server::cmd_quit(keyspace, frame, now_nanos).into()),
        b"DBSIZE" => Some(server::cmd_dbsize(keyspace, frame, now_nanos).into()),
        b"FLUSHDB" => Some(server::cmd_flushdb(keyspace, frame, now_nanos)),
        b"FLUSHALL" => Some(server::cmd_flushall(keyspace, frame, now_nanos)),
        b"INFO" => Some(server::cmd_info(keyspace, frame, now_nanos).into()),
        b"COMMAND" => Some(server::cmd_command(keyspace, frame, now_nanos).into()),
        b"SELECT" => Some(server::cmd_select(keyspace, frame, now_nanos).into()),
        b"TIME" => {
            Some(server::cmd_time_with_clock(keyspace, frame, now_nanos, unix_now_nanos).into())
        }
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
        for child in &mut children {
            args.push(child.as_bytes()?);
        }
        Some(Self { args })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.args.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.args.is_empty()
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
    use crate::keyspace::ConcurrentKeyspace;
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

    #[test]
    fn absolute_deadline_round_trips_between_unix_and_monotonic_domains() {
        let unix_now = 1_750_000_000 * NS_PER_SEC;
        let mono_now = 9_000 * NS_PER_SEC;
        let absolute = unix_now + 60 * NS_PER_SEC;

        let deadline = common_absolute_unix_nanos_to_deadline_nanos(absolute, mono_now, unix_now);
        assert_eq!(deadline, mono_now + 60 * NS_PER_SEC);

        let round_trip = common_deadline_nanos_to_absolute_unix_nanos(deadline, mono_now, unix_now);
        assert_eq!(round_trip, absolute);
    }
}
