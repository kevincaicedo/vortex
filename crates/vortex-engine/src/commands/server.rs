//! Server & connection command handlers.
//!
//! PING, ECHO, QUIT, DBSIZE, FLUSHDB, FLUSHALL, INFO, COMMAND,
//! SELECT, TIME, and transaction stubs (MULTI/EXEC/DISCARD/WATCH/UNWATCH).

use vortex_proto::{CommandFlags, CommandMeta, FrameRef, RespFrame};

use super::{
    CmdResult, CommandArgs, ExecutedCommand, NS_PER_SEC, RESP_EMPTY_ARRAY, RESP_OK,
    resolve_unix_time_now_nanos,
};
use crate::ConcurrentKeyspace;

// ── Pre-computed static responses ───────────────────────────────────

static RESP_PONG: &[u8] = b"+PONG\r\n";
static ERR_DB_INDEX: &[u8] = b"-ERR DB index is out of range\r\n";
static ERR_MULTI_NOT_IMPL: &[u8] = b"-ERR MULTI/EXEC is not yet implemented\r\n";

// ── 3.6.1 — PING / ECHO / QUIT ─────────────────────────────────────

/// PING [message]
///
/// If no argument: +PONG\r\n.
/// If argument: return it as bulk string.
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
    // PING message — return message as bulk string.
    if let Some(msg) = args.get(1) {
        CmdResult::Resp(RespFrame::bulk_string(bytes::Bytes::copy_from_slice(msg)))
    } else {
        CmdResult::Static(RESP_PONG)
    }
}

/// ECHO message
///
/// Returns the message as a bulk string.
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
/// Returns +OK. Connection close is handled by the reactor layer.
#[inline]
pub fn cmd_quit(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    CmdResult::Static(RESP_OK)
}

// ── 3.6.2 — DBSIZE / FLUSHDB / FLUSHALL ────────────────────────────

/// DBSIZE
///
/// Returns the number of keys in the shard as an integer.
#[inline]
pub fn cmd_dbsize(
    keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    super::int_resp(keyspace.cmd_dbsize(now_nanos) as i64)
}

/// FLUSHDB [ASYNC|SYNC]
///
/// Removes all keys from the shard. Also clears the expiry wheel.
/// Phase 3: ASYNC is accepted but executes synchronously.
#[inline]
pub fn cmd_flushdb(
    keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> ExecutedCommand {
    ExecutedCommand::with_aof_lsn(CmdResult::Static(RESP_OK), keyspace.cmd_flush_all())
}

/// FLUSHALL [ASYNC|SYNC]
///
/// Same as FLUSHDB in single-shard Phase 3.
#[inline]
pub fn cmd_flushall(
    keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> ExecutedCommand {
    ExecutedCommand::with_aof_lsn(CmdResult::Static(RESP_OK), keyspace.cmd_flush_all())
}

// ── 3.6.3 — INFO ───────────────────────────────────────────────────

/// INFO [section]
///
/// Returns a bulk string with server statistics.
/// Sections: server, clients, memory, keyspace. Default = all.
pub fn cmd_info(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let section = CommandArgs::collect(frame)
        .and_then(|args| args.get(1))
        .unwrap_or(b"all");
    let all = eq_ci(section, b"all") || eq_ci(section, b"everything");
    let (keys, expires) = keyspace.info_keyspace(now_nanos);

    let mut buf = Vec::with_capacity(512);

    if all || eq_ci(section, b"server") {
        write_info_server(&mut buf);
    }
    if all || eq_ci(section, b"clients") {
        write_info_clients(&mut buf);
    }
    if all || eq_ci(section, b"memory") {
        write_info_memory(&mut buf);
    }
    if all || eq_ci(section, b"keyspace") {
        write_info_keyspace(&mut buf, keys, expires);
    }

    CmdResult::Resp(RespFrame::bulk_string(bytes::Bytes::from(buf)))
}

fn write_info_server(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"# Server\r\n");
    buf.extend_from_slice(b"vortex_version:0.1.0\r\n");
    buf.extend_from_slice(b"redis_version:7.4.0\r\n");
    #[cfg(target_arch = "aarch64")]
    buf.extend_from_slice(b"arch_bits:64\r\nserver_arch:aarch64\r\n");
    #[cfg(target_arch = "x86_64")]
    buf.extend_from_slice(b"arch_bits:64\r\nserver_arch:x86_64\r\n");
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    buf.extend_from_slice(b"arch_bits:64\r\n");
    buf.extend_from_slice(b"tcp_port:6379\r\n");
    buf.extend_from_slice(b"process_id:");
    itoa_append(buf, std::process::id() as i64);
    buf.extend_from_slice(b"\r\n\r\n");
}

fn write_info_clients(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"# Clients\r\n");
    // Phase 3: client count not tracked in engine; reactor owns that.
    // Return 0 as placeholder — reactor integration (Task 3.7) will inject real value.
    buf.extend_from_slice(b"connected_clients:0\r\n\r\n");
}

fn write_info_memory(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"# Memory\r\n");
    // Approximate process memory from the OS.
    buf.extend_from_slice(b"used_memory:0\r\n");
    buf.extend_from_slice(b"used_memory_human:0B\r\n\r\n");
}

fn write_info_keyspace(buf: &mut Vec<u8>, keys: usize, expires: usize) {
    buf.extend_from_slice(b"# Keyspace\r\n");
    if keys > 0 {
        buf.extend_from_slice(b"db0:keys=");
        itoa_append(buf, keys as i64);
        buf.extend_from_slice(b",expires=");
        itoa_append(buf, expires as i64);
        buf.extend_from_slice(b",avg_ttl=0\r\n");
    }
    buf.extend_from_slice(b"\r\n");
}

/// Fast itoa into a Vec (avoids pulling in itoa for a Vec target).
#[inline]
fn itoa_append(buf: &mut Vec<u8>, n: i64) {
    let mut tmp = itoa::Buffer::new();
    buf.extend_from_slice(tmp.format(n).as_bytes());
}

// ── 3.6.4 — COMMAND ────────────────────────────────────────────────

/// COMMAND [subcommand [args...]]
///
/// - COMMAND (no args): returns metadata for all commands.
/// - COMMAND COUNT: returns number of registered commands.
/// - COMMAND INFO cmd [cmd ...]: returns metadata for specific commands.
pub fn cmd_command(
    _keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    let Some(args) = CommandArgs::collect(frame) else {
        return cmd_command_all();
    };
    let argc = args.len();

    if argc <= 1 {
        // COMMAND (no subcommand) — return all command metadata.
        return cmd_command_all();
    }

    let sub = args.get(1).unwrap_or(b"");
    if eq_ci(sub, b"count") {
        return cmd_command_count();
    }
    if eq_ci(sub, b"info") {
        return cmd_command_info(&args);
    }
    if eq_ci(sub, b"docs") {
        // COMMAND DOCS — not implemented, return empty array.
        return CmdResult::Static(RESP_EMPTY_ARRAY);
    }
    if eq_ci(sub, b"list") {
        // COMMAND LIST — not implemented, return empty array.
        return CmdResult::Static(RESP_EMPTY_ARRAY);
    }
    if eq_ci(sub, b"getkeys") {
        return CmdResult::Static(RESP_EMPTY_ARRAY);
    }
    // Unknown subcommand.
    CmdResult::Static(b"-ERR unknown subcommand or wrong number of arguments\r\n")
}

/// COMMAND (no args) — returns metadata array for all commands.
fn cmd_command_all() -> CmdResult {
    let entries = collect_all_command_metas();
    let mut frames = Vec::with_capacity(entries.len());
    for meta in &entries {
        frames.push(command_meta_to_frame(meta));
    }
    CmdResult::Resp(RespFrame::Array(Some(frames)))
}

/// COMMAND COUNT — returns the number of registered commands.
fn cmd_command_count() -> CmdResult {
    let count = collect_all_command_metas().len();
    super::int_resp(count as i64)
}

/// COMMAND INFO cmd [cmd ...]
fn cmd_command_info(args: &CommandArgs<'_>) -> CmdResult {
    let argc = args.len();
    let mut frames = Vec::with_capacity(argc.saturating_sub(2));
    for i in 2..argc {
        if let Some(name_bytes) = args.get(i) {
            // Uppercase for lookup.
            let mut buf = [0u8; 32];
            let len = name_bytes.len().min(32);
            buf[..len].copy_from_slice(&name_bytes[..len]);
            vortex_proto::uppercase_inplace(&mut buf[..len]);
            // SAFETY: uppercase ASCII is valid UTF-8.
            let name_str = unsafe { std::str::from_utf8_unchecked(&buf[..len]) };
            match vortex_proto::command::lookup_command(name_str) {
                Some(meta) => frames.push(command_meta_to_frame(meta)),
                None => frames.push(RespFrame::Null),
            }
        } else {
            frames.push(RespFrame::Null);
        }
    }
    CmdResult::Resp(RespFrame::Array(Some(frames)))
}

/// Serialize a single CommandMeta to a RESP array:
/// [name, arity, [flags...], first_key, last_key, step]
fn command_meta_to_frame(meta: &CommandMeta) -> RespFrame {
    let name = RespFrame::bulk_string(bytes::Bytes::from_static(meta.name.as_bytes()));
    let arity = RespFrame::integer(i64::from(meta.arity));

    // Build flags array.
    let mut flags = Vec::with_capacity(4);
    if meta.flags.contains(CommandFlags::READ) {
        flags.push(RespFrame::simple_string("readonly"));
    }
    if meta.flags.contains(CommandFlags::WRITE) {
        flags.push(RespFrame::simple_string("write"));
    }
    if meta.flags.contains(CommandFlags::FAST) {
        flags.push(RespFrame::simple_string("fast"));
    }
    if meta.flags.contains(CommandFlags::SLOW) {
        flags.push(RespFrame::simple_string("slow"));
    }
    if meta.flags.contains(CommandFlags::ADMIN) {
        flags.push(RespFrame::simple_string("admin"));
    }
    if meta.flags.contains(CommandFlags::BLOCKING) {
        flags.push(RespFrame::simple_string("blocking"));
    }
    if meta.flags.contains(CommandFlags::PUBSUB) {
        flags.push(RespFrame::simple_string("pubsub"));
    }
    if meta.flags.contains(CommandFlags::SCRIPTING) {
        flags.push(RespFrame::simple_string("scripting"));
    }
    let flags_arr = RespFrame::Array(Some(flags));

    let first_key = RespFrame::integer(i64::from(meta.key_range.first));
    let last_key = RespFrame::integer(i64::from(meta.key_range.last));
    let step = RespFrame::integer(i64::from(meta.key_range.step));

    RespFrame::Array(Some(vec![
        name, arity, flags_arr, first_key, last_key, step,
    ]))
}

/// Collect all command metadata from the PHF table.
///
/// The PHF table is compile-time generated; we must iterate it via
/// the known command set. We use `lookup_command` against the known list.
fn collect_all_command_metas() -> Vec<&'static CommandMeta> {
    // The known command names. We include all registered names from build.rs.
    // This is the canonical list for COMMAND output.
    static KNOWN_COMMANDS: &[&str] = &[
        // Connection
        "PING",
        "ECHO",
        "QUIT",
        "AUTH",
        "SELECT",
        "HELLO",
        "RESET",
        "CLIENT",
        // Server
        "COMMAND",
        "INFO",
        "DBSIZE",
        "FLUSHDB",
        "FLUSHALL",
        "CONFIG",
        "DEBUG",
        "SAVE",
        "BGSAVE",
        "BGREWRITEAOF",
        "LASTSAVE",
        "TIME",
        "SLOWLOG",
        "MULTI",
        "EXEC",
        "DISCARD",
        "WATCH",
        "UNWATCH",
        "WAIT",
        "SHUTDOWN",
        "SWAPDB",
        "OBJECT",
        "TYPE",
        "DUMP",
        "RESTORE",
        // String
        "SET",
        "GET",
        "SETNX",
        "SETEX",
        "PSETEX",
        "MSET",
        "MSETNX",
        "MGET",
        "GETSET",
        "GETDEL",
        "GETEX",
        "INCR",
        "DECR",
        "INCRBY",
        "DECRBY",
        "INCRBYFLOAT",
        "APPEND",
        "STRLEN",
        "GETRANGE",
        "SETRANGE",
        "SUBSTR",
        "LCS",
        // Key
        "DEL",
        "UNLINK",
        "EXISTS",
        "EXPIRE",
        "PEXPIRE",
        "EXPIREAT",
        "PEXPIREAT",
        "PERSIST",
        "TTL",
        "PTTL",
        "EXPIRETIME",
        "PEXPIRETIME",
        "RENAME",
        "RENAMENX",
        "KEYS",
        "SCAN",
        "RANDOMKEY",
        "TOUCH",
        "COPY",
        "SORT",
        "SORT_RO",
        "MOVE",
        "WAIT",
        // Hash
        "HSET",
        "HGET",
        "HMSET",
        "HMGET",
        "HDEL",
        "HEXISTS",
        "HLEN",
        "HKEYS",
        "HVALS",
        "HGETALL",
        "HINCRBY",
        "HINCRBYFLOAT",
        "HSETNX",
        "HRANDFIELD",
        "HSCAN",
        // Set
        "SADD",
        "SREM",
        "SISMEMBER",
        "SMISMEMBER",
        "SMEMBERS",
        "SCARD",
        "SPOP",
        "SRANDMEMBER",
        "SINTER",
        "SINTERSTORE",
        "SINTERCARD",
        "SUNION",
        "SUNIONSTORE",
        "SDIFF",
        "SDIFFSTORE",
        "SSCAN",
        // Sorted Set
        "ZADD",
        "ZREM",
        "ZSCORE",
        "ZRANK",
        "ZREVRANK",
        "ZCARD",
        "ZCOUNT",
        "ZLEXCOUNT",
        "ZRANGE",
        "ZRANGEBYLEX",
        "ZRANGEBYSCORE",
        "ZREVRANGE",
        "ZREVRANGEBYLEX",
        "ZREVRANGEBYSCORE",
        "ZRANGESTORE",
        "ZINCRBY",
        "ZPOPMIN",
        "ZPOPMAX",
        "ZRANDMEMBER",
        "ZMSCORE",
        "ZUNIONSTORE",
        "ZINTERSTORE",
        "ZUNION",
        "ZINTER",
        "ZDIFF",
        "ZDIFFSTORE",
        "ZSCAN",
        "BZPOPMIN",
        "BZPOPMAX",
        "ZINTERCARD",
        // List
        "LPUSH",
        "RPUSH",
        "LPOP",
        "RPOP",
        "LLEN",
        "LINDEX",
        "LSET",
        "LRANGE",
        "LTRIM",
        "LREM",
        "LINSERT",
        "LPOS",
        "LMPOP",
        "RPOPLPUSH",
        "LMOVE",
        "LPUSHX",
        "RPUSHX",
        "BLPOP",
        "BRPOP",
        "BRPOPLPUSH",
        "BLMOVE",
        "BLMPOP",
        // Stream
        "XADD",
        "XLEN",
        "XRANGE",
        "XREVRANGE",
        "XREAD",
        "XINFO",
        "XACK",
        "XCLAIM",
        "XAUTOCLAIM",
        "XDEL",
        "XTRIM",
        "XGROUP",
        "XREADGROUP",
        "XPENDING",
        "XSETID",
        // Pub/Sub
        "SUBSCRIBE",
        "UNSUBSCRIBE",
        "PUBLISH",
        "PSUBSCRIBE",
        "PUNSUBSCRIBE",
        "PUBSUB",
        "SSUBSCRIBE",
        "SUNSUBSCRIBE",
        // Scripting
        "EVAL",
        "EVALSHA",
        "EVALRO",
        "EVALSHA_RO",
        "SCRIPT",
        "FUNCTION",
        // HyperLogLog
        "PFADD",
        "PFCOUNT",
        "PFMERGE",
        // Bitmap
        "SETBIT",
        "GETBIT",
        "BITCOUNT",
        "BITOP",
        "BITPOS",
        "BITFIELD",
        "BITFIELD_RO",
        // Geo
        "GEOADD",
        "GEODIST",
        "GEOHASH",
        "GEOPOS",
        "GEORADIUS",
        "GEORADIUSBYMEMBER",
        "GEOSEARCH",
        "GEOSEARCHSTORE",
        "GEORADIUS_RO",
        "GEORADIUSBYMEMBER_RO",
        // Cluster
        "CLUSTER",
        "READONLY",
        "READWRITE",
        "ASKING",
        // ACL
        "ACL",
        // Module
        "MODULE",
        // Memory
        "MEMORY",
    ];

    let mut metas = Vec::with_capacity(KNOWN_COMMANDS.len());
    for &name in KNOWN_COMMANDS {
        if let Some(meta) = vortex_proto::command::lookup_command(name) {
            metas.push(meta);
        }
    }
    metas
}

// ── 3.6.5 — SELECT / TIME / transaction stubs ──────────────────────

/// SELECT index
///
/// Accept 0 (single DB in Phase 3), reject N>0.
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

/// TIME
///
/// Returns [unix_seconds_string, microseconds_string] as a two-element array.
#[inline]
#[allow(dead_code)]
pub fn cmd_time(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    cmd_time_with_clock(_keyspace, _frame, now_nanos, 0)
}

#[inline]
pub(crate) fn cmd_time_with_clock(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
    unix_now_nanos: u64,
) -> CmdResult {
    let unix_now_nanos = resolve_unix_time_now_nanos(unix_now_nanos);
    let secs = unix_now_nanos / NS_PER_SEC;
    let usecs = (unix_now_nanos % NS_PER_SEC) / 1_000;

    let mut sec_buf = itoa::Buffer::new();
    let sec_str = sec_buf.format(secs);
    let mut usec_buf = itoa::Buffer::new();
    let usec_str = usec_buf.format(usecs);

    CmdResult::Resp(RespFrame::Array(Some(vec![
        RespFrame::bulk_string(bytes::Bytes::copy_from_slice(sec_str.as_bytes())),
        RespFrame::bulk_string(bytes::Bytes::copy_from_slice(usec_str.as_bytes())),
    ])))
}

/// MULTI — stub.
#[inline]
pub fn cmd_multi(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    CmdResult::Static(ERR_MULTI_NOT_IMPL)
}

/// EXEC — stub.
#[inline]
pub fn cmd_exec(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    CmdResult::Static(b"-ERR EXEC without MULTI\r\n")
}

/// DISCARD — stub.
#[inline]
pub fn cmd_discard(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    CmdResult::Static(b"-ERR DISCARD without MULTI\r\n")
}

/// WATCH key [key ...] — stub.
#[inline]
pub fn cmd_watch(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    CmdResult::Static(ERR_MULTI_NOT_IMPL)
}

/// UNWATCH — stub.
#[inline]
pub fn cmd_unwatch(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    CmdResult::Static(RESP_OK)
}

// ── Helpers ─────────────────────────────────────────────────────────

/// ASCII case-insensitive comparison.
#[inline]
fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

// ── Tests ───────────────────────────────────────────────────────────

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

    fn assert_integer(r: &CmdResult, expected: i64) {
        match r {
            CmdResult::Resp(RespFrame::Integer(n)) => {
                assert_eq!(*n, expected, "integer mismatch");
            }
            CmdResult::Static(s) => {
                let expected_bytes: &[u8] = match expected {
                    0 => b":0\r\n",
                    1 => b":1\r\n",
                    -1 => b":-1\r\n",
                    -2 => b":-2\r\n",
                    _ => panic!(
                        "expected Integer({expected}), got Static({:?})",
                        std::str::from_utf8(s)
                    ),
                };
                assert_eq!(*s, expected_bytes, "static integer mismatch for {expected}");
            }
            other => panic!("expected Integer({expected}), got {other:?}"),
        }
    }

    fn assert_bulk_contains(r: &CmdResult, needle: &[u8]) {
        match r {
            CmdResult::Resp(RespFrame::BulkString(Some(b))) => {
                let bytes: &[u8] = b.as_ref();
                assert!(
                    bytes.windows(needle.len()).any(|w| w == needle),
                    "bulk string does not contain {:?}",
                    std::str::from_utf8(needle).unwrap_or("<binary>")
                );
            }
            other => panic!("expected BulkString, got {other:?}"),
        }
    }

    fn assert_array_len(r: &CmdResult, expected: usize) {
        match r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert_eq!(
                    arr.len(),
                    expected,
                    "array length mismatch: got {}, expected {}",
                    arr.len(),
                    expected
                );
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    // ── PING ──

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

    // ── ECHO ──

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

    // ── QUIT ──

    #[test]
    fn quit_returns_ok() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"QUIT"]);
        assert_static(&r, RESP_OK);
    }

    // ── DBSIZE ──

    #[test]
    fn dbsize_empty() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"DBSIZE"]);
        assert_integer(&r, 0);
    }

    #[test]
    fn dbsize_with_keys() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("a"), VortexValue::from_bytes(b"1"));
        h.set(VortexKey::from("b"), VortexValue::from_bytes(b"2"));
        h.set(VortexKey::from("c"), VortexValue::from_bytes(b"3"));
        let r = exec(&h, &[b"DBSIZE"]);
        assert_integer(&r, 3);
    }

    // ── FLUSHDB ──

    #[test]
    fn flushdb_empties_shard() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("a"), VortexValue::from_bytes(b"1"));
        h.set(VortexKey::from("b"), VortexValue::from_bytes(b"2"));
        assert_eq!(h.len(), 2);
        let r = exec(&h, &[b"FLUSHDB"]);
        assert_static(&r, RESP_OK);
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn flushall_empties_shard() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("x"), VortexValue::from_bytes(b"val"));
        let r = exec(&h, &[b"FLUSHALL"]);
        assert_static(&r, RESP_OK);
        assert_eq!(h.len(), 0);
    }

    // ── INFO ──

    #[test]
    fn info_all_contains_sections() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"INFO"]);
        assert_bulk_contains(&r, b"# Server");
        assert_bulk_contains(&r, b"# Clients");
        assert_bulk_contains(&r, b"# Memory");
        assert_bulk_contains(&r, b"# Keyspace");
    }

    #[test]
    fn info_server_section() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"INFO", b"server"]);
        assert_bulk_contains(&r, b"# Server");
        assert_bulk_contains(&r, b"vortex_version:");
        assert_bulk_contains(&r, b"redis_version:");
    }

    #[test]
    fn info_keyspace_with_keys() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("k1"), VortexValue::from_bytes(b"v1"));
        h.set(VortexKey::from("k2"), VortexValue::from_bytes(b"v2"));
        let r = exec(&h, &[b"INFO", b"keyspace"]);
        assert_bulk_contains(&r, b"db0:keys=2");
    }

    // ── COMMAND ──

    #[test]
    fn command_count_returns_positive() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"COMMAND", b"COUNT"]);
        match &r {
            CmdResult::Resp(RespFrame::Integer(n)) => {
                assert!(*n > 100, "expected > 100 commands, got {n}");
            }
            other => panic!("expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn command_info_known() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"COMMAND", b"INFO", b"GET"]);
        // Should return a 1-element array with GET metadata.
        assert_array_len(&r, 1);
    }

    #[test]
    fn command_info_unknown() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"COMMAND", b"INFO", b"NONEXISTENT"]);
        // Should return array with Null entry.
        assert_array_len(&r, 1);
    }

    // ── SELECT ──

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

    // ── TIME ──

    #[test]
    fn time_returns_two_element_array() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"TIME"]);
        assert_array_len(&r, 2);
    }

    #[test]
    fn time_values_are_numeric() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"TIME"]);
        match &r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                // Both should be bulk strings containing numeric values.
                for frame in arr {
                    match frame {
                        RespFrame::BulkString(Some(b)) => {
                            let s = std::str::from_utf8(b).unwrap();
                            assert!(s.parse::<u64>().is_ok(), "not numeric: {s}");
                        }
                        other => panic!("expected BulkString in TIME array, got {other:?}"),
                    }
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    // ── Transaction stubs ──

    #[test]
    fn multi_returns_error() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"MULTI"]);
        assert_static(&r, ERR_MULTI_NOT_IMPL);
    }

    #[test]
    fn exec_without_multi() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"EXEC"]);
        assert_static(&r, b"-ERR EXEC without MULTI\r\n");
    }

    #[test]
    fn discard_without_multi() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"DISCARD"]);
        assert_static(&r, b"-ERR DISCARD without MULTI\r\n");
    }

    #[test]
    fn watch_returns_error() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"WATCH", b"mykey"]);
        assert_static(&r, ERR_MULTI_NOT_IMPL);
    }

    #[test]
    fn unwatch_returns_ok() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"UNWATCH"]);
        assert_static(&r, RESP_OK);
    }
}
