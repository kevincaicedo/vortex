//! Key management command handlers.
//!
//! Implements DEL, UNLINK, EXISTS, EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT,
//! PERSIST, TTL, PTTL, EXPIRETIME, PEXPIRETIME, TYPE, RENAME, RENAMENX,
//! KEYS, SCAN, RANDOMKEY, TOUCH, COPY.
//!
//! All handlers are free functions: `cmd_xxx(shard, frame, now_nanos) -> CmdResult`.
//! Zero dynamic dispatch, designed for inlining by the compiler.

use vortex_common::VortexKey;
use vortex_proto::{FrameRef, RespFrame};

use super::context::TtlState;
use super::{
    CmdResult, ExecutedCommand, NS_PER_MS, NS_PER_SEC, RESP_NEG_ONE, RESP_NEG_TWO, RESP_NIL,
    RESP_OK, RESP_ONE, RESP_ZERO, arg_bytes, arg_count, arg_i64, int_resp, key_from_bytes,
};
use crate::commands::context::MutationOutcome;
use crate::ConcurrentKeyspace;

// ── Error constants ─────────────────────────────────────────────────

#[cfg(test)]
static ERR_NO_SUCH_KEY: &[u8] = b"-ERR no such key\r\n";
static ERR_WRONG_ARGS: &[u8] = b"-ERR wrong number of arguments\r\n";

// ── DEL / UNLINK / EXISTS ───────────────────────────────────────────

/// DEL key [key ...]
/// Removes the specified keys. Returns the number of keys removed.
#[inline]
pub fn cmd_del(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    let argc = arg_count(frame);
    if argc < 2 {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    }
    // Single-key fast path: avoid Vec allocation.
    if argc == 2 {
        if let Some(kb) = arg_bytes(frame, 1) {
            let key = key_from_bytes(kb);
            let outcome = keyspace.delete_keys(std::slice::from_ref(&key), now_nanos);
            return ExecutedCommand::with_aof_lsn(int_resp(outcome.value), outcome.aof_lsn);
        }
        return ExecutedCommand::from(CmdResult::Static(RESP_ZERO));
    }
    let mut keys = Vec::with_capacity(argc - 1);
    for i in 1..argc {
        if let Some(kb) = arg_bytes(frame, i) {
            keys.push(key_from_bytes(kb));
        }
    }
    let outcome = keyspace.delete_keys(&keys, now_nanos);
    ExecutedCommand::with_aof_lsn(int_resp(outcome.value), outcome.aof_lsn)
}

/// UNLINK key [key ...]
/// Same as DEL for now (heap deallocation is deferred in Phase 3 via
/// inline entry tombstoning — no heap to free for inline entries).
#[inline]
pub fn cmd_unlink(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    cmd_del(keyspace, frame, now_nanos)
}

/// EXISTS key [key ...]
/// Returns the count of specified keys that exist.
#[inline]
pub fn cmd_exists(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 2 {
        return CmdResult::Static(ERR_WRONG_ARGS);
    }
    // Single-key fast path: avoid Vec allocation.
    if argc == 2 {
        if let Some(kb) = arg_bytes(frame, 1) {
            let key = key_from_bytes(kb);
            return int_resp(keyspace.count_existing(std::slice::from_ref(&key), now_nanos));
        }
        return CmdResult::Static(RESP_ZERO);
    }
    let mut keys = Vec::with_capacity(argc - 1);
    for i in 1..argc {
        if let Some(kb) = arg_bytes(frame, i) {
            keys.push(key_from_bytes(kb));
        }
    }
    int_resp(keyspace.count_existing(&keys, now_nanos))
}

// ── EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT / PERSIST ───────────────

/// EXPIRE key seconds [NX|XX|GT|LT]
#[inline]
pub fn cmd_expire(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    expire_generic(keyspace, frame, now_nanos, ExpireMode::RelativeSeconds)
}

/// PEXPIRE key milliseconds [NX|XX|GT|LT]
#[inline]
pub fn cmd_pexpire(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    expire_generic(keyspace, frame, now_nanos, ExpireMode::RelativeMillis)
}

/// EXPIREAT key timestamp [NX|XX|GT|LT]
#[inline]
pub fn cmd_expireat(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    expire_generic(keyspace, frame, now_nanos, ExpireMode::AbsoluteSeconds)
}

/// PEXPIREAT key ms-timestamp [NX|XX|GT|LT]
#[inline]
pub fn cmd_pexpireat(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    expire_generic(keyspace, frame, now_nanos, ExpireMode::AbsoluteMillis)
}

/// PERSIST key
#[inline]
pub fn cmd_persist(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> ExecutedCommand {
    let Some(kb) = arg_bytes(frame, 1) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    };
    let key = key_from_bytes(kb);
    let outcome = keyspace.persist_key(&key);
    let response = if outcome.value {
        CmdResult::Static(RESP_ONE)
    } else {
        CmdResult::Static(RESP_ZERO)
    };
    ExecutedCommand::with_aof_lsn(response, outcome.aof_lsn)
}

#[derive(Clone, Copy)]
enum ExpireMode {
    RelativeSeconds,
    RelativeMillis,
    AbsoluteSeconds,
    AbsoluteMillis,
}

/// Generic implementation for all EXPIRE variants with NX/XX/GT/LT flags.
fn expire_generic(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
    mode: ExpireMode,
) -> ExecutedCommand {
    let argc = arg_count(frame);
    if argc < 3 {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    }
    let Some(kb) = arg_bytes(frame, 1) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    };
    let Some(time_val) = arg_i64(frame, 2) else {
        return ExecutedCommand::from(CmdResult::Static(super::ERR_NOT_INTEGER));
    };

    // Parse optional flags (NX, XX, GT, LT).
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    for i in 3..argc {
        if let Some(flag) = arg_bytes(frame, i) {
            match flag.len() {
                2 => {
                    let upper = [flag[0] | 0x20, flag[1] | 0x20];
                    match &upper {
                        b"nx" => nx = true,
                        b"xx" => xx = true,
                        b"gt" => gt = true,
                        b"lt" => lt = true,
                        _ => return ExecutedCommand::from(CmdResult::Static(super::ERR_SYNTAX)),
                    }
                }
                _ => return ExecutedCommand::from(CmdResult::Static(super::ERR_SYNTAX)),
            }
        }
    }

    // Compute absolute deadline in nanos.
    let deadline_nanos = match mode {
        ExpireMode::RelativeSeconds => {
            if time_val <= 0 {
                let key = key_from_bytes(kb);
                let outcome = keyspace.remove_value(&key, now_nanos);
                let response = if outcome.value.is_some() {
                    CmdResult::Static(RESP_ONE)
                } else {
                    CmdResult::Static(RESP_ZERO)
                };
                return ExecutedCommand::with_aof_lsn(response, outcome.aof_lsn);
            }
            now_nanos + (time_val as u64) * NS_PER_SEC
        }
        ExpireMode::RelativeMillis => {
            if time_val <= 0 {
                let key = key_from_bytes(kb);
                let outcome = keyspace.remove_value(&key, now_nanos);
                let response = if outcome.value.is_some() {
                    CmdResult::Static(RESP_ONE)
                } else {
                    CmdResult::Static(RESP_ZERO)
                };
                return ExecutedCommand::with_aof_lsn(response, outcome.aof_lsn);
            }
            now_nanos + (time_val as u64) * NS_PER_MS
        }
        ExpireMode::AbsoluteSeconds => {
            let deadline = (time_val as u64) * NS_PER_SEC;
            if deadline <= now_nanos {
                let key = key_from_bytes(kb);
                let outcome = keyspace.remove_value(&key, now_nanos);
                let response = if outcome.value.is_some() {
                    CmdResult::Static(RESP_ONE)
                } else {
                    CmdResult::Static(RESP_ZERO)
                };
                return ExecutedCommand::with_aof_lsn(response, outcome.aof_lsn);
            }
            deadline
        }
        ExpireMode::AbsoluteMillis => {
            let deadline = (time_val as u64) * NS_PER_MS;
            if deadline <= now_nanos {
                let key = key_from_bytes(kb);
                let outcome = keyspace.remove_value(&key, now_nanos);
                let response = if outcome.value.is_some() {
                    CmdResult::Static(RESP_ONE)
                } else {
                    CmdResult::Static(RESP_ZERO)
                };
                return ExecutedCommand::with_aof_lsn(response, outcome.aof_lsn);
            }
            deadline
        }
    };

    let key = key_from_bytes(kb);

    let current_ttl = match keyspace.ttl_state(&key, now_nanos) {
        TtlState::Missing => return ExecutedCommand::from(CmdResult::Static(RESP_ZERO)),
        TtlState::Persistent => 0,
        TtlState::Deadline(deadline) => deadline,
    };

    if nx && current_ttl != 0 {
        return ExecutedCommand::from(CmdResult::Static(RESP_ZERO));
    }
    if xx && current_ttl == 0 {
        return ExecutedCommand::from(CmdResult::Static(RESP_ZERO));
    }
    if gt && current_ttl != 0 && deadline_nanos <= current_ttl {
        return ExecutedCommand::from(CmdResult::Static(RESP_ZERO));
    }
    if lt && current_ttl != 0 && deadline_nanos >= current_ttl {
        return ExecutedCommand::from(CmdResult::Static(RESP_ZERO));
    }

    let outcome = keyspace.expire_key(&key, deadline_nanos, now_nanos);
    let response = if outcome.value {
        CmdResult::Static(RESP_ONE)
    } else {
        CmdResult::Static(RESP_ZERO)
    };
    ExecutedCommand::with_aof_lsn(response, outcome.aof_lsn)
}

// ── TTL / PTTL / EXPIRETIME / PEXPIRETIME ───────────────────────────

/// TTL key
#[inline]
pub fn cmd_ttl(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    match keyspace.ttl_state(&key, now_nanos) {
        TtlState::Missing => CmdResult::Static(RESP_NEG_TWO),
        TtlState::Persistent => CmdResult::Static(RESP_NEG_ONE),
        TtlState::Deadline(deadline) => int_resp(((deadline - now_nanos) / NS_PER_SEC) as i64),
    }
}

/// PTTL key
#[inline]
pub fn cmd_pttl(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    match keyspace.ttl_state(&key, now_nanos) {
        TtlState::Missing => CmdResult::Static(RESP_NEG_TWO),
        TtlState::Persistent => CmdResult::Static(RESP_NEG_ONE),
        TtlState::Deadline(deadline) => int_resp(((deadline - now_nanos) / NS_PER_MS) as i64),
    }
}

/// EXPIRETIME key
#[inline]
pub fn cmd_expiretime(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    match keyspace.ttl_state(&key, now_nanos) {
        TtlState::Missing => CmdResult::Static(RESP_NEG_TWO),
        TtlState::Persistent => CmdResult::Static(RESP_NEG_ONE),
        TtlState::Deadline(deadline) => int_resp((deadline / NS_PER_SEC) as i64),
    }
}

/// PEXPIRETIME key
#[inline]
pub fn cmd_pexpiretime(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    match keyspace.ttl_state(&key, now_nanos) {
        TtlState::Missing => CmdResult::Static(RESP_NEG_TWO),
        TtlState::Persistent => CmdResult::Static(RESP_NEG_ONE),
        TtlState::Deadline(deadline) => int_resp((deadline / NS_PER_MS) as i64),
    }
}

// ── TYPE ────────────────────────────────────────────────────────────

/// TYPE key
#[inline]
pub fn cmd_type(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    match keyspace.type_of_key(&key, now_nanos) {
        Some(type_name) => CmdResult::Resp(RespFrame::simple_string(type_name)),
        None => CmdResult::Resp(RespFrame::simple_string("none")),
    }
}

// ── RENAME / RENAMENX ───────────────────────────────────────────────

/// RENAME key newkey
#[inline]
pub fn cmd_rename(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    let argc = arg_count(frame);
    if argc < 3 {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    }
    let Some(old_kb) = arg_bytes(frame, 1) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    };
    let Some(new_kb) = arg_bytes(frame, 2) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    };
    let old_key = key_from_bytes(old_kb);
    let new_key = key_from_bytes(new_kb);
    match keyspace.rename_key(&old_key, new_key, now_nanos, false) {
        Ok(MutationOutcome { value, aof_lsn }) => {
            let response = if value {
                CmdResult::Static(RESP_OK)
            } else {
                CmdResult::Static(RESP_ZERO)
            };
            ExecutedCommand::with_aof_lsn(response, aof_lsn)
        }
        Err(msg) => ExecutedCommand::from(CmdResult::Static(msg)),
    }
}

/// RENAMENX key newkey
#[inline]
pub fn cmd_renamenx(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    let argc = arg_count(frame);
    if argc < 3 {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    }
    let Some(old_kb) = arg_bytes(frame, 1) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    };
    let Some(new_kb) = arg_bytes(frame, 2) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    };
    let old_key = key_from_bytes(old_kb);
    let new_key = key_from_bytes(new_kb);

    match keyspace.rename_key(&old_key, new_key, now_nanos, true) {
        Ok(MutationOutcome { value, aof_lsn }) => {
            let response = if value {
                CmdResult::Static(RESP_ONE)
            } else {
                CmdResult::Static(RESP_ZERO)
            };
            ExecutedCommand::with_aof_lsn(response, aof_lsn)
        }
        Err(msg) => ExecutedCommand::from(CmdResult::Static(msg)),
    }
}

// ── SCAN ────────────────────────────────────────────────────────────

/// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
pub fn cmd_scan(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 2 {
        return CmdResult::Static(ERR_WRONG_ARGS);
    }
    let Some(cursor_val) = arg_i64(frame, 1) else {
        return CmdResult::Static(super::ERR_NOT_INTEGER);
    };
    let cursor = cursor_val as u64;

    // Parse optional arguments.
    let mut pattern: Option<&[u8]> = None;
    let mut count: usize = 10;
    let mut type_filter: Option<&[u8]> = None;

    let mut i = 2;
    while i < argc {
        if let Some(opt) = arg_bytes(frame, i) {
            if eq_ci(opt, b"MATCH") {
                i += 1;
                pattern = arg_bytes(frame, i);
            } else if eq_ci(opt, b"COUNT") {
                i += 1;
                if let Some(n) = arg_i64(frame, i) {
                    if n > 0 {
                        count = n as usize;
                    }
                }
            } else if eq_ci(opt, b"TYPE") {
                i += 1;
                type_filter = arg_bytes(frame, i);
            }
        }
        i += 1;
    }

    let (next_cursor, results) = keyspace.scan_keys(cursor, pattern, count, type_filter, now_nanos);
    scan_response(next_cursor, &results)
}

/// Build a SCAN response: `*2\r\n $cursor_len\r\n cursor\r\n *N\r\n ...keys...`
fn scan_response(cursor: u64, keys: &[VortexKey]) -> CmdResult {
    let cursor_str = itoa::Buffer::new().format(cursor).to_owned();
    let mut elements = Vec::with_capacity(keys.len());
    for k in keys {
        elements.push(RespFrame::bulk_string(bytes::Bytes::copy_from_slice(
            k.as_bytes(),
        )));
    }
    CmdResult::Resp(RespFrame::Array(Some(vec![
        RespFrame::bulk_string(bytes::Bytes::copy_from_slice(cursor_str.as_bytes())),
        RespFrame::Array(Some(elements)),
    ])))
}

// ── KEYS ────────────────────────────────────────────────────────────

/// KEYS pattern
pub fn cmd_keys(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(pat) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let mut results = Vec::new();
    for key in keyspace.keys_matching(pat, now_nanos) {
        results.push(RespFrame::bulk_string(bytes::Bytes::copy_from_slice(
            key.as_bytes(),
        )));
    }

    CmdResult::Resp(RespFrame::Array(Some(results)))
}

// ── RANDOMKEY ───────────────────────────────────────────────────────

/// RANDOMKEY
#[inline]
pub fn cmd_randomkey(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let _ = frame;
    match keyspace.random_key(now_nanos, now_nanos) {
        Some(key) => CmdResult::Resp(RespFrame::bulk_string(bytes::Bytes::copy_from_slice(
            key.as_bytes(),
        ))),
        None => CmdResult::Static(RESP_NIL),
    }
}

// ── TOUCH ───────────────────────────────────────────────────────────

/// TOUCH key [key ...]
/// Like EXISTS but conceptually "touches" the key (updates access time).
/// In Phase 3, behaves identically to EXISTS.
#[inline]
pub fn cmd_touch(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    cmd_exists(keyspace, frame, now_nanos)
}

// ── COPY ────────────────────────────────────────────────────────────

/// COPY source destination [DB destination-db] [REPLACE]
pub fn cmd_copy(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    let argc = arg_count(frame);
    if argc < 3 {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    }
    let Some(src_kb) = arg_bytes(frame, 1) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    };
    let Some(dst_kb) = arg_bytes(frame, 2) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS));
    };

    // Parse optional flags.
    let mut replace = false;
    let mut i = 3;
    while i < argc {
        if let Some(opt) = arg_bytes(frame, i) {
            if eq_ci(opt, b"REPLACE") {
                replace = true;
            } else if eq_ci(opt, b"DB") {
                // DB selection not supported in Phase 3 (single-shard).
                i += 1; // Skip the DB number.
            }
        }
        i += 1;
    }

    let src_key = key_from_bytes(src_kb);
    let dst_key = key_from_bytes(dst_kb);

    let outcome = keyspace.copy_key(&src_key, dst_key, replace, now_nanos);
    let response = if outcome.value {
        CmdResult::Static(RESP_ONE)
    } else {
        CmdResult::Static(RESP_ZERO)
    };
    ExecutedCommand::with_aof_lsn(response, outcome.aof_lsn)
}

// ── Glob pattern matcher ────────────────────────────────────────────

/// Match a Redis-style glob pattern against a string.
#[inline]
#[cfg(test)]
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    super::pattern::glob_match(pattern, string)
}

// ── Helpers ─────────────────────────────────────────────────────────

/// ASCII case-insensitive comparison.
#[inline]
fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for i in 0..a.len() {
        if (a[i] | 0x20) != (b[i] | 0x20) {
            return false;
        }
    }
    true
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::commands::test_harness::TestHarness;
    use vortex_common::VortexValue;
    use vortex_proto::RespTape;

    const NOW: u64 = 1_000_000_000_000; // 1000 seconds in nanos.

    /// Build raw RESP array bytes from parts.
    fn make_resp(parts: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            buf.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            buf.extend_from_slice(p);
            buf.extend_from_slice(b"\r\n");
        }
        buf
    }

    /// Parse RESP bytes and execute command via handler function.
    fn exec<R>(
        keyspace: &ConcurrentKeyspace,
        handler: fn(&ConcurrentKeyspace, &FrameRef<'_>, u64) -> R,
        parts: &[&[u8]],
        now: u64,
    ) -> CmdResult
    where
        R: Into<ExecutedCommand>,
    {
        let data = make_resp(parts);
        let tape = RespTape::parse_pipeline(&data).expect("valid RESP input");
        let frame = tape.iter().next().unwrap();
        handler(keyspace, &frame, now).into().response
    }

    fn assert_static(result: CmdResult, expected: &[u8]) {
        match result {
            CmdResult::Static(s) => assert_eq!(s, expected, "Static mismatch"),
            CmdResult::Inline(_) => panic!("Expected Static, got Inline"),
            CmdResult::Resp(_) => panic!("Expected Static, got Resp"),
        }
    }

    fn assert_integer(result: CmdResult, expected: i64) {
        match result {
            CmdResult::Resp(RespFrame::Integer(n)) => assert_eq!(n, expected),
            CmdResult::Static(s) => {
                // int_resp may return static bytes for common values (0, 1, -1, -2).
                let expected_bytes = match expected {
                    0 => super::RESP_ZERO,
                    1 => super::RESP_ONE,
                    -1 => super::RESP_NEG_ONE,
                    -2 => super::RESP_NEG_TWO,
                    _ => panic!(
                        "Expected Integer({}), got Static({:?})",
                        expected,
                        std::str::from_utf8(s)
                    ),
                };
                assert_eq!(s, expected_bytes, "Static integer mismatch for {}", expected);
            }
            other => panic!("Expected Integer({}), got {:?}", expected, other),
        }
    }

    fn new_harness() -> TestHarness {
        TestHarness::new()
    }

    // ── DEL tests ───────────────────────────────────────────────────

    #[test]
    fn del_single() {
        let h = new_harness();
        h.set(VortexKey::from("foo"), VortexValue::from("bar"));
        let r = exec(&h.keyspace, cmd_del, &[b"DEL", b"foo"], NOW);
        assert_integer(r, 1);
        assert!(!h.exists(&VortexKey::from("foo"), NOW));
    }

    #[test]
    fn del_multi() {
        let h = new_harness();
        h.set(VortexKey::from("a"), VortexValue::from("1"));
        h.set(VortexKey::from("b"), VortexValue::from("2"));
        h.set(VortexKey::from("c"), VortexValue::from("3"));
        let r = exec(&h.keyspace, cmd_del, &[b"DEL", b"a", b"b", b"d"], NOW);
        assert_integer(r, 2); // "d" doesn't exist.
    }

    #[test]
    fn del_nonexistent() {
        let h = new_harness();
        let r = exec(&h.keyspace, cmd_del, &[b"DEL", b"x"], NOW);
        assert_integer(r, 0);
    }

    // ── EXISTS tests ────────────────────────────────────────────────

    #[test]
    fn exists_single() {
        let h = new_harness();
        h.set(VortexKey::from("foo"), VortexValue::from("bar"));
        let r = exec(&h.keyspace, cmd_exists, &[b"EXISTS", b"foo"], NOW);
        assert_integer(r, 1);
    }

    #[test]
    fn exists_multi_with_duplicates() {
        let h = new_harness();
        h.set(VortexKey::from("k"), VortexValue::from("v"));
        // EXISTS k k (same key twice) — Redis counts each occurrence.
        let r = exec(&h.keyspace, cmd_exists, &[b"EXISTS", b"k", b"k"], NOW);
        assert_integer(r, 2);
    }

    #[test]
    fn exists_miss() {
        let h = new_harness();
        let r = exec(&h.keyspace, cmd_exists, &[b"EXISTS", b"nope"], NOW);
        assert_integer(r, 0);
    }

    // ── EXPIRE + TTL round-trip ─────────────────────────────────────

    #[test]
    fn expire_and_ttl() {
        let h = new_harness();
        h.set(VortexKey::from("mykey"), VortexValue::from("val"));

        // EXPIRE mykey 100
        let r = exec(&h.keyspace, cmd_expire, &[b"EXPIRE", b"mykey", b"100"], NOW);
        assert_static(r, RESP_ONE);

        // TTL mykey => ~100 seconds.
        let r = exec(&h.keyspace, cmd_ttl, &[b"TTL", b"mykey"], NOW);
        // Deadline = NOW + 100*NS_PER_SEC. Remaining = 100.
        assert_integer(r, 100);
    }

    #[test]
    fn pexpire_and_pttl() {
        let h = new_harness();
        h.set(VortexKey::from("pk"), VortexValue::from("pv"));

        let r = exec(&h.keyspace, cmd_pexpire, &[b"PEXPIRE", b"pk", b"5000"], NOW);
        assert_static(r, RESP_ONE);

        let r = exec(&h.keyspace, cmd_pttl, &[b"PTTL", b"pk"], NOW);
        assert_integer(r, 5000);
    }

    #[test]
    fn persist_removes_ttl() {
        let h = new_harness();
        h.set(VortexKey::from("tk"), VortexValue::from("tv"));
        exec(&h.keyspace, cmd_expire, &[b"EXPIRE", b"tk", b"10"], NOW);

        let r = exec(&h.keyspace, cmd_persist, &[b"PERSIST", b"tk"], NOW);
        assert_static(r, RESP_ONE);

        let r = exec(&h.keyspace, cmd_ttl, &[b"TTL", b"tk"], NOW);
        assert_static(r, RESP_NEG_ONE); // No TTL.
    }

    #[test]
    fn ttl_no_key() {
        let h = new_harness();
        let r = exec(&h.keyspace, cmd_ttl, &[b"TTL", b"nokey"], NOW);
        assert_static(r, RESP_NEG_TWO);
    }

    #[test]
    fn ttl_no_expiry() {
        let h = new_harness();
        h.set(VortexKey::from("p"), VortexValue::from("v"));
        let r = exec(&h.keyspace, cmd_ttl, &[b"TTL", b"p"], NOW);
        assert_static(r, RESP_NEG_ONE);
    }

    // ── EXPIREAT / PEXPIREAT ────────────────────────────────────────

    #[test]
    fn expireat_and_expiretime() {
        let h = new_harness();
        h.set(VortexKey::from("ea"), VortexValue::from("v"));

        // EXPIREAT ea 2000 (unix timestamp in seconds)
        let r = exec(
            &h.keyspace,
            cmd_expireat,
            &[b"EXPIREAT", b"ea", b"2000"],
            NOW,
        );
        assert_static(r, RESP_ONE);

        // EXPIRETIME ea => 2000
        let r = exec(&h.keyspace, cmd_expiretime, &[b"EXPIRETIME", b"ea"], NOW);
        assert_integer(r, 2000);
    }

    // ── TYPE ────────────────────────────────────────────────────────

    #[test]
    fn type_string() {
        let h = new_harness();
        h.set(VortexKey::from("s"), VortexValue::from("hello"));
        let r = exec(&h.keyspace, cmd_type, &[b"TYPE", b"s"], NOW);
        match r {
            CmdResult::Resp(RespFrame::SimpleString(s)) => {
                assert_eq!(s.as_ref(), b"string")
            }
            other => panic!("Expected SimpleString, got {:?}", other),
        }
    }

    #[test]
    fn type_none() {
        let h = new_harness();
        let r = exec(&h.keyspace, cmd_type, &[b"TYPE", b"missing"], NOW);
        match r {
            CmdResult::Resp(RespFrame::SimpleString(s)) => {
                assert_eq!(s.as_ref(), b"none")
            }
            other => panic!("Expected SimpleString('none'), got {:?}", other),
        }
    }

    // ── RENAME / RENAMENX ───────────────────────────────────────────

    #[test]
    fn rename_basic() {
        let h = new_harness();
        h.set(VortexKey::from("old"), VortexValue::from("val"));
        let r = exec(&h.keyspace, cmd_rename, &[b"RENAME", b"old", b"new"], NOW);
        assert_static(r, RESP_OK);
        assert!(!h.exists(&VortexKey::from("old"), NOW));
        assert!(h.exists(&VortexKey::from("new"), NOW));
    }

    #[test]
    fn rename_preserves_ttl() {
        let h = new_harness();
        h.set(VortexKey::from("rk"), VortexValue::from("rv"));
        exec(&h.keyspace, cmd_expire, &[b"EXPIRE", b"rk", b"60"], NOW);

        exec(&h.keyspace, cmd_rename, &[b"RENAME", b"rk", b"rk2"], NOW);

        // TTL should still be ~60 on the new key.
        let r = exec(&h.keyspace, cmd_ttl, &[b"TTL", b"rk2"], NOW);
        assert_integer(r, 60);
    }

    #[test]
    fn rename_same_key() {
        let h = new_harness();
        h.set(VortexKey::from("same"), VortexValue::from("v"));
        let r = exec(&h.keyspace, cmd_rename, &[b"RENAME", b"same", b"same"], NOW);
        assert_static(r, RESP_OK);
        assert!(h.exists(&VortexKey::from("same"), NOW));
    }

    #[test]
    fn rename_no_key() {
        let h = new_harness();
        let r = exec(
            &h.keyspace,
            cmd_rename,
            &[b"RENAME", b"missing", b"new"],
            NOW,
        );
        assert_static(r, ERR_NO_SUCH_KEY);
    }

    #[test]
    fn renamenx_success() {
        let h = new_harness();
        h.set(VortexKey::from("a"), VortexValue::from("1"));
        let r = exec(&h.keyspace, cmd_renamenx, &[b"RENAMENX", b"a", b"b"], NOW);
        assert_static(r, RESP_ONE);
    }

    #[test]
    fn renamenx_dest_exists() {
        let h = new_harness();
        h.set(VortexKey::from("a"), VortexValue::from("1"));
        h.set(VortexKey::from("b"), VortexValue::from("2"));
        let r = exec(&h.keyspace, cmd_renamenx, &[b"RENAMENX", b"a", b"b"], NOW);
        assert_static(r, RESP_ZERO);
    }

    // ── SCAN ────────────────────────────────────────────────────────

    #[test]
    fn scan_full_iteration() {
        let h = new_harness();
        let n = 50;
        for i in 0..n {
            let key = format!("key:{i}");
            h.set(VortexKey::from(key.as_str()), VortexValue::from("v"));
        }

        let mut all_keys: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        let mut iterations = 0;

        loop {
            let data = make_resp(&[b"SCAN", cursor.to_string().as_bytes(), b"COUNT", b"10"]);
            let tape = RespTape::parse_pipeline(&data).expect("valid RESP");
            let frame = tape.iter().next().unwrap();
            let result = cmd_scan(&h.keyspace, &frame, NOW);

            match result {
                CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                    // arr[0] = cursor bulk string, arr[1] = keys array.
                    if let RespFrame::BulkString(Some(c)) = &arr[0] {
                        cursor = std::str::from_utf8(c).unwrap().parse().unwrap();
                    }
                    if let RespFrame::Array(Some(keys)) = &arr[1] {
                        for k in keys {
                            if let RespFrame::BulkString(Some(kb)) = k {
                                all_keys.push(String::from_utf8(kb.to_vec()).unwrap());
                            }
                        }
                    }
                }
                _ => panic!("Expected Array response from SCAN"),
            }

            iterations += 1;
            if cursor == 0 {
                break;
            }
            // Safety: prevent infinite loop.
            assert!(
                iterations < 1000,
                "SCAN did not terminate after 1000 iterations"
            );
        }

        // All 50 keys must have been returned at least once.
        all_keys.sort();
        all_keys.dedup();
        assert_eq!(
            all_keys.len(),
            n,
            "SCAN must return all keys. Got {} of {}",
            all_keys.len(),
            n
        );
    }

    #[test]
    fn scan_with_match() {
        let h = new_harness();
        h.set(VortexKey::from("user:1"), VortexValue::from("a"));
        h.set(VortexKey::from("user:2"), VortexValue::from("b"));
        h.set(VortexKey::from("post:1"), VortexValue::from("c"));

        let mut matched: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        loop {
            let cur_str = cursor.to_string();
            let data = make_resp(&[b"SCAN", cur_str.as_bytes(), b"MATCH", b"user:*"]);
            let tape = RespTape::parse_pipeline(&data).expect("valid RESP");
            let frame = tape.iter().next().unwrap();
            let result = cmd_scan(&h.keyspace, &frame, NOW);

            match result {
                CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                    if let RespFrame::BulkString(Some(c)) = &arr[0] {
                        cursor = std::str::from_utf8(c).unwrap().parse().unwrap();
                    }
                    if let RespFrame::Array(Some(keys)) = &arr[1] {
                        for k in keys {
                            if let RespFrame::BulkString(Some(kb)) = k {
                                matched.push(String::from_utf8(kb.to_vec()).unwrap());
                            }
                        }
                    }
                }
                _ => panic!("Expected Array"),
            }
            if cursor == 0 {
                break;
            }
        }

        matched.sort();
        matched.dedup();
        assert_eq!(matched.len(), 2);
        assert!(matched.contains(&"user:1".to_string()));
        assert!(matched.contains(&"user:2".to_string()));
    }

    // ── KEYS ────────────────────────────────────────────────────────

    #[test]
    fn keys_star() {
        let h = new_harness();
        h.set(VortexKey::from("a"), VortexValue::from("1"));
        h.set(VortexKey::from("b"), VortexValue::from("2"));
        let r = exec(&h.keyspace, cmd_keys, &[b"KEYS", b"*"], NOW);
        match r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn keys_pattern() {
        let h = new_harness();
        h.set(VortexKey::from("hello"), VortexValue::from("1"));
        h.set(VortexKey::from("hallo"), VortexValue::from("2"));
        h.set(VortexKey::from("world"), VortexValue::from("3"));
        let r = exec(&h.keyspace, cmd_keys, &[b"KEYS", b"h?llo"], NOW);
        match r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn keys_char_class() {
        let h = new_harness();
        h.set(VortexKey::from("hello"), VortexValue::from("1"));
        h.set(VortexKey::from("hallo"), VortexValue::from("2"));
        h.set(VortexKey::from("hxllo"), VortexValue::from("3"));
        let r = exec(&h.keyspace, cmd_keys, &[b"KEYS", b"h[ae]llo"], NOW);
        match r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert_eq!(arr.len(), 2); // hello, hallo — not hxllo.
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn keys_negated_class() {
        let h = new_harness();
        h.set(VortexKey::from("hello"), VortexValue::from("1"));
        h.set(VortexKey::from("hallo"), VortexValue::from("2"));
        h.set(VortexKey::from("hxllo"), VortexValue::from("3"));
        let r = exec(&h.keyspace, cmd_keys, &[b"KEYS", b"h[^ae]llo"], NOW);
        match r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert_eq!(arr.len(), 1); // hxllo only.
            }
            _ => panic!("Expected array"),
        }
    }

    // ── COPY ────────────────────────────────────────────────────────

    #[test]
    fn copy_basic() {
        let h = new_harness();
        h.set(VortexKey::from("src"), VortexValue::from("val"));
        let r = exec(&h.keyspace, cmd_copy, &[b"COPY", b"src", b"dst"], NOW);
        assert_static(r, RESP_ONE);
        assert!(h.exists(&VortexKey::from("dst"), NOW));
        // Source still exists.
        assert!(h.exists(&VortexKey::from("src"), NOW));
    }

    #[test]
    fn copy_no_replace() {
        let h = new_harness();
        h.set(VortexKey::from("src"), VortexValue::from("v1"));
        h.set(VortexKey::from("dst"), VortexValue::from("v2"));
        let r = exec(&h.keyspace, cmd_copy, &[b"COPY", b"src", b"dst"], NOW);
        assert_static(r, RESP_ZERO); // Destination exists, no REPLACE.
    }

    #[test]
    fn copy_with_replace() {
        let h = new_harness();
        h.set(VortexKey::from("src"), VortexValue::from("new_val"));
        h.set(VortexKey::from("dst"), VortexValue::from("old_val"));
        let r = exec(
            &h.keyspace,
            cmd_copy,
            &[b"COPY", b"src", b"dst", b"REPLACE"],
            NOW,
        );
        assert_static(r, RESP_ONE);
    }

    // ── RANDOMKEY ───────────────────────────────────────────────────

    #[test]
    fn randomkey_empty() {
        let h = new_harness();
        let r = exec(&h.keyspace, cmd_randomkey, &[b"RANDOMKEY"], NOW);
        assert_static(r, RESP_NIL);
    }

    #[test]
    fn randomkey_nonempty() {
        let h = new_harness();
        h.set(VortexKey::from("only"), VortexValue::from("one"));
        let r = exec(&h.keyspace, cmd_randomkey, &[b"RANDOMKEY"], NOW);
        match r {
            CmdResult::Resp(RespFrame::BulkString(Some(b))) => {
                assert_eq!(b.as_ref(), b"only");
            }
            _ => panic!("Expected BulkString"),
        }
    }

    // ── Lazy expiry double-checked locking tests ────────────────────

    /// When GET encounters an expired key, the double-checked locking
    /// mechanism must clean it up (not just return nil).
    #[test]
    fn lazy_expiry_get_cleans_up_expired_key() {
        let h = new_harness();
        // Insert key with TTL deadline = 1000 ns.
        h.set_with_ttl(VortexKey::from("ek"), VortexValue::from("val"), 1000);

        // Before expiry: key exists.
        assert_eq!(h.keyspace.dbsize(), 1);

        // GET at now=2000 → nil AND key removed from table.
        let r = exec(
            &h.keyspace,
            super::super::string::cmd_get,
            &[b"GET", b"ek"],
            2000,
        );
        assert_static(r, RESP_NIL);

        // Key must be cleaned up — dbsize should be 0.
        assert_eq!(h.keyspace.dbsize(), 0);
    }

    /// When TTL encounters an expired key, it returns -2 AND cleans up.
    #[test]
    fn lazy_expiry_ttl_cleans_up_expired_key() {
        let h = new_harness();
        h.set_with_ttl(VortexKey::from("tk"), VortexValue::from("val"), 1000);
        assert_eq!(h.keyspace.dbsize(), 1);

        // TTL at now=2000 → -2 (missing).
        let r = exec(&h.keyspace, cmd_ttl, &[b"TTL", b"tk"], 2000);
        assert_static(r, RESP_NEG_TWO);

        // Key cleaned up.
        assert_eq!(h.keyspace.dbsize(), 0);
    }

    /// When TYPE encounters an expired key, it returns "none" AND cleans up.
    #[test]
    fn lazy_expiry_type_cleans_up_expired_key() {
        let h = new_harness();
        h.set_with_ttl(VortexKey::from("typekey"), VortexValue::from("val"), 1000);
        assert_eq!(h.keyspace.dbsize(), 1);

        let r = exec(&h.keyspace, cmd_type, &[b"TYPE", b"typekey"], 2000);
        match r {
            CmdResult::Resp(vortex_proto::RespFrame::SimpleString(s)) => assert_eq!(s, "none"),
            other => panic!("expected SimpleString(none), got {:?}", other),
        }

        assert_eq!(h.keyspace.dbsize(), 0);
    }

    /// EXISTS with expired keys should return 0 AND clean up.
    #[test]
    fn lazy_expiry_exists_cleans_up_expired_key() {
        let h = new_harness();
        h.set_with_ttl(VortexKey::from("ex1"), VortexValue::from("v1"), 1000);
        h.set_with_ttl(VortexKey::from("ex2"), VortexValue::from("v2"), 1000);
        h.set(VortexKey::from("live"), VortexValue::from("v3"));
        assert_eq!(h.keyspace.dbsize(), 3);

        // EXISTS ex1 ex2 live at now=2000 → only "live" counted.
        let r = exec(
            &h.keyspace,
            cmd_exists,
            &[b"EXISTS", b"ex1", b"ex2", b"live"],
            2000,
        );
        assert_integer(r, 1);

        // Expired keys cleaned up.
        assert_eq!(h.keyspace.dbsize(), 1);
    }

    // ── glob_match tests ────────────────────────────────────────────

    #[test]
    fn glob_star() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"h*o", b"hello"));
        assert!(glob_match(b"h*o", b"ho"));
        assert!(!glob_match(b"h*o", b"hex"));
    }

    #[test]
    fn glob_question() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h?llo", b"hallo"));
        assert!(!glob_match(b"h?llo", b"hllo"));
    }

    #[test]
    fn glob_char_class() {
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hxllo"));
    }

    #[test]
    fn glob_negated_class() {
        assert!(!glob_match(b"h[^ae]llo", b"hello"));
        assert!(glob_match(b"h[^ae]llo", b"hxllo"));
    }

    #[test]
    fn glob_range() {
        assert!(glob_match(b"[a-z]", b"m"));
        assert!(!glob_match(b"[a-z]", b"M"));
    }

    #[test]
    fn glob_escape() {
        assert!(glob_match(b"h\\*llo", b"h*llo"));
        assert!(!glob_match(b"h\\*llo", b"hello"));
    }
}
