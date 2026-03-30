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

use crate::Shard;

use super::{
    CmdResult, NS_PER_MS, NS_PER_SEC, RESP_NEG_ONE, RESP_NEG_TWO, RESP_NIL, RESP_OK, RESP_ONE,
    RESP_ZERO, arg_bytes, arg_count, arg_i64, int_resp, key_from_bytes,
};

// ── Error constants ─────────────────────────────────────────────────

static ERR_NO_SUCH_KEY: &[u8] = b"-ERR no such key\r\n";
static ERR_WRONG_ARGS: &[u8] = b"-ERR wrong number of arguments\r\n";

// ── DEL / UNLINK / EXISTS ───────────────────────────────────────────

/// DEL key [key ...]
/// Removes the specified keys. Returns the number of keys removed.
///
/// Uses software-prefetch pipeline when multiple keys are provided:
/// Phase 1 hashes + prefetches all keys, Phase 2 deletes sequentially.
#[inline]
pub fn cmd_del(shard: &mut Shard, frame: &FrameRef<'_>, _now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 2 {
        return CmdResult::Static(ERR_WRONG_ARGS);
    }
    let n = argc - 1;
    if n == 1 {
        // Fast path: single key, no prefetch overhead.
        if let Some(kb) = arg_bytes(frame, 1) {
            let key = key_from_bytes(kb);
            return int_resp(if shard.del(&key) { 1 } else { 0 });
        }
        return CmdResult::Static(RESP_ZERO);
    }

    // Phase 1: Collect keys and prefetch with write intent.
    let mut key_refs: Vec<&[u8]> = Vec::with_capacity(n);
    for i in 1..argc {
        if let Some(kb) = arg_bytes(frame, i) {
            let key = VortexKey::from(kb);
            shard.prefetch_write(&key);
            key_refs.push(kb);
        }
    }

    // Phase 2: Execute deletes (cache is now warm).
    let mut deleted = 0i64;
    for &kb in &key_refs {
        let key = key_from_bytes(kb);
        if shard.del(&key) {
            deleted += 1;
        }
    }
    int_resp(deleted)
}

/// UNLINK key [key ...]
/// Same as DEL for now (heap deallocation is deferred in Phase 3 via
/// inline entry tombstoning — no heap to free for inline entries).
#[inline]
pub fn cmd_unlink(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    cmd_del(shard, frame, now_nanos)
}

/// EXISTS key [key ...]
/// Returns the count of specified keys that exist.
///
/// Uses software-prefetch pipeline when multiple keys are provided:
/// Phase 1 hashes + prefetches all keys, Phase 2 checks sequentially.
#[inline]
pub fn cmd_exists(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 2 {
        return CmdResult::Static(ERR_WRONG_ARGS);
    }
    let n = argc - 1;
    if n == 1 {
        // Fast path: single key, no prefetch overhead.
        if let Some(kb) = arg_bytes(frame, 1) {
            let key = key_from_bytes(kb);
            return int_resp(if shard.exists(&key, now_nanos) { 1 } else { 0 });
        }
        return CmdResult::Static(RESP_ZERO);
    }

    // Phase 1: Collect keys and prefetch with read intent.
    let mut key_refs: Vec<&[u8]> = Vec::with_capacity(n);
    for i in 1..argc {
        if let Some(kb) = arg_bytes(frame, i) {
            let key = VortexKey::from(kb);
            shard.prefetch(&key);
            key_refs.push(kb);
        }
    }

    // Phase 2: Execute existence checks (cache is now warm).
    let mut count = 0i64;
    for &kb in &key_refs {
        let key = key_from_bytes(kb);
        if shard.exists(&key, now_nanos) {
            count += 1;
        }
    }
    int_resp(count)
}

// ── EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT / PERSIST ───────────────

/// EXPIRE key seconds [NX|XX|GT|LT]
#[inline]
pub fn cmd_expire(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    expire_generic(shard, frame, now_nanos, ExpireMode::RelativeSeconds)
}

/// PEXPIRE key milliseconds [NX|XX|GT|LT]
#[inline]
pub fn cmd_pexpire(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    expire_generic(shard, frame, now_nanos, ExpireMode::RelativeMillis)
}

/// EXPIREAT key timestamp [NX|XX|GT|LT]
#[inline]
pub fn cmd_expireat(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    expire_generic(shard, frame, now_nanos, ExpireMode::AbsoluteSeconds)
}

/// PEXPIREAT key ms-timestamp [NX|XX|GT|LT]
#[inline]
pub fn cmd_pexpireat(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    expire_generic(shard, frame, now_nanos, ExpireMode::AbsoluteMillis)
}

/// PERSIST key
#[inline]
pub fn cmd_persist(shard: &mut Shard, frame: &FrameRef<'_>, _now_nanos: u64) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    if shard.persist(&key) {
        CmdResult::Static(RESP_ONE)
    } else {
        CmdResult::Static(RESP_ZERO)
    }
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
    shard: &mut Shard,
    frame: &FrameRef<'_>,
    now_nanos: u64,
    mode: ExpireMode,
) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 3 {
        return CmdResult::Static(ERR_WRONG_ARGS);
    }
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let Some(time_val) = arg_i64(frame, 2) else {
        return CmdResult::Static(super::ERR_NOT_INTEGER);
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
                        _ => return CmdResult::Static(super::ERR_SYNTAX),
                    }
                }
                _ => return CmdResult::Static(super::ERR_SYNTAX),
            }
        }
    }

    // Compute absolute deadline in nanos.
    let deadline_nanos = match mode {
        ExpireMode::RelativeSeconds => {
            if time_val <= 0 {
                // Non-positive relative TTL: delete the key immediately.
                let key = key_from_bytes(kb);
                return if shard.del(&key) {
                    CmdResult::Static(RESP_ONE)
                } else {
                    CmdResult::Static(RESP_ZERO)
                };
            }
            now_nanos + (time_val as u64) * NS_PER_SEC
        }
        ExpireMode::RelativeMillis => {
            if time_val <= 0 {
                let key = key_from_bytes(kb);
                return if shard.del(&key) {
                    CmdResult::Static(RESP_ONE)
                } else {
                    CmdResult::Static(RESP_ZERO)
                };
            }
            now_nanos + (time_val as u64) * NS_PER_MS
        }
        ExpireMode::AbsoluteSeconds => {
            let deadline = (time_val as u64) * NS_PER_SEC;
            if deadline <= now_nanos {
                let key = key_from_bytes(kb);
                return if shard.del(&key) {
                    CmdResult::Static(RESP_ONE)
                } else {
                    CmdResult::Static(RESP_ZERO)
                };
            }
            deadline
        }
        ExpireMode::AbsoluteMillis => {
            let deadline = (time_val as u64) * NS_PER_MS;
            if deadline <= now_nanos {
                let key = key_from_bytes(kb);
                return if shard.del(&key) {
                    CmdResult::Static(RESP_ONE)
                } else {
                    CmdResult::Static(RESP_ZERO)
                };
            }
            deadline
        }
    };

    let key = key_from_bytes(kb);

    // Check if key exists and get current TTL.
    let current_ttl = match shard.ttl(&key) {
        Some(t) => t,
        None => return CmdResult::Static(RESP_ZERO), // Key doesn't exist.
    };

    // NX: only set if no TTL currently.
    if nx && current_ttl != 0 {
        return CmdResult::Static(RESP_ZERO);
    }
    // XX: only set if TTL already exists.
    if xx && current_ttl == 0 {
        return CmdResult::Static(RESP_ZERO);
    }
    // GT: only set if new deadline > current deadline.
    if gt && current_ttl != 0 && deadline_nanos <= current_ttl {
        return CmdResult::Static(RESP_ZERO);
    }
    // LT: only set if no current TTL or new deadline < current deadline.
    if lt && current_ttl != 0 && deadline_nanos >= current_ttl {
        return CmdResult::Static(RESP_ZERO);
    }

    if shard.expire(&key, deadline_nanos) {
        CmdResult::Static(RESP_ONE)
    } else {
        CmdResult::Static(RESP_ZERO)
    }
}

// ── TTL / PTTL / EXPIRETIME / PEXPIRETIME ───────────────────────────

/// TTL key
#[inline]
pub fn cmd_ttl(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    // Lazy expire.
    if !shard.exists(&key, now_nanos) {
        return CmdResult::Static(RESP_NEG_TWO); // Key doesn't exist.
    }
    match shard.ttl(&key) {
        None => CmdResult::Static(RESP_NEG_TWO),
        Some(0) => CmdResult::Static(RESP_NEG_ONE), // No TTL (persistent).
        Some(deadline) => {
            if deadline <= now_nanos {
                CmdResult::Static(RESP_NEG_TWO)
            } else {
                let remaining_secs = ((deadline - now_nanos) / NS_PER_SEC) as i64;
                int_resp(remaining_secs)
            }
        }
    }
}

/// PTTL key
#[inline]
pub fn cmd_pttl(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    if !shard.exists(&key, now_nanos) {
        return CmdResult::Static(RESP_NEG_TWO);
    }
    match shard.ttl(&key) {
        None => CmdResult::Static(RESP_NEG_TWO),
        Some(0) => CmdResult::Static(RESP_NEG_ONE),
        Some(deadline) => {
            if deadline <= now_nanos {
                CmdResult::Static(RESP_NEG_TWO)
            } else {
                let remaining_ms = ((deadline - now_nanos) / NS_PER_MS) as i64;
                int_resp(remaining_ms)
            }
        }
    }
}

/// EXPIRETIME key
#[inline]
pub fn cmd_expiretime(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    if !shard.exists(&key, now_nanos) {
        return CmdResult::Static(RESP_NEG_TWO);
    }
    match shard.ttl(&key) {
        None => CmdResult::Static(RESP_NEG_TWO),
        Some(0) => CmdResult::Static(RESP_NEG_ONE),
        Some(deadline) => int_resp((deadline / NS_PER_SEC) as i64),
    }
}

/// PEXPIRETIME key
#[inline]
pub fn cmd_pexpiretime(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    if !shard.exists(&key, now_nanos) {
        return CmdResult::Static(RESP_NEG_TWO);
    }
    match shard.ttl(&key) {
        None => CmdResult::Static(RESP_NEG_TWO),
        Some(0) => CmdResult::Static(RESP_NEG_ONE),
        Some(deadline) => int_resp((deadline / NS_PER_MS) as i64),
    }
}

// ── TYPE ────────────────────────────────────────────────────────────

/// TYPE key
#[inline]
pub fn cmd_type(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let key = key_from_bytes(kb);
    match shard.type_of(&key, now_nanos) {
        Some(type_name) => {
            // TYPE returns a simple string response: "+string\r\n"
            CmdResult::Resp(RespFrame::simple_string(type_name))
        }
        None => CmdResult::Resp(RespFrame::simple_string("none")),
    }
}

// ── RENAME / RENAMENX ───────────────────────────────────────────────

/// RENAME key newkey
#[inline]
pub fn cmd_rename(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 3 {
        return CmdResult::Static(ERR_WRONG_ARGS);
    }
    let Some(old_kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let Some(new_kb) = arg_bytes(frame, 2) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let old_key = key_from_bytes(old_kb);
    let new_key = key_from_bytes(new_kb);
    match shard.rename(&old_key, new_key, now_nanos) {
        Ok(()) => CmdResult::Static(RESP_OK),
        Err(msg) => CmdResult::Static(msg),
    }
}

/// RENAMENX key newkey
#[inline]
pub fn cmd_renamenx(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 3 {
        return CmdResult::Static(ERR_WRONG_ARGS);
    }
    let Some(old_kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let Some(new_kb) = arg_bytes(frame, 2) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let old_key = key_from_bytes(old_kb);
    let new_key = key_from_bytes(new_kb);

    // Check if old key exists.
    if !shard.exists(&old_key, now_nanos) {
        return CmdResult::Static(ERR_NO_SUCH_KEY);
    }
    // RENAMENX: fail if newkey already exists.
    if shard.exists(&new_key, now_nanos) {
        return CmdResult::Static(RESP_ZERO);
    }
    match shard.rename(&old_key, new_key, now_nanos) {
        Ok(()) => CmdResult::Static(RESP_ONE),
        Err(msg) => CmdResult::Static(msg),
    }
}

// ── SCAN ────────────────────────────────────────────────────────────

/// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
pub fn cmd_scan(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
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

    // Check for KEYS * equivalent (no pattern or "*").
    let match_all = pattern.is_none() || pattern == Some(b"*");

    let total_slots = shard.total_slots();
    if total_slots == 0 {
        return scan_response(0, &[]);
    }

    let bits = (total_slots as u64).trailing_zeros();
    let mask = total_slots as u64 - 1;

    // Reverse-bit cursor iteration.
    let mut pos = cursor;
    let mut results: Vec<VortexKey> = Vec::with_capacity(count);

    // Walk up to `total_slots` steps maximum.
    for _ in 0..total_slots {
        let slot = pos as usize;
        if slot < total_slots {
            if let Some((key, val)) = shard.slot_key_value(slot) {
                // Check TTL expiry lazily.
                let ttl = shard.slot_entry_ttl(slot);
                if ttl == 0 || ttl > now_nanos {
                    let mut accept = true;
                    // Pattern filter.
                    if !match_all {
                        if let Some(pat) = pattern {
                            accept = glob_match(pat, key.as_bytes());
                        }
                    }
                    // Type filter.
                    if accept {
                        if let Some(tf) = type_filter {
                            accept = eq_ci(tf, val.type_name().as_bytes());
                        }
                    }
                    if accept {
                        results.push(key.clone());
                    }
                }
            }
        }

        // Advance reverse-bit cursor.
        pos = reverse_bit_advance(pos, bits, mask);
        if pos == 0 {
            // Full scan complete.
            return scan_response(0, &results);
        }
        if results.len() >= count {
            return scan_response(pos, &results);
        }
    }

    scan_response(0, &results)
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

/// Reverse-bit increment for SCAN cursor.
///
/// Redis approach: set all high bits outside the mask, reverse all 64 bits,
/// increment, reverse again. This handles table resizes gracefully.
#[inline]
fn reverse_bit_advance(cursor: u64, _bits: u32, mask: u64) -> u64 {
    let mut v = cursor;
    v |= !mask; // Set bits outside table size.
    v = v.reverse_bits(); // Reverse all 64 bits.
    v = v.wrapping_add(1); // Increment.
    v = v.reverse_bits(); // Reverse back.
    v & mask // Mask to table size.
}

// ── KEYS ────────────────────────────────────────────────────────────

/// KEYS pattern
pub fn cmd_keys(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(pat) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let match_all = pat == b"*";
    let total_slots = shard.total_slots();
    let mut results: Vec<RespFrame> = Vec::new();

    for slot in 0..total_slots {
        if let Some((key, _val)) = shard.slot_key_value(slot) {
            let ttl = shard.slot_entry_ttl(slot);
            if ttl != 0 && ttl <= now_nanos {
                continue; // Expired.
            }
            if match_all || glob_match(pat, key.as_bytes()) {
                results.push(RespFrame::bulk_string(bytes::Bytes::copy_from_slice(
                    key.as_bytes(),
                )));
            }
        }
    }

    CmdResult::Resp(RespFrame::Array(Some(results)))
}

// ── RANDOMKEY ───────────────────────────────────────────────────────

/// RANDOMKEY
#[inline]
pub fn cmd_randomkey(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let _ = frame;
    // Use now_nanos as a pseudo-random seed.
    match shard.random_key(now_nanos) {
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
pub fn cmd_touch(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    cmd_exists(shard, frame, now_nanos)
}

// ── COPY ────────────────────────────────────────────────────────────

/// COPY source destination [DB destination-db] [REPLACE]
pub fn cmd_copy(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 3 {
        return CmdResult::Static(ERR_WRONG_ARGS);
    }
    let Some(src_kb) = arg_bytes(frame, 1) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
    };
    let Some(dst_kb) = arg_bytes(frame, 2) else {
        return CmdResult::Static(ERR_WRONG_ARGS);
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

    if shard.copy_key(&src_key, dst_key, replace, now_nanos) {
        CmdResult::Static(RESP_ONE)
    } else {
        CmdResult::Static(RESP_ZERO)
    }
}

// ── Glob pattern matcher ────────────────────────────────────────────

/// Match a Redis-style glob pattern against a string.
///
/// Supported: `*` (any sequence), `?` (any one byte), `[abc]` (char class),
/// `[^abc]` / `[!abc]` (negated char class), `\x` (escape).
///
/// Implemented iteratively with backtracking stack for `*` wildcards.
#[inline]
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut pi = 0usize; // Pattern index.
    let mut si = 0usize; // String index.
    let mut star_pi = usize::MAX; // Pattern index after last `*`.
    let mut star_si = usize::MAX; // String index at last `*` match.

    while si < string.len() {
        if pi < pattern.len() {
            match pattern[pi] {
                b'*' => {
                    star_pi = pi + 1;
                    star_si = si;
                    pi += 1;
                    continue;
                }
                b'?' => {
                    pi += 1;
                    si += 1;
                    continue;
                }
                b'[' => {
                    pi += 1;
                    let negate = pi < pattern.len() && (pattern[pi] == b'^' || pattern[pi] == b'!');
                    if negate {
                        pi += 1;
                    }
                    let mut found = false;
                    let mut class_end = pi;
                    while class_end < pattern.len() && pattern[class_end] != b']' {
                        // Range: a-z
                        if class_end + 2 < pattern.len()
                            && pattern[class_end + 1] == b'-'
                            && pattern[class_end + 2] != b']'
                        {
                            if string[si] >= pattern[class_end]
                                && string[si] <= pattern[class_end + 2]
                            {
                                found = true;
                            }
                            class_end += 3;
                        } else {
                            if string[si] == pattern[class_end] {
                                found = true;
                            }
                            class_end += 1;
                        }
                    }
                    // Skip closing ']'.
                    if class_end < pattern.len() {
                        class_end += 1;
                    }
                    if negate {
                        found = !found;
                    }
                    if found {
                        pi = class_end;
                        si += 1;
                        continue;
                    }
                    // Character class didn't match — fall through to backtrack.
                }
                b'\\' => {
                    pi += 1;
                    if pi < pattern.len() && pattern[pi] == string[si] {
                        pi += 1;
                        si += 1;
                        continue;
                    }
                    // Escaped char didn't match — fall through to backtrack.
                }
                c => {
                    if c == string[si] {
                        pi += 1;
                        si += 1;
                        continue;
                    }
                    // Literal didn't match — fall through to backtrack.
                }
            }
        }

        // No match — try backtracking to last `*`.
        if star_pi != usize::MAX {
            pi = star_pi;
            star_si += 1;
            si = star_si;
            continue;
        }

        return false;
    }

    // Consume trailing `*` in pattern.
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
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

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_common::{ShardId, VortexValue};
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
    fn exec(
        shard: &mut Shard,
        handler: fn(&mut Shard, &FrameRef<'_>, u64) -> CmdResult,
        parts: &[&[u8]],
        now: u64,
    ) -> CmdResult {
        let data = make_resp(parts);
        let tape = RespTape::parse_pipeline(&data).expect("valid RESP input");
        let frame = tape.iter().next().unwrap();
        handler(shard, &frame, now)
    }

    fn assert_static(result: CmdResult, expected: &[u8]) {
        match result {
            CmdResult::Static(s) => assert_eq!(s, expected, "Static mismatch"),
            CmdResult::Resp(_) => panic!("Expected Static, got Resp"),
        }
    }

    fn assert_integer(result: CmdResult, expected: i64) {
        match result {
            CmdResult::Resp(RespFrame::Integer(n)) => assert_eq!(n, expected),
            CmdResult::Static(s) => panic!(
                "Expected Integer({}), got Static({:?})",
                expected,
                std::str::from_utf8(s)
            ),
            other => panic!("Expected Integer({}), got {:?}", expected, other),
        }
    }

    fn new_shard() -> Shard {
        Shard::new_with_time(ShardId::new(0), NOW)
    }

    // ── DEL tests ───────────────────────────────────────────────────

    #[test]
    fn del_single() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("foo"), VortexValue::from("bar"));
        let r = exec(&mut shard, cmd_del, &[b"DEL", b"foo"], NOW);
        assert_integer(r, 1);
        assert!(!shard.exists(&VortexKey::from("foo"), NOW));
    }

    #[test]
    fn del_multi() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("a"), VortexValue::from("1"));
        shard.set(VortexKey::from("b"), VortexValue::from("2"));
        shard.set(VortexKey::from("c"), VortexValue::from("3"));
        let r = exec(&mut shard, cmd_del, &[b"DEL", b"a", b"b", b"d"], NOW);
        assert_integer(r, 2); // "d" doesn't exist.
    }

    #[test]
    fn del_nonexistent() {
        let mut shard = new_shard();
        let r = exec(&mut shard, cmd_del, &[b"DEL", b"x"], NOW);
        assert_integer(r, 0);
    }

    // ── EXISTS tests ────────────────────────────────────────────────

    #[test]
    fn exists_single() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("foo"), VortexValue::from("bar"));
        let r = exec(&mut shard, cmd_exists, &[b"EXISTS", b"foo"], NOW);
        assert_integer(r, 1);
    }

    #[test]
    fn exists_multi_with_duplicates() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("k"), VortexValue::from("v"));
        // EXISTS k k (same key twice) — Redis counts each occurrence.
        let r = exec(&mut shard, cmd_exists, &[b"EXISTS", b"k", b"k"], NOW);
        assert_integer(r, 2);
    }

    #[test]
    fn exists_miss() {
        let mut shard = new_shard();
        let r = exec(&mut shard, cmd_exists, &[b"EXISTS", b"nope"], NOW);
        assert_integer(r, 0);
    }

    // ── EXPIRE + TTL round-trip ─────────────────────────────────────

    #[test]
    fn expire_and_ttl() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("mykey"), VortexValue::from("val"));

        // EXPIRE mykey 100
        let r = exec(&mut shard, cmd_expire, &[b"EXPIRE", b"mykey", b"100"], NOW);
        assert_static(r, RESP_ONE);

        // TTL mykey => ~100 seconds.
        let r = exec(&mut shard, cmd_ttl, &[b"TTL", b"mykey"], NOW);
        // Deadline = NOW + 100*NS_PER_SEC. Remaining = 100.
        assert_integer(r, 100);
    }

    #[test]
    fn pexpire_and_pttl() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("pk"), VortexValue::from("pv"));

        let r = exec(&mut shard, cmd_pexpire, &[b"PEXPIRE", b"pk", b"5000"], NOW);
        assert_static(r, RESP_ONE);

        let r = exec(&mut shard, cmd_pttl, &[b"PTTL", b"pk"], NOW);
        assert_integer(r, 5000);
    }

    #[test]
    fn persist_removes_ttl() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("tk"), VortexValue::from("tv"));
        exec(&mut shard, cmd_expire, &[b"EXPIRE", b"tk", b"10"], NOW);

        let r = exec(&mut shard, cmd_persist, &[b"PERSIST", b"tk"], NOW);
        assert_static(r, RESP_ONE);

        let r = exec(&mut shard, cmd_ttl, &[b"TTL", b"tk"], NOW);
        assert_static(r, RESP_NEG_ONE); // No TTL.
    }

    #[test]
    fn ttl_no_key() {
        let mut shard = new_shard();
        let r = exec(&mut shard, cmd_ttl, &[b"TTL", b"nokey"], NOW);
        assert_static(r, RESP_NEG_TWO);
    }

    #[test]
    fn ttl_no_expiry() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("p"), VortexValue::from("v"));
        let r = exec(&mut shard, cmd_ttl, &[b"TTL", b"p"], NOW);
        assert_static(r, RESP_NEG_ONE);
    }

    // ── EXPIREAT / PEXPIREAT ────────────────────────────────────────

    #[test]
    fn expireat_and_expiretime() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("ea"), VortexValue::from("v"));

        // EXPIREAT ea 2000 (unix timestamp in seconds)
        let r = exec(
            &mut shard,
            cmd_expireat,
            &[b"EXPIREAT", b"ea", b"2000"],
            NOW,
        );
        assert_static(r, RESP_ONE);

        // EXPIRETIME ea => 2000
        let r = exec(&mut shard, cmd_expiretime, &[b"EXPIRETIME", b"ea"], NOW);
        assert_integer(r, 2000);
    }

    // ── TYPE ────────────────────────────────────────────────────────

    #[test]
    fn type_string() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("s"), VortexValue::from("hello"));
        let r = exec(&mut shard, cmd_type, &[b"TYPE", b"s"], NOW);
        match r {
            CmdResult::Resp(RespFrame::SimpleString(s)) => {
                assert_eq!(s.as_ref(), b"string")
            }
            other => panic!("Expected SimpleString, got {:?}", other),
        }
    }

    #[test]
    fn type_none() {
        let mut shard = new_shard();
        let r = exec(&mut shard, cmd_type, &[b"TYPE", b"missing"], NOW);
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
        let mut shard = new_shard();
        shard.set(VortexKey::from("old"), VortexValue::from("val"));
        let r = exec(&mut shard, cmd_rename, &[b"RENAME", b"old", b"new"], NOW);
        assert_static(r, RESP_OK);
        assert!(!shard.exists(&VortexKey::from("old"), NOW));
        assert!(shard.exists(&VortexKey::from("new"), NOW));
    }

    #[test]
    fn rename_preserves_ttl() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("rk"), VortexValue::from("rv"));
        exec(&mut shard, cmd_expire, &[b"EXPIRE", b"rk", b"60"], NOW);

        exec(&mut shard, cmd_rename, &[b"RENAME", b"rk", b"rk2"], NOW);

        // TTL should still be ~60 on the new key.
        let r = exec(&mut shard, cmd_ttl, &[b"TTL", b"rk2"], NOW);
        assert_integer(r, 60);
    }

    #[test]
    fn rename_same_key() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("same"), VortexValue::from("v"));
        let r = exec(&mut shard, cmd_rename, &[b"RENAME", b"same", b"same"], NOW);
        assert_static(r, RESP_OK);
        assert!(shard.exists(&VortexKey::from("same"), NOW));
    }

    #[test]
    fn rename_no_key() {
        let mut shard = new_shard();
        let r = exec(
            &mut shard,
            cmd_rename,
            &[b"RENAME", b"missing", b"new"],
            NOW,
        );
        assert_static(r, ERR_NO_SUCH_KEY);
    }

    #[test]
    fn renamenx_success() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("a"), VortexValue::from("1"));
        let r = exec(&mut shard, cmd_renamenx, &[b"RENAMENX", b"a", b"b"], NOW);
        assert_static(r, RESP_ONE);
    }

    #[test]
    fn renamenx_dest_exists() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("a"), VortexValue::from("1"));
        shard.set(VortexKey::from("b"), VortexValue::from("2"));
        let r = exec(&mut shard, cmd_renamenx, &[b"RENAMENX", b"a", b"b"], NOW);
        assert_static(r, RESP_ZERO);
    }

    // ── SCAN ────────────────────────────────────────────────────────

    #[test]
    fn scan_full_iteration() {
        let mut shard = new_shard();
        let n = 50;
        for i in 0..n {
            let key = format!("key:{i}");
            shard.set(VortexKey::from(key.as_str()), VortexValue::from("v"));
        }

        let mut all_keys: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        let mut iterations = 0;

        loop {
            let data = make_resp(&[b"SCAN", cursor.to_string().as_bytes(), b"COUNT", b"10"]);
            let tape = RespTape::parse_pipeline(&data).expect("valid RESP");
            let frame = tape.iter().next().unwrap();
            let result = cmd_scan(&mut shard, &frame, NOW);

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
        let mut shard = new_shard();
        shard.set(VortexKey::from("user:1"), VortexValue::from("a"));
        shard.set(VortexKey::from("user:2"), VortexValue::from("b"));
        shard.set(VortexKey::from("post:1"), VortexValue::from("c"));

        let mut matched: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        loop {
            let cur_str = cursor.to_string();
            let data = make_resp(&[b"SCAN", cur_str.as_bytes(), b"MATCH", b"user:*"]);
            let tape = RespTape::parse_pipeline(&data).expect("valid RESP");
            let frame = tape.iter().next().unwrap();
            let result = cmd_scan(&mut shard, &frame, NOW);

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
        let mut shard = new_shard();
        shard.set(VortexKey::from("a"), VortexValue::from("1"));
        shard.set(VortexKey::from("b"), VortexValue::from("2"));
        let r = exec(&mut shard, cmd_keys, &[b"KEYS", b"*"], NOW);
        match r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn keys_pattern() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("hello"), VortexValue::from("1"));
        shard.set(VortexKey::from("hallo"), VortexValue::from("2"));
        shard.set(VortexKey::from("world"), VortexValue::from("3"));
        let r = exec(&mut shard, cmd_keys, &[b"KEYS", b"h?llo"], NOW);
        match r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn keys_char_class() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("hello"), VortexValue::from("1"));
        shard.set(VortexKey::from("hallo"), VortexValue::from("2"));
        shard.set(VortexKey::from("hxllo"), VortexValue::from("3"));
        let r = exec(&mut shard, cmd_keys, &[b"KEYS", b"h[ae]llo"], NOW);
        match r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert_eq!(arr.len(), 2); // hello, hallo — not hxllo.
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn keys_negated_class() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("hello"), VortexValue::from("1"));
        shard.set(VortexKey::from("hallo"), VortexValue::from("2"));
        shard.set(VortexKey::from("hxllo"), VortexValue::from("3"));
        let r = exec(&mut shard, cmd_keys, &[b"KEYS", b"h[^ae]llo"], NOW);
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
        let mut shard = new_shard();
        shard.set(VortexKey::from("src"), VortexValue::from("val"));
        let r = exec(&mut shard, cmd_copy, &[b"COPY", b"src", b"dst"], NOW);
        assert_static(r, RESP_ONE);
        assert!(shard.exists(&VortexKey::from("dst"), NOW));
        // Source still exists.
        assert!(shard.exists(&VortexKey::from("src"), NOW));
    }

    #[test]
    fn copy_no_replace() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("src"), VortexValue::from("v1"));
        shard.set(VortexKey::from("dst"), VortexValue::from("v2"));
        let r = exec(&mut shard, cmd_copy, &[b"COPY", b"src", b"dst"], NOW);
        assert_static(r, RESP_ZERO); // Destination exists, no REPLACE.
    }

    #[test]
    fn copy_with_replace() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("src"), VortexValue::from("new_val"));
        shard.set(VortexKey::from("dst"), VortexValue::from("old_val"));
        let r = exec(
            &mut shard,
            cmd_copy,
            &[b"COPY", b"src", b"dst", b"REPLACE"],
            NOW,
        );
        assert_static(r, RESP_ONE);
    }

    // ── RANDOMKEY ───────────────────────────────────────────────────

    #[test]
    fn randomkey_empty() {
        let mut shard = new_shard();
        let r = exec(&mut shard, cmd_randomkey, &[b"RANDOMKEY"], NOW);
        assert_static(r, RESP_NIL);
    }

    #[test]
    fn randomkey_nonempty() {
        let mut shard = new_shard();
        shard.set(VortexKey::from("only"), VortexValue::from("one"));
        let r = exec(&mut shard, cmd_randomkey, &[b"RANDOMKEY"], NOW);
        match r {
            CmdResult::Resp(RespFrame::BulkString(Some(b))) => {
                assert_eq!(b.as_ref(), b"only");
            }
            _ => panic!("Expected BulkString"),
        }
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
