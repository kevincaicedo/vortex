//! String command handlers for VortexDB.
//!
//! All 19 Redis String commands: GET, SET, SETNX, SETEX, PSETEX, MSET, MSETNX,
//! MGET, GETSET, GETDEL, GETEX, GETRANGE, SETRANGE, APPEND, INCR, INCRBY,
//! INCRBYFLOAT, DECR, DECRBY, STRLEN.

use bytes::Bytes;
use vortex_common::VortexValue;
use vortex_proto::{FrameRef, RespFrame};

use crate::shard::{SetOptions, SetResult};

#[cfg(test)]
use vortex_common::VortexKey;

use super::{
    CmdResult, ERR_NOT_FLOAT, ERR_NOT_INTEGER, ERR_SYNTAX, NS_PER_MS, NS_PER_SEC, RESP_NIL,
    RESP_OK, RESP_ZERO, arg_bytes, arg_count, arg_i64, int_resp, key_from_bytes,
    owned_value_to_resp, value_from_bytes, value_to_resp,
};
use crate::ConcurrentKeyspace;

#[cfg(test)]
use super::ERR_OVERFLOW;

// ── GET ─────────────────────────────────────────────────────────────────────

/// GET key
///
/// Returns the value of key, or nil if the key does not exist.
/// Performs lazy expiry with double-checked locking.
/// Formats RESP directly from borrowed `&VortexValue` while the read guard
/// is held — zero-clone hot path.
#[inline]
pub fn cmd_get(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(RESP_NIL),
    };
    let key = key_from_bytes(key_bytes);
    let shard_index = keyspace.shard_index(key.as_bytes());
    let guard = keyspace.read_shard_by_index(shard_index);
    match guard.get_with_ttl(&key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
            // Hot path: format RESP from &VortexValue while read lock is held.
            value_to_resp(value)
        }
        Some(_) => {
            // Expired — double-checked locking cleanup.
            drop(guard);
            let mut wguard = keyspace.write_shard_by_index(shard_index);
            super::context::remove_if_expired(&mut wguard, &key, now_nanos);
            CmdResult::Static(RESP_NIL)
        }
        None => CmdResult::Static(RESP_NIL),
    }
}

// ── SET ─────────────────────────────────────────────────────────────────────

/// SET key value [EX seconds | PX milliseconds | EXAT unix-time-seconds |
///   PXAT unix-time-milliseconds | KEEPTTL] [NX | XX] [GET]
#[inline]
pub fn cmd_set(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 3 {
        return CmdResult::Static(ERR_SYNTAX);
    }

    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let val_bytes = match arg_bytes(frame, 2) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };

    let key = key_from_bytes(key_bytes);
    let value = value_from_bytes(val_bytes);

    // Parse options.
    let mut ttl_deadline: u64 = 0;
    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut keepttl = false;

    let mut i = 3;
    while i < argc {
        let opt = match arg_bytes(frame, i) {
            Some(b) => b,
            None => return CmdResult::Static(ERR_SYNTAX),
        };
        match opt_upper(opt) {
            OptToken::EX => {
                i += 1;
                let secs = match arg_i64(frame, i) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                ttl_deadline = now_nanos + secs * NS_PER_SEC;
            }
            OptToken::PX => {
                i += 1;
                let ms = match arg_i64(frame, i) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                ttl_deadline = now_nanos + ms * NS_PER_MS;
            }
            OptToken::EXAT => {
                i += 1;
                let secs = match arg_i64(frame, i) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                ttl_deadline = secs * NS_PER_SEC;
            }
            OptToken::PXAT => {
                i += 1;
                let ms = match arg_i64(frame, i) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                ttl_deadline = ms * NS_PER_MS;
            }
            OptToken::NX => nx = true,
            OptToken::XX => xx = true,
            OptToken::GET => get = true,
            OptToken::KEEPTTL => keepttl = true,
            OptToken::Unknown => return CmdResult::Static(ERR_SYNTAX),
        }
        i += 1;
    }

    // NX and XX are mutually exclusive.
    if nx && xx {
        return CmdResult::Static(ERR_SYNTAX);
    }

    let options = SetOptions {
        ttl_deadline,
        nx,
        xx,
        get,
        keepttl,
    };

    match keyspace.set_value_with_options(key, value, options, now_nanos) {
        SetResult::Ok => CmdResult::Static(RESP_OK),
        SetResult::NotSet => CmdResult::Static(RESP_NIL),
        SetResult::OkGet(Some(old)) => owned_value_to_resp(old),
        SetResult::OkGet(None) => CmdResult::Static(RESP_NIL),
        SetResult::NotSetGet(Some(old)) => owned_value_to_resp(old),
        SetResult::NotSetGet(None) => CmdResult::Static(RESP_NIL),
    }
}

// ── SETNX ───────────────────────────────────────────────────────────────────

/// SETNX key value — SET if Not eXists.
///
/// Returns 1 if set, 0 if key already exists.
#[inline]
pub fn cmd_setnx(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let val_bytes = match arg_bytes(frame, 2) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };

    let key = key_from_bytes(key_bytes);
    let value = value_from_bytes(val_bytes);
    match keyspace.set_value_with_options(
        key,
        value,
        SetOptions {
            nx: true,
            ..SetOptions::default()
        },
        now_nanos,
    ) {
        SetResult::Ok => CmdResult::Static(super::RESP_ONE),
        SetResult::NotSet => CmdResult::Static(RESP_ZERO),
        _ => CmdResult::Static(RESP_ZERO),
    }
}

// ── SETEX ───────────────────────────────────────────────────────────────────

/// SETEX key seconds value — SET with EXpire.
#[inline]
pub fn cmd_setex(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let secs = match arg_i64(frame, 2) {
        Some(s) if s > 0 => s as u64,
        _ => return CmdResult::Static(ERR_NOT_INTEGER),
    };
    let val_bytes = match arg_bytes(frame, 3) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };

    let key = key_from_bytes(key_bytes);
    let value = value_from_bytes(val_bytes);
    let deadline = now_nanos + secs * NS_PER_SEC;
    keyspace.set_value_with_ttl(key, value, deadline);
    CmdResult::Static(RESP_OK)
}

// ── PSETEX ──────────────────────────────────────────────────────────────────

/// PSETEX key milliseconds value — SET with PX expire.
#[inline]
pub fn cmd_psetex(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let ms = match arg_i64(frame, 2) {
        Some(s) if s > 0 => s as u64,
        _ => return CmdResult::Static(ERR_NOT_INTEGER),
    };
    let val_bytes = match arg_bytes(frame, 3) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };

    let key = key_from_bytes(key_bytes);
    let value = value_from_bytes(val_bytes);
    let deadline = now_nanos + ms * NS_PER_MS;
    keyspace.set_value_with_ttl(key, value, deadline);
    CmdResult::Static(RESP_OK)
}

// ── MGET ────────────────────────────────────────────────────────────────────

/// MGET key [key ...] — Returns values of all specified keys.
pub fn cmd_mget(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 2 {
        return CmdResult::Static(ERR_SYNTAX);
    }
    let mut keys = Vec::with_capacity(argc - 1);
    if let Some(mut children) = frame.children() {
        children.next(); // skip command name
        for child in children {
            if let Some(bytes) = child.as_bytes() {
                keys.push(key_from_bytes(bytes));
            }
        }
    }

    let mut frames = Vec::with_capacity(keys.len());
    for value in keyspace.mget_values(&keys, now_nanos) {
        match value {
            Some(val) => match val {
                VortexValue::InlineString(ib) => {
                    frames.push(RespFrame::bulk_string(Bytes::copy_from_slice(
                        ib.as_bytes(),
                    )));
                }
                VortexValue::String(b) => {
                    frames.push(RespFrame::bulk_string(b.clone()));
                }
                VortexValue::Integer(n) => {
                    let mut buf = itoa::Buffer::new();
                    let s = buf.format(n);
                    frames.push(RespFrame::bulk_string(Bytes::copy_from_slice(s.as_bytes())));
                }
                _ => frames.push(RespFrame::null_bulk_string()),
            },
            None => frames.push(RespFrame::null_bulk_string()),
        }
    }

    CmdResult::Resp(RespFrame::Array(Some(frames)))
}

// ── MSET ────────────────────────────────────────────────────────────────────

/// MSET key value [key value ...] — Sets multiple key-value pairs.
pub fn cmd_mset(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, _now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 3 || (argc - 1) % 2 != 0 {
        return CmdResult::Static(ERR_SYNTAX);
    }

    let mut pairs = Vec::with_capacity((argc - 1) / 2);
    if let Some(mut children) = frame.children() {
        children.next(); // skip command name
        while let Some(key_frame) = children.next() {
            if let Some(val_frame) = children.next() {
                if let (Some(kb), Some(vb)) = (key_frame.as_bytes(), val_frame.as_bytes()) {
                    pairs.push((key_from_bytes(kb), value_from_bytes(vb)));
                }
            }
        }
    }

    keyspace.mset_values(pairs);

    CmdResult::Static(RESP_OK)
}

// ── MSETNX ──────────────────────────────────────────────────────────────────

/// MSETNX key value [key value ...] — SET NX for multiple keys.
///
/// Atomic: either ALL keys are set, or NONE. Returns 1 if set, 0 otherwise.
pub fn cmd_msetnx(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 3 || (argc - 1) % 2 != 0 {
        return CmdResult::Static(ERR_SYNTAX);
    }

    let mut pairs = Vec::with_capacity((argc - 1) / 2);
    if let Some(mut children) = frame.children() {
        children.next();
        while let Some(key_frame) = children.next() {
            if let Some(value_frame) = children.next() {
                if let (Some(key_bytes), Some(value_bytes)) =
                    (key_frame.as_bytes(), value_frame.as_bytes())
                {
                    pairs.push((key_from_bytes(key_bytes), value_from_bytes(value_bytes)));
                }
            }
        }
    }

    if keyspace.msetnx_values(pairs, now_nanos) {
        CmdResult::Static(super::RESP_ONE)
    } else {
        CmdResult::Static(RESP_ZERO)
    }
}

// ── GETSET ──────────────────────────────────────────────────────────────────

/// GETSET key value — Atomically set and return old value.
///
/// Deprecated in favor of SET ... GET, but still supported.
#[inline]
pub fn cmd_getset(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let val_bytes = match arg_bytes(frame, 2) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };

    let key = key_from_bytes(key_bytes);
    let value = value_from_bytes(val_bytes);
    match keyspace.set_value_with_options(
        key,
        value,
        SetOptions {
            get: true,
            ..SetOptions::default()
        },
        now_nanos,
    ) {
        SetResult::OkGet(Some(old)) => owned_value_to_resp(old),
        SetResult::OkGet(None) => CmdResult::Static(RESP_NIL),
        _ => CmdResult::Static(RESP_NIL),
    }
}

// ── GETDEL ──────────────────────────────────────────────────────────────────

/// GETDEL key — Get the value and delete the key.
#[inline]
pub fn cmd_getdel(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let key = key_from_bytes(key_bytes);
    match keyspace.remove_value(&key, now_nanos) {
        Some(val) => owned_value_to_resp(val),
        None => CmdResult::Static(RESP_NIL),
    }
}

// ── GETEX ───────────────────────────────────────────────────────────────────

/// GETEX key [EX seconds | PX ms | EXAT secs | PXAT ms | PERSIST]
///
/// Get value and optionally set/remove TTL.
#[inline]
pub fn cmd_getex(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let key = key_from_bytes(key_bytes);
    let argc = arg_count(frame);

    // First, get the value.
    let val = match keyspace.get_value(&key, now_nanos) {
        Some(v) => v,
        None => return CmdResult::Static(RESP_NIL),
    };

    // Then apply TTL modification if specified.
    if argc >= 3 {
        let opt = match arg_bytes(frame, 2) {
            Some(b) => b,
            None => return CmdResult::Static(ERR_SYNTAX),
        };
        match opt_upper(opt) {
            OptToken::EX => {
                let secs = match arg_i64(frame, 3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                keyspace.expire_key(&key, now_nanos + secs * NS_PER_SEC, now_nanos);
            }
            OptToken::PX => {
                let ms = match arg_i64(frame, 3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                keyspace.expire_key(&key, now_nanos + ms * NS_PER_MS, now_nanos);
            }
            OptToken::EXAT => {
                let secs = match arg_i64(frame, 3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                keyspace.expire_key(&key, secs * NS_PER_SEC, now_nanos);
            }
            OptToken::PXAT => {
                let ms = match arg_i64(frame, 3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                keyspace.expire_key(&key, ms * NS_PER_MS, now_nanos);
            }
            OptToken::KEEPTTL => { /* PERSIST alias in GETEX context */ }
            _ => {
                // Check for "PERSIST" keyword.
                if eq_ci(opt, b"PERSIST") {
                    keyspace.persist_key(&key);
                } else {
                    return CmdResult::Static(ERR_SYNTAX);
                }
            }
        }
    }

    owned_value_to_resp(val)
}

// ── INCR / INCRBY / DECR / DECRBY ──────────────────────────────────────────

/// Generic increment/decrement implementation.
///
/// If key doesn't exist, treats it as 0.
/// If key exists with an integer value, modifies in-place.
/// If key exists with a string value that parses as integer, converts.
#[inline]
fn incr_by(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    delta: i64,
    now_nanos: u64,
) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let key = key_from_bytes(key_bytes);

    match keyspace.increment_by(key, delta, now_nanos) {
        Ok(result) => int_resp(result),
        Err(err) => CmdResult::Static(err),
    }
}

/// INCR key — Increment by 1.
#[inline]
pub fn cmd_incr(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    incr_by(keyspace, frame, 1, now_nanos)
}

/// DECR key — Decrement by 1.
#[inline]
pub fn cmd_decr(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    incr_by(keyspace, frame, -1, now_nanos)
}

/// INCRBY key increment.
#[inline]
pub fn cmd_incrby(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let delta = match arg_i64(frame, 2) {
        Some(d) => d,
        None => return CmdResult::Static(ERR_NOT_INTEGER),
    };
    incr_by(keyspace, frame, delta, now_nanos)
}

/// DECRBY key decrement.
#[inline]
pub fn cmd_decrby(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let delta = match arg_i64(frame, 2) {
        Some(d) => d,
        None => return CmdResult::Static(ERR_NOT_INTEGER),
    };
    incr_by(keyspace, frame, -delta, now_nanos)
}

// ── INCRBYFLOAT ─────────────────────────────────────────────────────────────

/// INCRBYFLOAT key increment.
///
/// Result is stored as a string (Redis behavior).
pub fn cmd_incrbyfloat(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let incr_bytes = match arg_bytes(frame, 2) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };

    let incr: f64 = match std::str::from_utf8(incr_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(f) => f,
        None => return CmdResult::Static(ERR_NOT_FLOAT),
    };

    if incr.is_nan() || incr.is_infinite() {
        return CmdResult::Static(ERR_NOT_FLOAT);
    }

    let key = key_from_bytes(key_bytes);

    match keyspace.increment_by_float(key, incr, now_nanos) {
        Ok(bytes) => CmdResult::Resp(RespFrame::bulk_string(bytes)),
        Err(err) => CmdResult::Static(err),
    }
}

// ── APPEND ──────────────────────────────────────────────────────────────────

/// APPEND key value — Appends to existing string or creates new one.
///
/// Returns the length of the string after the append operation.
pub fn cmd_append(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let append_bytes = match arg_bytes(frame, 2) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };

    let key = key_from_bytes(key_bytes);

    match keyspace.append_value(key, append_bytes, now_nanos) {
        Ok(length) => int_resp(length as i64),
        Err(err) => CmdResult::Static(err),
    }
}

// ── STRLEN ──────────────────────────────────────────────────────────────────

/// STRLEN key — Returns the length of the string value.
#[inline]
pub fn cmd_strlen(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let key = key_from_bytes(key_bytes);
    match keyspace.strlen_value(&key, now_nanos) {
        Ok(Some(length)) => int_resp(length as i64),
        Ok(None) => CmdResult::Static(RESP_ZERO),
        Err(err) => CmdResult::Static(err),
    }
}

// ── GETRANGE ────────────────────────────────────────────────────────────────

/// GETRANGE key start end — Returns a substring of the string.
///
/// Supports negative indices (from the end).
pub fn cmd_getrange(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let start = match arg_i64(frame, 2) {
        Some(s) => s,
        None => return CmdResult::Static(ERR_NOT_INTEGER),
    };
    let end = match arg_i64(frame, 3) {
        Some(e) => e,
        None => return CmdResult::Static(ERR_NOT_INTEGER),
    };

    let key = key_from_bytes(key_bytes);
    match keyspace.getrange_value(&key, start, end, now_nanos) {
        Ok(Some(bytes)) if bytes.is_empty() => CmdResult::Static(super::RESP_EMPTY_BULK),
        Ok(Some(bytes)) => CmdResult::Resp(RespFrame::bulk_string(bytes)),
        Ok(None) => CmdResult::Static(super::RESP_EMPTY_BULK),
        Err(err) => CmdResult::Static(err),
    }
}

// ── SETRANGE ────────────────────────────────────────────────────────────────

/// SETRANGE key offset value — Overwrites part of the string.
///
/// If offset is beyond current length, zero-pads the string.
pub fn cmd_setrange(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let offset = match arg_i64(frame, 2) {
        Some(o) if o >= 0 => o as usize,
        _ => return CmdResult::Static(ERR_NOT_INTEGER),
    };
    let new_bytes = match arg_bytes(frame, 3) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };

    let key = key_from_bytes(key_bytes);
    match keyspace.setrange_value(key, offset, new_bytes, now_nanos) {
        Ok(length) => int_resp(length as i64),
        Err(err) => CmdResult::Static(err),
    }
}

// ── Option parsing helpers ──────────────────────────────────────────────────

/// SET option tokens.
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, PartialEq, Eq)]
enum OptToken {
    EX,
    PX,
    EXAT,
    PXAT,
    NX,
    XX,
    GET,
    KEEPTTL,
    Unknown,
}

/// Case-insensitive option matching using length + first byte.
#[inline]
fn opt_upper(b: &[u8]) -> OptToken {
    match b.len() {
        2 => {
            let a = b[0] | 0x20;
            let b = b[1] | 0x20;
            match (a, b) {
                (b'e', b'x') => OptToken::EX,
                (b'p', b'x') => OptToken::PX,
                (b'n', b'x') => OptToken::NX,
                (b'x', b'x') => OptToken::XX,
                _ => OptToken::Unknown,
            }
        }
        3 => {
            if eq_ci(b, b"GET") {
                OptToken::GET
            } else {
                OptToken::Unknown
            }
        }
        4 => {
            let a = b[0] | 0x20;
            if a == b'e' && eq_ci(b, b"EXAT") {
                OptToken::EXAT
            } else if a == b'p' && eq_ci(b, b"PXAT") {
                OptToken::PXAT
            } else {
                OptToken::Unknown
            }
        }
        7 => {
            if eq_ci(b, b"KEEPTTL") {
                OptToken::KEEPTTL
            } else {
                OptToken::Unknown
            }
        }
        _ => OptToken::Unknown,
    }
}

/// Case-insensitive comparison (ASCII only).
#[inline]
fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::commands::test_harness::TestHarness;
    use vortex_proto::RespTape;

    /// Helper: parse a raw RESP command and return (tape, shard).
    /// Caller must do `tape.iter().next().unwrap()` to get the FrameRef.
    fn make_tape(input: &[u8]) -> RespTape {
        RespTape::parse_pipeline(input).expect("valid RESP input")
    }

    /// Assert a CmdResult is a static byte slice.
    fn assert_static(result: &CmdResult, expected: &[u8]) {
        match result {
            CmdResult::Static(b) => assert_eq!(*b, expected, "static mismatch"),
            CmdResult::Resp(_) => panic!("expected Static, got Resp"),
        }
    }

    /// Extract bulk string bytes from a CmdResult::Resp.
    fn resp_bytes(result: &CmdResult) -> &[u8] {
        match result {
            CmdResult::Resp(RespFrame::BulkString(Some(b))) => b.as_ref(),
            CmdResult::Resp(other) => panic!("expected BulkString, got {:?}", other),
            CmdResult::Static(b) => {
                panic!("expected Resp, got Static({:?})", std::str::from_utf8(b))
            }
        }
    }

    /// Extract integer from a CmdResult::Resp.
    fn resp_int(result: &CmdResult) -> i64 {
        match result {
            CmdResult::Resp(RespFrame::Integer(n)) => *n,
            CmdResult::Resp(other) => panic!("expected Integer, got {:?}", other),
            CmdResult::Static(b) => {
                panic!("expected Resp, got Static({:?})", std::str::from_utf8(b))
            }
        }
    }

    // ── Option parsing ──

    #[test]
    fn opt_upper_cases() {
        assert_eq!(opt_upper(b"EX"), OptToken::EX);
        assert_eq!(opt_upper(b"ex"), OptToken::EX);
        assert_eq!(opt_upper(b"PX"), OptToken::PX);
        assert_eq!(opt_upper(b"px"), OptToken::PX);
        assert_eq!(opt_upper(b"NX"), OptToken::NX);
        assert_eq!(opt_upper(b"XX"), OptToken::XX);
        assert_eq!(opt_upper(b"EXAT"), OptToken::EXAT);
        assert_eq!(opt_upper(b"PXAT"), OptToken::PXAT);
        assert_eq!(opt_upper(b"GET"), OptToken::GET);
        assert_eq!(opt_upper(b"get"), OptToken::GET);
        assert_eq!(opt_upper(b"KEEPTTL"), OptToken::KEEPTTL);
        assert_eq!(opt_upper(b"keepttl"), OptToken::KEEPTTL);
        assert_eq!(opt_upper(b"BOGUS"), OptToken::Unknown);
    }

    #[test]
    fn eq_ci_cases() {
        assert!(eq_ci(b"GET", b"GET"));
        assert!(eq_ci(b"get", b"GET"));
        assert!(eq_ci(b"Get", b"GET"));
        assert!(!eq_ci(b"GE", b"GET"));
        assert!(!eq_ci(b"SET", b"GET"));
    }

    // ── GET ──

    #[test]
    fn get_existing_key() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"foo" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"bar"));
        let tape = make_tape(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_get(&h.keyspace, &frame, 0);
        assert_eq!(resp_bytes(&result), b"bar");
    }

    #[test]
    fn get_missing_key() {
        let h = TestHarness::new();
        let tape = make_tape(b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_get(&h.keyspace, &frame, 0);
        assert_static(&result, RESP_NIL);
    }

    #[test]
    fn get_integer_value() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"num" as &[u8]);
        h.set(key, VortexValue::Integer(42));
        let tape = make_tape(b"*2\r\n$3\r\nGET\r\n$3\r\nnum\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_get(&h.keyspace, &frame, 0);
        assert_eq!(resp_bytes(&result), b"42");
    }

    // ── SET ──

    #[test]
    fn set_basic() {
        let h = TestHarness::new();
        let tape = make_tape(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&h.keyspace, &frame, 0);
        assert_static(&result, RESP_OK);

        // Verify stored.
        let key = VortexKey::from(b"foo" as &[u8]);
        let val = h.get(&key, 0).unwrap();
        assert_eq!(val.as_string_bytes().unwrap(), b"bar");
    }

    #[test]
    fn set_with_ex() {
        let h = TestHarness::new();
        // SET foo bar EX 10
        let tape =
            make_tape(b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nEX\r\n$2\r\n10\r\n");
        let frame = tape.iter().next().unwrap();
        let now = 1_000_000_000u64; // 1 sec
        let result = cmd_set(&h.keyspace, &frame, now);
        assert_static(&result, RESP_OK);

        // Should exist before expiry.
        let key = VortexKey::from(b"foo" as &[u8]);
        assert!(h.get(&key, now).is_some());

        // Should be expired after 10 sec.
        let after = now + 11 * NS_PER_SEC;
        assert!(h.get(&key, after).is_none());
    }

    #[test]
    fn set_nx_when_key_absent() {
        let h = TestHarness::new();
        // SET foo bar NX
        let tape = make_tape(b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nNX\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&h.keyspace, &frame, 0);
        assert_static(&result, RESP_OK);
    }

    #[test]
    fn set_nx_when_key_exists() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"foo" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"old"));

        let tape = make_tape(b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nnew\r\n$2\r\nNX\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&h.keyspace, &frame, 0);
        assert_static(&result, RESP_NIL);

        // Value should remain "old".
        let key = VortexKey::from(b"foo" as &[u8]);
        let val = h.get(&key, 0).unwrap();
        assert_eq!(val.as_string_bytes().unwrap(), b"old");
    }

    #[test]
    fn set_xx_when_key_exists() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"foo" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"old"));

        let tape = make_tape(b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nnew\r\n$2\r\nXX\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&h.keyspace, &frame, 0);
        assert_static(&result, RESP_OK);
    }

    #[test]
    fn set_xx_when_key_absent() {
        let h = TestHarness::new();
        let tape = make_tape(b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nXX\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&h.keyspace, &frame, 0);
        assert_static(&result, RESP_NIL);
    }

    #[test]
    fn set_get_returns_old_value() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"foo" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"old"));

        let tape = make_tape(b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nnew\r\n$3\r\nGET\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&h.keyspace, &frame, 0);
        assert_eq!(resp_bytes(&result), b"old");
    }

    // ── SETNX ──

    #[test]
    fn setnx_set_when_absent() {
        let h = TestHarness::new();
        let tape = make_tape(b"*3\r\n$5\r\nSETNX\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_setnx(&h.keyspace, &frame, 0);
        assert_static(&result, super::super::RESP_ONE);
    }

    #[test]
    fn setnx_skip_when_exists() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"foo" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"old"));

        let tape = make_tape(b"*3\r\n$5\r\nSETNX\r\n$3\r\nfoo\r\n$3\r\nnew\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_setnx(&h.keyspace, &frame, 0);
        assert_static(&result, RESP_ZERO);
    }

    // ── SETEX / PSETEX ──

    #[test]
    fn setex_sets_with_ttl() {
        let h = TestHarness::new();
        let tape = make_tape(b"*4\r\n$5\r\nSETEX\r\n$3\r\nfoo\r\n$2\r\n10\r\n$3\r\nbar\r\n");
        let frame = tape.iter().next().unwrap();
        let now = 1_000_000_000u64;
        let result = cmd_setex(&h.keyspace, &frame, now);
        assert_static(&result, RESP_OK);

        let key = VortexKey::from(b"foo" as &[u8]);
        assert!(h.get(&key, now).is_some());
        assert!(h.get(&key, now + 11 * NS_PER_SEC).is_none());
    }

    #[test]
    fn psetex_sets_with_ttl_ms() {
        let h = TestHarness::new();
        let tape = make_tape(b"*4\r\n$6\r\nPSETEX\r\n$3\r\nfoo\r\n$4\r\n5000\r\n$3\r\nbar\r\n");
        let frame = tape.iter().next().unwrap();
        let now = 1_000_000_000u64;
        let result = cmd_psetex(&h.keyspace, &frame, now);
        assert_static(&result, RESP_OK);

        let key = VortexKey::from(b"foo" as &[u8]);
        assert!(h.get(&key, now + 4 * NS_PER_SEC).is_some());
        assert!(h.get(&key, now + 6 * NS_PER_SEC).is_none());
    }

    // ── INCR / DECR ──

    #[test]
    fn incr_creates_from_zero() {
        let h = TestHarness::new();
        let tape = make_tape(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&h.keyspace, &frame, 0);
        assert_eq!(resp_int(&result), 1);
    }

    #[test]
    fn incr_existing_integer() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"counter" as &[u8]);
        h.set(key, VortexValue::Integer(10));

        let tape = make_tape(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&h.keyspace, &frame, 0);
        assert_eq!(resp_int(&result), 11);
    }

    #[test]
    fn decr_creates_from_zero() {
        let h = TestHarness::new();
        let tape = make_tape(b"*2\r\n$4\r\nDECR\r\n$7\r\ncounter\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_decr(&h.keyspace, &frame, 0);
        assert_eq!(resp_int(&result), -1);
    }

    #[test]
    fn incrby_delta() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"counter" as &[u8]);
        h.set(key, VortexValue::Integer(5));

        let tape = make_tape(b"*3\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$2\r\n10\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incrby(&h.keyspace, &frame, 0);
        assert_eq!(resp_int(&result), 15);
    }

    #[test]
    fn incr_overflow_returns_error() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"c" as &[u8]);
        h.set(key, VortexValue::Integer(i64::MAX));

        let tape = make_tape(b"*2\r\n$4\r\nINCR\r\n$1\r\nc\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&h.keyspace, &frame, 0);
        assert_static(&result, ERR_OVERFLOW);
    }

    #[test]
    fn incr_string_integer() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"c" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"100"));

        let tape = make_tape(b"*2\r\n$4\r\nINCR\r\n$1\r\nc\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&h.keyspace, &frame, 0);
        assert_eq!(resp_int(&result), 101);
    }

    #[test]
    fn incr_non_integer_string_errors() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"c" as &[u8]);
        h.set(key, VortexValue::String(Bytes::from_static(b"hello")));

        let tape = make_tape(b"*2\r\n$4\r\nINCR\r\n$1\r\nc\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&h.keyspace, &frame, 0);
        assert_static(&result, ERR_NOT_INTEGER);
    }

    // ── INCRBYFLOAT ──

    #[test]
    fn incrbyfloat_basic() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"f" as &[u8]);
        h.set(key, VortexValue::Integer(10));

        let tape = make_tape(b"*3\r\n$11\r\nINCRBYFLOAT\r\n$1\r\nf\r\n$3\r\n0.5\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incrbyfloat(&h.keyspace, &frame, 0);
        let s = std::str::from_utf8(resp_bytes(&result)).unwrap();
        let v: f64 = s.parse().unwrap();
        assert!((v - 10.5).abs() < 1e-10);
    }

    // ── MGET ──

    #[test]
    fn mget_returns_values_and_nils() {
        let h = TestHarness::new();
        h.set(
            VortexKey::from(b"a" as &[u8]),
            VortexValue::from_bytes(b"1"),
        );
        h.set(
            VortexKey::from(b"c" as &[u8]),
            VortexValue::from_bytes(b"3"),
        );

        // MGET a b c (b missing)
        let tape = make_tape(b"*4\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_mget(&h.keyspace, &frame, 0);
        match result {
            CmdResult::Resp(RespFrame::Array(Some(frames))) => {
                assert_eq!(frames.len(), 3);
                // a = "1" (stored as Integer(1))
                match &frames[0] {
                    RespFrame::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"1"),
                    other => panic!("expected bulk string, got {:?}", other),
                }
                // b = nil
                assert!(matches!(&frames[1], RespFrame::BulkString(None)));
                // c = "3"
                match &frames[2] {
                    RespFrame::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"3"),
                    other => panic!("expected bulk string, got {:?}", other),
                }
            }
            _ => panic!("expected Array, got something else"),
        }
    }

    // ── MSET ──

    #[test]
    fn mset_sets_multiple() {
        let h = TestHarness::new();
        // MSET a 1 b 2 c 3
        let tape = make_tape(
            b"*7\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n",
        );
        let frame = tape.iter().next().unwrap();
        let result = cmd_mset(&h.keyspace, &frame, 0);
        assert_static(&result, RESP_OK);

        let a = VortexKey::from(b"a" as &[u8]);
        let b_key = VortexKey::from(b"b" as &[u8]);
        let c = VortexKey::from(b"c" as &[u8]);
        assert!(h.get(&a, 0).is_some());
        assert!(h.get(&b_key, 0).is_some());
        assert!(h.get(&c, 0).is_some());
    }

    // ── GETSET ──

    #[test]
    fn getset_returns_old() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"k" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"old"));

        let tape = make_tape(b"*3\r\n$6\r\nGETSET\r\n$1\r\nk\r\n$3\r\nnew\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_getset(&h.keyspace, &frame, 0);
        assert_eq!(resp_bytes(&result), b"old");
    }

    // ── GETDEL ──

    #[test]
    fn getdel_returns_and_removes() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"k" as &[u8]);
        h.set(key.clone(), VortexValue::from_bytes(b"val"));

        let tape = make_tape(b"*2\r\n$6\r\nGETDEL\r\n$1\r\nk\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_getdel(&h.keyspace, &frame, 0);
        assert_eq!(resp_bytes(&result), b"val");

        // Should be gone.
        assert!(h.get(&key, 0).is_none());
    }

    // ── APPEND ──

    #[test]
    fn append_to_existing() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"k" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"hello"));

        let tape = make_tape(b"*3\r\n$6\r\nAPPEND\r\n$1\r\nk\r\n$6\r\n world\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_append(&h.keyspace, &frame, 0);
        assert_eq!(resp_int(&result), 11); // "hello world" = 11 bytes
    }

    #[test]
    fn append_creates_new() {
        let h = TestHarness::new();
        let tape = make_tape(b"*3\r\n$6\r\nAPPEND\r\n$1\r\nk\r\n$5\r\nhello\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_append(&h.keyspace, &frame, 0);
        assert_eq!(resp_int(&result), 5);
    }

    // ── STRLEN ──

    #[test]
    fn strlen_existing() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"k" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"hello"));

        let tape = make_tape(b"*2\r\n$6\r\nSTRLEN\r\n$1\r\nk\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_strlen(&h.keyspace, &frame, 0);
        assert_eq!(resp_int(&result), 5);
    }

    #[test]
    fn strlen_missing() {
        let h = TestHarness::new();
        let tape = make_tape(b"*2\r\n$6\r\nSTRLEN\r\n$1\r\nk\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_strlen(&h.keyspace, &frame, 0);
        assert_static(&result, RESP_ZERO);
    }

    // ── GETRANGE ──

    #[test]
    fn getrange_basic() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"k" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"Hello, World!"));

        // GETRANGE k 0 4 => "Hello"
        let tape = make_tape(b"*4\r\n$8\r\nGETRANGE\r\n$1\r\nk\r\n$1\r\n0\r\n$1\r\n4\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_getrange(&h.keyspace, &frame, 0);
        assert_eq!(resp_bytes(&result), b"Hello");
    }

    #[test]
    fn getrange_negative_index() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"k" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"Hello, World!"));

        // GETRANGE k -6 -1 => "orld!"  (wait, "World!" is 6 chars)
        let tape = make_tape(b"*4\r\n$8\r\nGETRANGE\r\n$1\r\nk\r\n$2\r\n-6\r\n$2\r\n-1\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_getrange(&h.keyspace, &frame, 0);
        assert_eq!(resp_bytes(&result), b"World!");
    }

    // ── SETRANGE ──

    #[test]
    fn setrange_basic() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"k" as &[u8]);
        h.set(key, VortexValue::from_bytes(b"Hello World"));

        // SETRANGE k 6 Redis
        let tape = make_tape(b"*4\r\n$8\r\nSETRANGE\r\n$1\r\nk\r\n$1\r\n6\r\n$5\r\nRedis\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_setrange(&h.keyspace, &frame, 0);
        assert_eq!(resp_int(&result), 11);

        let key = VortexKey::from(b"k" as &[u8]);
        let val = h.get(&key, 0).unwrap();
        assert_eq!(val.as_string_bytes().unwrap(), b"Hello Redis");
    }

    #[test]
    fn setrange_pads_with_zeros() {
        let h = TestHarness::new();
        // SETRANGE k 5 hello (key doesn't exist)
        let tape = make_tape(b"*4\r\n$8\r\nSETRANGE\r\n$1\r\nk\r\n$1\r\n5\r\n$5\r\nhello\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_setrange(&h.keyspace, &frame, 0);
        assert_eq!(resp_int(&result), 10); // 5 zeros + "hello"
    }

    // ── MSETNX ──

    #[test]
    fn msetnx_all_new() {
        let h = TestHarness::new();
        let tape = make_tape(b"*5\r\n$6\r\nMSETNX\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_msetnx(&h.keyspace, &frame, 0);
        assert_static(&result, super::super::RESP_ONE);
    }

    #[test]
    fn msetnx_one_exists() {
        let h = TestHarness::new();
        h.set(
            VortexKey::from(b"a" as &[u8]),
            VortexValue::from_bytes(b"old"),
        );

        let tape = make_tape(b"*5\r\n$6\r\nMSETNX\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_msetnx(&h.keyspace, &frame, 0);
        assert_static(&result, RESP_ZERO);

        // b should NOT be set.
        let b_key = VortexKey::from(b"b" as &[u8]);
        assert!(h.get(&b_key, 0).is_none());
    }
}
