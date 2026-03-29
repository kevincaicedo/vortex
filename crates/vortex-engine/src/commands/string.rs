//! String command handlers for VortexDB.
//!
//! All 19 Redis String commands: GET, SET, SETNX, SETEX, PSETEX, MSET, MSETNX,
//! MGET, GETSET, GETDEL, GETEX, GETRANGE, SETRANGE, APPEND, INCR, INCRBY,
//! INCRBYFLOAT, DECR, DECRBY, STRLEN.
//!
//! Each handler is a free function with signature:
//!   `fn cmd_xxx(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult`

use bytes::Bytes;
use vortex_common::value::InlineBytes;
use vortex_common::{VortexKey, VortexValue};
use vortex_proto::{FrameRef, RespFrame};

use crate::Shard;
use crate::shard::SetResult;

use super::{
    CmdResult, ERR_NOT_FLOAT, ERR_NOT_INTEGER, ERR_OVERFLOW, ERR_SYNTAX, ERR_WRONG_TYPE, NS_PER_MS,
    NS_PER_SEC, RESP_NIL, RESP_OK, RESP_ZERO, arg_bytes, arg_count, arg_i64, int_resp,
    key_from_bytes, owned_value_to_resp, parse_i64, value_from_bytes, value_to_resp,
};

// ── GET ─────────────────────────────────────────────────────────────────────

/// GET key
///
/// Returns the value of key, or nil if the key does not exist.
/// Performs lazy expiry.
#[inline]
pub fn cmd_get(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(RESP_NIL),
    };
    let key = key_from_bytes(key_bytes);
    match shard.get(&key, now_nanos) {
        Some(val) => value_to_resp(val),
        None => CmdResult::Static(RESP_NIL),
    }
}

// ── SET ─────────────────────────────────────────────────────────────────────

/// SET key value [EX seconds | PX milliseconds | EXAT unix-time-seconds |
///   PXAT unix-time-milliseconds | KEEPTTL] [NX | XX] [GET]
#[inline]
pub fn cmd_set(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
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

    match shard.set_with_options(key, value, ttl_deadline, nx, xx, get, keepttl) {
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
pub fn cmd_setnx(shard: &mut Shard, frame: &FrameRef<'_>, _now_nanos: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let val_bytes = match arg_bytes(frame, 2) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };

    let key = key_from_bytes(key_bytes);
    if shard.exists(&key, _now_nanos) {
        return CmdResult::Static(RESP_ZERO);
    }
    let value = value_from_bytes(val_bytes);
    shard.set(key, value);
    CmdResult::Static(super::RESP_ONE)
}

// ── SETEX ───────────────────────────────────────────────────────────────────

/// SETEX key seconds value — SET with EXpire.
#[inline]
pub fn cmd_setex(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
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
    shard.set_with_ttl(key, value, deadline);
    CmdResult::Static(RESP_OK)
}

// ── PSETEX ──────────────────────────────────────────────────────────────────

/// PSETEX key milliseconds value — SET with PX expire.
#[inline]
pub fn cmd_psetex(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
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
    shard.set_with_ttl(key, value, deadline);
    CmdResult::Static(RESP_OK)
}

// ── MGET ────────────────────────────────────────────────────────────────────

/// MGET key [key ...] — Returns values of all specified keys.
///
/// Uses software-prefetch pipeline: hash all keys first, prefetch target
/// groups, then execute lookups sequentially with warm L1.
pub fn cmd_mget(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 2 {
        return CmdResult::Static(ERR_SYNTAX);
    }
    let n = argc - 1;

    // Phase 1: Collect key bytes and prefetch.
    let mut key_refs: Vec<&[u8]> = Vec::with_capacity(n);
    if let Some(mut children) = frame.children() {
        children.next(); // skip command name
        for child in children {
            if let Some(bytes) = child.as_bytes() {
                let key = VortexKey::from(bytes);
                shard.prefetch(&key);
                key_refs.push(bytes);
            }
        }
    }

    // Phase 2: Execute lookups (L1 is now warm from prefetch).
    let mut frames = Vec::with_capacity(n);
    for &kb in &key_refs {
        let key = VortexKey::from(kb);
        match shard.get(&key, now_nanos) {
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
                    let s = buf.format(*n);
                    frames.push(RespFrame::bulk_string(Bytes::copy_from_slice(s.as_bytes())));
                }
                _ => {
                    frames.push(RespFrame::null_bulk_string());
                }
            },
            None => {
                frames.push(RespFrame::null_bulk_string());
            }
        }
    }

    CmdResult::Resp(RespFrame::Array(Some(frames)))
}

// ── MSET ────────────────────────────────────────────────────────────────────

/// MSET key value [key value ...] — Sets multiple key-value pairs.
pub fn cmd_mset(shard: &mut Shard, frame: &FrameRef<'_>, _now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 3 || (argc - 1) % 2 != 0 {
        return CmdResult::Static(ERR_SYNTAX);
    }

    let n = (argc - 1) / 2;

    // Phase 1: Collect and prefetch.
    let mut pairs: Vec<(&[u8], &[u8])> = Vec::with_capacity(n);
    if let Some(mut children) = frame.children() {
        children.next(); // skip command name
        while let Some(key_frame) = children.next() {
            if let Some(val_frame) = children.next() {
                if let (Some(kb), Some(vb)) = (key_frame.as_bytes(), val_frame.as_bytes()) {
                    let key = VortexKey::from(kb);
                    shard.prefetch(&key);
                    pairs.push((kb, vb));
                }
            }
        }
    }

    // Phase 2: Insert all pairs.
    for (kb, vb) in pairs {
        let key = key_from_bytes(kb);
        let value = value_from_bytes(vb);
        shard.set(key, value);
    }

    CmdResult::Static(RESP_OK)
}

// ── MSETNX ──────────────────────────────────────────────────────────────────

/// MSETNX key value [key value ...] — SET NX for multiple keys.
///
/// Atomic: either ALL keys are set, or NONE. Returns 1 if set, 0 otherwise.
pub fn cmd_msetnx(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let argc = arg_count(frame);
    if argc < 3 || (argc - 1) % 2 != 0 {
        return CmdResult::Static(ERR_SYNTAX);
    }

    // Pass 1: Check all keys don't exist.
    let mut pairs: Vec<(&[u8], &[u8])> = Vec::with_capacity((argc - 1) / 2);
    if let Some(mut children) = frame.children() {
        children.next(); // skip command name
        while let Some(key_frame) = children.next() {
            if let Some(val_frame) = children.next() {
                if let (Some(kb), Some(vb)) = (key_frame.as_bytes(), val_frame.as_bytes()) {
                    pairs.push((kb, vb));
                }
            }
        }
    }

    for &(kb, _) in &pairs {
        let key = VortexKey::from(kb);
        if shard.exists(&key, now_nanos) {
            return CmdResult::Static(RESP_ZERO);
        }
    }

    // Pass 2: All keys are new — insert.
    for (kb, vb) in pairs {
        let key = key_from_bytes(kb);
        let value = value_from_bytes(vb);
        shard.set(key, value);
    }

    CmdResult::Static(super::RESP_ONE)
}

// ── GETSET ──────────────────────────────────────────────────────────────────

/// GETSET key value — Atomically set and return old value.
///
/// Deprecated in favor of SET ... GET, but still supported.
#[inline]
pub fn cmd_getset(shard: &mut Shard, frame: &FrameRef<'_>, _now_nanos: u64) -> CmdResult {
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
    match shard.set(key, value) {
        Some(old) => owned_value_to_resp(old),
        None => CmdResult::Static(RESP_NIL),
    }
}

// ── GETDEL ──────────────────────────────────────────────────────────────────

/// GETDEL key — Get the value and delete the key.
#[inline]
pub fn cmd_getdel(shard: &mut Shard, frame: &FrameRef<'_>, _now_nanos: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let key = key_from_bytes(key_bytes);
    match shard.remove(&key) {
        Some(val) => owned_value_to_resp(val),
        None => CmdResult::Static(RESP_NIL),
    }
}

// ── GETEX ───────────────────────────────────────────────────────────────────

/// GETEX key [EX seconds | PX ms | EXAT secs | PXAT ms | PERSIST]
///
/// Get value and optionally set/remove TTL.
#[inline]
pub fn cmd_getex(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let key = key_from_bytes(key_bytes);
    let argc = arg_count(frame);

    // First, get the value.
    let val = match shard.get(&key, now_nanos) {
        Some(v) => v.clone(),
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
                shard.expire(&key, now_nanos + secs * NS_PER_SEC);
            }
            OptToken::PX => {
                let ms = match arg_i64(frame, 3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                shard.expire(&key, now_nanos + ms * NS_PER_MS);
            }
            OptToken::EXAT => {
                let secs = match arg_i64(frame, 3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                shard.expire(&key, secs * NS_PER_SEC);
            }
            OptToken::PXAT => {
                let ms = match arg_i64(frame, 3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return CmdResult::Static(ERR_NOT_INTEGER),
                };
                shard.expire(&key, ms * NS_PER_MS);
            }
            OptToken::KEEPTTL => { /* PERSIST alias in GETEX context */ }
            _ => {
                // Check for "PERSIST" keyword.
                if eq_ci(opt, b"PERSIST") {
                    shard.persist(&key);
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
fn incr_by(shard: &mut Shard, frame: &FrameRef<'_>, delta: i64, _now: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let key = key_from_bytes(key_bytes);

    match shard.get_mut(&key) {
        Some(val) => {
            let current = match val {
                VortexValue::Integer(n) => *n,
                VortexValue::InlineString(ib) => match parse_i64(ib.as_bytes()) {
                    Some(n) => n,
                    None => return CmdResult::Static(ERR_NOT_INTEGER),
                },
                VortexValue::String(b) => match parse_i64(b.as_ref()) {
                    Some(n) => n,
                    None => return CmdResult::Static(ERR_NOT_INTEGER),
                },
                _ => return CmdResult::Static(ERR_WRONG_TYPE),
            };
            match current.checked_add(delta) {
                Some(result) => {
                    *val = VortexValue::Integer(result);
                    int_resp(result)
                }
                None => CmdResult::Static(ERR_OVERFLOW),
            }
        }
        None => {
            // Key doesn't exist — treat as 0.
            let result = delta; // 0 + delta
            shard.set(key, VortexValue::Integer(result));
            int_resp(result)
        }
    }
}

/// INCR key — Increment by 1.
#[inline]
pub fn cmd_incr(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    incr_by(shard, frame, 1, now_nanos)
}

/// DECR key — Decrement by 1.
#[inline]
pub fn cmd_decr(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    incr_by(shard, frame, -1, now_nanos)
}

/// INCRBY key increment.
#[inline]
pub fn cmd_incrby(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let delta = match arg_i64(frame, 2) {
        Some(d) => d,
        None => return CmdResult::Static(ERR_NOT_INTEGER),
    };
    incr_by(shard, frame, delta, now_nanos)
}

/// DECRBY key decrement.
#[inline]
pub fn cmd_decrby(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let delta = match arg_i64(frame, 2) {
        Some(d) => d,
        None => return CmdResult::Static(ERR_NOT_INTEGER),
    };
    incr_by(shard, frame, -delta, now_nanos)
}

// ── INCRBYFLOAT ─────────────────────────────────────────────────────────────

/// INCRBYFLOAT key increment.
///
/// Result is stored as a string (Redis behavior).
pub fn cmd_incrbyfloat(shard: &mut Shard, frame: &FrameRef<'_>, _now_nanos: u64) -> CmdResult {
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

    let current: f64 = match shard.get_mut(&key) {
        Some(val) => match val {
            VortexValue::Integer(n) => *n as f64,
            VortexValue::InlineString(ib) => {
                match std::str::from_utf8(ib.as_bytes())
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return CmdResult::Static(ERR_NOT_FLOAT),
                }
            }
            VortexValue::String(b) => {
                match std::str::from_utf8(b.as_ref())
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(f) => f,
                    None => return CmdResult::Static(ERR_NOT_FLOAT),
                }
            }
            _ => return CmdResult::Static(ERR_WRONG_TYPE),
        },
        None => 0.0,
    };

    let result = current + incr;
    if result.is_nan() || result.is_infinite() {
        return CmdResult::Static(ERR_NOT_FLOAT);
    }

    // Serialize with ryu for fast f64→string.
    let mut buf = ryu::Buffer::new();
    let s = buf.format(result);
    let new_val = VortexValue::from_bytes(s.as_bytes());
    shard.set(key, new_val);

    CmdResult::Resp(RespFrame::bulk_string(Bytes::copy_from_slice(s.as_bytes())))
}

// ── APPEND ──────────────────────────────────────────────────────────────────

/// APPEND key value — Appends to existing string or creates new one.
///
/// Returns the length of the string after the append operation.
pub fn cmd_append(shard: &mut Shard, frame: &FrameRef<'_>, _now_nanos: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let append_bytes = match arg_bytes(frame, 2) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };

    let key = key_from_bytes(key_bytes);

    match shard.get_mut(&key) {
        Some(existing) => {
            let new_val = match existing {
                VortexValue::InlineString(ib) => {
                    let old = ib.as_bytes();
                    let new_len = old.len() + append_bytes.len();
                    if new_len <= 23 {
                        // Stays inline — extend in-place.
                        let mut data = [0u8; 23];
                        data[..old.len()].copy_from_slice(old);
                        data[old.len()..new_len].copy_from_slice(append_bytes);
                        VortexValue::InlineString(InlineBytes::from_slice(&data[..new_len]))
                    } else {
                        // Promote to heap.
                        let mut combined = Vec::with_capacity(new_len);
                        combined.extend_from_slice(old);
                        combined.extend_from_slice(append_bytes);
                        VortexValue::String(Bytes::from(combined))
                    }
                }
                VortexValue::String(b) => {
                    let mut combined = Vec::with_capacity(b.len() + append_bytes.len());
                    combined.extend_from_slice(b.as_ref());
                    combined.extend_from_slice(append_bytes);
                    let new_len = combined.len();
                    if new_len <= 23 {
                        VortexValue::InlineString(InlineBytes::from_slice(&combined))
                    } else {
                        VortexValue::String(Bytes::from(combined))
                    }
                }
                VortexValue::Integer(n) => {
                    let mut buf = itoa::Buffer::new();
                    let s = buf.format(*n);
                    let mut combined = Vec::with_capacity(s.len() + append_bytes.len());
                    combined.extend_from_slice(s.as_bytes());
                    combined.extend_from_slice(append_bytes);
                    VortexValue::from_bytes(&combined)
                }
                _ => return CmdResult::Static(ERR_WRONG_TYPE),
            };
            let len = match &new_val {
                VortexValue::InlineString(ib) => ib.len(),
                VortexValue::String(b) => b.len(),
                _ => 0,
            };
            *existing = new_val;
            int_resp(len as i64)
        }
        None => {
            // Key doesn't exist — create new.
            let len = append_bytes.len();
            let value = VortexValue::from_bytes(append_bytes);
            shard.set(key, value);
            int_resp(len as i64)
        }
    }
}

// ── STRLEN ──────────────────────────────────────────────────────────────────

/// STRLEN key — Returns the length of the string value.
#[inline]
pub fn cmd_strlen(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let key_bytes = match arg_bytes(frame, 1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let key = key_from_bytes(key_bytes);
    match shard.get(&key, now_nanos) {
        Some(val) => {
            if !val.is_string() {
                return CmdResult::Static(ERR_WRONG_TYPE);
            }
            int_resp(val.strlen() as i64)
        }
        None => CmdResult::Static(RESP_ZERO),
    }
}

// ── GETRANGE ────────────────────────────────────────────────────────────────

/// GETRANGE key start end — Returns a substring of the string.
///
/// Supports negative indices (from the end).
pub fn cmd_getrange(shard: &mut Shard, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
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
    let val_bytes = match shard.get(&key, now_nanos) {
        Some(val) => match val {
            VortexValue::InlineString(ib) => ib.as_bytes().to_vec(),
            VortexValue::String(b) => b.to_vec(),
            VortexValue::Integer(n) => {
                let mut buf = itoa::Buffer::new();
                buf.format(*n).as_bytes().to_vec()
            }
            _ => return CmdResult::Static(ERR_WRONG_TYPE),
        },
        None => return CmdResult::Static(super::RESP_EMPTY_BULK),
    };

    let len = val_bytes.len() as i64;
    // Normalize indices (Redis semantics).
    let s = if start < 0 {
        (len + start).max(0) as usize
    } else {
        start.min(len) as usize
    };
    let e = if end < 0 {
        (len + end).max(0) as usize
    } else {
        end.min(len - 1).max(0) as usize
    };

    if s > e || s >= val_bytes.len() {
        return CmdResult::Static(super::RESP_EMPTY_BULK);
    }

    let slice = &val_bytes[s..=e.min(val_bytes.len() - 1)];
    CmdResult::Resp(RespFrame::bulk_string(Bytes::copy_from_slice(slice)))
}

// ── SETRANGE ────────────────────────────────────────────────────────────────

/// SETRANGE key offset value — Overwrites part of the string.
///
/// If offset is beyond current length, zero-pads the string.
pub fn cmd_setrange(shard: &mut Shard, frame: &FrameRef<'_>, _now_nanos: u64) -> CmdResult {
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

    // Get existing data or empty.
    let mut data: Vec<u8> = match shard.get_mut(&key) {
        Some(val) => match val {
            VortexValue::InlineString(ib) => ib.as_bytes().to_vec(),
            VortexValue::String(b) => b.to_vec(),
            VortexValue::Integer(n) => {
                let mut buf = itoa::Buffer::new();
                buf.format(*n).as_bytes().to_vec()
            }
            _ => return CmdResult::Static(ERR_WRONG_TYPE),
        },
        None => Vec::new(),
    };

    // Extend with zeros if needed.
    let required = offset + new_bytes.len();
    if data.len() < required {
        data.resize(required, 0);
    }
    data[offset..offset + new_bytes.len()].copy_from_slice(new_bytes);

    let len = data.len();
    let new_val = VortexValue::from_bytes(&data);
    shard.set(key, new_val);
    int_resp(len as i64)
}

// ── Option parsing helpers ──────────────────────────────────────────────────

/// SET option tokens.
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
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(&x, &y)| x | 0x20 == y | 0x20)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Shard;
    use vortex_common::ShardId;
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
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"foo" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"bar"));
        let tape = make_tape(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_get(&mut shard, &frame, 0);
        assert_eq!(resp_bytes(&result), b"bar");
    }

    #[test]
    fn get_missing_key() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_get(&mut shard, &frame, 0);
        assert_static(&result, RESP_NIL);
    }

    #[test]
    fn get_integer_value() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"num" as &[u8]);
        shard.set(key, VortexValue::Integer(42));
        let tape = make_tape(b"*2\r\n$3\r\nGET\r\n$3\r\nnum\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_get(&mut shard, &frame, 0);
        assert_eq!(resp_bytes(&result), b"42");
    }

    // ── SET ──

    #[test]
    fn set_basic() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&mut shard, &frame, 0);
        assert_static(&result, RESP_OK);

        // Verify stored.
        let key = VortexKey::from(b"foo" as &[u8]);
        let val = shard.get(&key, 0).unwrap();
        assert_eq!(val.as_string_bytes().unwrap(), b"bar");
    }

    #[test]
    fn set_with_ex() {
        let mut shard = Shard::new(ShardId::new(0));
        // SET foo bar EX 10
        let tape =
            make_tape(b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nEX\r\n$2\r\n10\r\n");
        let frame = tape.iter().next().unwrap();
        let now = 1_000_000_000u64; // 1 sec
        let result = cmd_set(&mut shard, &frame, now);
        assert_static(&result, RESP_OK);

        // Should exist before expiry.
        let key = VortexKey::from(b"foo" as &[u8]);
        assert!(shard.get(&key, now).is_some());

        // Should be expired after 10 sec.
        let after = now + 11 * NS_PER_SEC;
        assert!(shard.get(&key, after).is_none());
    }

    #[test]
    fn set_nx_when_key_absent() {
        let mut shard = Shard::new(ShardId::new(0));
        // SET foo bar NX
        let tape = make_tape(b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nNX\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&mut shard, &frame, 0);
        assert_static(&result, RESP_OK);
    }

    #[test]
    fn set_nx_when_key_exists() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"foo" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"old"));

        let tape = make_tape(b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nnew\r\n$2\r\nNX\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&mut shard, &frame, 0);
        assert_static(&result, RESP_NIL);

        // Value should remain "old".
        let key = VortexKey::from(b"foo" as &[u8]);
        let val = shard.get(&key, 0).unwrap();
        assert_eq!(val.as_string_bytes().unwrap(), b"old");
    }

    #[test]
    fn set_xx_when_key_exists() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"foo" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"old"));

        let tape = make_tape(b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nnew\r\n$2\r\nXX\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&mut shard, &frame, 0);
        assert_static(&result, RESP_OK);
    }

    #[test]
    fn set_xx_when_key_absent() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nXX\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&mut shard, &frame, 0);
        assert_static(&result, RESP_NIL);
    }

    #[test]
    fn set_get_returns_old_value() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"foo" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"old"));

        let tape = make_tape(b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nnew\r\n$3\r\nGET\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&mut shard, &frame, 0);
        assert_eq!(resp_bytes(&result), b"old");
    }

    // ── SETNX ──

    #[test]
    fn setnx_set_when_absent() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*3\r\n$5\r\nSETNX\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_setnx(&mut shard, &frame, 0);
        assert_static(&result, super::super::RESP_ONE);
    }

    #[test]
    fn setnx_skip_when_exists() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"foo" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"old"));

        let tape = make_tape(b"*3\r\n$5\r\nSETNX\r\n$3\r\nfoo\r\n$3\r\nnew\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_setnx(&mut shard, &frame, 0);
        assert_static(&result, RESP_ZERO);
    }

    // ── SETEX / PSETEX ──

    #[test]
    fn setex_sets_with_ttl() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*4\r\n$5\r\nSETEX\r\n$3\r\nfoo\r\n$2\r\n10\r\n$3\r\nbar\r\n");
        let frame = tape.iter().next().unwrap();
        let now = 1_000_000_000u64;
        let result = cmd_setex(&mut shard, &frame, now);
        assert_static(&result, RESP_OK);

        let key = VortexKey::from(b"foo" as &[u8]);
        assert!(shard.get(&key, now).is_some());
        assert!(shard.get(&key, now + 11 * NS_PER_SEC).is_none());
    }

    #[test]
    fn psetex_sets_with_ttl_ms() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*4\r\n$6\r\nPSETEX\r\n$3\r\nfoo\r\n$4\r\n5000\r\n$3\r\nbar\r\n");
        let frame = tape.iter().next().unwrap();
        let now = 1_000_000_000u64;
        let result = cmd_psetex(&mut shard, &frame, now);
        assert_static(&result, RESP_OK);

        let key = VortexKey::from(b"foo" as &[u8]);
        assert!(shard.get(&key, now + 4 * NS_PER_SEC).is_some());
        assert!(shard.get(&key, now + 6 * NS_PER_SEC).is_none());
    }

    // ── INCR / DECR ──

    #[test]
    fn incr_creates_from_zero() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&mut shard, &frame, 0);
        assert_eq!(resp_int(&result), 1);
    }

    #[test]
    fn incr_existing_integer() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"counter" as &[u8]);
        shard.set(key, VortexValue::Integer(10));

        let tape = make_tape(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&mut shard, &frame, 0);
        assert_eq!(resp_int(&result), 11);
    }

    #[test]
    fn decr_creates_from_zero() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*2\r\n$4\r\nDECR\r\n$7\r\ncounter\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_decr(&mut shard, &frame, 0);
        assert_eq!(resp_int(&result), -1);
    }

    #[test]
    fn incrby_delta() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"counter" as &[u8]);
        shard.set(key, VortexValue::Integer(5));

        let tape = make_tape(b"*3\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$2\r\n10\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incrby(&mut shard, &frame, 0);
        assert_eq!(resp_int(&result), 15);
    }

    #[test]
    fn incr_overflow_returns_error() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"c" as &[u8]);
        shard.set(key, VortexValue::Integer(i64::MAX));

        let tape = make_tape(b"*2\r\n$4\r\nINCR\r\n$1\r\nc\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&mut shard, &frame, 0);
        assert_static(&result, ERR_OVERFLOW);
    }

    #[test]
    fn incr_string_integer() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"c" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"100"));

        let tape = make_tape(b"*2\r\n$4\r\nINCR\r\n$1\r\nc\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&mut shard, &frame, 0);
        assert_eq!(resp_int(&result), 101);
    }

    #[test]
    fn incr_non_integer_string_errors() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"c" as &[u8]);
        shard.set(key, VortexValue::String(Bytes::from_static(b"hello")));

        let tape = make_tape(b"*2\r\n$4\r\nINCR\r\n$1\r\nc\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&mut shard, &frame, 0);
        assert_static(&result, ERR_NOT_INTEGER);
    }

    // ── INCRBYFLOAT ──

    #[test]
    fn incrbyfloat_basic() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"f" as &[u8]);
        shard.set(key, VortexValue::Integer(10));

        let tape = make_tape(b"*3\r\n$11\r\nINCRBYFLOAT\r\n$1\r\nf\r\n$3\r\n0.5\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_incrbyfloat(&mut shard, &frame, 0);
        let s = std::str::from_utf8(resp_bytes(&result)).unwrap();
        let v: f64 = s.parse().unwrap();
        assert!((v - 10.5).abs() < 1e-10);
    }

    // ── MGET ──

    #[test]
    fn mget_returns_values_and_nils() {
        let mut shard = Shard::new(ShardId::new(0));
        shard.set(
            VortexKey::from(b"a" as &[u8]),
            VortexValue::from_bytes(b"1"),
        );
        shard.set(
            VortexKey::from(b"c" as &[u8]),
            VortexValue::from_bytes(b"3"),
        );

        // MGET a b c (b missing)
        let tape = make_tape(b"*4\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_mget(&mut shard, &frame, 0);
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
        let mut shard = Shard::new(ShardId::new(0));
        // MSET a 1 b 2 c 3
        let tape = make_tape(
            b"*7\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n",
        );
        let frame = tape.iter().next().unwrap();
        let result = cmd_mset(&mut shard, &frame, 0);
        assert_static(&result, RESP_OK);

        let a = VortexKey::from(b"a" as &[u8]);
        let b_key = VortexKey::from(b"b" as &[u8]);
        let c = VortexKey::from(b"c" as &[u8]);
        assert!(shard.get(&a, 0).is_some());
        assert!(shard.get(&b_key, 0).is_some());
        assert!(shard.get(&c, 0).is_some());
    }

    // ── GETSET ──

    #[test]
    fn getset_returns_old() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"k" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"old"));

        let tape = make_tape(b"*3\r\n$6\r\nGETSET\r\n$1\r\nk\r\n$3\r\nnew\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_getset(&mut shard, &frame, 0);
        assert_eq!(resp_bytes(&result), b"old");
    }

    // ── GETDEL ──

    #[test]
    fn getdel_returns_and_removes() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"k" as &[u8]);
        shard.set(key.clone(), VortexValue::from_bytes(b"val"));

        let tape = make_tape(b"*2\r\n$6\r\nGETDEL\r\n$1\r\nk\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_getdel(&mut shard, &frame, 0);
        assert_eq!(resp_bytes(&result), b"val");

        // Should be gone.
        assert!(shard.get(&key, 0).is_none());
    }

    // ── APPEND ──

    #[test]
    fn append_to_existing() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"k" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"hello"));

        let tape = make_tape(b"*3\r\n$6\r\nAPPEND\r\n$1\r\nk\r\n$6\r\n world\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_append(&mut shard, &frame, 0);
        assert_eq!(resp_int(&result), 11); // "hello world" = 11 bytes
    }

    #[test]
    fn append_creates_new() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*3\r\n$6\r\nAPPEND\r\n$1\r\nk\r\n$5\r\nhello\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_append(&mut shard, &frame, 0);
        assert_eq!(resp_int(&result), 5);
    }

    // ── STRLEN ──

    #[test]
    fn strlen_existing() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"k" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"hello"));

        let tape = make_tape(b"*2\r\n$6\r\nSTRLEN\r\n$1\r\nk\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_strlen(&mut shard, &frame, 0);
        assert_eq!(resp_int(&result), 5);
    }

    #[test]
    fn strlen_missing() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*2\r\n$6\r\nSTRLEN\r\n$1\r\nk\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_strlen(&mut shard, &frame, 0);
        assert_static(&result, RESP_ZERO);
    }

    // ── GETRANGE ──

    #[test]
    fn getrange_basic() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"k" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"Hello, World!"));

        // GETRANGE k 0 4 => "Hello"
        let tape = make_tape(b"*4\r\n$8\r\nGETRANGE\r\n$1\r\nk\r\n$1\r\n0\r\n$1\r\n4\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_getrange(&mut shard, &frame, 0);
        assert_eq!(resp_bytes(&result), b"Hello");
    }

    #[test]
    fn getrange_negative_index() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"k" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"Hello, World!"));

        // GETRANGE k -6 -1 => "orld!"  (wait, "World!" is 6 chars)
        let tape = make_tape(b"*4\r\n$8\r\nGETRANGE\r\n$1\r\nk\r\n$2\r\n-6\r\n$2\r\n-1\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_getrange(&mut shard, &frame, 0);
        assert_eq!(resp_bytes(&result), b"World!");
    }

    // ── SETRANGE ──

    #[test]
    fn setrange_basic() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"k" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"Hello World"));

        // SETRANGE k 6 Redis
        let tape = make_tape(b"*4\r\n$8\r\nSETRANGE\r\n$1\r\nk\r\n$1\r\n6\r\n$5\r\nRedis\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_setrange(&mut shard, &frame, 0);
        assert_eq!(resp_int(&result), 11);

        let key = VortexKey::from(b"k" as &[u8]);
        let val = shard.get(&key, 0).unwrap();
        assert_eq!(val.as_string_bytes().unwrap(), b"Hello Redis");
    }

    #[test]
    fn setrange_pads_with_zeros() {
        let mut shard = Shard::new(ShardId::new(0));
        // SETRANGE k 5 hello (key doesn't exist)
        let tape = make_tape(b"*4\r\n$8\r\nSETRANGE\r\n$1\r\nk\r\n$1\r\n5\r\n$5\r\nhello\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_setrange(&mut shard, &frame, 0);
        assert_eq!(resp_int(&result), 10); // 5 zeros + "hello"
    }

    // ── MSETNX ──

    #[test]
    fn msetnx_all_new() {
        let mut shard = Shard::new(ShardId::new(0));
        let tape = make_tape(b"*5\r\n$6\r\nMSETNX\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_msetnx(&mut shard, &frame, 0);
        assert_static(&result, super::super::RESP_ONE);
    }

    #[test]
    fn msetnx_one_exists() {
        let mut shard = Shard::new(ShardId::new(0));
        shard.set(
            VortexKey::from(b"a" as &[u8]),
            VortexValue::from_bytes(b"old"),
        );

        let tape = make_tape(b"*5\r\n$6\r\nMSETNX\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_msetnx(&mut shard, &frame, 0);
        assert_static(&result, RESP_ZERO);

        // b should NOT be set.
        let b_key = VortexKey::from(b"b" as &[u8]);
        assert!(shard.get(&b_key, 0).is_none());
    }
}
