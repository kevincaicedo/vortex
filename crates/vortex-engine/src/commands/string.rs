//! String command handlers for VortexDB.
//!
//! All 19 Redis String commands: GET, SET, SETNX, SETEX, PSETEX, MSET, MSETNX,
//! MGET, GETSET, GETDEL, GETEX, GETRANGE, SETRANGE, APPEND, INCR, INCRBY,
//! INCRBYFLOAT, DECR, DECRBY, STRLEN.

use smallvec::SmallVec;
use vortex_proto::{FrameRef, RespFrame};

#[cfg(test)]
use bytes::Bytes;
#[cfg(test)]
use vortex_common::VortexKey;
#[cfg(test)]
use vortex_common::VortexValue;

use super::{
    CmdResult, CommandArgs, ERR_NOT_FLOAT, ERR_NOT_INTEGER, ERR_SYNTAX, ExecutedCommand, NS_PER_MS,
    NS_PER_SEC, RESP_NIL, RESP_OK, RESP_ZERO, absolute_unix_nanos_to_deadline_nanos, arg_bytes,
    deadline_nanos_to_absolute_unix_nanos, encode_aof_persist, encode_aof_pexpireat,
    encode_aof_set_pxat, int_resp, key_from_bytes, owned_value_to_resp, value_from_bytes,
    value_to_resp,
};
use crate::ConcurrentKeyspace;
use crate::commands::context::{MutationOutcome, SetOptions, SetResult};

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
    let shard_index = keyspace.shard_index(key_bytes);
    // Pre-hash for the table BEFORE acquiring the read lock.
    let table_hash = keyspace.table_hash_key(key_bytes);
    let guard = keyspace.read_shard_by_index(shard_index);
    match guard.get_with_ttl_prehashed(key_bytes, table_hash) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
            keyspace.record_access_prehashed(&guard, key_bytes, table_hash);
            // Hot path: format RESP from &VortexValue while read lock is held.
            value_to_resp(value)
        }
        Some(_) => {
            // Expired — double-checked locking cleanup.
            drop(guard);
            let key = key_from_bytes(key_bytes);
            let mut wguard = keyspace.write_shard_by_index(shard_index);
            let had_ttl = matches!(wguard.get_entry_ttl(&key), Some(ttl) if ttl != 0);
            if super::context::remove_if_expired(&mut wguard, &key, now_nanos) {
                keyspace.update_expiry_count(shard_index, had_ttl, false);
                keyspace.bump_watch_key(&key);
            }
            CmdResult::Static(RESP_NIL)
        }
        None => CmdResult::Static(RESP_NIL),
    }
}

// ── SET ─────────────────────────────────────────────────────────────────────

/// SET key value [EX seconds | PX milliseconds | EXAT unix-time-seconds |
///   PXAT unix-time-milliseconds | KEEPTTL] [NX | XX] [GET]
#[inline]
#[allow(dead_code)]
pub fn cmd_set(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    cmd_set_with_clock(keyspace, frame, now_nanos, now_nanos)
}

#[inline]
pub(crate) fn cmd_set_with_clock(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
    unix_now_nanos: u64,
) -> ExecutedCommand {
    // Fast path: plain `SET key value` (argc == 3, no options).
    // Avoids SmallVec allocation in CommandArgs::collect and skips
    // option parsing entirely.
    let argc = match frame.element_count() {
        Some(n) => n as usize,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    if argc < 3 {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    }
    if argc == 3 {
        let key_bytes = match arg_bytes(frame, 1) {
            Some(b) => b,
            None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
        };
        let val_bytes = match arg_bytes(frame, 2) {
            Some(b) => b,
            None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
        };
        let outcome = match keyspace.set_value_plain_bytes(key_bytes, val_bytes, now_nanos) {
            Ok(outcome) => outcome,
            Err(err) => return err.into_executed(),
        };
        return ExecutedCommand::with_aof_records(
            CmdResult::Static(RESP_OK),
            outcome.aof_records,
            outcome.aof_lsn,
        );
    }

    // Slow path: SET with options (EX, PX, NX, XX, GET, KEEPTTL, etc.).
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };

    let key_bytes = match args.get(1) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let val_bytes = match args.get(2) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };

    let key = key_from_bytes(key_bytes);
    let value = value_from_bytes(val_bytes);

    // Parse options.
    let mut ttl_deadline: u64 = 0;
    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut keepttl = false;
    let mut has_explicit_ttl = false;

    let mut i = 3;
    while i < argc {
        let opt = match args.get(i) {
            Some(b) => b,
            None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
        };
        match opt_upper(opt) {
            OptToken::EX => {
                i += 1;
                let secs = match args.i64(i) {
                    Some(s) if s > 0 => s as u64,
                    _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
                };
                ttl_deadline = now_nanos + secs * NS_PER_SEC;
                has_explicit_ttl = true;
            }
            OptToken::PX => {
                i += 1;
                let ms = match args.i64(i) {
                    Some(s) if s > 0 => s as u64,
                    _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
                };
                ttl_deadline = now_nanos + ms * NS_PER_MS;
                has_explicit_ttl = true;
            }
            OptToken::EXAT => {
                i += 1;
                let secs = match args.i64(i) {
                    Some(s) if s > 0 => s as u64,
                    _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
                };
                ttl_deadline = absolute_unix_nanos_to_deadline_nanos(
                    secs * NS_PER_SEC,
                    now_nanos,
                    unix_now_nanos,
                );
                has_explicit_ttl = true;
            }
            OptToken::PXAT => {
                i += 1;
                let ms = match args.i64(i) {
                    Some(s) if s > 0 => s as u64,
                    _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
                };
                ttl_deadline = absolute_unix_nanos_to_deadline_nanos(
                    ms * NS_PER_MS,
                    now_nanos,
                    unix_now_nanos,
                );
                has_explicit_ttl = true;
            }
            OptToken::NX => nx = true,
            OptToken::XX => xx = true,
            OptToken::GET => get = true,
            OptToken::KEEPTTL => keepttl = true,
            OptToken::Unknown => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
        }
        i += 1;
    }

    // NX and XX are mutually exclusive.
    if nx && xx {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    }

    let options = SetOptions {
        ttl_deadline,
        nx,
        xx,
        get,
        keepttl,
    };

    let outcome = match keyspace.set_value_with_options(key, value, options, now_nanos) {
        Ok(outcome) => outcome,
        Err(err) => return err.into_executed(),
    };
    let response = match outcome.value {
        SetResult::Ok => CmdResult::Static(RESP_OK),
        SetResult::NotSet => CmdResult::Static(RESP_NIL),
        SetResult::OkGet(Some(old)) => owned_value_to_resp(old),
        SetResult::OkGet(None) => CmdResult::Static(RESP_NIL),
        SetResult::NotSetGet(Some(old)) => owned_value_to_resp(old),
        SetResult::NotSetGet(None) => CmdResult::Static(RESP_NIL),
    };
    let aof_payload =
        if outcome.aof_lsn.is_some() && has_explicit_ttl && !keepttl && unix_now_nanos != 0 {
            let absolute_deadline_ms =
                deadline_nanos_to_absolute_unix_nanos(ttl_deadline, now_nanos, unix_now_nanos)
                    / NS_PER_MS;
            Some(encode_aof_set_pxat(
                key_bytes,
                val_bytes,
                absolute_deadline_ms,
            ))
        } else {
            None
        };
    ExecutedCommand::with_optional_aof_payload_and_records(
        response,
        outcome.aof_records,
        outcome.aof_lsn,
        aof_payload,
    )
}

// ── SETNX ───────────────────────────────────────────────────────────────────

/// SETNX key value — SET if Not eXists.
///
/// Returns 1 if set, 0 if key already exists.
#[inline]
pub fn cmd_setnx(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let key_bytes = match args.get(1) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let val_bytes = match args.get(2) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };

    let key = key_from_bytes(key_bytes);
    let value = value_from_bytes(val_bytes);
    let outcome = match keyspace.set_value_with_options(
        key,
        value,
        SetOptions {
            nx: true,
            ..SetOptions::default()
        },
        now_nanos,
    ) {
        Ok(outcome) => outcome,
        Err(err) => return err.into_executed(),
    };
    let response = match outcome.value {
        SetResult::Ok => CmdResult::Static(super::RESP_ONE),
        SetResult::NotSet => CmdResult::Static(RESP_ZERO),
        _ => CmdResult::Static(RESP_ZERO),
    };
    ExecutedCommand::with_aof_records(response, outcome.aof_records, outcome.aof_lsn)
}

// ── SETEX ───────────────────────────────────────────────────────────────────

/// SETEX key seconds value — SET with EXpire.
#[inline]
#[cfg_attr(not(test), allow(dead_code))]
pub fn cmd_setex(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    cmd_setex_with_clock(keyspace, frame, now_nanos, 0)
}

#[inline]
pub(crate) fn cmd_setex_with_clock(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
    unix_now_nanos: u64,
) -> ExecutedCommand {
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let key_bytes = match args.get(1) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let secs = match args.i64(2) {
        Some(s) if s > 0 => s as u64,
        _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
    };
    let val_bytes = match args.get(3) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };

    let key = key_from_bytes(key_bytes);
    let value = value_from_bytes(val_bytes);
    let deadline = now_nanos + secs * NS_PER_SEC;
    let outcome = match keyspace.set_value_with_ttl(key, value, deadline, now_nanos) {
        Ok(outcome) => outcome,
        Err(err) => return err.into_executed(),
    };
    let aof_payload = if outcome.aof_lsn.is_some() && unix_now_nanos != 0 {
        let absolute_deadline_ms =
            deadline_nanos_to_absolute_unix_nanos(deadline, now_nanos, unix_now_nanos) / NS_PER_MS;
        Some(encode_aof_set_pxat(
            key_bytes,
            val_bytes,
            absolute_deadline_ms,
        ))
    } else {
        None
    };
    ExecutedCommand::with_optional_aof_payload_and_records(
        CmdResult::Static(RESP_OK),
        outcome.aof_records,
        outcome.aof_lsn,
        aof_payload,
    )
}

// ── PSETEX ──────────────────────────────────────────────────────────────────

/// PSETEX key milliseconds value — SET with PX expire.
#[inline]
#[cfg_attr(not(test), allow(dead_code))]
pub fn cmd_psetex(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    cmd_psetex_with_clock(keyspace, frame, now_nanos, 0)
}

#[inline]
pub(crate) fn cmd_psetex_with_clock(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
    unix_now_nanos: u64,
) -> ExecutedCommand {
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let key_bytes = match args.get(1) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let ms = match args.i64(2) {
        Some(s) if s > 0 => s as u64,
        _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
    };
    let val_bytes = match args.get(3) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };

    let key = key_from_bytes(key_bytes);
    let value = value_from_bytes(val_bytes);
    let deadline = now_nanos + ms * NS_PER_MS;
    let outcome = match keyspace.set_value_with_ttl(key, value, deadline, now_nanos) {
        Ok(outcome) => outcome,
        Err(err) => return err.into_executed(),
    };
    let aof_payload = if outcome.aof_lsn.is_some() && unix_now_nanos != 0 {
        let absolute_deadline_ms =
            deadline_nanos_to_absolute_unix_nanos(deadline, now_nanos, unix_now_nanos) / NS_PER_MS;
        Some(encode_aof_set_pxat(
            key_bytes,
            val_bytes,
            absolute_deadline_ms,
        ))
    } else {
        None
    };
    ExecutedCommand::with_optional_aof_payload_and_records(
        CmdResult::Static(RESP_OK),
        outcome.aof_records,
        outcome.aof_lsn,
        aof_payload,
    )
}

// ── MGET ────────────────────────────────────────────────────────────────────

/// MGET key [key ...] — Returns values of all specified keys.
pub fn cmd_mget(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let Some(args) = CommandArgs::collect(frame) else {
        return CmdResult::Static(ERR_SYNTAX);
    };
    let argc = args.len();
    if argc < 2 {
        return CmdResult::Static(ERR_SYNTAX);
    }
    let mut keys: SmallVec<[&[u8]; 16]> = SmallVec::with_capacity(argc - 1);
    keys.extend(args.iter_from(1));

    CmdResult::Resp(RespFrame::Array(Some(
        keyspace.mget_frames(&keys, now_nanos),
    )))
}

// ── MSET ────────────────────────────────────────────────────────────────────

/// MSET key value [key value ...] — Sets multiple key-value pairs.
pub fn cmd_mset(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> ExecutedCommand {
    let argc = match frame.element_count() {
        Some(n) => n as usize,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    if argc < 3 || (argc - 1) % 2 != 0 {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    }

    let Some(mut children) = frame.children() else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let _ = children.next();
    let mut pairs = Vec::with_capacity((argc - 1) / 2);
    while let (Some(key_arg), Some(value_arg)) = (children.next(), children.next()) {
        let Some(key_bytes) = key_arg.as_bytes() else {
            return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
        };
        let Some(value_bytes) = value_arg.as_bytes() else {
            return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
        };
        pairs.push((key_from_bytes(key_bytes), value_from_bytes(value_bytes)));
    }

    let outcome = match keyspace.mset_values(pairs, _now_nanos) {
        Ok(outcome) => outcome,
        Err(err) => return err.into_executed(),
    };
    ExecutedCommand::with_aof_records(
        CmdResult::Static(RESP_OK),
        outcome.aof_records,
        outcome.aof_lsn,
    )
}

// ── MSETNX ──────────────────────────────────────────────────────────────────

/// MSETNX key value [key value ...] — SET NX for multiple keys.
///
/// Atomic: either ALL keys are set, or NONE. Returns 1 if set, 0 otherwise.
pub fn cmd_msetnx(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    let argc = match frame.element_count() {
        Some(n) => n as usize,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    if argc < 3 || (argc - 1) % 2 != 0 {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    }

    let Some(mut children) = frame.children() else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let _ = children.next();
    let mut pairs = Vec::with_capacity((argc - 1) / 2);
    while let (Some(key_arg), Some(value_arg)) = (children.next(), children.next()) {
        let Some(key_bytes) = key_arg.as_bytes() else {
            return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
        };
        let Some(value_bytes) = value_arg.as_bytes() else {
            return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
        };
        pairs.push((key_from_bytes(key_bytes), value_from_bytes(value_bytes)));
    }

    let outcome = match keyspace.msetnx_values(pairs, now_nanos) {
        Ok(outcome) => outcome,
        Err(err) => return err.into_executed(),
    };
    let response = if outcome.value {
        CmdResult::Static(super::RESP_ONE)
    } else {
        CmdResult::Static(RESP_ZERO)
    };
    ExecutedCommand::with_aof_records(response, outcome.aof_records, outcome.aof_lsn)
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
) -> ExecutedCommand {
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let key_bytes = match args.get(1) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let val_bytes = match args.get(2) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };

    let key = key_from_bytes(key_bytes);
    let value = value_from_bytes(val_bytes);
    let outcome = match keyspace.set_value_with_options(
        key,
        value,
        SetOptions {
            get: true,
            ..SetOptions::default()
        },
        now_nanos,
    ) {
        Ok(outcome) => outcome,
        Err(err) => return err.into_executed(),
    };
    let response = match outcome.value {
        SetResult::OkGet(Some(old)) => owned_value_to_resp(old),
        SetResult::OkGet(None) => CmdResult::Static(RESP_NIL),
        _ => CmdResult::Static(RESP_NIL),
    };
    ExecutedCommand::with_aof_records(response, outcome.aof_records, outcome.aof_lsn)
}

// ── GETDEL ──────────────────────────────────────────────────────────────────

/// GETDEL key — Get the value and delete the key.
#[inline]
pub fn cmd_getdel(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let key_bytes = match args.get(1) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let key = key_from_bytes(key_bytes);
    let outcome = keyspace.remove_value(&key, now_nanos);
    let response = match outcome.value {
        Some(val) => owned_value_to_resp(val),
        None => CmdResult::Static(RESP_NIL),
    };
    ExecutedCommand::with_aof_lsn(response, outcome.aof_lsn)
}

// ── GETEX ───────────────────────────────────────────────────────────────────

/// GETEX key [EX seconds | PX ms | EXAT secs | PXAT ms | PERSIST]
///
/// Get value and optionally set/remove TTL.
#[inline]
#[allow(dead_code)]
pub fn cmd_getex(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    cmd_getex_with_clock(keyspace, frame, now_nanos, now_nanos)
}

#[inline]
pub(crate) fn cmd_getex_with_clock(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
    unix_now_nanos: u64,
) -> ExecutedCommand {
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let key_bytes = match args.get(1) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let key = key_from_bytes(key_bytes);
    let argc = args.len();

    // First, get the value.
    let val = match keyspace.get_value(&key, now_nanos) {
        Some(v) => v,
        None => return ExecutedCommand::from(CmdResult::Static(RESP_NIL)),
    };

    // Then apply TTL modification if specified.
    let mut aof_lsn = None;
    let mut aof_payload = None;
    if argc >= 3 {
        let opt = match args.get(2) {
            Some(b) => b,
            None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
        };
        match opt_upper(opt) {
            OptToken::EX => {
                let secs = match args.i64(3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
                };
                let deadline = now_nanos + secs * NS_PER_SEC;
                aof_lsn = keyspace.expire_key(&key, deadline, now_nanos).aof_lsn;
                if aof_lsn.is_some() && unix_now_nanos != 0 {
                    let absolute_deadline_ms =
                        deadline_nanos_to_absolute_unix_nanos(deadline, now_nanos, unix_now_nanos)
                            / NS_PER_MS;
                    aof_payload = Some(encode_aof_pexpireat(key_bytes, absolute_deadline_ms));
                }
            }
            OptToken::PX => {
                let ms = match args.i64(3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
                };
                let deadline = now_nanos + ms * NS_PER_MS;
                aof_lsn = keyspace.expire_key(&key, deadline, now_nanos).aof_lsn;
                if aof_lsn.is_some() && unix_now_nanos != 0 {
                    let absolute_deadline_ms =
                        deadline_nanos_to_absolute_unix_nanos(deadline, now_nanos, unix_now_nanos)
                            / NS_PER_MS;
                    aof_payload = Some(encode_aof_pexpireat(key_bytes, absolute_deadline_ms));
                }
            }
            OptToken::EXAT => {
                let secs = match args.i64(3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
                };
                let deadline = absolute_unix_nanos_to_deadline_nanos(
                    secs * NS_PER_SEC,
                    now_nanos,
                    unix_now_nanos,
                );
                aof_lsn = keyspace.expire_key(&key, deadline, now_nanos).aof_lsn;
                if aof_lsn.is_some() {
                    aof_payload = Some(encode_aof_pexpireat(key_bytes, secs * 1_000));
                }
            }
            OptToken::PXAT => {
                let ms = match args.i64(3) {
                    Some(s) if s > 0 => s as u64,
                    _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
                };
                let deadline = absolute_unix_nanos_to_deadline_nanos(
                    ms * NS_PER_MS,
                    now_nanos,
                    unix_now_nanos,
                );
                aof_lsn = keyspace.expire_key(&key, deadline, now_nanos).aof_lsn;
                if aof_lsn.is_some() {
                    aof_payload = Some(encode_aof_pexpireat(key_bytes, ms));
                }
            }
            OptToken::KEEPTTL => { /* PERSIST alias in GETEX context */ }
            _ => {
                // Check for "PERSIST" keyword.
                if eq_ci(opt, b"PERSIST") {
                    aof_lsn = keyspace.persist_key(&key).aof_lsn;
                    if aof_lsn.is_some() {
                        aof_payload = Some(encode_aof_persist(key_bytes));
                    }
                } else {
                    return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
                }
            }
        }
    }

    ExecutedCommand::with_optional_aof_payload(owned_value_to_resp(val), aof_lsn, aof_payload)
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
    key_bytes: &[u8],
    delta: i64,
    now_nanos: u64,
) -> ExecutedCommand {
    let key = key_from_bytes(key_bytes);

    match keyspace.increment_by(key, delta, now_nanos) {
        Ok(MutationOutcome {
            value,
            aof_records,
            aof_lsn,
        }) => ExecutedCommand::with_aof_records(int_resp(value), aof_records, aof_lsn),
        Err(err) => err.into_executed(),
    }
}

/// INCR key — Increment by 1.
#[inline]
pub fn cmd_incr(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    match arg_bytes(frame, 1) {
        Some(key_bytes) => incr_by(keyspace, key_bytes, 1, now_nanos),
        None => ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    }
}

/// DECR key — Decrement by 1.
#[inline]
pub fn cmd_decr(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    match arg_bytes(frame, 1) {
        Some(key_bytes) => incr_by(keyspace, key_bytes, -1, now_nanos),
        None => ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    }
}

/// INCRBY key increment.
#[inline]
pub fn cmd_incrby(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let key_bytes = match args.get(1) {
        Some(key_bytes) => key_bytes,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let delta = match args.i64(2) {
        Some(d) => d,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
    };
    incr_by(keyspace, key_bytes, delta, now_nanos)
}

/// DECRBY key decrement.
#[inline]
pub fn cmd_decrby(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let key_bytes = match args.get(1) {
        Some(key_bytes) => key_bytes,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let delta = match args.i64(2) {
        Some(d) => d,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
    };
    incr_by(keyspace, key_bytes, -delta, now_nanos)
}

// ── INCRBYFLOAT ─────────────────────────────────────────────────────────────

/// INCRBYFLOAT key increment.
///
/// Result is stored as a string (Redis behavior).
pub fn cmd_incrbyfloat(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> ExecutedCommand {
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let key_bytes = match args.get(1) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let incr_bytes = match args.get(2) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };

    let incr: f64 = match std::str::from_utf8(incr_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(f) => f,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_FLOAT)),
    };

    if incr.is_nan() || incr.is_infinite() {
        return ExecutedCommand::from(CmdResult::Static(ERR_NOT_FLOAT));
    }

    let key = key_from_bytes(key_bytes);

    match keyspace.increment_by_float(key, incr, now_nanos) {
        Ok(MutationOutcome {
            value,
            aof_records,
            aof_lsn,
        }) => ExecutedCommand::with_aof_records(
            CmdResult::Resp(RespFrame::bulk_string(value)),
            aof_records,
            aof_lsn,
        ),
        Err(err) => err.into_executed(),
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
) -> ExecutedCommand {
    // APPEND always has exactly 3 args: APPEND key value.
    if let (Some(key_bytes), Some(append_bytes)) = (arg_bytes(frame, 1), arg_bytes(frame, 2)) {
        let key = key_from_bytes(key_bytes);
        return match keyspace.append_value(key, append_bytes, now_nanos) {
            Ok(MutationOutcome {
                value,
                aof_records,
                aof_lsn,
            }) => ExecutedCommand::with_aof_records(int_resp(value as i64), aof_records, aof_lsn),
            Err(err) => err.into_executed(),
        };
    }
    ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX))
}

// ── STRLEN ──────────────────────────────────────────────────────────────────

/// STRLEN key — Returns the length of the string value.
#[inline]
pub fn cmd_strlen(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    let Some(args) = CommandArgs::collect(frame) else {
        return CmdResult::Static(ERR_SYNTAX);
    };
    let key_bytes = match args.get(1) {
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
    let Some(args) = CommandArgs::collect(frame) else {
        return CmdResult::Static(ERR_SYNTAX);
    };
    let key_bytes = match args.get(1) {
        Some(b) => b,
        None => return CmdResult::Static(ERR_SYNTAX),
    };
    let start = match args.i64(2) {
        Some(s) => s,
        None => return CmdResult::Static(ERR_NOT_INTEGER),
    };
    let end = match args.i64(3) {
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
) -> ExecutedCommand {
    let Some(args) = CommandArgs::collect(frame) else {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    };
    let key_bytes = match args.get(1) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };
    let offset = match args.i64(2) {
        Some(o) if o >= 0 => o as usize,
        _ => return ExecutedCommand::from(CmdResult::Static(ERR_NOT_INTEGER)),
    };
    let new_bytes = match args.get(3) {
        Some(b) => b,
        None => return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
    };

    let key = key_from_bytes(key_bytes);
    match keyspace.setrange_value(key, offset, new_bytes, now_nanos) {
        Ok(MutationOutcome {
            value,
            aof_records,
            aof_lsn,
        }) => ExecutedCommand::with_aof_records(int_resp(value as i64), aof_records, aof_lsn),
        Err(err) => err.into_executed(),
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

fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::commands::test_harness::TestHarness;
    use crate::eviction::EvictionPolicy;
    use vortex_proto::RespTape;

    trait ResultView {
        fn as_cmd_result(&self) -> &CmdResult;
    }

    impl ResultView for CmdResult {
        fn as_cmd_result(&self) -> &CmdResult {
            self
        }
    }

    impl ResultView for ExecutedCommand {
        fn as_cmd_result(&self) -> &CmdResult {
            &self.response
        }
    }

    /// Helper: parse a raw RESP command and return (tape, shard).
    /// Caller must do `tape.iter().next().unwrap()` to get the FrameRef.
    fn make_tape(input: &[u8]) -> RespTape {
        RespTape::parse_pipeline(input).expect("valid RESP input")
    }

    fn make_resp(parts: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            buf.extend_from_slice(part);
            buf.extend_from_slice(b"\r\n");
        }
        buf
    }

    /// Assert a CmdResult is a static byte slice.
    fn assert_static(result: &impl ResultView, expected: &[u8]) {
        match result.as_cmd_result() {
            CmdResult::Static(b) => assert_eq!(*b, expected, "static mismatch"),
            CmdResult::Inline(_) => panic!("expected Static, got Inline"),
            CmdResult::Resp(_) => panic!("expected Static, got Resp"),
        }
    }

    /// Extract bulk string bytes from a CmdResult::Resp.
    fn resp_bytes(result: &impl ResultView) -> &[u8] {
        match result.as_cmd_result() {
            CmdResult::Inline(inline) => inline.payload(),
            CmdResult::Resp(RespFrame::BulkString(Some(b))) => b.as_ref(),
            CmdResult::Resp(other) => panic!("expected BulkString, got {:?}", other),
            CmdResult::Static(b) => {
                panic!("expected Resp, got Static({:?})", std::str::from_utf8(b))
            }
        }
    }

    /// Extract integer from a CmdResult::Resp or CmdResult::Static.
    fn resp_int(result: &impl ResultView) -> i64 {
        match result.as_cmd_result() {
            CmdResult::Resp(RespFrame::Integer(n)) => *n,
            CmdResult::Resp(other) => panic!("expected Integer, got {:?}", other),
            CmdResult::Inline(_) => panic!("expected Integer, got Inline bulk string"),
            CmdResult::Static(b) => {
                // Handle static integer responses from int_resp optimization.
                if *b == b":0\r\n" {
                    return 0;
                }
                if *b == b":1\r\n" {
                    return 1;
                }
                if *b == b":-1\r\n" {
                    return -1;
                }
                if *b == b":-2\r\n" {
                    return -2;
                }
                panic!("expected Resp, got Static({:?})", std::str::from_utf8(b))
            }
        }
    }

    fn same_shard_keys(keyspace: &ConcurrentKeyspace, count: usize) -> Vec<Vec<u8>> {
        let mut found = Vec::with_capacity(count);
        let target = keyspace.shard_index(b"evict:seed");
        for index in 0..10_000usize {
            let key = format!("evict:{index:04}").into_bytes();
            if keyspace.shard_index(&key) != target {
                continue;
            }
            found.push(key);
            if found.len() == count {
                return found;
            }
        }
        panic!("failed to find {count} keys on shard {target}");
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

    #[test]
    fn get_expired_key_cleans_up_ttl_count() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"ttl" as &[u8]);
        let deadline = NS_PER_SEC;
        let now = deadline + 1;
        h.set_with_ttl(key.clone(), VortexValue::from_bytes(b"bar"), deadline);

        assert_eq!(h.keyspace.info_keyspace(now), (1, 1));

        let tape = make_tape(b"*2\r\n$3\r\nGET\r\n$3\r\nttl\r\n");
        let frame = tape.iter().next().unwrap();
        let result = cmd_get(&h.keyspace, &frame, now);
        assert_static(&result, RESP_NIL);
        assert_eq!(h.keyspace.info_keyspace(now), (0, 0));
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
    fn set_noeviction_returns_oom_without_mutating() {
        let h = TestHarness::new();
        let keys = same_shard_keys(&h.keyspace, 2);
        let first = VortexKey::from(keys[0].as_slice());
        h.set(first.clone(), VortexValue::from_bytes(b"warm"));

        h.keyspace
            .configure_eviction(h.keyspace.memory_used(), EvictionPolicy::NoEviction);

        let cmd = make_resp(&[b"SET", keys[1].as_slice(), b"new"]);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&h.keyspace, &frame, 0);

        assert_static(&result, super::super::ERR_OOM);
        assert!(h.get(&first, 0).is_some());
        assert!(h.get(&VortexKey::from(keys[1].as_slice()), 0).is_none());
    }

    #[test]
    fn set_allkeys_lru_evicts_colder_same_shard_key() {
        let h = TestHarness::new();
        h.keyspace.enable_aof_recording();
        let keys = same_shard_keys(&h.keyspace, 3);
        let hot = VortexKey::from(keys[0].as_slice());
        let cold = VortexKey::from(keys[1].as_slice());
        let incoming = VortexKey::from(keys[2].as_slice());

        h.set(hot.clone(), VortexValue::from_bytes(b"warm"));
        h.set(cold.clone(), VortexValue::from_bytes(b"cool"));

        h.keyspace
            .configure_eviction(h.keyspace.memory_used(), EvictionPolicy::AllKeysLru);
        for _ in 0..16 {
            let _ = h.get(&hot, 0);
        }

        let cmd = make_resp(&[b"SET", keys[2].as_slice(), b"mild"]);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&h.keyspace, &frame, 0);

        assert_static(&result, RESP_OK);
        let aof_records = result.aof_records.as_ref().expect("eviction records");
        assert_eq!(aof_records.len(), 1);
        assert_eq!(aof_records[0].key.as_bytes(), keys[1].as_slice());
        assert!(
            aof_records[0].lsn < result.aof_lsn().expect("SET allocates LSN"),
            "eviction must be logged before the triggering SET"
        );
        assert!(h.get(&hot, 0).is_some());
        assert!(h.get(&incoming, 0).is_some());
        assert!(h.get(&cold, 0).is_none());
    }

    #[test]
    fn set_logs_eviction_even_when_write_still_ooms() {
        let h = TestHarness::new();
        h.keyspace.enable_aof_recording();
        let keys = same_shard_keys(&h.keyspace, 2);
        let resident = VortexKey::from(keys[0].as_slice());
        let incoming = VortexKey::from(keys[1].as_slice());

        h.set(resident.clone(), VortexValue::from_bytes(b"warm"));
        h.keyspace
            .configure_eviction(h.keyspace.memory_used(), EvictionPolicy::AllKeysLru);

        let oversized = vec![b'x'; h.keyspace.memory_used().saturating_add(4096)];
        let cmd = make_resp(&[b"SET", keys[1].as_slice(), oversized.as_slice()]);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&h.keyspace, &frame, 0);

        assert_static(&result, super::super::ERR_OOM);
        let aof_records = result.aof_records.as_ref().expect("eviction records");
        assert_eq!(aof_records.len(), 1);
        assert_eq!(aof_records[0].key.as_bytes(), keys[0].as_slice());
        assert!(result.aof_lsn().is_none());
        assert!(h.get(&resident, 0).is_none());
        assert!(h.get(&incoming, 0).is_none());
    }

    #[test]
    fn set_allkeys_lfu_evicts_lower_frequency_same_shard_key() {
        let h = TestHarness::new();
        let keys = same_shard_keys(&h.keyspace, 3);
        let hot = VortexKey::from(keys[0].as_slice());
        let cold = VortexKey::from(keys[1].as_slice());
        let incoming = VortexKey::from(keys[2].as_slice());

        h.set(hot.clone(), VortexValue::from_bytes(b"warm"));
        h.set(cold.clone(), VortexValue::from_bytes(b"cool"));

        h.keyspace
            .configure_eviction(h.keyspace.memory_used(), EvictionPolicy::AllKeysLfu);
        for _ in 0..32 {
            let _ = h.get(&hot, 0);
        }

        let cmd = make_resp(&[b"SET", keys[2].as_slice(), b"mild"]);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();
        let frame = tape.iter().next().unwrap();
        let result = cmd_set(&h.keyspace, &frame, 0);

        assert_static(&result, RESP_OK);
        assert!(h.get(&hot, 0).is_some());
        assert!(h.get(&incoming, 0).is_some());
        assert!(h.get(&cold, 0).is_none());
    }

    #[test]
    fn append_noeviction_returns_oom_without_mutating() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"grow" as &[u8]);
        h.set(key.clone(), VortexValue::from_bytes(b"hi"));

        h.keyspace
            .configure_eviction(h.keyspace.memory_used(), EvictionPolicy::NoEviction);

        let cmd = make_resp(&[b"APPEND", b"grow", b"-there"]);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();
        let frame = tape.iter().next().unwrap();
        let result = cmd_append(&h.keyspace, &frame, 0);

        assert_static(&result, super::super::ERR_OOM);
        assert_eq!(h.get(&key, 0).unwrap().as_string_bytes().unwrap(), b"hi");
    }

    #[test]
    fn setrange_noeviction_returns_oom_without_mutating() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"range" as &[u8]);
        h.set(key.clone(), VortexValue::from_bytes(b"hi"));

        h.keyspace
            .configure_eviction(h.keyspace.memory_used(), EvictionPolicy::NoEviction);

        let cmd = make_resp(&[b"SETRANGE", b"range", b"5", b"x"]);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();
        let frame = tape.iter().next().unwrap();
        let result = cmd_setrange(&h.keyspace, &frame, 0);

        assert_static(&result, super::super::ERR_OOM);
        assert_eq!(h.get(&key, 0).unwrap().as_string_bytes().unwrap(), b"hi");
    }

    #[test]
    fn incr_missing_noeviction_returns_oom_without_mutating() {
        let h = TestHarness::new();
        let anchor = VortexKey::from(b"anchor" as &[u8]);
        h.set(anchor.clone(), VortexValue::from_bytes(b"stay"));

        h.keyspace
            .configure_eviction(h.keyspace.memory_used(), EvictionPolicy::NoEviction);

        let cmd = make_resp(&[b"INCR", b"counter"]);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();
        let frame = tape.iter().next().unwrap();
        let result = cmd_incr(&h.keyspace, &frame, 0);

        assert_static(&result, super::super::ERR_OOM);
        assert!(h.get(&VortexKey::from(b"counter" as &[u8]), 0).is_none());
        assert_eq!(
            h.get(&anchor, 0).unwrap().as_string_bytes().unwrap(),
            b"stay"
        );
    }

    #[test]
    fn incrbyfloat_noeviction_returns_oom_without_mutating() {
        let h = TestHarness::new();
        let key = VortexKey::from(b"float" as &[u8]);
        h.set(key.clone(), VortexValue::Integer(1));

        h.keyspace
            .configure_eviction(h.keyspace.memory_used(), EvictionPolicy::NoEviction);

        let cmd = make_resp(&[b"INCRBYFLOAT", b"float", b"0.25"]);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();
        let frame = tape.iter().next().unwrap();
        let result = cmd_incrbyfloat(&h.keyspace, &frame, 0);

        assert_static(&result, super::super::ERR_OOM);
        assert_eq!(
            resp_bytes(&cmd_get(
                &h.keyspace,
                &make_tape(b"*2\r\n$3\r\nGET\r\n$5\r\nfloat\r\n")
                    .iter()
                    .next()
                    .unwrap(),
                0
            )),
            b"1"
        );
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
