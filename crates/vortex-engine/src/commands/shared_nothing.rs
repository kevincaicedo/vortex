//! Single-key command bridge for the shared-nothing executor experiment.
//!
//! This module intentionally owns RESP argument parsing and response shaping
//! for the shared-nothing adapter. Owner code receives typed operations and
//! returns typed outcomes; it does not parse RESP or choose wire encodings.

use bytes::Bytes;
use vortex_common::{VortexKey, VortexValue};
use vortex_proto::{FrameRef, RespFrame};

use super::{
    CmdResult, CommandArgs, CommandClock, ERR_NOT_INTEGER, ERR_SYNTAX, ExecutedCommand, NS_PER_MS,
    NS_PER_SEC, RESP_NIL, RESP_OK, absolute_unix_nanos_to_deadline_nanos, arg_bytes, int_resp,
    owned_value_to_resp, value_to_resp,
};
use crate::engine::domain::TtlState;
use crate::owner::{TxnId, TxnIntent, TxnIntentKind};

static ERR_WRONG_ARGS: &[u8] = b"-ERR wrong number of arguments\r\n";

/// Parsed single-key operation with borrowed payloads where that is safe for
/// the current reactor turn.
#[derive(Debug)]
pub(crate) enum SharedNothingCommand<'a> {
    Get {
        key_bytes: &'a [u8],
    },
    SetPlain {
        key_bytes: &'a [u8],
        value_bytes: &'a [u8],
    },
    Set {
        key: VortexKey,
        value: VortexValue,
        options: SharedNothingSetOptions,
    },
    Del {
        key_bytes: &'a [u8],
    },
    Incr {
        key_bytes: &'a [u8],
    },
    Exists {
        key_bytes: &'a [u8],
    },
    Ttl {
        key_bytes: &'a [u8],
        unit: TtlUnit,
    },
    Type {
        key_bytes: &'a [u8],
    },
}

impl<'a> SharedNothingCommand<'a> {
    /// Key bytes used for routing.
    #[inline]
    pub(crate) fn key_bytes(&self) -> &[u8] {
        match self {
            Self::Get { key_bytes }
            | Self::SetPlain { key_bytes, .. }
            | Self::Del { key_bytes }
            | Self::Incr { key_bytes }
            | Self::Exists { key_bytes }
            | Self::Ttl { key_bytes, .. }
            | Self::Type { key_bytes } => key_bytes,
            Self::Set { key, .. } => key.as_bytes(),
        }
    }

    /// Converts a borrowed command into an owned remote-owner payload.
    #[inline]
    pub(crate) fn into_owned(self) -> OwnedSharedNothingCommand {
        match self {
            Self::Get { key_bytes } => OwnedSharedNothingCommand::Get {
                key: key_bytes.into(),
            },
            Self::SetPlain {
                key_bytes,
                value_bytes,
            } => OwnedSharedNothingCommand::SetPlain {
                key: key_bytes.into(),
                value: VortexValue::from_bytes(value_bytes),
            },
            Self::Set {
                key,
                value,
                options,
            } => OwnedSharedNothingCommand::Set {
                key,
                value,
                options,
            },
            Self::Del { key_bytes } => OwnedSharedNothingCommand::Del {
                key: key_bytes.into(),
            },
            Self::Incr { key_bytes } => OwnedSharedNothingCommand::Incr {
                key: key_bytes.into(),
            },
            Self::Exists { key_bytes } => OwnedSharedNothingCommand::Exists {
                key: key_bytes.into(),
            },
            Self::Ttl { key_bytes, unit } => OwnedSharedNothingCommand::Ttl {
                key: key_bytes.into(),
                unit,
            },
            Self::Type { key_bytes } => OwnedSharedNothingCommand::Type {
                key: key_bytes.into(),
            },
        }
    }
}

/// Owned single-key command sent to a remote owner worker.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub enum OwnedSharedNothingCommand {
    Get {
        key: VortexKey,
    },
    SetPlain {
        key: VortexKey,
        value: VortexValue,
    },
    Set {
        key: VortexKey,
        value: VortexValue,
        options: SharedNothingSetOptions,
    },
    Del {
        key: VortexKey,
    },
    Incr {
        key: VortexKey,
    },
    Exists {
        key: VortexKey,
    },
    Ttl {
        key: VortexKey,
        unit: TtlUnit,
    },
    Type {
        key: VortexKey,
    },
    Mget {
        keys: Box<[VortexKey]>,
    },
    ExistsMany {
        keys: Box<[VortexKey]>,
    },
    TxnPrepare {
        txn_id: TxnId,
        intent: TxnIntent,
    },
    TxnCaptureSource {
        txn_id: TxnId,
        kind: TxnIntentKind,
        source: VortexKey,
    },
    TxnPrepareLocalDual {
        txn_id: TxnId,
        kind: TxnIntentKind,
        source: VortexKey,
        destination: VortexKey,
    },
    TxnCommit {
        txn_id: TxnId,
    },
    TxnAbort {
        txn_id: TxnId,
    },
}

/// SET options parsed from the command frame.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SharedNothingSetOptions {
    pub(crate) ttl_deadline: u64,
    pub(crate) nx: bool,
    pub(crate) xx: bool,
    pub(crate) get: bool,
    pub(crate) keepttl: bool,
}

/// TTL response unit.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TtlUnit {
    Seconds,
    Millis,
}

/// SET command result from the owner storage path.
#[derive(Debug, PartialEq)]
pub(crate) enum SharedNothingSetResult {
    Ok,
    NotSet,
    OkGet(Option<VortexValue>),
    NotSetGet(Option<VortexValue>),
}

/// Parse result for the shared-nothing single-key adapter.
#[derive(Debug)]
pub(crate) enum SharedNothingParse<'a> {
    Command(SharedNothingCommand<'a>),
    Immediate(ExecutedCommand),
    Unsupported,
}

/// Parses the SN-004 single-key command set.
///
/// Returns `None` when the command is not part of the SN-004 adapter surface.
#[inline]
pub(crate) fn parse_shared_nothing_command<'a>(
    name: &[u8],
    frame: &FrameRef<'a>,
    clock: CommandClock,
) -> Option<SharedNothingParse<'a>> {
    match name {
        b"PING" => Some(parse_ping(frame)),
        b"GET" => Some(parse_get(frame)),
        b"SET" => Some(parse_set(frame, clock)),
        b"DEL" => Some(parse_one_key_or_multi_unsupported(frame, |key_bytes| {
            SharedNothingCommand::Del { key_bytes }
        })),
        b"UNLINK" => Some(parse_one_key_or_multi_unsupported(frame, |key_bytes| {
            SharedNothingCommand::Del { key_bytes }
        })),
        b"INCR" => Some(parse_exact_one_key(frame, |key_bytes| {
            SharedNothingCommand::Incr { key_bytes }
        })),
        b"EXISTS" => Some(parse_one_key_or_multi_unsupported(frame, |key_bytes| {
            SharedNothingCommand::Exists { key_bytes }
        })),
        b"TTL" => Some(parse_exact_one_key(frame, |key_bytes| {
            SharedNothingCommand::Ttl {
                key_bytes,
                unit: TtlUnit::Seconds,
            }
        })),
        b"PTTL" => Some(parse_exact_one_key(frame, |key_bytes| {
            SharedNothingCommand::Ttl {
                key_bytes,
                unit: TtlUnit::Millis,
            }
        })),
        b"TYPE" => Some(parse_exact_one_key(frame, |key_bytes| {
            SharedNothingCommand::Type { key_bytes }
        })),
        _ => None,
    }
}

#[inline]
fn parse_ping<'a>(frame: &FrameRef<'a>) -> SharedNothingParse<'a> {
    static RESP_PONG: &[u8] = b"+PONG\r\n";

    let Some(args) = CommandArgs::collect(frame) else {
        return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(RESP_PONG)));
    };
    if args.len() <= 1 {
        return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(RESP_PONG)));
    }
    if let Some(message) = args.get(1) {
        return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Resp(
            RespFrame::bulk_string(Bytes::copy_from_slice(message)),
        )));
    }
    SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(RESP_PONG)))
}

#[inline]
fn parse_get<'a>(frame: &FrameRef<'a>) -> SharedNothingParse<'a> {
    let Some(key_bytes) = arg_bytes(frame, 1) else {
        return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(RESP_NIL)));
    };
    SharedNothingParse::Command(SharedNothingCommand::Get { key_bytes })
}

#[inline]
fn parse_one_key_or_multi_unsupported<'a>(
    frame: &FrameRef<'a>,
    build: impl FnOnce(&'a [u8]) -> SharedNothingCommand<'a>,
) -> SharedNothingParse<'a> {
    match frame.element_count().map(|count| count as usize) {
        Some(2) => parse_exact_one_key(frame, build),
        Some(argc) if argc > 2 => SharedNothingParse::Unsupported,
        _ => {
            SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS)))
        }
    }
}

#[inline]
fn parse_exact_one_key<'a>(
    frame: &FrameRef<'a>,
    build: impl FnOnce(&'a [u8]) -> SharedNothingCommand<'a>,
) -> SharedNothingParse<'a> {
    match (
        frame.element_count().map(|count| count as usize),
        arg_bytes(frame, 1),
    ) {
        (Some(2), Some(key_bytes)) => SharedNothingParse::Command(build(key_bytes)),
        _ => {
            SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(ERR_WRONG_ARGS)))
        }
    }
}

#[inline]
fn parse_set<'a>(frame: &FrameRef<'a>, clock: CommandClock) -> SharedNothingParse<'a> {
    let argc = match frame.element_count() {
        Some(n) => n as usize,
        None => {
            return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(
                ERR_SYNTAX,
            )));
        }
    };
    if argc < 3 {
        return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
    }

    if argc == 3 {
        let Some(key_bytes) = arg_bytes(frame, 1) else {
            return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(
                ERR_SYNTAX,
            )));
        };
        let Some(value_bytes) = arg_bytes(frame, 2) else {
            return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(
                ERR_SYNTAX,
            )));
        };
        return SharedNothingParse::Command(SharedNothingCommand::SetPlain {
            key_bytes,
            value_bytes,
        });
    }

    let Some(args) = CommandArgs::collect(frame) else {
        return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
    };
    let Some(key_bytes) = args.get(1) else {
        return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
    };
    let Some(value_bytes) = args.get(2) else {
        return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
    };

    let mut options = SharedNothingSetOptions::default();
    let mut i = 3;
    while i < argc {
        let Some(opt) = args.get(i) else {
            return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(
                ERR_SYNTAX,
            )));
        };

        match set_opt_token(opt) {
            SetOptToken::EX => {
                i += 1;
                let secs = match args.i64(i) {
                    Some(secs) if secs > 0 => secs as u64,
                    _ => {
                        return SharedNothingParse::Immediate(ExecutedCommand::from(
                            CmdResult::Static(ERR_NOT_INTEGER),
                        ));
                    }
                };
                options.ttl_deadline = clock.monotonic_nanos + secs * NS_PER_SEC;
            }
            SetOptToken::PX => {
                i += 1;
                let millis = match args.i64(i) {
                    Some(millis) if millis > 0 => millis as u64,
                    _ => {
                        return SharedNothingParse::Immediate(ExecutedCommand::from(
                            CmdResult::Static(ERR_NOT_INTEGER),
                        ));
                    }
                };
                options.ttl_deadline = clock.monotonic_nanos + millis * NS_PER_MS;
            }
            SetOptToken::EXAT => {
                i += 1;
                let secs = match args.i64(i) {
                    Some(secs) if secs > 0 => secs as u64,
                    _ => {
                        return SharedNothingParse::Immediate(ExecutedCommand::from(
                            CmdResult::Static(ERR_NOT_INTEGER),
                        ));
                    }
                };
                options.ttl_deadline = absolute_unix_nanos_to_deadline_nanos(
                    secs * NS_PER_SEC,
                    clock.monotonic_nanos,
                    clock.unix_nanos,
                );
            }
            SetOptToken::PXAT => {
                i += 1;
                let millis = match args.i64(i) {
                    Some(millis) if millis > 0 => millis as u64,
                    _ => {
                        return SharedNothingParse::Immediate(ExecutedCommand::from(
                            CmdResult::Static(ERR_NOT_INTEGER),
                        ));
                    }
                };
                options.ttl_deadline = absolute_unix_nanos_to_deadline_nanos(
                    millis * NS_PER_MS,
                    clock.monotonic_nanos,
                    clock.unix_nanos,
                );
            }
            SetOptToken::NX => options.nx = true,
            SetOptToken::XX => options.xx = true,
            SetOptToken::GET => options.get = true,
            SetOptToken::KEEPTTL => options.keepttl = true,
            SetOptToken::Unknown => {
                return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(
                    ERR_SYNTAX,
                )));
            }
        }
        i += 1;
    }

    if options.nx && options.xx {
        return SharedNothingParse::Immediate(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
    }

    SharedNothingParse::Command(SharedNothingCommand::Set {
        key: key_bytes.into(),
        value: super::value_from_bytes(value_bytes),
        options,
    })
}

/// Shapes GET response bytes.
#[inline]
pub(crate) fn get_response(value: Option<&VortexValue>) -> CmdResult {
    value.map_or(CmdResult::Static(RESP_NIL), value_to_resp)
}

/// Shapes SET response bytes.
#[inline]
pub(crate) fn set_response(result: SharedNothingSetResult) -> CmdResult {
    match result {
        SharedNothingSetResult::Ok => CmdResult::Static(RESP_OK),
        SharedNothingSetResult::NotSet => CmdResult::Static(RESP_NIL),
        SharedNothingSetResult::OkGet(Some(value))
        | SharedNothingSetResult::NotSetGet(Some(value)) => owned_value_to_resp(value),
        SharedNothingSetResult::OkGet(None) | SharedNothingSetResult::NotSetGet(None) => {
            CmdResult::Static(RESP_NIL)
        }
    }
}

/// Shapes DEL response bytes.
#[inline]
pub(crate) fn del_response(deleted: bool) -> CmdResult {
    int_resp(i64::from(deleted))
}

/// Shapes EXISTS response bytes.
#[inline]
pub(crate) fn exists_response(exists: bool) -> CmdResult {
    int_resp(i64::from(exists))
}

/// Shapes TTL/PTTL response bytes.
#[inline]
pub(crate) fn ttl_response(state: TtlState, unit: TtlUnit, now_nanos: u64) -> CmdResult {
    match state {
        TtlState::Missing => super::CmdResult::Static(super::RESP_NEG_TWO),
        TtlState::Persistent => super::CmdResult::Static(super::RESP_NEG_ONE),
        TtlState::Deadline(deadline) => {
            let divisor = match unit {
                TtlUnit::Seconds => NS_PER_SEC,
                TtlUnit::Millis => NS_PER_MS,
            };
            int_resp(((deadline - now_nanos) / divisor) as i64)
        }
    }
}

/// Shapes TYPE response bytes.
#[inline]
pub(crate) fn type_response(type_name: Option<&'static str>) -> CmdResult {
    CmdResult::Resp(RespFrame::simple_string(type_name.unwrap_or("none")))
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SetOptToken {
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

#[inline]
fn set_opt_token(bytes: &[u8]) -> SetOptToken {
    match bytes.len() {
        2 => {
            let a = bytes[0] | 0x20;
            let b = bytes[1] | 0x20;
            match (a, b) {
                (b'e', b'x') => SetOptToken::EX,
                (b'p', b'x') => SetOptToken::PX,
                (b'n', b'x') => SetOptToken::NX,
                (b'x', b'x') => SetOptToken::XX,
                _ => SetOptToken::Unknown,
            }
        }
        3 if bytes.eq_ignore_ascii_case(b"GET") => SetOptToken::GET,
        4 if bytes.eq_ignore_ascii_case(b"EXAT") => SetOptToken::EXAT,
        4 if bytes.eq_ignore_ascii_case(b"PXAT") => SetOptToken::PXAT,
        7 if bytes.eq_ignore_ascii_case(b"KEEPTTL") => SetOptToken::KEEPTTL,
        _ => SetOptToken::Unknown,
    }
}
