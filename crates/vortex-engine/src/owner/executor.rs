//! Shared-nothing command executor adapter for the research branch.
//!
//! The adapter keeps RESP parsing and reply shaping in
//! `crate::commands::shared_nothing`, while this module owns routing,
//! owner-local table execution, remote owner handoff, and connection-generation
//! response guards.

use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes;
use vortex_common::{VortexKey, VortexValue};
use vortex_sync::{SpscReceiver, SpscSender, spsc_channel};

use crate::SwissTable;
pub use crate::commands::shared_nothing::OwnedSharedNothingCommand;
use crate::commands::shared_nothing::{
    SharedNothingCommand, SharedNothingParse, SharedNothingSetOptions, SharedNothingSetResult,
    del_response, exists_response, get_response, parse_shared_nothing_command, set_response,
    ttl_response, type_response,
};
use crate::commands::{
    CmdResult, CommandClock, ERR_NOT_INTEGER, ERR_OVERFLOW, ERR_SYNTAX, ERR_WRONG_TYPE,
    ExecutedCommand, RESP_NIL, arg_bytes, parse_i64,
};
use crate::engine::domain::TtlState;
use crate::table::{BorrowedKey, MutationPolicy, RawValueBytes, SlotCursor, TableHash};

use super::{KeyRoute, OwnerId, OwnerTopology, TopologyConfig, TopologyConfigError};
use super::{TxnAction, TxnFinishOutcome, TxnId, TxnIntent, TxnIntentKind, TxnPrepareOutcome};

static RESP_ERR_TXN_PENDING: &[u8] = b"-TRYAGAIN shared-nothing transaction is pending\r\n";

/// Synthetic connection identifier carried by remote continuations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct SharedNothingConnectionId(u32);

impl SharedNothingConnectionId {
    /// Creates a connection identifier.
    #[inline]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Returns the raw connection identifier.
    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Connection generation used to reject stale remote replies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SharedNothingConnectionGeneration(u64);

impl SharedNothingConnectionGeneration {
    /// Initial connection generation.
    pub const INITIAL: Self = Self(0);

    /// Creates a generation from a reactor-owned generation counter.
    #[inline]
    pub const fn new(generation: u64) -> Self {
        Self(generation)
    }

    /// Returns the next generation.
    #[inline]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    /// Returns the raw generation.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Connection token captured when a remote command is accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SharedNothingConnectionToken {
    id: SharedNothingConnectionId,
    generation: SharedNothingConnectionGeneration,
}

impl SharedNothingConnectionToken {
    /// Creates a token from reactor-owned connection identity.
    #[inline]
    pub const fn new(
        id: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
    ) -> Self {
        Self { id, generation }
    }

    /// Synthetic token for engine-only tests and benchmarks without sockets.
    #[inline]
    pub const fn synthetic() -> Self {
        Self::new(
            SharedNothingConnectionId::new(0),
            SharedNothingConnectionGeneration::INITIAL,
        )
    }

    /// Connection ID.
    #[inline]
    pub const fn id(self) -> SharedNothingConnectionId {
        self.id
    }

    /// Generation captured at command acceptance.
    #[inline]
    pub const fn generation(self) -> SharedNothingConnectionGeneration {
        self.generation
    }

    /// Returns whether `current` still matches this token.
    #[inline]
    pub const fn is_current(self, current: SharedNothingConnectionGeneration) -> bool {
        self.generation.0 == current.0
    }
}

/// Remote response continuation captured by the ingress owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteContinuation {
    request_id: u64,
    ingress_owner: OwnerId,
    target_owner: OwnerId,
    connection: SharedNothingConnectionToken,
}

impl RemoteContinuation {
    #[inline]
    fn new(
        request_id: u64,
        ingress_owner: OwnerId,
        target_owner: OwnerId,
        connection: SharedNothingConnectionToken,
    ) -> Self {
        Self {
            request_id,
            ingress_owner,
            target_owner,
            connection,
        }
    }

    /// Request ID.
    #[inline]
    pub const fn request_id(self) -> u64 {
        self.request_id
    }

    /// Ingress owner that accepted the client command.
    #[inline]
    pub const fn ingress_owner(self) -> OwnerId {
        self.ingress_owner
    }

    /// Owner that executed the command.
    #[inline]
    pub const fn target_owner(self) -> OwnerId {
        self.target_owner
    }

    /// Captured connection token.
    #[inline]
    pub const fn connection(self) -> SharedNothingConnectionToken {
        self.connection
    }
}

/// Result of executing through the shared-nothing adapter.
#[derive(Debug)]
pub enum SharedNothingExecutionResult {
    /// A response is current and can be queued for the connection.
    Ready {
        /// Owner selected by routing.
        owner: OwnerId,
        /// Remote queue wait when measured; zero for local execution.
        queue_wait: Duration,
        /// Executed command response.
        command: ExecutedCommand,
    },
    /// The command completed after the connection generation changed.
    StaleConnection {
        /// Dropped continuation.
        continuation: RemoteContinuation,
    },
    /// The command is outside the SN-004 single-key adapter surface.
    Unsupported,
}

/// Reactor-facing result from parsing and routing one shared-nothing command.
#[doc(hidden)]
pub enum SharedNothingOwnerDispatch {
    /// The command completed against the reactor-local owner partition.
    Ready {
        /// Owner selected by routing.
        owner: OwnerId,
        /// Executed command response.
        command: ExecutedCommand,
    },
    /// The command targets another owner and must be sent through the reactor
    /// mailbox fabric.
    Remote {
        /// Owner selected by routing.
        owner: OwnerId,
        /// Owned command payload safe to retain after the parser buffer moves on.
        command: OwnedSharedNothingCommand,
    },
    /// The command produced a response without owner-table execution.
    Immediate(ExecutedCommand),
    /// The command touches an owner-local prepared key and must wait until the
    /// transaction commits or aborts.
    Blocked,
    /// The command is outside the current shared-nothing server surface.
    Unsupported,
}

/// Coarse parse result used by shared-nothing cost probes.
///
/// This intentionally does not expose parsed command internals. It exists so
/// SN-004A Criterion rows can isolate parser cost without making owner command
/// handlers depend on private parser representation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SharedNothingParseCostKind {
    /// The frame parsed into a command that would touch owner state.
    Command,
    /// The parser produced a complete response without owner execution.
    Immediate,
    /// The command belongs to the SN surface but not the current single-key subset.
    Unsupported,
    /// The command name is outside the shared-nothing adapter surface.
    NotSharedNothingCommand,
}

impl SharedNothingExecutionResult {
    /// Returns the command when the result is ready.
    #[inline]
    pub fn into_command(self) -> Option<ExecutedCommand> {
        match self {
            Self::Ready { command, .. } => Some(command),
            Self::StaleConnection { .. } | Self::Unsupported => None,
        }
    }
}

struct OwnerExecutorPartition {
    owner: OwnerId,
    table: SwissTable,
    prepared: Vec<PreparedOwnerTxn>,
}

struct PreparedOwnerTxn {
    txn_id: TxnId,
    intent: TxnIntent,
}

#[derive(Clone, Debug)]
struct CapturedSourceValue {
    value: VortexValue,
    ttl_deadline: u64,
}

impl OwnerExecutorPartition {
    fn with_capacity(owner: OwnerId, capacity: usize) -> Self {
        Self {
            owner,
            table: SwissTable::with_capacity(capacity),
            prepared: Vec::new(),
        }
    }

    #[inline]
    fn execute_borrowed(
        &mut self,
        owner: OwnerId,
        command: SharedNothingCommand<'_>,
        clock: CommandClock,
    ) -> ExecutedCommand {
        self.assert_owner(owner);
        match command {
            SharedNothingCommand::Get { key_bytes } => {
                if self.key_has_prepared_intent(key_bytes) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(self.get_bytes(key_bytes, clock.monotonic_nanos))
            }
            SharedNothingCommand::SetPlain {
                key_bytes,
                value_bytes,
            } => {
                if self.key_has_prepared_intent(key_bytes) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                self.set_plain_bytes(key_bytes, value_bytes);
                ExecutedCommand::from(CmdResult::Static(crate::commands::RESP_OK))
            }
            SharedNothingCommand::Set {
                key,
                value,
                options,
            } => ExecutedCommand::from(set_response(self.set_with_options(
                {
                    if self.key_has_prepared_intent(key.as_bytes()) {
                        return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                    }
                    key
                },
                value,
                options,
                clock.monotonic_nanos,
            ))),
            SharedNothingCommand::Del { key_bytes } => {
                if self.key_has_prepared_intent(key_bytes) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(del_response(
                    self.delete_bytes(key_bytes, clock.monotonic_nanos),
                ))
            }
            SharedNothingCommand::Incr { key_bytes } => {
                if self.key_has_prepared_intent(key_bytes) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(self.incr_bytes(key_bytes, clock.monotonic_nanos))
            }
            SharedNothingCommand::Exists { key_bytes } => {
                if self.key_has_prepared_intent(key_bytes) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(exists_response(
                    self.exists_bytes(key_bytes, clock.monotonic_nanos),
                ))
            }
            SharedNothingCommand::Ttl { key_bytes, unit } => {
                if self.key_has_prepared_intent(key_bytes) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(ttl_response(
                    self.ttl_bytes(key_bytes, clock.monotonic_nanos),
                    unit,
                    clock.monotonic_nanos,
                ))
            }
            SharedNothingCommand::Type { key_bytes } => {
                if self.key_has_prepared_intent(key_bytes) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(type_response(
                    self.type_bytes(key_bytes, clock.monotonic_nanos),
                ))
            }
        }
    }

    #[inline]
    fn execute_owned(
        &mut self,
        owner: OwnerId,
        command: OwnedSharedNothingCommand,
        clock: CommandClock,
    ) -> ExecutedCommand {
        self.assert_owner(owner);
        match command {
            OwnedSharedNothingCommand::Get { key } => {
                if self.key_has_prepared_intent(key.as_bytes()) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(self.get_bytes(key.as_bytes(), clock.monotonic_nanos))
            }
            OwnedSharedNothingCommand::SetPlain { key, value } => {
                if self.key_has_prepared_intent(key.as_bytes()) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                self.set_plain_owned(key, value);
                ExecutedCommand::from(CmdResult::Static(crate::commands::RESP_OK))
            }
            OwnedSharedNothingCommand::Set {
                key,
                value,
                options,
            } => ExecutedCommand::from(set_response(self.set_with_options(
                {
                    if self.key_has_prepared_intent(key.as_bytes()) {
                        return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                    }
                    key
                },
                value,
                options,
                clock.monotonic_nanos,
            ))),
            OwnedSharedNothingCommand::Del { key } => {
                if self.key_has_prepared_intent(key.as_bytes()) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(del_response(
                    self.delete_bytes(key.as_bytes(), clock.monotonic_nanos),
                ))
            }
            OwnedSharedNothingCommand::Incr { key } => {
                if self.key_has_prepared_intent(key.as_bytes()) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(self.incr_key(key, clock.monotonic_nanos))
            }
            OwnedSharedNothingCommand::Exists { key } => {
                if self.key_has_prepared_intent(key.as_bytes()) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(exists_response(
                    self.exists_bytes(key.as_bytes(), clock.monotonic_nanos),
                ))
            }
            OwnedSharedNothingCommand::Ttl { key, unit } => {
                if self.key_has_prepared_intent(key.as_bytes()) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(ttl_response(
                    self.ttl_bytes(key.as_bytes(), clock.monotonic_nanos),
                    unit,
                    clock.monotonic_nanos,
                ))
            }
            OwnedSharedNothingCommand::Type { key } => {
                if self.key_has_prepared_intent(key.as_bytes()) {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                ExecutedCommand::from(type_response(
                    self.type_bytes(key.as_bytes(), clock.monotonic_nanos),
                ))
            }
            OwnedSharedNothingCommand::Mget { keys } => {
                if keys
                    .iter()
                    .any(|key| self.key_has_prepared_intent(key.as_bytes()))
                {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                let mut values = Vec::with_capacity(keys.len());
                for key in keys.iter() {
                    values.push(self.mget_frame_for_bytes(key.as_bytes(), clock.monotonic_nanos));
                }
                ExecutedCommand::from(CmdResult::Resp(vortex_proto::RespFrame::Array(Some(
                    values,
                ))))
            }
            OwnedSharedNothingCommand::ExistsMany { keys } => {
                if keys
                    .iter()
                    .any(|key| self.key_has_prepared_intent(key.as_bytes()))
                {
                    return ExecutedCommand::from(CmdResult::Static(RESP_ERR_TXN_PENDING));
                }
                let mut count = 0i64;
                for key in keys.iter() {
                    count += i64::from(self.exists_bytes(key.as_bytes(), clock.monotonic_nanos));
                }
                ExecutedCommand::from(crate::commands::int_resp(count))
            }
            OwnedSharedNothingCommand::TxnPrepare { txn_id, intent } => {
                let outcome =
                    self.prepare_transaction(owner, txn_id, intent, clock.monotonic_nanos);
                ExecutedCommand::from(CmdResult::Resp(vortex_proto::RespFrame::Integer(
                    outcome.code(),
                )))
            }
            OwnedSharedNothingCommand::TxnCaptureSource {
                txn_id,
                kind,
                source,
            } => {
                let (outcome, capture) =
                    self.prepare_source_capture(owner, txn_id, kind, source, clock.monotonic_nanos);
                ExecutedCommand::from(source_capture_response(outcome, capture))
            }
            OwnedSharedNothingCommand::TxnPrepareLocalDual {
                txn_id,
                kind,
                source,
                destination,
            } => {
                let outcome = self.prepare_local_dual_transaction(
                    owner,
                    txn_id,
                    kind,
                    source,
                    destination,
                    clock.monotonic_nanos,
                );
                ExecutedCommand::from(CmdResult::Resp(vortex_proto::RespFrame::Integer(
                    outcome.code(),
                )))
            }
            OwnedSharedNothingCommand::TxnCommit { txn_id } => {
                let outcome = self.commit_transaction(owner, txn_id, clock.monotonic_nanos);
                ExecutedCommand::from(CmdResult::Resp(vortex_proto::RespFrame::Integer(
                    outcome.code(),
                )))
            }
            OwnedSharedNothingCommand::TxnAbort { txn_id } => {
                let outcome = self.abort_transaction(owner, txn_id);
                ExecutedCommand::from(CmdResult::Resp(vortex_proto::RespFrame::Integer(
                    outcome.code(),
                )))
            }
        }
    }

    #[inline]
    fn assert_owner(&self, owner: OwnerId) {
        debug_assert_eq!(
            self.owner, owner,
            "owner partition accessed through the wrong owner"
        );
    }

    #[inline]
    fn insert_with_ttl(
        &mut self,
        owner: OwnerId,
        key: VortexKey,
        value: VortexValue,
        ttl_deadline: u64,
    ) {
        self.assert_owner(owner);
        let _ = self.table.insert_with(key, value, ttl_deadline, None);
    }

    #[inline]
    fn set_plain_bytes_for_owner(&mut self, owner: OwnerId, key_bytes: &[u8], value_bytes: &[u8]) {
        self.assert_owner(owner);
        self.set_plain_bytes(key_bytes, value_bytes);
    }

    #[inline]
    fn set_plain_bytes(&mut self, key_bytes: &[u8], value_bytes: &[u8]) {
        let hash = self.table.table_hash_key_bytes(key_bytes);
        let _ = self.table.mutate_prehashed(
            BorrowedKey(key_bytes),
            RawValueBytes(value_bytes),
            hash,
            MutationPolicy::clear(None),
        );
    }

    #[inline]
    fn set_plain_bytes_storage_only(
        &mut self,
        owner: OwnerId,
        key_bytes: &[u8],
        value_bytes: &[u8],
    ) {
        self.assert_owner(owner);
        self.set_plain_bytes(key_bytes, value_bytes);
    }

    #[inline]
    fn set_plain_owned(&mut self, key: VortexKey, value: VortexValue) {
        let hash = self.table.table_hash_key_bytes(key.as_bytes());
        let _ = self
            .table
            .mutate_prehashed(key, value, hash, MutationPolicy::clear(None));
    }

    #[inline]
    fn set_owned_with_ttl(&mut self, key: VortexKey, value: VortexValue, ttl_deadline: u64) {
        let hash = self.table.table_hash_key_bytes(key.as_bytes());
        let _ = self
            .table
            .mutate_prehashed(key, value, hash, ttl_policy(ttl_deadline));
    }

    #[inline]
    fn borrowed_command_blocked(&self, command: &SharedNothingCommand<'_>) -> bool {
        match command {
            SharedNothingCommand::Get { key_bytes }
            | SharedNothingCommand::SetPlain { key_bytes, .. }
            | SharedNothingCommand::Del { key_bytes }
            | SharedNothingCommand::Incr { key_bytes }
            | SharedNothingCommand::Exists { key_bytes }
            | SharedNothingCommand::Ttl { key_bytes, .. }
            | SharedNothingCommand::Type { key_bytes } => self.key_has_prepared_intent(key_bytes),
            SharedNothingCommand::Set { key, .. } => self.key_has_prepared_intent(key.as_bytes()),
        }
    }

    #[inline]
    fn owned_command_blocked(&self, command: &OwnedSharedNothingCommand) -> bool {
        match command {
            OwnedSharedNothingCommand::Get { key }
            | OwnedSharedNothingCommand::SetPlain { key, .. }
            | OwnedSharedNothingCommand::Set { key, .. }
            | OwnedSharedNothingCommand::Del { key }
            | OwnedSharedNothingCommand::Incr { key }
            | OwnedSharedNothingCommand::Exists { key }
            | OwnedSharedNothingCommand::Ttl { key, .. }
            | OwnedSharedNothingCommand::Type { key } => {
                self.key_has_prepared_intent(key.as_bytes())
            }
            OwnedSharedNothingCommand::Mget { keys }
            | OwnedSharedNothingCommand::ExistsMany { keys } => keys
                .iter()
                .any(|key| self.key_has_prepared_intent(key.as_bytes())),
            OwnedSharedNothingCommand::TxnPrepare { intent, .. } => intent
                .actions()
                .iter()
                .any(|action| self.key_has_prepared_intent(action.key().as_bytes())),
            OwnedSharedNothingCommand::TxnCaptureSource { source, .. } => {
                self.key_has_prepared_intent(source.as_bytes())
            }
            OwnedSharedNothingCommand::TxnPrepareLocalDual {
                source,
                destination,
                ..
            } => {
                self.key_has_prepared_intent(source.as_bytes())
                    || self.key_has_prepared_intent(destination.as_bytes())
            }
            OwnedSharedNothingCommand::TxnCommit { .. }
            | OwnedSharedNothingCommand::TxnAbort { .. } => false,
        }
    }

    #[inline]
    fn get_bytes(&mut self, key_bytes: &[u8], now_nanos: u64) -> CmdResult {
        let hash = self.table.table_hash_key_bytes(key_bytes);
        match self.table.get_with_ttl_prehashed(key_bytes, hash) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => get_response(Some(value)),
            Some(_) => {
                self.remove_expired_bytes(key_bytes, hash, now_nanos);
                get_response(None)
            }
            None => get_response(None),
        }
    }

    #[inline]
    fn get_bytes_storage_only(&mut self, owner: OwnerId, key_bytes: &[u8], now_nanos: u64) -> bool {
        self.assert_owner(owner);
        let hash = self.table.table_hash_key_bytes(key_bytes);
        match self.table.get_with_ttl_prehashed(key_bytes, hash) {
            Some((_, ttl)) if ttl == 0 || ttl > now_nanos => true,
            Some(_) => {
                self.remove_expired_bytes(key_bytes, hash, now_nanos);
                false
            }
            None => false,
        }
    }

    #[inline]
    fn delete_bytes(&mut self, key_bytes: &[u8], now_nanos: u64) -> bool {
        let hash = self.table.table_hash_key_bytes(key_bytes);
        match self.table.slot_cursor_prehashed(key_bytes, hash, now_nanos) {
            SlotCursor::Live(live) => live.remove().is_some(),
            SlotCursor::Expired(expired) => {
                let _ = expired.remove();
                false
            }
            SlotCursor::Vacant(_) => false,
        }
    }

    #[inline]
    fn incr_bytes(&mut self, key_bytes: &[u8], now_nanos: u64) -> CmdResult {
        self.incr_key(VortexKey::from(key_bytes), now_nanos)
    }

    #[inline]
    fn incr_key(&mut self, key: VortexKey, now_nanos: u64) -> CmdResult {
        let hash = self.table.table_hash_key_bytes(key.as_bytes());
        match self
            .table
            .slot_cursor_prehashed(key.as_bytes(), hash, now_nanos)
        {
            SlotCursor::Live(live) => {
                let current = match live.value() {
                    VortexValue::Integer(number) => *number,
                    VortexValue::InlineString(inline) => match parse_i64(inline.as_bytes()) {
                        Some(number) => number,
                        None => return CmdResult::Static(ERR_NOT_INTEGER),
                    },
                    VortexValue::String(bytes) => match parse_i64(bytes.as_ref()) {
                        Some(number) => number,
                        None => return CmdResult::Static(ERR_NOT_INTEGER),
                    },
                    _ => return CmdResult::Static(ERR_WRONG_TYPE),
                };
                let Some(next) = current.checked_add(1) else {
                    return CmdResult::Static(ERR_OVERFLOW);
                };
                let _ = live.replace_value(
                    VortexValue::Integer(next),
                    MutationPolicy::preserve_ttl(None),
                );
                crate::commands::int_resp(next)
            }
            SlotCursor::Expired(expired) => {
                let _ = expired.remove();
                let _ = self.table.mutate_prehashed(
                    key,
                    VortexValue::Integer(1),
                    hash,
                    MutationPolicy::clear(None),
                );
                crate::commands::int_resp(1)
            }
            SlotCursor::Vacant(vacant) => {
                let _ = vacant.insert(key, VortexValue::Integer(1), MutationPolicy::clear(None));
                crate::commands::int_resp(1)
            }
        }
    }

    #[inline]
    fn exists_bytes(&mut self, key_bytes: &[u8], now_nanos: u64) -> bool {
        let hash = self.table.table_hash_key_bytes(key_bytes);
        match self.table.get_with_ttl_prehashed(key_bytes, hash) {
            Some((_, ttl)) if ttl == 0 || ttl > now_nanos => true,
            Some(_) => {
                self.remove_expired_bytes(key_bytes, hash, now_nanos);
                false
            }
            None => false,
        }
    }

    #[inline]
    fn ttl_bytes(&mut self, key_bytes: &[u8], now_nanos: u64) -> TtlState {
        let hash = self.table.table_hash_key_bytes(key_bytes);
        match self.table.get_with_ttl_prehashed(key_bytes, hash) {
            Some((_, 0)) => TtlState::Persistent,
            Some((_, deadline)) if deadline > now_nanos => TtlState::Deadline(deadline),
            Some(_) => {
                self.remove_expired_bytes(key_bytes, hash, now_nanos);
                TtlState::Missing
            }
            None => TtlState::Missing,
        }
    }

    #[inline]
    fn type_bytes(&mut self, key_bytes: &[u8], now_nanos: u64) -> Option<&'static str> {
        let hash = self.table.table_hash_key_bytes(key_bytes);
        match self.table.get_with_ttl_prehashed(key_bytes, hash) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value.type_name()),
            Some(_) => {
                self.remove_expired_bytes(key_bytes, hash, now_nanos);
                None
            }
            None => None,
        }
    }

    #[inline]
    fn mget_frame_for_bytes(
        &mut self,
        key_bytes: &[u8],
        now_nanos: u64,
    ) -> vortex_proto::RespFrame {
        let hash = self.table.table_hash_key_bytes(key_bytes);
        match self.table.get_with_ttl_prehashed(key_bytes, hash) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => match value {
                VortexValue::InlineString(inline) => {
                    vortex_proto::RespFrame::bulk_string(Bytes::copy_from_slice(inline.as_bytes()))
                }
                VortexValue::String(bytes) => vortex_proto::RespFrame::bulk_string(bytes.clone()),
                VortexValue::Integer(number) => {
                    let mut buffer = itoa::Buffer::new();
                    vortex_proto::RespFrame::bulk_string(Bytes::copy_from_slice(
                        buffer.format(*number).as_bytes(),
                    ))
                }
                _ => vortex_proto::RespFrame::null_bulk_string(),
            },
            Some(_) => {
                self.remove_expired_bytes(key_bytes, hash, now_nanos);
                vortex_proto::RespFrame::null_bulk_string()
            }
            None => vortex_proto::RespFrame::null_bulk_string(),
        }
    }

    #[inline]
    fn remove_expired_bytes(&mut self, key_bytes: &[u8], hash: TableHash, now_nanos: u64) {
        if let SlotCursor::Expired(expired) =
            self.table.slot_cursor_prehashed(key_bytes, hash, now_nanos)
        {
            let _ = expired.remove();
        }
    }

    fn set_with_options(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        options: SharedNothingSetOptions,
        now_nanos: u64,
    ) -> SharedNothingSetResult {
        let hash = self.table.table_hash_key_bytes(key.as_bytes());
        match self
            .table
            .slot_cursor_prehashed(key.as_bytes(), hash, now_nanos)
        {
            SlotCursor::Live(live) => set_live_with_options(live, value, options),
            SlotCursor::Expired(expired) => {
                let _ = expired.remove();
                set_absent_after_cursor_probe(&mut self.table, key, value, hash, options)
            }
            SlotCursor::Vacant(vacant) => {
                if options.xx {
                    if options.get {
                        SharedNothingSetResult::NotSetGet(None)
                    } else {
                        SharedNothingSetResult::NotSet
                    }
                } else {
                    let policy = ttl_policy_for_absent(options);
                    let _ = vacant.insert(key, value, policy);
                    if options.get {
                        SharedNothingSetResult::OkGet(None)
                    } else {
                        SharedNothingSetResult::Ok
                    }
                }
            }
        }
    }

    fn prepare_transaction(
        &mut self,
        owner: OwnerId,
        txn_id: TxnId,
        intent: TxnIntent,
        now_nanos: u64,
    ) -> TxnPrepareOutcome {
        self.assert_owner(owner);
        if self
            .prepared
            .iter()
            .any(|prepared| prepared.txn_id == txn_id)
        {
            return TxnPrepareOutcome::DuplicateTxn;
        }
        if self.intent_conflicts(&intent) {
            return TxnPrepareOutcome::Conflict;
        }
        if intent.kind() == TxnIntentKind::MsetNx
            && intent
                .actions()
                .iter()
                .any(|action| self.exists_bytes(action.key().as_bytes(), now_nanos))
        {
            return TxnPrepareOutcome::ConditionFailed;
        }
        for action in intent.actions() {
            match action {
                TxnAction::AssertExists { key } => {
                    if !self.exists_bytes(key.as_bytes(), now_nanos) {
                        return TxnPrepareOutcome::SourceMissing;
                    }
                }
                TxnAction::AssertAbsent { key } => {
                    if self.exists_bytes(key.as_bytes(), now_nanos) {
                        return TxnPrepareOutcome::DestinationExists;
                    }
                }
                TxnAction::Set { .. } | TxnAction::Delete { .. } => {}
            }
        }
        self.prepared.push(PreparedOwnerTxn { txn_id, intent });
        TxnPrepareOutcome::Prepared
    }

    fn prepare_source_capture(
        &mut self,
        owner: OwnerId,
        txn_id: TxnId,
        kind: TxnIntentKind,
        source: VortexKey,
        now_nanos: u64,
    ) -> (TxnPrepareOutcome, Option<CapturedSourceValue>) {
        self.assert_owner(owner);
        if self
            .prepared
            .iter()
            .any(|prepared| prepared.txn_id == txn_id)
        {
            return (TxnPrepareOutcome::DuplicateTxn, None);
        }
        let intent = TxnIntent::from_actions(
            kind,
            source_actions(kind, source.clone()).into_boxed_slice(),
        );
        if self.intent_conflicts(&intent) {
            return (TxnPrepareOutcome::Conflict, None);
        }
        let Some(capture) = self.capture_live_value(&source, now_nanos) else {
            return (TxnPrepareOutcome::SourceMissing, None);
        };
        self.prepared.push(PreparedOwnerTxn { txn_id, intent });
        (TxnPrepareOutcome::Prepared, Some(capture))
    }

    fn prepare_local_dual_transaction(
        &mut self,
        owner: OwnerId,
        txn_id: TxnId,
        kind: TxnIntentKind,
        source: VortexKey,
        destination: VortexKey,
        now_nanos: u64,
    ) -> TxnPrepareOutcome {
        self.assert_owner(owner);
        if self
            .prepared
            .iter()
            .any(|prepared| prepared.txn_id == txn_id)
        {
            return TxnPrepareOutcome::DuplicateTxn;
        }
        let preflight = TxnIntent::from_actions(
            kind,
            local_dual_preflight_actions(kind, source.clone(), destination.clone())
                .into_boxed_slice(),
        );
        if self.intent_conflicts(&preflight) {
            return TxnPrepareOutcome::Conflict;
        }
        let Some(capture) = self.capture_live_value(&source, now_nanos) else {
            return TxnPrepareOutcome::SourceMissing;
        };
        if source == destination {
            match kind {
                TxnIntentKind::Rename | TxnIntentKind::CopyReplace => {
                    self.prepared.push(PreparedOwnerTxn {
                        txn_id,
                        intent: TxnIntent::from_actions(
                            kind,
                            vec![TxnAction::AssertExists { key: source }].into_boxed_slice(),
                        ),
                    });
                    return TxnPrepareOutcome::Prepared;
                }
                TxnIntentKind::RenameNx | TxnIntentKind::Copy => {
                    return TxnPrepareOutcome::DestinationExists;
                }
                TxnIntentKind::Mset | TxnIntentKind::MsetNx => {
                    return TxnPrepareOutcome::Conflict;
                }
            }
        }
        if destination_must_be_absent(kind) && self.exists_bytes(destination.as_bytes(), now_nanos)
        {
            return TxnPrepareOutcome::DestinationExists;
        }
        let intent = TxnIntent::from_actions(
            kind,
            local_dual_actions(kind, source, destination, capture).into_boxed_slice(),
        );
        self.prepared.push(PreparedOwnerTxn { txn_id, intent });
        TxnPrepareOutcome::Prepared
    }

    fn commit_transaction(
        &mut self,
        owner: OwnerId,
        txn_id: TxnId,
        now_nanos: u64,
    ) -> TxnFinishOutcome {
        self.assert_owner(owner);
        let Some(index) = self
            .prepared
            .iter()
            .position(|prepared| prepared.txn_id == txn_id)
        else {
            return TxnFinishOutcome::Missing;
        };
        let prepared = self.prepared.swap_remove(index);
        for action in prepared.intent.into_actions().into_vec() {
            match action {
                TxnAction::Set {
                    key,
                    value,
                    ttl_deadline,
                } => self.set_owned_with_ttl(key, value, ttl_deadline),
                TxnAction::Delete { key } => {
                    let _ = self.delete_bytes(key.as_bytes(), now_nanos);
                }
                TxnAction::AssertExists { .. } | TxnAction::AssertAbsent { .. } => {}
            }
        }
        TxnFinishOutcome::Finished
    }

    fn abort_transaction(&mut self, owner: OwnerId, txn_id: TxnId) -> TxnFinishOutcome {
        self.assert_owner(owner);
        let Some(index) = self
            .prepared
            .iter()
            .position(|prepared| prepared.txn_id == txn_id)
        else {
            return TxnFinishOutcome::Missing;
        };
        self.prepared.swap_remove(index);
        TxnFinishOutcome::Finished
    }

    fn capture_live_value(
        &mut self,
        key: &VortexKey,
        now_nanos: u64,
    ) -> Option<CapturedSourceValue> {
        let hash = self.table.table_hash_key_bytes(key.as_bytes());
        match self.table.get_with_ttl_prehashed(key.as_bytes(), hash) {
            Some((value, ttl_deadline)) if ttl_deadline == 0 || ttl_deadline > now_nanos => {
                Some(CapturedSourceValue {
                    value: value.clone(),
                    ttl_deadline,
                })
            }
            Some(_) => {
                self.remove_expired_bytes(key.as_bytes(), hash, now_nanos);
                None
            }
            None => None,
        }
    }

    #[inline]
    fn intent_conflicts(&self, incoming: &TxnIntent) -> bool {
        self.prepared.iter().any(|prepared| {
            prepared.intent.overlaps(incoming)
                && (prepared.intent.kind() == TxnIntentKind::MsetNx
                    || incoming.kind() == TxnIntentKind::MsetNx
                    || prepared.intent.kind().is_dual_key()
                    || incoming.kind().is_dual_key())
        })
    }

    #[inline]
    fn key_has_prepared_intent(&self, key: &[u8]) -> bool {
        self.prepared
            .iter()
            .any(|prepared| prepared.intent.touches_key(key))
    }
}

/// Owner-local execution state used by the nonblocking `vortex-io` reactor mode.
#[doc(hidden)]
pub struct SharedNothingOwnerRuntime {
    local_owner: OwnerId,
    topology: OwnerTopology,
    partition: OwnerExecutorPartition,
}

impl SharedNothingOwnerRuntime {
    /// Creates an owner runtime for one reactor.
    ///
    /// # Errors
    ///
    /// Returns topology validation errors when the owner index is not valid for
    /// the provided topology config.
    pub fn new(
        config: TopologyConfig,
        local_owner_index: usize,
        capacity_per_owner: usize,
    ) -> Result<Self, TopologyConfigError> {
        let topology = OwnerTopology::new(config);
        let local_owner = topology.owner_id(local_owner_index).ok_or(
            TopologyConfigError::OwnerCountTooLarge {
                owner_count: local_owner_index,
            },
        )?;
        Ok(Self {
            local_owner,
            topology,
            partition: OwnerExecutorPartition::with_capacity(local_owner, capacity_per_owner),
        })
    }

    /// Returns the owner assigned to this runtime.
    #[inline]
    pub const fn local_owner(&self) -> OwnerId {
        self.local_owner
    }

    /// Returns the topology config used by this runtime.
    #[inline]
    pub const fn topology_config(&self) -> TopologyConfig {
        self.topology.config()
    }

    /// Routes and executes, or prepares for remote execution, one command.
    #[inline]
    pub fn dispatch_ingress(
        &mut self,
        name: &[u8],
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
    ) -> SharedNothingOwnerDispatch {
        if name == b"GET" {
            return self.dispatch_get_fast(frame, clock);
        }
        if name == b"SET" && frame.element_count() == Some(3) {
            return self.dispatch_set_plain_fast(frame, clock);
        }

        let parsed = match parse_shared_nothing_command(name, frame, clock) {
            Some(SharedNothingParse::Command(command)) => command,
            Some(SharedNothingParse::Immediate(command)) => {
                return SharedNothingOwnerDispatch::Immediate(command);
            }
            Some(SharedNothingParse::Unsupported) | None => {
                return SharedNothingOwnerDispatch::Unsupported;
            }
        };

        let route = self
            .topology
            .route_key_with_debug(self.local_owner, parsed.key_bytes());
        if route.is_local_to(self.local_owner) {
            if self.partition.borrowed_command_blocked(&parsed) {
                return SharedNothingOwnerDispatch::Blocked;
            }
            let command = self
                .partition
                .execute_borrowed(route.owner(), parsed, clock);
            SharedNothingOwnerDispatch::Ready {
                owner: route.owner(),
                command,
            }
        } else {
            SharedNothingOwnerDispatch::Remote {
                owner: route.owner(),
                command: parsed.into_owned(),
            }
        }
    }

    /// Executes a command received from another owner reactor.
    #[inline]
    pub fn execute_remote_command(
        &mut self,
        owner: OwnerId,
        command: OwnedSharedNothingCommand,
        clock: CommandClock,
    ) -> ExecutedCommand {
        self.partition.execute_owned(owner, command, clock)
    }

    /// Returns true when a remote-owner command currently touches a prepared
    /// key and must wait for the prepare/commit/abort sequence to finish.
    #[inline]
    pub fn remote_command_blocked(
        &self,
        owner: OwnerId,
        command: &OwnedSharedNothingCommand,
    ) -> bool {
        self.partition.assert_owner(owner);
        self.partition.owned_command_blocked(command)
    }

    /// Returns true when any routed owner-local key is currently prepared.
    #[inline]
    pub fn keys_blocked<'a>(
        &self,
        owner: OwnerId,
        keys: impl IntoIterator<Item = &'a VortexKey>,
    ) -> bool {
        self.partition.assert_owner(owner);
        keys.into_iter()
            .any(|key| self.partition.key_has_prepared_intent(key.as_bytes()))
    }

    /// Returns routing debug counters.
    #[inline]
    pub fn routing_debug_counters(&self) -> crate::owner::RoutingDebugSnapshot {
        self.topology.routing_debug_counters()
    }

    /// Routes one key from this ingress owner and updates routing debug counters.
    #[inline]
    pub fn route_owner_with_debug(&self, key: &[u8]) -> OwnerId {
        self.topology
            .route_key_with_debug(self.local_owner, key)
            .owner()
    }

    #[inline]
    fn dispatch_get_fast(
        &mut self,
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
    ) -> SharedNothingOwnerDispatch {
        let Some(key_bytes) = arg_bytes(frame, 1) else {
            return SharedNothingOwnerDispatch::Immediate(ExecutedCommand::from(
                CmdResult::Static(RESP_NIL),
            ));
        };

        let route = self
            .topology
            .route_key_with_debug(self.local_owner, key_bytes);
        if route.is_local_to(self.local_owner) {
            if self.partition.key_has_prepared_intent(key_bytes) {
                return SharedNothingOwnerDispatch::Blocked;
            }
            let command = self.partition.execute_borrowed(
                route.owner(),
                SharedNothingCommand::Get { key_bytes },
                clock,
            );
            SharedNothingOwnerDispatch::Ready {
                owner: route.owner(),
                command,
            }
        } else {
            SharedNothingOwnerDispatch::Remote {
                owner: route.owner(),
                command: OwnedSharedNothingCommand::Get {
                    key: key_bytes.into(),
                },
            }
        }
    }

    #[inline]
    fn dispatch_set_plain_fast(
        &mut self,
        frame: &vortex_proto::FrameRef<'_>,
        _clock: CommandClock,
    ) -> SharedNothingOwnerDispatch {
        let (Some(key_bytes), Some(value_bytes)) = (arg_bytes(frame, 1), arg_bytes(frame, 2))
        else {
            return SharedNothingOwnerDispatch::Immediate(ExecutedCommand::from(
                CmdResult::Static(ERR_SYNTAX),
            ));
        };

        let route = self
            .topology
            .route_key_with_debug(self.local_owner, key_bytes);
        if route.is_local_to(self.local_owner) {
            if self.partition.key_has_prepared_intent(key_bytes) {
                return SharedNothingOwnerDispatch::Blocked;
            }
            self.partition
                .set_plain_bytes_for_owner(route.owner(), key_bytes, value_bytes);
            SharedNothingOwnerDispatch::Ready {
                owner: route.owner(),
                command: ExecutedCommand::from(CmdResult::Static(crate::commands::RESP_OK)),
            }
        } else {
            SharedNothingOwnerDispatch::Remote {
                owner: route.owner(),
                command: OwnedSharedNothingCommand::SetPlain {
                    key: key_bytes.into(),
                    value: VortexValue::from_bytes(value_bytes),
                },
            }
        }
    }
}

/// Hidden SN-004A benchmark harness for decomposing executor adapter cost.
///
/// The harness deliberately lives in the owner module so Criterion can measure
/// private adapter phases without exposing owner internals to command handlers
/// or `vortex-io`.
#[doc(hidden)]
pub struct SharedNothingExecutorCostProbe {
    local_owner: OwnerId,
    topology: OwnerTopology,
    local_partition: OwnerExecutorPartition,
}

impl SharedNothingExecutorCostProbe {
    /// Creates a cost probe with one local owner partition and no remote
    /// workers.
    ///
    /// # Errors
    ///
    /// Returns topology validation errors when owner or capsule counts are
    /// invalid.
    pub fn new(
        owner_count: usize,
        capsule_count: usize,
        capacity_per_owner: usize,
        local_owner_index: usize,
    ) -> Result<Self, TopologyConfigError> {
        let config = TopologyConfig::new(owner_count, capsule_count)?;
        let topology = OwnerTopology::new(config);
        let local_owner = topology.owner_id(local_owner_index).ok_or(
            TopologyConfigError::OwnerCountTooLarge {
                owner_count: local_owner_index,
            },
        )?;
        Ok(Self {
            local_owner,
            topology,
            local_partition: OwnerExecutorPartition::with_capacity(local_owner, capacity_per_owner),
        })
    }

    /// Routes bytes through the same topology as the shared-nothing executor.
    #[inline]
    pub fn route_owner_bytes(&self, key: &[u8]) -> OwnerId {
        self.topology.route_key(key).owner()
    }

    /// Seeds one local key. The key must route to the configured local owner.
    #[inline]
    pub fn insert_local(&mut self, key: VortexKey, value: VortexValue) {
        debug_assert!(
            self.topology
                .route_key(key.as_bytes())
                .is_local_to(self.local_owner)
        );
        self.local_partition
            .insert_with_ttl(self.local_owner, key, value, 0);
    }

    /// Parses one SN-004 command and returns only a coarse result kind.
    #[inline]
    pub fn parse_kind(
        &self,
        name: &[u8],
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
    ) -> SharedNothingParseCostKind {
        match parse_shared_nothing_command(name, frame, clock) {
            Some(SharedNothingParse::Command(_)) => SharedNothingParseCostKind::Command,
            Some(SharedNothingParse::Immediate(_)) => SharedNothingParseCostKind::Immediate,
            Some(SharedNothingParse::Unsupported) => SharedNothingParseCostKind::Unsupported,
            None => SharedNothingParseCostKind::NotSharedNothingCommand,
        }
    }

    /// Executes only owner-local GET storage logic without response shaping.
    #[inline]
    pub fn storage_get_hit(&mut self, key_bytes: &[u8], now_nanos: u64) -> bool {
        self.local_partition
            .get_bytes_storage_only(self.local_owner, key_bytes, now_nanos)
    }

    /// Executes only owner-local SET replacement storage logic without
    /// response shaping.
    #[inline]
    pub fn storage_set_plain(&mut self, key_bytes: &[u8], value_bytes: &[u8]) {
        self.local_partition
            .set_plain_bytes_storage_only(self.local_owner, key_bytes, value_bytes);
    }

    /// Executes a pre-parsed local GET through the owner partition.
    #[inline]
    pub fn execute_preparsed_get(
        &mut self,
        key_bytes: &[u8],
        clock: CommandClock,
    ) -> ExecutedCommand {
        self.local_partition.execute_borrowed(
            self.local_owner,
            SharedNothingCommand::Get { key_bytes },
            clock,
        )
    }

    /// Routes a pre-parsed local GET and executes it through the owner
    /// partition.
    #[inline]
    pub fn execute_routed_preparsed_get(
        &mut self,
        key_bytes: &[u8],
        clock: CommandClock,
    ) -> ExecutedCommand {
        let route = self.topology.route_key(key_bytes);
        debug_assert!(route.is_local_to(self.local_owner));
        self.local_partition.execute_borrowed(
            self.local_owner,
            SharedNothingCommand::Get { key_bytes },
            clock,
        )
    }

    /// Parses and executes a local GET/SET frame without routing.
    #[inline]
    pub fn execute_parsed_no_route(
        &mut self,
        name: &[u8],
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
    ) -> Option<ExecutedCommand> {
        match parse_shared_nothing_command(name, frame, clock)? {
            SharedNothingParse::Command(command) => Some(self.local_partition.execute_borrowed(
                self.local_owner,
                command,
                clock,
            )),
            SharedNothingParse::Immediate(command) => Some(command),
            SharedNothingParse::Unsupported => None,
        }
    }

    /// Executes a pre-parsed local SET without routing or response parsing.
    #[inline]
    pub fn execute_preparsed_set_plain(
        &mut self,
        key_bytes: &[u8],
        value_bytes: &[u8],
        clock: CommandClock,
    ) -> ExecutedCommand {
        self.local_partition.execute_borrowed(
            self.local_owner,
            SharedNothingCommand::SetPlain {
                key_bytes,
                value_bytes,
            },
            clock,
        )
    }

    /// Routes a pre-parsed local SET and executes it through the owner
    /// partition.
    #[inline]
    pub fn execute_routed_preparsed_set_plain(
        &mut self,
        key_bytes: &[u8],
        value_bytes: &[u8],
        clock: CommandClock,
    ) -> ExecutedCommand {
        let route = self.topology.route_key(key_bytes);
        debug_assert!(route.is_local_to(self.local_owner));
        self.execute_preparsed_set_plain(key_bytes, value_bytes, clock)
    }
}

fn set_live_with_options(
    live: crate::table::LiveSlotCursor<'_>,
    value: VortexValue,
    options: SharedNothingSetOptions,
) -> SharedNothingSetResult {
    if options.nx {
        return if options.get {
            SharedNothingSetResult::NotSetGet(Some(live.cloned_value()))
        } else {
            SharedNothingSetResult::NotSet
        };
    }

    let effective_ttl = if options.keepttl {
        live.ttl_deadline()
    } else {
        options.ttl_deadline
    };
    let policy = ttl_policy(effective_ttl);
    let mut report = live.replace_value(value, policy);
    if options.get {
        SharedNothingSetResult::OkGet(report.take_previous())
    } else {
        SharedNothingSetResult::Ok
    }
}

fn set_absent_after_cursor_probe(
    table: &mut SwissTable,
    key: VortexKey,
    value: VortexValue,
    hash: TableHash,
    options: SharedNothingSetOptions,
) -> SharedNothingSetResult {
    if options.xx {
        return if options.get {
            SharedNothingSetResult::NotSetGet(None)
        } else {
            SharedNothingSetResult::NotSet
        };
    }

    let _ = table.mutate_prehashed(key, value, hash, ttl_policy_for_absent(options));
    if options.get {
        SharedNothingSetResult::OkGet(None)
    } else {
        SharedNothingSetResult::Ok
    }
}

#[inline]
fn ttl_policy_for_absent(options: SharedNothingSetOptions) -> MutationPolicy {
    if options.keepttl {
        MutationPolicy::clear(None)
    } else {
        ttl_policy(options.ttl_deadline)
    }
}

#[inline]
fn ttl_policy(ttl_deadline: u64) -> MutationPolicy {
    if ttl_deadline == 0 {
        MutationPolicy::clear(None)
    } else {
        MutationPolicy::set(ttl_deadline, None)
    }
}

fn source_capture_response(
    outcome: TxnPrepareOutcome,
    capture: Option<CapturedSourceValue>,
) -> CmdResult {
    let mut frames = Vec::with_capacity(3);
    frames.push(vortex_proto::RespFrame::Integer(outcome.code()));
    if let Some(capture) = capture {
        frames.push(value_to_bulk_frame(&capture.value));
        let mut buffer = itoa::Buffer::new();
        frames.push(vortex_proto::RespFrame::bulk_string(
            Bytes::copy_from_slice(buffer.format(capture.ttl_deadline).as_bytes()),
        ));
    }
    CmdResult::Resp(vortex_proto::RespFrame::Array(Some(frames)))
}

fn value_to_bulk_frame(value: &VortexValue) -> vortex_proto::RespFrame {
    match value {
        VortexValue::InlineString(inline) => {
            vortex_proto::RespFrame::bulk_string(Bytes::copy_from_slice(inline.as_bytes()))
        }
        VortexValue::String(bytes) => vortex_proto::RespFrame::bulk_string(bytes.clone()),
        VortexValue::Integer(number) => {
            let mut buffer = itoa::Buffer::new();
            vortex_proto::RespFrame::bulk_string(Bytes::copy_from_slice(
                buffer.format(*number).as_bytes(),
            ))
        }
        _ => vortex_proto::RespFrame::null_bulk_string(),
    }
}

#[inline]
fn destination_must_be_absent(kind: TxnIntentKind) -> bool {
    matches!(kind, TxnIntentKind::RenameNx | TxnIntentKind::Copy)
}

fn source_actions(kind: TxnIntentKind, source: VortexKey) -> Vec<TxnAction> {
    let mut actions = Vec::with_capacity(2);
    actions.push(TxnAction::AssertExists {
        key: source.clone(),
    });
    if kind.deletes_source() {
        actions.push(TxnAction::Delete { key: source });
    }
    actions
}

fn local_dual_preflight_actions(
    kind: TxnIntentKind,
    source: VortexKey,
    destination: VortexKey,
) -> Vec<TxnAction> {
    let mut actions = Vec::with_capacity(2);
    actions.push(TxnAction::AssertExists { key: source });
    if destination_must_be_absent(kind) {
        actions.push(TxnAction::AssertAbsent { key: destination });
    } else {
        actions.push(TxnAction::AssertExists { key: destination });
    }
    actions
}

fn local_dual_actions(
    kind: TxnIntentKind,
    source: VortexKey,
    destination: VortexKey,
    capture: CapturedSourceValue,
) -> Vec<TxnAction> {
    let mut actions = Vec::with_capacity(4);
    actions.push(TxnAction::AssertExists {
        key: source.clone(),
    });
    if destination_must_be_absent(kind) {
        actions.push(TxnAction::AssertAbsent {
            key: destination.clone(),
        });
    }
    if kind.deletes_source() {
        actions.push(TxnAction::Delete { key: source });
    }
    actions.push(TxnAction::Set {
        key: destination,
        value: capture.value,
        ttl_deadline: capture.ttl_deadline,
    });
    actions
}

enum OwnerExecutorRequest {
    Execute {
        command: OwnedSharedNothingCommand,
        clock: CommandClock,
        continuation: RemoteContinuation,
        enqueued_at: Option<Instant>,
    },
    Seed {
        key: VortexKey,
        value: VortexValue,
        ttl_deadline: u64,
    },
    Shutdown,
}

struct OwnerExecutorReply {
    continuation: RemoteContinuation,
    command: ExecutedCommand,
    queue_wait: Duration,
}

struct OwnerExecutorWorker<const N: usize> {
    owner: OwnerId,
    request_tx: SpscSender<OwnerExecutorRequest, N>,
    reply_rx: SpscReceiver<OwnerExecutorReply, N>,
    join: Option<JoinHandle<()>>,
}

/// Shared-nothing command executor adapter for SN-004.
pub struct SharedNothingExecutor<const N: usize> {
    local_owner: OwnerId,
    topology: OwnerTopology,
    local_partition: OwnerExecutorPartition,
    workers: Vec<Option<OwnerExecutorWorker<N>>>,
    next_request_id: u64,
}

impl<const N: usize> SharedNothingExecutor<N> {
    /// Starts an executor with one local owner partition and worker threads for
    /// all non-local owners.
    ///
    /// # Errors
    ///
    /// Returns topology validation errors when owner or capsule counts are
    /// invalid.
    pub fn start(
        owner_count: usize,
        capsule_count: usize,
        capacity_per_owner: usize,
        local_owner_index: usize,
    ) -> Result<Self, TopologyConfigError> {
        let config = TopologyConfig::new(owner_count, capsule_count)?;
        let topology = OwnerTopology::new(config);
        let local_owner = topology.owner_id(local_owner_index).ok_or(
            TopologyConfigError::OwnerCountTooLarge {
                owner_count: local_owner_index,
            },
        )?;
        let local_partition =
            OwnerExecutorPartition::with_capacity(local_owner, capacity_per_owner);
        let mut workers = Vec::with_capacity(owner_count);

        for owner_index in 0..owner_count {
            let owner = OwnerId::from_validated_index(owner_index);
            if owner == local_owner {
                workers.push(None);
                continue;
            }

            let (request_tx, request_rx) = spsc_channel();
            let (reply_tx, reply_rx) = spsc_channel();
            let partition = OwnerExecutorPartition::with_capacity(owner, capacity_per_owner);
            let join = thread::Builder::new()
                .name(format!("vortex-sn-executor-owner-{owner_index}"))
                .spawn(move || owner_executor_worker_loop(owner, partition, request_rx, reply_tx))
                .expect("shared-nothing owner executor worker spawned");
            workers.push(Some(OwnerExecutorWorker {
                owner,
                request_tx,
                reply_rx,
                join: Some(join),
            }));
        }

        Ok(Self {
            local_owner,
            topology,
            local_partition,
            workers,
            next_request_id: 0,
        })
    }

    /// Returns the local owner ID.
    #[inline]
    pub const fn local_owner(&self) -> OwnerId {
        self.local_owner
    }

    /// Routes `key` to an owner.
    #[inline]
    pub fn route_owner_bytes(&self, key: &[u8]) -> OwnerId {
        self.topology.route_key(key).owner()
    }

    /// Returns routing debug counters.
    #[inline]
    pub fn routing_debug_counters(&self) -> crate::owner::RoutingDebugSnapshot {
        self.topology.routing_debug_counters()
    }

    /// Seeds one key into its routed owner without parsing RESP.
    pub fn insert_routed(&mut self, key: VortexKey, value: VortexValue) {
        self.insert_routed_with_ttl(key, value, 0);
    }

    /// Seeds one key with an explicit TTL deadline.
    pub fn insert_routed_with_ttl(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        ttl_deadline: u64,
    ) {
        let route = self.topology.route_key(key.as_bytes());
        if route.is_local_to(self.local_owner) {
            self.local_partition
                .insert_with_ttl(route.owner(), key, value, ttl_deadline);
            return;
        }

        let worker = self.remote_worker(route.owner());
        let mut request = OwnerExecutorRequest::Seed {
            key,
            value,
            ttl_deadline,
        };
        loop {
            match worker.request_tx.try_send(request) {
                Ok(()) => return,
                Err(rejected) => {
                    request = rejected;
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Executes one SN-004 command without a connection-generation guard.
    #[inline]
    pub fn execute(
        &mut self,
        name: &[u8],
        frame: &vortex_proto::FrameRef<'_>,
        clock: impl Into<CommandClock>,
    ) -> Option<ExecutedCommand> {
        self.execute_with_generation(
            SharedNothingConnectionToken::synthetic(),
            name,
            frame,
            clock,
            |_| SharedNothingConnectionGeneration::INITIAL,
        )
        .into_command()
    }

    /// Executes one command with remote queue-wait timing enabled.
    pub fn execute_timed(
        &mut self,
        name: &[u8],
        frame: &vortex_proto::FrameRef<'_>,
        clock: impl Into<CommandClock>,
    ) -> SharedNothingExecutionResult {
        self.execute_with_generation_inner(
            SharedNothingConnectionToken::synthetic(),
            name,
            frame,
            clock,
            |_| SharedNothingConnectionGeneration::INITIAL,
            true,
        )
    }

    /// Executes one SN-004 command and validates the connection generation
    /// immediately before returning a response.
    #[inline]
    pub fn execute_with_generation<F>(
        &mut self,
        connection: SharedNothingConnectionToken,
        name: &[u8],
        frame: &vortex_proto::FrameRef<'_>,
        clock: impl Into<CommandClock>,
        current_generation: F,
    ) -> SharedNothingExecutionResult
    where
        F: FnOnce(SharedNothingConnectionId) -> SharedNothingConnectionGeneration,
    {
        self.execute_with_generation_inner(
            connection,
            name,
            frame,
            clock,
            current_generation,
            false,
        )
    }

    #[inline]
    fn execute_with_generation_inner<F>(
        &mut self,
        connection: SharedNothingConnectionToken,
        name: &[u8],
        frame: &vortex_proto::FrameRef<'_>,
        clock: impl Into<CommandClock>,
        current_generation: F,
        timed: bool,
    ) -> SharedNothingExecutionResult
    where
        F: FnOnce(SharedNothingConnectionId) -> SharedNothingConnectionGeneration,
    {
        let clock = clock.into();
        if name == b"GET" {
            return self.execute_get_fast(connection, frame, clock, current_generation, timed);
        }
        if name == b"SET" && frame.element_count() == Some(3) {
            return self.execute_set_plain_fast(
                connection,
                frame,
                clock,
                current_generation,
                timed,
            );
        }

        let parsed = match parse_shared_nothing_command(name, frame, clock) {
            Some(SharedNothingParse::Command(command)) => command,
            Some(SharedNothingParse::Immediate(command)) => {
                return Self::complete_local_response(
                    self.local_owner,
                    connection,
                    current_generation(connection.id()),
                    command,
                );
            }
            Some(SharedNothingParse::Unsupported) | None => {
                return SharedNothingExecutionResult::Unsupported;
            }
        };

        let route = self.topology.route_key(parsed.key_bytes());
        if route.is_local_to(self.local_owner) {
            let command = self
                .local_partition
                .execute_borrowed(route.owner(), parsed, clock);
            return Self::complete_local_response(
                route.owner(),
                connection,
                current_generation(connection.id()),
                command,
            );
        }

        let continuation = self.next_continuation(route, connection);
        let reply = self.execute_remote(
            route.owner(),
            parsed.into_owned(),
            clock,
            continuation,
            timed,
        );
        Self::complete_remote_response(
            reply.continuation,
            current_generation(connection.id()),
            reply.command,
            reply.queue_wait,
        )
    }

    #[inline]
    fn execute_get_fast<F>(
        &mut self,
        connection: SharedNothingConnectionToken,
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
        current_generation: F,
        timed: bool,
    ) -> SharedNothingExecutionResult
    where
        F: FnOnce(SharedNothingConnectionId) -> SharedNothingConnectionGeneration,
    {
        let Some(key_bytes) = arg_bytes(frame, 1) else {
            return Self::complete_local_response(
                self.local_owner,
                connection,
                current_generation(connection.id()),
                ExecutedCommand::from(CmdResult::Static(RESP_NIL)),
            );
        };

        let route = self.topology.route_key(key_bytes);
        if route.is_local_to(self.local_owner) {
            let command = self.local_partition.execute_borrowed(
                route.owner(),
                SharedNothingCommand::Get { key_bytes },
                clock,
            );
            return Self::complete_local_response(
                route.owner(),
                connection,
                current_generation(connection.id()),
                command,
            );
        }

        let continuation = self.next_continuation(route, connection);
        let reply = self.execute_remote(
            route.owner(),
            OwnedSharedNothingCommand::Get {
                key: key_bytes.into(),
            },
            clock,
            continuation,
            timed,
        );
        Self::complete_remote_response(
            reply.continuation,
            current_generation(connection.id()),
            reply.command,
            reply.queue_wait,
        )
    }

    #[inline]
    fn execute_set_plain_fast<F>(
        &mut self,
        connection: SharedNothingConnectionToken,
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
        current_generation: F,
        timed: bool,
    ) -> SharedNothingExecutionResult
    where
        F: FnOnce(SharedNothingConnectionId) -> SharedNothingConnectionGeneration,
    {
        let (Some(key_bytes), Some(value_bytes)) = (arg_bytes(frame, 1), arg_bytes(frame, 2))
        else {
            return Self::complete_local_response(
                self.local_owner,
                connection,
                current_generation(connection.id()),
                ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
            );
        };

        let route = self.topology.route_key(key_bytes);
        if route.is_local_to(self.local_owner) {
            self.local_partition
                .set_plain_bytes_for_owner(route.owner(), key_bytes, value_bytes);
            return Self::complete_local_response(
                route.owner(),
                connection,
                current_generation(connection.id()),
                ExecutedCommand::from(CmdResult::Static(crate::commands::RESP_OK)),
            );
        }

        let continuation = self.next_continuation(route, connection);
        let reply = self.execute_remote(
            route.owner(),
            OwnedSharedNothingCommand::SetPlain {
                key: key_bytes.into(),
                value: VortexValue::from_bytes(value_bytes),
            },
            clock,
            continuation,
            timed,
        );
        Self::complete_remote_response(
            reply.continuation,
            current_generation(connection.id()),
            reply.command,
            reply.queue_wait,
        )
    }

    /// Applies the generation guard for a remote owner reply.
    pub fn complete_remote_response(
        continuation: RemoteContinuation,
        current_generation: SharedNothingConnectionGeneration,
        command: ExecutedCommand,
        queue_wait: Duration,
    ) -> SharedNothingExecutionResult {
        if continuation.connection().is_current(current_generation) {
            SharedNothingExecutionResult::Ready {
                owner: continuation.target_owner(),
                queue_wait,
                command,
            }
        } else {
            drop(command);
            SharedNothingExecutionResult::StaleConnection { continuation }
        }
    }

    #[inline]
    fn complete_local_response(
        owner: OwnerId,
        connection: SharedNothingConnectionToken,
        current_generation: SharedNothingConnectionGeneration,
        command: ExecutedCommand,
    ) -> SharedNothingExecutionResult {
        if connection.is_current(current_generation) {
            SharedNothingExecutionResult::Ready {
                owner,
                queue_wait: Duration::ZERO,
                command,
            }
        } else {
            SharedNothingExecutionResult::StaleConnection {
                continuation: RemoteContinuation::new(0, owner, owner, connection),
            }
        }
    }

    #[inline]
    fn next_continuation(
        &mut self,
        route: KeyRoute,
        connection: SharedNothingConnectionToken,
    ) -> RemoteContinuation {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        RemoteContinuation::new(request_id, self.local_owner, route.owner(), connection)
    }

    fn execute_remote(
        &self,
        owner: OwnerId,
        command: OwnedSharedNothingCommand,
        clock: CommandClock,
        continuation: RemoteContinuation,
        timed: bool,
    ) -> OwnerExecutorReply {
        let worker = self.remote_worker(owner);
        debug_assert_eq!(worker.owner, owner);
        let mut request = OwnerExecutorRequest::Execute {
            command,
            clock,
            continuation,
            enqueued_at: timed.then(Instant::now),
        };

        loop {
            match worker.request_tx.try_send(request) {
                Ok(()) => break,
                Err(rejected) => {
                    request = rejected;
                    std::hint::spin_loop();
                }
            }
        }

        loop {
            if let Some(reply) = worker.reply_rx.try_recv() {
                return reply;
            }
            std::hint::spin_loop();
        }
    }

    #[inline]
    fn remote_worker(&self, owner: OwnerId) -> &OwnerExecutorWorker<N> {
        self.workers[owner.get()]
            .as_ref()
            .expect("remote route must target a worker")
    }
}

impl<const N: usize> Drop for SharedNothingExecutor<N> {
    fn drop(&mut self) {
        for worker in self.workers.iter().flatten() {
            let mut request = OwnerExecutorRequest::Shutdown;
            loop {
                match worker.request_tx.try_send(request) {
                    Ok(()) => break,
                    Err(rejected) => {
                        request = rejected;
                        std::hint::spin_loop();
                    }
                }
            }
        }

        for worker in self.workers.iter_mut().flatten() {
            if let Some(join) = worker.join.take() {
                join.join()
                    .expect("shared-nothing executor worker exits cleanly");
            }
        }
    }
}

fn owner_executor_worker_loop<const N: usize>(
    owner: OwnerId,
    mut partition: OwnerExecutorPartition,
    request_rx: SpscReceiver<OwnerExecutorRequest, N>,
    reply_tx: SpscSender<OwnerExecutorReply, N>,
) {
    loop {
        let Some(request) = request_rx.try_recv() else {
            std::hint::spin_loop();
            continue;
        };

        match request {
            OwnerExecutorRequest::Execute {
                command,
                clock,
                continuation,
                enqueued_at,
            } => {
                let queue_wait = enqueued_at.map_or(Duration::ZERO, |started| started.elapsed());
                let command = partition.execute_owned(owner, command, clock);
                let mut reply = OwnerExecutorReply {
                    continuation,
                    command,
                    queue_wait,
                };
                loop {
                    match reply_tx.try_send(reply) {
                        Ok(()) => break,
                        Err(rejected) => {
                            reply = rejected;
                            std::hint::spin_loop();
                        }
                    }
                }
            }
            OwnerExecutorRequest::Seed {
                key,
                value,
                ttl_deadline,
            } => {
                partition.insert_with_ttl(owner, key, value, ttl_deadline);
            }
            OwnerExecutorRequest::Shutdown => break,
        }
    }

    let _ = owner;
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use vortex_proto::{RespSerializer, RespTape};

    use super::*;

    const RING_SLOTS: usize = 64;

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

    fn response_bytes(command: ExecutedCommand) -> Vec<u8> {
        match command.response {
            CmdResult::Static(bytes) => bytes.to_vec(),
            CmdResult::Inline(inline) => inline.as_bytes().to_vec(),
            CmdResult::Resp(frame) => {
                let mut buf = BytesMut::new();
                RespSerializer::serialize(&frame, &mut buf);
                buf.to_vec()
            }
        }
    }

    fn exec(
        executor: &mut SharedNothingExecutor<RING_SLOTS>,
        name: &[u8],
        parts: &[&[u8]],
        now_nanos: u64,
    ) -> Vec<u8> {
        let wire = make_resp(parts);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        response_bytes(
            executor
                .execute(name, &frame, CommandClock::new(now_nanos, now_nanos))
                .expect("command supported"),
        )
    }

    fn key_for(
        executor: &SharedNothingExecutor<RING_SLOTS>,
        prefix: &str,
        is_match: impl Fn(OwnerId) -> bool,
    ) -> Vec<u8> {
        (0usize..10_000)
            .map(|index| format!("{prefix}:{index:04}").into_bytes())
            .find(|key| is_match(executor.route_owner_bytes(key)))
            .expect("routed key found")
    }

    fn executor() -> SharedNothingExecutor<RING_SLOTS> {
        SharedNothingExecutor::start(4, 64, 64, 0).expect("valid executor")
    }

    #[test]
    fn local_single_key_commands_match_resp_shapes() {
        let mut executor = executor();
        let local_owner = executor.local_owner();
        let key = key_for(&executor, "local", |owner| owner == local_owner);

        assert_eq!(
            exec(&mut executor, b"PING", &[b"PING"], 1_000),
            b"+PONG\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"PING", &[b"PING", b"ready"], 1_000),
            b"$5\r\nready\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"SET", &[b"SET", &key, b"value"], 1_000),
            b"+OK\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"GET", &[b"GET", &key], 1_000),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"EXISTS", &[b"EXISTS", &key], 1_000),
            b":1\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"TYPE", &[b"TYPE", &key], 1_000),
            b"+string\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"TTL", &[b"TTL", &key], 1_000),
            b":-1\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"DEL", &[b"DEL", &key], 1_000),
            b":1\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"SET", &[b"SET", &key, b"value"], 1_000),
            b"+OK\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"UNLINK", &[b"UNLINK", &key], 1_000),
            b":1\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"GET", &[b"GET", &key], 1_000),
            b"$-1\r\n"
        );
    }

    #[test]
    fn remote_single_key_commands_return_through_continuation() {
        let mut executor = executor();
        let local_owner = executor.local_owner();
        let key = key_for(&executor, "remote", |owner| owner != local_owner);

        assert_eq!(
            exec(&mut executor, b"SET", &[b"SET", &key, b"value"], 2_000),
            b"+OK\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"GET", &[b"GET", &key], 2_000),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"DEL", &[b"DEL", &key], 2_000),
            b":1\r\n"
        );
    }

    #[test]
    fn incr_matches_integer_and_error_response_shapes() {
        let mut executor = executor();
        let key = key_for(&executor, "incr", |_| true);

        assert_eq!(
            exec(&mut executor, b"INCR", &[b"INCR", &key], 1_000),
            b":1\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"INCR", &[b"INCR", &key], 1_000),
            b":2\r\n"
        );
        assert_eq!(
            exec(
                &mut executor,
                b"SET",
                &[b"SET", &key, b"9223372036854775807"],
                1_000,
            ),
            b"+OK\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"INCR", &[b"INCR", &key], 1_000),
            b"-ERR increment or decrement would overflow\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"SET", &[b"SET", &key, b"not-int"], 1_000),
            b"+OK\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"INCR", &[b"INCR", &key], 1_000),
            b"-ERR value is not an integer or out of range\r\n"
        );
    }

    #[test]
    fn ttl_and_pttl_expire_owner_local_state() {
        let mut executor = executor();
        let key = key_for(&executor, "ttl", |_| true);
        let now = 10_000_000;

        assert_eq!(
            exec(
                &mut executor,
                b"SET",
                &[b"SET", &key, b"value", b"PX", b"1000"],
                now,
            ),
            b"+OK\r\n"
        );
        assert_eq!(exec(&mut executor, b"TTL", &[b"TTL", &key], now), b":1\r\n");
        assert_eq!(
            exec(&mut executor, b"PTTL", &[b"PTTL", &key], now),
            b":1000\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"GET", &[b"GET", &key], now + 1_000_000_000),
            b"$-1\r\n"
        );
        assert_eq!(
            exec(
                &mut executor,
                b"EXISTS",
                &[b"EXISTS", &key],
                now + 1_000_000_000,
            ),
            b":0\r\n"
        );
    }

    #[test]
    fn set_options_match_single_key_semantics() {
        let mut executor = executor();
        let key = key_for(&executor, "setopt", |_| true);

        assert_eq!(
            exec(&mut executor, b"SET", &[b"SET", &key, b"one", b"NX"], 1),
            b"+OK\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"SET", &[b"SET", &key, b"two", b"NX"], 1),
            b"$-1\r\n"
        );
        assert_eq!(
            exec(
                &mut executor,
                b"SET",
                &[b"SET", &key, b"two", b"XX", b"GET"],
                1,
            ),
            b"$3\r\none\r\n"
        );
        assert_eq!(
            exec(&mut executor, b"GET", &[b"GET", &key], 1),
            b"$3\r\ntwo\r\n"
        );
    }

    #[test]
    fn stale_generation_drops_remote_response() {
        let command = ExecutedCommand::from(CmdResult::Static(crate::commands::RESP_OK));
        let token = SharedNothingConnectionToken::new(
            SharedNothingConnectionId::new(7),
            SharedNothingConnectionGeneration::new(3),
        );
        let continuation = RemoteContinuation::new(
            42,
            OwnerId::from_validated_index(0),
            OwnerId::from_validated_index(1),
            token,
        );

        let result = SharedNothingExecutor::<RING_SLOTS>::complete_remote_response(
            continuation,
            SharedNothingConnectionGeneration::new(4),
            command,
            Duration::from_nanos(10),
        );

        assert!(matches!(
            result,
            SharedNothingExecutionResult::StaleConnection { continuation: c }
                if c.connection() == token
        ));
    }

    #[test]
    fn multi_key_commands_are_not_claimed_by_sn004() {
        let mut executor = executor();
        let key_a = b"k-a";
        let key_b = b"k-b";
        let wire = make_resp(&[b"DEL", key_a, key_b]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");

        let result = executor.execute_with_generation(
            SharedNothingConnectionToken::synthetic(),
            b"DEL",
            &frame,
            CommandClock::new(0, 0),
            |_| SharedNothingConnectionGeneration::INITIAL,
        );

        assert!(matches!(result, SharedNothingExecutionResult::Unsupported));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "owner partition accessed through the wrong owner")]
    fn owner_partition_asserts_affinity_in_debug_builds() {
        let owner_zero = OwnerId::from_validated_index(0);
        let owner_one = OwnerId::from_validated_index(1);
        let mut partition = OwnerExecutorPartition::with_capacity(owner_zero, 8);

        partition.insert_with_ttl(
            owner_one,
            VortexKey::from("wrong-owner"),
            VortexValue::Integer(1),
            0,
        );
    }
}
