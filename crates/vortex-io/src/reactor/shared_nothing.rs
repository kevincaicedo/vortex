//! Reactor-facing shared-nothing continuation contract.
//!
//! This module is the SN-006B bridge between the engine owner harnesses and
//! the real `vortex-io` reactor contract. It deliberately models the event-loop
//! shape without sockets first: remote owner dispatch is accepted as pending and
//! returns to the caller immediately, while reply drain and ordered publication
//! happen on later reactor ticks.

#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::backend::BackendWaker;

use super::CommandResponse;
use vortex_common::{VortexKey, VortexValue};
use vortex_engine::OwnedSharedNothingCommand;
use vortex_engine::commands::{
    CmdResult, CommandClock, ERR_NO_SUCH_KEY, ERR_SYNTAX, ExecutedCommand, RESP_OK, RESP_ONE,
    RESP_ZERO, value_from_bytes,
};
use vortex_engine::owner::{
    ContinuationAcceptError, ContinuationBackpressure, ContinuationHarnessError, KeyCapsuleId,
    OwnerId, OwnerReplyStatus, PublishedContinuation, ReactorContinuationHarness,
    ReactorContinuationMetrics, SharedNothingConnectionGeneration, SharedNothingConnectionId,
    SharedNothingOwnerDispatch, SharedNothingOwnerRuntime, TopologyConfig, TopologyConfigError,
    TxnAction, TxnFinishOutcome, TxnId, TxnIntent, TxnIntentKind, TxnPrepareOutcome,
};
use vortex_sync::{SpscReceiver, SpscSender, spsc_channel};

/// Reactor-facing result of dispatching one shared-nothing command.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum SharedNothingDispatchResult<R> {
    /// The command completed in the current reactor turn and may be appended to
    /// the connection write queue immediately.
    Ready {
        /// Local sequencing request ID.
        request_id: u64,
        /// Response payload owned by the ingress reactor.
        response: R,
    },
    /// The command was accepted, stored as a continuation, and must publish
    /// after a later reply-drain phase.
    Pending {
        /// Remote sequencing request ID.
        request_id: u64,
        /// Client connection that owns the continuation.
        connection: SharedNothingConnectionId,
        /// Bytes retained after parser-buffer ownership promotion.
        retained_bytes: usize,
    },
    /// Bounded queue or continuation pressure rejected the command before
    /// acceptance.
    Backpressure(SharedNothingDispatchBackpressure),
    /// The command is outside the current shared-nothing adapter surface.
    Unsupported,
}

/// Backpressure details surfaced to reactor admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SharedNothingDispatchBackpressure {
    pub(super) detail: ContinuationBackpressure,
}

/// Minimal SN-006B reactor adapter used to prove nonblocking remote dispatch.
#[doc(hidden)]
pub(super) struct SharedNothingReactorAdapter<const N: usize> {
    local_owner: OwnerId,
    continuation: ReactorContinuationHarness<N>,
    blocked_remote_wait_nanos: u64,
    reactor_activations_while_remote_pending: u64,
}

impl<const N: usize> SharedNothingReactorAdapter<N> {
    /// Builds a reactor adapter for one ingress/owner reactor.
    pub(super) fn new(
        owner_count: usize,
        capsule_count: usize,
        connection_count: usize,
        local_owner_index: usize,
    ) -> Result<Self, ContinuationHarnessError> {
        let continuation =
            ReactorContinuationHarness::new(owner_count, capsule_count, connection_count)?;
        let local_owner = continuation.owner_id(local_owner_index).ok_or_else(|| {
            ContinuationHarnessError::Topology(
                vortex_engine::owner::TopologyConfigError::OwnerCountTooLarge {
                    owner_count: local_owner_index,
                },
            )
        })?;

        Ok(Self {
            local_owner,
            continuation,
            blocked_remote_wait_nanos: 0,
            reactor_activations_while_remote_pending: 0,
        })
    }

    /// Returns the owner assigned to this reactor.
    #[inline]
    pub(super) const fn local_owner(&self) -> OwnerId {
        self.local_owner
    }

    /// Returns an owner ID for tests or future topology wiring.
    #[inline]
    pub(super) fn owner_id(&self, index: usize) -> Option<OwnerId> {
        self.continuation.owner_id(index)
    }

    /// Returns a capsule ID for tests or future topology wiring.
    #[inline]
    pub(super) fn capsule_id(&self, index: usize) -> Option<KeyCapsuleId> {
        self.continuation.capsule_id(index)
    }

    /// Returns immutable continuation metrics.
    #[inline]
    pub(super) const fn metrics(&self) -> &ReactorContinuationMetrics {
        self.continuation.metrics()
    }

    /// Nanoseconds spent synchronously waiting for a remote owner reply.
    ///
    /// This must remain zero in server mode. The adapter has no API that waits
    /// for a remote reply; replies can only be made visible through explicit
    /// drain calls.
    #[inline]
    pub(super) const fn blocked_remote_wait_nanos(&self) -> u64 {
        self.blocked_remote_wait_nanos
    }

    /// Number of reactor activations observed while remote continuations were
    /// pending.
    #[inline]
    pub(super) const fn reactor_activations_while_remote_pending(&self) -> u64 {
        self.reactor_activations_while_remote_pending
    }

    /// Number of remote continuations currently waiting for a reply.
    #[inline]
    pub(super) const fn pending_count(&self) -> usize {
        self.continuation.pending_count()
    }

    /// Records one reactor activation. This is intentionally separate from
    /// reply drain so tests can prove the reactor keeps making progress while a
    /// remote owner is delayed.
    pub(super) fn record_reactor_activation(&mut self) {
        if self.continuation.pending_count() != 0 {
            self.reactor_activations_while_remote_pending = self
                .reactor_activations_while_remote_pending
                .saturating_add(1);
        }
    }

    /// Accepts a remote command without waiting for owner execution.
    pub(super) fn accept_remote<R>(
        &mut self,
        destination: OwnerId,
        capsule: KeyCapsuleId,
        connection: SharedNothingConnectionId,
        retained_bytes: usize,
    ) -> SharedNothingDispatchResult<R> {
        match self.continuation.try_accept_remote(
            self.local_owner,
            destination,
            capsule,
            connection,
        ) {
            Ok(request_id) => SharedNothingDispatchResult::Pending {
                request_id,
                connection,
                retained_bytes,
            },
            Err(ContinuationAcceptError::Backpressure(detail)) => {
                SharedNothingDispatchResult::Backpressure(SharedNothingDispatchBackpressure {
                    detail,
                })
            }
            Err(ContinuationAcceptError::InvalidRoute(_))
            | Err(ContinuationAcceptError::UnknownConnection(_)) => {
                SharedNothingDispatchResult::Unsupported
            }
        }
    }

    /// Completes one local command through the same sequencing model used by
    /// remote continuations.
    pub(super) fn accept_local_ready<R>(
        &mut self,
        connection: SharedNothingConnectionId,
        response: R,
    ) -> SharedNothingDispatchResult<R> {
        match self.continuation.try_accept_local_ready(
            self.local_owner,
            connection,
            OwnerReplyStatus::Ok,
        ) {
            Ok(request_id) => SharedNothingDispatchResult::Ready {
                request_id,
                response,
            },
            Err(ContinuationAcceptError::UnknownConnection(_))
            | Err(ContinuationAcceptError::InvalidRoute(_)) => {
                SharedNothingDispatchResult::Unsupported
            }
            Err(ContinuationAcceptError::Backpressure(detail)) => {
                SharedNothingDispatchResult::Backpressure(SharedNothingDispatchBackpressure {
                    detail,
                })
            }
        }
    }

    /// Drains owner command work for one owner.
    #[inline]
    pub(super) fn drain_owner_commands(
        &mut self,
        owner: OwnerId,
        budget: usize,
    ) -> Result<usize, vortex_engine::owner::InvalidMailboxRoute> {
        self.continuation.drain_owner_commands(owner, budget)
    }

    /// Drains reply work returning to this ingress reactor.
    #[inline]
    pub(super) fn drain_ingress_replies(
        &mut self,
        budget: usize,
    ) -> Result<usize, vortex_engine::owner::InvalidMailboxRoute> {
        self.continuation
            .drain_ingress_replies(self.local_owner, budget)
    }

    /// Disconnects a connection and releases pending continuations.
    #[inline]
    pub(super) fn disconnect(&mut self, connection: SharedNothingConnectionId) -> usize {
        self.continuation.disconnect(connection)
    }

    /// Published replies for one connection.
    #[inline]
    pub(super) fn published_for_connection(
        &self,
        connection: SharedNothingConnectionId,
    ) -> &[PublishedContinuation] {
        self.continuation
            .published_for_connection(connection)
            .unwrap_or(&[])
    }
}

#[allow(dead_code)]
fn _assert_no_blocking_wait_metric_can_move(duration: Duration) -> u64 {
    let _ = duration;
    0
}

pub(crate) const SHARED_NOTHING_MAILBOX_RING_SLOTS: usize = 1024;

pub(super) static RESP_ERR_SHARED_NOTHING_UNSUPPORTED: &[u8] =
    b"-ERR command is not supported by shared-nothing research mode\r\n";
static RESP_ERR_SHARED_NOTHING_TXN_CONFLICT: &[u8] =
    b"-TRYAGAIN shared-nothing transaction conflict\r\n";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct SharedNothingServerMetrics {
    pub(super) accepted_remote: u64,
    pub(super) local_ready: u64,
    pub(super) remote_backpressure: u64,
    pub(super) reply_backpressure: u64,
    pub(super) deferred_replies: u64,
    pub(super) owner_commands_drained: u64,
    pub(super) replies_drained: u64,
    pub(super) published_replies: u64,
    pub(super) stale_reply_drops: u64,
    pub(super) orphan_reply_drops: u64,
    pub(super) disconnect_cleanups: u64,
    pub(super) blocked_remote_wait_nanos: u64,
    pub(super) aggregates_accepted: u64,
    pub(super) aggregate_width_total: u64,
    pub(super) aggregate_width_max: u64,
    pub(super) aggregate_remote_subplans: u64,
    pub(super) aggregate_partial_replies: u64,
    pub(super) txns_accepted: u64,
    pub(super) txn_prepare_messages: u64,
    pub(super) txn_commit_messages: u64,
    pub(super) txn_abort_messages: u64,
    pub(super) txn_condition_aborts: u64,
    pub(super) txn_conflict_aborts: u64,
    pub(super) prepared_key_waits: u64,
    pub(super) prepared_key_retries: u64,
    pub(super) prepared_key_wait_nanos_total: u64,
    pub(super) prepared_key_wait_nanos_max: u64,
    pub(super) wakeups_sent: u64,
    pub(super) wakeup_failures: u64,
}

pub(crate) struct SharedNothingServerFabric<const N: usize> {
    config: TopologyConfig,
    command_senders: Box<[SpscSender<SharedNothingOwnerCommand, N>]>,
    command_receivers: Box<[SpscReceiver<SharedNothingOwnerCommand, N>]>,
    reply_senders: Box<[SpscSender<SharedNothingOwnerReply, N>]>,
    reply_receivers: Box<[SpscReceiver<SharedNothingOwnerReply, N>]>,
    wakers: Box<[OnceLock<BackendWaker>]>,
}

impl<const N: usize> SharedNothingServerFabric<N> {
    pub(crate) fn new(config: TopologyConfig) -> Result<Self, SharedNothingServerFabricError> {
        if N < 2 {
            return Err(SharedNothingServerFabricError::RingTooSmall { ring_slots: N });
        }
        if !N.is_power_of_two() {
            return Err(SharedNothingServerFabricError::RingNotPowerOfTwo { ring_slots: N });
        }
        let queue_count = config
            .owner_count()
            .checked_mul(config.owner_count())
            .ok_or(SharedNothingServerFabricError::QueueCountOverflow {
                owner_count: config.owner_count(),
            })?;
        let mut command_senders = Vec::with_capacity(queue_count);
        let mut command_receivers = Vec::with_capacity(queue_count);
        let mut reply_senders = Vec::with_capacity(queue_count);
        let mut reply_receivers = Vec::with_capacity(queue_count);
        for _ in 0..queue_count {
            let (sender, receiver) = spsc_channel();
            command_senders.push(sender);
            command_receivers.push(receiver);
            let (sender, receiver) = spsc_channel();
            reply_senders.push(sender);
            reply_receivers.push(receiver);
        }
        let wakers = (0..config.owner_count())
            .map(|_| OnceLock::new())
            .collect::<Vec<_>>();
        Ok(Self {
            config,
            command_senders: command_senders.into_boxed_slice(),
            command_receivers: command_receivers.into_boxed_slice(),
            reply_senders: reply_senders.into_boxed_slice(),
            reply_receivers: reply_receivers.into_boxed_slice(),
            wakers: wakers.into_boxed_slice(),
        })
    }

    #[inline]
    pub(super) const fn config(&self) -> TopologyConfig {
        self.config
    }

    #[inline]
    pub(super) const fn owner_count(&self) -> usize {
        self.config.owner_count()
    }

    #[inline]
    pub(super) const fn lane_capacity(&self) -> usize {
        N - 1
    }

    pub(super) fn register_waker(&self, owner: OwnerId, waker: BackendWaker) {
        if let Some(slot) = self.wakers.get(owner.get()) {
            let _ = slot.set(waker);
        }
    }

    #[inline]
    fn queue_index(&self, source: OwnerId, destination: OwnerId) -> Option<usize> {
        if source.get() >= self.owner_count() || destination.get() >= self.owner_count() {
            return None;
        }
        Some(source.get() * self.owner_count() + destination.get())
    }

    #[inline]
    fn try_send_command(
        &self,
        command: SharedNothingOwnerCommand,
    ) -> Result<(), (SharedNothingServerBackpressure, SharedNothingOwnerCommand)> {
        let Some(index) = self.queue_index(command.source, command.destination) else {
            return Err((
                SharedNothingServerBackpressure {
                    source: command.source,
                    destination: command.destination,
                    lane: SharedNothingServerLane::Command,
                    used_slots: 0,
                    capacity_slots: self.lane_capacity(),
                },
                command,
            ));
        };
        self.command_senders[index]
            .try_send(command)
            .map_err(|command| {
                (
                    SharedNothingServerBackpressure {
                        source: command.source,
                        destination: command.destination,
                        lane: SharedNothingServerLane::Command,
                        used_slots: self.command_senders[index].len(),
                        capacity_slots: self.lane_capacity(),
                    },
                    command,
                )
            })
    }

    #[inline]
    fn command_lane_available(&self, source: OwnerId, destination: OwnerId) -> usize {
        let Some(index) = self.queue_index(source, destination) else {
            return 0;
        };
        self.lane_capacity()
            .saturating_sub(self.command_senders[index].len())
    }

    #[inline]
    fn reply_lane_available(&self, source: OwnerId, destination: OwnerId) -> usize {
        let Some(index) = self.queue_index(source, destination) else {
            return 0;
        };
        self.lane_capacity()
            .saturating_sub(self.reply_senders[index].len())
    }

    #[inline]
    fn try_send_reply(
        &self,
        reply: SharedNothingOwnerReply,
    ) -> Result<(), (SharedNothingServerBackpressure, SharedNothingOwnerReply)> {
        let Some(index) = self.queue_index(reply.source, reply.destination) else {
            return Err((
                SharedNothingServerBackpressure {
                    source: reply.source,
                    destination: reply.destination,
                    lane: SharedNothingServerLane::Reply,
                    used_slots: 0,
                    capacity_slots: self.lane_capacity(),
                },
                reply,
            ));
        };
        self.reply_senders[index].try_send(reply).map_err(|reply| {
            (
                SharedNothingServerBackpressure {
                    source: reply.source,
                    destination: reply.destination,
                    lane: SharedNothingServerLane::Reply,
                    used_slots: self.reply_senders[index].len(),
                    capacity_slots: self.lane_capacity(),
                },
                reply,
            )
        })
    }

    fn wake_owner(&self, owner: OwnerId) -> bool {
        self.wakers
            .get(owner.get())
            .and_then(OnceLock::get)
            .is_some_and(|waker| waker.wake().is_ok())
    }

    fn drain_commands<F>(
        &self,
        destination: OwnerId,
        cursor: &mut SharedNothingSourceCursor,
        limit: usize,
        mut visit: F,
    ) -> usize
    where
        F: FnMut(SharedNothingOwnerCommand),
    {
        self.drain_owner_lanes(
            destination,
            cursor,
            limit,
            |fabric, source, destination| {
                let index = fabric
                    .queue_index(source, destination)
                    .expect("drain source/destination are valid owners");
                &fabric.command_receivers[index]
            },
            |message| visit(message),
        )
    }

    fn drain_replies<F>(
        &self,
        destination: OwnerId,
        cursor: &mut SharedNothingSourceCursor,
        limit: usize,
        mut visit: F,
    ) -> usize
    where
        F: FnMut(SharedNothingOwnerReply),
    {
        self.drain_owner_lanes(
            destination,
            cursor,
            limit,
            |fabric, source, destination| {
                let index = fabric
                    .queue_index(source, destination)
                    .expect("drain source/destination are valid owners");
                &fabric.reply_receivers[index]
            },
            |message| visit(message),
        )
    }

    fn drain_owner_lanes<T, F, R>(
        &self,
        destination: OwnerId,
        cursor: &mut SharedNothingSourceCursor,
        limit: usize,
        receiver: R,
        mut visit: F,
    ) -> usize
    where
        F: FnMut(T),
        R: Fn(&Self, OwnerId, OwnerId) -> &SpscReceiver<T, N>,
    {
        if limit == 0 || self.owner_count() <= 1 || destination.get() >= self.owner_count() {
            return 0;
        }

        let owner_count = self.owner_count();
        let mut source_index = cursor.next_source % owner_count;
        let mut drained = 0usize;
        for _ in 0..owner_count {
            let source = OwnerId::from_validated_index(source_index);
            source_index = (source_index + 1) % owner_count;
            if source == destination {
                continue;
            }
            let remaining = limit - drained;
            let drained_now = receiver(self, source, destination)
                .drain_batch(remaining, |message| visit(message));
            drained += drained_now;
            if drained == limit {
                break;
            }
        }
        cursor.next_source = source_index;
        drained
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SharedNothingServerFabricError {
    RingTooSmall { ring_slots: usize },
    RingNotPowerOfTwo { ring_slots: usize },
    QueueCountOverflow { owner_count: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SharedNothingServerLane {
    Command,
    Reply,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SharedNothingServerBackpressure {
    pub(super) source: OwnerId,
    pub(super) destination: OwnerId,
    pub(super) lane: SharedNothingServerLane,
    pub(super) used_slots: usize,
    pub(super) capacity_slots: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct SharedNothingSourceCursor {
    next_source: usize,
}

struct SharedNothingOwnerCommand {
    request_id: u64,
    source: OwnerId,
    destination: OwnerId,
    connection: SharedNothingConnectionId,
    generation: SharedNothingConnectionGeneration,
    sequence: u64,
    command: OwnedSharedNothingCommand,
    clock: CommandClock,
    enqueued_at: Instant,
}

struct SharedNothingOwnerReply {
    request_id: u64,
    source: OwnerId,
    destination: OwnerId,
    connection: SharedNothingConnectionId,
    generation: SharedNothingConnectionGeneration,
    sequence: u64,
    command: ExecutedCommand,
    clock: CommandClock,
    queue_wait: Duration,
}

#[derive(Default)]
struct SharedNothingConnectionState {
    next_sequence: u64,
    next_publish_sequence: u64,
    completed: BTreeMap<u64, CommandResponse>,
}

struct SharedNothingPendingRequest {
    connection: SharedNothingConnectionId,
    generation: SharedNothingConnectionGeneration,
    kind: SharedNothingPendingKind,
}

enum SharedNothingPendingKind {
    Single,
    Aggregate {
        aggregate_id: u64,
        positions: Box<[usize]>,
    },
    TxnPrepare {
        txn_id: TxnId,
        owner: OwnerId,
    },
    TxnFinish {
        txn_id: TxnId,
        owner: OwnerId,
    },
    DualSourcePrepare {
        txn_id: TxnId,
        owner: OwnerId,
    },
    DualPrepare {
        txn_id: TxnId,
        owner: OwnerId,
    },
    DualFinish {
        txn_id: TxnId,
        owner: OwnerId,
    },
}

struct SharedNothingAggregateState {
    connection: SharedNothingConnectionId,
    generation: SharedNothingConnectionGeneration,
    sequence: u64,
    remaining: usize,
    kind: SharedNothingAggregateKind,
}

enum SharedNothingAggregateKind {
    Mget {
        values: Vec<vortex_proto::RespFrame>,
    },
    Exists {
        count: i64,
    },
}

struct SharedNothingAggregateSubplan {
    owner: OwnerId,
    positions: Vec<usize>,
    keys: Vec<VortexKey>,
}

struct SharedNothingWriteSubplan {
    owner: OwnerId,
    pairs: Vec<(VortexKey, VortexValue)>,
}

struct SharedNothingWriteTxnState {
    connection: SharedNothingConnectionId,
    generation: SharedNothingConnectionGeneration,
    sequence: u64,
    kind: TxnIntentKind,
    prepare_queue: VecDeque<SharedNothingWriteSubplan>,
    remaining_prepares: usize,
    remaining_finishes: usize,
    prepared_owners: Vec<OwnerId>,
    failed_prepare: Option<TxnPrepareOutcome>,
    finish_failed: bool,
}

struct SharedNothingDualKeyPlan {
    kind: TxnIntentKind,
    source: VortexKey,
    destination: VortexKey,
    source_owner: OwnerId,
    destination_owner: OwnerId,
}

struct SharedNothingDualKeyTxnState {
    connection: SharedNothingConnectionId,
    generation: SharedNothingConnectionGeneration,
    sequence: u64,
    kind: TxnIntentKind,
    destination: VortexKey,
    destination_owner: OwnerId,
    capture: Option<SharedNothingDualKeyCapture>,
    remaining_finishes: usize,
    prepared_owners: Vec<OwnerId>,
    failed_prepare: Option<TxnPrepareOutcome>,
    finish_failed: bool,
}

#[derive(Clone)]
struct SharedNothingDualKeyCapture {
    value: VortexValue,
    ttl_deadline: u64,
}

pub(super) enum SharedNothingServerDispatch {
    Ready,
    Pending,
    Backpressure(SharedNothingServerBackpressure),
    Unsupported,
}

pub(super) struct SharedNothingServerRuntime<const N: usize> {
    local_owner: OwnerId,
    owner: SharedNothingOwnerRuntime,
    fabric: Arc<SharedNothingServerFabric<N>>,
    command_cursor: SharedNothingSourceCursor,
    reply_cursor: SharedNothingSourceCursor,
    connections: Vec<SharedNothingConnectionState>,
    pending: Vec<Option<SharedNothingPendingRequest>>,
    aggregates: Vec<Option<SharedNothingAggregateState>>,
    write_txns: Vec<Option<SharedNothingWriteTxnState>>,
    dual_key_txns: Vec<Option<SharedNothingDualKeyTxnState>>,
    next_request_id: u64,
    next_aggregate_id: u64,
    next_txn_id: u64,
    deferred_commands: VecDeque<SharedNothingOwnerCommand>,
    deferred_blocked_commands: VecDeque<SharedNothingOwnerCommand>,
    prepared_key_release_pending: bool,
    deferred_replies: VecDeque<SharedNothingOwnerReply>,
    metrics: SharedNothingServerMetrics,
}

impl<const N: usize> SharedNothingServerRuntime<N> {
    pub(super) fn new(
        fabric: Arc<SharedNothingServerFabric<N>>,
        local_owner_index: usize,
        connection_count: usize,
        capacity_per_owner: usize,
    ) -> Result<Self, TopologyConfigError> {
        let owner =
            SharedNothingOwnerRuntime::new(fabric.config(), local_owner_index, capacity_per_owner)?;
        let local_owner = owner.local_owner();
        Ok(Self {
            local_owner,
            owner,
            fabric,
            command_cursor: SharedNothingSourceCursor::default(),
            reply_cursor: SharedNothingSourceCursor::default(),
            connections: (0..connection_count)
                .map(|_| SharedNothingConnectionState::default())
                .collect(),
            pending: Vec::new(),
            aggregates: Vec::new(),
            write_txns: Vec::new(),
            dual_key_txns: Vec::new(),
            next_request_id: 0,
            next_aggregate_id: 0,
            next_txn_id: 0,
            deferred_commands: VecDeque::new(),
            deferred_blocked_commands: VecDeque::new(),
            prepared_key_release_pending: false,
            deferred_replies: VecDeque::new(),
            metrics: SharedNothingServerMetrics::default(),
        })
    }

    #[inline]
    pub(super) const fn local_owner(&self) -> OwnerId {
        self.local_owner
    }

    #[inline]
    pub(super) const fn metrics(&self) -> SharedNothingServerMetrics {
        self.metrics
    }

    #[inline]
    pub(super) const fn blocked_remote_wait_nanos(&self) -> u64 {
        self.metrics.blocked_remote_wait_nanos
    }

    pub(super) fn register_waker(&self, waker: BackendWaker) {
        self.fabric.register_waker(self.local_owner, waker);
    }

    pub(super) fn dispatch_ingress(
        &mut self,
        connection: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
        name: &[u8],
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
    ) -> SharedNothingServerDispatch {
        let connection_index = connection.get() as usize;
        if connection_index >= self.connections.len() {
            return SharedNothingServerDispatch::Unsupported;
        }
        if !self.flush_deferred_commands(16) {
            self.metrics.remote_backpressure = self.metrics.remote_backpressure.saturating_add(1);
            return SharedNothingServerDispatch::Backpressure(SharedNothingServerBackpressure {
                source: self.local_owner,
                destination: self.local_owner,
                lane: SharedNothingServerLane::Command,
                used_slots: self.fabric.lane_capacity(),
                capacity_slots: self.fabric.lane_capacity(),
            });
        }

        if name == b"MGET" {
            return self.dispatch_mget(connection, generation, frame, clock);
        }
        if name == b"MSET" {
            return self.dispatch_write_txn(
                connection,
                generation,
                frame,
                clock,
                TxnIntentKind::Mset,
            );
        }
        if name == b"MSETNX" {
            return self.dispatch_write_txn(
                connection,
                generation,
                frame,
                clock,
                TxnIntentKind::MsetNx,
            );
        }
        if name == b"RENAME" {
            return self.dispatch_dual_key_txn(
                connection,
                generation,
                frame,
                clock,
                TxnIntentKind::Rename,
            );
        }
        if name == b"RENAMENX" {
            return self.dispatch_dual_key_txn(
                connection,
                generation,
                frame,
                clock,
                TxnIntentKind::RenameNx,
            );
        }
        if name == b"COPY" {
            return self.dispatch_copy_txn(connection, generation, frame, clock);
        }
        if name == b"EXISTS" && frame.element_count().is_some_and(|argc| argc > 2) {
            return self.dispatch_exists_many(connection, generation, frame, clock);
        }

        match self.owner.dispatch_ingress(name, frame, clock) {
            SharedNothingOwnerDispatch::Ready { command, .. }
            | SharedNothingOwnerDispatch::Immediate(command) => {
                let sequence = self.reserve_sequence(connection_index);
                self.complete_sequence(connection_index, sequence, response_from_executed(command));
                self.metrics.local_ready = self.metrics.local_ready.saturating_add(1);
                SharedNothingServerDispatch::Ready
            }
            SharedNothingOwnerDispatch::Remote { owner, command } => {
                let sequence = self.reserve_sequence(connection_index);
                let request_id = self.next_request_id;
                let owner_command = SharedNothingOwnerCommand {
                    request_id,
                    source: self.local_owner,
                    destination: owner,
                    connection,
                    generation,
                    sequence,
                    command,
                    clock,
                    enqueued_at: Instant::now(),
                };
                match self.fabric.try_send_command(owner_command) {
                    Ok(()) => {
                        self.next_request_id = self.next_request_id.wrapping_add(1);
                        self.insert_pending(
                            request_id,
                            SharedNothingPendingRequest {
                                connection,
                                generation,
                                kind: SharedNothingPendingKind::Single,
                            },
                        );
                        self.record_wakeup(owner);
                        self.metrics.accepted_remote =
                            self.metrics.accepted_remote.saturating_add(1);
                        SharedNothingServerDispatch::Pending
                    }
                    Err((backpressure, _command)) => {
                        self.rollback_sequence(connection_index, sequence);
                        self.metrics.remote_backpressure =
                            self.metrics.remote_backpressure.saturating_add(1);
                        SharedNothingServerDispatch::Backpressure(backpressure)
                    }
                }
            }
            SharedNothingOwnerDispatch::Blocked => {
                self.metrics.prepared_key_waits = self.metrics.prepared_key_waits.saturating_add(1);
                SharedNothingServerDispatch::Backpressure(
                    self.prepared_key_backpressure(self.local_owner),
                )
            }
            SharedNothingOwnerDispatch::Unsupported => SharedNothingServerDispatch::Unsupported,
        }
    }

    pub(super) fn complete_admin_response(
        &mut self,
        connection: SharedNothingConnectionId,
        response: CommandResponse,
    ) -> SharedNothingServerDispatch {
        let connection_index = connection.get() as usize;
        if connection_index >= self.connections.len() {
            return SharedNothingServerDispatch::Unsupported;
        }
        let sequence = self.reserve_sequence(connection_index);
        self.complete_sequence(connection_index, sequence, response);
        self.metrics.local_ready = self.metrics.local_ready.saturating_add(1);
        SharedNothingServerDispatch::Ready
    }

    fn dispatch_mget(
        &mut self,
        connection: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
    ) -> SharedNothingServerDispatch {
        let (width, subplans) = match self.collect_aggregate_subplans(frame) {
            Ok(plan) => plan,
            Err(command) => {
                return self.complete_immediate(connection, command);
            }
        };
        if width == 0 {
            return self.complete_immediate(
                connection,
                ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)),
            );
        }
        self.dispatch_aggregate(connection, generation, clock, width, subplans, true)
    }

    fn dispatch_exists_many(
        &mut self,
        connection: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
    ) -> SharedNothingServerDispatch {
        let (width, subplans) = match self.collect_aggregate_subplans(frame) {
            Ok(plan) => plan,
            Err(command) => {
                return self.complete_immediate(connection, command);
            }
        };
        self.dispatch_aggregate(connection, generation, clock, width, subplans, false)
    }

    fn dispatch_aggregate(
        &mut self,
        connection: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
        clock: CommandClock,
        width: usize,
        mut subplans: Vec<SharedNothingAggregateSubplan>,
        is_mget: bool,
    ) -> SharedNothingServerDispatch {
        let connection_index = connection.get() as usize;
        if connection_index >= self.connections.len() {
            return SharedNothingServerDispatch::Unsupported;
        }

        for subplan in &subplans {
            if subplan.owner == self.local_owner
                && self
                    .owner
                    .keys_blocked(self.local_owner, subplan.keys.iter())
            {
                self.metrics.prepared_key_waits = self.metrics.prepared_key_waits.saturating_add(1);
                return SharedNothingServerDispatch::Backpressure(
                    self.prepared_key_backpressure(self.local_owner),
                );
            }
        }

        for subplan in &subplans {
            if subplan.owner == self.local_owner {
                continue;
            }
            if self
                .fabric
                .command_lane_available(self.local_owner, subplan.owner)
                == 0
            {
                self.metrics.remote_backpressure =
                    self.metrics.remote_backpressure.saturating_add(1);
                return SharedNothingServerDispatch::Backpressure(
                    self.command_backpressure(subplan.owner),
                );
            }
        }

        let sequence = self.reserve_sequence(connection_index);
        let aggregate_id = self.next_aggregate_id;
        self.next_aggregate_id = self.next_aggregate_id.wrapping_add(1);
        let remote_subplans = subplans
            .iter()
            .filter(|subplan| subplan.owner != self.local_owner)
            .count();

        let kind = if is_mget {
            SharedNothingAggregateKind::Mget {
                values: vec![vortex_proto::RespFrame::null_bulk_string(); width],
            }
        } else {
            SharedNothingAggregateKind::Exists { count: 0 }
        };
        let mut state = SharedNothingAggregateState {
            connection,
            generation,
            sequence,
            remaining: remote_subplans,
            kind,
        };

        for subplan in subplans.iter_mut() {
            if subplan.owner != self.local_owner {
                continue;
            }
            let positions = std::mem::take(&mut subplan.positions).into_boxed_slice();
            let keys = std::mem::take(&mut subplan.keys).into_boxed_slice();
            let command = if is_mget {
                OwnedSharedNothingCommand::Mget { keys }
            } else {
                OwnedSharedNothingCommand::ExistsMany { keys }
            };
            let executed = self
                .owner
                .execute_remote_command(self.local_owner, command, clock);
            apply_aggregate_partial(&mut state, &positions, executed);
        }

        self.insert_aggregate(aggregate_id, state);

        for subplan in subplans {
            if subplan.owner == self.local_owner {
                continue;
            }
            let request_id = self.next_request_id;
            self.next_request_id = self.next_request_id.wrapping_add(1);
            let positions = subplan.positions.into_boxed_slice();
            let command = if is_mget {
                OwnedSharedNothingCommand::Mget {
                    keys: subplan.keys.into_boxed_slice(),
                }
            } else {
                OwnedSharedNothingCommand::ExistsMany {
                    keys: subplan.keys.into_boxed_slice(),
                }
            };
            self.insert_pending(
                request_id,
                SharedNothingPendingRequest {
                    connection,
                    generation,
                    kind: SharedNothingPendingKind::Aggregate {
                        aggregate_id,
                        positions,
                    },
                },
            );
            let owner_command = SharedNothingOwnerCommand {
                request_id,
                source: self.local_owner,
                destination: subplan.owner,
                connection,
                generation,
                sequence,
                command,
                clock,
                enqueued_at: Instant::now(),
            };
            if let Err((backpressure, _command)) = self.fabric.try_send_command(owner_command) {
                self.metrics.remote_backpressure =
                    self.metrics.remote_backpressure.saturating_add(1);
                let _ = self.take_pending(request_id);
                self.remove_aggregate(aggregate_id);
                self.rollback_sequence(connection_index, sequence);
                return SharedNothingServerDispatch::Backpressure(backpressure);
            }
            self.record_wakeup(subplan.owner);
        }

        self.metrics.aggregates_accepted = self.metrics.aggregates_accepted.saturating_add(1);
        self.metrics.aggregate_width_total = self
            .metrics
            .aggregate_width_total
            .saturating_add(width as u64);
        self.metrics.aggregate_width_max = self.metrics.aggregate_width_max.max(width as u64);
        self.metrics.aggregate_remote_subplans = self
            .metrics
            .aggregate_remote_subplans
            .saturating_add(remote_subplans as u64);

        self.complete_aggregate_if_ready(aggregate_id);
        if remote_subplans == 0 {
            SharedNothingServerDispatch::Ready
        } else {
            self.metrics.accepted_remote = self.metrics.accepted_remote.saturating_add(1);
            SharedNothingServerDispatch::Pending
        }
    }

    fn dispatch_write_txn(
        &mut self,
        connection: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
        kind: TxnIntentKind,
    ) -> SharedNothingServerDispatch {
        let connection_index = connection.get() as usize;
        if connection_index >= self.connections.len() {
            return SharedNothingServerDispatch::Unsupported;
        }

        let mut subplans = match self.collect_write_subplans(frame) {
            Ok(subplans) => subplans,
            Err(command) => {
                return self.complete_immediate(connection, command);
            }
        };
        subplans.sort_by_key(|subplan| subplan.owner.get());

        for subplan in &subplans {
            if subplan.owner == self.local_owner
                && self
                    .owner
                    .keys_blocked(self.local_owner, subplan.pairs.iter().map(|(key, _)| key))
            {
                self.metrics.prepared_key_waits = self.metrics.prepared_key_waits.saturating_add(1);
                return SharedNothingServerDispatch::Backpressure(
                    self.prepared_key_backpressure(self.local_owner),
                );
            }
        }

        for subplan in &subplans {
            if subplan.owner == self.local_owner {
                continue;
            }
            if self
                .fabric
                .command_lane_available(self.local_owner, subplan.owner)
                == 0
            {
                self.metrics.remote_backpressure =
                    self.metrics.remote_backpressure.saturating_add(1);
                return SharedNothingServerDispatch::Backpressure(
                    self.command_backpressure(subplan.owner),
                );
            }
        }

        let sequence = self.reserve_sequence(connection_index);
        let txn_id = self.allocate_txn_id();
        let participant_count = subplans.len();
        self.insert_write_txn(
            txn_id,
            SharedNothingWriteTxnState {
                connection,
                generation,
                sequence,
                kind,
                prepare_queue: subplans.into_iter().collect(),
                remaining_prepares: participant_count,
                remaining_finishes: 0,
                prepared_owners: Vec::with_capacity(participant_count),
                failed_prepare: None,
                finish_failed: false,
            },
        );

        self.metrics.txns_accepted = self.metrics.txns_accepted.saturating_add(1);
        self.send_next_write_prepare(txn_id, clock);
        if self.write_txn_is_complete(txn_id) {
            SharedNothingServerDispatch::Ready
        } else {
            self.metrics.accepted_remote = self.metrics.accepted_remote.saturating_add(1);
            SharedNothingServerDispatch::Pending
        }
    }

    fn dispatch_copy_txn(
        &mut self,
        connection: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
    ) -> SharedNothingServerDispatch {
        let kind = match parse_copy_kind(frame) {
            Ok(kind) => kind,
            Err(command) => return self.complete_immediate(connection, command),
        };
        self.dispatch_dual_key_txn(connection, generation, frame, clock, kind)
    }

    fn dispatch_dual_key_txn(
        &mut self,
        connection: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
        frame: &vortex_proto::FrameRef<'_>,
        clock: CommandClock,
        kind: TxnIntentKind,
    ) -> SharedNothingServerDispatch {
        let connection_index = connection.get() as usize;
        if connection_index >= self.connections.len() {
            return SharedNothingServerDispatch::Unsupported;
        }

        let plan = match self.collect_dual_key_plan(frame, kind) {
            Ok(plan) => plan,
            Err(command) => return self.complete_immediate(connection, command),
        };

        if plan.source_owner == self.local_owner
            && self
                .owner
                .keys_blocked(self.local_owner, std::iter::once(&plan.source))
        {
            self.metrics.prepared_key_waits = self.metrics.prepared_key_waits.saturating_add(1);
            return SharedNothingServerDispatch::Backpressure(
                self.prepared_key_backpressure(self.local_owner),
            );
        }
        if plan.destination_owner == self.local_owner
            && self
                .owner
                .keys_blocked(self.local_owner, std::iter::once(&plan.destination))
        {
            self.metrics.prepared_key_waits = self.metrics.prepared_key_waits.saturating_add(1);
            return SharedNothingServerDispatch::Backpressure(
                self.prepared_key_backpressure(self.local_owner),
            );
        }

        if plan.source_owner == plan.destination_owner {
            if plan.source_owner != self.local_owner
                && self
                    .fabric
                    .command_lane_available(self.local_owner, plan.source_owner)
                    == 0
            {
                self.metrics.remote_backpressure =
                    self.metrics.remote_backpressure.saturating_add(1);
                return SharedNothingServerDispatch::Backpressure(
                    self.command_backpressure(plan.source_owner),
                );
            }
        } else {
            for owner in [plan.source_owner, plan.destination_owner] {
                if owner == self.local_owner {
                    continue;
                }
                if self.fabric.command_lane_available(self.local_owner, owner) == 0 {
                    self.metrics.remote_backpressure =
                        self.metrics.remote_backpressure.saturating_add(1);
                    return SharedNothingServerDispatch::Backpressure(
                        self.command_backpressure(owner),
                    );
                }
            }
        }

        let sequence = self.reserve_sequence(connection_index);
        let txn_id = self.allocate_txn_id();
        self.insert_dual_key_txn(
            txn_id,
            SharedNothingDualKeyTxnState {
                connection,
                generation,
                sequence,
                kind: plan.kind,
                destination: plan.destination.clone(),
                destination_owner: plan.destination_owner,
                capture: None,
                remaining_finishes: 0,
                prepared_owners: Vec::with_capacity(
                    if plan.source_owner == plan.destination_owner {
                        1
                    } else {
                        2
                    },
                ),
                failed_prepare: None,
                finish_failed: false,
            },
        );

        let dispatch = if plan.source_owner == plan.destination_owner {
            self.dispatch_local_dual_prepare(txn_id, plan, connection, generation, sequence, clock)
        } else {
            self.dispatch_source_capture_prepare(
                txn_id, plan, connection, generation, sequence, clock,
            )
        };

        self.metrics.txns_accepted = self.metrics.txns_accepted.saturating_add(1);
        dispatch
    }

    fn dispatch_local_dual_prepare(
        &mut self,
        txn_id: TxnId,
        plan: SharedNothingDualKeyPlan,
        connection: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
        sequence: u64,
        clock: CommandClock,
    ) -> SharedNothingServerDispatch {
        let command = OwnedSharedNothingCommand::TxnPrepareLocalDual {
            txn_id,
            kind: plan.kind,
            source: plan.source,
            destination: plan.destination,
        };
        if plan.source_owner == self.local_owner {
            let executed = self
                .owner
                .execute_remote_command(self.local_owner, command, clock);
            self.apply_dual_prepare(txn_id, self.local_owner, executed);
            if self.dual_key_txn_is_complete(txn_id) {
                SharedNothingServerDispatch::Ready
            } else {
                SharedNothingServerDispatch::Pending
            }
        } else {
            let request_id = self.next_request_id;
            self.next_request_id = self.next_request_id.wrapping_add(1);
            self.insert_pending(
                request_id,
                SharedNothingPendingRequest {
                    connection,
                    generation,
                    kind: SharedNothingPendingKind::DualPrepare {
                        txn_id,
                        owner: plan.source_owner,
                    },
                },
            );
            let owner_command = SharedNothingOwnerCommand {
                request_id,
                source: self.local_owner,
                destination: plan.source_owner,
                connection,
                generation,
                sequence,
                command,
                clock,
                enqueued_at: Instant::now(),
            };
            if let Err((backpressure, _command)) = self.fabric.try_send_command(owner_command) {
                let _ = self.take_pending(request_id);
                self.remove_dual_key_txn(txn_id);
                self.rollback_sequence(connection.get() as usize, sequence);
                self.metrics.remote_backpressure =
                    self.metrics.remote_backpressure.saturating_add(1);
                return SharedNothingServerDispatch::Backpressure(backpressure);
            }
            self.record_wakeup(plan.source_owner);
            self.metrics.accepted_remote = self.metrics.accepted_remote.saturating_add(1);
            self.metrics.txn_prepare_messages = self.metrics.txn_prepare_messages.saturating_add(1);
            SharedNothingServerDispatch::Pending
        }
    }

    fn dispatch_source_capture_prepare(
        &mut self,
        txn_id: TxnId,
        plan: SharedNothingDualKeyPlan,
        connection: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
        sequence: u64,
        clock: CommandClock,
    ) -> SharedNothingServerDispatch {
        let command = OwnedSharedNothingCommand::TxnCaptureSource {
            txn_id,
            kind: plan.kind,
            source: plan.source,
        };
        if plan.source_owner == self.local_owner {
            let executed = self
                .owner
                .execute_remote_command(self.local_owner, command, clock);
            self.apply_dual_source_prepare(txn_id, self.local_owner, executed, clock);
            if self.dual_key_txn_is_complete(txn_id) {
                SharedNothingServerDispatch::Ready
            } else {
                SharedNothingServerDispatch::Pending
            }
        } else {
            let request_id = self.next_request_id;
            self.next_request_id = self.next_request_id.wrapping_add(1);
            self.insert_pending(
                request_id,
                SharedNothingPendingRequest {
                    connection,
                    generation,
                    kind: SharedNothingPendingKind::DualSourcePrepare {
                        txn_id,
                        owner: plan.source_owner,
                    },
                },
            );
            let owner_command = SharedNothingOwnerCommand {
                request_id,
                source: self.local_owner,
                destination: plan.source_owner,
                connection,
                generation,
                sequence,
                command,
                clock,
                enqueued_at: Instant::now(),
            };
            if let Err((backpressure, _command)) = self.fabric.try_send_command(owner_command) {
                let _ = self.take_pending(request_id);
                self.remove_dual_key_txn(txn_id);
                self.rollback_sequence(connection.get() as usize, sequence);
                self.metrics.remote_backpressure =
                    self.metrics.remote_backpressure.saturating_add(1);
                return SharedNothingServerDispatch::Backpressure(backpressure);
            }
            self.record_wakeup(plan.source_owner);
            self.metrics.accepted_remote = self.metrics.accepted_remote.saturating_add(1);
            self.metrics.txn_prepare_messages = self.metrics.txn_prepare_messages.saturating_add(1);
            SharedNothingServerDispatch::Pending
        }
    }

    fn collect_aggregate_subplans(
        &mut self,
        frame: &vortex_proto::FrameRef<'_>,
    ) -> Result<(usize, Vec<SharedNothingAggregateSubplan>), ExecutedCommand> {
        let argc = frame
            .element_count()
            .map(|argc| argc as usize)
            .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
        if argc < 2 {
            return Err(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
        }
        let mut children = frame
            .children()
            .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
        let _ = children.next();
        let mut subplans: Vec<SharedNothingAggregateSubplan> =
            Vec::with_capacity(self.fabric.owner_count().min(argc - 1));
        for (position, child) in children.enumerate() {
            let key_bytes = child
                .as_bytes()
                .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
            let owner = self.owner.route_owner_with_debug(key_bytes);
            if let Some(subplan) = subplans.iter_mut().find(|subplan| subplan.owner == owner) {
                subplan.positions.push(position);
                subplan.keys.push(key_bytes.into());
            } else {
                subplans.push(SharedNothingAggregateSubplan {
                    owner,
                    positions: vec![position],
                    keys: vec![key_bytes.into()],
                });
            }
        }
        Ok((argc - 1, subplans))
    }

    fn collect_write_subplans(
        &mut self,
        frame: &vortex_proto::FrameRef<'_>,
    ) -> Result<Vec<SharedNothingWriteSubplan>, ExecutedCommand> {
        let argc = frame
            .element_count()
            .map(|argc| argc as usize)
            .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
        if argc < 3 || (argc - 1) % 2 != 0 {
            return Err(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
        }

        let mut children = frame
            .children()
            .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
        let _ = children.next();
        let mut subplans: Vec<SharedNothingWriteSubplan> =
            Vec::with_capacity(self.fabric.owner_count().min((argc - 1) / 2));
        while let (Some(key_arg), Some(value_arg)) = (children.next(), children.next()) {
            let key_bytes = key_arg
                .as_bytes()
                .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
            let value_bytes = value_arg
                .as_bytes()
                .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
            let owner = self.owner.route_owner_with_debug(key_bytes);
            let pair = (
                VortexKey::from(key_bytes),
                VortexValue::from_bytes(value_bytes),
            );
            if let Some(subplan) = subplans.iter_mut().find(|subplan| subplan.owner == owner) {
                subplan.pairs.push(pair);
            } else {
                subplans.push(SharedNothingWriteSubplan {
                    owner,
                    pairs: vec![pair],
                });
            }
        }
        Ok(subplans)
    }

    fn collect_dual_key_plan(
        &mut self,
        frame: &vortex_proto::FrameRef<'_>,
        kind: TxnIntentKind,
    ) -> Result<SharedNothingDualKeyPlan, ExecutedCommand> {
        let argc = frame
            .element_count()
            .map(|argc| argc as usize)
            .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
        if argc < 3 {
            return Err(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
        }
        if kind != TxnIntentKind::Copy && kind != TxnIntentKind::CopyReplace && argc != 3 {
            return Err(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
        }
        let mut children = frame
            .children()
            .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
        let _ = children.next();
        let source_bytes = children
            .next()
            .and_then(|frame| frame.as_bytes())
            .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
        let destination_bytes = children
            .next()
            .and_then(|frame| frame.as_bytes())
            .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
        let source_owner = self.owner.route_owner_with_debug(source_bytes);
        let destination_owner = self.owner.route_owner_with_debug(destination_bytes);
        Ok(SharedNothingDualKeyPlan {
            kind,
            source: VortexKey::from(source_bytes),
            destination: VortexKey::from(destination_bytes),
            source_owner,
            destination_owner,
        })
    }

    fn complete_immediate(
        &mut self,
        connection: SharedNothingConnectionId,
        command: ExecutedCommand,
    ) -> SharedNothingServerDispatch {
        let connection_index = connection.get() as usize;
        if connection_index >= self.connections.len() {
            return SharedNothingServerDispatch::Unsupported;
        }
        let sequence = self.reserve_sequence(connection_index);
        self.complete_sequence(connection_index, sequence, response_from_executed(command));
        self.metrics.local_ready = self.metrics.local_ready.saturating_add(1);
        SharedNothingServerDispatch::Ready
    }

    fn command_backpressure(&self, destination: OwnerId) -> SharedNothingServerBackpressure {
        let available = self
            .fabric
            .command_lane_available(self.local_owner, destination);
        SharedNothingServerBackpressure {
            source: self.local_owner,
            destination,
            lane: SharedNothingServerLane::Command,
            used_slots: self.fabric.lane_capacity().saturating_sub(available),
            capacity_slots: self.fabric.lane_capacity(),
        }
    }

    fn prepared_key_backpressure(&self, destination: OwnerId) -> SharedNothingServerBackpressure {
        SharedNothingServerBackpressure {
            source: self.local_owner,
            destination,
            lane: SharedNothingServerLane::Command,
            used_slots: 0,
            capacity_slots: self.fabric.lane_capacity(),
        }
    }

    pub(super) fn drain_owner_commands(&mut self, budget: usize) -> usize {
        if !self.flush_deferred_replies(budget) {
            return 0;
        }

        let retried = self.drain_deferred_blocked_commands(budget);
        let remaining = budget.saturating_sub(retried);
        if remaining == 0 {
            self.metrics.owner_commands_drained = self
                .metrics
                .owner_commands_drained
                .saturating_add(retried as u64);
            return retried;
        }

        let mut messages = Vec::with_capacity(remaining.min(64));
        let drained = self.fabric.drain_commands(
            self.local_owner,
            &mut self.command_cursor,
            remaining,
            |message| messages.push(message),
        );

        for message in messages {
            let _ = self.try_execute_owner_command(message, false);
        }

        self.metrics.owner_commands_drained = self
            .metrics
            .owner_commands_drained
            .saturating_add((retried + drained) as u64);
        retried + drained
    }

    fn drain_deferred_blocked_commands(&mut self, budget: usize) -> usize {
        if budget == 0
            || self.deferred_blocked_commands.is_empty()
            || !self.prepared_key_release_pending
        {
            return 0;
        }
        self.prepared_key_release_pending = false;

        let scan_limit = self.deferred_blocked_commands.len().min(budget);
        let mut executed = 0usize;
        for _ in 0..scan_limit {
            let Some(message) = self.deferred_blocked_commands.pop_front() else {
                break;
            };
            if self.try_execute_owner_command(message, true) {
                executed += 1;
                if executed == budget {
                    break;
                }
            }
        }
        executed
    }

    fn try_execute_owner_command(
        &mut self,
        message: SharedNothingOwnerCommand,
        was_waiting: bool,
    ) -> bool {
        if self
            .owner
            .remote_command_blocked(message.destination, &message.command)
        {
            self.deferred_blocked_commands.push_back(message);
            self.metrics.prepared_key_waits = self.metrics.prepared_key_waits.saturating_add(1);
            return false;
        }

        if was_waiting {
            self.metrics.prepared_key_retries = self.metrics.prepared_key_retries.saturating_add(1);
            let wait_nanos = duration_nanos_u64(message.enqueued_at.elapsed());
            self.metrics.prepared_key_wait_nanos_total = self
                .metrics
                .prepared_key_wait_nanos_total
                .saturating_add(wait_nanos);
            self.metrics.prepared_key_wait_nanos_max =
                self.metrics.prepared_key_wait_nanos_max.max(wait_nanos);
        }
        let releases_prepared_key = matches!(
            &message.command,
            OwnedSharedNothingCommand::TxnCommit { .. }
                | OwnedSharedNothingCommand::TxnAbort { .. }
        );
        let command =
            self.owner
                .execute_remote_command(message.destination, message.command, message.clock);
        if releases_prepared_key {
            self.prepared_key_release_pending = true;
        }
        let reply = SharedNothingOwnerReply {
            request_id: message.request_id,
            source: self.local_owner,
            destination: message.source,
            connection: message.connection,
            generation: message.generation,
            sequence: message.sequence,
            command,
            clock: message.clock,
            queue_wait: message.enqueued_at.elapsed(),
        };
        let _ = self.try_send_or_defer_reply(reply);
        true
    }

    fn flush_deferred_replies(&mut self, budget: usize) -> bool {
        let mut flushed = 0usize;
        while flushed < budget {
            let Some(reply) = self.deferred_replies.pop_front() else {
                return true;
            };
            let destination = reply.destination;
            match self.fabric.try_send_reply(reply) {
                Ok(()) => {
                    self.record_wakeup(destination);
                    flushed += 1;
                }
                Err((_, reply)) => {
                    self.deferred_replies.push_front(reply);
                    self.metrics.reply_backpressure =
                        self.metrics.reply_backpressure.saturating_add(1);
                    return false;
                }
            }
        }
        self.deferred_replies.is_empty()
    }

    fn try_send_or_defer_reply(&mut self, reply: SharedNothingOwnerReply) -> bool {
        let destination = reply.destination;
        match self.fabric.try_send_reply(reply) {
            Ok(()) => {
                self.record_wakeup(destination);
                true
            }
            Err((_, reply)) => {
                self.deferred_replies.push_back(reply);
                self.metrics.reply_backpressure = self.metrics.reply_backpressure.saturating_add(1);
                self.metrics.deferred_replies = self
                    .metrics
                    .deferred_replies
                    .max(self.deferred_replies.len() as u64);
                false
            }
        }
    }

    pub(super) fn drain_replies(&mut self, budget: usize) -> usize {
        let _ = self.flush_deferred_commands(budget);
        let mut replies = Vec::with_capacity(budget.min(64));
        let drained =
            self.fabric
                .drain_replies(self.local_owner, &mut self.reply_cursor, budget, |reply| {
                    replies.push(reply)
                });

        for reply in replies {
            let Some(pending) = self.take_pending(reply.request_id) else {
                self.metrics.orphan_reply_drops = self.metrics.orphan_reply_drops.saturating_add(1);
                continue;
            };
            if pending.connection != reply.connection || pending.generation != reply.generation {
                self.metrics.stale_reply_drops = self.metrics.stale_reply_drops.saturating_add(1);
                continue;
            }
            let connection_index = reply.connection.get() as usize;
            if connection_index >= self.connections.len() {
                self.metrics.stale_reply_drops = self.metrics.stale_reply_drops.saturating_add(1);
                continue;
            }
            match pending.kind {
                SharedNothingPendingKind::Single => {
                    self.complete_sequence(
                        connection_index,
                        reply.sequence,
                        response_from_executed(reply.command),
                    );
                }
                SharedNothingPendingKind::Aggregate {
                    aggregate_id,
                    positions,
                } => {
                    self.apply_remote_aggregate_partial(aggregate_id, &positions, reply.command);
                    self.complete_aggregate_if_ready(aggregate_id);
                }
                SharedNothingPendingKind::TxnPrepare { txn_id, owner } => {
                    self.apply_transaction_prepare(txn_id, owner, reply.command, reply.clock);
                }
                SharedNothingPendingKind::TxnFinish { txn_id, owner } => {
                    self.apply_transaction_finish(txn_id, owner, reply.command);
                }
                SharedNothingPendingKind::DualSourcePrepare { txn_id, owner } => {
                    self.apply_dual_source_prepare(txn_id, owner, reply.command, reply.clock);
                }
                SharedNothingPendingKind::DualPrepare { txn_id, owner } => {
                    self.apply_dual_prepare(txn_id, owner, reply.command);
                }
                SharedNothingPendingKind::DualFinish { txn_id, owner } => {
                    self.apply_dual_finish(txn_id, owner, reply.command);
                }
            }
            let _ = reply.queue_wait;
        }

        self.metrics.replies_drained = self.metrics.replies_drained.saturating_add(drained as u64);
        drained
    }

    pub(super) fn take_publishable(
        &mut self,
        connection: SharedNothingConnectionId,
    ) -> Option<CommandResponse> {
        let connection_index = connection.get() as usize;
        let state = self.connections.get_mut(connection_index)?;
        let response = state.completed.remove(&state.next_publish_sequence)?;
        state.next_publish_sequence = state.next_publish_sequence.wrapping_add(1);
        self.metrics.published_replies = self.metrics.published_replies.saturating_add(1);
        Some(response)
    }

    pub(super) fn disconnect(&mut self, connection: SharedNothingConnectionId) -> usize {
        let connection_index = connection.get() as usize;
        if let Some(state) = self.connections.get_mut(connection_index) {
            state.next_sequence = 0;
            state.next_publish_sequence = 0;
            state.completed.clear();
        }

        let mut removed = 0usize;
        for pending in &mut self.pending {
            if pending
                .as_ref()
                .is_some_and(|request| request.connection == connection)
            {
                *pending = None;
                removed += 1;
            }
        }
        for aggregate in &mut self.aggregates {
            if aggregate
                .as_ref()
                .is_some_and(|state| state.connection == connection)
            {
                *aggregate = None;
            }
        }
        for txn in &mut self.write_txns {
            if txn
                .as_ref()
                .is_some_and(|state| state.connection == connection)
            {
                *txn = None;
            }
        }
        for txn in &mut self.dual_key_txns {
            if txn
                .as_ref()
                .is_some_and(|state| state.connection == connection)
            {
                *txn = None;
            }
        }
        self.deferred_replies
            .retain(|reply| reply.connection != connection);
        self.deferred_commands
            .retain(|command| command.connection != connection);
        self.deferred_blocked_commands
            .retain(|command| command.connection != connection);
        self.metrics.disconnect_cleanups = self
            .metrics
            .disconnect_cleanups
            .saturating_add(removed as u64);
        removed
    }

    #[inline]
    fn reserve_sequence(&mut self, connection_index: usize) -> u64 {
        let state = &mut self.connections[connection_index];
        let sequence = state.next_sequence;
        state.next_sequence = state.next_sequence.wrapping_add(1);
        sequence
    }

    #[inline]
    fn allocate_txn_id(&mut self) -> TxnId {
        let local_sequence = self.next_txn_id & u64::from(u32::MAX);
        self.next_txn_id = self.next_txn_id.wrapping_add(1);
        TxnId::new(((self.local_owner.get() as u64) << 32) | local_sequence)
    }

    #[inline]
    fn rollback_sequence(&mut self, connection_index: usize, sequence: u64) {
        let state = &mut self.connections[connection_index];
        if state.next_sequence == sequence.wrapping_add(1) {
            state.next_sequence = sequence;
        }
    }

    #[inline]
    fn complete_sequence(
        &mut self,
        connection_index: usize,
        sequence: u64,
        response: CommandResponse,
    ) {
        self.connections[connection_index]
            .completed
            .insert(sequence, response);
    }

    fn insert_pending(&mut self, request_id: u64, pending: SharedNothingPendingRequest) {
        let index = request_id as usize;
        if index >= self.pending.len() {
            self.pending.resize_with(index + 1, || None);
        }
        debug_assert!(self.pending[index].is_none());
        self.pending[index] = Some(pending);
    }

    fn take_pending(&mut self, request_id: u64) -> Option<SharedNothingPendingRequest> {
        self.pending.get_mut(request_id as usize)?.take()
    }

    fn insert_aggregate(&mut self, aggregate_id: u64, state: SharedNothingAggregateState) {
        let index = aggregate_id as usize;
        if index >= self.aggregates.len() {
            self.aggregates.resize_with(index + 1, || None);
        }
        debug_assert!(self.aggregates[index].is_none());
        self.aggregates[index] = Some(state);
    }

    fn remove_aggregate(&mut self, aggregate_id: u64) {
        if let Some(slot) = self.aggregates.get_mut(aggregate_id as usize) {
            *slot = None;
        }
    }

    fn apply_remote_aggregate_partial(
        &mut self,
        aggregate_id: u64,
        positions: &[usize],
        command: ExecutedCommand,
    ) {
        let Some(Some(state)) = self.aggregates.get_mut(aggregate_id as usize) else {
            self.metrics.orphan_reply_drops = self.metrics.orphan_reply_drops.saturating_add(1);
            return;
        };
        apply_aggregate_partial(state, positions, command);
        state.remaining = state.remaining.saturating_sub(1);
        self.metrics.aggregate_partial_replies =
            self.metrics.aggregate_partial_replies.saturating_add(1);
    }

    fn complete_aggregate_if_ready(&mut self, aggregate_id: u64) {
        let index = aggregate_id as usize;
        let ready = self
            .aggregates
            .get(index)
            .and_then(Option::as_ref)
            .is_some_and(|state| state.remaining == 0);
        if !ready {
            return;
        }
        let Some(state) = self.aggregates.get_mut(index).and_then(Option::take) else {
            return;
        };
        let connection_index = state.connection.get() as usize;
        if connection_index >= self.connections.len() {
            return;
        }
        let response = match state.kind {
            SharedNothingAggregateKind::Mget { values } => {
                CommandResponse::Frame(vortex_proto::RespFrame::Array(Some(values)))
            }
            SharedNothingAggregateKind::Exists { count } => {
                CommandResponse::Frame(vortex_proto::RespFrame::Integer(count))
            }
        };
        self.complete_sequence(connection_index, state.sequence, response);
    }

    fn send_next_write_prepare(&mut self, txn_id: TxnId, clock: CommandClock) {
        let Some((subplan, connection, generation, sequence, kind)) =
            self.pop_next_write_prepare(txn_id)
        else {
            self.begin_transaction_finish(txn_id);
            return;
        };
        let owner = subplan.owner;
        let intent = TxnIntent::new(kind, subplan.pairs.into_boxed_slice());
        let command = OwnedSharedNothingCommand::TxnPrepare { txn_id, intent };
        if owner == self.local_owner {
            let executed = self
                .owner
                .execute_remote_command(self.local_owner, command, clock);
            self.apply_transaction_prepare(txn_id, self.local_owner, executed, clock);
            return;
        }

        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.insert_pending(
            request_id,
            SharedNothingPendingRequest {
                connection,
                generation,
                kind: SharedNothingPendingKind::TxnPrepare { txn_id, owner },
            },
        );
        let owner_command = SharedNothingOwnerCommand {
            request_id,
            source: self.local_owner,
            destination: owner,
            connection,
            generation,
            sequence,
            command,
            clock,
            enqueued_at: Instant::now(),
        };
        if self.fabric.try_send_command(owner_command).is_err() {
            let _ = self.take_pending(request_id);
            self.mark_write_prepare_failed(txn_id, TxnPrepareOutcome::Conflict);
            self.metrics.remote_backpressure = self.metrics.remote_backpressure.saturating_add(1);
            self.begin_transaction_finish(txn_id);
            return;
        }
        self.record_wakeup(owner);
        self.metrics.txn_prepare_messages = self.metrics.txn_prepare_messages.saturating_add(1);
    }

    fn pop_next_write_prepare(
        &mut self,
        txn_id: TxnId,
    ) -> Option<(
        SharedNothingWriteSubplan,
        SharedNothingConnectionId,
        SharedNothingConnectionGeneration,
        u64,
        TxnIntentKind,
    )> {
        let state = self.write_txns.get_mut(txn_state_index(txn_id))?.as_mut()?;
        let subplan = state.prepare_queue.pop_front()?;
        Some((
            subplan,
            state.connection,
            state.generation,
            state.sequence,
            state.kind,
        ))
    }

    fn mark_write_prepare_failed(&mut self, txn_id: TxnId, outcome: TxnPrepareOutcome) {
        let Some(Some(state)) = self.write_txns.get_mut(txn_state_index(txn_id)) else {
            return;
        };
        if state.failed_prepare.is_none() {
            state.failed_prepare = Some(outcome);
        }
        state.prepare_queue.clear();
        state.remaining_prepares = 0;
    }

    fn apply_transaction_prepare(
        &mut self,
        txn_id: TxnId,
        owner: OwnerId,
        command: ExecutedCommand,
        clock: CommandClock,
    ) {
        let outcome = TxnPrepareOutcome::from_code(integer_response_value(command.response));
        let Some(Some(state)) = self.write_txns.get_mut(txn_state_index(txn_id)) else {
            self.metrics.orphan_reply_drops = self.metrics.orphan_reply_drops.saturating_add(1);
            return;
        };
        if outcome == TxnPrepareOutcome::Prepared {
            state.prepared_owners.push(owner);
        } else if state.failed_prepare.is_none() {
            state.failed_prepare = Some(outcome);
        }
        state.remaining_prepares = state.remaining_prepares.saturating_sub(1);
        if state.failed_prepare.is_some() {
            state.prepare_queue.clear();
            state.remaining_prepares = 0;
            self.begin_transaction_finish(txn_id);
        } else if state.remaining_prepares == 0 {
            self.begin_transaction_finish(txn_id);
        } else {
            self.send_next_write_prepare(txn_id, clock);
        }
    }

    fn begin_transaction_finish(&mut self, txn_id: TxnId) {
        let Some(Some(state)) = self.write_txns.get_mut(txn_state_index(txn_id)) else {
            return;
        };
        let owners = state.prepared_owners.clone();
        let failed_prepare = state.failed_prepare;
        state.remaining_finishes = owners.len();
        if owners.is_empty() {
            self.complete_write_txn(txn_id);
            return;
        }

        let command_kind = if failed_prepare.is_some() {
            self.metrics.txn_abort_messages = self
                .metrics
                .txn_abort_messages
                .saturating_add(owners.len() as u64);
            TxnFinishCommandKind::Abort
        } else {
            self.metrics.txn_commit_messages = self
                .metrics
                .txn_commit_messages
                .saturating_add(owners.len() as u64);
            TxnFinishCommandKind::Commit
        };

        for owner in owners {
            self.send_transaction_finish(txn_id, owner, command_kind);
        }
    }

    fn send_transaction_finish(
        &mut self,
        txn_id: TxnId,
        owner: OwnerId,
        kind: TxnFinishCommandKind,
    ) {
        let Some(Some(state)) = self.write_txns.get(txn_state_index(txn_id)) else {
            return;
        };
        let connection = state.connection;
        let generation = state.generation;
        let sequence = state.sequence;
        let command = match kind {
            TxnFinishCommandKind::Commit => OwnedSharedNothingCommand::TxnCommit { txn_id },
            TxnFinishCommandKind::Abort => OwnedSharedNothingCommand::TxnAbort { txn_id },
        };

        if owner == self.local_owner {
            let executed = self.owner.execute_remote_command(
                self.local_owner,
                command,
                CommandClock::default(),
            );
            self.prepared_key_release_pending = true;
            self.apply_transaction_finish(txn_id, owner, executed);
            return;
        }

        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.insert_pending(
            request_id,
            SharedNothingPendingRequest {
                connection,
                generation,
                kind: SharedNothingPendingKind::TxnFinish { txn_id, owner },
            },
        );
        let owner_command = SharedNothingOwnerCommand {
            request_id,
            source: self.local_owner,
            destination: owner,
            connection,
            generation,
            sequence,
            command,
            clock: CommandClock::default(),
            enqueued_at: Instant::now(),
        };
        if let Err((_backpressure, owner_command)) = self.fabric.try_send_command(owner_command) {
            self.deferred_commands.push_back(owner_command);
            self.metrics.remote_backpressure = self.metrics.remote_backpressure.saturating_add(1);
            return;
        }
        self.record_wakeup(owner);
    }

    fn flush_deferred_commands(&mut self, budget: usize) -> bool {
        let mut flushed = 0usize;
        while flushed < budget {
            let Some(command) = self.deferred_commands.pop_front() else {
                return true;
            };
            let destination = command.destination;
            match self.fabric.try_send_command(command) {
                Ok(()) => {
                    self.record_wakeup(destination);
                    flushed += 1;
                }
                Err((_backpressure, command)) => {
                    self.deferred_commands.push_front(command);
                    return false;
                }
            }
        }
        self.deferred_commands.is_empty()
    }

    fn apply_transaction_finish(
        &mut self,
        txn_id: TxnId,
        _owner: OwnerId,
        command: ExecutedCommand,
    ) {
        let outcome = TxnFinishOutcome::from_code(integer_response_value(command.response));
        let Some(Some(state)) = self.write_txns.get_mut(txn_state_index(txn_id)) else {
            self.metrics.orphan_reply_drops = self.metrics.orphan_reply_drops.saturating_add(1);
            return;
        };
        if outcome != TxnFinishOutcome::Finished {
            state.finish_failed = true;
        }
        state.remaining_finishes = state.remaining_finishes.saturating_sub(1);
        if state.remaining_finishes == 0 {
            self.complete_write_txn(txn_id);
        }
    }

    fn complete_write_txn(&mut self, txn_id: TxnId) {
        let Some(state) = self.remove_write_txn(txn_id) else {
            return;
        };
        let connection_index = state.connection.get() as usize;
        if connection_index >= self.connections.len() {
            return;
        }
        let response = match (state.kind, state.failed_prepare, state.finish_failed) {
            (TxnIntentKind::Mset, None, false) => {
                CommandResponse::Static(vortex_engine::commands::RESP_OK)
            }
            (TxnIntentKind::MsetNx, None, false) => {
                CommandResponse::Static(vortex_engine::commands::RESP_ONE)
            }
            (TxnIntentKind::MsetNx, Some(TxnPrepareOutcome::ConditionFailed), _) => {
                self.metrics.txn_condition_aborts =
                    self.metrics.txn_condition_aborts.saturating_add(1);
                CommandResponse::Static(vortex_engine::commands::RESP_ZERO)
            }
            (TxnIntentKind::MsetNx, Some(_), _) => {
                self.metrics.txn_conflict_aborts =
                    self.metrics.txn_conflict_aborts.saturating_add(1);
                CommandResponse::Static(vortex_engine::commands::RESP_ZERO)
            }
            _ => {
                self.metrics.txn_conflict_aborts =
                    self.metrics.txn_conflict_aborts.saturating_add(1);
                CommandResponse::Static(RESP_ERR_SHARED_NOTHING_TXN_CONFLICT)
            }
        };
        self.complete_sequence(connection_index, state.sequence, response);
    }

    fn apply_dual_source_prepare(
        &mut self,
        txn_id: TxnId,
        owner: OwnerId,
        command: ExecutedCommand,
        clock: CommandClock,
    ) {
        let (outcome, capture) = source_capture_response_value(command.response);
        let Some(Some(state)) = self.dual_key_txns.get_mut(txn_state_index(txn_id)) else {
            self.metrics.orphan_reply_drops = self.metrics.orphan_reply_drops.saturating_add(1);
            return;
        };
        if outcome == TxnPrepareOutcome::Prepared {
            state.prepared_owners.push(owner);
            state.capture = capture;
        } else if state.failed_prepare.is_none() {
            state.failed_prepare = Some(outcome);
        }
        if state.failed_prepare.is_some() {
            self.begin_dual_finish(txn_id);
            return;
        }
        self.send_dual_destination_prepare(txn_id, clock);
    }

    fn send_dual_destination_prepare(&mut self, txn_id: TxnId, clock: CommandClock) {
        let Some(Some(state)) = self.dual_key_txns.get(txn_state_index(txn_id)) else {
            return;
        };
        let Some(capture) = state.capture.clone() else {
            self.mark_dual_prepare_failed(txn_id, TxnPrepareOutcome::Conflict);
            self.begin_dual_finish(txn_id);
            return;
        };
        let destination_owner = state.destination_owner;
        let connection = state.connection;
        let generation = state.generation;
        let sequence = state.sequence;
        let command = OwnedSharedNothingCommand::TxnPrepare {
            txn_id,
            intent: TxnIntent::from_actions(
                state.kind,
                destination_actions(state.kind, state.destination.clone(), capture)
                    .into_boxed_slice(),
            ),
        };

        if destination_owner == self.local_owner {
            let executed = self
                .owner
                .execute_remote_command(self.local_owner, command, clock);
            self.apply_dual_prepare(txn_id, destination_owner, executed);
            return;
        }

        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.insert_pending(
            request_id,
            SharedNothingPendingRequest {
                connection,
                generation,
                kind: SharedNothingPendingKind::DualPrepare {
                    txn_id,
                    owner: destination_owner,
                },
            },
        );
        let owner_command = SharedNothingOwnerCommand {
            request_id,
            source: self.local_owner,
            destination: destination_owner,
            connection,
            generation,
            sequence,
            command,
            clock,
            enqueued_at: Instant::now(),
        };
        if let Err((_backpressure, owner_command)) = self.fabric.try_send_command(owner_command) {
            self.deferred_commands.push_back(owner_command);
            self.metrics.remote_backpressure = self.metrics.remote_backpressure.saturating_add(1);
            return;
        }
        self.record_wakeup(destination_owner);
        self.metrics.txn_prepare_messages = self.metrics.txn_prepare_messages.saturating_add(1);
    }

    fn apply_dual_prepare(&mut self, txn_id: TxnId, owner: OwnerId, command: ExecutedCommand) {
        let outcome = TxnPrepareOutcome::from_code(integer_response_value(command.response));
        let Some(Some(state)) = self.dual_key_txns.get_mut(txn_state_index(txn_id)) else {
            self.metrics.orphan_reply_drops = self.metrics.orphan_reply_drops.saturating_add(1);
            return;
        };
        if outcome == TxnPrepareOutcome::Prepared {
            state.prepared_owners.push(owner);
        } else if state.failed_prepare.is_none() {
            state.failed_prepare = Some(outcome);
        }
        self.begin_dual_finish(txn_id);
    }

    fn mark_dual_prepare_failed(&mut self, txn_id: TxnId, outcome: TxnPrepareOutcome) {
        if let Some(Some(state)) = self.dual_key_txns.get_mut(txn_state_index(txn_id))
            && state.failed_prepare.is_none()
        {
            state.failed_prepare = Some(outcome);
        }
    }

    fn begin_dual_finish(&mut self, txn_id: TxnId) {
        let Some(Some(state)) = self.dual_key_txns.get_mut(txn_state_index(txn_id)) else {
            return;
        };
        let owners = state.prepared_owners.clone();
        let failed_prepare = state.failed_prepare;
        state.remaining_finishes = owners.len();
        if owners.is_empty() {
            self.complete_dual_key_txn(txn_id);
            return;
        }

        let command_kind = if failed_prepare.is_some() {
            self.metrics.txn_abort_messages = self
                .metrics
                .txn_abort_messages
                .saturating_add(owners.len() as u64);
            TxnFinishCommandKind::Abort
        } else {
            self.metrics.txn_commit_messages = self
                .metrics
                .txn_commit_messages
                .saturating_add(owners.len() as u64);
            TxnFinishCommandKind::Commit
        };

        for owner in owners {
            self.send_dual_finish(txn_id, owner, command_kind);
        }
    }

    fn send_dual_finish(&mut self, txn_id: TxnId, owner: OwnerId, kind: TxnFinishCommandKind) {
        let Some(Some(state)) = self.dual_key_txns.get(txn_state_index(txn_id)) else {
            return;
        };
        let connection = state.connection;
        let generation = state.generation;
        let sequence = state.sequence;
        let command = match kind {
            TxnFinishCommandKind::Commit => OwnedSharedNothingCommand::TxnCommit { txn_id },
            TxnFinishCommandKind::Abort => OwnedSharedNothingCommand::TxnAbort { txn_id },
        };

        if owner == self.local_owner {
            let executed = self.owner.execute_remote_command(
                self.local_owner,
                command,
                CommandClock::default(),
            );
            self.prepared_key_release_pending = true;
            self.apply_dual_finish(txn_id, owner, executed);
            return;
        }

        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.insert_pending(
            request_id,
            SharedNothingPendingRequest {
                connection,
                generation,
                kind: SharedNothingPendingKind::DualFinish { txn_id, owner },
            },
        );
        let owner_command = SharedNothingOwnerCommand {
            request_id,
            source: self.local_owner,
            destination: owner,
            connection,
            generation,
            sequence,
            command,
            clock: CommandClock::default(),
            enqueued_at: Instant::now(),
        };
        if let Err((_backpressure, owner_command)) = self.fabric.try_send_command(owner_command) {
            self.deferred_commands.push_back(owner_command);
            self.metrics.remote_backpressure = self.metrics.remote_backpressure.saturating_add(1);
            return;
        }
        self.record_wakeup(owner);
    }

    fn apply_dual_finish(&mut self, txn_id: TxnId, _owner: OwnerId, command: ExecutedCommand) {
        let outcome = TxnFinishOutcome::from_code(integer_response_value(command.response));
        let Some(Some(state)) = self.dual_key_txns.get_mut(txn_state_index(txn_id)) else {
            self.metrics.orphan_reply_drops = self.metrics.orphan_reply_drops.saturating_add(1);
            return;
        };
        if outcome != TxnFinishOutcome::Finished {
            state.finish_failed = true;
        }
        state.remaining_finishes = state.remaining_finishes.saturating_sub(1);
        if state.remaining_finishes == 0 {
            self.complete_dual_key_txn(txn_id);
        }
    }

    fn complete_dual_key_txn(&mut self, txn_id: TxnId) {
        let Some(state) = self.remove_dual_key_txn(txn_id) else {
            return;
        };
        let connection_index = state.connection.get() as usize;
        if connection_index >= self.connections.len() {
            return;
        }
        let response = dual_key_response(
            state.kind,
            state.failed_prepare,
            state.finish_failed,
            &mut self.metrics,
        );
        self.complete_sequence(connection_index, state.sequence, response);
    }

    #[inline]
    fn write_txn_is_complete(&self, txn_id: TxnId) -> bool {
        self.write_txns
            .get(txn_state_index(txn_id))
            .and_then(Option::as_ref)
            .is_none()
    }

    #[inline]
    fn dual_key_txn_is_complete(&self, txn_id: TxnId) -> bool {
        self.dual_key_txns
            .get(txn_state_index(txn_id))
            .and_then(Option::as_ref)
            .is_none()
    }

    fn insert_write_txn(&mut self, txn_id: TxnId, state: SharedNothingWriteTxnState) {
        let index = txn_state_index(txn_id);
        if index >= self.write_txns.len() {
            self.write_txns.resize_with(index + 1, || None);
        }
        debug_assert!(self.write_txns[index].is_none());
        self.write_txns[index] = Some(state);
    }

    fn remove_write_txn(&mut self, txn_id: TxnId) -> Option<SharedNothingWriteTxnState> {
        self.write_txns.get_mut(txn_state_index(txn_id))?.take()
    }

    fn insert_dual_key_txn(&mut self, txn_id: TxnId, state: SharedNothingDualKeyTxnState) {
        let index = txn_state_index(txn_id);
        if index >= self.dual_key_txns.len() {
            self.dual_key_txns.resize_with(index + 1, || None);
        }
        debug_assert!(self.dual_key_txns[index].is_none());
        self.dual_key_txns[index] = Some(state);
    }

    fn remove_dual_key_txn(&mut self, txn_id: TxnId) -> Option<SharedNothingDualKeyTxnState> {
        self.dual_key_txns.get_mut(txn_state_index(txn_id))?.take()
    }

    fn abort_local_prepared_txn(&mut self, txn_id: TxnId, clock: CommandClock) {
        let _ = self.owner.execute_remote_command(
            self.local_owner,
            OwnedSharedNothingCommand::TxnAbort { txn_id },
            clock,
        );
    }

    fn record_wakeup(&mut self, owner: OwnerId) {
        if self.fabric.wake_owner(owner) {
            self.metrics.wakeups_sent = self.metrics.wakeups_sent.saturating_add(1);
        } else {
            self.metrics.wakeup_failures = self.metrics.wakeup_failures.saturating_add(1);
        }
    }
}

#[derive(Clone, Copy)]
enum TxnFinishCommandKind {
    Commit,
    Abort,
}

fn apply_aggregate_partial(
    state: &mut SharedNothingAggregateState,
    positions: &[usize],
    command: ExecutedCommand,
) {
    match &mut state.kind {
        SharedNothingAggregateKind::Mget { values } => {
            let frames = match command.response {
                CmdResult::Resp(vortex_proto::RespFrame::Array(Some(frames))) => frames,
                _ => vec![vortex_proto::RespFrame::null_bulk_string(); positions.len()],
            };
            for (position, frame) in positions.iter().copied().zip(frames.into_iter()) {
                if let Some(slot) = values.get_mut(position) {
                    *slot = frame;
                }
            }
        }
        SharedNothingAggregateKind::Exists { count } => {
            *count += integer_response_value(command.response);
        }
    }
}

fn integer_response_value(response: CmdResult) -> i64 {
    match response {
        CmdResult::Static(bytes) if bytes == b":0\r\n" => 0,
        CmdResult::Static(bytes) if bytes == b":1\r\n" => 1,
        CmdResult::Resp(vortex_proto::RespFrame::Integer(value)) => value,
        _ => 0,
    }
}

fn source_capture_response_value(
    response: CmdResult,
) -> (TxnPrepareOutcome, Option<SharedNothingDualKeyCapture>) {
    let CmdResult::Resp(vortex_proto::RespFrame::Array(Some(mut frames))) = response else {
        return (TxnPrepareOutcome::Conflict, None);
    };
    if frames.is_empty() {
        return (TxnPrepareOutcome::Conflict, None);
    }
    let outcome = match frames.remove(0) {
        vortex_proto::RespFrame::Integer(code) => TxnPrepareOutcome::from_code(code),
        _ => TxnPrepareOutcome::Conflict,
    };
    if outcome != TxnPrepareOutcome::Prepared || frames.len() < 2 {
        return (outcome, None);
    }
    let value_bytes = match frames.remove(0) {
        vortex_proto::RespFrame::BulkString(Some(bytes)) => bytes,
        _ => return (TxnPrepareOutcome::Conflict, None),
    };
    let ttl_deadline = match frames.remove(0) {
        vortex_proto::RespFrame::BulkString(Some(bytes)) => parse_u64_bytes(&bytes).unwrap_or(0),
        vortex_proto::RespFrame::Integer(value) if value >= 0 => value as u64,
        _ => 0,
    };
    (
        outcome,
        Some(SharedNothingDualKeyCapture {
            value: value_from_bytes(&value_bytes),
            ttl_deadline,
        }),
    )
}

fn destination_actions(
    kind: TxnIntentKind,
    destination: VortexKey,
    capture: SharedNothingDualKeyCapture,
) -> Vec<TxnAction> {
    let mut actions = Vec::with_capacity(2);
    if matches!(kind, TxnIntentKind::RenameNx | TxnIntentKind::Copy) {
        actions.push(TxnAction::AssertAbsent {
            key: destination.clone(),
        });
    }
    actions.push(TxnAction::Set {
        key: destination,
        value: capture.value,
        ttl_deadline: capture.ttl_deadline,
    });
    actions
}

fn dual_key_response(
    kind: TxnIntentKind,
    failed_prepare: Option<TxnPrepareOutcome>,
    finish_failed: bool,
    metrics: &mut SharedNothingServerMetrics,
) -> CommandResponse {
    match (kind, failed_prepare, finish_failed) {
        (TxnIntentKind::Rename, None, false) => CommandResponse::Static(RESP_OK),
        (TxnIntentKind::RenameNx, None, false) => CommandResponse::Static(RESP_ONE),
        (TxnIntentKind::Copy | TxnIntentKind::CopyReplace, None, false) => {
            CommandResponse::Static(RESP_ONE)
        }
        (
            TxnIntentKind::Rename | TxnIntentKind::RenameNx,
            Some(TxnPrepareOutcome::SourceMissing),
            _,
        ) => {
            metrics.txn_condition_aborts = metrics.txn_condition_aborts.saturating_add(1);
            CommandResponse::Static(ERR_NO_SUCH_KEY)
        }
        (
            TxnIntentKind::Copy | TxnIntentKind::CopyReplace,
            Some(TxnPrepareOutcome::SourceMissing),
            _,
        )
        | (
            TxnIntentKind::RenameNx | TxnIntentKind::Copy,
            Some(TxnPrepareOutcome::DestinationExists),
            _,
        ) => {
            metrics.txn_condition_aborts = metrics.txn_condition_aborts.saturating_add(1);
            CommandResponse::Static(RESP_ZERO)
        }
        _ => {
            metrics.txn_conflict_aborts = metrics.txn_conflict_aborts.saturating_add(1);
            CommandResponse::Static(RESP_ERR_SHARED_NOTHING_TXN_CONFLICT)
        }
    }
}

fn parse_copy_kind(frame: &vortex_proto::FrameRef<'_>) -> Result<TxnIntentKind, ExecutedCommand> {
    let argc = frame
        .element_count()
        .map(|argc| argc as usize)
        .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
    if argc < 3 {
        return Err(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
    }
    let mut children = frame
        .children()
        .ok_or_else(|| ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)))?;
    let _ = children.next();
    let _ = children.next();
    let _ = children.next();
    let mut replace = false;
    while let Some(option) = children.next() {
        let Some(bytes) = option.as_bytes() else {
            return Err(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
        };
        if bytes.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
            continue;
        }
        if bytes.eq_ignore_ascii_case(b"DB") {
            if children.next().and_then(|frame| frame.as_bytes()).is_none() {
                return Err(ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX)));
            }
        }
    }
    Ok(if replace {
        TxnIntentKind::CopyReplace
    } else {
        TxnIntentKind::Copy
    })
}

fn parse_u64_bytes(bytes: &[u8]) -> Option<u64> {
    let mut value = 0u64;
    if bytes.is_empty() {
        return None;
    }
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?;
        value = value.checked_add(u64::from(byte - b'0'))?;
    }
    Some(value)
}

#[inline]
fn txn_state_index(txn_id: TxnId) -> usize {
    (txn_id.get() & u64::from(u32::MAX)) as usize
}

fn duration_nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn response_from_executed(executed: ExecutedCommand) -> CommandResponse {
    match executed.response {
        CmdResult::Static(buf) => CommandResponse::Static(buf),
        CmdResult::Inline(inline) => CommandResponse::Inline(inline),
        CmdResult::Resp(frame) => CommandResponse::Frame(frame),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::PollingBackend;
    use vortex_engine::owner::MailboxLane;
    use vortex_engine::owner::OwnerTopology;
    use vortex_proto::{RespSerializer, RespTape};

    fn adapter<const N: usize>() -> SharedNothingReactorAdapter<N> {
        SharedNothingReactorAdapter::new(4, 64, 128, 0).expect("adapter builds")
    }

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

    fn response_bytes(response: CommandResponse) -> Vec<u8> {
        match response {
            CommandResponse::Static(bytes) => bytes.to_vec(),
            CommandResponse::Inline(inline) => inline.as_bytes().to_vec(),
            CommandResponse::Owned(bytes) => bytes.into_vec(),
            CommandResponse::Frame(frame) => {
                let mut out = bytes::BytesMut::new();
                RespSerializer::serialize(&frame, &mut out);
                out.to_vec()
            }
        }
    }

    fn key_for(config: TopologyConfig, owner: OwnerId, prefix: &str) -> Vec<u8> {
        let topology = OwnerTopology::new(config);
        (0..10_000)
            .map(|index| format!("{prefix}:{index:04}").into_bytes())
            .find(|key| topology.route_key(key).owner() == owner)
            .expect("routed key found")
    }

    fn server_runtimes<const N: usize>() -> (
        Arc<SharedNothingServerFabric<N>>,
        SharedNothingServerRuntime<N>,
        SharedNothingServerRuntime<N>,
    ) {
        let config = TopologyConfig::new(2, 64).expect("valid topology");
        let fabric = Arc::new(SharedNothingServerFabric::new(config).expect("fabric builds"));
        let owner0 =
            SharedNothingServerRuntime::new(Arc::clone(&fabric), 0, 16, 64).expect("owner0");
        let owner1 =
            SharedNothingServerRuntime::new(Arc::clone(&fabric), 1, 16, 64).expect("owner1");
        (fabric, owner0, owner1)
    }

    fn dispatch_parts<const N: usize>(
        runtime: &mut SharedNothingServerRuntime<N>,
        connection: SharedNothingConnectionId,
        generation: SharedNothingConnectionGeneration,
        name: &[u8],
        parts: &[&[u8]],
        clock: CommandClock,
    ) -> SharedNothingServerDispatch {
        let wire = make_resp(parts);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        runtime.dispatch_ingress(connection, generation, name, &frame, clock)
    }

    fn drive_pair_until_response<const N: usize>(
        owner0: &mut SharedNothingServerRuntime<N>,
        owner1: &mut SharedNothingServerRuntime<N>,
        connection: SharedNothingConnectionId,
    ) -> Vec<u8> {
        if let Some(response) = owner0.take_publishable(connection) {
            return response_bytes(response);
        }
        for _ in 0..32 {
            let _ = owner0.drain_owner_commands(16);
            let _ = owner1.drain_owner_commands(16);
            let _ = owner0.drain_replies(16);
            let _ = owner1.drain_replies(16);
            if let Some(response) = owner0.take_publishable(connection) {
                return response_bytes(response);
            }
        }
        panic!("shared-nothing response did not publish");
    }

    fn owner<const N: usize>(adapter: &SharedNothingReactorAdapter<N>, index: usize) -> OwnerId {
        adapter.owner_id(index).expect("owner exists")
    }

    fn capsule<const N: usize>(
        adapter: &SharedNothingReactorAdapter<N>,
        index: usize,
    ) -> KeyCapsuleId {
        adapter.capsule_id(index).expect("capsule exists")
    }

    #[test]
    fn shared_nothing_remote_accept_returns_pending_without_waiting() {
        let mut adapter = adapter::<8>();
        let destination = owner(&adapter, 1);
        let capsule = capsule(&adapter, 0);
        let connection = SharedNothingConnectionId::new(0);

        let result: SharedNothingDispatchResult<&'static [u8]> =
            adapter.accept_remote(destination, capsule, connection, 32);

        assert_eq!(
            result,
            SharedNothingDispatchResult::Pending {
                request_id: 0,
                connection,
                retained_bytes: 32,
            }
        );
        assert_eq!(adapter.pending_count(), 1);
        assert_eq!(adapter.blocked_remote_wait_nanos(), 0);
        assert!(adapter.published_for_connection(connection).is_empty());
    }

    #[test]
    fn shared_nothing_slow_remote_owner_does_not_block_other_connection_progress() {
        let mut adapter = adapter::<16>();
        let delayed_owner = owner(&adapter, 1);
        let capsule = capsule(&adapter, 0);
        let delayed_connection = SharedNothingConnectionId::new(0);
        let other_connection = SharedNothingConnectionId::new(1);

        let _: SharedNothingDispatchResult<&'static [u8]> =
            adapter.accept_remote(delayed_owner, capsule, delayed_connection, 0);

        for _ in 0..100 {
            adapter.record_reactor_activation();
            let result = adapter.accept_local_ready(other_connection, b"+OK\r\n" as &'static [u8]);
            assert!(matches!(result, SharedNothingDispatchResult::Ready { .. }));
        }

        assert_eq!(adapter.pending_count(), 1);
        assert_eq!(adapter.blocked_remote_wait_nanos(), 0);
        assert_eq!(adapter.reactor_activations_while_remote_pending(), 100);
        assert_eq!(
            adapter.published_for_connection(other_connection).len(),
            100
        );
        assert!(
            adapter
                .published_for_connection(delayed_connection)
                .is_empty()
        );

        adapter
            .drain_owner_commands(delayed_owner, 1)
            .expect("owner drain succeeds");
        adapter
            .drain_ingress_replies(1)
            .expect("reply drain succeeds");

        assert_eq!(adapter.pending_count(), 0);
        assert_eq!(
            adapter.published_for_connection(delayed_connection).len(),
            1
        );
    }

    #[test]
    fn shared_nothing_same_connection_ready_waits_behind_delayed_remote() {
        let mut adapter = adapter::<16>();
        let delayed_owner = owner(&adapter, 1);
        let capsule = capsule(&adapter, 0);
        let connection = SharedNothingConnectionId::new(0);

        let _: SharedNothingDispatchResult<&'static [u8]> =
            adapter.accept_remote(delayed_owner, capsule, connection, 0);
        let ready = adapter.accept_local_ready(connection, b"+LOCAL\r\n" as &'static [u8]);
        assert!(matches!(ready, SharedNothingDispatchResult::Ready { .. }));
        assert!(adapter.published_for_connection(connection).is_empty());

        adapter
            .drain_owner_commands(delayed_owner, 1)
            .expect("owner drain succeeds");
        adapter
            .drain_ingress_replies(1)
            .expect("reply drain succeeds");

        let published = adapter.published_for_connection(connection);
        assert_eq!(published.len(), 2);
        assert_eq!(published[0].sequence, 0);
        assert_eq!(published[1].sequence, 1);
    }

    #[test]
    fn shared_nothing_disconnect_cleanup_turns_late_reply_into_orphan_drop() {
        let mut adapter = adapter::<16>();
        let destination = owner(&adapter, 1);
        let capsule = capsule(&adapter, 0);
        let connection = SharedNothingConnectionId::new(0);

        let _: SharedNothingDispatchResult<&'static [u8]> =
            adapter.accept_remote(destination, capsule, connection, 0);
        assert_eq!(adapter.disconnect(connection), 1);
        assert_eq!(adapter.pending_count(), 0);

        adapter
            .drain_owner_commands(destination, 1)
            .expect("owner drain succeeds");
        adapter
            .drain_ingress_replies(1)
            .expect("reply drain succeeds");

        assert!(adapter.published_for_connection(connection).is_empty());
        assert_eq!(adapter.metrics().orphan_replies, 1);
        assert_eq!(adapter.metrics().disconnect_cleanups, 1);
    }

    #[test]
    fn shared_nothing_queue_full_backpressures_before_acceptance() {
        let mut adapter = adapter::<2>();
        let destination = owner(&adapter, 1);
        let capsule = capsule(&adapter, 0);
        let mut accepted = 0usize;
        let backpressure = loop {
            let connection = SharedNothingConnectionId::new(accepted as u32);
            let result: SharedNothingDispatchResult<&'static [u8]> =
                adapter.accept_remote(destination, capsule, connection, 0);
            match result {
                SharedNothingDispatchResult::Pending { .. } => {
                    accepted += 1;
                    assert!(
                        accepted < 16,
                        "small ring should backpressure before this many accepts"
                    );
                }
                SharedNothingDispatchResult::Backpressure(backpressure) => break backpressure,
                SharedNothingDispatchResult::Ready { .. }
                | SharedNothingDispatchResult::Unsupported => {
                    panic!("expected pending or backpressure");
                }
            }
        };

        assert_eq!(backpressure.detail.lane, MailboxLane::Command);
        assert_eq!(adapter.pending_count(), accepted);
        assert_eq!(adapter.metrics().accepted_commands, accepted as u64);
        assert_eq!(adapter.metrics().rejected_commands, 1);
    }

    #[test]
    fn server_runtime_remote_set_get_returns_without_blocking() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "remote-server",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(7);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"SET", &key, b"value"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        let dispatch = owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock);
        assert!(matches!(dispatch, SharedNothingServerDispatch::Pending));
        assert_eq!(owner0.blocked_remote_wait_nanos(), 0);
        assert!(owner0.take_publishable(connection).is_none());

        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("SET response")),
            b"+OK\r\n"
        );

        let wire = make_resp(&[b"GET", &key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        let dispatch = owner0.dispatch_ingress(connection, generation, b"GET", &frame, clock);
        assert!(matches!(dispatch, SharedNothingServerDispatch::Pending));
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("GET response")),
            b"$5\r\nvalue\r\n"
        );
    }

    #[test]
    fn server_runtime_incr_local_and_remote_keys() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let local_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "incr-local",
        );
        let remote_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "incr-remote",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"INCR", &local_key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"INCR", &frame, clock),
            SharedNothingServerDispatch::Ready
        ));
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("local INCR")),
            b":1\r\n"
        );

        let wire = make_resp(&[b"INCR", &remote_key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"INCR", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("remote INCR")),
            b":1\r\n"
        );

        let wire = make_resp(&[b"SET", &local_key, b"not-int"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        let _ = owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock);
        let _ = owner0.take_publishable(connection);

        let wire = make_resp(&[b"INCR", &local_key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        let _ = owner0.dispatch_ingress(connection, generation, b"INCR", &frame, clock);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("bad INCR")),
            b"-ERR value is not an integer or out of range\r\n"
        );
    }

    #[test]
    fn server_runtime_same_connection_order_waits_for_delayed_remote() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let remote_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "remote-order",
        );
        let local_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "local-order",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"SET", &remote_key, b"remote"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));

        let wire = make_resp(&[b"SET", &local_key, b"local"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock),
            SharedNothingServerDispatch::Ready
        ));
        assert!(owner0.take_publishable(connection).is_none());

        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(
                owner0
                    .take_publishable(connection)
                    .expect("remote response")
            ),
            b"+OK\r\n"
        );
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("local response")),
            b"+OK\r\n"
        );
    }

    #[test]
    fn server_runtime_disconnect_drops_late_reply() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "disconnect",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(3);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"SET", &key, b"value"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));

        assert_eq!(owner0.disconnect(connection), 1);
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert!(owner0.take_publishable(connection).is_none());
        assert_eq!(owner0.metrics().orphan_reply_drops, 1);
        assert_eq!(owner0.metrics().disconnect_cleanups, 1);
    }

    #[test]
    fn server_runtime_mget_scatter_gather_preserves_order() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let local_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "mget-local",
        );
        let remote_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "mget-remote",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"SET", &local_key, b"local"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock),
            SharedNothingServerDispatch::Ready
        ));
        let _ = owner0.take_publishable(connection);

        let wire = make_resp(&[b"SET", &remote_key, b"remote"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        let _ = owner0.take_publishable(connection);

        let wire = make_resp(&[b"MGET", &remote_key, &local_key, b"missing"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"MGET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);

        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("MGET response")),
            b"*3\r\n$6\r\nremote\r\n$5\r\nlocal\r\n$-1\r\n"
        );
        assert_eq!(owner0.metrics().aggregates_accepted, 1);
        assert_eq!(owner0.metrics().aggregate_remote_subplans, 1);
    }

    #[test]
    fn server_runtime_multi_exists_counts_duplicates_across_owners() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let local_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "exists-local",
        );
        let remote_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "exists-remote",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"SET", &local_key, b"1"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        let _ = owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock);
        let _ = owner0.take_publishable(connection);

        let wire = make_resp(&[b"SET", &remote_key, b"1"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        let _ = owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock);
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        let _ = owner0.take_publishable(connection);

        let wire = make_resp(&[b"EXISTS", &remote_key, &local_key, &remote_key, b"missing"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"EXISTS", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(
                owner0
                    .take_publishable(connection)
                    .expect("EXISTS response")
            ),
            b":3\r\n"
        );
    }

    #[test]
    fn server_runtime_rename_cross_owner_moves_value_and_ttl() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let source = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "rename-source",
        );
        let destination = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "rename-destination",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"SET",
            &[b"SET", &source, b"moved", b"PX", b"10000"],
            clock,
        );
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b"+OK\r\n"
        );

        assert!(matches!(
            dispatch_parts(
                &mut owner0,
                connection,
                generation,
                b"RENAME",
                &[b"RENAME", &source, &destination],
                clock,
            ),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b"+OK\r\n"
        );

        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"GET",
            &[b"GET", &source],
            clock,
        );
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b"$-1\r\n"
        );
        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"GET",
            &[b"GET", &destination],
            clock,
        );
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b"$5\r\nmoved\r\n"
        );
        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"PTTL",
            &[b"PTTL", &destination],
            clock,
        );
        let ttl = drive_pair_until_response(&mut owner0, &mut owner1, connection);
        assert!(
            ttl.starts_with(b":") && ttl != b":-1\r\n" && ttl != b":-2\r\n",
            "destination should keep source TTL, got {ttl:?}"
        );
    }

    #[test]
    fn server_runtime_renamenx_cross_owner_existing_destination_aborts() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let source = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "renamenx-source",
        );
        let destination = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "renamenx-destination",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"SET",
            &[b"SET", &source, b"source-value"],
            clock,
        );
        let _ = drive_pair_until_response(&mut owner0, &mut owner1, connection);
        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"SET",
            &[b"SET", &destination, b"dest-value"],
            clock,
        );
        let _ = drive_pair_until_response(&mut owner0, &mut owner1, connection);

        assert!(matches!(
            dispatch_parts(
                &mut owner0,
                connection,
                generation,
                b"RENAMENX",
                &[b"RENAMENX", &source, &destination],
                clock,
            ),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b":0\r\n"
        );

        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"GET",
            &[b"GET", &source],
            clock,
        );
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b"$12\r\nsource-value\r\n"
        );
        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"GET",
            &[b"GET", &destination],
            clock,
        );
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b"$10\r\ndest-value\r\n"
        );
    }

    #[test]
    fn server_runtime_copy_cross_owner_preserves_source_and_replace_flag() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let source = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "copy-source",
        );
        let destination = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "copy-destination",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"SET",
            &[b"SET", &source, b"first"],
            clock,
        );
        let _ = drive_pair_until_response(&mut owner0, &mut owner1, connection);
        assert!(matches!(
            dispatch_parts(
                &mut owner0,
                connection,
                generation,
                b"COPY",
                &[b"COPY", &source, &destination],
                clock,
            ),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b":1\r\n"
        );

        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"SET",
            &[b"SET", &source, b"second"],
            clock,
        );
        let _ = drive_pair_until_response(&mut owner0, &mut owner1, connection);
        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"COPY",
            &[b"COPY", &source, &destination],
            clock,
        );
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b":0\r\n"
        );
        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"GET",
            &[b"GET", &destination],
            clock,
        );
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b"$5\r\nfirst\r\n"
        );

        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"COPY",
            &[b"COPY", &source, &destination, b"REPLACE"],
            clock,
        );
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b":1\r\n"
        );
        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"GET",
            &[b"GET", &source],
            clock,
        );
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b"$6\r\nsecond\r\n"
        );
        let _ = dispatch_parts(
            &mut owner0,
            connection,
            generation,
            b"GET",
            &[b"GET", &destination],
            clock,
        );
        assert_eq!(
            drive_pair_until_response(&mut owner0, &mut owner1, connection),
            b"$6\r\nsecond\r\n"
        );
    }

    #[test]
    fn server_runtime_mset_cross_owner_commits_after_all_prepares() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let local_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "mset-local",
        );
        let remote_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "mset-remote",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[
            b"MSET",
            &local_key,
            b"local-value",
            &remote_key,
            b"remote-value",
        ]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"MSET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert!(owner0.take_publishable(connection).is_none());

        assert_eq!(owner1.drain_owner_commands(1), 1); // remote prepare
        assert_eq!(owner0.drain_replies(1), 1); // coordinator sends commit
        assert!(owner0.take_publishable(connection).is_none());
        assert_eq!(owner1.drain_owner_commands(1), 1); // remote commit
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("MSET response")),
            b"+OK\r\n"
        );

        let wire = make_resp(&[b"GET", &local_key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"GET", &frame, clock),
            SharedNothingServerDispatch::Ready
        ));
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("local GET")),
            b"$11\r\nlocal-value\r\n"
        );

        let wire = make_resp(&[b"GET", &remote_key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"GET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("remote GET")),
            b"$12\r\nremote-value\r\n"
        );
    }

    #[test]
    fn server_runtime_mset_opposite_owner_order_does_not_deadlock() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let owner0_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "mset-owner-zero",
        );
        let owner1_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "mset-owner-one",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire0 = make_resp(&[
            b"MSET",
            &owner0_key,
            b"owner0-zero",
            &owner1_key,
            b"owner0-one",
        ]);
        let tape0 = RespTape::parse_pipeline(&wire0).expect("valid RESP");
        let frame0 = tape0.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"MSET", &frame0, clock),
            SharedNothingServerDispatch::Pending
        ));

        let wire1 = make_resp(&[
            b"MSET",
            &owner1_key,
            b"owner1-one",
            &owner0_key,
            b"owner1-zero",
        ]);
        let tape1 = RespTape::parse_pipeline(&wire1).expect("valid RESP");
        let frame1 = tape1.iter().next().expect("one frame");
        assert!(matches!(
            owner1.dispatch_ingress(connection, generation, b"MSET", &frame1, clock),
            SharedNothingServerDispatch::Pending
        ));

        let mut response0 = None;
        let mut response1 = None;
        for _ in 0..32 {
            let _ = owner0.drain_owner_commands(8);
            let _ = owner1.drain_owner_commands(8);
            let _ = owner0.drain_replies(8);
            let _ = owner1.drain_replies(8);
            if response0.is_none() {
                response0 = owner0.take_publishable(connection);
            }
            if response1.is_none() {
                response1 = owner1.take_publishable(connection);
            }
            if response0.is_some() && response1.is_some() {
                break;
            }
        }

        assert_eq!(
            response_bytes(response0.expect("owner0 MSET response")),
            b"+OK\r\n"
        );
        assert_eq!(
            response_bytes(response1.expect("owner1 MSET response")),
            b"+OK\r\n"
        );
        assert!(owner0.metrics().prepared_key_waits >= 1);
        assert!(owner0.metrics().prepared_key_retries >= 1);
    }

    #[test]
    fn server_runtime_local_prepared_key_waits_without_tryagain() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let local_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "wait-local",
        );
        let remote_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "wait-remote",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"MSET", &local_key, b"local", &remote_key, b"remote"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"MSET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));

        let wire = make_resp(&[b"GET", &local_key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"GET", &frame, clock),
            SharedNothingServerDispatch::Backpressure(_)
        ));
        assert!(owner0.take_publishable(connection).is_none());
        assert_eq!(owner0.metrics().prepared_key_waits, 1);

        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("MSET response")),
            b"+OK\r\n"
        );

        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"GET", &frame, clock),
            SharedNothingServerDispatch::Ready
        ));
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("GET response")),
            b"$5\r\nlocal\r\n"
        );
    }

    #[test]
    fn server_runtime_remote_prepared_key_defers_until_commit() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let local_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "wait-remote-local",
        );
        let remote_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "wait-remote-key",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"MSET", &local_key, b"local", &remote_key, b"remote"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"MSET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(owner1.drain_owner_commands(1), 1); // remote prepare installs intent

        let wire = make_resp(&[b"GET", &remote_key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"GET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert!(owner0.take_publishable(connection).is_none());
        assert!(owner1.metrics().prepared_key_waits >= 1);
        assert_eq!(owner1.drain_owner_commands(2), 0);
        assert_eq!(owner1.metrics().prepared_key_retries, 0);

        assert_eq!(owner0.drain_replies(1), 1); // remote prepare reply sends commit
        assert_eq!(owner1.drain_owner_commands(2), 1); // commit overtakes blocked GET
        assert_eq!(owner0.drain_replies(1), 1); // commit reply completes MSET
        assert_eq!(owner1.drain_owner_commands(2), 1); // deferred GET retries
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(owner1.metrics().prepared_key_retries, 1);

        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("MSET response")),
            b"+OK\r\n"
        );
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("GET response")),
            b"$6\r\nremote\r\n"
        );
    }

    #[test]
    fn server_runtime_msetnx_cross_owner_abort_leaves_no_partial_write() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<16>();
        let local_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "msetnx-local",
        );
        let remote_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "msetnx-remote",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"SET", &remote_key, b"old"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        let _ = owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock);
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        let _ = owner0.take_publishable(connection);

        let wire = make_resp(&[b"MSETNX", &local_key, b"new", &remote_key, b"new"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"MSETNX", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(owner1.drain_owner_commands(1), 1); // remote prepare rejects
        assert_eq!(owner0.drain_replies(1), 1); // local abort and response
        assert_eq!(
            response_bytes(
                owner0
                    .take_publishable(connection)
                    .expect("MSETNX response")
            ),
            b":0\r\n"
        );

        let wire = make_resp(&[b"GET", &local_key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        let _ = owner0.dispatch_ingress(connection, generation, b"GET", &frame, clock);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("local miss")),
            b"$-1\r\n"
        );

        let wire = make_resp(&[b"GET", &remote_key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        let _ = owner0.dispatch_ingress(connection, generation, b"GET", &frame, clock);
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("remote old")),
            b"$3\r\nold\r\n"
        );
    }

    #[test]
    fn server_runtime_msetnx_duplicate_key_uses_last_value() {
        let (fabric, mut owner0, _owner1) = server_runtimes::<16>();
        let local_key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(0),
            "msetnx-dup",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"MSETNX", &local_key, b"first", &local_key, b"second"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"MSETNX", &frame, clock),
            SharedNothingServerDispatch::Ready
        ));
        assert_eq!(
            response_bytes(
                owner0
                    .take_publishable(connection)
                    .expect("MSETNX response")
            ),
            b":1\r\n"
        );

        let wire = make_resp(&[b"GET", &local_key]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        let _ = owner0.dispatch_ingress(connection, generation, b"GET", &frame, clock);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("GET response")),
            b"$6\r\nsecond\r\n"
        );
    }

    #[test]
    fn server_runtime_owner_command_wakes_remote_reactor() {
        let (fabric, mut owner0, _owner1) = server_runtimes::<16>();
        let backend = PollingBackend::new().expect("polling backend");
        fabric.register_waker(OwnerId::from_validated_index(1), backend.waker());
        let key = key_for(
            fabric.config(),
            OwnerId::from_validated_index(1),
            "wake-remote",
        );
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"SET", &key, b"value"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(owner0.metrics().wakeups_sent, 1);
        assert_eq!(owner0.metrics().wakeup_failures, 0);
    }

    #[test]
    fn server_runtime_command_full_retries_without_accepting_second_command() {
        let (fabric, mut owner0, mut owner1) = server_runtimes::<2>();
        let key_a = key_for(fabric.config(), OwnerId::from_validated_index(1), "retry-a");
        let key_b = key_for(fabric.config(), OwnerId::from_validated_index(1), "retry-b");
        let connection = SharedNothingConnectionId::new(0);
        let generation = SharedNothingConnectionGeneration::new(1);
        let clock = CommandClock::new(1_000, 1_000);

        let wire = make_resp(&[b"SET", &key_a, b"a"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));

        let wire = make_resp(&[b"SET", &key_b, b"b"]);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().expect("one frame");
        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock),
            SharedNothingServerDispatch::Backpressure(_)
        ));
        assert_eq!(owner0.metrics().accepted_remote, 1);

        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("first response")),
            b"+OK\r\n"
        );

        assert!(matches!(
            owner0.dispatch_ingress(connection, generation, b"SET", &frame, clock),
            SharedNothingServerDispatch::Pending
        ));
        assert_eq!(owner1.drain_owner_commands(1), 1);
        assert_eq!(owner0.drain_replies(1), 1);
        assert_eq!(
            response_bytes(owner0.take_publishable(connection).expect("retry response")),
            b"+OK\r\n"
        );
    }
}
