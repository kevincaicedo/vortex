//! Reactor-style remote continuation harness for the shared-nothing branch.
//!
//! The harness models the `vortex-io` shape we want before integrating with
//! sockets: ingress accepts remote commands into bounded owner queues, stores a
//! continuation, drains owner work by budget, then drains replies and publishes
//! responses in per-connection pipeline order.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use super::{
    InvalidMailboxRoute, MailboxBackpressure, MailboxDrainCursor, MailboxFabricError, MailboxLane,
    MailboxLaneSnapshot, MailboxSendError, OwnerId, OwnerMailboxMesh, OwnerMessage,
    OwnerReplyStatus, TopologyConfig, TopologyConfigError, TopologyEpoch,
};
use super::{KeyCapsuleId, OwnerTopology};
use super::{SharedNothingConnectionGeneration, SharedNothingConnectionId};

/// Failure returned by non-blocking remote command acceptance.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuationAcceptError {
    /// Source/destination was invalid for the mailbox mesh.
    InvalidRoute(InvalidMailboxRoute),
    /// Bounded queue or reply-credit pressure rejected the command before it
    /// entered the continuation table.
    Backpressure(ContinuationBackpressure),
    /// The connection ID is outside the harness connection table.
    UnknownConnection(SharedNothingConnectionId),
}

/// Backpressure details for a rejected remote command.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContinuationBackpressure {
    /// Backpressure action the reactor should take before retrying.
    pub signal: MailboxBackpressure,
    /// Lane that blocked acceptance.
    pub lane: MailboxLane,
    /// Source owner.
    pub source: OwnerId,
    /// Destination owner.
    pub destination: OwnerId,
    /// Queue slots used when the rejection was observed.
    pub used_slots: usize,
    /// Queue slot capacity.
    pub capacity_slots: usize,
    /// Queue bytes used when the rejection was observed.
    pub used_bytes: usize,
    /// Queue byte capacity.
    pub capacity_bytes: usize,
    /// Reserved reply credits for source to destination.
    pub reply_credits_used: usize,
    /// Reply-credit capacity.
    pub reply_credits_capacity: usize,
}

/// Completed reply published to a connection after generation and ordering
/// checks.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishedContinuation {
    /// Request ID assigned at acceptance.
    pub request_id: u64,
    /// Per-connection sequence number.
    pub sequence: u64,
    /// Owner that executed the command.
    pub owner: OwnerId,
    /// Reply status.
    pub status: OwnerReplyStatus,
    /// Time between ingress acceptance and owner command dequeue.
    pub queue_wait: Duration,
}

/// Aggregated continuation harness counters.
#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReactorContinuationMetrics {
    /// Commands accepted into a remote owner queue.
    pub accepted_commands: u64,
    /// Local commands that completed through the same ingress sequencing model
    /// without entering a remote owner queue.
    pub local_ready_commands: u64,
    /// Acceptance attempts rejected before command acceptance.
    pub rejected_commands: u64,
    /// Rejections caused by full command queues.
    pub command_queue_rejections: u64,
    /// Rejections caused by exhausted reply credits.
    pub reply_credit_rejections: u64,
    /// Invalid-route rejections.
    pub invalid_route_rejections: u64,
    /// Owner command messages drained.
    pub owner_commands_drained: u64,
    /// Reply messages drained by ingress.
    pub replies_drained: u64,
    /// Replies published to live connection generations.
    pub published_replies: u64,
    /// Commands rejected by owner because their topology epoch was stale.
    pub stale_epoch_replies: u64,
    /// Replies dropped because the connection generation changed.
    pub stale_generation_drops: u64,
    /// Replies with no matching continuation, usually after disconnect cleanup.
    pub orphan_replies: u64,
    /// Pending continuations removed by disconnect cleanup.
    pub disconnect_cleanups: u64,
    /// Owner drain turns that ended with command backlog still visible.
    pub owner_turn_budget_overruns: u64,
    /// Ingress reply-drain turns that ended with reply backlog still visible.
    pub ingress_turn_budget_overruns: u64,
    /// Maximum pending continuation count observed.
    pub max_pending_continuations: usize,
    /// Maximum command queue slots observed in any directed lane.
    pub max_command_queue_slots: usize,
    /// Maximum reply queue slots observed in any directed lane.
    pub max_reply_queue_slots: usize,
    /// Maximum command queue bytes observed in any directed lane.
    pub max_command_queue_bytes: usize,
    /// Maximum reply queue bytes observed in any directed lane.
    pub max_reply_queue_bytes: usize,
    /// Maximum reserved reply credits observed for any directed pair.
    pub max_reply_credits_used: usize,
}

#[derive(Clone, Debug)]
struct PendingContinuation {
    request_id: u64,
    connection: SharedNothingConnectionId,
    generation: SharedNothingConnectionGeneration,
    sequence: u64,
    source: OwnerId,
    destination: OwnerId,
    accepted_at: Instant,
}

#[derive(Debug)]
struct ConnectionState {
    generation: SharedNothingConnectionGeneration,
    next_sequence: u64,
    next_publish_sequence: u64,
    completed: BTreeMap<u64, PublishedContinuation>,
    published: Vec<PublishedContinuation>,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            generation: SharedNothingConnectionGeneration::INITIAL,
            next_sequence: 0,
            next_publish_sequence: 0,
            completed: BTreeMap::new(),
            published: Vec::new(),
        }
    }
}

/// Engine-only continuation harness for SN-004C/SN-004D.
#[doc(hidden)]
pub struct ReactorContinuationHarness<const N: usize> {
    topology: OwnerTopology,
    mesh: OwnerMailboxMesh<N>,
    epoch: TopologyEpoch,
    connections: Vec<ConnectionState>,
    continuations: Vec<Option<PendingContinuation>>,
    owner_cursors: Vec<MailboxDrainCursor>,
    ingress_reply_cursors: Vec<MailboxDrainCursor>,
    next_request_id: u64,
    pending_count: usize,
    metrics: ReactorContinuationMetrics,
}

impl<const N: usize> ReactorContinuationHarness<N> {
    /// Builds a continuation harness.
    pub fn new(
        owner_count: usize,
        capsule_count: usize,
        connection_count: usize,
    ) -> Result<Self, ContinuationHarnessError> {
        let config = TopologyConfig::new(owner_count, capsule_count)?;
        let topology = OwnerTopology::new(config);
        let mesh = OwnerMailboxMesh::new(config)?;
        let connections = (0..connection_count)
            .map(|_| ConnectionState::default())
            .collect();
        let owner_cursors = vec![MailboxDrainCursor::new(); owner_count];
        let ingress_reply_cursors = vec![MailboxDrainCursor::new(); owner_count];

        Ok(Self {
            topology,
            mesh,
            epoch: config.epoch(),
            connections,
            continuations: Vec::new(),
            owner_cursors,
            ingress_reply_cursors,
            next_request_id: 0,
            pending_count: 0,
            metrics: ReactorContinuationMetrics::default(),
        })
    }

    /// Returns an owner ID for a valid owner index.
    #[inline]
    pub fn owner_id(&self, index: usize) -> Option<OwnerId> {
        self.topology.owner_id(index)
    }

    /// Returns a capsule ID for a valid capsule index.
    #[inline]
    pub fn capsule_id(&self, index: usize) -> Option<KeyCapsuleId> {
        self.topology.capsule_id(index)
    }

    /// Current topology epoch used for owner-side stale route checks.
    #[inline]
    pub const fn epoch(&self) -> TopologyEpoch {
        self.epoch
    }

    /// Publishes a new topology epoch for future owner drains.
    #[inline]
    pub fn publish_epoch(&mut self, epoch: TopologyEpoch) {
        self.epoch = epoch;
    }

    /// Returns an immutable metrics snapshot.
    #[inline]
    pub const fn metrics(&self) -> &ReactorContinuationMetrics {
        &self.metrics
    }

    /// Number of continuations currently waiting for a reply.
    #[inline]
    pub const fn pending_count(&self) -> usize {
        self.pending_count
    }

    /// Returns published replies for a connection.
    #[inline]
    pub fn published_for_connection(
        &self,
        connection: SharedNothingConnectionId,
    ) -> Option<&[PublishedContinuation]> {
        self.connection(connection)
            .map(|state| state.published.as_slice())
    }

    /// Returns a directed mailbox pressure snapshot.
    #[inline]
    pub fn lane_snapshot(
        &self,
        source: OwnerId,
        destination: OwnerId,
        lane: MailboxLane,
    ) -> Result<MailboxLaneSnapshot, InvalidMailboxRoute> {
        self.mesh.lane_snapshot(source, destination, lane)
    }

    /// Attempts to accept one remote command without waiting for owner
    /// execution or reply drain.
    pub fn try_accept_remote(
        &mut self,
        source: OwnerId,
        destination: OwnerId,
        capsule: KeyCapsuleId,
        connection: SharedNothingConnectionId,
    ) -> Result<u64, ContinuationAcceptError> {
        let connection_index = connection_index(connection);
        if connection_index >= self.connections.len() {
            self.metrics.rejected_commands += 1;
            return Err(ContinuationAcceptError::UnknownConnection(connection));
        }

        let request_id = self.next_request_id;
        let message = OwnerMessage::command(request_id, source, destination, capsule, self.epoch);
        match self.mesh.try_send(message) {
            Ok(()) => {}
            Err(error) => return Err(self.record_accept_error(error)),
        }

        let generation = self.connections[connection_index].generation;
        let sequence = self.connections[connection_index].next_sequence;
        self.connections[connection_index].next_sequence = self.connections[connection_index]
            .next_sequence
            .wrapping_add(1);
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.insert_pending(PendingContinuation {
            request_id,
            connection,
            generation,
            sequence,
            source,
            destination,
            accepted_at: Instant::now(),
        });
        self.metrics.accepted_commands += 1;
        self.refresh_pressure_metrics(source, destination);

        Ok(request_id)
    }

    /// Accepts one owner-local command into the same per-connection ordering
    /// model used by remote continuations.
    ///
    /// This models the `vortex-io` contract for a local command that completes
    /// while an earlier remote command on the same connection may still be
    /// pending. The local response is publishable immediately only when every
    /// earlier sequence for that connection is also complete.
    pub fn try_accept_local_ready(
        &mut self,
        owner: OwnerId,
        connection: SharedNothingConnectionId,
        status: OwnerReplyStatus,
    ) -> Result<u64, ContinuationAcceptError> {
        let connection_index = connection_index(connection);
        if connection_index >= self.connections.len() {
            self.metrics.rejected_commands += 1;
            return Err(ContinuationAcceptError::UnknownConnection(connection));
        }

        let request_id = self.next_request_id;
        let sequence = self.connections[connection_index].next_sequence;
        self.connections[connection_index].next_sequence = self.connections[connection_index]
            .next_sequence
            .wrapping_add(1);
        self.next_request_id = self.next_request_id.wrapping_add(1);

        let completed = PublishedContinuation {
            request_id,
            sequence,
            owner,
            status,
            queue_wait: Duration::ZERO,
        };
        self.connections[connection_index]
            .completed
            .insert(completed.sequence, completed);
        self.metrics.local_ready_commands = self.metrics.local_ready_commands.saturating_add(1);
        self.publish_ready(connection_index);

        Ok(request_id)
    }

    /// Drains up to `budget` command messages for `owner` and enqueues replies.
    pub fn drain_owner_commands(
        &mut self,
        owner: OwnerId,
        budget: usize,
    ) -> Result<usize, InvalidMailboxRoute> {
        let mut messages = Vec::new();
        let drained = self.mesh.drain_owner_lane_with_cursor(
            owner,
            MailboxLane::Command,
            &mut self.owner_cursors[owner.get()],
            budget,
            |message| messages.push(message),
        )?;

        for message in messages {
            let OwnerMessage::Command(command) = message else {
                continue;
            };
            let status = if command.epoch() == self.epoch {
                OwnerReplyStatus::Ok
            } else {
                self.metrics.stale_epoch_replies += 1;
                OwnerReplyStatus::StaleEpoch
            };
            let reply = OwnerMessage::reply_to(command, status);
            self.mesh
                .try_send(reply)
                .expect("reserved reply credit guarantees reply enqueue");
            self.refresh_pressure_metrics(command.source(), command.destination());
        }

        self.metrics.owner_commands_drained += drained as u64;
        if drained == budget && self.owner_command_backlog(owner)? > 0 {
            self.metrics.owner_turn_budget_overruns += 1;
        }
        Ok(drained)
    }

    /// Drains up to `budget` replies for `owner` and publishes ready
    /// per-connection responses in sequence order.
    pub fn drain_ingress_replies(
        &mut self,
        owner: OwnerId,
        budget: usize,
    ) -> Result<usize, InvalidMailboxRoute> {
        let mut messages = Vec::new();
        let drained = self.mesh.drain_owner_lane_with_cursor(
            owner,
            MailboxLane::Reply,
            &mut self.ingress_reply_cursors[owner.get()],
            budget,
            |message| messages.push(message),
        )?;

        for message in messages {
            let request_id = message.request_id();
            let status = match message {
                OwnerMessage::Reply(reply) => reply.status(),
                _ => continue,
            };
            let Some(pending) = self.take_pending(request_id) else {
                self.metrics.orphan_replies += 1;
                continue;
            };
            let connection_index = connection_index(pending.connection);
            if self.connections[connection_index].generation != pending.generation {
                self.metrics.stale_generation_drops += 1;
                continue;
            }

            let completed = PublishedContinuation {
                request_id,
                sequence: pending.sequence,
                owner: pending.destination,
                status,
                queue_wait: pending.accepted_at.elapsed(),
            };
            self.connections[connection_index]
                .completed
                .insert(completed.sequence, completed);
            self.publish_ready(connection_index);
            self.refresh_pressure_metrics(pending.source, pending.destination);
        }

        self.metrics.replies_drained += drained as u64;
        if drained == budget && self.ingress_reply_backlog(owner)? > 0 {
            self.metrics.ingress_turn_budget_overruns += 1;
        }
        Ok(drained)
    }

    /// Drains all owners and ingress reply queues until no more messages are
    /// visible or `max_turns` is exhausted.
    pub fn drain_until_idle(
        &mut self,
        owner_budget: usize,
        ingress_budget: usize,
        max_turns: usize,
    ) -> Result<usize, InvalidMailboxRoute> {
        let mut turns = 0;
        for _ in 0..max_turns {
            turns += 1;
            let mut drained = 0;
            for owner_index in 0..self.topology.config().owner_count() {
                let owner = OwnerId::from_validated_index(owner_index);
                drained += self.drain_owner_commands(owner, owner_budget)?;
            }
            for owner_index in 0..self.topology.config().owner_count() {
                let owner = OwnerId::from_validated_index(owner_index);
                drained += self.drain_ingress_replies(owner, ingress_budget)?;
            }
            if drained == 0 {
                return Ok(turns);
            }
        }
        Ok(turns)
    }

    /// Advances a connection generation and removes pending continuations for
    /// the old generation.
    pub fn disconnect(&mut self, connection: SharedNothingConnectionId) -> usize {
        let index = connection_index(connection);
        if index >= self.connections.len() {
            return 0;
        }

        let mut removed = 0usize;
        for slot in &mut self.continuations {
            let remove = slot
                .as_ref()
                .is_some_and(|pending| pending.connection == connection);
            if remove {
                *slot = None;
                removed += 1;
            }
        }

        self.pending_count = self.pending_count.saturating_sub(removed);
        self.metrics.disconnect_cleanups += removed as u64;
        let state = &mut self.connections[index];
        state.generation = state.generation.next();
        state.next_sequence = 0;
        state.next_publish_sequence = 0;
        state.completed.clear();
        state.published.clear();
        removed
    }

    /// Advances a connection generation without removing pending
    /// continuations. This models a generation check failure independently from
    /// disconnect cleanup.
    pub fn advance_connection_generation(&mut self, connection: SharedNothingConnectionId) {
        let index = connection_index(connection);
        if let Some(state) = self.connections.get_mut(index) {
            state.generation = state.generation.next();
        }
    }

    fn record_accept_error(&mut self, error: MailboxSendError) -> ContinuationAcceptError {
        self.metrics.rejected_commands += 1;
        match error {
            MailboxSendError::InvalidRoute(route) => {
                self.metrics.invalid_route_rejections += 1;
                ContinuationAcceptError::InvalidRoute(route)
            }
            MailboxSendError::Full(full) => {
                self.metrics.command_queue_rejections += 1;
                ContinuationAcceptError::Backpressure(self.backpressure_from_full(full))
            }
            MailboxSendError::ReplyCreditExhausted(full) => {
                self.metrics.reply_credit_rejections += 1;
                ContinuationAcceptError::Backpressure(self.backpressure_from_full(full))
            }
        }
    }

    fn backpressure_from_full(&self, full: super::MailboxFull) -> ContinuationBackpressure {
        let snapshot = self
            .mesh
            .lane_snapshot(full.source(), full.destination(), full.lane())
            .unwrap_or(MailboxLaneSnapshot {
                source: full.source(),
                destination: full.destination(),
                lane: full.lane(),
                used_slots: 0,
                capacity_slots: 0,
                used_bytes: 0,
                capacity_bytes: 0,
                reply_credits_used: 0,
                reply_credits_capacity: 0,
            });
        ContinuationBackpressure {
            signal: full.signal(),
            lane: full.lane(),
            source: full.source(),
            destination: full.destination(),
            used_slots: snapshot.used_slots,
            capacity_slots: snapshot.capacity_slots,
            used_bytes: snapshot.used_bytes,
            capacity_bytes: snapshot.capacity_bytes,
            reply_credits_used: snapshot.reply_credits_used,
            reply_credits_capacity: snapshot.reply_credits_capacity,
        }
    }

    fn insert_pending(&mut self, pending: PendingContinuation) {
        let request_index = pending.request_id as usize;
        if request_index >= self.continuations.len() {
            self.continuations.resize_with(request_index + 1, || None);
        }
        debug_assert!(self.continuations[request_index].is_none());
        self.continuations[request_index] = Some(pending);
        self.pending_count += 1;
        self.metrics.max_pending_continuations = self
            .metrics
            .max_pending_continuations
            .max(self.pending_count);
    }

    fn take_pending(&mut self, request_id: u64) -> Option<PendingContinuation> {
        let index = request_id as usize;
        let pending = self.continuations.get_mut(index)?.take()?;
        self.pending_count = self.pending_count.saturating_sub(1);
        Some(pending)
    }

    fn publish_ready(&mut self, connection_index: usize) {
        loop {
            let next = self.connections[connection_index].next_publish_sequence;
            let Some(completed) = self.connections[connection_index].completed.remove(&next) else {
                break;
            };
            self.connections[connection_index].published.push(completed);
            self.connections[connection_index].next_publish_sequence = self.connections
                [connection_index]
                .next_publish_sequence
                .wrapping_add(1);
            self.metrics.published_replies += 1;
        }
    }

    fn owner_command_backlog(&self, owner: OwnerId) -> Result<usize, InvalidMailboxRoute> {
        self.owner_lane_backlog(owner, MailboxLane::Command)
    }

    fn ingress_reply_backlog(&self, owner: OwnerId) -> Result<usize, InvalidMailboxRoute> {
        self.owner_lane_backlog(owner, MailboxLane::Reply)
    }

    fn owner_lane_backlog(
        &self,
        owner: OwnerId,
        lane: MailboxLane,
    ) -> Result<usize, InvalidMailboxRoute> {
        let mut total = 0usize;
        for source_index in 0..self.topology.config().owner_count() {
            let source = OwnerId::from_validated_index(source_index);
            if source == owner {
                continue;
            }
            total += self.mesh.lane_len(source, owner, lane)?;
        }
        Ok(total)
    }

    fn refresh_pressure_metrics(&mut self, source: OwnerId, destination: OwnerId) {
        if let Ok(command) = self
            .mesh
            .lane_snapshot(source, destination, MailboxLane::Command)
        {
            self.metrics.max_command_queue_slots =
                self.metrics.max_command_queue_slots.max(command.used_slots);
            self.metrics.max_command_queue_bytes =
                self.metrics.max_command_queue_bytes.max(command.used_bytes);
            self.metrics.max_reply_credits_used = self
                .metrics
                .max_reply_credits_used
                .max(command.reply_credits_used);
        }
        if let Ok(reply) = self
            .mesh
            .lane_snapshot(destination, source, MailboxLane::Reply)
        {
            self.metrics.max_reply_queue_slots =
                self.metrics.max_reply_queue_slots.max(reply.used_slots);
            self.metrics.max_reply_queue_bytes =
                self.metrics.max_reply_queue_bytes.max(reply.used_bytes);
        }
    }

    #[inline]
    fn connection(&self, connection: SharedNothingConnectionId) -> Option<&ConnectionState> {
        self.connections.get(connection_index(connection))
    }
}

/// Construction error for the continuation harness.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuationHarnessError {
    /// Topology validation failed.
    Topology(TopologyConfigError),
    /// Mailbox mesh validation failed.
    Mailbox(MailboxFabricError),
}

impl From<TopologyConfigError> for ContinuationHarnessError {
    fn from(error: TopologyConfigError) -> Self {
        Self::Topology(error)
    }
}

impl From<MailboxFabricError> for ContinuationHarnessError {
    fn from(error: MailboxFabricError) -> Self {
        Self::Mailbox(error)
    }
}

#[inline]
fn connection_index(connection: SharedNothingConnectionId) -> usize {
    connection.get() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn harness<const N: usize>() -> ReactorContinuationHarness<N> {
        ReactorContinuationHarness::new(4, 64, 8).expect("harness builds")
    }

    fn owner<const N: usize>(harness: &ReactorContinuationHarness<N>, index: usize) -> OwnerId {
        harness.owner_id(index).expect("owner exists")
    }

    fn capsule<const N: usize>(
        harness: &ReactorContinuationHarness<N>,
        index: usize,
    ) -> KeyCapsuleId {
        harness.capsule_id(index).expect("capsule exists")
    }

    #[test]
    fn remote_accept_does_not_wait_for_reply() {
        let mut harness = harness::<8>();
        let source = owner(&harness, 0);
        let destination = owner(&harness, 1);
        let capsule = capsule(&harness, 0);
        let connection = SharedNothingConnectionId::new(0);

        let request_id = harness
            .try_accept_remote(source, destination, capsule, connection)
            .expect("remote command accepted");

        assert_eq!(request_id, 0);
        assert_eq!(harness.pending_count(), 1);
        assert_eq!(harness.metrics().accepted_commands, 1);
        assert_eq!(
            harness
                .published_for_connection(connection)
                .expect("connection exists"),
            &[]
        );
    }

    #[test]
    fn pipeline_replies_publish_in_connection_order() {
        let mut harness = harness::<16>();
        let source = owner(&harness, 0);
        let first_destination = owner(&harness, 1);
        let second_destination = owner(&harness, 2);
        let capsule = capsule(&harness, 0);
        let connection = SharedNothingConnectionId::new(0);

        harness
            .try_accept_remote(source, first_destination, capsule, connection)
            .expect("first accepted");
        harness
            .try_accept_remote(source, second_destination, capsule, connection)
            .expect("second accepted");

        harness
            .drain_owner_commands(second_destination, 8)
            .expect("second owner drained");
        harness
            .drain_ingress_replies(source, 8)
            .expect("ingress drained");
        assert_eq!(
            harness
                .published_for_connection(connection)
                .expect("connection exists")
                .len(),
            0,
            "second reply must wait for first sequence"
        );

        harness
            .drain_owner_commands(first_destination, 8)
            .expect("first owner drained");
        harness
            .drain_ingress_replies(source, 8)
            .expect("ingress drained");
        let published = harness
            .published_for_connection(connection)
            .expect("connection exists");
        assert_eq!(published.len(), 2);
        assert_eq!(published[0].sequence, 0);
        assert_eq!(published[1].sequence, 1);
    }

    #[test]
    fn queue_full_is_backpressure_before_acceptance() {
        let mut harness = harness::<4>();
        let source = owner(&harness, 0);
        let destination = owner(&harness, 1);
        let capsule = capsule(&harness, 0);
        let connection = SharedNothingConnectionId::new(0);

        for _ in 0..3 {
            harness
                .try_accept_remote(source, destination, capsule, connection)
                .expect("accepted until usable capacity");
        }

        let error = harness
            .try_accept_remote(source, destination, capsule, connection)
            .expect_err("queue is full");
        let ContinuationAcceptError::Backpressure(backpressure) = error else {
            panic!("expected backpressure");
        };
        assert_eq!(backpressure.signal, MailboxBackpressure::PauseIngressReads);
        assert_eq!(backpressure.used_slots, 3);
        assert_eq!(harness.pending_count(), 3);
        assert_eq!(harness.metrics().rejected_commands, 1);

        harness
            .drain_until_idle(8, 8, 8)
            .expect("accepted commands drain");
        assert_eq!(
            harness
                .published_for_connection(connection)
                .expect("connection exists")
                .len(),
            3
        );
    }

    #[test]
    fn stale_topology_epoch_returns_stale_epoch_reply() {
        let mut harness = harness::<16>();
        let source = owner(&harness, 0);
        let destination = owner(&harness, 1);
        let capsule = capsule(&harness, 0);
        let connection = SharedNothingConnectionId::new(0);

        harness
            .try_accept_remote(source, destination, capsule, connection)
            .expect("accepted under initial epoch");
        harness.publish_epoch(TopologyEpoch::new(1));
        harness
            .drain_until_idle(8, 8, 8)
            .expect("stale command drains");

        let published = harness
            .published_for_connection(connection)
            .expect("connection exists");
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].status, OwnerReplyStatus::StaleEpoch);
        assert_eq!(harness.metrics().stale_epoch_replies, 1);
    }

    #[test]
    fn stale_connection_generation_drops_reply() {
        let mut harness = harness::<16>();
        let source = owner(&harness, 0);
        let destination = owner(&harness, 1);
        let capsule = capsule(&harness, 0);
        let connection = SharedNothingConnectionId::new(0);

        harness
            .try_accept_remote(source, destination, capsule, connection)
            .expect("accepted");
        harness
            .drain_owner_commands(destination, 8)
            .expect("owner produced reply");
        harness.advance_connection_generation(connection);
        harness
            .drain_ingress_replies(source, 8)
            .expect("reply drained after generation change");

        assert_eq!(
            harness
                .published_for_connection(connection)
                .expect("connection exists")
                .len(),
            0
        );
        assert_eq!(harness.metrics().stale_generation_drops, 1);
        assert_eq!(harness.metrics().orphan_replies, 0);
    }

    #[test]
    fn disconnect_cleanup_removes_pending_continuations() {
        let mut harness = harness::<16>();
        let source = owner(&harness, 0);
        let destination = owner(&harness, 1);
        let capsule = capsule(&harness, 0);
        let connection = SharedNothingConnectionId::new(0);

        for _ in 0..4 {
            harness
                .try_accept_remote(source, destination, capsule, connection)
                .expect("accepted");
        }

        assert_eq!(harness.pending_count(), 4);
        assert_eq!(harness.disconnect(connection), 4);
        assert_eq!(harness.pending_count(), 0);
        assert_eq!(harness.metrics().disconnect_cleanups, 4);
    }
}
