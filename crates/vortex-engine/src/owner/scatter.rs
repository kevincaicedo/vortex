//! Read-only scatter/gather harness for the shared-nothing branch.
//!
//! This module models the SN-006 execution shape before wiring it into
//! `vortex-io`: ingress owns connection ordering and aggregate continuations,
//! while owner reactors execute table reads and return owned partial replies.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use vortex_common::{VortexKey, VortexValue};

use crate::SwissTable;
use crate::table::{SlotCursor, TableHash};

use super::{
    InvalidMailboxRoute, MailboxBackpressure, MailboxDrainCursor, MailboxFabricError, MailboxLane,
    MailboxLaneSnapshot, OwnerId, OwnerMailboxMesh, OwnerMessage, OwnerReplyStatus, OwnerTopology,
    SharedNothingConnectionGeneration, SharedNothingConnectionId, SharedNothingConnectionToken,
    TopologyConfig, TopologyConfigError, TopologyEpoch,
};

/// Inline target for scatter/gather key and output planning.
pub const SCATTER_INLINE_KEY_TARGET: usize = 16;
/// Maximum keys accepted by the SN-006 phase-1 read path.
pub const SCATTER_MAX_READ_WIDTH: usize = 256;
/// Maximum keys carried by one owner read subplan.
pub const SCATTER_MAX_KEYS_PER_SUBPLAN: usize = 64;
/// Initial foreground owner read budget in keys.
pub const SCATTER_OWNER_READ_BUDGET_KEYS: usize = 64;
/// Initial ingress reply drain target in partial replies.
pub const SCATTER_INGRESS_REPLY_BUDGET_PARTIALS: usize = 64;
/// Initial ingress reply drain target in response cells.
pub const SCATTER_INGRESS_REPLY_BUDGET_CELLS: usize = 256;
/// Initial per-connection aggregate cap.
pub const SCATTER_CONNECTION_PENDING_AGGREGATES: usize = 64;
/// Initial per-connection pending-key cap.
pub const SCATTER_CONNECTION_PENDING_KEYS: usize = 4096;

/// Read-only scatter/gather command kind.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScatterGatherReadKind {
    /// `MGET key [key ...]`.
    Mget,
    /// `EXISTS key [key ...]`.
    Exists,
}

/// Completed read-only scatter/gather response.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub enum ScatterGatherResponse {
    /// One cell per input key in original request order.
    Mget(Vec<Option<VortexValue>>),
    /// Redis-compatible count, including duplicate key occurrences.
    Exists(usize),
}

/// Response published to a live connection generation after ordered assembly.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub struct PublishedScatterGather {
    /// Aggregate request ID assigned at acceptance.
    pub aggregate_id: u64,
    /// Per-connection sequence number.
    pub sequence: u64,
    /// Read command kind.
    pub kind: ScatterGatherReadKind,
    /// Assembled response.
    pub response: ScatterGatherResponse,
    /// Total owner subplans in the aggregate.
    pub subplans: usize,
    /// Remote owner subplans in the aggregate.
    pub remote_subplans: usize,
    /// Time from ingress acceptance to publish.
    pub aggregate_wait: Duration,
}

/// Scatter/gather acceptance failure.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScatterGatherAcceptError {
    /// The command had no keys.
    Empty,
    /// The command exceeded the SN-006 phase-1 width cap.
    WidthExceeded { width: usize, max_width: usize },
    /// Connection ID is outside the harness table.
    UnknownConnection(SharedNothingConnectionId),
    /// The connection already has too many retained aggregate continuations.
    ConnectionAggregateCap { pending: usize, cap: usize },
    /// The connection already has too many retained scatter/gather keys.
    ConnectionKeyCap {
        pending_keys: usize,
        new_keys: usize,
        cap: usize,
    },
    /// A remote subplan cannot be accepted without exceeding bounded queues.
    Backpressure(ScatterGatherBackpressure),
}

/// Backpressure details for rejected scatter/gather acceptance.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScatterGatherBackpressure {
    /// Backpressure signal the ingress scheduler should apply.
    pub signal: MailboxBackpressure,
    /// Source ingress owner.
    pub source: OwnerId,
    /// Destination owner.
    pub destination: OwnerId,
    /// Required remote subplans for this directed owner pair.
    pub required_subplans: usize,
    /// Snapshot observed before rejection.
    pub snapshot: MailboxLaneSnapshot,
}

/// Aggregate scatter/gather metrics.
#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScatterGatherMetrics {
    /// Accepted aggregate commands.
    pub accepted_aggregates: u64,
    /// Rejected aggregate commands.
    pub rejected_aggregates: u64,
    /// Aggregate responses published to live connections.
    pub published_aggregates: u64,
    /// Remote owner subplans enqueued.
    pub remote_subplans_enqueued: u64,
    /// Owner subplans executed locally on ingress.
    pub local_subplans_executed: u64,
    /// Remote owner subplans drained/executed.
    pub remote_subplans_executed: u64,
    /// Remote partial replies drained by ingress.
    pub partial_replies_drained: u64,
    /// Aggregates dropped because the connection generation changed.
    pub stale_generation_drops: u64,
    /// Aggregates failed because a subplan used a stale topology epoch.
    pub stale_epoch_drops: u64,
    /// Reply descriptors with no matching aggregate.
    pub orphan_replies: u64,
    /// Pending aggregate continuations removed by disconnect cleanup.
    pub disconnect_cleanups: u64,
    /// Owner drain turns that ended with command backlog still visible.
    pub owner_turn_budget_overruns: u64,
    /// Ingress drain turns that ended with reply backlog still visible.
    pub ingress_turn_budget_overruns: u64,
    /// Maximum aggregate continuations pending at once.
    pub max_pending_aggregates: usize,
    /// Maximum keys retained for one connection.
    pub max_connection_pending_keys: usize,
    /// Maximum width accepted.
    pub max_width: usize,
    /// Maximum subplans in one aggregate.
    pub max_subplans_per_aggregate: usize,
    /// Maximum remote subplans in one aggregate.
    pub max_remote_subplans_per_aggregate: usize,
    /// Maximum command queue slots observed.
    pub max_command_queue_slots: usize,
    /// Maximum command queue descriptor bytes observed.
    pub max_command_queue_bytes: usize,
    /// Maximum reply queue slots observed.
    pub max_reply_queue_slots: usize,
    /// Maximum reply queue descriptor bytes observed.
    pub max_reply_queue_bytes: usize,
    /// Maximum reply credits observed.
    pub max_reply_credits_used: usize,
    /// Owned response bytes returned by owners.
    pub owned_response_bytes: usize,
}

#[derive(Clone, Debug)]
struct ScatterReadKey {
    output_index: usize,
    key: Vec<u8>,
}

#[derive(Clone, Debug)]
struct OwnerReadSubplan {
    aggregate_id: u64,
    subplan_id: u64,
    source: OwnerId,
    destination: OwnerId,
    epoch: TopologyEpoch,
    kind: ScatterGatherReadKind,
    keys: Vec<ScatterReadKey>,
}

#[derive(Clone, Debug)]
struct OwnerReadReplyPayload {
    aggregate_id: u64,
    kind: ScatterGatherReadKind,
    mget_cells: Vec<(usize, Option<VortexValue>)>,
    exists_count: usize,
    response_bytes: usize,
}

#[derive(Clone, Debug)]
struct PendingAggregate {
    aggregate_id: u64,
    connection: SharedNothingConnectionId,
    generation: SharedNothingConnectionGeneration,
    sequence: u64,
    kind: ScatterGatherReadKind,
    remaining_subplans: usize,
    subplans: usize,
    remote_subplans: usize,
    width: usize,
    accepted_at: Instant,
    mget_cells: Vec<Option<VortexValue>>,
    exists_count: usize,
}

#[derive(Debug)]
struct ScatterConnectionState {
    generation: SharedNothingConnectionGeneration,
    next_sequence: u64,
    next_publish_sequence: u64,
    pending_aggregates: usize,
    pending_keys: usize,
    completed: BTreeMap<u64, PublishedScatterGather>,
    published: Vec<PublishedScatterGather>,
}

impl Default for ScatterConnectionState {
    fn default() -> Self {
        Self {
            generation: SharedNothingConnectionGeneration::INITIAL,
            next_sequence: 0,
            next_publish_sequence: 0,
            pending_aggregates: 0,
            pending_keys: 0,
            completed: BTreeMap::new(),
            published: Vec::new(),
        }
    }
}

struct ReadOwnerPartition {
    owner: OwnerId,
    table: SwissTable,
}

impl ReadOwnerPartition {
    fn with_capacity(owner: OwnerId, capacity: usize) -> Self {
        Self {
            owner,
            table: SwissTable::with_capacity(capacity),
        }
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
    fn contains_raw(&self, owner: OwnerId, key: &[u8]) -> bool {
        self.assert_owner(owner);
        let hash = self.table.table_hash_key_bytes(key);
        self.table.get_value_ttl_lsn_prehashed(key, hash).is_some()
    }

    fn execute_read_subplan(
        &mut self,
        owner: OwnerId,
        subplan: &OwnerReadSubplan,
        now_nanos: u64,
    ) -> OwnerReadReplyPayload {
        self.assert_owner(owner);
        debug_assert_eq!(self.owner, subplan.destination);
        match subplan.kind {
            ScatterGatherReadKind::Mget => self.execute_mget_subplan(subplan, now_nanos),
            ScatterGatherReadKind::Exists => self.execute_exists_subplan(subplan, now_nanos),
        }
    }

    fn execute_mget_subplan(
        &mut self,
        subplan: &OwnerReadSubplan,
        now_nanos: u64,
    ) -> OwnerReadReplyPayload {
        let mut cells = Vec::with_capacity(subplan.keys.len());
        let mut response_bytes = 0usize;

        for key in &subplan.keys {
            let value = self.live_string_value(&key.key, now_nanos);
            response_bytes += mget_cell_response_bytes(value.as_ref());
            cells.push((key.output_index, value));
        }

        OwnerReadReplyPayload {
            aggregate_id: subplan.aggregate_id,
            kind: subplan.kind,
            mget_cells: cells,
            exists_count: 0,
            response_bytes,
        }
    }

    fn execute_exists_subplan(
        &mut self,
        subplan: &OwnerReadSubplan,
        now_nanos: u64,
    ) -> OwnerReadReplyPayload {
        let mut exists_count = 0usize;
        for key in &subplan.keys {
            if self.exists_key(&key.key, now_nanos) {
                exists_count += 1;
            }
        }

        OwnerReadReplyPayload {
            aggregate_id: subplan.aggregate_id,
            kind: subplan.kind,
            mget_cells: Vec::new(),
            exists_count,
            response_bytes: integer_response_bytes(exists_count),
        }
    }

    #[inline]
    fn live_string_value(&mut self, key_bytes: &[u8], now_nanos: u64) -> Option<VortexValue> {
        let hash = self.table.table_hash_key_bytes(key_bytes);
        match self.table.get_with_ttl_prehashed(key_bytes, hash) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                if value.is_string() {
                    Some(value.clone())
                } else {
                    None
                }
            }
            Some(_) => {
                self.remove_expired_bytes(key_bytes, hash, now_nanos);
                None
            }
            None => None,
        }
    }

    #[inline]
    fn exists_key(&mut self, key_bytes: &[u8], now_nanos: u64) -> bool {
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
    fn remove_expired_bytes(&mut self, key_bytes: &[u8], hash: TableHash, now_nanos: u64) {
        if let SlotCursor::Expired(expired) =
            self.table.slot_cursor_prehashed(key_bytes, hash, now_nanos)
        {
            let _ = expired.remove();
        }
    }

    #[inline]
    fn assert_owner(&self, owner: OwnerId) {
        debug_assert_eq!(
            self.owner, owner,
            "scatter owner partition accessed through the wrong owner"
        );
    }
}

/// Engine-only read scatter/gather harness for SN-006.
#[doc(hidden)]
pub struct ScatterGatherHarness<const N: usize> {
    topology: OwnerTopology,
    mesh: OwnerMailboxMesh<N>,
    epoch: TopologyEpoch,
    partitions: Vec<ReadOwnerPartition>,
    owner_cursors: Vec<MailboxDrainCursor>,
    ingress_reply_cursors: Vec<MailboxDrainCursor>,
    connections: Vec<ScatterConnectionState>,
    aggregates: Vec<Option<PendingAggregate>>,
    subplans: Vec<Option<OwnerReadSubplan>>,
    subplan_to_aggregate: Vec<Option<u64>>,
    reply_payloads: Vec<Option<OwnerReadReplyPayload>>,
    next_aggregate_id: u64,
    next_subplan_id: u64,
    pending_count: usize,
    metrics: ScatterGatherMetrics,
}

impl<const N: usize> ScatterGatherHarness<N> {
    /// Builds a scatter/gather harness with one table partition per owner.
    pub fn new(
        owner_count: usize,
        capsule_count: usize,
        capacity_per_owner: usize,
        connection_count: usize,
    ) -> Result<Self, ScatterGatherHarnessError> {
        let config = TopologyConfig::new(owner_count, capsule_count)?;
        let topology = OwnerTopology::new(config);
        let mesh = OwnerMailboxMesh::new(config)?;
        let partitions = (0..owner_count)
            .map(|owner_index| {
                ReadOwnerPartition::with_capacity(
                    OwnerId::from_validated_index(owner_index),
                    capacity_per_owner,
                )
            })
            .collect();
        let owner_cursors = vec![MailboxDrainCursor::new(); owner_count];
        let ingress_reply_cursors = vec![MailboxDrainCursor::new(); owner_count];
        let connections = (0..connection_count)
            .map(|_| ScatterConnectionState::default())
            .collect();

        Ok(Self {
            topology,
            mesh,
            epoch: config.epoch(),
            partitions,
            owner_cursors,
            ingress_reply_cursors,
            connections,
            aggregates: Vec::new(),
            subplans: Vec::new(),
            subplan_to_aggregate: Vec::new(),
            reply_payloads: Vec::new(),
            next_aggregate_id: 0,
            next_subplan_id: 0,
            pending_count: 0,
            metrics: ScatterGatherMetrics::default(),
        })
    }

    /// Returns an owner ID for a valid owner index.
    #[inline]
    pub fn owner_id(&self, index: usize) -> Option<OwnerId> {
        self.topology.owner_id(index)
    }

    /// Current topology epoch used for stale route checks.
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
    pub const fn metrics(&self) -> &ScatterGatherMetrics {
        &self.metrics
    }

    /// Number of aggregate continuations currently pending.
    #[inline]
    pub const fn pending_count(&self) -> usize {
        self.pending_count
    }

    /// Routes key bytes to an owner.
    #[inline]
    pub fn route_owner_bytes(&self, key: &[u8]) -> OwnerId {
        self.topology.route_key(key).owner()
    }

    /// Seeds one key into its routed owner partition.
    pub fn insert_routed(&mut self, key: VortexKey, value: VortexValue) {
        self.insert_routed_with_ttl(key, value, 0);
    }

    /// Seeds one key with an explicit TTL deadline into its routed owner.
    pub fn insert_routed_with_ttl(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        ttl_deadline: u64,
    ) {
        let owner = self.topology.route_key(key.as_bytes()).owner();
        self.partitions[owner.get()].insert_with_ttl(owner, key, value, ttl_deadline);
    }

    /// Returns whether the routed owner table still physically contains `key`,
    /// including expired keys that have not been lazy-cleaned.
    #[inline]
    pub fn debug_contains_raw_key(&self, key: &[u8]) -> bool {
        let owner = self.topology.route_key(key).owner();
        self.partitions[owner.get()].contains_raw(owner, key)
    }

    /// Returns published responses for a connection.
    #[inline]
    pub fn published_for_connection(
        &self,
        connection: SharedNothingConnectionId,
    ) -> Option<&[PublishedScatterGather]> {
        self.connections
            .get(connection_index(connection))
            .map(|state| state.published.as_slice())
    }

    /// Attempts to accept one read-only scatter/gather command.
    pub fn try_accept_read(
        &mut self,
        source: OwnerId,
        kind: ScatterGatherReadKind,
        keys: &[&[u8]],
        connection: SharedNothingConnectionToken,
        now_nanos: u64,
    ) -> Result<u64, ScatterGatherAcceptError> {
        let connection_index = connection_index(connection.id());
        if connection_index >= self.connections.len() {
            self.metrics.rejected_aggregates += 1;
            return Err(ScatterGatherAcceptError::UnknownConnection(connection.id()));
        }
        if keys.is_empty() {
            self.metrics.rejected_aggregates += 1;
            return Err(ScatterGatherAcceptError::Empty);
        }
        if keys.len() > SCATTER_MAX_READ_WIDTH {
            self.metrics.rejected_aggregates += 1;
            return Err(ScatterGatherAcceptError::WidthExceeded {
                width: keys.len(),
                max_width: SCATTER_MAX_READ_WIDTH,
            });
        }

        self.check_connection_caps(connection_index, keys.len())?;

        let mut groups: Vec<Vec<ScatterReadKey>> = (0..self.topology.config().owner_count())
            .map(|_| Vec::new())
            .collect();
        for (output_index, key) in keys.iter().enumerate() {
            let route = self.topology.route_key(key);
            groups[route.owner().get()].push(ScatterReadKey {
                output_index,
                key: key.to_vec(),
            });
        }

        let mut subplans = Vec::new();
        let aggregate_id = self.next_aggregate_id;
        let mut remote_counts = vec![0usize; self.topology.config().owner_count()];
        for (owner_index, mut owner_keys) in groups.into_iter().enumerate() {
            if owner_keys.is_empty() {
                continue;
            }
            let destination = OwnerId::from_validated_index(owner_index);
            while !owner_keys.is_empty() {
                let split_at = owner_keys.len().min(SCATTER_MAX_KEYS_PER_SUBPLAN);
                let remaining = owner_keys.split_off(split_at);
                let chunk = std::mem::replace(&mut owner_keys, remaining);
                let subplan_id = self.next_subplan_id;
                self.next_subplan_id = self.next_subplan_id.wrapping_add(1);
                if destination != source {
                    remote_counts[destination.get()] += 1;
                }
                subplans.push(OwnerReadSubplan {
                    aggregate_id,
                    subplan_id,
                    source,
                    destination,
                    epoch: self.epoch,
                    kind,
                    keys: chunk,
                });
            }
        }

        self.preflight_remote_subplans(source, &remote_counts)?;

        let sequence = self.connections[connection_index].next_sequence;
        self.connections[connection_index].next_sequence = self.connections[connection_index]
            .next_sequence
            .wrapping_add(1);
        self.next_aggregate_id = self.next_aggregate_id.wrapping_add(1);
        self.insert_aggregate(PendingAggregate {
            aggregate_id,
            connection: connection.id(),
            generation: connection.generation(),
            sequence,
            kind,
            remaining_subplans: subplans.len(),
            subplans: subplans.len(),
            remote_subplans: remote_counts.iter().sum(),
            width: keys.len(),
            accepted_at: Instant::now(),
            mget_cells: match kind {
                ScatterGatherReadKind::Mget => vec![None; keys.len()],
                ScatterGatherReadKind::Exists => Vec::new(),
            },
            exists_count: 0,
        });
        self.connections[connection_index].pending_aggregates += 1;
        self.connections[connection_index].pending_keys += keys.len();
        self.metrics.max_connection_pending_keys = self
            .metrics
            .max_connection_pending_keys
            .max(self.connections[connection_index].pending_keys);
        self.metrics.accepted_aggregates += 1;

        for subplan in subplans {
            if subplan.destination == source {
                self.execute_local_subplan(subplan, now_nanos);
            } else {
                self.enqueue_remote_subplan(subplan);
            }
        }

        Ok(aggregate_id)
    }

    /// Drains up to `budget` remote read subplans for `owner`.
    pub fn drain_owner_subplans(
        &mut self,
        owner: OwnerId,
        budget: usize,
        now_nanos: u64,
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
            let subplan_id = command.request_id();
            let subplan = self.take_subplan(subplan_id);
            if command.epoch() != self.epoch {
                let reply = OwnerMessage::reply_to(command, OwnerReplyStatus::StaleEpoch);
                self.mesh
                    .try_send(reply)
                    .expect("reserved reply credit guarantees stale reply enqueue");
                continue;
            }

            let Some(subplan) = subplan else {
                let reply = OwnerMessage::reply_to(command, OwnerReplyStatus::Error);
                self.mesh
                    .try_send(reply)
                    .expect("reserved reply credit guarantees error reply enqueue");
                continue;
            };

            let payload =
                self.partitions[owner.get()].execute_read_subplan(owner, &subplan, now_nanos);
            self.store_reply_payload(subplan_id, payload);
            self.metrics.remote_subplans_executed += 1;
            let reply = OwnerMessage::reply_to(command, OwnerReplyStatus::Ok);
            self.mesh
                .try_send(reply)
                .expect("reserved reply credit guarantees reply enqueue");
            self.refresh_pressure_metrics(command.source(), command.destination());
        }

        if drained == budget && self.owner_command_backlog(owner)? > 0 {
            self.metrics.owner_turn_budget_overruns += 1;
        }
        Ok(drained)
    }

    /// Drains up to `partial_budget` remote partial replies for `owner`.
    pub fn drain_ingress_replies(
        &mut self,
        owner: OwnerId,
        partial_budget: usize,
    ) -> Result<usize, InvalidMailboxRoute> {
        let mut messages = Vec::new();
        let drained = self.mesh.drain_owner_lane_with_cursor(
            owner,
            MailboxLane::Reply,
            &mut self.ingress_reply_cursors[owner.get()],
            partial_budget,
            |message| messages.push(message),
        )?;

        for message in messages {
            let OwnerMessage::Reply(reply) = message else {
                continue;
            };
            let subplan_id = reply.request_id();
            match reply.status() {
                OwnerReplyStatus::Ok => {
                    let Some(payload) = self.take_reply_payload(subplan_id) else {
                        self.metrics.orphan_replies += 1;
                        continue;
                    };
                    self.apply_read_reply(payload);
                }
                OwnerReplyStatus::StaleEpoch => {
                    self.drop_aggregate_for_subplan(subplan_id, true);
                }
                OwnerReplyStatus::Nil
                | OwnerReplyStatus::Error
                | OwnerReplyStatus::Backpressure => {
                    self.drop_aggregate_for_subplan(subplan_id, false);
                    self.metrics.orphan_replies += 1;
                }
            }
            self.metrics.partial_replies_drained += 1;
        }

        if drained == partial_budget && self.ingress_reply_backlog(owner)? > 0 {
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
        now_nanos: u64,
        max_turns: usize,
    ) -> Result<usize, InvalidMailboxRoute> {
        let mut turns = 0;
        for _ in 0..max_turns {
            turns += 1;
            let mut drained = 0;
            for owner_index in 0..self.topology.config().owner_count() {
                let owner = OwnerId::from_validated_index(owner_index);
                drained += self.drain_owner_subplans(owner, owner_budget, now_nanos)?;
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

    /// Advances a connection generation and removes pending aggregates for that
    /// connection.
    pub fn disconnect(&mut self, connection: SharedNothingConnectionId) -> usize {
        let index = connection_index(connection);
        if index >= self.connections.len() {
            return 0;
        }

        let mut removed = 0usize;
        let mut removed_keys = 0usize;
        for slot in &mut self.aggregates {
            let remove = slot
                .as_ref()
                .is_some_and(|pending| pending.connection == connection);
            if remove {
                if let Some(pending) = slot.take() {
                    removed += 1;
                    removed_keys += pending.width;
                }
            }
        }
        self.pending_count = self.pending_count.saturating_sub(removed);
        self.metrics.disconnect_cleanups += removed as u64;

        let state = &mut self.connections[index];
        state.generation = state.generation.next();
        state.next_sequence = 0;
        state.next_publish_sequence = 0;
        state.pending_aggregates = state.pending_aggregates.saturating_sub(removed);
        state.pending_keys = state.pending_keys.saturating_sub(removed_keys);
        state.completed.clear();
        state.published.clear();
        removed
    }

    /// Advances the generation without cleanup so stale-generation behavior can
    /// be tested independently.
    pub fn advance_connection_generation(&mut self, connection: SharedNothingConnectionId) {
        if let Some(state) = self.connections.get_mut(connection_index(connection)) {
            state.generation = state.generation.next();
        }
    }

    fn check_connection_caps(
        &mut self,
        connection_index: usize,
        width: usize,
    ) -> Result<(), ScatterGatherAcceptError> {
        let state = &self.connections[connection_index];
        if state.pending_aggregates >= SCATTER_CONNECTION_PENDING_AGGREGATES {
            self.metrics.rejected_aggregates += 1;
            return Err(ScatterGatherAcceptError::ConnectionAggregateCap {
                pending: state.pending_aggregates,
                cap: SCATTER_CONNECTION_PENDING_AGGREGATES,
            });
        }
        if state.pending_keys.saturating_add(width) > SCATTER_CONNECTION_PENDING_KEYS {
            self.metrics.rejected_aggregates += 1;
            return Err(ScatterGatherAcceptError::ConnectionKeyCap {
                pending_keys: state.pending_keys,
                new_keys: width,
                cap: SCATTER_CONNECTION_PENDING_KEYS,
            });
        }
        Ok(())
    }

    fn preflight_remote_subplans(
        &mut self,
        source: OwnerId,
        remote_counts: &[usize],
    ) -> Result<(), ScatterGatherAcceptError> {
        for (owner_index, &required) in remote_counts.iter().enumerate() {
            if required == 0 {
                continue;
            }
            let destination = OwnerId::from_validated_index(owner_index);
            let snapshot = self
                .mesh
                .lane_snapshot(source, destination, MailboxLane::Command)
                .expect("routed owner pair must be valid");
            if snapshot.available_slots() < required
                || snapshot.available_reply_credits() < required
            {
                self.metrics.rejected_aggregates += 1;
                return Err(ScatterGatherAcceptError::Backpressure(
                    ScatterGatherBackpressure {
                        signal: MailboxBackpressure::PauseIngressReads,
                        source,
                        destination,
                        required_subplans: required,
                        snapshot,
                    },
                ));
            }
        }
        Ok(())
    }

    fn insert_aggregate(&mut self, aggregate: PendingAggregate) {
        let index = aggregate.aggregate_id as usize;
        if index >= self.aggregates.len() {
            self.aggregates.resize_with(index + 1, || None);
        }
        debug_assert!(self.aggregates[index].is_none());
        self.metrics.max_width = self.metrics.max_width.max(aggregate.width);
        self.metrics.max_subplans_per_aggregate = self
            .metrics
            .max_subplans_per_aggregate
            .max(aggregate.subplans);
        self.metrics.max_remote_subplans_per_aggregate = self
            .metrics
            .max_remote_subplans_per_aggregate
            .max(aggregate.remote_subplans);
        self.aggregates[index] = Some(aggregate);
        self.pending_count += 1;
        self.metrics.max_pending_aggregates =
            self.metrics.max_pending_aggregates.max(self.pending_count);
    }

    fn enqueue_remote_subplan(&mut self, subplan: OwnerReadSubplan) {
        let subplan_id = subplan.subplan_id;
        let capsule = self.topology.route_key(&subplan.keys[0].key).capsule();
        let message = OwnerMessage::command(
            subplan_id,
            subplan.source,
            subplan.destination,
            capsule,
            subplan.epoch,
        );
        self.store_subplan(subplan);
        self.mesh
            .try_send(message)
            .expect("scatter/gather preflight must reserve command and reply capacity");
        self.metrics.remote_subplans_enqueued += 1;
        self.refresh_pressure_metrics(message.source(), message.destination());
    }

    fn execute_local_subplan(&mut self, subplan: OwnerReadSubplan, now_nanos: u64) {
        let owner = subplan.destination;
        let payload = self.partitions[owner.get()].execute_read_subplan(owner, &subplan, now_nanos);
        self.metrics.local_subplans_executed += 1;
        self.apply_read_reply(payload);
    }

    fn store_subplan(&mut self, subplan: OwnerReadSubplan) {
        let index = subplan.subplan_id as usize;
        if index >= self.subplans.len() {
            self.subplans.resize_with(index + 1, || None);
            self.subplan_to_aggregate.resize_with(index + 1, || None);
        }
        self.subplan_to_aggregate[index] = Some(subplan.aggregate_id);
        self.subplans[index] = Some(subplan);
    }

    fn take_subplan(&mut self, subplan_id: u64) -> Option<OwnerReadSubplan> {
        self.subplans.get_mut(subplan_id as usize)?.take()
    }

    fn store_reply_payload(&mut self, subplan_id: u64, payload: OwnerReadReplyPayload) {
        let index = subplan_id as usize;
        if index >= self.reply_payloads.len() {
            self.reply_payloads.resize_with(index + 1, || None);
        }
        self.reply_payloads[index] = Some(payload);
    }

    fn take_reply_payload(&mut self, subplan_id: u64) -> Option<OwnerReadReplyPayload> {
        self.reply_payloads.get_mut(subplan_id as usize)?.take()
    }

    fn apply_read_reply(&mut self, payload: OwnerReadReplyPayload) {
        let aggregate_index = payload.aggregate_id as usize;
        let Some(aggregate) = self
            .aggregates
            .get_mut(aggregate_index)
            .and_then(Option::as_mut)
        else {
            self.metrics.orphan_replies += 1;
            return;
        };

        debug_assert_eq!(aggregate.kind, payload.kind);
        match payload.kind {
            ScatterGatherReadKind::Mget => {
                for (output_index, value) in payload.mget_cells {
                    aggregate.mget_cells[output_index] = value;
                }
            }
            ScatterGatherReadKind::Exists => {
                aggregate.exists_count += payload.exists_count;
            }
        }
        aggregate.remaining_subplans = aggregate.remaining_subplans.saturating_sub(1);
        self.metrics.owned_response_bytes += payload.response_bytes;

        if aggregate.remaining_subplans == 0 {
            let aggregate = self.aggregates[aggregate_index]
                .take()
                .expect("completed aggregate exists");
            self.pending_count = self.pending_count.saturating_sub(1);
            self.complete_aggregate(aggregate);
        }
    }

    fn complete_aggregate(&mut self, aggregate: PendingAggregate) {
        let connection_index = connection_index(aggregate.connection);
        let Some(connection) = self.connections.get_mut(connection_index) else {
            return;
        };
        connection.pending_aggregates = connection.pending_aggregates.saturating_sub(1);
        connection.pending_keys = connection.pending_keys.saturating_sub(aggregate.width);
        if connection.generation != aggregate.generation {
            self.metrics.stale_generation_drops += 1;
            return;
        }

        let response = match aggregate.kind {
            ScatterGatherReadKind::Mget => ScatterGatherResponse::Mget(aggregate.mget_cells),
            ScatterGatherReadKind::Exists => ScatterGatherResponse::Exists(aggregate.exists_count),
        };
        let completed = PublishedScatterGather {
            aggregate_id: aggregate.aggregate_id,
            sequence: aggregate.sequence,
            kind: aggregate.kind,
            response,
            subplans: aggregate.subplans,
            remote_subplans: aggregate.remote_subplans,
            aggregate_wait: aggregate.accepted_at.elapsed(),
        };
        connection.completed.insert(completed.sequence, completed);
        self.publish_ready(connection_index);
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
            self.metrics.published_aggregates += 1;
        }
    }

    fn drop_aggregate_for_subplan(&mut self, subplan_id: u64, stale_epoch: bool) {
        let Some(aggregate_id) = self
            .subplan_to_aggregate
            .get_mut(subplan_id as usize)
            .and_then(Option::take)
        else {
            self.metrics.orphan_replies += 1;
            return;
        };
        let Some(aggregate) = self
            .aggregates
            .get_mut(aggregate_id as usize)
            .and_then(Option::take)
        else {
            self.metrics.orphan_replies += 1;
            return;
        };
        self.pending_count = self.pending_count.saturating_sub(1);
        if let Some(connection) = self
            .connections
            .get_mut(connection_index(aggregate.connection))
        {
            connection.pending_aggregates = connection.pending_aggregates.saturating_sub(1);
            connection.pending_keys = connection.pending_keys.saturating_sub(aggregate.width);
        }
        if stale_epoch {
            self.metrics.stale_epoch_drops += 1;
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
}

/// Construction error for the scatter/gather harness.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScatterGatherHarnessError {
    /// Topology validation failed.
    Topology(TopologyConfigError),
    /// Mailbox mesh validation failed.
    Mailbox(MailboxFabricError),
}

impl From<TopologyConfigError> for ScatterGatherHarnessError {
    fn from(error: TopologyConfigError) -> Self {
        Self::Topology(error)
    }
}

impl From<MailboxFabricError> for ScatterGatherHarnessError {
    fn from(error: MailboxFabricError) -> Self {
        Self::Mailbox(error)
    }
}

#[inline]
fn connection_index(connection: SharedNothingConnectionId) -> usize {
    connection.get() as usize
}

#[inline]
fn mget_cell_response_bytes(value: Option<&VortexValue>) -> usize {
    match value {
        Some(VortexValue::InlineString(inline)) => bulk_response_bytes(inline.len()),
        Some(VortexValue::String(bytes)) => bulk_response_bytes(bytes.len()),
        Some(VortexValue::Integer(number)) => bulk_response_bytes(itoa_decimal_len(*number)),
        _ => 5,
    }
}

#[inline]
fn integer_response_bytes(value: usize) -> usize {
    1 + decimal_len(value) + 2
}

#[inline]
fn bulk_response_bytes(payload_len: usize) -> usize {
    1 + decimal_len(payload_len) + 2 + payload_len + 2
}

#[inline]
fn decimal_len(value: usize) -> usize {
    if value == 0 {
        return 1;
    }
    let mut n = value;
    let mut len = 0;
    while n != 0 {
        len += 1;
        n /= 10;
    }
    len
}

#[inline]
fn itoa_decimal_len(value: i64) -> usize {
    let mut buffer = itoa::Buffer::new();
    buffer.format(value).len()
}

#[cfg(test)]
mod tests {
    use super::*;

    const RING_SLOTS: usize = 64;
    const NOW_NANOS: u64 = 10_000_000;

    fn harness<const N: usize>() -> ScatterGatherHarness<N> {
        ScatterGatherHarness::new(4, 64, 128, 8).expect("scatter harness builds")
    }

    fn token(connection_id: u32) -> SharedNothingConnectionToken {
        SharedNothingConnectionToken::new(
            SharedNothingConnectionId::new(connection_id),
            SharedNothingConnectionGeneration::INITIAL,
        )
    }

    fn key_for<const N: usize>(
        harness: &ScatterGatherHarness<N>,
        prefix: &str,
        is_match: impl Fn(OwnerId) -> bool,
    ) -> Vec<u8> {
        (0usize..20_000)
            .map(|index| format!("{prefix}:{index:05}").into_bytes())
            .find(|key| is_match(harness.route_owner_bytes(key)))
            .expect("routed key found")
    }

    fn drain_all<const N: usize>(harness: &mut ScatterGatherHarness<N>) {
        harness
            .drain_until_idle(
                SCATTER_OWNER_READ_BUDGET_KEYS,
                SCATTER_INGRESS_REPLY_BUDGET_PARTIALS,
                NOW_NANOS,
                128,
            )
            .expect("scatter drains");
    }

    #[test]
    fn mget_duplicates_preserve_output_order_across_owners() {
        let mut harness = harness::<RING_SLOTS>();
        let source = harness.owner_id(0).expect("owner zero");
        let local_key = key_for(&harness, "mget-local", |owner| owner == source);
        let remote_key = key_for(&harness, "mget-remote", |owner| owner != source);

        harness.insert_routed(
            VortexKey::from_bytes(&local_key),
            VortexValue::from_bytes(b"local"),
        );
        harness.insert_routed(
            VortexKey::from_bytes(&remote_key),
            VortexValue::from_bytes(b"remote"),
        );

        harness
            .try_accept_read(
                source,
                ScatterGatherReadKind::Mget,
                &[&remote_key, &local_key, &remote_key],
                token(0),
                NOW_NANOS,
            )
            .expect("mget accepted");
        drain_all(&mut harness);

        let published = harness
            .published_for_connection(SharedNothingConnectionId::new(0))
            .expect("connection exists");
        assert_eq!(published.len(), 1);
        let ScatterGatherResponse::Mget(cells) = &published[0].response else {
            panic!("expected mget response");
        };
        assert_eq!(cells.len(), 3);
        assert_eq!(cells[0], Some(VortexValue::from_bytes(b"remote")));
        assert_eq!(cells[1], Some(VortexValue::from_bytes(b"local")));
        assert_eq!(cells[2], Some(VortexValue::from_bytes(b"remote")));
    }

    #[test]
    fn exists_counts_duplicate_occurrences() {
        let mut harness = harness::<RING_SLOTS>();
        let source = harness.owner_id(0).expect("owner zero");
        let local_key = key_for(&harness, "exists-local", |owner| owner == source);
        let remote_key = key_for(&harness, "exists-remote", |owner| owner != source);
        let missing_key = b"missing-key".to_vec();

        harness.insert_routed(
            VortexKey::from_bytes(&local_key),
            VortexValue::from_bytes(b"local"),
        );
        harness.insert_routed(
            VortexKey::from_bytes(&remote_key),
            VortexValue::from_bytes(b"remote"),
        );

        harness
            .try_accept_read(
                source,
                ScatterGatherReadKind::Exists,
                &[&local_key, &remote_key, &remote_key, &missing_key],
                token(0),
                NOW_NANOS,
            )
            .expect("exists accepted");
        drain_all(&mut harness);

        let published = harness
            .published_for_connection(SharedNothingConnectionId::new(0))
            .expect("connection exists");
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].response, ScatterGatherResponse::Exists(3));
    }

    #[test]
    fn queue_full_rejects_before_local_lazy_expiry_runs() {
        let mut harness =
            ScatterGatherHarness::<2>::new(4, 64, 128, 8).expect("small scatter harness builds");
        let source = harness.owner_id(0).expect("owner zero");
        let local_key = key_for(&harness, "expired-local", |owner| owner == source);
        let remote_owner = harness.owner_id(1).expect("owner one");
        let mut remote_keys = Vec::new();
        for index in 0usize..65 {
            let key = key_for(&harness, &format!("full-remote-{index}"), |owner| {
                owner == remote_owner
            });
            remote_keys.push(key);
        }

        harness.insert_routed_with_ttl(
            VortexKey::from_bytes(&local_key),
            VortexValue::from_bytes(b"expired"),
            NOW_NANOS - 1,
        );
        assert!(harness.debug_contains_raw_key(&local_key));

        let mut keys: Vec<&[u8]> = Vec::with_capacity(remote_keys.len() + 1);
        keys.push(&local_key);
        for key in &remote_keys {
            keys.push(key);
        }

        let error = harness
            .try_accept_read(
                source,
                ScatterGatherReadKind::Mget,
                &keys,
                token(0),
                NOW_NANOS,
            )
            .expect_err("capacity-1 lane rejects two remote chunks");

        assert!(matches!(error, ScatterGatherAcceptError::Backpressure(_)));
        assert_eq!(harness.metrics().accepted_aggregates, 0);
        assert_eq!(harness.metrics().local_subplans_executed, 0);
        assert!(harness.debug_contains_raw_key(&local_key));
    }

    #[test]
    fn later_local_completion_waits_for_earlier_remote_sequence() {
        let mut harness = harness::<RING_SLOTS>();
        let source = harness.owner_id(0).expect("owner zero");
        let local_key = key_for(&harness, "seq-local", |owner| owner == source);
        let remote_key = key_for(&harness, "seq-remote", |owner| owner != source);

        harness.insert_routed(
            VortexKey::from_bytes(&local_key),
            VortexValue::from_bytes(b"local"),
        );
        harness.insert_routed(
            VortexKey::from_bytes(&remote_key),
            VortexValue::from_bytes(b"remote"),
        );

        harness
            .try_accept_read(
                source,
                ScatterGatherReadKind::Mget,
                &[&remote_key],
                token(0),
                NOW_NANOS,
            )
            .expect("remote accepted");
        harness
            .try_accept_read(
                source,
                ScatterGatherReadKind::Mget,
                &[&local_key],
                token(0),
                NOW_NANOS,
            )
            .expect("local accepted");

        assert!(
            harness
                .published_for_connection(SharedNothingConnectionId::new(0))
                .expect("connection exists")
                .is_empty(),
            "local response must wait behind earlier remote sequence"
        );

        drain_all(&mut harness);
        let published = harness
            .published_for_connection(SharedNothingConnectionId::new(0))
            .expect("connection exists");
        assert_eq!(published.len(), 2);
        assert_eq!(published[0].sequence, 0);
        assert_eq!(published[1].sequence, 1);
    }

    #[test]
    fn stale_generation_drops_pending_aggregate() {
        let mut harness = harness::<RING_SLOTS>();
        let source = harness.owner_id(0).expect("owner zero");
        let remote_key = key_for(&harness, "stale-remote", |owner| owner != source);
        harness.insert_routed(
            VortexKey::from_bytes(&remote_key),
            VortexValue::from_bytes(b"remote"),
        );

        harness
            .try_accept_read(
                source,
                ScatterGatherReadKind::Mget,
                &[&remote_key],
                token(0),
                NOW_NANOS,
            )
            .expect("remote accepted");
        harness.advance_connection_generation(SharedNothingConnectionId::new(0));
        drain_all(&mut harness);

        assert!(
            harness
                .published_for_connection(SharedNothingConnectionId::new(0))
                .expect("connection exists")
                .is_empty()
        );
        assert_eq!(harness.metrics().stale_generation_drops, 1);
    }

    #[test]
    fn stale_topology_epoch_never_publishes_partial_array() {
        let mut harness = harness::<RING_SLOTS>();
        let source = harness.owner_id(0).expect("owner zero");
        let remote_key = key_for(&harness, "epoch-remote", |owner| owner != source);
        harness.insert_routed(
            VortexKey::from_bytes(&remote_key),
            VortexValue::from_bytes(b"remote"),
        );

        harness
            .try_accept_read(
                source,
                ScatterGatherReadKind::Mget,
                &[&remote_key],
                token(0),
                NOW_NANOS,
            )
            .expect("remote accepted");
        harness.publish_epoch(TopologyEpoch::new(1));
        drain_all(&mut harness);

        assert!(
            harness
                .published_for_connection(SharedNothingConnectionId::new(0))
                .expect("connection exists")
                .is_empty()
        );
        assert_eq!(harness.metrics().stale_epoch_drops, 1);
    }

    #[test]
    fn width_256_hot_owner_splits_into_four_chunks() {
        let mut harness = harness::<RING_SLOTS>();
        let source = harness.owner_id(0).expect("owner zero");
        let remote_owner = harness.owner_id(1).expect("owner one");
        let mut owned_keys = Vec::with_capacity(SCATTER_MAX_READ_WIDTH);
        for index in 0..SCATTER_MAX_READ_WIDTH {
            let key = key_for(&harness, &format!("hot-{index}"), |owner| {
                owner == remote_owner
            });
            harness.insert_routed(
                VortexKey::from_bytes(&key),
                VortexValue::Integer(index as i64),
            );
            owned_keys.push(key);
        }
        let keys: Vec<&[u8]> = owned_keys.iter().map(Vec::as_slice).collect();

        harness
            .try_accept_read(
                source,
                ScatterGatherReadKind::Mget,
                &keys,
                token(0),
                NOW_NANOS,
            )
            .expect("wide mget accepted");

        assert_eq!(harness.metrics().max_subplans_per_aggregate, 4);
        assert_eq!(harness.metrics().max_remote_subplans_per_aggregate, 4);
    }
}
