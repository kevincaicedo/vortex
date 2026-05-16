//! Bounded owner-to-owner mailbox fabric for the shared-nothing experiment.
//!
//! The fabric is intentionally not wired into command execution yet. It adapts
//! the existing `vortex-sync` SPSC ring into one directed queue per
//! `(source, destination, lane)`.

use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
use vortex_sync::{SpscReceiver, SpscSender, spsc_channel};

use super::{KeyCapsuleId, OwnerId, TopologyConfig, TopologyEpoch};

/// Number of mailbox lanes in the owner fabric.
pub const MAILBOX_LANE_COUNT: usize = 5;

/// Current owner message descriptor size.
pub const OWNER_MESSAGE_SIZE_BYTES: usize = std::mem::size_of::<OwnerMessage>();

/// Current ring slot payload size for the adapted SPSC queue.
pub const OWNER_MESSAGE_SLOT_SIZE_BYTES: usize = std::mem::size_of::<Option<OwnerMessage>>();

/// Mailbox lanes ordered by owner-drain priority.
pub const MAILBOX_DRAIN_PRIORITY: [MailboxLane; MAILBOX_LANE_COUNT] = [
    MailboxLane::Control,
    MailboxLane::Reply,
    MailboxLane::Aof,
    MailboxLane::Command,
    MailboxLane::Maintenance,
];

/// Traffic class for a bounded owner mailbox queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum MailboxLane {
    /// Topology, shutdown, and fatal control events.
    Control = 0,
    /// Remote command responses returning to ingress.
    Reply = 1,
    /// Remote command execution requests.
    Command = 2,
    /// Expiry, migration, and cold maintenance work.
    Maintenance = 3,
    /// AOF handoff and durability completion messages.
    Aof = 4,
}

impl MailboxLane {
    /// All lanes in stable index order.
    pub const ALL: [Self; MAILBOX_LANE_COUNT] = [
        Self::Control,
        Self::Reply,
        Self::Command,
        Self::Maintenance,
        Self::Aof,
    ];

    /// Returns the zero-based lane index used in mesh addressing.
    #[inline(always)]
    pub const fn index(self) -> usize {
        self as usize
    }

    /// Backpressure signal to publish when this lane is full.
    #[inline]
    pub const fn backpressure_signal(self) -> MailboxBackpressure {
        match self {
            Self::Control => MailboxBackpressure::PrioritizeControlDrain,
            Self::Reply => MailboxBackpressure::PrioritizeReplyDrain,
            Self::Command => MailboxBackpressure::PauseIngressReads,
            Self::Maintenance => MailboxBackpressure::DeferMaintenance,
            Self::Aof => MailboxBackpressure::PrioritizeAofDrain,
        }
    }
}

/// Backpressure decision returned to the future ingress scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MailboxBackpressure {
    /// Disable or defer ingress reads before accepting more remote commands.
    PauseIngressReads,
    /// Drain control traffic before normal work.
    PrioritizeControlDrain,
    /// Drain replies before accepting more remote commands for that ingress.
    PrioritizeReplyDrain,
    /// Defer maintenance work and spend the tick budget on foreground traffic.
    DeferMaintenance,
    /// Drain AOF completion traffic before accepting durability-dependent work.
    PrioritizeAofDrain,
}

/// Why a mailbox route is not valid for the fabric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidMailboxRouteKind {
    /// Source or destination is outside the configured owner set.
    UnknownOwner,
    /// Source and destination are the same owner and must bypass the mailbox.
    SelfRoute,
}

/// Result status carried by a remote owner reply descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum OwnerReplyStatus {
    /// The remote operation completed successfully.
    Ok,
    /// The requested key or value was absent.
    Nil,
    /// The remote operation failed with a command/domain error.
    Error,
    /// The owner rejected work because bounded capacity was exhausted.
    Backpressure,
    /// The message was routed under a stale topology epoch.
    StaleEpoch,
}

/// Remote command descriptor used by the fabric skeleton.
///
/// Payload ownership is intentionally deferred to SN-005. This descriptor is
/// fixed-size so SN-002 can measure the queue fabric without RESP buffer
/// lifetime complexity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnerCommand {
    request_id: u64,
    source: OwnerId,
    destination: OwnerId,
    capsule: KeyCapsuleId,
    epoch: TopologyEpoch,
    reply_required: bool,
}

impl OwnerCommand {
    /// Creates a remote command descriptor.
    #[inline]
    pub const fn new(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        capsule: KeyCapsuleId,
        epoch: TopologyEpoch,
    ) -> Self {
        Self {
            request_id,
            source,
            destination,
            capsule,
            epoch,
            reply_required: true,
        }
    }

    /// Creates a one-way remote command descriptor.
    #[inline]
    pub const fn one_way(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        capsule: KeyCapsuleId,
        epoch: TopologyEpoch,
    ) -> Self {
        Self {
            request_id,
            source,
            destination,
            capsule,
            epoch,
            reply_required: false,
        }
    }

    /// Request ID carried by this command.
    #[inline]
    pub const fn request_id(self) -> u64 {
        self.request_id
    }

    /// Source owner.
    #[inline]
    pub const fn source(self) -> OwnerId {
        self.source
    }

    /// Destination owner.
    #[inline]
    pub const fn destination(self) -> OwnerId {
        self.destination
    }

    /// Target capsule.
    #[inline]
    pub const fn capsule(self) -> KeyCapsuleId {
        self.capsule
    }

    /// Topology epoch used for routing this command.
    #[inline]
    pub const fn epoch(self) -> TopologyEpoch {
        self.epoch
    }

    /// Returns whether the command reserved capacity on the reverse reply lane.
    #[inline]
    pub const fn reply_required(self) -> bool {
        self.reply_required
    }
}

/// Remote reply descriptor returning to the ingress owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnerReply {
    request_id: u64,
    source: OwnerId,
    destination: OwnerId,
    status: OwnerReplyStatus,
    epoch: TopologyEpoch,
    releases_credit: bool,
}

impl OwnerReply {
    /// Creates an uncredited remote reply descriptor.
    ///
    /// Normal replies to accepted remote commands should use
    /// [`OwnerReply::for_command`] so draining the reply releases the reserved
    /// reverse-lane credit.
    #[inline]
    pub const fn new(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        status: OwnerReplyStatus,
        epoch: TopologyEpoch,
    ) -> Self {
        Self {
            request_id,
            source,
            destination,
            status,
            epoch,
            releases_credit: false,
        }
    }

    /// Creates a reply for a command accepted with reserved reply capacity.
    #[inline]
    pub const fn for_command(command: OwnerCommand, status: OwnerReplyStatus) -> Self {
        Self {
            request_id: command.request_id,
            source: command.destination,
            destination: command.source,
            status,
            epoch: command.epoch,
            releases_credit: command.reply_required,
        }
    }

    /// Reply status.
    #[inline]
    pub const fn status(self) -> OwnerReplyStatus {
        self.status
    }

    /// Request ID.
    #[inline]
    pub const fn request_id(self) -> u64 {
        self.request_id
    }

    /// Source owner.
    #[inline]
    pub const fn source(self) -> OwnerId {
        self.source
    }

    /// Destination owner.
    #[inline]
    pub const fn destination(self) -> OwnerId {
        self.destination
    }

    /// Topology epoch.
    #[inline]
    pub const fn epoch(self) -> TopologyEpoch {
        self.epoch
    }

    /// Returns whether draining this reply releases a reserved reply credit.
    #[inline]
    pub const fn releases_credit(self) -> bool {
        self.releases_credit
    }
}

/// Control message kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum OwnerControlKind {
    /// Stop an owner loop.
    Shutdown,
    /// A topology epoch was published.
    TopologyChanged,
    /// Fatal owner-local error notification.
    Fatal,
}

/// Control message descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnerControl {
    request_id: u64,
    source: OwnerId,
    destination: OwnerId,
    kind: OwnerControlKind,
    epoch: TopologyEpoch,
}

impl OwnerControl {
    /// Creates a control descriptor.
    #[inline]
    pub const fn new(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        kind: OwnerControlKind,
        epoch: TopologyEpoch,
    ) -> Self {
        Self {
            request_id,
            source,
            destination,
            kind,
            epoch,
        }
    }
}

/// Maintenance message kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum OwnerMaintenanceKind {
    /// Run owner-local active expiry work.
    Expire,
    /// Run capsule migration work.
    Migrate,
    /// Flush cold owner stats.
    StatsFlush,
}

/// Maintenance message descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnerMaintenance {
    request_id: u64,
    source: OwnerId,
    destination: OwnerId,
    capsule: KeyCapsuleId,
    kind: OwnerMaintenanceKind,
    epoch: TopologyEpoch,
}

impl OwnerMaintenance {
    /// Creates a maintenance descriptor.
    #[inline]
    pub const fn new(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        capsule: KeyCapsuleId,
        kind: OwnerMaintenanceKind,
        epoch: TopologyEpoch,
    ) -> Self {
        Self {
            request_id,
            source,
            destination,
            capsule,
            kind,
            epoch,
        }
    }
}

/// AOF message kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum OwnerAofKind {
    /// Owner-local records are ready for durability coordination.
    RecordsReady,
    /// Durability policy was satisfied.
    FsyncComplete,
    /// AOF writer entered a fatal state.
    Fatal,
}

/// AOF message descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnerAof {
    request_id: u64,
    source: OwnerId,
    destination: OwnerId,
    kind: OwnerAofKind,
    epoch: TopologyEpoch,
}

impl OwnerAof {
    /// Creates an AOF descriptor.
    #[inline]
    pub const fn new(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        kind: OwnerAofKind,
        epoch: TopologyEpoch,
    ) -> Self {
        Self {
            request_id,
            source,
            destination,
            kind,
            epoch,
        }
    }
}

/// Fixed-size owner mailbox message descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerMessage {
    /// Remote command execution request.
    Command(OwnerCommand),
    /// Remote command response.
    Reply(OwnerReply),
    /// Control-plane event.
    Control(OwnerControl),
    /// Owner maintenance task.
    Maintenance(OwnerMaintenance),
    /// AOF coordination event.
    Aof(OwnerAof),
}

impl OwnerMessage {
    /// Creates a command message.
    #[inline]
    pub const fn command(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        capsule: KeyCapsuleId,
        epoch: TopologyEpoch,
    ) -> Self {
        Self::Command(OwnerCommand::new(
            request_id,
            source,
            destination,
            capsule,
            epoch,
        ))
    }

    /// Creates a one-way command message that does not reserve reply capacity.
    #[inline]
    pub const fn one_way_command(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        capsule: KeyCapsuleId,
        epoch: TopologyEpoch,
    ) -> Self {
        Self::Command(OwnerCommand::one_way(
            request_id,
            source,
            destination,
            capsule,
            epoch,
        ))
    }

    /// Creates an uncredited reply message.
    ///
    /// Normal replies to accepted remote commands should use
    /// [`OwnerMessage::reply_to`] so reply-credit accounting remains balanced.
    #[inline]
    pub const fn reply(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        status: OwnerReplyStatus,
        epoch: TopologyEpoch,
    ) -> Self {
        Self::Reply(OwnerReply::new(
            request_id,
            source,
            destination,
            status,
            epoch,
        ))
    }

    /// Creates a reply message from an accepted command.
    #[inline]
    pub const fn reply_to(command: OwnerCommand, status: OwnerReplyStatus) -> Self {
        Self::Reply(OwnerReply::for_command(command, status))
    }

    /// Creates a control message.
    #[inline]
    pub const fn control(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        kind: OwnerControlKind,
        epoch: TopologyEpoch,
    ) -> Self {
        Self::Control(OwnerControl::new(
            request_id,
            source,
            destination,
            kind,
            epoch,
        ))
    }

    /// Creates a maintenance message.
    #[inline]
    pub const fn maintenance(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        capsule: KeyCapsuleId,
        kind: OwnerMaintenanceKind,
        epoch: TopologyEpoch,
    ) -> Self {
        Self::Maintenance(OwnerMaintenance::new(
            request_id,
            source,
            destination,
            capsule,
            kind,
            epoch,
        ))
    }

    /// Creates an AOF message.
    #[inline]
    pub const fn aof(
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        kind: OwnerAofKind,
        epoch: TopologyEpoch,
    ) -> Self {
        Self::Aof(OwnerAof::new(request_id, source, destination, kind, epoch))
    }

    /// Lane selected by this message.
    #[inline(always)]
    pub const fn lane(self) -> MailboxLane {
        match self {
            Self::Command(_) => MailboxLane::Command,
            Self::Reply(_) => MailboxLane::Reply,
            Self::Control(_) => MailboxLane::Control,
            Self::Maintenance(_) => MailboxLane::Maintenance,
            Self::Aof(_) => MailboxLane::Aof,
        }
    }

    /// Source owner.
    #[inline(always)]
    pub const fn source(self) -> OwnerId {
        match self {
            Self::Command(message) => message.source,
            Self::Reply(message) => message.source,
            Self::Control(message) => message.source,
            Self::Maintenance(message) => message.source,
            Self::Aof(message) => message.source,
        }
    }

    /// Destination owner.
    #[inline(always)]
    pub const fn destination(self) -> OwnerId {
        match self {
            Self::Command(message) => message.destination,
            Self::Reply(message) => message.destination,
            Self::Control(message) => message.destination,
            Self::Maintenance(message) => message.destination,
            Self::Aof(message) => message.destination,
        }
    }

    /// Topology epoch carried by this message.
    #[inline(always)]
    pub const fn epoch(self) -> TopologyEpoch {
        match self {
            Self::Command(message) => message.epoch,
            Self::Reply(message) => message.epoch,
            Self::Control(message) => message.epoch,
            Self::Maintenance(message) => message.epoch,
            Self::Aof(message) => message.epoch,
        }
    }

    /// Request or event ID carried by this message.
    #[inline(always)]
    pub const fn request_id(self) -> u64 {
        match self {
            Self::Command(message) => message.request_id,
            Self::Reply(message) => message.request_id,
            Self::Control(message) => message.request_id,
            Self::Maintenance(message) => message.request_id,
            Self::Aof(message) => message.request_id,
        }
    }

    #[inline(always)]
    const fn reply_credit_pair(self) -> Option<(OwnerId, OwnerId)> {
        match self {
            Self::Reply(reply) if reply.releases_credit => Some((reply.destination, reply.source)),
            _ => None,
        }
    }
}

/// Full-queue error preserving the rejected message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MailboxFull {
    message: OwnerMessage,
    lane: MailboxLane,
    source: OwnerId,
    destination: OwnerId,
    signal: MailboxBackpressure,
}

impl MailboxFull {
    #[inline]
    fn new(message: OwnerMessage) -> Self {
        let lane = message.lane();
        Self {
            message,
            lane,
            source: message.source(),
            destination: message.destination(),
            signal: lane.backpressure_signal(),
        }
    }

    #[inline]
    fn reply_credit_exhausted(message: OwnerMessage) -> Self {
        Self {
            message,
            lane: MailboxLane::Reply,
            source: message.source(),
            destination: message.destination(),
            signal: MailboxBackpressure::PrioritizeReplyDrain,
        }
    }

    /// Message that was not accepted by the mailbox.
    #[inline]
    pub const fn message(self) -> OwnerMessage {
        self.message
    }

    /// Full lane.
    #[inline]
    pub const fn lane(self) -> MailboxLane {
        self.lane
    }

    /// Source owner.
    #[inline]
    pub const fn source(self) -> OwnerId {
        self.source
    }

    /// Destination owner.
    #[inline]
    pub const fn destination(self) -> OwnerId {
        self.destination
    }

    /// Backpressure action required before retrying this lane.
    #[inline]
    pub const fn signal(self) -> MailboxBackpressure {
        self.signal
    }
}

/// Invalid owner route supplied to the mailbox fabric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidMailboxRoute {
    source: OwnerId,
    destination: OwnerId,
    owner_count: usize,
    kind: InvalidMailboxRouteKind,
}

impl InvalidMailboxRoute {
    #[inline]
    fn unknown_owner(source: OwnerId, destination: OwnerId, owner_count: usize) -> Self {
        Self {
            source,
            destination,
            owner_count,
            kind: InvalidMailboxRouteKind::UnknownOwner,
        }
    }

    #[inline]
    fn self_route(source: OwnerId, destination: OwnerId, owner_count: usize) -> Self {
        Self {
            source,
            destination,
            owner_count,
            kind: InvalidMailboxRouteKind::SelfRoute,
        }
    }

    /// Source owner.
    #[inline]
    pub const fn source(self) -> OwnerId {
        self.source
    }

    /// Destination owner.
    #[inline]
    pub const fn destination(self) -> OwnerId {
        self.destination
    }

    /// Configured owner count.
    #[inline]
    pub const fn owner_count(self) -> usize {
        self.owner_count
    }

    /// Reason this route is invalid for the mailbox fabric.
    #[inline]
    pub const fn kind(self) -> InvalidMailboxRouteKind {
        self.kind
    }
}

impl std::fmt::Display for InvalidMailboxRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid mailbox route source={} destination={} owner_count={} kind={:?}",
            self.source.get(),
            self.destination.get(),
            self.owner_count,
            self.kind
        )
    }
}

impl std::error::Error for InvalidMailboxRoute {}

/// Send failure for an owner mailbox message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MailboxSendError {
    /// Source/destination is outside the owner set or is a local self route.
    InvalidRoute(InvalidMailboxRoute),
    /// The reverse reply lane has no credit for accepting another command.
    ReplyCreditExhausted(MailboxFull),
    /// The selected bounded queue was full, and the message was not accepted.
    Full(MailboxFull),
}

impl MailboxSendError {
    /// Returns the rejected message when the message reached admission checks.
    #[inline]
    pub const fn rejected_message(self) -> Option<OwnerMessage> {
        match self {
            Self::InvalidRoute(_) => None,
            Self::ReplyCreditExhausted(full) => Some(full.message),
            Self::Full(full) => Some(full.message),
        }
    }
}

impl std::fmt::Display for MailboxSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRoute(route) => route.fmt(f),
            Self::ReplyCreditExhausted(full) => write!(
                f,
                "reply credit exhausted for command from owner {} to owner {}",
                full.source.get(),
                full.destination.get()
            ),
            Self::Full(full) => write!(
                f,
                "mailbox lane {:?} from owner {} to owner {} is full",
                full.lane,
                full.source.get(),
                full.destination.get()
            ),
        }
    }
}

impl std::error::Error for MailboxSendError {}

/// Mailbox fabric construction error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MailboxFabricError {
    /// Ring slots must be greater than one because one slot is a sentinel.
    RingSlotsTooSmall { ring_slots: usize },
    /// Ring slots must be a power of two for mask-based wraparound.
    RingSlotsNotPowerOfTwo { ring_slots: usize },
    /// The full mesh queue count overflowed `usize`.
    QueueCountOverflow {
        owner_count: usize,
        lane_count: usize,
    },
}

impl std::fmt::Display for MailboxFabricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::RingSlotsTooSmall { ring_slots } => {
                write!(f, "mailbox ring slots must be > 1, got {ring_slots}")
            }
            Self::RingSlotsNotPowerOfTwo { ring_slots } => {
                write!(
                    f,
                    "mailbox ring slots must be a power of two, got {ring_slots}"
                )
            }
            Self::QueueCountOverflow {
                owner_count,
                lane_count,
            } => write!(
                f,
                "mailbox queue count overflow for owner_count={owner_count} lane_count={lane_count}"
            ),
        }
    }
}

impl std::error::Error for MailboxFabricError {}

/// Per-lane source cursor used to avoid always draining owner zero first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MailboxDrainCursor {
    next_source: usize,
}

impl MailboxDrainCursor {
    /// Creates a cursor starting from source owner zero.
    #[inline]
    pub const fn new() -> Self {
        Self { next_source: 0 }
    }

    /// Returns the source index that will be attempted first on the next drain.
    #[inline]
    pub const fn next_source_index(self) -> usize {
        self.next_source
    }
}

/// Queue pressure snapshot for one directed mailbox lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MailboxLaneSnapshot {
    /// Source owner.
    pub source: OwnerId,
    /// Destination owner.
    pub destination: OwnerId,
    /// Lane represented by this snapshot.
    pub lane: MailboxLane,
    /// Approximate queued slot count.
    pub used_slots: usize,
    /// Usable slot capacity.
    pub capacity_slots: usize,
    /// Approximate queued descriptor bytes.
    pub used_bytes: usize,
    /// Descriptor byte capacity.
    pub capacity_bytes: usize,
    /// Reserved reply credits for commands from source to destination.
    pub reply_credits_used: usize,
    /// Maximum reserved reply credits for this source/destination pair.
    pub reply_credits_capacity: usize,
}

impl MailboxLaneSnapshot {
    /// Remaining slots in this directed lane.
    #[inline]
    pub const fn available_slots(self) -> usize {
        self.capacity_slots.saturating_sub(self.used_slots)
    }

    /// Remaining reply credits for commands from source to destination.
    #[inline]
    pub const fn available_reply_credits(self) -> usize {
        self.reply_credits_capacity
            .saturating_sub(self.reply_credits_used)
    }
}

/// Bounded SPSC owner mailbox mesh.
///
/// `N` is the ring slot count. Usable per-lane capacity is `N - 1` because
/// one slot is reserved to distinguish full from empty.
pub struct OwnerMailboxMesh<const N: usize> {
    config: TopologyConfig,
    senders: Box<[SpscSender<OwnerMessage, N>]>,
    receivers: Box<[SpscReceiver<OwnerMessage, N>]>,
    reply_credits: Box<[CachePadded<AtomicUsize>]>,
}

impl<const N: usize> OwnerMailboxMesh<N> {
    /// Builds an owner-to-owner full mesh for a validated topology.
    ///
    /// # Errors
    ///
    /// Returns an error if `N` is not a usable power-of-two ring size or if the
    /// full mesh queue count overflows `usize`.
    pub fn new(config: TopologyConfig) -> Result<Self, MailboxFabricError> {
        validate_ring_slots::<N>()?;

        let remote_pair_count = config
            .owner_count()
            .checked_mul(config.owner_count().saturating_sub(1))
            .ok_or(MailboxFabricError::QueueCountOverflow {
                owner_count: config.owner_count(),
                lane_count: MAILBOX_LANE_COUNT,
            })?;
        let queue_count = remote_pair_count.checked_mul(MAILBOX_LANE_COUNT).ok_or(
            MailboxFabricError::QueueCountOverflow {
                owner_count: config.owner_count(),
                lane_count: MAILBOX_LANE_COUNT,
            },
        )?;

        let mut senders = Vec::with_capacity(queue_count);
        let mut receivers = Vec::with_capacity(queue_count);
        for _ in 0..queue_count {
            let (sender, receiver) = spsc_channel();
            senders.push(sender);
            receivers.push(receiver);
        }
        let mut reply_credits = Vec::with_capacity(remote_pair_count);
        for _ in 0..remote_pair_count {
            reply_credits.push(CachePadded::new(AtomicUsize::new(0)));
        }

        Ok(Self {
            config,
            senders: senders.into_boxed_slice(),
            receivers: receivers.into_boxed_slice(),
            reply_credits: reply_credits.into_boxed_slice(),
        })
    }

    /// Topology config used to size the mesh.
    #[inline]
    pub const fn config(&self) -> TopologyConfig {
        self.config
    }

    /// Number of owner reactors in this mesh.
    #[inline]
    pub const fn owner_count(&self) -> usize {
        self.config.owner_count()
    }

    /// Ring slot count, including the sentinel slot.
    #[inline]
    pub const fn ring_slots(&self) -> usize {
        N
    }

    /// Usable capacity per directed `(source, destination, lane)` queue.
    #[inline]
    pub const fn lane_capacity(&self) -> usize {
        N - 1
    }

    /// Number of directed SPSC queues in the full mesh.
    #[inline]
    pub fn queue_count(&self) -> usize {
        self.senders.len()
    }

    /// Attempts to enqueue a message into its directed lane.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the route is invalid, the bounded queue has
    /// no spare slot, or reply credit is exhausted. Queue and credit errors
    /// return the original message so the caller can retry after backpressure.
    #[inline]
    pub fn try_send(&self, message: OwnerMessage) -> Result<(), MailboxSendError> {
        let source = message.source();
        let destination = message.destination();
        let lane = message.lane();
        let index = self
            .queue_index(source, destination, lane)
            .map_err(MailboxSendError::InvalidRoute)?;
        let command_requires_reply =
            matches!(message, OwnerMessage::Command(command) if command.reply_required);

        if command_requires_reply && self.senders[index].is_full() {
            return Err(MailboxSendError::Full(MailboxFull::new(message)));
        }
        let reserved_credit = if command_requires_reply {
            if !self.try_reserve_reply_credit(source, destination) {
                return Err(MailboxSendError::ReplyCreditExhausted(
                    MailboxFull::reply_credit_exhausted(message),
                ));
            }
            true
        } else {
            false
        };

        match self.senders[index].try_send(message) {
            Ok(()) => Ok(()),
            Err(message) => {
                if reserved_credit {
                    self.release_reply_credit(source, destination);
                }
                Err(MailboxSendError::Full(MailboxFull::new(message)))
            }
        }
    }

    /// Drains up to `limit` messages for one directed queue.
    ///
    /// # Errors
    ///
    /// Returns an error if `source` or `destination` is not valid for this
    /// mesh's topology.
    #[inline]
    pub fn drain_lane<F>(
        &self,
        source: OwnerId,
        destination: OwnerId,
        lane: MailboxLane,
        limit: usize,
        visit: F,
    ) -> Result<usize, InvalidMailboxRoute>
    where
        F: FnMut(OwnerMessage),
    {
        let index = self.queue_index(source, destination, lane)?;
        Ok(self.drain_queue(index, limit, visit))
    }

    /// Drains one lane for `destination` from all source owners using a cursor.
    ///
    /// The cursor advances after each visited source so a saturated source does
    /// not permanently monopolize the destination's drain budget.
    ///
    /// # Errors
    ///
    /// Returns an error if `destination` is not valid for this mesh's topology.
    #[inline]
    pub fn drain_owner_lane_with_cursor<F>(
        &self,
        destination: OwnerId,
        lane: MailboxLane,
        cursor: &mut MailboxDrainCursor,
        limit: usize,
        mut visit: F,
    ) -> Result<usize, InvalidMailboxRoute>
    where
        F: FnMut(OwnerMessage),
    {
        if !self.owner_exists(destination) {
            return Err(InvalidMailboxRoute::unknown_owner(
                OwnerId::from_validated_index(0),
                destination,
                self.owner_count(),
            ));
        }
        if limit == 0 || self.owner_count() <= 1 {
            return Ok(0);
        }

        let owner_count = self.owner_count();
        let mut source_index = cursor.next_source % owner_count;
        let mut drained = 0;

        for _ in 0..owner_count {
            let source = OwnerId::from_validated_index(source_index);
            if source == destination {
                source_index = (source_index + 1) % owner_count;
                continue;
            }
            let remaining = limit - drained;
            let drained_now = self.drain_lane(source, destination, lane, remaining, |message| {
                visit(message)
            })?;

            drained += drained_now;
            source_index = (source_index + 1) % owner_count;

            if drained == limit {
                break;
            }
        }

        cursor.next_source = source_index;
        Ok(drained)
    }

    /// Returns the approximate length of one directed queue.
    #[inline]
    pub fn lane_len(
        &self,
        source: OwnerId,
        destination: OwnerId,
        lane: MailboxLane,
    ) -> Result<usize, InvalidMailboxRoute> {
        let index = self.queue_index(source, destination, lane)?;
        Ok(self.receivers[index].len())
    }

    /// Returns the currently reserved reply credits for commands from `source`
    /// to `destination`.
    #[inline]
    pub fn reply_credits_used(
        &self,
        source: OwnerId,
        destination: OwnerId,
    ) -> Result<usize, InvalidMailboxRoute> {
        self.queue_index(source, destination, MailboxLane::Command)?;
        Ok(
            self.reply_credits[self.remote_pair_index_unchecked(source, destination)]
                .load(Ordering::Acquire),
        )
    }

    /// Returns one directed lane's slot, byte, and reply-credit pressure.
    #[inline]
    pub fn lane_snapshot(
        &self,
        source: OwnerId,
        destination: OwnerId,
        lane: MailboxLane,
    ) -> Result<MailboxLaneSnapshot, InvalidMailboxRoute> {
        let index = self.queue_index(source, destination, lane)?;
        let used_slots = self.receivers[index].len();
        let capacity_slots = self.lane_capacity();
        let reply_credits_used = self.reply_credits_used(source, destination)?;
        Ok(MailboxLaneSnapshot {
            source,
            destination,
            lane,
            used_slots,
            capacity_slots,
            used_bytes: used_slots * OWNER_MESSAGE_SLOT_SIZE_BYTES,
            capacity_bytes: capacity_slots * OWNER_MESSAGE_SLOT_SIZE_BYTES,
            reply_credits_used,
            reply_credits_capacity: capacity_slots,
        })
    }

    /// Returns whether one directed queue is full.
    #[inline]
    pub fn lane_is_full(
        &self,
        source: OwnerId,
        destination: OwnerId,
        lane: MailboxLane,
    ) -> Result<bool, InvalidMailboxRoute> {
        let index = self.queue_index(source, destination, lane)?;
        Ok(self.senders[index].is_full())
    }

    #[inline(always)]
    fn queue_index(
        &self,
        source: OwnerId,
        destination: OwnerId,
        lane: MailboxLane,
    ) -> Result<usize, InvalidMailboxRoute> {
        if !self.owner_exists(source) || !self.owner_exists(destination) {
            return Err(InvalidMailboxRoute::unknown_owner(
                source,
                destination,
                self.owner_count(),
            ));
        }
        if source == destination {
            return Err(InvalidMailboxRoute::self_route(
                source,
                destination,
                self.owner_count(),
            ));
        }

        Ok(self.queue_index_unchecked(source, destination, lane))
    }

    #[inline(always)]
    fn queue_index_unchecked(
        &self,
        source: OwnerId,
        destination: OwnerId,
        lane: MailboxLane,
    ) -> usize {
        self.remote_pair_index_unchecked(source, destination) * MAILBOX_LANE_COUNT + lane.index()
    }

    #[inline(always)]
    fn remote_pair_index_unchecked(&self, source: OwnerId, destination: OwnerId) -> usize {
        debug_assert!(source != destination);
        let destination_rank = if destination.get() < source.get() {
            destination.get()
        } else {
            destination.get() - 1
        };
        source.get() * (self.owner_count() - 1) + destination_rank
    }

    #[inline(always)]
    const fn owner_exists(&self, owner: OwnerId) -> bool {
        owner.get() < self.config.owner_count()
    }

    #[inline]
    fn try_reserve_reply_credit(&self, source: OwnerId, destination: OwnerId) -> bool {
        let index = self.remote_pair_index_unchecked(source, destination);
        self.reply_credits[index]
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                (value < self.lane_capacity()).then_some(value + 1)
            })
            .is_ok()
    }

    #[inline]
    fn release_reply_credit(&self, source: OwnerId, destination: OwnerId) {
        let index = self.remote_pair_index_unchecked(source, destination);
        let result =
            self.reply_credits[index].fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                (value > 0).then_some(value - 1)
            });
        debug_assert!(result.is_ok(), "released missing mailbox reply credit");
    }

    #[inline]
    fn drain_queue<F>(&self, index: usize, limit: usize, mut visit: F) -> usize
    where
        F: FnMut(OwnerMessage),
    {
        self.receivers[index].drain_batch(limit, |message| {
            let reply_credit_pair = message.reply_credit_pair();
            visit(message);
            if let Some((source, destination)) = reply_credit_pair {
                self.release_reply_credit(source, destination);
            }
        })
    }
}

fn validate_ring_slots<const N: usize>() -> Result<(), MailboxFabricError> {
    if N <= 1 {
        return Err(MailboxFabricError::RingSlotsTooSmall { ring_slots: N });
    }
    if !N.is_power_of_two() {
        return Err(MailboxFabricError::RingSlotsNotPowerOfTwo { ring_slots: N });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;
    use crate::owner::OwnerTopology;

    fn topology(owner_count: usize) -> OwnerTopology {
        OwnerTopology::new(TopologyConfig::new(owner_count, 64).expect("valid topology"))
    }

    fn owner(topology: &OwnerTopology, index: usize) -> OwnerId {
        topology.owner_id(index).expect("owner exists")
    }

    fn capsule(topology: &OwnerTopology, index: usize) -> KeyCapsuleId {
        topology.capsule_id(index).expect("capsule exists")
    }

    fn message_for_lane(
        lane: MailboxLane,
        request_id: u64,
        source: OwnerId,
        destination: OwnerId,
        capsule: KeyCapsuleId,
    ) -> OwnerMessage {
        match lane {
            MailboxLane::Control => OwnerMessage::control(
                request_id,
                source,
                destination,
                OwnerControlKind::TopologyChanged,
                TopologyEpoch::INITIAL,
            ),
            MailboxLane::Reply => OwnerMessage::reply(
                request_id,
                source,
                destination,
                OwnerReplyStatus::Ok,
                TopologyEpoch::INITIAL,
            ),
            MailboxLane::Command => OwnerMessage::one_way_command(
                request_id,
                source,
                destination,
                capsule,
                TopologyEpoch::INITIAL,
            ),
            MailboxLane::Maintenance => OwnerMessage::maintenance(
                request_id,
                source,
                destination,
                capsule,
                OwnerMaintenanceKind::Expire,
                TopologyEpoch::INITIAL,
            ),
            MailboxLane::Aof => OwnerMessage::aof(
                request_id,
                source,
                destination,
                OwnerAofKind::RecordsReady,
                TopologyEpoch::INITIAL,
            ),
        }
    }

    #[test]
    fn mailbox_message_descriptor_stays_cache_line_sized() {
        assert!(OWNER_MESSAGE_SIZE_BYTES <= 64);
        assert!(OWNER_MESSAGE_SLOT_SIZE_BYTES <= 64);
    }

    #[test]
    fn mailbox_build_rejects_invalid_ring_slots() {
        let config = TopologyConfig::new(2, 64).expect("valid topology");

        assert_eq!(
            OwnerMailboxMesh::<1>::new(config).map(|_| ()),
            Err(MailboxFabricError::RingSlotsTooSmall { ring_slots: 1 })
        );
        assert_eq!(
            OwnerMailboxMesh::<3>::new(config).map(|_| ()),
            Err(MailboxFabricError::RingSlotsNotPowerOfTwo { ring_slots: 3 })
        );
    }

    #[test]
    fn mailbox_fifo_per_lane() {
        let topology = topology(2);
        let config = topology.config();
        let mesh = OwnerMailboxMesh::<16>::new(config).expect("mesh builds");
        let source = owner(&topology, 0);
        let destination = owner(&topology, 1);
        let capsule = capsule(&topology, 0);

        for lane in MailboxLane::ALL {
            for request_id in 0..6 {
                mesh.try_send(message_for_lane(
                    lane,
                    request_id,
                    source,
                    destination,
                    capsule,
                ))
                .expect("message accepted");
            }
        }

        for lane in MailboxLane::ALL {
            let mut drained = Vec::new();
            let count = mesh
                .drain_lane(source, destination, lane, 16, |message| {
                    drained.push(message.request_id());
                    assert_eq!(message.lane(), lane);
                })
                .expect("valid lane");

            assert_eq!(count, 6);
            assert_eq!(drained, vec![0, 1, 2, 3, 4, 5]);
        }
    }

    #[test]
    fn mailbox_self_route_requires_local_bypass() {
        let topology = topology(2);
        let mesh = OwnerMailboxMesh::<16>::new(topology.config()).expect("mesh builds");
        let source = owner(&topology, 0);
        let capsule = capsule(&topology, 0);
        let message =
            OwnerMessage::one_way_command(1, source, source, capsule, TopologyEpoch::INITIAL);

        let error = mesh
            .try_send(message)
            .expect_err("self routes bypass mailbox");
        let MailboxSendError::InvalidRoute(route) = error else {
            panic!("expected invalid self route");
        };
        assert_eq!(route.kind(), InvalidMailboxRouteKind::SelfRoute);
    }

    #[test]
    fn mailbox_full_returns_message_and_preserves_accepted_commands() {
        let topology = topology(2);
        let config = topology.config();
        let mesh = OwnerMailboxMesh::<4>::new(config).expect("mesh builds");
        let source = owner(&topology, 0);
        let destination = owner(&topology, 1);
        let capsule = capsule(&topology, 0);

        for request_id in 0..3 {
            mesh.try_send(OwnerMessage::one_way_command(
                request_id,
                source,
                destination,
                capsule,
                TopologyEpoch::INITIAL,
            ))
            .expect("message accepted");
        }

        let rejected =
            OwnerMessage::one_way_command(3, source, destination, capsule, TopologyEpoch::INITIAL);
        let error = mesh.try_send(rejected).expect_err("queue is full");
        let MailboxSendError::Full(full) = error else {
            panic!("expected full queue");
        };
        assert_eq!(full.message(), rejected);
        assert_eq!(full.signal(), MailboxBackpressure::PauseIngressReads);
        assert_eq!(
            mesh.lane_is_full(source, destination, MailboxLane::Command),
            Ok(true)
        );

        let mut accepted = Vec::new();
        let count = mesh
            .drain_lane(source, destination, MailboxLane::Command, 4, |message| {
                accepted.push(message.request_id());
            })
            .expect("valid lane");
        assert_eq!(count, 3);
        assert_eq!(accepted, vec![0, 1, 2]);

        mesh.try_send(rejected).expect("rejected message can retry");
        let mut retried = Vec::new();
        mesh.drain_lane(source, destination, MailboxLane::Command, 4, |message| {
            retried.push(message.request_id());
        })
        .expect("valid lane");
        assert_eq!(retried, vec![3]);
    }

    #[test]
    fn mailbox_reply_credit_guarantees_reverse_reply_capacity() {
        let topology = topology(2);
        let mesh = OwnerMailboxMesh::<4>::new(topology.config()).expect("mesh builds");
        let source = owner(&topology, 0);
        let destination = owner(&topology, 1);
        let capsule = capsule(&topology, 0);

        for request_id in 0..3 {
            mesh.try_send(OwnerMessage::command(
                request_id,
                source,
                destination,
                capsule,
                TopologyEpoch::INITIAL,
            ))
            .expect("credited command accepted");
        }

        let mut commands = Vec::new();
        mesh.drain_lane(source, destination, MailboxLane::Command, 3, |message| {
            let OwnerMessage::Command(command) = message else {
                panic!("expected command");
            };
            commands.push(command);
        })
        .expect("valid lane");

        for command in commands {
            let reply = OwnerMessage::reply_to(command, OwnerReplyStatus::Ok);
            mesh.try_send(reply)
                .expect("reserved reply credit guarantees capacity");
        }
        assert_eq!(
            mesh.lane_is_full(destination, source, MailboxLane::Reply),
            Ok(true)
        );

        let blocked =
            OwnerMessage::command(3, source, destination, capsule, TopologyEpoch::INITIAL);
        let error = mesh
            .try_send(blocked)
            .expect_err("reply credits block further command acceptance");
        let MailboxSendError::ReplyCreditExhausted(full) = error else {
            panic!("expected reply credit exhaustion");
        };
        assert_eq!(full.message(), blocked);
        assert_eq!(full.lane(), MailboxLane::Reply);
        assert_eq!(full.signal(), MailboxBackpressure::PrioritizeReplyDrain);

        let mut replies = Vec::new();
        mesh.drain_lane(destination, source, MailboxLane::Reply, 3, |message| {
            replies.push(message.request_id());
        })
        .expect("valid reply lane");
        assert_eq!(replies, vec![0, 1, 2]);

        mesh.try_send(blocked)
            .expect("reply credit was released by draining replies");
    }

    #[test]
    fn mailbox_pressure_snapshot_reports_slots_bytes_and_credits() {
        let topology = topology(2);
        let mesh = OwnerMailboxMesh::<8>::new(topology.config()).expect("mesh builds");
        let source = owner(&topology, 0);
        let destination = owner(&topology, 1);
        let capsule = capsule(&topology, 0);

        for request_id in 0..2 {
            mesh.try_send(OwnerMessage::command(
                request_id,
                source,
                destination,
                capsule,
                TopologyEpoch::INITIAL,
            ))
            .expect("credited command accepted");
        }

        let snapshot = mesh
            .lane_snapshot(source, destination, MailboxLane::Command)
            .expect("valid lane");
        assert_eq!(snapshot.used_slots, 2);
        assert_eq!(snapshot.capacity_slots, 7);
        assert_eq!(snapshot.used_bytes, 2 * OWNER_MESSAGE_SLOT_SIZE_BYTES);
        assert_eq!(snapshot.capacity_bytes, 7 * OWNER_MESSAGE_SLOT_SIZE_BYTES);
        assert_eq!(snapshot.reply_credits_used, 2);
        assert_eq!(snapshot.available_reply_credits(), 5);

        let mut commands = Vec::new();
        mesh.drain_lane(source, destination, MailboxLane::Command, 2, |message| {
            let OwnerMessage::Command(command) = message else {
                panic!("expected command");
            };
            commands.push(command);
        })
        .expect("valid command lane");
        for command in commands {
            mesh.try_send(OwnerMessage::reply_to(command, OwnerReplyStatus::Ok))
                .expect("reserved reply credit guarantees reply slot");
        }

        let reply_snapshot = mesh
            .lane_snapshot(destination, source, MailboxLane::Reply)
            .expect("valid reply lane");
        assert_eq!(reply_snapshot.used_slots, 2);
        assert_eq!(
            mesh.reply_credits_used(source, destination),
            Ok(2),
            "reply credits stay reserved until reply drain"
        );

        mesh.drain_lane(destination, source, MailboxLane::Reply, 2, |_| {})
            .expect("valid reply lane");
        assert_eq!(mesh.reply_credits_used(source, destination), Ok(0));
    }

    #[test]
    fn mailbox_stress_all_owners_to_all_owners() {
        const OWNER_COUNT: usize = 4;
        const RING_SLOTS: usize = 1024;

        let topology = topology(OWNER_COUNT);
        let mesh =
            Arc::new(OwnerMailboxMesh::<RING_SLOTS>::new(topology.config()).expect("mesh builds"));
        let owners: Vec<OwnerId> = (0..OWNER_COUNT)
            .map(|index| owner(&topology, index))
            .collect();
        let capsule = capsule(&topology, 0);
        let messages_per_pair = if cfg!(miri) { 16 } else { 2_048 };

        let mut consumers = Vec::new();
        for &destination in &owners {
            let mesh = Arc::clone(&mesh);
            consumers.push(thread::spawn(move || {
                let expected = (OWNER_COUNT - 1) * messages_per_pair;
                let mut received = 0usize;
                let mut cursor = MailboxDrainCursor::new();
                let mut next_by_source = [0u64; OWNER_COUNT];

                while received < expected {
                    let drained = mesh
                        .drain_owner_lane_with_cursor(
                            destination,
                            MailboxLane::Command,
                            &mut cursor,
                            64,
                            |message| {
                                assert_eq!(message.destination(), destination);
                                assert_eq!(message.lane(), MailboxLane::Command);
                                let source_index = message.source().get();
                                assert_eq!(message.request_id(), next_by_source[source_index]);
                                next_by_source[source_index] += 1;
                                received += 1;
                            },
                        )
                        .expect("valid destination");

                    if drained == 0 {
                        std::hint::spin_loop();
                    }
                }

                next_by_source
            }));
        }

        let mut producers = Vec::new();
        for &source in &owners {
            let mesh = Arc::clone(&mesh);
            let destinations = owners.clone();
            producers.push(thread::spawn(move || {
                for request_id in 0..messages_per_pair as u64 {
                    for &destination in &destinations {
                        if destination == source {
                            continue;
                        }

                        let mut message = OwnerMessage::one_way_command(
                            request_id,
                            source,
                            destination,
                            capsule,
                            TopologyEpoch::INITIAL,
                        );

                        loop {
                            match mesh.try_send(message) {
                                Ok(()) => break,
                                Err(MailboxSendError::Full(full)) => {
                                    message = full.message();
                                    std::hint::spin_loop();
                                }
                                Err(error) => panic!("unexpected mailbox send error: {error}"),
                            }
                        }
                    }
                }
            }));
        }

        for producer in producers {
            producer.join().expect("producer finished");
        }

        for consumer in consumers {
            let next_by_source = consumer.join().expect("consumer finished");
            assert_eq!(
                next_by_source.iter().sum::<u64>(),
                (OWNER_COUNT as u64 - 1) * messages_per_pair as u64
            );
            assert_eq!(
                next_by_source
                    .iter()
                    .filter(|&&count| count == messages_per_pair as u64)
                    .count(),
                OWNER_COUNT - 1
            );
        }
    }
}
