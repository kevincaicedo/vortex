//! Shared-nothing owner topology primitives.
//!
//! This module is intentionally not wired into command execution yet. It holds
//! typed routing contracts for the shared-nothing experiment while the default
//! engine continues to execute through [`crate::ConcurrentKeyspace`].

pub mod continuation;
pub mod envelope;
pub mod executor;
pub mod harness;
pub mod locality;
pub mod mailbox;
pub mod routing;
pub mod scatter;
pub mod transaction;

pub use continuation::{
    ContinuationAcceptError, ContinuationBackpressure, ContinuationHarnessError,
    PublishedContinuation, ReactorContinuationHarness, ReactorContinuationMetrics,
};
pub use envelope::{
    LocalBorrowedEnvelope, RemoteBufferLease, RemoteCommandEnvelope, RemoteEnvelopeAccounting,
    RemoteEnvelopeOp, RemoteLeaseEnvelope, RemoteReplyBuffer, RemoteSmallEnvelope,
};
pub use executor::{
    OwnedSharedNothingCommand, RemoteContinuation, SharedNothingConnectionGeneration,
    SharedNothingConnectionId, SharedNothingConnectionToken, SharedNothingExecutionResult,
    SharedNothingExecutor, SharedNothingExecutorCostProbe, SharedNothingOwnerDispatch,
    SharedNothingOwnerRuntime, SharedNothingParseCostKind,
};
pub use harness::{
    EngineOwnerHarness, OwnerHarnessCommand, OwnerHarnessReply, OwnerHarnessResult,
    ThreadedOwnerHarness, shared_keyspace_del, shared_keyspace_get, shared_keyspace_set,
};
pub use mailbox::{
    InvalidMailboxRoute, InvalidMailboxRouteKind, MAILBOX_DRAIN_PRIORITY, MAILBOX_LANE_COUNT,
    MailboxBackpressure, MailboxDrainCursor, MailboxFabricError, MailboxFull, MailboxLane,
    MailboxLaneSnapshot, MailboxSendError, OWNER_MESSAGE_SIZE_BYTES, OWNER_MESSAGE_SLOT_SIZE_BYTES,
    OwnerAof, OwnerAofKind, OwnerCommand, OwnerControl, OwnerControlKind, OwnerMailboxMesh,
    OwnerMaintenance, OwnerMaintenanceKind, OwnerMessage, OwnerReply, OwnerReplyStatus,
};
pub use routing::{
    KeyCapsuleId, KeyRoute, OwnerId, OwnerTopology, RouteHash, RoutingDebugSnapshot,
    TopologyConfig, TopologyConfigError, TopologyEpoch,
};
pub use scatter::{
    PublishedScatterGather, SCATTER_CONNECTION_PENDING_AGGREGATES, SCATTER_CONNECTION_PENDING_KEYS,
    SCATTER_INGRESS_REPLY_BUDGET_CELLS, SCATTER_INGRESS_REPLY_BUDGET_PARTIALS,
    SCATTER_INLINE_KEY_TARGET, SCATTER_MAX_KEYS_PER_SUBPLAN, SCATTER_MAX_READ_WIDTH,
    SCATTER_OWNER_READ_BUDGET_KEYS, ScatterGatherAcceptError, ScatterGatherBackpressure,
    ScatterGatherHarness, ScatterGatherHarnessError, ScatterGatherMetrics, ScatterGatherReadKind,
    ScatterGatherResponse,
};
pub use transaction::{
    TxnAction, TxnFinishOutcome, TxnId, TxnIntent, TxnIntentKind, TxnPrepareOutcome,
};
