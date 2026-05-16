//! Shared-nothing owner transaction protocol primitives.
//!
//! These types describe the SN-007 prepare/commit/abort vocabulary without
//! making command parsing or table layout aware of cross-owner coordination.

use vortex_common::{VortexKey, VortexValue};

/// Monotonic transaction identifier allocated by the ingress coordinator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct TxnId(u64);

impl TxnId {
    /// Creates a transaction identifier.
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw identifier.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Owner-local intent operation kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TxnIntentKind {
    /// `MSET key value [key value ...]`.
    Mset = 0,
    /// `MSETNX key value [key value ...]`.
    MsetNx = 1,
    /// `RENAME source destination`.
    Rename = 2,
    /// `RENAMENX source destination`.
    RenameNx = 3,
    /// `COPY source destination`.
    Copy = 4,
    /// `COPY source destination REPLACE`.
    CopyReplace = 5,
}

impl TxnIntentKind {
    /// Returns whether this transaction kind is a dual-key command.
    #[inline]
    pub const fn is_dual_key(self) -> bool {
        matches!(
            self,
            Self::Rename | Self::RenameNx | Self::Copy | Self::CopyReplace
        )
    }

    /// Returns whether the source key must be deleted at commit.
    #[inline]
    pub const fn deletes_source(self) -> bool {
        matches!(self, Self::Rename | Self::RenameNx)
    }
}

/// Owner-local action validated during prepare and applied during commit.
#[derive(Clone, Debug, PartialEq)]
pub enum TxnAction {
    /// Require that a key is live when prepare runs.
    AssertExists { key: VortexKey },
    /// Require that a key is not live when prepare runs.
    AssertAbsent { key: VortexKey },
    /// Set a key to a value, preserving the supplied TTL deadline.
    Set {
        key: VortexKey,
        value: VortexValue,
        ttl_deadline: u64,
    },
    /// Delete a key at commit.
    Delete { key: VortexKey },
}

impl TxnAction {
    /// Key touched by this action.
    #[inline]
    pub fn key(&self) -> &VortexKey {
        match self {
            Self::AssertExists { key }
            | Self::AssertAbsent { key }
            | Self::Set { key, .. }
            | Self::Delete { key } => key,
        }
    }
}

/// Owner-local write intent prepared before a cross-owner commit decision.
#[derive(Clone, Debug, PartialEq)]
pub struct TxnIntent {
    kind: TxnIntentKind,
    actions: Box<[TxnAction]>,
}

impl TxnIntent {
    /// Builds an owner-local write intent from already routed key/value pairs.
    #[inline]
    pub fn new(kind: TxnIntentKind, pairs: Box<[(VortexKey, VortexValue)]>) -> Self {
        let actions = pairs
            .into_vec()
            .into_iter()
            .map(|(key, value)| TxnAction::Set {
                key,
                value,
                ttl_deadline: 0,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { kind, actions }
    }

    /// Builds an owner-local intent from explicit transaction actions.
    #[inline]
    pub fn from_actions(kind: TxnIntentKind, actions: Box<[TxnAction]>) -> Self {
        Self { kind, actions }
    }

    /// Intent kind.
    #[inline]
    pub const fn kind(&self) -> TxnIntentKind {
        self.kind
    }

    /// Actions owned by this participant.
    #[inline]
    pub fn actions(&self) -> &[TxnAction] {
        &self.actions
    }

    /// Consumes the intent and returns the owned actions.
    #[inline]
    pub fn into_actions(self) -> Box<[TxnAction]> {
        self.actions
    }

    /// Returns whether this intent mentions `key`.
    #[inline]
    pub fn touches_key(&self, key: &[u8]) -> bool {
        self.actions
            .iter()
            .any(|action| action.key().as_bytes() == key)
    }

    /// Returns whether two owner-local intents overlap.
    #[inline]
    pub fn overlaps(&self, other: &Self) -> bool {
        self.actions
            .iter()
            .any(|action| other.touches_key(action.key().as_bytes()))
    }
}

/// Result of an owner-local prepare vote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i64)]
pub enum TxnPrepareOutcome {
    /// Intent was accepted and must be committed or aborted.
    Prepared = 1,
    /// Command precondition failed before mutation, for example `MSETNX`.
    ConditionFailed = 0,
    /// Another prepared transaction currently owns an overlapping key.
    Conflict = -1,
    /// A duplicate transaction identifier was observed.
    DuplicateTxn = -2,
    /// Source key was missing for a source-dependent operation.
    SourceMissing = -3,
    /// Destination key existed for an NX/no-replace operation.
    DestinationExists = -4,
}

impl TxnPrepareOutcome {
    /// Encodes the outcome as a small integer for internal owner replies.
    #[inline]
    pub const fn code(self) -> i64 {
        self as i64
    }

    /// Decodes an internal owner reply status.
    #[inline]
    pub const fn from_code(code: i64) -> Self {
        match code {
            1 => Self::Prepared,
            0 => Self::ConditionFailed,
            -2 => Self::DuplicateTxn,
            -3 => Self::SourceMissing,
            -4 => Self::DestinationExists,
            _ => Self::Conflict,
        }
    }
}

/// Result of commit or abort cleanup for a prepared owner transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i64)]
pub enum TxnFinishOutcome {
    /// Prepared transaction was found and finished.
    Finished = 1,
    /// The owner had no prepared state for that transaction.
    Missing = 0,
}

impl TxnFinishOutcome {
    /// Encodes the outcome as a small integer for internal owner replies.
    #[inline]
    pub const fn code(self) -> i64 {
        self as i64
    }

    /// Decodes an internal owner reply status.
    #[inline]
    pub const fn from_code(code: i64) -> Self {
        match code {
            1 => Self::Finished,
            _ => Self::Missing,
        }
    }
}
