//! Engine side-effect vocabulary shared with IO coordination.
//!
//! Command handlers shape RESP replies; domain/keyspace code owns mutation
//! sequencing and returns these typed effects for the reactor to persist.

use vortex_common::VortexKey;

use crate::keyspace::AofLsn;

/// A persistable AOF record produced by a side effect outside the original command payload.
#[derive(Debug)]
pub struct AofRecord {
    pub lsn: AofLsn,
    pub key: VortexKey,
}

/// Optional batch of AOF side records, currently used for eviction deletes.
pub type AofRecords = Option<Box<[AofRecord]>>;

/// Command-domain mutation error classified before wire-response formatting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationErrorKind {
    WrongType,
    NotInteger,
    NotFloat,
    Overflow,
    LsnOverflow,
    OutOfMemory,
    NoSuchKey,
}

/// A committed command mutation that must be appended to AOF by the reactor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AofCommitEffect {
    lsn: AofLsn,
}

impl AofCommitEffect {
    #[inline]
    pub const fn new(lsn: AofLsn) -> Self {
        Self { lsn }
    }

    #[inline]
    pub const fn lsn(self) -> AofLsn {
        self.lsn
    }
}
