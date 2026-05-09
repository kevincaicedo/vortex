//! Engine-domain operations for command handlers.
//!
//! Command modules parse RESP frames and shape replies. This module owns the
//! zero-cost mutation/read coordination over `ConcurrentKeyspace`: shard locks,
//! memory admission, TTL transitions, WATCH invalidation, eviction effects, AOF
//! LSN stamping, and table-level mutation helpers. It uses only inherent impls
//! and free functions, so there is no trait-object or boxed-operation overhead.

use core::mem::size_of;
use std::collections::HashMap;

use bytes::Bytes;
use smallvec::SmallVec;
use vortex_common::value::InlineBytes;
use vortex_common::{VortexKey, VortexValue};

use crate::EvictionConfig;
use crate::SwissTable;
use crate::effects::{AofRecord, AofRecords, MutationErrorKind};
use crate::entry::Entry;
use crate::keyspace::{
    AofLsn, ConcurrentKeyspace, EvictedKey, EvictedKeys, EvictionAdmissionError, ExpiryTransition,
    MemoryReservation, PositiveDelta, PrehashedKeyPlan, PrehashedShardPlan, ProjectedDelta,
    ShardPlan, ShardWriteGuard, ShardWriteGuards,
};
use crate::table::{
    BorrowedKey, MutationPolicy, RawValueBytes, SlotCursor, SlotMutationReport, TableHash,
};

use crate::commands::pattern::glob_match;

mod admin_ops;
mod key_ops;
mod mutation;
mod scan_ops;
mod string_ops;
mod string_tables;

#[cfg(test)]
mod tests;

pub(crate) use mutation::{
    ExpireOptions, GetExOption, MutationError, MutationOutcome, SetOptions, SetResult, TtlState,
};
