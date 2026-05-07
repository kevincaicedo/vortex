use crossbeam_utils::CachePadded;
use parking_lot::{RwLock, RwLockReadGuard};
use smallvec::SmallVec;

use crate::table::SwissTable;

use super::{ConcurrentKeyspace, MAX_SHARD_COUNT, MIN_SHARD_COUNT, ShardWriteGuard};

/// A single shard: a `SwissTable` behind a `RwLock`, padded to a full
/// 128-byte boundary so that adjacent shard locks never share a cache line.
pub(super) type Shard = CachePadded<RwLock<SwissTable>>;

/// Return type for multi-key read lock acquisition.
/// `(guards_with_shard_id, shard_plan)`
pub(crate) type MultiReadGuards<'a> = (ShardReadGuards<'a>, ShardPlan);

/// Return type for multi-key write lock acquisition.
/// `(guards_with_shard_id, shard_plan)`
pub(crate) type MultiWriteGuards<'a> = (ShardWriteGuards<'a>, ShardPlan);

pub(super) type ShardReadGuards<'a> = SmallVec<[(usize, RwLockReadGuard<'a, SwissTable>); 16]>;
pub(crate) type ShardWriteGuards<'a> = SmallVec<[(usize, ShardWriteGuard<'a>); 16]>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShardCountError {
    attempted: usize,
}

impl ShardCountError {
    #[inline]
    pub const fn attempted(self) -> usize {
        self.attempted
    }
}

impl std::fmt::Display for ShardCountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "num_shards must be a power of two in [{MIN_SHARD_COUNT}, {MAX_SHARD_COUNT}], got {}",
            self.attempted
        )
    }
}

impl std::error::Error for ShardCountError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardCount(usize);

impl ShardCount {
    #[inline]
    pub const fn get(self) -> usize {
        self.0
    }

    #[inline]
    pub const fn mask(self) -> u64 {
        (self.0 - 1) as u64
    }

    #[inline]
    pub fn try_new(count: usize) -> Result<Self, ShardCountError> {
        if (MIN_SHARD_COUNT..=MAX_SHARD_COUNT).contains(&count) && count.is_power_of_two() {
            Ok(Self(count))
        } else {
            Err(ShardCountError { attempted: count })
        }
    }

    #[inline]
    pub(crate) const fn from_validated(count: usize) -> Self {
        Self(count)
    }

    #[inline]
    pub(crate) const fn contains(self, shard: ShardId) -> bool {
        shard.get() < self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ShardId(usize);

impl ShardId {
    #[inline]
    pub(crate) const fn from_masked_index(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub(crate) fn try_new(index: usize, shard_count: ShardCount) -> Option<Self> {
        let shard = Self(index);
        shard_count.contains(shard).then_some(shard)
    }

    #[inline]
    pub(crate) const fn get(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct GuardIndex(usize);

impl GuardIndex {
    #[inline]
    pub(crate) const fn new(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub(crate) const fn get(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ShardPlan {
    pub(super) sorted_shards: SmallVec<[ShardId; 16]>,
    pub(super) per_key_shards: SmallVec<[ShardId; 16]>,
    pub(super) per_key_guard_indices: SmallVec<[GuardIndex; 16]>,
}

impl ShardPlan {
    pub(super) fn new(keyspace: &ConcurrentKeyspace, keys: &[&[u8]]) -> Self {
        let mut per_key_shards = SmallVec::with_capacity(keys.len());
        for &key in keys {
            per_key_shards.push(keyspace.shard_id(key));
        }

        let mut sorted_shards = per_key_shards.clone();
        sorted_shards.sort_unstable();
        sorted_shards.dedup();

        let mut per_key_guard_indices = SmallVec::with_capacity(per_key_shards.len());
        for shard in &per_key_shards {
            let guard_index = sorted_shards
                .binary_search(shard)
                .expect("planned shard must be present in sorted shard set");
            per_key_guard_indices.push(GuardIndex::new(guard_index));
        }

        Self {
            sorted_shards,
            per_key_shards,
            per_key_guard_indices,
        }
    }

    #[inline]
    pub(crate) fn sorted_shards(&self) -> &[ShardId] {
        &self.sorted_shards
    }

    #[inline]
    pub(crate) fn shard_for_key(&self, key_index: usize) -> ShardId {
        self.per_key_shards[key_index]
    }

    #[inline]
    pub(crate) fn guard_index_for_key(&self, key_index: usize) -> GuardIndex {
        self.per_key_guard_indices[key_index]
    }
}
