use crossbeam_utils::CachePadded;
use parking_lot::RwLock;
use smallvec::SmallVec;

use crate::table::{SwissTable, TableHash};

use super::{
    ConcurrentKeyspace, MAX_SHARD_COUNT, MIN_SHARD_COUNT, ShardReadGuard, ShardWriteGuard,
};

/// A single shard: a `SwissTable` behind a `RwLock`, padded to a full
/// 128-byte boundary so that adjacent shard locks never share a cache line.
pub(super) type Shard = CachePadded<RwLock<SwissTable>>;

/// Return type for multi-key read lock acquisition.
/// `(guards_with_shard_id, shard_plan)`
pub(crate) type MultiReadGuards<'a> = (ShardReadGuards<'a>, ShardPlan);

/// Return type for multi-key write lock acquisition.
/// `(guards_with_shard_id, shard_plan)`
pub(crate) type MultiWriteGuards<'a> = (ShardWriteGuards<'a>, ShardPlan);

pub(super) type ShardReadGuards<'a> = SmallVec<[(usize, ShardReadGuard<'a>); 16]>;
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
        let mut shard_order: SmallVec<[(ShardId, usize); 16]> = SmallVec::with_capacity(keys.len());
        for (key_index, &key) in keys.iter().enumerate() {
            let shard = keyspace.shard_id(key);
            per_key_shards.push(shard);
            shard_order.push((shard, key_index));
        }

        shard_order.sort_unstable_by_key(|&(shard, key_index)| (shard, key_index));
        let mut sorted_shards = SmallVec::new();
        let mut per_key_guard_indices = SmallVec::with_capacity(per_key_shards.len());
        per_key_guard_indices.resize(per_key_shards.len(), GuardIndex::new(0));

        let mut current_shard = None;
        let mut current_guard = GuardIndex::new(0);
        for (shard, key_index) in shard_order {
            if current_shard != Some(shard) {
                current_shard = Some(shard);
                current_guard = GuardIndex::new(sorted_shards.len());
                sorted_shards.push(shard);
            }
            per_key_guard_indices[key_index] = current_guard;
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

    #[cfg(test)]
    #[inline]
    pub(crate) fn shard_for_key(&self, key_index: usize) -> ShardId {
        self.per_key_shards[key_index]
    }

    #[inline]
    pub(crate) fn guard_index_for_key(&self, key_index: usize) -> GuardIndex {
        self.per_key_guard_indices[key_index]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PrehashedKeyPlan<'a> {
    key_index: usize,
    key_bytes: &'a [u8],
    shard_id: ShardId,
    table_hash: TableHash,
    guard_index: GuardIndex,
}

impl<'a> PrehashedKeyPlan<'a> {
    #[inline]
    pub(crate) const fn key_index(self) -> usize {
        self.key_index
    }

    #[inline]
    pub(crate) const fn key_bytes(self) -> &'a [u8] {
        self.key_bytes
    }

    #[inline]
    pub(crate) const fn shard_index(self) -> usize {
        self.shard_id.get()
    }

    #[inline]
    pub(crate) const fn table_hash(self) -> TableHash {
        self.table_hash
    }

    #[inline]
    pub(crate) const fn guard_index(self) -> GuardIndex {
        self.guard_index
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PrehashedShardPlan<'a> {
    sorted_shards: SmallVec<[ShardId; 16]>,
    entries: SmallVec<[PrehashedKeyPlan<'a>; 16]>,
}

impl<'a> PrehashedShardPlan<'a> {
    pub(super) fn new<I>(keyspace: &ConcurrentKeyspace, keys: I) -> Self
    where
        I: IntoIterator<Item = (usize, &'a [u8])>,
    {
        let mut entries: SmallVec<[PrehashedKeyPlan<'a>; 16]> = SmallVec::new();
        for (key_index, key_bytes) in keys {
            entries.push(PrehashedKeyPlan {
                key_index,
                key_bytes,
                shard_id: keyspace.shard_id(key_bytes),
                table_hash: keyspace.table_hash_key(key_bytes),
                guard_index: GuardIndex::new(0),
            });
        }

        entries.sort_unstable_by_key(|entry| (entry.shard_id, entry.key_index));

        let mut sorted_shards = SmallVec::new();
        let mut current_shard = None;
        let mut current_guard = GuardIndex::new(0);
        for entry in &mut entries {
            if current_shard != Some(entry.shard_id) {
                current_shard = Some(entry.shard_id);
                current_guard = GuardIndex::new(sorted_shards.len());
                sorted_shards.push(entry.shard_id);
            }
            entry.guard_index = current_guard;
        }

        Self {
            sorted_shards,
            entries,
        }
    }

    #[inline]
    pub(crate) fn sorted_shards(&self) -> &[ShardId] {
        &self.sorted_shards
    }

    #[inline]
    pub(crate) fn entries(&self) -> &[PrehashedKeyPlan<'a>] {
        &self.entries
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}
