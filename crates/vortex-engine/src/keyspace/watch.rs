use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crossbeam_utils::CachePadded;
use parking_lot::RwLock;
use vortex_common::VortexKey;

use super::{ABSENT_WATCH_SHARD_COUNT, ConcurrentKeyspace};

#[derive(Debug)]
/// Cold WATCH metadata for keys that were absent when WATCH ran.
pub(super) struct AbsentWatchSlot {
    version: u64,
    refs: usize,
}

pub(super) type AbsentWatchShard = CachePadded<RwLock<HashMap<VortexKey, AbsentWatchSlot>>>;

#[derive(Debug)]
struct WatchKeyState {
    key: VortexKey,
    shard_index: usize,
    table_hash: u64,
    version: u64,
    present: bool,
}

impl WatchKeyState {
    #[inline]
    pub fn key(&self) -> &VortexKey {
        &self.key
    }
}

#[derive(Debug)]
pub struct WatchRegistration {
    state: WatchKeyState,
}

impl WatchRegistration {
    #[inline]
    pub fn key(&self) -> &VortexKey {
        self.state.key()
    }

    #[inline]
    fn state(&self) -> &WatchKeyState {
        &self.state
    }
}

pub(super) fn make_absent_watch_shards() -> Box<[AbsentWatchShard]> {
    (0..ABSENT_WATCH_SHARD_COUNT)
        .map(|_| CachePadded::new(RwLock::new(HashMap::new())))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

impl ConcurrentKeyspace {
    #[inline]
    pub fn current_watch_epoch(&self) -> u64 {
        self.watch_epoch.load(Ordering::Acquire)
    }

    pub fn watch_key(&self, key: VortexKey) -> WatchRegistration {
        // Own an active WATCH reference before installing an absent-key slot.
        // `mutation_features` derives the WATCH bit from this counter, so a
        // racing final UNWATCH cannot hide an in-progress registration.
        self.watch_active.fetch_add(1, Ordering::AcqRel);
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);

        let (version, present) = match self
            .read_shard_by_index(shard_index)
            .get_lsn_version_prehashed(key_bytes, table_hash)
        {
            Some(version) => (version, true),
            None => {
                let registered_version = self.register_absent_watch_key(&key, table_hash);
                match self
                    .read_shard_by_index(shard_index)
                    .get_lsn_version_prehashed(key_bytes, table_hash)
                {
                    Some(version) => {
                        self.release_absent_watch_key(key_bytes, table_hash);
                        (version, true)
                    }
                    None => (registered_version, false),
                }
            }
        };

        WatchRegistration {
            state: WatchKeyState {
                key,
                shard_index,
                table_hash,
                version,
                present,
            },
        }
    }

    #[inline]
    fn release_watch_registration(&self, watched: WatchRegistration) {
        let WatchRegistration {
            state:
                WatchKeyState {
                    key,
                    table_hash,
                    present,
                    ..
                },
        } = watched;

        if !present {
            self.release_absent_watch_key(key.as_bytes(), table_hash);
        }

        self.watch_active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |refs| {
                (refs != 0).then_some(refs - 1)
            })
            .expect("WATCH registration release without active registration");
    }

    pub fn unwatch_keys<I>(&self, keys: I)
    where
        I: IntoIterator<Item = WatchRegistration>,
    {
        for watched in keys {
            self.release_watch_registration(watched);
        }
    }

    pub fn watched_keys_changed(&self, epoch: u64, keys: &[WatchRegistration]) -> bool {
        if self.current_watch_epoch() != epoch {
            return true;
        }

        for watched in keys {
            let watched = watched.state();
            let key_bytes = watched.key.as_bytes();
            let guard = self.read_shard_by_index(watched.shard_index);
            let current_lsn = guard.get_lsn_version_prehashed(key_bytes, watched.table_hash);
            drop(guard);

            if watched.present {
                if current_lsn != Some(watched.version) {
                    return true;
                }
                continue;
            }

            if current_lsn.is_some() {
                return true;
            }

            if self.absent_watch_version(key_bytes, watched.table_hash) != Some(watched.version) {
                return true;
            }
        }

        false
    }

    #[inline]
    fn absent_watch_shard_index(&self, table_hash: u64) -> usize {
        (table_hash as usize) & (ABSENT_WATCH_SHARD_COUNT - 1)
    }

    #[inline]
    pub(crate) fn watch_tracking_active(&self) -> bool {
        self.watch_active.load(Ordering::Acquire) != 0
    }

    #[inline]
    pub(crate) fn bump_watch_key(&self, key: &VortexKey) {
        if !self.watch_tracking_active() {
            return;
        }
        self.bump_watch_key_known_active(key.as_bytes(), self.table_hash_key(key.as_bytes()));
    }

    #[inline]
    pub(crate) fn bump_watch_key_known_active(&self, key_bytes: &[u8], table_hash: u64) {
        if self.absent_watch_active.load(Ordering::Acquire) == 0 {
            return;
        }

        let shard_idx = self.absent_watch_shard_index(table_hash);
        let mut guard = self.absent_watch_shards[shard_idx].write();
        if let Some(slot) = guard.get_mut(key_bytes) {
            slot.version = slot.version.wrapping_add(1).max(1);
        }
    }

    #[inline]
    pub(crate) fn bump_watch_key_bytes(&self, key_bytes: &[u8]) {
        if !self.watch_tracking_active() {
            return;
        }
        self.bump_watch_key_known_active(key_bytes, self.table_hash_key(key_bytes));
    }

    #[inline]
    pub(crate) fn bump_all_watches(&self) {
        if self.watch_active.load(Ordering::Acquire) != 0 {
            self.watch_epoch.fetch_add(1, Ordering::Release);
        }
    }

    #[inline]
    fn absent_watch_version(&self, key_bytes: &[u8], table_hash: u64) -> Option<u64> {
        let shard_idx = self.absent_watch_shard_index(table_hash);
        let guard = self.absent_watch_shards[shard_idx].read();
        guard.get(key_bytes).map(|slot| slot.version)
    }

    #[inline]
    fn register_absent_watch_key(&self, key: &VortexKey, table_hash: u64) -> u64 {
        let shard_idx = self.absent_watch_shard_index(table_hash);
        let owned_key = key.clone();
        let mut guard = self.absent_watch_shards[shard_idx].write();
        if !guard.contains_key(&owned_key) {
            guard.reserve(1);
        }
        self.absent_watch_active.fetch_add(1, Ordering::Release);
        let slot = guard.entry(owned_key).or_insert(AbsentWatchSlot {
            version: 0,
            refs: 0,
        });
        slot.refs += 1;
        slot.version
    }

    #[inline]
    fn release_absent_watch_key(&self, key_bytes: &[u8], table_hash: u64) {
        let shard_idx = self.absent_watch_shard_index(table_hash);
        let mut guard = self.absent_watch_shards[shard_idx].write();
        if let Some(slot) = guard.get_mut(key_bytes) {
            if slot.refs > 1 {
                slot.refs -= 1;
            } else {
                guard.remove(key_bytes);
            }
            self.absent_watch_active.fetch_sub(1, Ordering::Release);
        }
    }
}
