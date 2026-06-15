use std::sync::atomic::Ordering;

use vortex_common::VortexKey;

use super::ConcurrentKeyspace;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ExpiryTransition {
    had_ttl: bool,
    has_ttl_after: bool,
}

impl ExpiryTransition {
    #[inline]
    pub(crate) const fn new(had_ttl: bool, has_ttl_after: bool) -> Self {
        Self {
            had_ttl,
            has_ttl_after,
        }
    }

    #[inline]
    pub(crate) const fn remove(had_ttl: bool) -> Self {
        Self::new(had_ttl, false)
    }

    #[inline]
    pub(crate) const fn ttl_removed() -> Self {
        Self::new(true, false)
    }

    #[inline]
    pub(crate) const fn is_noop(self) -> bool {
        self.had_ttl == self.has_ttl_after
    }

    #[inline]
    const fn before_after(self) -> (bool, bool) {
        (self.had_ttl, self.has_ttl_after)
    }
}

impl ConcurrentKeyspace {
    #[inline(always)]
    pub(crate) fn apply_expiry_transition(&self, shard_idx: usize, transition: ExpiryTransition) {
        let (had_ttl, has_ttl) = transition.before_after();
        self.update_expiry_count(shard_idx, had_ttl, has_ttl);
    }

    #[inline(always)]
    fn update_expiry_count(&self, shard_idx: usize, had_ttl: bool, has_ttl: bool) {
        debug_assert!(shard_idx < self.expiry_key_count.len());
        match (had_ttl, has_ttl) {
            (false, true) => {
                self.expiry_key_count[shard_idx].fetch_add(1, Ordering::Relaxed);
                self.expiry_key_total.fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                self.expiry_key_count[shard_idx].fetch_sub(1, Ordering::Relaxed);
                self.expiry_key_total.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    #[inline]
    pub(crate) fn total_expiry_keys(&self) -> usize {
        self.expiry_key_total.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn approx_expiring_keys(&self) -> usize {
        self.expiry_key_total.load(Ordering::Relaxed)
    }

    /// Cheap global check used by event loops to skip active-expiry work for
    /// pure no-TTL workloads.
    #[inline]
    pub fn has_expiring_keys(&self) -> bool {
        self.approx_expiring_keys() != 0
    }

    #[inline]
    pub(super) fn shard_has_expiring_keys(&self, shard_idx: usize) -> bool {
        debug_assert!(shard_idx < self.expiry_key_count.len());
        self.expiry_key_count[shard_idx].load(Ordering::Relaxed) != 0
    }

    /// Run one round of active expiry on a single shard by index.
    ///
    /// Acquires a write lock on the target shard, scans up to `max_effort`
    /// occupied slots starting from `start_slot`, and removes entries whose
    /// TTL deadline has passed. Returns `(expired_count, sampled_count)`.
    ///
    /// The caller should rotate `start_slot` across calls to ensure full
    /// coverage over time (e.g. increment by `max_effort` each tick).
    ///
    /// # Mechanical sympathy
    ///
    /// - One write lock per call, not held across shards.
    /// - Sequential slot access is prefetcher-friendly.
    /// - Zero allocation: `delete_slot()` removes by slot index directly,
    ///   avoiding the key clone + hash + re-probe overhead of `remove()`.
    pub fn run_active_expiry_on_shard(
        &self,
        shard_idx: usize,
        start_slot: usize,
        max_effort: usize,
        now_nanos: u64,
    ) -> (usize, usize) {
        debug_assert!(shard_idx < self.shards.len());
        if !self.shard_has_expiring_keys(shard_idx) {
            return (0, 0);
        }
        #[cfg(feature = "lock-profile")]
        let _lock_profile =
            self.enter_lock_profile_scope(crate::keyspace::LockProfileClass::ExpiryCleanup);
        let mut guard = self.write_shard_by_index(shard_idx);
        let total_slots = guard.total_slots();
        if total_slots == 0 {
            return (0, 0);
        }

        let mut expired = 0usize;
        let mut sampled = 0usize;

        for i in 0..max_effort {
            let slot = (start_slot + i) % total_slots;
            let deadline = guard.slot_entry_ttl(slot);
            // deadline == 0 means empty/deleted or no TTL; skip.
            if deadline == 0 {
                continue;
            }
            sampled += 1;
            if deadline <= now_nanos {
                let watched_key = if self.watch_tracking_active() {
                    guard
                        .slot_key_value(slot)
                        .map(|(key, _)| VortexKey::from_bytes(key))
                } else {
                    None
                };
                guard.delete_slot(slot);
                self.apply_expiry_transition(shard_idx, ExpiryTransition::ttl_removed());
                if let Some(key) = watched_key {
                    self.bump_watch_key(&key);
                }
                expired += 1;
            }
        }

        (expired, sampled)
    }
}
