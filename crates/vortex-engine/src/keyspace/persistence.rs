use std::sync::atomic::Ordering;

use crate::entry::EntryLsn;

use super::{ConcurrentKeyspace, MutationFeatures};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

impl Lsn {
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AofLsn(u64);

impl AofLsn {
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[inline]
    pub fn try_from_raw(lsn: u64) -> Result<Self, LsnOverflow> {
        entry_lsn_from_raw(lsn).map(|entry_lsn| Self(entry_lsn.get()))
    }

    #[inline]
    pub(crate) const fn from_entry_lsn(lsn: EntryLsn) -> Self {
        Self(lsn.get())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LsnOverflow {
    pub attempted: u64,
    pub max: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LsnRestoreError {
    pub max_replayed_lsn: u64,
}

#[inline]
fn entry_lsn_from_raw(lsn: u64) -> Result<EntryLsn, LsnOverflow> {
    EntryLsn::from_raw(lsn).ok_or(LsnOverflow {
        attempted: lsn,
        max: EntryLsn::MAX,
    })
}

pub struct ReplayModeGuard<'a> {
    pub(super) keyspace: &'a ConcurrentKeyspace,
}

impl Drop for ReplayModeGuard<'_> {
    fn drop(&mut self) {
        self.keyspace.replay_depth.fetch_sub(1, Ordering::Relaxed);
    }
}

impl ConcurrentKeyspace {
    #[inline]
    pub fn enter_replay_mode(&self) -> ReplayModeGuard<'_> {
        self.replay_depth.fetch_add(1, Ordering::Relaxed);
        ReplayModeGuard { keyspace: self }
    }

    #[inline]
    pub(crate) fn replay_mode_active(&self) -> bool {
        self.replay_depth.load(Ordering::Relaxed) != 0
    }

    /// Allocate the next LSN. **Must be called while holding a shard write lock**
    /// to guarantee causal ordering: if Op1 -> Op2 on the same key, then
    /// LSN1 < LSN2.
    ///
    /// `Relaxed` ordering is correct because the shard `RwLock` provides the
    /// necessary acquire/release synchronization. The atomic itself only needs
    /// monotonicity, which `fetch_add` guarantees on all architectures.
    #[inline(always)]
    pub(crate) fn next_lsn(&self) -> Result<EntryLsn, LsnOverflow> {
        match self
            .global_lsn
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current <= EntryLsn::MAX).then_some(current + 1)
            }) {
            Ok(raw) => entry_lsn_from_raw(raw),
            Err(attempted) => Err(LsnOverflow {
                attempted,
                max: EntryLsn::MAX,
            }),
        }
    }

    #[inline(always)]
    fn next_watch_visible_lsn(&self) -> Result<EntryLsn, LsnOverflow> {
        loop {
            let lsn = self.next_lsn()?;
            if lsn.get() != 0 {
                return Ok(lsn);
            }
        }
    }

    #[inline]
    pub fn enable_aof_recording(&self) {
        if self.aof_recording_refs.fetch_add(1, Ordering::Release) == 0 {
            self.enable_mutation_feature(MutationFeatures::AOF);
        }
    }

    #[inline]
    pub fn disable_aof_recording(&self) {
        if let Ok(previous) =
            self.aof_recording_refs
                .fetch_update(Ordering::Release, Ordering::Relaxed, |refs| {
                    (refs != 0).then_some(refs - 1)
                })
        {
            if previous == 1 {
                self.disable_mutation_feature(MutationFeatures::AOF);
            }
        }
    }

    #[inline(always)]
    pub(crate) fn aof_recording_enabled(&self) -> bool {
        self.mutation_feature_active(MutationFeatures::AOF)
    }

    #[inline(always)]
    pub(crate) fn next_aof_lsn(&self) -> Result<Option<AofLsn>, LsnOverflow> {
        if !self.aof_recording_enabled() {
            return Ok(None);
        }
        Ok(Some(AofLsn::from_entry_lsn(self.next_lsn()?)))
    }

    #[inline(always)]
    pub(crate) fn allocate_observed_mutation_lsn_with_features(
        &self,
        features: MutationFeatures,
    ) -> Result<(Option<EntryLsn>, Option<AofLsn>), LsnOverflow> {
        if !features.entry_lsn_observed() {
            return Ok((None, None));
        }

        let lsn = if features.watch() {
            self.next_watch_visible_lsn()?
        } else {
            self.next_lsn()?
        };
        Ok((
            Some(lsn),
            if features.aof() {
                Some(AofLsn::from_entry_lsn(lsn))
            } else {
                None
            },
        ))
    }

    /// Read the current LSN value (the next LSN to be assigned).
    ///
    /// Useful for snapshot points (BGREWRITEAOF) and restoring LSN state
    /// after AOF replay.
    #[inline]
    pub fn current_lsn(&self) -> u64 {
        self.global_lsn.load(Ordering::Relaxed)
    }

    /// Restore the global LSN after AOF replay by advancing it to one greater
    /// than the highest persisted LSN.
    ///
    /// The counter is only moved forward; stale or duplicate restore attempts
    /// cannot move it backward.
    ///
    /// # Safety
    ///
    /// Must only be called during single-threaded replay/initialization
    /// before reactors are spawned, or while all reactors are quiesced.
    pub unsafe fn restore_lsn_after_replay(
        &self,
        max_replayed_lsn: Option<AofLsn>,
    ) -> Result<(), LsnRestoreError> {
        let Some(max_replayed_lsn) = max_replayed_lsn else {
            return Ok(());
        };

        let next_lsn = max_replayed_lsn
            .get()
            .checked_add(1)
            .ok_or(LsnRestoreError {
                max_replayed_lsn: max_replayed_lsn.get(),
            })?;
        self.global_lsn.fetch_max(next_lsn, Ordering::Relaxed);
        Ok(())
    }
}
