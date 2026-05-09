//! Pool-owned AOF mode coordination.
//!
//! Runtime mode transitions are intentionally cold and protected by a mutex.
//! The command hot path only needs the reactor-local writer slot and, when AOF
//! is active, one relaxed epoch/fatal check before appending an LSN record.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crossbeam_utils::CachePadded;
use vortex_engine::keyspace::ConcurrentKeyspace;
use vortex_persist::aof::{AofManifest, AofManifestState};

/// Epoch attached to a pool-wide AOF writer generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct AofEpoch(u64);

impl AofEpoch {
    #[inline]
    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

/// Pool-wide AOF lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AofCoordinatorMode {
    /// AOF recording is disabled and no reactor should own a writer.
    Off,
    /// Writers are being created for this epoch; keyspace AOF is not enabled.
    Enabling(AofEpoch),
    /// All expected writers exist and keyspace AOF LSN allocation is enabled.
    On(AofEpoch),
    /// Runtime disable is in progress for this epoch.
    Disabling(AofEpoch),
    /// A strict AOF append/fsync failure occurred; writes must stop.
    Failed,
}

#[derive(Debug)]
struct AofCoordinatorState {
    mode: AofCoordinatorMode,
    next_epoch: u64,
}

impl Default for AofCoordinatorState {
    fn default() -> Self {
        Self {
            mode: AofCoordinatorMode::Off,
            next_epoch: 1,
        }
    }
}

/// Coordinates AOF mode across all reactors in a pool.
pub(crate) struct AofCoordinator {
    reactor_count: usize,
    state: Mutex<AofCoordinatorState>,
    active_epoch: CachePadded<AtomicU64>,
    failed: CachePadded<AtomicBool>,
}

impl AofCoordinator {
    /// Create a coordinator for a reactor pool.
    pub(crate) fn new(reactor_count: usize) -> Self {
        Self {
            reactor_count: reactor_count.max(1),
            state: Mutex::new(AofCoordinatorState::default()),
            active_epoch: CachePadded::new(AtomicU64::new(0)),
            failed: CachePadded::new(AtomicBool::new(false)),
        }
    }

    /// Number of reactors governed by this coordinator.
    #[inline]
    pub(crate) fn reactor_count(&self) -> usize {
        self.reactor_count
    }

    /// Snapshot the cold lifecycle state.
    #[cfg(test)]
    pub(crate) fn mode(&self) -> AofCoordinatorMode {
        self.state.lock().expect("AOF coordinator poisoned").mode
    }

    /// Begin a startup enable transition. Callers must create every reactor
    /// writer before committing the epoch.
    pub(crate) fn begin_startup_enable(&self) -> io::Result<AofEpoch> {
        let mut state = self.state.lock().expect("AOF coordinator poisoned");
        match state.mode {
            AofCoordinatorMode::Off => {
                let epoch = AofEpoch(state.next_epoch);
                state.next_epoch = state.next_epoch.checked_add(1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "AOF epoch overflow")
                })?;
                state.mode = AofCoordinatorMode::Enabling(epoch);
                Ok(epoch)
            }
            other => Err(invalid_transition("begin startup AOF enable", other)),
        }
    }

    /// Begin a runtime enable transition. Alpha only allows this when one
    /// reactor is present; multi-reactor runtime handoff is explicitly rejected.
    pub(crate) fn begin_runtime_enable(&self) -> io::Result<AofEpoch> {
        if self.reactor_count != 1 {
            return Err(runtime_reconfigure_unsupported());
        }
        self.begin_startup_enable()
    }

    /// Commit an enable transition after all required writers exist.
    pub(crate) fn commit_enable(
        &self,
        epoch: AofEpoch,
        keyspace: &ConcurrentKeyspace,
    ) -> io::Result<()> {
        let mut state = self.state.lock().expect("AOF coordinator poisoned");
        match state.mode {
            AofCoordinatorMode::Enabling(current) if current == epoch => {
                self.failed.store(false, Ordering::Relaxed);
                self.active_epoch.store(epoch.get(), Ordering::Release);
                keyspace.enable_aof_recording();
                state.mode = AofCoordinatorMode::On(epoch);
                Ok(())
            }
            other => Err(invalid_transition("commit AOF enable", other)),
        }
    }

    /// Abort an enable transition before keyspace AOF recording is enabled.
    pub(crate) fn abort_enable(&self, epoch: AofEpoch) {
        let mut state = self.state.lock().expect("AOF coordinator poisoned");
        if matches!(state.mode, AofCoordinatorMode::Enabling(current) if current == epoch) {
            state.mode = AofCoordinatorMode::Off;
        }
    }

    /// Begin a runtime disable transition.
    pub(crate) fn begin_runtime_disable(&self, epoch: AofEpoch) -> io::Result<()> {
        if self.reactor_count != 1 {
            return Err(runtime_reconfigure_unsupported());
        }

        let mut state = self.state.lock().expect("AOF coordinator poisoned");
        match state.mode {
            AofCoordinatorMode::On(current) if current == epoch => {
                state.mode = AofCoordinatorMode::Disabling(epoch);
                Ok(())
            }
            other => Err(invalid_transition("begin AOF disable", other)),
        }
    }

    /// Commit a disable transition. Keyspace AOF LSN allocation is stopped
    /// before the caller drops its reactor-local writer.
    pub(crate) fn commit_disable(
        &self,
        epoch: AofEpoch,
        keyspace: &ConcurrentKeyspace,
    ) -> io::Result<()> {
        let mut state = self.state.lock().expect("AOF coordinator poisoned");
        match state.mode {
            AofCoordinatorMode::Disabling(current) if current == epoch => {
                keyspace.disable_aof_recording();
                self.active_epoch.store(0, Ordering::Release);
                state.mode = AofCoordinatorMode::Off;
                Ok(())
            }
            other => Err(invalid_transition("commit AOF disable", other)),
        }
    }

    /// Abort a disable transition and return to `On`.
    pub(crate) fn abort_disable(&self, epoch: AofEpoch) {
        let mut state = self.state.lock().expect("AOF coordinator poisoned");
        if matches!(state.mode, AofCoordinatorMode::Disabling(current) if current == epoch) {
            state.mode = AofCoordinatorMode::On(epoch);
        }
    }

    /// Returns true after any strict AOF failure.
    #[inline]
    pub(crate) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }

    /// Validate that a reactor-local writer belongs to the active AOF epoch.
    #[inline]
    pub(crate) fn validate_writer_epoch(&self, epoch: AofEpoch) -> io::Result<()> {
        if self.is_failed() {
            return Err(io::Error::other("AOF is in failed write-stop state"));
        }

        let active = self.active_epoch.load(Ordering::Acquire);
        if active == epoch.get() {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "AOF writer epoch {} does not match active epoch {active}",
                epoch.get()
            )))
        }
    }

    /// Enter strict failed/write-stop mode after append or fsync failure.
    pub(crate) fn mark_failed(&self, reactor_id: usize, context: &'static str, error: &io::Error) {
        let was_failed = self.failed.swap(true, Ordering::Relaxed);
        self.active_epoch.store(0, Ordering::Release);
        {
            let mut state = self.state.lock().expect("AOF coordinator poisoned");
            state.mode = AofCoordinatorMode::Failed;
        }

        if was_failed {
            tracing::debug!(
                reactor_id,
                context,
                error = %error,
                "AOF fatal state already set"
            );
        } else {
            tracing::error!(
                reactor_id,
                context,
                error = %error,
                "AOF persistence failed; rejecting future writes"
            );
        }
    }
}

impl Default for AofCoordinator {
    fn default() -> Self {
        Self::new(1)
    }
}

/// Returns the AOF path owned by one reactor.
pub(crate) fn reactor_aof_path(base: &Path, reactor_id: usize) -> PathBuf {
    if reactor_id == 0 {
        return base.to_path_buf();
    }

    let stem = base.file_stem().unwrap_or_default().to_string_lossy();
    let ext = base.extension().unwrap_or_default().to_string_lossy();
    base.with_file_name(format!("{stem}-shard{reactor_id}.{ext}"))
}

/// Return the file set that startup recovery should replay for one reactor.
pub(crate) fn reactor_aof_replay_paths(base: &Path, reactor_id: usize) -> io::Result<Vec<PathBuf>> {
    let legacy_path = reactor_aof_path(base, reactor_id);
    let Some(manifest) = AofManifest::load_for_aof(&legacy_path)? else {
        return Ok(vec![legacy_path]);
    };
    if manifest.state() == AofManifestState::Active {
        return Err(io::Error::other(format!(
            "active AOF manifest {} requires manifest-aware replay",
            manifest.path().display()
        )));
    }
    Ok(manifest.replay_paths())
}

/// Return the append target for one reactor after manifest recovery.
pub(crate) fn reactor_aof_writer_path(base: &Path, reactor_id: usize) -> io::Result<PathBuf> {
    let legacy_path = reactor_aof_path(base, reactor_id);
    let Some(manifest) = AofManifest::load_for_aof(&legacy_path)? else {
        return Ok(legacy_path);
    };
    Ok(manifest
        .active_tail_path()
        .map(Path::to_path_buf)
        .unwrap_or(legacy_path))
}

pub(crate) fn runtime_reconfigure_unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "runtime CONFIG SET appendonly yes/no is disabled in multi-reactor alpha mode; configure AOF at startup",
    )
}

fn invalid_transition(action: &'static str, mode: AofCoordinatorMode) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{action} is invalid while AOF mode is {mode:?}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_engine::keyspace::DEFAULT_SHARD_COUNT;

    #[test]
    fn coordinator_startup_enable_transitions_to_on() {
        let coordinator = AofCoordinator::new(2);
        let keyspace = ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT);

        let epoch = coordinator.begin_startup_enable().unwrap();
        assert_eq!(coordinator.mode(), AofCoordinatorMode::Enabling(epoch));

        coordinator.commit_enable(epoch, &keyspace).unwrap();
        assert_eq!(coordinator.mode(), AofCoordinatorMode::On(epoch));
        assert!(coordinator.validate_writer_epoch(epoch).is_ok());
    }

    #[test]
    fn coordinator_rejects_multi_reactor_runtime_enable() {
        let coordinator = AofCoordinator::new(2);
        let error = coordinator.begin_runtime_enable().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert_eq!(coordinator.mode(), AofCoordinatorMode::Off);
    }

    #[test]
    fn coordinator_failed_state_rejects_writer_epoch() {
        let coordinator = AofCoordinator::new(1);
        let keyspace = ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT);
        let epoch = coordinator.begin_startup_enable().unwrap();
        coordinator.commit_enable(epoch, &keyspace).unwrap();

        coordinator.mark_failed(0, "test", &io::Error::other("forced"));
        assert_eq!(coordinator.mode(), AofCoordinatorMode::Failed);
        assert!(coordinator.validate_writer_epoch(epoch).is_err());
    }
}
