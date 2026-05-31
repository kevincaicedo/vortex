//! AOF rewrite (compaction).
//!
//! Rewrites the AOF by dumping the current keyspace state as a sequence of SET
//! commands and publishing the resulting file set through a manifest.
//!
//! ## Rewrite Strategy
//!
//! 1. Flush/sync the active writer and record the old tail boundary.
//! 2. Create a temporary base snapshot file and write all live keys.
//! 3. Sync and rename the base snapshot, then sync the parent directory.
//! 4. Copy the old AOF bytes after the recorded boundary into a new v2 tail.
//! 5. Publish a preparing manifest that still recovers from the old active AOF.
//! 6. Epoch-check and swap the writer to the new tail file.
//! 7. Publish an active manifest naming the base snapshot and new tail.
//! 8. Best-effort remove the old active AOF after the active manifest is durable.
//!
//! The base snapshot uses the v1-style format (no LSN prefix) because it is a
//! point-in-time image. The tail uses the v2 LSN-prefixed format. During this
//! alpha implementation, snapshot creation holds shard read locks while it
//! streams records to disk. That is deliberately conservative: it avoids a
//! fuzzy snapshot plus duplicated non-idempotent tail commands. `BGREWRITEAOF`
//! remains disabled in the live server until a non-blocking epoch snapshot is
//! implemented and measured.

use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use vortex_common::{
    Timestamp, VortexValue, current_unix_time_nanos, deadline_nanos_to_absolute_unix_nanos,
};
use vortex_engine::ConcurrentKeyspace;

use super::error::{AofErrorKind, aof_error, aof_io_error};
use super::format::{AofHeader, AofReactorId, AofWriterMode};
use super::writer::AofFileWriter;

/// Buffer size for the rewrite file (128 KB — larger since we write a batch).
const REWRITE_BUF_SIZE: usize = 128 * 1024;
const MANIFEST_MAGIC: &str = "VXAOFMANIFEST";
const MANIFEST_VERSION: u32 = 1;

/// Durable state described by an AOF manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AofManifestState {
    /// Recovery must ignore the candidate base/tail and use the legacy file.
    Preparing,
    /// Recovery must use the manifest base/tail file set.
    Active,
}

impl AofManifestState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Active => "active",
        }
    }

    fn parse(value: &str) -> io::Result<Self> {
        match value {
            "preparing" => Ok(Self::Preparing),
            "active" => Ok(Self::Active),
            other => Err(aof_error(
                AofErrorKind::UnsupportedFormat,
                format!("unsupported AOF manifest state {other:?}"),
            )),
        }
    }
}

/// A crash-recovery manifest for rewritten AOF file sets.
#[derive(Clone, Debug)]
pub struct AofManifest {
    path: PathBuf,
    state: AofManifestState,
    epoch: u64,
    base_files: Vec<PathBuf>,
    tail_files: Vec<PathBuf>,
    legacy_files: Vec<PathBuf>,
}

impl AofManifest {
    /// Return the manifest path associated with one legacy AOF path.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` when `aof_path` has no file name.
    pub fn path_for_aof(aof_path: &Path) -> io::Result<PathBuf> {
        let file_name = aof_path.file_name().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("AOF path has no file name: {}", aof_path.display()),
            )
        })?;
        let mut manifest_name = file_name.to_os_string();
        manifest_name.push(".manifest");
        Ok(aof_path.with_file_name(manifest_name))
    }

    /// Load the manifest associated with one legacy AOF path.
    ///
    /// Returns `Ok(None)` when the sidecar manifest does not exist.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` when the sidecar path cannot be derived, manifest
    /// metadata cannot be checked, or the manifest exists but fails validation.
    pub fn load_for_aof(aof_path: &Path) -> io::Result<Option<Self>> {
        let manifest_path = Self::path_for_aof(aof_path)?;
        if !manifest_path.try_exists()? {
            return Ok(None);
        }
        Self::load(&manifest_path).map(Some)
    }

    /// Load a manifest by exact path.
    ///
    /// The loader accepts only manifest-local file names for `base`, `tail`,
    /// and `legacy` entries. Absolute paths, path separators, traversal, and
    /// duplicate scalar fields fail closed as unsupported format errors.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` with [`AofErrorKind::UnsupportedFormat`] for
    /// malformed or unsupported manifest content, and propagates filesystem
    /// read errors with AOF context.
    pub fn load(path: &Path) -> io::Result<Self> {
        let data = fs::read_to_string(path).map_err(|error| {
            aof_io_error(
                AofErrorKind::UnsupportedFormat,
                "failed to read AOF manifest",
                error,
            )
        })?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let mut lines = data.lines();
        let header = lines
            .next()
            .ok_or_else(|| aof_error(AofErrorKind::UnsupportedFormat, "empty AOF manifest"))?;
        let mut header_parts = header.split_whitespace();
        let magic = header_parts.next();
        let version = header_parts.next();
        if magic != Some(MANIFEST_MAGIC) || version != Some("1") {
            return Err(aof_error(
                AofErrorKind::UnsupportedFormat,
                "unsupported AOF manifest header",
            ));
        }

        let mut state = None;
        let mut epoch = None;
        let mut base_files = Vec::new();
        let mut tail_files = Vec::new();
        let mut legacy_files = Vec::new();
        for line in lines {
            let Some((key, value)) = line.split_once(' ') else {
                return Err(aof_error(
                    AofErrorKind::UnsupportedFormat,
                    format!("malformed AOF manifest line {line:?}"),
                ));
            };
            match key {
                "state" => {
                    if state.is_some() {
                        return Err(aof_error(
                            AofErrorKind::UnsupportedFormat,
                            "duplicate AOF manifest state",
                        ));
                    }
                    state = Some(AofManifestState::parse(value)?);
                }
                "epoch" => {
                    if epoch.is_some() {
                        return Err(aof_error(
                            AofErrorKind::UnsupportedFormat,
                            "duplicate AOF manifest epoch",
                        ));
                    }
                    epoch = Some(value.parse::<u64>().map_err(|error| {
                        aof_error(
                            AofErrorKind::UnsupportedFormat,
                            format!("invalid AOF manifest epoch {value:?}: {error}"),
                        )
                    })?);
                }
                "base" => base_files.push(manifest_entry_path(parent, key, value)?),
                "tail" => tail_files.push(manifest_entry_path(parent, key, value)?),
                "legacy" => legacy_files.push(manifest_entry_path(parent, key, value)?),
                _ => {
                    return Err(aof_error(
                        AofErrorKind::UnsupportedFormat,
                        format!("unknown AOF manifest key {key:?}"),
                    ));
                }
            }
        }

        let state = state.ok_or_else(|| {
            aof_error(
                AofErrorKind::UnsupportedFormat,
                "AOF manifest missing state",
            )
        })?;
        let epoch = epoch.ok_or_else(|| {
            aof_error(
                AofErrorKind::UnsupportedFormat,
                "AOF manifest missing epoch",
            )
        })?;
        if state == AofManifestState::Active && (base_files.is_empty() || tail_files.is_empty()) {
            return Err(aof_error(
                AofErrorKind::UnsupportedFormat,
                "active AOF manifest must name base and tail files",
            ));
        }
        if state == AofManifestState::Preparing && legacy_files.is_empty() {
            return Err(aof_error(
                AofErrorKind::UnsupportedFormat,
                "preparing AOF manifest must name a legacy file",
            ));
        }

        Ok(Self {
            path: path.to_path_buf(),
            state,
            epoch,
            base_files,
            tail_files,
            legacy_files,
        })
    }

    /// Return paths that recovery should replay for this manifest state.
    pub fn replay_paths(&self) -> Vec<PathBuf> {
        match self.state {
            AofManifestState::Preparing => self.legacy_files.clone(),
            AofManifestState::Active => self
                .base_files
                .iter()
                .chain(self.tail_files.iter())
                .cloned()
                .collect(),
        }
    }

    /// Return base snapshot files named by the manifest.
    pub fn base_files(&self) -> &[PathBuf] {
        &self.base_files
    }

    /// Return tail journal files named by the manifest.
    pub fn tail_files(&self) -> &[PathBuf] {
        &self.tail_files
    }

    /// Return legacy fallback files named by the manifest.
    pub fn legacy_files(&self) -> &[PathBuf] {
        &self.legacy_files
    }

    /// Return the active writer tail path for an active manifest.
    pub fn active_tail_path(&self) -> Option<&Path> {
        (self.state == AofManifestState::Active)
            .then(|| self.tail_files.last().map(PathBuf::as_path))
            .flatten()
    }

    /// Return the manifest state.
    pub const fn state(&self) -> AofManifestState {
        self.state
    }

    /// Return the rewrite epoch recorded in the manifest.
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Return the manifest file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn write_atomic_inner(
        path: &Path,
        state: AofManifestState,
        epoch: u64,
        base_files: &[PathBuf],
        tail_files: &[PathBuf],
        legacy_files: &[PathBuf],
        fault: Option<AofRewriteFaultPoint>,
    ) -> io::Result<()> {
        let tmp_path = tmp_path_for(path);
        let mut file = File::create(&tmp_path).map_err(|error| {
            aof_io_error(
                AofErrorKind::Append,
                "failed to create AOF manifest temp file",
                error,
            )
        })?;
        writeln!(file, "{MANIFEST_MAGIC} {MANIFEST_VERSION}")?;
        writeln!(file, "state {}", state.as_str())?;
        writeln!(file, "epoch {epoch}")?;
        for path in base_files {
            writeln!(file, "base {}", manifest_file_name(path)?)?;
        }
        for path in tail_files {
            writeln!(file, "tail {}", manifest_file_name(path)?)?;
        }
        for path in legacy_files {
            writeln!(file, "legacy {}", manifest_file_name(path)?)?;
        }
        file.sync_all().map_err(|error| {
            aof_io_error(
                AofErrorKind::Fsync,
                "failed to sync AOF manifest temp file",
                error,
            )
        })?;
        drop(file);
        maybe_fault(fault, AofRewriteFaultPoint::ManifestRenameFailure)?;
        fs::rename(&tmp_path, path).map_err(|error| {
            aof_io_error(AofErrorKind::Append, "failed to rename AOF manifest", error)
        })?;
        sync_parent_dir_with_fault(path, fault)
    }
}

/// Result of a manifest-backed rewrite.
#[derive(Clone, Debug)]
pub struct AofRewriteOutcome {
    /// Durable sidecar manifest path.
    pub manifest_path: PathBuf,
    /// Base snapshot file written by the rewrite.
    pub base_path: PathBuf,
    /// New active tail file used after writer swap.
    pub tail_path: PathBuf,
    /// Previous active AOF path.
    pub old_path: PathBuf,
    /// Number of keys serialized into the base snapshot.
    pub keys_written: u64,
    /// Number of bytes copied from the old active AOF into the new tail.
    pub tail_bytes_copied: u64,
    /// Rewrite epoch that was validated against the writer epoch.
    pub epoch: u64,
    /// Whether best-effort cleanup removed the previous active AOF.
    pub old_file_removed: bool,
}

#[cfg(any(test, feature = "test-faults"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AofRewriteFaultPoint {
    AfterBaseTempWrite,
    AfterBaseTempSync,
    AfterBaseRename,
    AfterPreparingManifest,
    ManifestRenameFailure,
    DirectoryFsyncFailure,
    WriterSwapFailure,
    AfterWriterSwap,
    AfterActiveManifest,
    BeforeOldCleanup,
}

#[cfg(not(any(test, feature = "test-faults")))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AofRewriteFaultPoint {
    AfterBaseTempWrite,
    AfterBaseTempSync,
    AfterBaseRename,
    AfterPreparingManifest,
    ManifestRenameFailure,
    DirectoryFsyncFailure,
    WriterSwapFailure,
    AfterWriterSwap,
    AfterActiveManifest,
    BeforeOldCleanup,
}

#[cfg(any(test, feature = "test-faults"))]
impl AofRewriteFaultPoint {
    /// Stable lowercase name for crash-test artifact notes.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AfterBaseTempWrite => "after-base-temp-write",
            Self::AfterBaseTempSync => "after-base-temp-sync",
            Self::AfterBaseRename => "after-base-rename",
            Self::AfterPreparingManifest => "after-preparing-manifest",
            Self::ManifestRenameFailure => "manifest-rename-failure",
            Self::DirectoryFsyncFailure => "directory-fsync-failure",
            Self::WriterSwapFailure => "writer-swap-failure",
            Self::AfterWriterSwap => "after-writer-swap",
            Self::AfterActiveManifest => "after-active-manifest",
            Self::BeforeOldCleanup => "before-old-cleanup",
        }
    }
}

fn validate_rewrite_epoch(expected_epoch: u64, writer_epoch: u64) -> io::Result<()> {
    if expected_epoch == writer_epoch {
        return Ok(());
    }

    Err(io::Error::other(format!(
        "AOF rewrite epoch {expected_epoch} does not match writer epoch {writer_epoch}"
    )))
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    path.with_file_name(format!("{file_name}.tmp"))
}

fn is_manifest_file_name(value: &str) -> bool {
    if value.is_empty()
        || value.contains('\n')
        || value.contains('\r')
        || value.contains('/')
        || value.contains('\\')
    {
        return false;
    }

    let mut components = Path::new(value).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn manifest_entry_path(parent: &Path, key: &str, value: &str) -> io::Result<PathBuf> {
    if is_manifest_file_name(value) {
        return Ok(parent.join(value));
    }

    Err(aof_error(
        AofErrorKind::UnsupportedFormat,
        format!("AOF manifest {key} entry must be a file name, got {value:?}"),
    ))
}

fn manifest_file_name(path: &Path) -> io::Result<String> {
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("AOF manifest entry has no file name: {}", path.display()),
        )
    })?;
    let name = file_name.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("AOF manifest entry is not valid UTF-8: {}", path.display()),
        )
    })?;
    if !is_manifest_file_name(name) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "AOF manifest entry must be a single file name: {}",
                path.display()
            ),
        ));
    }
    Ok(name.to_owned())
}

fn rewrite_component_path(aof_path: &Path, component: &str, epoch: u64) -> io::Result<PathBuf> {
    let file_name = aof_path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("AOF path has no file name: {}", aof_path.display()),
        )
    })?;
    let name = file_name.to_string_lossy();
    Ok(aof_path.with_file_name(format!("{name}.{component}-{epoch}.aof")))
}

fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|error| {
            aof_io_error(
                AofErrorKind::Fsync,
                format!("failed to sync AOF parent directory {}", parent.display()),
                error,
            )
        })
}

fn sync_parent_dir_with_fault(path: &Path, fault: Option<AofRewriteFaultPoint>) -> io::Result<()> {
    maybe_fault(fault, AofRewriteFaultPoint::DirectoryFsyncFailure)?;
    sync_parent_dir(path)
}

#[cfg(any(test, feature = "test-faults"))]
fn maybe_fault(fault: Option<AofRewriteFaultPoint>, point: AofRewriteFaultPoint) -> io::Result<()> {
    if fault == Some(point) {
        return Err(io::Error::other(format!(
            "injected AOF rewrite fault at {point:?}"
        )));
    }
    Ok(())
}

#[cfg(not(any(test, feature = "test-faults")))]
fn maybe_fault(
    _fault: Option<AofRewriteFaultPoint>,
    _point: AofRewriteFaultPoint,
) -> io::Result<()> {
    Ok(())
}

/// AOF rewrite engine.
pub struct AofRewriter;

impl AofRewriter {
    /// Write a standalone snapshot AOF by dumping the current keyspace state.
    ///
    /// This helper is safe for offline tests/tools: it refuses to replace an
    /// existing file. Live rewrite handoff must use [`Self::rewrite_with_writer`].
    ///
    /// # Errors
    ///
    /// Returns an error if `aof_path` already exists, the snapshot cannot be
    /// written or synced, the parent directory cannot be synced, or any live
    /// value cannot be encoded into an AOF record.
    pub fn rewrite(
        keyspace: &ConcurrentKeyspace,
        aof_path: &Path,
        reactor_id: AofReactorId,
    ) -> io::Result<(PathBuf, u64)> {
        let keys_written = Self::write_snapshot_file(keyspace, aof_path, reactor_id, true)?;
        Ok((aof_path.to_path_buf(), keys_written))
    }

    /// Rewrite an active AOF using a crash-safe base/tail manifest handoff.
    ///
    /// `expected_epoch` must match `writer_epoch`, mirroring the pool-owned
    /// AOF coordinator epoch. This keeps stale rewrite requests from swapping
    /// a writer generation that is no longer active.
    ///
    /// # Errors
    ///
    /// Returns an error if the epoch check fails, the active writer cannot be
    /// flushed/synced, the base or tail file cannot be written and synced, the
    /// preparing or active manifest cannot be published durably, the writer swap
    /// fails, or any live value cannot be encoded into an AOF record.
    ///
    /// On error, recovery must load the manifest associated with the original
    /// AOF path and replay according to its state. A `preparing` manifest keeps
    /// recovery on the legacy AOF; an `active` manifest selects the base/tail
    /// set. Live server exposure remains gated separately until non-blocking
    /// rewrite handoff is implemented and measured.
    pub fn rewrite_with_writer(
        keyspace: &ConcurrentKeyspace,
        writer: &mut AofFileWriter,
        reactor_id: AofReactorId,
        expected_epoch: u64,
        writer_epoch: u64,
    ) -> io::Result<AofRewriteOutcome> {
        Self::rewrite_with_writer_inner::<fn()>(
            keyspace,
            writer,
            reactor_id,
            expected_epoch,
            writer_epoch,
            None,
            None,
        )
    }

    /// Test-only rewrite entrypoint that injects a named crash point.
    #[cfg(any(test, feature = "test-faults"))]
    pub fn rewrite_with_writer_fault(
        keyspace: &ConcurrentKeyspace,
        writer: &mut AofFileWriter,
        reactor_id: AofReactorId,
        expected_epoch: u64,
        writer_epoch: u64,
        fault: AofRewriteFaultPoint,
    ) -> io::Result<AofRewriteOutcome> {
        Self::rewrite_with_writer_inner::<fn()>(
            keyspace,
            writer,
            reactor_id,
            expected_epoch,
            writer_epoch,
            Some(fault),
            None,
        )
    }

    fn rewrite_with_writer_inner<F>(
        keyspace: &ConcurrentKeyspace,
        writer: &mut AofFileWriter,
        reactor_id: AofReactorId,
        expected_epoch: u64,
        writer_epoch: u64,
        fault: Option<AofRewriteFaultPoint>,
        after_snapshot_locks_acquired: Option<F>,
    ) -> io::Result<AofRewriteOutcome>
    where
        F: FnOnce(),
    {
        validate_rewrite_epoch(expected_epoch, writer_epoch)?;

        let old_path = writer.path().to_path_buf();
        let manifest_path = AofManifest::path_for_aof(&old_path)?;
        let base_path = rewrite_component_path(&old_path, "base", expected_epoch)?;
        let tail_path = rewrite_component_path(&old_path, "tail", expected_epoch)?;
        let base_tmp_path = tmp_path_for(&base_path);
        let tail_tmp_path = tmp_path_for(&tail_path);

        remove_file_if_exists(&base_tmp_path)?;
        remove_file_if_exists(&tail_tmp_path)?;

        writer.flush_and_sync()?;
        let tail_start = fs::metadata(&old_path)
            .map_err(|error| {
                aof_io_error(
                    AofErrorKind::Append,
                    "failed to stat active AOF before rewrite",
                    error,
                )
            })?
            .len();

        let keys_written = Self::write_snapshot_file_inner(
            keyspace,
            &base_tmp_path,
            reactor_id,
            false,
            fault,
            after_snapshot_locks_acquired,
        )?;
        maybe_fault(fault, AofRewriteFaultPoint::AfterBaseTempSync)?;
        fs::rename(&base_tmp_path, &base_path).map_err(|error| {
            aof_io_error(
                AofErrorKind::Append,
                "failed to rename AOF rewrite base file",
                error,
            )
        })?;
        sync_parent_dir_with_fault(&base_path, fault)?;
        maybe_fault(fault, AofRewriteFaultPoint::AfterBaseRename)?;

        writer.flush_and_sync()?;
        let tail_end = fs::metadata(&old_path)
            .map_err(|error| {
                aof_io_error(
                    AofErrorKind::Append,
                    "failed to stat active AOF after rewrite",
                    error,
                )
            })?
            .len();
        let tail_bytes_copied =
            Self::write_tail_file(&old_path, &tail_tmp_path, reactor_id, tail_start, tail_end)?;
        fs::rename(&tail_tmp_path, &tail_path).map_err(|error| {
            aof_io_error(
                AofErrorKind::Append,
                "failed to rename AOF rewrite tail file",
                error,
            )
        })?;
        sync_parent_dir_with_fault(&tail_path, fault)?;

        AofManifest::write_atomic_inner(
            &manifest_path,
            AofManifestState::Preparing,
            expected_epoch,
            std::slice::from_ref(&base_path),
            std::slice::from_ref(&tail_path),
            std::slice::from_ref(&old_path),
            fault,
        )?;
        maybe_fault(fault, AofRewriteFaultPoint::AfterPreparingManifest)?;

        validate_rewrite_epoch(expected_epoch, writer_epoch)?;
        maybe_fault(fault, AofRewriteFaultPoint::WriterSwapFailure)?;
        writer.swap_file(&tail_path)?;
        maybe_fault(fault, AofRewriteFaultPoint::AfterWriterSwap)?;

        AofManifest::write_atomic_inner(
            &manifest_path,
            AofManifestState::Active,
            expected_epoch,
            std::slice::from_ref(&base_path),
            std::slice::from_ref(&tail_path),
            std::slice::from_ref(&old_path),
            fault,
        )?;
        maybe_fault(fault, AofRewriteFaultPoint::AfterActiveManifest)?;
        maybe_fault(fault, AofRewriteFaultPoint::BeforeOldCleanup)?;

        let old_file_removed = match fs::remove_file(&old_path) {
            Ok(()) => {
                sync_parent_dir_with_fault(&old_path, fault)?;
                true
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => true,
            Err(_) => false,
        };

        Ok(AofRewriteOutcome {
            manifest_path,
            base_path,
            tail_path,
            old_path,
            keys_written,
            tail_bytes_copied,
            epoch: expected_epoch,
            old_file_removed,
        })
    }

    fn write_snapshot_file(
        keyspace: &ConcurrentKeyspace,
        path: &Path,
        reactor_id: AofReactorId,
        create_new: bool,
    ) -> io::Result<u64> {
        Self::write_snapshot_file_inner::<fn()>(keyspace, path, reactor_id, create_new, None, None)
    }

    fn write_snapshot_file_inner<F>(
        keyspace: &ConcurrentKeyspace,
        path: &Path,
        reactor_id: AofReactorId,
        create_new: bool,
        fault: Option<AofRewriteFaultPoint>,
        after_snapshot_locks_acquired: Option<F>,
    ) -> io::Result<u64>
    where
        F: FnOnce(),
    {
        let mut options = File::options();
        options.write(true).create(true);
        if create_new {
            options.create_new(true);
        } else {
            options.truncate(true);
        }
        let file = options.open(path).map_err(|error| {
            aof_io_error(
                AofErrorKind::Append,
                "failed to create AOF rewrite snapshot file",
                error,
            )
        })?;
        let mut writer = BufWriter::with_capacity(REWRITE_BUF_SIZE, file);

        // Rewrite files use v1 format (no LSN prefix per record) since they
        // are point-in-time snapshots. LSN numbering resumes from the current
        // global LSN after the writer swap.
        let header = AofHeader::new(reactor_id, AofWriterMode::SnapshotRewrite);
        header.write_to(&mut writer).map_err(|error| {
            aof_io_error(
                AofErrorKind::Append,
                "failed to write AOF rewrite header",
                error,
            )
        })?;

        let now_nanos = Timestamp::now().as_nanos();
        let unix_now_nanos = current_unix_time_nanos();
        let mut keys_written = 0u64;

        // Alpha-safe rewrite deliberately holds all shard read locks while
        // streaming the base snapshot. This blocks writers rather than taking
        // a fuzzy snapshot that could double-apply non-idempotent tail records.
        let mut shards = Vec::with_capacity(keyspace.num_shards());
        for shard_idx in 0..keyspace.num_shards() {
            shards.push(keyspace.read_shard_by_index(shard_idx));
        }
        if let Some(hook) = after_snapshot_locks_acquired {
            hook();
        }

        for shard in &shards {
            let total = shard.total_slots();
            for slot in 0..total {
                if let Some((key_bytes, value)) = shard.slot_key_value(slot) {
                    let ttl_nanos = shard.slot_entry_ttl(slot);

                    // Skip expired entries.
                    if ttl_nanos != 0 && ttl_nanos <= now_nanos {
                        continue;
                    }

                    // Serialize as RESP SET command.
                    match value {
                        VortexValue::Integer(n) => {
                            let mut buf = itoa::Buffer::new();
                            let s = buf.format(*n);
                            Self::write_set_cmd(&mut writer, key_bytes, s.as_bytes())?;
                        }
                        VortexValue::InlineString(_) | VortexValue::String(_) => {
                            let bytes = value.as_string_bytes().unwrap_or(b"");
                            Self::write_set_cmd(&mut writer, key_bytes, bytes)?;
                        }
                        _ => {
                            // Skip non-string types for now (Phase 4 will extend).
                            continue;
                        }
                    }

                    // If the key has a TTL, emit PEXPIREAT.
                    if ttl_nanos != 0 && ttl_nanos > now_nanos {
                        let deadline_ms = deadline_nanos_to_absolute_unix_nanos(
                            ttl_nanos,
                            now_nanos,
                            unix_now_nanos,
                        ) / 1_000_000;
                        Self::write_pexpireat_cmd(&mut writer, key_bytes, deadline_ms)?;
                    }

                    keys_written += 1;
                }
            }
        }
        drop(shards);
        maybe_fault(fault, AofRewriteFaultPoint::AfterBaseTempWrite)?;

        writer.flush().map_err(|error| {
            aof_io_error(
                AofErrorKind::Flush,
                "failed to flush AOF rewrite file",
                error,
            )
        })?;
        writer.get_ref().sync_all().map_err(|error| {
            aof_io_error(
                AofErrorKind::Fsync,
                "failed to sync AOF rewrite file",
                error,
            )
        })?;

        Ok(keys_written)
    }

    fn write_tail_file(
        old_path: &Path,
        tail_path: &Path,
        reactor_id: AofReactorId,
        start: u64,
        end: u64,
    ) -> io::Result<u64> {
        let mut source = File::open(old_path).map_err(|error| {
            aof_io_error(
                AofErrorKind::Append,
                "failed to open old AOF tail for rewrite",
                error,
            )
        })?;
        source.seek(SeekFrom::Start(start)).map_err(|error| {
            aof_io_error(
                AofErrorKind::Append,
                "failed to seek old AOF tail for rewrite",
                error,
            )
        })?;

        let tail_file = File::create(tail_path).map_err(|error| {
            aof_io_error(
                AofErrorKind::Append,
                "failed to create AOF rewrite tail file",
                error,
            )
        })?;
        let mut writer = BufWriter::with_capacity(REWRITE_BUF_SIZE, tail_file);
        let header = AofHeader::new(reactor_id, AofWriterMode::Journal);
        header.write_to(&mut writer).map_err(|error| {
            aof_io_error(
                AofErrorKind::Append,
                "failed to write AOF rewrite tail header",
                error,
            )
        })?;

        let bytes_to_copy = end.saturating_sub(start);
        if bytes_to_copy > 0 {
            io::copy(&mut source.take(bytes_to_copy), &mut writer).map_err(|error| {
                aof_io_error(
                    AofErrorKind::Append,
                    "failed to copy AOF rewrite tail bytes",
                    error,
                )
            })?;
        }
        writer.flush().map_err(|error| {
            aof_io_error(
                AofErrorKind::Flush,
                "failed to flush AOF rewrite tail file",
                error,
            )
        })?;
        writer.get_ref().sync_all().map_err(|error| {
            aof_io_error(
                AofErrorKind::Fsync,
                "failed to sync AOF rewrite tail file",
                error,
            )
        })?;

        Ok(bytes_to_copy)
    }

    /// Write a SET command in RESP format: `*3\r\n$3\r\nSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<value>\r\n`
    fn write_set_cmd<W: Write>(w: &mut W, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut klen_buf = itoa::Buffer::new();
        let mut vlen_buf = itoa::Buffer::new();
        let klen = klen_buf.format(key.len());
        let vlen = vlen_buf.format(value.len());

        w.write_all(b"*3\r\n$3\r\nSET\r\n$")?;
        w.write_all(klen.as_bytes())?;
        w.write_all(b"\r\n")?;
        w.write_all(key)?;
        w.write_all(b"\r\n$")?;
        w.write_all(vlen.as_bytes())?;
        w.write_all(b"\r\n")?;
        w.write_all(value)?;
        w.write_all(b"\r\n")?;
        Ok(())
    }

    /// Write a PEXPIREAT command: `*3\r\n$10\r\nPEXPIREAT\r\n$<klen>\r\n<key>\r\n$<tlen>\r\n<timestamp_ms>\r\n`
    fn write_pexpireat_cmd<W: Write>(w: &mut W, key: &[u8], timestamp_ms: u64) -> io::Result<()> {
        let mut klen_buf = itoa::Buffer::new();
        let klen = klen_buf.format(key.len());

        let mut ts_buf = itoa::Buffer::new();
        let ts = ts_buf.format(timestamp_ms);
        let mut tlen_buf = itoa::Buffer::new();
        let tlen = tlen_buf.format(ts.len());

        w.write_all(b"*3\r\n$9\r\nPEXPIREAT\r\n$")?;
        w.write_all(klen.as_bytes())?;
        w.write_all(b"\r\n")?;
        w.write_all(key)?;
        w.write_all(b"\r\n$")?;
        w.write_all(tlen.as_bytes())?;
        w.write_all(b"\r\n")?;
        w.write_all(ts.as_bytes())?;
        w.write_all(b"\r\n")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::fault::{AofFaultPlan, AofFaultPoint};
    use crate::aof::reader::AofReader;
    use crate::aof::{AofFsyncPolicy, AofRecordBytes};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use vortex_engine::commands::{CommandClock, RESP_NIL, execute_command};
    use vortex_engine::keyspace::AofLsn;
    use vortex_proto::{RespFrame, RespTape};

    fn temp_path(suffix: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut path = std::env::temp_dir();
        path.push(format!(
            "vortex-test-aof-rewrite-{}-{}-{}.aof",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            suffix
        ));
        path
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(path.with_extension("rewrite.tmp"));
        if let Ok(manifest_path) = AofManifest::path_for_aof(path) {
            let _ = fs::remove_file(&manifest_path);
            let _ = fs::remove_file(tmp_path_for(&manifest_path));
        }
        for epoch in 1..=8 {
            for component in ["base", "tail"] {
                if let Ok(component_path) = rewrite_component_path(path, component, epoch) {
                    let _ = fs::remove_file(&component_path);
                    let _ = fs::remove_file(tmp_path_for(&component_path));
                }
            }
        }
    }

    fn make_keyspace() -> ConcurrentKeyspace {
        ConcurrentKeyspace::new(64)
    }

    /// Helper: run a RESP command against the keyspace.
    fn run_cmd(ks: &ConcurrentKeyspace, cmd: &[u8]) {
        let tape = RespTape::parse_pipeline(cmd).unwrap();
        let frame = tape.iter().next().unwrap();
        let name = frame.command_name().unwrap();
        let now = Timestamp::now().as_nanos();
        let unix_now = current_unix_time_nanos();
        let _ = execute_command(ks, name, &frame, CommandClock::new(now, unix_now));
    }

    fn get_value(ks: &ConcurrentKeyspace, key: &[u8]) -> Option<Vec<u8>> {
        let cmd = format!(
            "*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n",
            key.len(),
            std::str::from_utf8(key).unwrap()
        );
        let tape = RespTape::parse_pipeline(cmd.as_bytes()).unwrap();
        let frame = tape.iter().next().unwrap();
        let now = Timestamp::now().as_nanos();
        let unix_now = current_unix_time_nanos();
        let executed =
            execute_command(ks, b"GET", &frame, CommandClock::new(now, unix_now)).unwrap();
        match executed.response {
            vortex_engine::commands::CmdResult::Static(s) if std::ptr::eq(s, RESP_NIL) => None,
            vortex_engine::commands::CmdResult::Inline(inline) => Some(inline.payload().to_vec()),
            vortex_engine::commands::CmdResult::Resp(RespFrame::BulkString(Some(bytes))) => {
                Some(bytes.to_vec())
            }
            _ => None,
        }
    }

    fn lsn(raw: u64) -> AofLsn {
        AofLsn::try_from_raw(raw).unwrap()
    }

    fn append_set(writer: &mut AofFileWriter, raw_lsn: u64, key: &[u8], value: &[u8]) {
        let cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            std::str::from_utf8(key).unwrap(),
            value.len(),
            std::str::from_utf8(value).unwrap()
        );
        writer
            .append_with_lsn(lsn(raw_lsn), AofRecordBytes::from_resp(cmd.as_bytes()))
            .unwrap();
    }

    fn replay_paths(paths: &[PathBuf]) -> ConcurrentKeyspace {
        let ks = make_keyspace();
        AofReader::replay_merge(paths, &ks).unwrap();
        ks
    }

    #[test]
    fn rewrite_basic() {
        let path = temp_path("basic");
        let ks = make_keyspace();

        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n");
        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n");

        let (_, keys) = AofRewriter::rewrite(&ks, &path, AofReactorId::from_u16(0)).unwrap();
        assert_eq!(keys, 2);

        // Replay the rewritten AOF into a fresh keyspace.
        let ks2 = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks2).unwrap();
        assert_eq!(stats.commands_replayed, 2);
        assert_eq!(get_value(&ks2, b"key1").as_deref(), Some(&b"value1"[..]));
        assert_eq!(get_value(&ks2, b"key2").as_deref(), Some(&b"value2"[..]));

        cleanup(&path);
    }

    #[test]
    fn rewrite_with_ttl() {
        let path = temp_path("ttl");
        let ks = make_keyspace();

        // SET ephemeral with TTL via PEXPIREAT.
        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$9\r\nephemeral\r\n$4\r\ndata\r\n");
        let future_ms = current_unix_time_nanos() / 1_000_000 + 60_000;
        let pexpireat_cmd = format!(
            "*3\r\n$9\r\nPEXPIREAT\r\n$9\r\nephemeral\r\n${}\r\n{}\r\n",
            format!("{future_ms}").len(),
            future_ms
        );
        run_cmd(&ks, pexpireat_cmd.as_bytes());

        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$9\r\npermanent\r\n$4\r\ndata\r\n");

        let (_, keys) = AofRewriter::rewrite(&ks, &path, AofReactorId::from_u16(0)).unwrap();
        assert_eq!(keys, 2);

        // Replay — 2 SETs + 1 PEXPIREAT = 3 commands.
        let ks2 = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks2).unwrap();
        assert_eq!(stats.commands_replayed, 3);
        assert_eq!(get_value(&ks2, b"ephemeral").as_deref(), Some(&b"data"[..]));
        assert_eq!(get_value(&ks2, b"permanent").as_deref(), Some(&b"data"[..]));

        cleanup(&path);
    }

    #[test]
    fn rewrite_integer_values() {
        let path = temp_path("integers");
        let ks = make_keyspace();

        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$2\r\n42\r\n");

        let (_, keys) = AofRewriter::rewrite(&ks, &path, AofReactorId::from_u16(0)).unwrap();
        assert_eq!(keys, 1);

        let ks2 = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks2).unwrap();
        assert_eq!(stats.commands_replayed, 1);
        assert_eq!(get_value(&ks2, b"counter").as_deref(), Some(&b"42"[..]));

        cleanup(&path);
    }

    #[test]
    fn rewrite_compacts_history() {
        let path = temp_path("compact");
        let ks = make_keyspace();

        // Overwrite key1 many times — only final value matters.
        for i in 0..100 {
            let val = format!("val{i}");
            let cmd = format!(
                "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n${}\r\n{}\r\n",
                val.len(),
                val
            );
            run_cmd(&ks, cmd.as_bytes());
        }
        // Set then delete key2 — should not appear in rewrite.
        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$4\r\ngone\r\n");
        run_cmd(&ks, b"*2\r\n$3\r\nDEL\r\n$4\r\nkey2\r\n");

        let (_, keys) = AofRewriter::rewrite(&ks, &path, AofReactorId::from_u16(0)).unwrap();
        assert_eq!(keys, 1); // Only key1 with final value.

        cleanup(&path);
    }

    #[test]
    fn manifest_rewrite_replays_base_and_post_swap_tail() {
        let path = temp_path("manifest-success");
        let ks = make_keyspace();
        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$4\r\nbase\r\n$5\r\nvalue\r\n");

        let mut writer =
            AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
        append_set(&mut writer, 1, b"base", b"value");
        writer.flush_and_sync().unwrap();

        let outcome =
            AofRewriter::rewrite_with_writer(&ks, &mut writer, AofReactorId::from_u16(0), 1, 1)
                .unwrap();
        assert_eq!(outcome.keys_written, 1);
        assert_eq!(writer.path(), outcome.tail_path.as_path());
        assert!(outcome.old_file_removed);

        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$4\r\npost\r\n$5\r\ntail!\r\n");
        append_set(&mut writer, 2, b"post", b"tail!");
        writer.flush_and_sync().unwrap();

        let manifest = AofManifest::load_for_aof(&path).unwrap().unwrap();
        assert_eq!(manifest.state(), AofManifestState::Active);
        assert_eq!(
            manifest.active_tail_path(),
            Some(outcome.tail_path.as_path())
        );

        let replayed = make_keyspace();
        let stats = AofReader::replay_manifest(&manifest, &replayed).unwrap();
        assert_eq!(stats.bytes_truncated, 0);
        assert_eq!(stats.max_persisted_lsn, 2);
        assert_eq!(
            get_value(&replayed, b"base").as_deref(),
            Some(&b"value"[..])
        );
        assert_eq!(
            get_value(&replayed, b"post").as_deref(),
            Some(&b"tail!"[..])
        );

        cleanup(&path);
    }

    #[test]
    fn concurrent_mutation_waits_for_manifest_rewrite_and_replays_from_tail() {
        let path = temp_path("manifest-concurrent");
        let ks = Arc::new(make_keyspace());
        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$4\r\nbase\r\n$5\r\nvalue\r\n");

        let mut writer =
            AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
        append_set(&mut writer, 1, b"base", b"value");
        writer.flush_and_sync().unwrap();

        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let attempted = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let worker_ks = Arc::clone(&ks);
        let worker_attempted = Arc::clone(&attempted);
        let worker_finished = Arc::clone(&finished);
        let worker = std::thread::spawn(move || {
            start_rx.recv().unwrap();
            worker_attempted.store(true, Ordering::Release);
            run_cmd(
                &worker_ks,
                b"*3\r\n$3\r\nSET\r\n$10\r\nconcurrent\r\n$4\r\ntail\r\n",
            );
            worker_finished.store(true, Ordering::Release);
        });

        let hook = || {
            start_tx.send(()).unwrap();
            while !attempted.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            std::thread::sleep(Duration::from_millis(20));
            assert!(
                !finished.load(Ordering::Acquire),
                "writer should wait while rewrite holds shard read locks"
            );
        };

        let outcome = AofRewriter::rewrite_with_writer_inner(
            &ks,
            &mut writer,
            AofReactorId::from_u16(0),
            1,
            1,
            None,
            Some(hook),
        )
        .unwrap();
        worker.join().unwrap();

        append_set(&mut writer, 2, b"concurrent", b"tail");
        writer.flush_and_sync().unwrap();

        let manifest = AofManifest::load_for_aof(&path).unwrap().unwrap();
        let replayed = make_keyspace();
        AofReader::replay_manifest(&manifest, &replayed).unwrap();
        assert_eq!(
            get_value(&replayed, b"base").as_deref(),
            Some(&b"value"[..])
        );
        assert_eq!(
            get_value(&replayed, b"concurrent").as_deref(),
            Some(&b"tail"[..])
        );
        assert_eq!(writer.path(), outcome.tail_path.as_path());

        cleanup(&path);
    }

    #[test]
    fn preparing_manifest_replays_legacy_after_failed_rewrite() {
        let path = temp_path("manifest-preparing");
        let ks = make_keyspace();
        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$6\r\nlegacy\r\n$5\r\nvalue\r\n");

        let mut writer =
            AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
        append_set(&mut writer, 1, b"legacy", b"value");
        writer.flush_and_sync().unwrap();

        let err = AofRewriter::rewrite_with_writer_inner::<fn()>(
            &ks,
            &mut writer,
            AofReactorId::from_u16(0),
            1,
            1,
            Some(AofRewriteFaultPoint::AfterPreparingManifest),
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("injected AOF rewrite fault"));

        let manifest = AofManifest::load_for_aof(&path).unwrap().unwrap();
        assert_eq!(manifest.state(), AofManifestState::Preparing);
        assert_eq!(manifest.replay_paths(), vec![path.clone()]);

        let replayed = replay_paths(&manifest.replay_paths());
        assert_eq!(
            get_value(&replayed, b"legacy").as_deref(),
            Some(&b"value"[..])
        );
        assert_eq!(writer.path(), path.as_path());

        cleanup(&path);
    }

    #[test]
    fn rewrite_rejects_stale_writer_epoch_before_swap() {
        let path = temp_path("stale-epoch");
        let ks = make_keyspace();
        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");

        let mut writer =
            AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
        append_set(&mut writer, 1, b"k", b"v");

        let error =
            AofRewriter::rewrite_with_writer(&ks, &mut writer, AofReactorId::from_u16(0), 2, 1)
                .unwrap_err();
        assert!(error.to_string().contains("does not match writer epoch"));
        assert_eq!(writer.path(), path.as_path());
        writer.flush_and_sync().unwrap();

        let replayed = replay_paths(std::slice::from_ref(&path));
        assert_eq!(get_value(&replayed, b"k").as_deref(), Some(&b"v"[..]));

        cleanup(&path);
    }

    #[test]
    fn rewrite_fault_points_leave_replayable_state() {
        let fault_points = [
            AofRewriteFaultPoint::AfterBaseTempWrite,
            AofRewriteFaultPoint::AfterBaseTempSync,
            AofRewriteFaultPoint::AfterBaseRename,
            AofRewriteFaultPoint::ManifestRenameFailure,
            AofRewriteFaultPoint::DirectoryFsyncFailure,
            AofRewriteFaultPoint::AfterPreparingManifest,
            AofRewriteFaultPoint::WriterSwapFailure,
            AofRewriteFaultPoint::AfterWriterSwap,
            AofRewriteFaultPoint::AfterActiveManifest,
            AofRewriteFaultPoint::BeforeOldCleanup,
        ];

        for (idx, fault) in fault_points.into_iter().enumerate() {
            let path = temp_path(&format!("fault-{idx}"));
            let ks = make_keyspace();
            run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$6\r\nstable\r\n$5\r\nvalue\r\n");

            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            append_set(&mut writer, 1, b"stable", b"value");
            writer.flush_and_sync().unwrap();

            let _ = AofRewriter::rewrite_with_writer_inner::<fn()>(
                &ks,
                &mut writer,
                AofReactorId::from_u16(0),
                1,
                1,
                Some(fault),
                None,
            );

            let replayed = if let Some(manifest) = AofManifest::load_for_aof(&path).unwrap() {
                let replayed = make_keyspace();
                AofReader::replay_manifest(&manifest, &replayed).unwrap();
                replayed
            } else {
                replay_paths(std::slice::from_ref(&path))
            };
            assert_eq!(
                get_value(&replayed, b"stable").as_deref(),
                Some(&b"value"[..]),
                "fault point {}",
                fault.as_str()
            );

            cleanup(&path);
        }
    }

    #[test]
    fn rewrite_failure_artifacts_include_seed_and_fault_name() {
        let required = [
            (
                AofFaultPoint::RewriteTempFailure,
                AofRewriteFaultPoint::AfterBaseTempWrite,
            ),
            (
                AofFaultPoint::ManifestRenameFailure,
                AofRewriteFaultPoint::ManifestRenameFailure,
            ),
            (
                AofFaultPoint::DirectoryFsyncFailure,
                AofRewriteFaultPoint::DirectoryFsyncFailure,
            ),
            (
                AofFaultPoint::WriterSwapFailure,
                AofRewriteFaultPoint::WriterSwapFailure,
            ),
        ];

        for (point, rewrite_point) in required {
            let plan = AofFaultPlan::new(point, 0x5eed);
            let label = plan.artifact_label();
            assert!(label.contains(point.as_str()));
            assert!(label.contains("seed-0000000000005eed"));
            assert!(!rewrite_point.as_str().is_empty());
        }
    }

    #[test]
    fn manifest_load_rejects_path_traversal_and_nested_entries() {
        let path = temp_path("manifest-path-validation");
        let manifest_path = AofManifest::path_for_aof(&path).unwrap();
        let invalid_entries = [
            "../escape.aof",
            "/tmp/escape.aof",
            "nested/base.aof",
            "nested\\base.aof",
            ".",
            "",
        ];

        for entry in invalid_entries {
            let manifest = format!(
                "{MANIFEST_MAGIC} {MANIFEST_VERSION}\nstate active\nepoch 1\nbase {entry}\ntail tail.aof\n"
            );
            fs::write(&manifest_path, manifest).unwrap();

            let error = AofManifest::load(&manifest_path).unwrap_err();
            assert_eq!(
                crate::aof::aof_error_kind(&error),
                Some(AofErrorKind::UnsupportedFormat),
                "entry {entry:?}"
            );
            assert!(
                error.to_string().contains("must be a file name"),
                "entry {entry:?}: {error}"
            );
        }

        cleanup(&path);
    }

    #[test]
    fn manifest_load_for_aof_missing_manifest_returns_none() {
        let path = temp_path("manifest-missing");
        assert!(AofManifest::load_for_aof(&path).unwrap().is_none());
        cleanup(&path);
    }

    #[test]
    fn manifest_load_rejects_duplicate_scalar_keys() {
        let path = temp_path("manifest-duplicate-scalars");
        let manifest_path = AofManifest::path_for_aof(&path).unwrap();
        let cases = [
            (
                format!(
                    "{MANIFEST_MAGIC} {MANIFEST_VERSION}\nstate active\nstate preparing\nepoch 1\nbase base.aof\ntail tail.aof\nlegacy old.aof\n"
                ),
                "duplicate AOF manifest state",
            ),
            (
                format!(
                    "{MANIFEST_MAGIC} {MANIFEST_VERSION}\nstate active\nepoch 1\nepoch 2\nbase base.aof\ntail tail.aof\n"
                ),
                "duplicate AOF manifest epoch",
            ),
        ];

        for (manifest, expected) in cases {
            fs::write(&manifest_path, manifest).unwrap();
            let error = AofManifest::load(&manifest_path).unwrap_err();
            assert_eq!(
                crate::aof::aof_error_kind(&error),
                Some(AofErrorKind::UnsupportedFormat)
            );
            assert!(error.to_string().contains(expected));
        }

        cleanup(&path);
    }

    #[test]
    fn standalone_rewrite_refuses_to_replace_existing_file() {
        let path = temp_path("standalone-existing");
        let ks = make_keyspace();
        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");

        let mut writer =
            AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
        append_set(&mut writer, 1, b"k", b"v");
        writer.flush_and_sync().unwrap();

        let error = AofRewriter::rewrite(&ks, &path, AofReactorId::from_u16(0)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);

        cleanup(&path);
    }
}
