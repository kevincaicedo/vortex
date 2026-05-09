//! AOF reader and replay engine.
//!
//! Reads VortexDB AOF files and replays mutation commands into a
//! [`ConcurrentKeyspace`] to restore state after restart.
//!
//! ## Format v2 (P1.6): LSN-Ordered K-Way Merge
//!
//! Each per-reactor AOF file contains `[LSN: 8 bytes LE] [RESP data]` records.
//! On startup, all reactor AOF files are opened simultaneously and merged
//! using a `BinaryHeap` min-heap keyed by LSN. This guarantees deterministic
//! chronological replay regardless of file read order.
//!
//! ## Backward Compatibility
//!
//! v1 files (no LSN prefix) are supported during transition: records are
//! assigned synthetic ordering keys for the merge heap. Synthetic keys are not
//! used to restore the engine's bounded global LSN counter.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

use vortex_common::{Timestamp, current_unix_time_nanos};
use vortex_engine::ConcurrentKeyspace;
use vortex_engine::commands::{CmdResult, CommandClock, execute_command};
use vortex_engine::keyspace::{AofLsn, LsnOverflow};
use vortex_proto::{FrameRef, ParseError, RespFrame, RespParser, RespTape};

use super::error::{AofErrorKind, aof_error, aof_io_error};
use super::format::{
    AOF_HEADER_SIZE, AOF_TRANSACTION_BATCH_COMMAND, AofHeader, AofReactorId, LSN_SIZE,
};
use super::rewrite::{AofManifest, AofManifestState};

const DEFAULT_REPLAY_READ_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_REPLAY_MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// Bounded AOF replay memory policy.
#[derive(Debug, Clone, Copy)]
pub struct AofReplayConfig {
    /// Bytes read from each AOF cursor per fill.
    pub read_chunk_bytes: usize,
    /// Maximum encoded record bytes retained for one cursor.
    pub max_record_bytes: usize,
}

impl Default for AofReplayConfig {
    fn default() -> Self {
        Self {
            read_chunk_bytes: DEFAULT_REPLAY_READ_CHUNK_BYTES,
            max_record_bytes: DEFAULT_REPLAY_MAX_RECORD_BYTES,
        }
    }
}

impl AofReplayConfig {
    #[inline]
    fn normalized(self) -> Self {
        let max_record_bytes = self.max_record_bytes.max(LSN_SIZE + 1);
        Self {
            read_chunk_bytes: self.read_chunk_bytes.max(1).min(max_record_bytes),
            max_record_bytes,
        }
    }
}

/// Statistics from an AOF replay.
#[derive(Debug, Clone)]
pub struct ReplayStats {
    /// Number of commands successfully replayed.
    pub commands_replayed: u64,
    /// Number of bytes read from AOF files (excluding headers).
    pub bytes_read: u64,
    /// Number of bytes discarded (truncated trailing records).
    pub bytes_truncated: u64,
    /// Wall-clock duration of the replay.
    pub duration_ms: u64,
    /// Reactor ID from the AOF header.
    pub reactor_id: AofReactorId,
    /// Timestamp when the AOF was created.
    pub created_at: u64,
    /// Highest LSN replayed (used to restore global LSN counter).
    pub max_lsn: u64,
    /// Highest persisted LSN replayed; v1 synthetic order keys are excluded.
    pub max_persisted_lsn: u64,
    /// Number of AOF files merged.
    pub files_merged: usize,
    /// Peak bytes retained by replay cursors and queued records.
    pub peak_replay_buffer_bytes: usize,
    /// Corrupt records observed before replay failed.
    pub corrupt_records: u64,
}

/// AOF file reader for replaying persistence on startup.
pub struct AofReader {
    /// Path to the AOF file.
    path: std::path::PathBuf,
}

struct ReplayRecord {
    order_lsn: u64,
    record_offset: usize,
    payload: Vec<u8>,
    restore_lsn: Option<AofLsn>,
    encoded_len: usize,
}

struct MergeHeapItem {
    cursor_idx: usize,
    sequence: usize,
    record: ReplayRecord,
}

impl PartialEq for MergeHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.record.order_lsn == other.record.order_lsn
            && self.cursor_idx == other.cursor_idx
            && self.record.record_offset == other.record.record_offset
            && self.sequence == other.sequence
    }
}

impl Eq for MergeHeapItem {}

impl PartialOrd for MergeHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .order_lsn
            .cmp(&self.record.order_lsn)
            .then_with(|| other.cursor_idx.cmp(&self.cursor_idx))
            .then_with(|| other.record.record_offset.cmp(&self.record.record_offset))
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl ReplayStats {
    fn empty() -> Self {
        Self {
            commands_replayed: 0,
            bytes_read: 0,
            bytes_truncated: 0,
            duration_ms: 0,
            reactor_id: AofReactorId::from_u16(0),
            created_at: 0,
            max_lsn: 0,
            max_persisted_lsn: 0,
            files_merged: 0,
            peak_replay_buffer_bytes: 0,
            corrupt_records: 0,
        }
    }
}

fn trim_resp_error(buf: &[u8]) -> String {
    let buf = buf.strip_prefix(b"-").unwrap_or(buf);
    let buf = buf.strip_suffix(b"\r\n").unwrap_or(buf);
    String::from_utf8_lossy(buf).into_owned()
}

fn replay_error_reply(response: &CmdResult) -> String {
    match response {
        CmdResult::Static(buf) => trim_resp_error(buf),
        CmdResult::Inline(inline) => trim_resp_error(inline.as_bytes()),
        CmdResult::Resp(RespFrame::Error(message)) => String::from_utf8_lossy(message).into_owned(),
        _ => "error reply".to_string(),
    }
}

fn replay_error(
    path: &Path,
    offset: usize,
    lsn: Option<u64>,
    command: Option<&[u8]>,
    detail: impl AsRef<str>,
) -> io::Error {
    let mut message = format!(
        "AOF replay failed for {} at offset={offset}",
        path.display()
    );
    if let Some(lsn) = lsn {
        message.push_str(&format!(", lsn={lsn}"));
    }
    if let Some(command) = command {
        message.push_str(&format!(", command={}", String::from_utf8_lossy(command)));
    }
    message.push_str(": ");
    message.push_str(detail.as_ref());
    aof_error(AofErrorKind::CorruptRecord, message)
}

fn replay_parse_error(
    path: &Path,
    offset: usize,
    lsn: Option<u64>,
    error: &ParseError,
) -> io::Error {
    replay_error(
        path,
        offset,
        lsn,
        None,
        format!("invalid RESP record: {error:?}"),
    )
}

fn execute_replay_record(
    path: &Path,
    keyspace: &ConcurrentKeyspace,
    record_offset: usize,
    lsn: Option<u64>,
    frame: &FrameRef<'_>,
    clock: CommandClock,
) -> io::Result<u64> {
    let name = frame.command_name().ok_or_else(|| {
        replay_error(
            path,
            record_offset,
            lsn,
            None,
            "record is not a command array",
        )
    })?;

    if name.eq_ignore_ascii_case(AOF_TRANSACTION_BATCH_COMMAND) {
        return execute_replay_transaction_batch(path, keyspace, record_offset, lsn, frame, clock);
    }

    let executed = execute_command(keyspace, name, frame, clock).ok_or_else(|| {
        replay_error(
            path,
            record_offset,
            lsn,
            Some(name),
            "command is not registered for replay",
        )
    })?;

    if executed.response.is_error() {
        return Err(replay_error(
            path,
            record_offset,
            lsn,
            Some(name),
            replay_error_reply(&executed.response),
        ));
    }

    Ok(1)
}

fn execute_replay_transaction_batch(
    path: &Path,
    keyspace: &ConcurrentKeyspace,
    record_offset: usize,
    lsn: Option<u64>,
    frame: &FrameRef<'_>,
    clock: CommandClock,
) -> io::Result<u64> {
    let mut children = frame.children().ok_or_else(|| {
        replay_error(
            path,
            record_offset,
            lsn,
            Some(AOF_TRANSACTION_BATCH_COMMAND),
            "transaction batch record is not an array",
        )
    })?;
    let _name = children.next();
    let payload = children
        .next()
        .and_then(|child| child.as_bytes())
        .ok_or_else(|| {
            replay_error(
                path,
                record_offset,
                lsn,
                Some(AOF_TRANSACTION_BATCH_COMMAND),
                "transaction batch is missing payload",
            )
        })?;
    if children.next().is_some() {
        return Err(replay_error(
            path,
            record_offset,
            lsn,
            Some(AOF_TRANSACTION_BATCH_COMMAND),
            "transaction batch has extra fields",
        ));
    }

    let tape = RespTape::parse_pipeline(payload)
        .map_err(|error| replay_parse_error(path, record_offset, lsn, &error))?;
    let mut replayed = 0u64;
    for frame in tape.iter() {
        replayed += execute_replay_record(path, keyspace, record_offset, lsn, &frame, clock)?;
    }
    Ok(replayed)
}

fn truncate_partial_tail(path: &Path, valid_len: u64) -> io::Result<()> {
    let file = File::options().write(true).open(path).map_err(|error| {
        aof_io_error(
            AofErrorKind::TruncatedTail,
            "failed to open AOF for tail truncation",
            error,
        )
    })?;
    file.set_len(valid_len).map_err(|error| {
        aof_io_error(
            AofErrorKind::TruncatedTail,
            "failed to truncate partial AOF tail",
            error,
        )
    })?;
    file.sync_all().map_err(|error| {
        aof_io_error(
            AofErrorKind::Fsync,
            "failed to sync AOF after tail truncation",
            error,
        )
    })
}

fn invalid_lsn_error(path: &Path, record_offset: usize, error: LsnOverflow) -> io::Error {
    aof_error(
        AofErrorKind::CorruptRecord,
        format!(
            "AOF replay error in {} at offset {}: lsn {} exceeds maximum entry LSN {}",
            path.display(),
            record_offset,
            error.attempted,
            error.max
        ),
    )
}

fn invalid_replay_record_error(
    path: &Path,
    record_offset: usize,
    lsn: Option<u64>,
    detail: impl AsRef<str>,
) -> io::Error {
    replay_error(path, record_offset, lsn, None, detail)
}

fn restore_lsn_after_replay(
    keyspace: &ConcurrentKeyspace,
    max_lsn: Option<AofLsn>,
) -> io::Result<()> {
    // SAFETY: AOF replay runs during startup recovery before reactors serve
    // traffic for this keyspace.
    unsafe {
        keyspace.restore_lsn_after_replay(max_lsn).map_err(|error| {
            aof_error(
                AofErrorKind::CorruptRecord,
                format!(
                    "AOF replay could not restore LSN after max replayed LSN {}",
                    error.max_replayed_lsn
                ),
            )
        })
    }
}

fn current_replay_buffer_bytes(
    cursors: &[AofFileCursor],
    heap: &BinaryHeap<MergeHeapItem>,
) -> usize {
    let cursor_bytes = cursors
        .iter()
        .map(AofFileCursor::current_buffer_bytes)
        .sum::<usize>();
    let queued_record_bytes = heap
        .iter()
        .map(|item| item.record.payload.len())
        .sum::<usize>();
    cursor_bytes.saturating_add(queued_record_bytes)
}

fn merge_stats(total: &mut ReplayStats, part: ReplayStats) {
    total.commands_replayed = total
        .commands_replayed
        .saturating_add(part.commands_replayed);
    total.bytes_read = total.bytes_read.saturating_add(part.bytes_read);
    total.bytes_truncated = total.bytes_truncated.saturating_add(part.bytes_truncated);
    total.duration_ms = total.duration_ms.saturating_add(part.duration_ms);
    total.max_lsn = total.max_lsn.max(part.max_lsn);
    total.max_persisted_lsn = total.max_persisted_lsn.max(part.max_persisted_lsn);
    total.files_merged = total.files_merged.saturating_add(part.files_merged);
    total.peak_replay_buffer_bytes = total
        .peak_replay_buffer_bytes
        .max(part.peak_replay_buffer_bytes);
    total.corrupt_records = total.corrupt_records.saturating_add(part.corrupt_records);
}

fn replay_lsn_order_error(
    path: &Path,
    record_offset: usize,
    lsn: u64,
    previous_lsn: u64,
) -> io::Error {
    invalid_replay_record_error(
        path,
        record_offset,
        Some(lsn),
        format!("AOF LSN order violation: previous={previous_lsn}, current={lsn}"),
    )
}

impl AofReader {
    /// Create a reader for the given AOF file path.
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }

    /// Check if the AOF file exists and is non-empty.
    pub fn exists(&self) -> bool {
        self.path
            .metadata()
            .map(|m| m.len() > AOF_HEADER_SIZE as u64)
            .unwrap_or(false)
    }

    /// Replay a single AOF file into a shared `ConcurrentKeyspace`.
    ///
    /// Handles both v1 (no LSN) and v2 (LSN-prefixed) formats.
    /// For v2, also tracks the highest LSN replayed.
    pub fn replay_into_keyspace(&self, keyspace: &ConcurrentKeyspace) -> io::Result<ReplayStats> {
        self.replay_into_keyspace_with_config(keyspace, AofReplayConfig::default())
    }

    /// Replay a single AOF file with an explicit bounded replay policy.
    pub fn replay_into_keyspace_with_config(
        &self,
        keyspace: &ConcurrentKeyspace,
        config: AofReplayConfig,
    ) -> io::Result<ReplayStats> {
        let start = Instant::now();

        if !self.path.exists() {
            return Ok(ReplayStats::empty());
        }

        let mut cursor = AofFileCursor::open(&self.path, 0, config)?;
        let header = cursor.header;

        let now_nanos = Timestamp::now().as_nanos();
        let unix_now_nanos = current_unix_time_nanos();
        let clock = CommandClock::new(now_nanos, unix_now_nanos);
        let _replay_guard = keyspace.enter_replay_mode();
        let mut commands_replayed = 0u64;
        let mut bytes_read = 0u64;
        let mut max_lsn: Option<AofLsn> = None;
        let mut peak_replay_buffer_bytes = cursor.current_buffer_bytes();
        let mut last_v2_lsn: Option<u64> = None;

        while let Some(record) = cursor.next_record()? {
            if let Some(replay_lsn) = record.restore_lsn {
                if let Some(previous_lsn) = last_v2_lsn {
                    if replay_lsn.get() <= previous_lsn {
                        return Err(replay_lsn_order_error(
                            &cursor.path,
                            record.record_offset,
                            replay_lsn.get(),
                            previous_lsn,
                        ));
                    }
                }
                last_v2_lsn = Some(replay_lsn.get());
                max_lsn = Some(max_lsn.map_or(replay_lsn, |max| max.max(replay_lsn)));
            }
            peak_replay_buffer_bytes = peak_replay_buffer_bytes.max(
                cursor
                    .current_buffer_bytes()
                    .saturating_add(record.payload.len()),
            );
            let tape = RespTape::parse_pipeline(&record.payload).map_err(|error| {
                replay_parse_error(
                    &cursor.path,
                    record.record_offset,
                    Some(record.order_lsn),
                    &error,
                )
            })?;
            let frame = tape.iter().next().ok_or_else(|| {
                replay_error(
                    &cursor.path,
                    record.record_offset,
                    Some(record.order_lsn),
                    None,
                    "record contained no command frame",
                )
            })?;
            commands_replayed += execute_replay_record(
                &cursor.path,
                keyspace,
                record.record_offset,
                Some(record.order_lsn),
                &frame,
                clock,
            )?;
            bytes_read = bytes_read.saturating_add(record.encoded_len as u64);
        }

        restore_lsn_after_replay(keyspace, max_lsn)?;

        let bytes_truncated = cursor.truncated_bytes as u64;

        if let Some(valid_data_len) = cursor.valid_data_len() {
            let valid_len = AOF_HEADER_SIZE as u64 + valid_data_len as u64;
            drop(cursor.file);
            truncate_partial_tail(&self.path, valid_len)?;
        }

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(ReplayStats {
            commands_replayed,
            bytes_read,
            bytes_truncated,
            duration_ms,
            reactor_id: header.reactor_id(),
            created_at: header.created_at(),
            max_lsn: max_lsn.map_or(0, AofLsn::get),
            max_persisted_lsn: max_lsn.map_or(0, AofLsn::get),
            files_merged: 1,
            peak_replay_buffer_bytes,
            corrupt_records: 0,
        })
    }

    /// K-Way merge replay of multiple per-reactor AOF files into a shared
    /// `ConcurrentKeyspace`, ordered by global LSN.
    ///
    /// Opens all AOF files, parses records lazily, and replays in strict
    /// LSN order using a `BinaryHeap` min-heap. This guarantees identical
    /// state regardless of the order files are read.
    ///
    /// Returns aggregate replay statistics. Restores the keyspace's global
    /// LSN counter to `max_lsn + 1` after replay.
    pub fn replay_merge(
        paths: &[PathBuf],
        keyspace: &ConcurrentKeyspace,
    ) -> io::Result<ReplayStats> {
        Self::replay_merge_with_config(paths, keyspace, AofReplayConfig::default())
    }

    /// K-Way merge replay with an explicit bounded replay policy.
    pub fn replay_merge_with_config(
        paths: &[PathBuf],
        keyspace: &ConcurrentKeyspace,
        config: AofReplayConfig,
    ) -> io::Result<ReplayStats> {
        let start = Instant::now();

        let mut cursors: Vec<AofFileCursor> = Vec::with_capacity(paths.len());
        let mut files_loaded = 0usize;

        for (idx, path) in paths.iter().enumerate() {
            if !path.exists() {
                continue;
            }
            let cursor = AofFileCursor::open(path, idx, config)?;
            cursors.push(cursor);
            files_loaded += 1;
        }

        if cursors.is_empty() {
            return Ok(ReplayStats::empty());
        }

        let now_nanos = Timestamp::now().as_nanos();
        let unix_now_nanos = current_unix_time_nanos();
        let clock = CommandClock::new(now_nanos, unix_now_nanos);
        let _replay_guard = keyspace.enter_replay_mode();

        let mut heap: BinaryHeap<MergeHeapItem> = BinaryHeap::new();
        let mut next_sequence = 0usize;
        for (ci, cursor) in cursors.iter_mut().enumerate() {
            match cursor.next_record() {
                Ok(Some(record)) => {
                    heap.push(MergeHeapItem {
                        cursor_idx: ci,
                        sequence: next_sequence,
                        record,
                    });
                    next_sequence = next_sequence.wrapping_add(1);
                }
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }

        let mut commands_replayed = 0u64;
        let mut total_bytes = 0u64;
        let mut max_lsn: Option<AofLsn> = None;
        let mut last_v2_lsn: Option<u64> = None;
        let mut peak_replay_buffer_bytes = current_replay_buffer_bytes(&cursors, &heap);

        // K-Way merge: always pop the smallest LSN, execute, advance that cursor.
        while let Some(item) = heap.pop() {
            let ci = item.cursor_idx;
            let record = item.record;
            if let Some(replay_lsn) = record.restore_lsn {
                if let Some(previous_lsn) = last_v2_lsn {
                    if replay_lsn.get() <= previous_lsn {
                        return Err(replay_lsn_order_error(
                            &cursors[ci].path,
                            record.record_offset,
                            replay_lsn.get(),
                            previous_lsn,
                        ));
                    }
                }
                last_v2_lsn = Some(replay_lsn.get());
                max_lsn = Some(max_lsn.map_or(replay_lsn, |max| max.max(replay_lsn)));
            }

            let tape = RespTape::parse_pipeline(&record.payload).map_err(|error| {
                replay_parse_error(
                    &cursors[ci].path,
                    record.record_offset,
                    Some(record.order_lsn),
                    &error,
                )
            })?;
            let frame = tape.iter().next().ok_or_else(|| {
                replay_error(
                    &cursors[ci].path,
                    record.record_offset,
                    Some(record.order_lsn),
                    None,
                    "record contained no command frame",
                )
            })?;
            commands_replayed += execute_replay_record(
                &cursors[ci].path,
                keyspace,
                record.record_offset,
                Some(record.order_lsn),
                &frame,
                clock,
            )?;
            total_bytes = total_bytes.saturating_add(record.encoded_len as u64);

            // Advance cursor and push next record to heap.
            match cursors[ci].next_record() {
                Ok(Some(record)) => {
                    heap.push(MergeHeapItem {
                        cursor_idx: ci,
                        sequence: next_sequence,
                        record,
                    });
                    next_sequence = next_sequence.wrapping_add(1);
                }
                Ok(None) => {}
                Err(error) => return Err(error),
            }
            peak_replay_buffer_bytes =
                peak_replay_buffer_bytes.max(current_replay_buffer_bytes(&cursors, &heap));
        }

        // Restore global LSN counter to the next value after the highest replayed
        // persisted LSN. v1 synthetic ordering keys are intentionally ignored.
        restore_lsn_after_replay(keyspace, max_lsn)?;

        for cursor in &cursors {
            if let Some(valid_data_len) = cursor.valid_data_len() {
                truncate_partial_tail(
                    &cursor.path,
                    AOF_HEADER_SIZE as u64 + valid_data_len as u64,
                )?;
            }
        }

        let duration_ms = start.elapsed().as_millis() as u64;
        let bytes_truncated = cursors
            .iter()
            .map(|cursor| cursor.truncated_bytes as u64)
            .sum();

        Ok(ReplayStats {
            commands_replayed,
            bytes_read: total_bytes,
            bytes_truncated,
            duration_ms,
            reactor_id: AofReactorId::from_u16(0),
            created_at: 0,
            max_lsn: max_lsn.map_or(0, AofLsn::get),
            max_persisted_lsn: max_lsn.map_or(0, AofLsn::get),
            files_merged: files_loaded,
            peak_replay_buffer_bytes,
            corrupt_records: 0,
        })
    }

    /// Replay the file set described by an AOF rewrite manifest.
    pub fn replay_manifest(
        manifest: &AofManifest,
        keyspace: &ConcurrentKeyspace,
    ) -> io::Result<ReplayStats> {
        match manifest.state() {
            AofManifestState::Preparing => Self::replay_merge(manifest.legacy_files(), keyspace),
            AofManifestState::Active => {
                let mut stats = ReplayStats::empty();
                for base in manifest.base_files() {
                    let base_stats = Self::new(base).replay_into_keyspace(keyspace)?;
                    merge_stats(&mut stats, base_stats);
                }
                let tail_stats = Self::replay_merge(manifest.tail_files(), keyspace)?;
                merge_stats(&mut stats, tail_stats);
                Ok(stats)
            }
        }
    }
}

/// Internal cursor for reading records from a single AOF file during K-Way merge.
struct AofFileCursor {
    /// Source file path used for diagnostics and truncation.
    path: PathBuf,
    /// Open file positioned after the AOF header.
    file: File,
    /// Header parsed from this file.
    header: AofHeader,
    /// Replay memory policy.
    config: AofReplayConfig,
    /// Total bytes in the data section, excluding the header.
    file_data_len: u64,
    /// Bytes already consumed from the data section.
    data_offset: u64,
    /// Bytes read from disk into the cursor buffer.
    file_data_read: u64,
    /// Buffered unread bytes, possibly with consumed prefix before `buffer_start`.
    buffer: Vec<u8>,
    /// Start index of unread data inside `buffer`.
    buffer_start: usize,
    /// Whether the file reached EOF.
    eof: bool,
    /// Whether this file uses v2 format (LSN-prefixed records).
    is_v2: bool,
    /// Base for synthetic LSN generation (v1 files only).
    /// Used only for merge ordering, never for restoring the engine LSN.
    synthetic_lsn_base: u64,
    /// Number of records read so far (for synthetic LSN generation).
    record_count: u64,
    /// Last v2 LSN observed in this file.
    last_v2_lsn: Option<u64>,
    /// Number of trailing bytes that were discarded as an incomplete record.
    truncated_bytes: usize,
    /// Start offset of the trailing partial record, if one was observed.
    truncated_record_offset: Option<usize>,
}

impl AofFileCursor {
    fn open(path: &Path, reactor_idx: usize, config: AofReplayConfig) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < AOF_HEADER_SIZE as u64 {
            return Err(aof_error(
                AofErrorKind::CorruptHeader,
                format!("AOF file too small for header: {}", path.display()),
            ));
        }
        let header = AofHeader::read_from(&mut file)?;
        let config = config.normalized();
        let file_data_len = file_len - AOF_HEADER_SIZE as u64;
        let initial_capacity = config.read_chunk_bytes.min(config.max_record_bytes);
        Ok(Self {
            path: path.to_path_buf(),
            file,
            header,
            config,
            file_data_len,
            data_offset: 0,
            file_data_read: 0,
            buffer: Vec::with_capacity(initial_capacity),
            buffer_start: 0,
            eof: file_data_len == 0,
            is_v2: header.is_v2(),
            synthetic_lsn_base: (reactor_idx as u64) << 48,
            record_count: 0,
            last_v2_lsn: None,
            truncated_bytes: 0,
            truncated_record_offset: None,
        })
    }

    fn valid_data_len(&self) -> Option<usize> {
        self.truncated_record_offset
    }

    fn current_buffer_bytes(&self) -> usize {
        self.buffer.len().saturating_sub(self.buffer_start)
    }

    fn unread(&self) -> &[u8] {
        &self.buffer[self.buffer_start..]
    }

    fn compact_if_needed(&mut self) {
        if self.buffer_start == 0 {
            return;
        }
        if self.buffer_start >= self.buffer.len() {
            self.buffer.clear();
            self.buffer_start = 0;
            return;
        }
        if self.buffer_start >= self.config.read_chunk_bytes
            || self.buffer_start.saturating_mul(2) >= self.buffer.len()
        {
            self.buffer.drain(..self.buffer_start);
            self.buffer_start = 0;
        }
    }

    fn consume(&mut self, bytes: usize) {
        self.data_offset = self.data_offset.saturating_add(bytes as u64);
        self.buffer_start = self.buffer_start.saturating_add(bytes);
        self.compact_if_needed();
    }

    fn record_too_large_error(&self, record_offset: usize) -> io::Error {
        invalid_replay_record_error(
            &self.path,
            record_offset,
            None,
            format!(
                "AOF record exceeds replay max_record_bytes {}",
                self.config.max_record_bytes
            ),
        )
    }

    fn mark_truncated_tail(&mut self, record_offset: usize) {
        self.truncated_bytes = self
            .file_data_len
            .saturating_sub(record_offset as u64)
            .min(usize::MAX as u64) as usize;
        self.truncated_record_offset = Some(record_offset);
        self.data_offset = self.file_data_len;
        self.buffer.clear();
        self.buffer_start = 0;
        self.eof = true;
    }

    fn fill_more_for_record(&mut self, record_offset: usize) -> io::Result<bool> {
        if self.eof {
            return Ok(false);
        }
        self.compact_if_needed();
        let unread_len = self.current_buffer_bytes();
        if unread_len >= self.config.max_record_bytes {
            return Err(self.record_too_large_error(record_offset));
        }
        let remaining_file = self.file_data_len.saturating_sub(self.file_data_read);
        if remaining_file == 0 {
            self.eof = true;
            return Ok(false);
        }
        let remaining_record_budget = self.config.max_record_bytes - unread_len;
        let read_len = self
            .config
            .read_chunk_bytes
            .min(remaining_record_budget)
            .min(remaining_file.min(usize::MAX as u64) as usize);
        if read_len == 0 {
            return Err(self.record_too_large_error(record_offset));
        }

        let old_len = self.buffer.len();
        self.buffer.resize(old_len + read_len, 0);
        let read = self.file.read(&mut self.buffer[old_len..])?;
        if read == 0 {
            self.buffer.truncate(old_len);
            self.eof = true;
            return Ok(false);
        }
        self.buffer.truncate(old_len + read);
        self.file_data_read = self.file_data_read.saturating_add(read as u64);
        Ok(true)
    }

    fn ensure_available(&mut self, needed: usize, record_offset: usize) -> io::Result<bool> {
        while self.current_buffer_bytes() < needed {
            if !self.fill_more_for_record(record_offset)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Read the next record.
    ///
    /// For v2 files: reads the 8-byte LSN prefix, then parses to find the
    /// RESP record boundary.
    /// For v1 files: assigns a synthetic monotonic LSN.
    fn next_record(&mut self) -> io::Result<Option<ReplayRecord>> {
        self.compact_if_needed();
        let record_offset = self.data_offset.min(usize::MAX as u64) as usize;
        if self.current_buffer_bytes() == 0 && !self.fill_more_for_record(record_offset)? {
            return Ok(None);
        }
        if self.current_buffer_bytes() == 0 {
            return Ok(None);
        }

        let (order_lsn, prefix_len, restore_lsn) = if self.is_v2 {
            if !self.ensure_available(LSN_SIZE, record_offset)? {
                self.mark_truncated_tail(record_offset);
                return Ok(None);
            }
            let lsn = u64::from_le_bytes(self.unread()[..LSN_SIZE].try_into().expect("8 bytes"));
            if let Some(previous_lsn) = self.last_v2_lsn {
                if lsn <= previous_lsn {
                    return Err(replay_lsn_order_error(
                        &self.path,
                        record_offset,
                        lsn,
                        previous_lsn,
                    ));
                }
            }
            let restore_lsn = AofLsn::try_from_raw(lsn)
                .map(Some)
                .map_err(|error| invalid_lsn_error(&self.path, record_offset, error))?;
            (lsn, LSN_SIZE, restore_lsn)
        } else {
            // v1: synthetic LSN preserving intra-file order and separating
            // per-reactor ranges to avoid collisions.
            let lsn = self.synthetic_lsn_base + self.record_count;
            (lsn, 0, None)
        };

        loop {
            let unread = self.unread();
            let resp = &unread[prefix_len..];
            match RespParser::parse(resp) {
                Ok((_, consumed)) if consumed > 0 => {
                    let encoded_len = prefix_len + consumed;
                    let payload = resp[..consumed].to_vec();
                    self.consume(encoded_len);
                    if self.is_v2 {
                        self.last_v2_lsn = Some(order_lsn);
                    } else {
                        self.record_count = self.record_count.saturating_add(1);
                    }
                    return Ok(Some(ReplayRecord {
                        order_lsn,
                        record_offset,
                        payload,
                        restore_lsn,
                        encoded_len,
                    }));
                }
                Ok(_) => {
                    self.mark_truncated_tail(record_offset);
                    return Ok(None);
                }
                Err(ParseError::NeedMoreData) => {
                    if self.eof || self.file_data_read >= self.file_data_len {
                        self.mark_truncated_tail(record_offset);
                        return Ok(None);
                    }
                    self.fill_more_for_record(record_offset)?;
                }
                Err(error) => {
                    return Err(replay_parse_error(
                        &self.path,
                        record_offset,
                        Some(order_lsn),
                        &error,
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::format::{AofFsyncPolicy, AofWriterMode};
    use crate::aof::writer::AofFileWriter;
    use vortex_common::{Timestamp, current_unix_time_nanos};
    use vortex_engine::EvictionPolicy;
    use vortex_engine::commands::{CmdResult, CommandClock, RESP_NIL, execute_command};
    use vortex_proto::frame::RespFrame;

    struct RecordedCommand {
        lsn: AofLsn,
        payload: Vec<u8>,
        side_effects: Vec<(AofLsn, Vec<u8>)>,
    }

    fn lsn<T>(raw: T) -> AofLsn
    where
        T: TryInto<u64>,
    {
        let raw = raw.try_into().ok().expect("test LSN converts to u64");
        AofLsn::try_from_raw(raw).expect("test LSN fits AOF range")
    }

    /// Helper: run GET via execute_command and return true if key exists.
    fn key_exists(ks: &ConcurrentKeyspace, key: &[u8]) -> bool {
        get_value(ks, key).is_some()
    }

    /// Helper: run GET and return the bulk string value.
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
        match execute_command(ks, b"GET", &frame, CommandClock::new(now, unix_now)) {
            Some(executed) => match executed.response {
                CmdResult::Static(s) if std::ptr::eq(s, RESP_NIL) => None,
                CmdResult::Inline(inline) => Some(inline.payload().to_vec()),
                CmdResult::Resp(RespFrame::BulkString(Some(bytes))) => Some(bytes.to_vec()),
                _ => None,
            },
            _ => None,
        }
    }

    fn temp_path(suffix: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut path = std::env::temp_dir();
        path.push(format!(
            "vortex-test-aof-reader-{}-{}-{}.aof",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
            suffix
        ));
        path
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
    }

    fn make_keyspace() -> ConcurrentKeyspace {
        ConcurrentKeyspace::new(64)
    }

    fn make_resp(parts: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            buf.extend_from_slice(part);
            buf.extend_from_slice(b"\r\n");
        }
        buf
    }

    fn record(bytes: &[u8]) -> crate::aof::AofRecordBytes<'_> {
        crate::aof::AofRecordBytes::from_resp(bytes)
    }

    fn record_live_command(
        keyspace: &ConcurrentKeyspace,
        wire: &[u8],
        clock: CommandClock,
    ) -> RecordedCommand {
        let tape = RespTape::parse_pipeline(wire).unwrap();
        let frame = tape.iter().next().unwrap();
        let name = frame.command_name().unwrap();
        keyspace.enable_aof_recording();
        let executed = execute_command(keyspace, name, &frame, clock).expect("command executes");
        keyspace.disable_aof_recording();
        let lsn = executed.aof_lsn().expect("mutation should allocate an LSN");

        let mut side_effects = Vec::new();
        if let Some(records) = executed.aof_records {
            for record in records {
                let payload = make_resp(&[b"DEL", record.key.as_bytes()]);
                side_effects.push((record.lsn, payload));
            }
        }

        let payload = if let Some(payload) = executed.aof_payload.as_deref() {
            payload.to_vec()
        } else {
            let mut scratch = vec![0u8; wire.len().saturating_add(64)];
            let written = loop {
                if let Some(written) = frame.write_resp_to(&mut scratch) {
                    break written;
                }
                scratch.resize(scratch.len() * 2, 0);
            };
            scratch[..written].to_vec()
        };

        RecordedCommand {
            lsn,
            payload,
            side_effects,
        }
    }

    fn append_recorded_command(writer: &mut AofFileWriter, recorded: RecordedCommand) {
        for (lsn, payload) in recorded.side_effects {
            writer.append_with_lsn(lsn, record(&payload)).unwrap();
        }
        writer
            .append_with_lsn(recorded.lsn, record(&recorded.payload))
            .unwrap();
    }

    fn append_live_command(
        writer: &mut AofFileWriter,
        keyspace: &ConcurrentKeyspace,
        wire: &[u8],
        clock: CommandClock,
    ) {
        append_recorded_command(writer, record_live_command(keyspace, wire, clock));
    }

    fn same_shard_keys(keyspace: &ConcurrentKeyspace, count: usize) -> Vec<Vec<u8>> {
        let mut found = Vec::with_capacity(count);
        let target = keyspace.shard_index(b"evict:seed");
        for index in 0..10_000usize {
            let key = format!("evict:{index:04}").into_bytes();
            if keyspace.shard_index(&key) != target {
                continue;
            }
            found.push(key);
            if found.len() == count {
                return found;
            }
        }
        panic!("failed to find {count} keys on shard {target}");
    }

    fn write_v1_aof(path: &Path, records: &[&[u8]]) {
        use std::io::Write;

        let mut file = File::create(path).unwrap();
        let header = AofHeader::new(AofReactorId::from_u16(0), AofWriterMode::SnapshotRewrite);
        header.write_to(&mut file).unwrap();
        for record in records {
            file.write_all(record).unwrap();
        }
        file.sync_all().unwrap();
    }

    #[test]
    fn replay_nonexistent_file() {
        let path = temp_path("nonexistent");
        let reader = AofReader::new(&path);
        let ks = make_keyspace();
        let stats = reader.replay_into_keyspace(&ks).unwrap();
        assert_eq!(stats.commands_replayed, 0);
    }

    #[test]
    fn replay_set_and_get() {
        let path = temp_path("set-get");
        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(
                    lsn(1),
                    record(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n"),
                )
                .unwrap();
            writer
                .append_with_lsn(
                    lsn(2),
                    record(b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n"),
                )
                .unwrap();
            writer.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks).unwrap();

        assert_eq!(stats.commands_replayed, 2);
        assert_eq!(stats.bytes_truncated, 0);
        assert_eq!(stats.max_lsn, 2);

        // Verify data restored via keyspace.
        assert!(key_exists(&ks, b"key1"));
        assert!(key_exists(&ks, b"key2"));

        cleanup(&path);
    }

    #[test]
    fn replay_lsn_zero_only_file_restores_next_lsn_to_one() {
        let path = temp_path("lsn-zero");
        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(lsn(0), record(b"*1\r\n$8\r\nFLUSHALL\r\n"))
                .unwrap();
            writer.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks).unwrap();

        assert_eq!(stats.commands_replayed, 1);
        assert_eq!(stats.max_lsn, 0);
        assert_eq!(ks.current_lsn(), 1);

        cleanup(&path);
    }

    #[test]
    fn replay_v1_synthetic_order_does_not_restore_lsn() {
        let path = temp_path("v1-synthetic-no-restore");
        write_v1_aof(
            &path,
            &[b"*3\r\n$3\r\nSET\r\n$6\r\nlegacy\r\n$5\r\nvalue\r\n"],
        );

        let ks = make_keyspace();
        let lsn_before_replay = ks.current_lsn();
        let stats = AofReader::replay_merge(std::slice::from_ref(&path), &ks).unwrap();

        assert_eq!(stats.commands_replayed, 1);
        assert_eq!(stats.max_lsn, 0);
        assert_eq!(stats.max_persisted_lsn, 0);
        assert_eq!(ks.current_lsn(), lsn_before_replay);
        assert_eq!(get_value(&ks, b"legacy").as_deref(), Some(&b"value"[..]));

        cleanup(&path);
    }

    #[test]
    fn replay_handles_truncated_file() {
        let path = temp_path("truncated");
        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(
                    lsn(1),
                    record(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n"),
                )
                .unwrap();
            writer.flush_buffer().unwrap();
        }

        // Append garbage (simulating a crash mid-write).
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            // Partial LSN + partial RESP.
            f.write_all(
                b"\x02\x00\x00\x00\x00\x00\x00\x00*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nval",
            )
            .unwrap();
        }

        let ks = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks).unwrap();

        assert_eq!(stats.commands_replayed, 1);
        assert!(stats.bytes_truncated > 0);

        // File should be truncated to last valid record.
        let file_len = std::fs::metadata(&path).unwrap().len();
        let expected = AOF_HEADER_SIZE as u64 + stats.bytes_read;
        assert_eq!(file_len, expected);

        cleanup(&path);
    }

    #[test]
    fn replay_del_command() {
        let path = temp_path("del");
        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(
                    lsn(1),
                    record(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n"),
                )
                .unwrap();
            writer
                .append_with_lsn(lsn(2), record(b"*2\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n"))
                .unwrap();
            writer.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks).unwrap();

        assert_eq!(stats.commands_replayed, 2);

        assert!(!key_exists(&ks, b"key1")); // Should be deleted.

        cleanup(&path);
    }

    #[test]
    fn replay_applies_eviction_records_before_triggering_write() {
        let path = temp_path("eviction-records");
        let source = make_keyspace();
        let keys = same_shard_keys(&source, 3);
        let hot = keys[0].clone();
        let cold = keys[1].clone();
        let incoming = keys[2].clone();
        let clock = CommandClock::new(1_000_000_000, current_unix_time_nanos());

        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            append_live_command(
                &mut writer,
                &source,
                &format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$4\r\nwarm\r\n",
                    hot.len(),
                    std::str::from_utf8(&hot).unwrap()
                )
                .into_bytes(),
                clock,
            );
            append_live_command(
                &mut writer,
                &source,
                &format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$4\r\ncool\r\n",
                    cold.len(),
                    std::str::from_utf8(&cold).unwrap()
                )
                .into_bytes(),
                clock,
            );
            source.configure_eviction(source.memory_used(), EvictionPolicy::AllKeysLru);
            for _ in 0..16 {
                let _ = get_value(&source, &hot);
            }
            append_live_command(
                &mut writer,
                &source,
                &format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$4\r\nmild\r\n",
                    incoming.len(),
                    std::str::from_utf8(&incoming).unwrap()
                )
                .into_bytes(),
                clock,
            );
            writer.flush_buffer().unwrap();
        }

        let replayed = make_keyspace();
        let stats = AofReader::new(&path)
            .replay_into_keyspace(&replayed)
            .unwrap();
        assert_eq!(stats.commands_replayed, 4);
        assert!(key_exists(&replayed, &hot));
        assert!(!key_exists(&replayed, &cold));
        assert!(key_exists(&replayed, &incoming));

        cleanup(&path);
    }

    #[test]
    fn replay_set_ex_does_not_extend_relative_ttl() {
        let path = temp_path("set-ex-relative");
        let source = make_keyspace();
        let replay_now_unix = current_unix_time_nanos();
        let live_clock = CommandClock::new(
            10_000 * 1_000_000_000,
            replay_now_unix.saturating_sub(120 * 1_000_000_000),
        );

        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            append_live_command(
                &mut writer,
                &source,
                b"*5\r\n$3\r\nSET\r\n$7\r\nsession\r\n$5\r\ntoken\r\n$2\r\nEX\r\n$2\r\n60\r\n",
                live_clock,
            );
            writer.flush_buffer().unwrap();
        }

        let replayed = make_keyspace();
        let stats = AofReader::new(&path)
            .replay_into_keyspace(&replayed)
            .unwrap();
        assert_eq!(stats.commands_replayed, 1);
        assert!(!key_exists(&replayed, b"session"));

        cleanup(&path);
    }

    #[test]
    fn replay_setex_does_not_extend_relative_ttl() {
        let path = temp_path("setex-relative");
        let source = make_keyspace();
        let replay_now_unix = current_unix_time_nanos();
        let live_clock = CommandClock::new(
            20_000 * 1_000_000_000,
            replay_now_unix.saturating_sub(120 * 1_000_000_000),
        );

        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            append_live_command(
                &mut writer,
                &source,
                b"*4\r\n$5\r\nSETEX\r\n$7\r\nsession\r\n$2\r\n60\r\n$5\r\ntoken\r\n",
                live_clock,
            );
            writer.flush_buffer().unwrap();
        }

        let replayed = make_keyspace();
        let stats = AofReader::new(&path)
            .replay_into_keyspace(&replayed)
            .unwrap();
        assert_eq!(stats.commands_replayed, 1);
        assert!(!key_exists(&replayed, b"session"));

        cleanup(&path);
    }

    #[test]
    fn replay_getex_ex_does_not_extend_relative_ttl() {
        let path = temp_path("getex-relative");
        let source = make_keyspace();
        let replay_now_unix = current_unix_time_nanos();
        let live_clock = CommandClock::new(
            30_000 * 1_000_000_000,
            replay_now_unix.saturating_sub(120 * 1_000_000_000),
        );

        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            append_live_command(
                &mut writer,
                &source,
                b"*3\r\n$3\r\nSET\r\n$7\r\nsession\r\n$5\r\ntoken\r\n",
                live_clock,
            );
            append_live_command(
                &mut writer,
                &source,
                b"*4\r\n$5\r\nGETEX\r\n$7\r\nsession\r\n$2\r\nEX\r\n$2\r\n60\r\n",
                live_clock,
            );
            writer.flush_buffer().unwrap();
        }

        let replayed = make_keyspace();
        let stats = AofReader::new(&path)
            .replay_into_keyspace(&replayed)
            .unwrap();
        assert_eq!(stats.commands_replayed, 2);
        assert!(!key_exists(&replayed, b"session"));

        cleanup(&path);
    }

    #[test]
    fn replay_expire_does_not_extend_relative_ttl() {
        let path = temp_path("expire-relative");
        let source = make_keyspace();
        let replay_now_unix = current_unix_time_nanos();
        let live_clock = CommandClock::new(
            40_000 * 1_000_000_000,
            replay_now_unix.saturating_sub(120 * 1_000_000_000),
        );

        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            append_live_command(
                &mut writer,
                &source,
                b"*3\r\n$3\r\nSET\r\n$7\r\nsession\r\n$5\r\ntoken\r\n",
                live_clock,
            );
            append_live_command(
                &mut writer,
                &source,
                b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nsession\r\n$2\r\n60\r\n",
                live_clock,
            );
            writer.flush_buffer().unwrap();
        }

        let replayed = make_keyspace();
        let stats = AofReader::new(&path)
            .replay_into_keyspace(&replayed)
            .unwrap();
        assert_eq!(stats.commands_replayed, 2);
        assert!(!key_exists(&replayed, b"session"));

        cleanup(&path);
    }

    #[test]
    fn replay_large_batch() {
        let path = temp_path("large-batch");
        let num_keys: u64 = 10_000;
        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            for i in 0..num_keys {
                let key = format!("key:{i:05}");
                let val = format!("val:{i:05}");
                let cmd = format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    val.len(),
                    val
                );
                writer
                    .append_with_lsn(lsn(i + 1), record(cmd.as_bytes()))
                    .unwrap();
            }
            writer.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks).unwrap();

        assert_eq!(stats.commands_replayed, num_keys);
        assert_eq!(stats.bytes_truncated, 0);
        assert_eq!(stats.max_lsn, num_keys);

        cleanup(&path);
    }

    #[test]
    fn replay_bypasses_noeviction_admission() {
        let path = temp_path("replay-noeviction");
        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(
                    lsn(1),
                    record(
                        b"*3\r\n$3\r\nSET\r\n$5\r\nlarge\r\n$32\r\n01234567890123456789012345678901\r\n",
                    ),
                )
                .unwrap();
            writer.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        ks.configure_eviction(1, EvictionPolicy::NoEviction);

        let stats = AofReader::new(&path).replay_into_keyspace(&ks).unwrap();
        assert_eq!(stats.commands_replayed, 1);
        assert_eq!(
            get_value(&ks, b"large").as_deref(),
            Some(&b"01234567890123456789012345678901"[..])
        );

        cleanup(&path);
    }

    #[test]
    fn replay_fails_on_command_error() {
        let path = temp_path("replay-command-error");
        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(lsn(7), record(b"*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n"))
                .unwrap();
            writer.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let error = AofReader::new(&path).replay_into_keyspace(&ks).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let message = error.to_string();
        assert!(message.contains("offset="));
        assert!(message.contains("lsn=7"));
        assert!(message.contains("command=SET"));

        cleanup(&path);
    }

    #[test]
    fn replay_incr_counter() {
        let path = temp_path("incr");
        {
            let mut writer =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(
                    lsn(1),
                    record(b"*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$1\r\n0\r\n"),
                )
                .unwrap();
            for i in 0..5u64 {
                writer
                    .append_with_lsn(lsn(i + 2), record(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"))
                    .unwrap();
            }
            writer.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks).unwrap();

        assert_eq!(stats.commands_replayed, 6);
        assert_eq!(stats.max_lsn, 6);

        cleanup(&path);
    }

    #[test]
    fn kway_merge_two_reactors() {
        let path0 = temp_path("merge-r0");
        let path1 = temp_path("merge-r1");

        // Reactor 0: LSN 1 (SET a 1), LSN 3 (SET c 3).
        {
            let mut w =
                AofFileWriter::open(&path0, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
                .unwrap();
            w.append_with_lsn(lsn(3), record(b"*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n3\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
        }

        // Reactor 1: LSN 2 (SET b 2), LSN 4 (SET d 4).
        {
            let mut w =
                AofFileWriter::open(&path1, AofReactorId::from_u16(1), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(2), record(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"))
                .unwrap();
            w.append_with_lsn(lsn(4), record(b"*3\r\n$3\r\nSET\r\n$1\r\nd\r\n$1\r\n4\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let paths = vec![path0.clone(), path1.clone()];
        let stats = AofReader::replay_merge(&paths, &ks).unwrap();

        assert_eq!(stats.commands_replayed, 4);
        assert_eq!(stats.max_lsn, 4);
        assert_eq!(stats.files_merged, 2);

        // Global LSN counter should be restored to max_lsn + 1.
        assert_eq!(ks.current_lsn(), 5);

        assert!(key_exists(&ks, b"a"));
        assert!(key_exists(&ks, b"b"));
        assert!(key_exists(&ks, b"c"));
        assert!(key_exists(&ks, b"d"));

        cleanup(&path0);
        cleanup(&path1);
    }

    #[test]
    fn kway_merge_ordering_matters() {
        // Two reactors both SET the same key — final value depends on LSN order.
        let path0 = temp_path("merge-order-r0");
        let path1 = temp_path("merge-order-r1");

        // Reactor 0: LSN 1 (SET x first).
        {
            let mut w =
                AofFileWriter::open(&path0, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(
                lsn(1),
                record(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$5\r\nfirst\r\n"),
            )
            .unwrap();
            w.flush_buffer().unwrap();
        }

        // Reactor 1: LSN 2 (SET x second) — should win.
        {
            let mut w =
                AofFileWriter::open(&path1, AofReactorId::from_u16(1), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(
                lsn(2),
                record(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$6\r\nsecond\r\n"),
            )
            .unwrap();
            w.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let paths = vec![path0.clone(), path1.clone()];
        let stats = AofReader::replay_merge(&paths, &ks).unwrap();

        assert_eq!(stats.commands_replayed, 2);

        let val = get_value(&ks, b"x").expect("key x should exist");
        assert_eq!(val, b"second");

        cleanup(&path0);
        cleanup(&path1);
    }

    #[test]
    fn replay_merge_preserves_post_flush_write_ordering() {
        let stale_key = b"stale";
        let stale_value = b"before-flush";
        let post_flush_key = b"survivor";
        let post_flush_value = b"post-flush";

        let path0 = temp_path("merge-flush-r0");
        let path1 = temp_path("merge-flush-r1");
        {
            let mut writer0 =
                AofFileWriter::open(&path0, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            writer0
                .append_with_lsn(
                    lsn(1),
                    record(&make_resp(&[b"SET", stale_key, stale_value])),
                )
                .unwrap();
            writer0
                .append_with_lsn(lsn(2), record(&make_resp(&[b"FLUSHALL"])))
                .unwrap();
            writer0.flush_buffer().unwrap();
        }
        {
            let mut writer1 =
                AofFileWriter::open(&path1, AofReactorId::from_u16(1), AofFsyncPolicy::No).unwrap();
            let post_flush_payload = make_resp(&[b"SET", post_flush_key, post_flush_value]);
            writer1
                .append_with_lsn(lsn(3), record(&post_flush_payload))
                .unwrap();
            writer1.flush_buffer().unwrap();
        }

        let replayed = make_keyspace();
        let stats = AofReader::replay_merge(&[path0.clone(), path1.clone()], &replayed).unwrap();

        assert_eq!(stats.commands_replayed, 3);
        assert_eq!(stats.files_merged, 2);
        assert_eq!(get_value(&replayed, stale_key), None);
        assert_eq!(
            get_value(&replayed, post_flush_key).as_deref(),
            Some(&post_flush_value[..])
        );
        assert_eq!(replayed.current_lsn(), stats.max_lsn + 1);

        cleanup(&path0);
        cleanup(&path1);
    }

    #[test]
    fn kway_merge_fails_on_command_error() {
        let path0 = temp_path("merge-error-r0");
        let path1 = temp_path("merge-error-r1");

        {
            let mut w =
                AofFileWriter::open(&path0, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
        }

        {
            let mut w =
                AofFileWriter::open(&path1, AofReactorId::from_u16(1), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(2), record(b"*2\r\n$3\r\nSET\r\n$1\r\nb\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let paths = vec![path0.clone(), path1.clone()];
        let error = AofReader::replay_merge(&paths, &ks).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let message = error.to_string();
        assert!(message.contains("lsn=2"));
        assert!(message.contains("command=SET"));
        assert!(message.contains(path1.file_name().unwrap().to_string_lossy().as_ref()));

        cleanup(&path0);
        cleanup(&path1);
    }

    #[test]
    fn kway_merge_truncates_trailing_partial_record() {
        let path0 = temp_path("merge-truncated-r0");
        let path1 = temp_path("merge-truncated-r1");

        {
            let mut w =
                AofFileWriter::open(&path0, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
        }

        let valid_len = {
            let mut w =
                AofFileWriter::open(&path1, AofReactorId::from_u16(1), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(2), record(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
            std::fs::metadata(&path1).unwrap().len()
        };

        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path1)
                .unwrap();
            file.write_all(b"\x03\x00\x00\x00\x00\x00\x00\x00*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n")
                .unwrap();
        }

        let ks = make_keyspace();
        let stats = AofReader::replay_merge(&[path0.clone(), path1.clone()], &ks).unwrap();

        assert_eq!(stats.commands_replayed, 2);
        assert!(stats.bytes_truncated > 0);
        assert_eq!(std::fs::metadata(&path1).unwrap().len(), valid_len);

        cleanup(&path0);
        cleanup(&path1);
    }

    #[test]
    fn kway_merge_streams_large_files_with_bounded_replay_buffers() {
        let path0 = temp_path("merge-bounded-r0");
        let path1 = temp_path("merge-bounded-r1");
        let record_count = 1_000u64;
        let config = AofReplayConfig {
            read_chunk_bytes: 128,
            max_record_bytes: 2 * 1024,
        };

        {
            let mut w =
                AofFileWriter::open(&path0, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            for i in (1..=record_count).step_by(2) {
                let key = format!("k:{i:04}");
                let val = format!("v:{i:04}");
                w.append_with_lsn(
                    lsn(i),
                    record(&make_resp(&[b"SET", key.as_bytes(), val.as_bytes()])),
                )
                .unwrap();
            }
            w.flush_buffer().unwrap();
        }
        {
            let mut w =
                AofFileWriter::open(&path1, AofReactorId::from_u16(1), AofFsyncPolicy::No).unwrap();
            for i in (2..=record_count).step_by(2) {
                let key = format!("k:{i:04}");
                let val = format!("v:{i:04}");
                w.append_with_lsn(
                    lsn(i),
                    record(&make_resp(&[b"SET", key.as_bytes(), val.as_bytes()])),
                )
                .unwrap();
            }
            w.flush_buffer().unwrap();
        }

        let total_data_bytes = std::fs::metadata(&path0).unwrap().len()
            + std::fs::metadata(&path1).unwrap().len()
            - (2 * AOF_HEADER_SIZE as u64);
        assert!(total_data_bytes > config.max_record_bytes as u64);

        let ks = make_keyspace();
        let stats =
            AofReader::replay_merge_with_config(&[path0.clone(), path1.clone()], &ks, config)
                .unwrap();

        assert_eq!(stats.commands_replayed, record_count);
        assert_eq!(stats.bytes_truncated, 0);
        assert_eq!(stats.bytes_read, total_data_bytes);
        assert_eq!(stats.max_persisted_lsn, record_count);
        assert!(stats.peak_replay_buffer_bytes < total_data_bytes as usize);
        assert!(
            stats.peak_replay_buffer_bytes
                <= (config.max_record_bytes + config.read_chunk_bytes) * 2
        );
        assert_eq!(get_value(&ks, b"k:1000").as_deref(), Some(&b"v:1000"[..]));

        cleanup(&path0);
        cleanup(&path1);
    }

    #[test]
    fn kway_merge_fails_on_duplicate_lsn() {
        let path0 = temp_path("merge-duplicate-r0");
        let path1 = temp_path("merge-duplicate-r1");

        {
            let mut w =
                AofFileWriter::open(&path0, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
        }
        {
            let mut w =
                AofFileWriter::open(&path1, AofReactorId::from_u16(1), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let error = AofReader::replay_merge(&[path0.clone(), path1.clone()], &ks).unwrap_err();

        assert_eq!(
            crate::aof::aof_error_kind(&error),
            Some(AofErrorKind::CorruptRecord)
        );
        assert!(error.to_string().contains("LSN order violation"));

        cleanup(&path0);
        cleanup(&path1);
    }

    #[test]
    fn kway_merge_fails_on_out_of_order_lsn() {
        let path = temp_path("merge-out-of-order");
        {
            let mut w =
                AofFileWriter::open(&path, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(2), record(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"))
                .unwrap();
            w.append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
        }

        let ks = make_keyspace();
        let error = AofReader::replay_merge(std::slice::from_ref(&path), &ks).unwrap_err();

        assert_eq!(
            crate::aof::aof_error_kind(&error),
            Some(AofErrorKind::CorruptRecord)
        );
        assert!(error.to_string().contains("LSN order violation"));

        cleanup(&path);
    }

    #[test]
    fn kway_merge_fails_on_unsupported_version() {
        use std::io::Write;

        let path = temp_path("merge-unsupported-version");
        let header = AofHeader::new(AofReactorId::from_u16(0), AofWriterMode::Journal);
        let mut bytes = header.to_bytes();
        bytes[6..8].copy_from_slice(&99u16.to_le_bytes());
        let mut file = File::create(&path).unwrap();
        file.write_all(&bytes).unwrap();
        file.write_all(&1u64.to_le_bytes()).unwrap();
        file.write_all(b"*1\r\n$8\r\nFLUSHALL\r\n").unwrap();
        file.sync_all().unwrap();

        let ks = make_keyspace();
        let error = AofReader::replay_merge(std::slice::from_ref(&path), &ks).unwrap_err();

        assert_eq!(
            crate::aof::aof_error_kind(&error),
            Some(AofErrorKind::UnsupportedFormat)
        );

        cleanup(&path);
    }

    #[test]
    fn kway_merge_fails_on_mid_file_resp_corruption() {
        let path0 = temp_path("merge-corrupt-r0");
        let path1 = temp_path("merge-corrupt-r1");

        {
            let mut w =
                AofFileWriter::open(&path0, AofReactorId::from_u16(0), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
        }

        {
            let mut w =
                AofFileWriter::open(&path1, AofReactorId::from_u16(1), AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(lsn(2), record(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"))
                .unwrap();
            w.flush_buffer().unwrap();
        }

        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path1)
                .unwrap();
            file.write_all(&3u64.to_le_bytes()).unwrap();
            file.write_all(b"!broken\r\n").unwrap();
            file.write_all(&4u64.to_le_bytes()).unwrap();
            file.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n3\r\n")
                .unwrap();
        }

        let ks = make_keyspace();
        let error = AofReader::replay_merge(&[path0.clone(), path1.clone()], &ks).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let message = error.to_string();
        assert!(message.contains("lsn=3"));
        assert!(message.contains(path1.file_name().unwrap().to_string_lossy().as_ref()));

        cleanup(&path0);
        cleanup(&path1);
    }

    #[test]
    fn kway_merge_empty_paths() {
        let ks = make_keyspace();
        let stats = AofReader::replay_merge(&[], &ks).unwrap();
        assert_eq!(stats.commands_replayed, 0);
        assert_eq!(stats.files_merged, 0);
    }

    #[test]
    fn kway_merge_missing_files() {
        let path0 = temp_path("merge-missing-r0");
        let path1 = temp_path("merge-missing-r1");
        // Neither file exists.

        let ks = make_keyspace();
        let paths = vec![path0, path1];
        let stats = AofReader::replay_merge(&paths, &ks).unwrap();
        assert_eq!(stats.commands_replayed, 0);
        assert_eq!(stats.files_merged, 0);
    }
}
