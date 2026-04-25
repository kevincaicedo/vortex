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
//! assigned synthetic sequential LSNs (reactor_index × 2⁴⁸ + record_index).
//! This preserves intra-file ordering.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

use vortex_common::{Timestamp, current_unix_time_nanos};
use vortex_engine::ConcurrentKeyspace;
use vortex_engine::commands::{CmdResult, CommandClock, execute_command};
use vortex_proto::{FrameRef, ParseError, RespFrame, RespParser, RespTape};

use super::format::{AOF_HEADER_SIZE, AofHeader, LSN_SIZE};

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
    pub reactor_id: u16,
    /// Timestamp when the AOF was created.
    pub created_at: u64,
    /// Highest LSN replayed (used to restore global LSN counter).
    pub max_lsn: u64,
    /// Number of AOF files merged.
    pub files_merged: usize,
}

/// AOF file reader for replaying persistence on startup.
pub struct AofReader {
    /// Path to the AOF file.
    path: std::path::PathBuf,
}

struct ReplayRecordFailure {
    record_offset: usize,
    lsn: Option<u64>,
    error: ParseError,
}

type MergeHeapItem = Reverse<(u64, usize, usize, usize, usize)>;

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
    io::Error::new(io::ErrorKind::InvalidData, message)
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
) -> io::Result<()> {
    let name = frame.command_name().ok_or_else(|| {
        replay_error(
            path,
            record_offset,
            lsn,
            None,
            "record is not a command array",
        )
    })?;

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

    Ok(())
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
        let start = Instant::now();

        if !self.path.exists() {
            return Ok(ReplayStats {
                commands_replayed: 0,
                bytes_read: 0,
                bytes_truncated: 0,
                duration_ms: 0,
                reactor_id: 0,
                created_at: 0,
                max_lsn: 0,
                files_merged: 0,
            });
        }

        let mut file = File::open(&self.path)?;
        let file_len = file.metadata()?.len();

        if file_len < AOF_HEADER_SIZE as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "AOF file too small — missing header",
            ));
        }

        let header = AofHeader::read_from(&mut file)?;
        let is_v2 = header.version >= 2;

        let data_len = (file_len - AOF_HEADER_SIZE as u64) as usize;
        let mut data = vec![0u8; data_len];
        file.read_exact(&mut data)?;

        let now_nanos = Timestamp::now().as_nanos();
        let unix_now_nanos = current_unix_time_nanos();
        let clock = CommandClock::new(now_nanos, unix_now_nanos);
        let _replay_guard = keyspace.enter_replay_mode();
        let mut offset = 0usize;
        let mut commands_replayed = 0u64;
        let mut max_lsn = 0u64;

        while offset < data.len() {
            let record_offset = offset;
            let lsn = if is_v2 {
                if offset + LSN_SIZE > data.len() {
                    break;
                }
                let lsn = u64::from_le_bytes(
                    data[offset..offset + LSN_SIZE].try_into().expect("8 bytes"),
                );
                offset += LSN_SIZE;
                if lsn > max_lsn {
                    max_lsn = lsn;
                }
                Some(lsn)
            } else {
                None
            };

            let resp_start = offset;
            let frame_size = match RespParser::parse(&data[resp_start..]) {
                Ok((_, consumed)) if consumed > 0 => consumed,
                Ok(_) => break,
                Err(ParseError::NeedMoreData) => {
                    offset = record_offset;
                    break;
                }
                Err(error) => {
                    return Err(replay_parse_error(&self.path, record_offset, lsn, &error));
                }
            };

            let resp_end = resp_start + frame_size;
            let tape = RespTape::parse_pipeline(&data[resp_start..resp_end])
                .map_err(|error| replay_parse_error(&self.path, record_offset, lsn, &error))?;
            let frame = tape.iter().next().ok_or_else(|| {
                replay_error(
                    &self.path,
                    record_offset,
                    lsn,
                    None,
                    "record contained no command frame",
                )
            })?;
            execute_replay_record(&self.path, keyspace, record_offset, lsn, &frame, clock)?;
            commands_replayed += 1;
            offset = resp_end;
        }

        if max_lsn > 0 {
            keyspace.set_lsn(max_lsn + 1);
        }

        let bytes_truncated = (data.len() - offset) as u64;

        if bytes_truncated > 0 {
            let valid_len = AOF_HEADER_SIZE as u64 + offset as u64;
            drop(file);
            let f = File::options().write(true).open(&self.path)?;
            f.set_len(valid_len)?;
            f.sync_all()?;
        }

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(ReplayStats {
            commands_replayed,
            bytes_read: offset as u64,
            bytes_truncated,
            duration_ms,
            reactor_id: header.reactor_id,
            created_at: header.created_at,
            max_lsn,
            files_merged: 1,
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
        let start = Instant::now();

        // Load all files into memory and parse headers.
        let mut cursors: Vec<AofFileCursor> = Vec::with_capacity(paths.len());
        let mut files_loaded = 0usize;

        for (idx, path) in paths.iter().enumerate() {
            if !path.exists() {
                continue;
            }
            let meta = std::fs::metadata(path)?;
            if meta.len() <= AOF_HEADER_SIZE as u64 {
                continue;
            }

            let mut file = File::open(path)?;
            let header = AofHeader::read_from(&mut file)?;
            let data_len = (meta.len() - AOF_HEADER_SIZE as u64) as usize;
            let mut data = vec![0u8; data_len];
            file.read_exact(&mut data)?;

            cursors.push(AofFileCursor {
                data,
                offset: 0,
                reactor_idx: idx,
                is_v2: header.version >= 2,
                synthetic_lsn_base: (idx as u64) << 48,
                record_count: 0,
                truncated_bytes: 0,
            });
            files_loaded += 1;
        }

        if cursors.is_empty() {
            return Ok(ReplayStats {
                commands_replayed: 0,
                bytes_read: 0,
                bytes_truncated: 0,
                duration_ms: 0,
                reactor_id: 0,
                created_at: 0,
                max_lsn: 0,
                files_merged: 0,
            });
        }

        let now_nanos = Timestamp::now().as_nanos();
        let unix_now_nanos = current_unix_time_nanos();
        let clock = CommandClock::new(now_nanos, unix_now_nanos);
        let _replay_guard = keyspace.enter_replay_mode();

        // Seed the min-heap with the first record from each file.
        // Heap elements: Reverse((lsn, cursor_idx, record_offset, resp_start, resp_len))
        let mut heap: BinaryHeap<MergeHeapItem> = BinaryHeap::new();
        for (ci, cursor) in cursors.iter_mut().enumerate() {
            match cursor.next_record() {
                Ok(Some((lsn, record_offset, resp_start, resp_len))) => {
                    heap.push(Reverse((lsn, ci, record_offset, resp_start, resp_len)));
                }
                Ok(None) => {}
                Err(failure) => {
                    return Err(replay_parse_error(
                        &paths[ci],
                        failure.record_offset,
                        failure.lsn,
                        &failure.error,
                    ));
                }
            }
        }

        let mut commands_replayed = 0u64;
        let mut total_bytes = 0u64;
        let mut max_lsn = 0u64;

        // K-Way merge: always pop the smallest LSN, execute, advance that cursor.
        while let Some(Reverse((lsn, ci, record_offset, resp_start, resp_len))) = heap.pop() {
            if lsn > max_lsn {
                max_lsn = lsn;
            }

            // Parse and execute the RESP record.
            let resp_data = &cursors[ci].data[resp_start..resp_start + resp_len];
            let tape = RespTape::parse_pipeline(resp_data).map_err(|error| {
                replay_parse_error(&paths[ci], record_offset, Some(lsn), &error)
            })?;
            let frame = tape.iter().next().ok_or_else(|| {
                replay_error(
                    &paths[ci],
                    record_offset,
                    Some(lsn),
                    None,
                    "record contained no command frame",
                )
            })?;
            execute_replay_record(
                &paths[ci],
                keyspace,
                record_offset,
                Some(lsn),
                &frame,
                clock,
            )?;
            commands_replayed += 1;
            total_bytes += resp_len as u64;

            // Advance cursor and push next record to heap.
            match cursors[ci].next_record() {
                Ok(Some((next_lsn, next_record_offset, next_start, next_len))) => {
                    heap.push(Reverse((
                        next_lsn,
                        ci,
                        next_record_offset,
                        next_start,
                        next_len,
                    )));
                }
                Ok(None) => {}
                Err(failure) => {
                    return Err(replay_parse_error(
                        &paths[ci],
                        failure.record_offset,
                        failure.lsn,
                        &failure.error,
                    ));
                }
            }
        }

        // Restore global LSN counter to the next value after the highest replayed.
        if max_lsn > 0 {
            keyspace.set_lsn(max_lsn + 1);
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
            reactor_id: 0,
            created_at: 0,
            max_lsn,
            files_merged: files_loaded,
        })
    }
}

/// Internal cursor for reading records from a single AOF file during K-Way merge.
struct AofFileCursor {
    /// File data (excluding header).
    data: Vec<u8>,
    /// Current read offset.
    offset: usize,
    /// Index of this reactor in the paths array (used for tie-breaking).
    #[allow(dead_code)]
    reactor_idx: usize,
    /// Whether this file uses v2 format (LSN-prefixed records).
    is_v2: bool,
    /// Base for synthetic LSN generation (v1 files only).
    /// Uses `reactor_idx << 48` to ensure non-overlapping ranges.
    synthetic_lsn_base: u64,
    /// Number of records read so far (for synthetic LSN generation).
    record_count: u64,
    /// Number of trailing bytes that were discarded as an incomplete record.
    truncated_bytes: usize,
}

impl AofFileCursor {
    /// Read the next record, returning `(lsn, record_offset, resp_data_start, resp_data_len)`.
    ///
    /// For v2 files: reads the 8-byte LSN prefix, then parses to find the
    /// RESP record boundary.
    /// For v1 files: assigns a synthetic monotonic LSN.
    fn next_record(&mut self) -> Result<Option<(u64, usize, usize, usize)>, ReplayRecordFailure> {
        if self.offset >= self.data.len() {
            return Ok(None);
        }

        let record_offset = self.offset;

        let lsn = if self.is_v2 {
            if self.offset + LSN_SIZE > self.data.len() {
                self.truncated_bytes = self.data.len() - record_offset;
                self.offset = self.data.len();
                return Ok(None);
            }
            let lsn = u64::from_le_bytes(
                self.data[self.offset..self.offset + LSN_SIZE]
                    .try_into()
                    .expect("8 bytes"),
            );
            self.offset += LSN_SIZE;
            lsn
        } else {
            // v1: synthetic LSN preserving intra-file order and separating
            // per-reactor ranges to avoid collisions.
            let lsn = self.synthetic_lsn_base + self.record_count;
            self.record_count += 1;
            lsn
        };

        // Parse one RESP frame to find the record boundary.
        // Use RespParser::parse() which returns exactly one frame's consumed
        // size — critical for v2 where LSN bytes follow each RESP record.
        let resp_start = self.offset;
        match RespParser::parse(&self.data[self.offset..]) {
            Ok((_, consumed)) => {
                if consumed == 0 {
                    self.truncated_bytes = self.data.len() - record_offset;
                    self.offset = self.data.len();
                    return Ok(None);
                }
                self.offset += consumed;
                Ok(Some((lsn, record_offset, resp_start, consumed)))
            }
            Err(ParseError::NeedMoreData) => {
                self.truncated_bytes = self.data.len() - record_offset;
                self.offset = self.data.len();
                Ok(None)
            }
            Err(error) => Err(ReplayRecordFailure {
                record_offset,
                lsn: Some(lsn),
                error,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::format::AofFsyncPolicy;
    use crate::aof::writer::AofFileWriter;
    use vortex_common::{Timestamp, current_unix_time_nanos};
    use vortex_engine::EvictionPolicy;
    use vortex_engine::commands::{CmdResult, CommandClock, RESP_NIL, execute_command};
    use vortex_proto::frame::RespFrame;

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

    fn append_live_command(
        writer: &mut AofFileWriter,
        keyspace: &ConcurrentKeyspace,
        wire: &[u8],
        clock: CommandClock,
    ) {
        let tape = RespTape::parse_pipeline(wire).unwrap();
        let frame = tape.iter().next().unwrap();
        let name = frame.command_name().unwrap();
        keyspace.enable_aof_recording();
        let executed = execute_command(keyspace, name, &frame, clock).expect("command executes");
        keyspace.disable_aof_recording();
        let lsn = executed.aof_lsn().expect("mutation should allocate an LSN");
        if let Some(records) = executed.aof_records {
            for record in records {
                let payload = make_resp(&[b"DEL", record.key.as_bytes()]);
                writer.append_with_lsn(record.lsn, &payload).unwrap();
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

        writer.append_with_lsn(lsn, &payload).unwrap();
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(1, b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n")
                .unwrap();
            writer
                .append_with_lsn(2, b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n")
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
    fn replay_handles_truncated_file() {
        let path = temp_path("truncated");
        {
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(1, b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n")
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(1, b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n")
                .unwrap();
            writer
                .append_with_lsn(2, b"*2\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n")
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
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
                writer.append_with_lsn(i + 1, cmd.as_bytes()).unwrap();
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(
                    1,
                    b"*3\r\n$3\r\nSET\r\n$5\r\nlarge\r\n$32\r\n01234567890123456789012345678901\r\n",
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(7, b"*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n")
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
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(1, b"*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$1\r\n0\r\n")
                .unwrap();
            for i in 0..5u64 {
                writer
                    .append_with_lsn(i + 2, b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n")
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
            let mut w = AofFileWriter::open(&path0, 0, AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(1, b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n")
                .unwrap();
            w.append_with_lsn(3, b"*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n3\r\n")
                .unwrap();
            w.flush_buffer().unwrap();
        }

        // Reactor 1: LSN 2 (SET b 2), LSN 4 (SET d 4).
        {
            let mut w = AofFileWriter::open(&path1, 1, AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(2, b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n")
                .unwrap();
            w.append_with_lsn(4, b"*3\r\n$3\r\nSET\r\n$1\r\nd\r\n$1\r\n4\r\n")
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
            let mut w = AofFileWriter::open(&path0, 0, AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(1, b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$5\r\nfirst\r\n")
                .unwrap();
            w.flush_buffer().unwrap();
        }

        // Reactor 1: LSN 2 (SET x second) — should win.
        {
            let mut w = AofFileWriter::open(&path1, 1, AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(2, b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$6\r\nsecond\r\n")
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
    fn kway_merge_fails_on_command_error() {
        let path0 = temp_path("merge-error-r0");
        let path1 = temp_path("merge-error-r1");

        {
            let mut w = AofFileWriter::open(&path0, 0, AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(1, b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n")
                .unwrap();
            w.flush_buffer().unwrap();
        }

        {
            let mut w = AofFileWriter::open(&path1, 1, AofFsyncPolicy::No).unwrap();
            w.append_with_lsn(2, b"*2\r\n$3\r\nSET\r\n$1\r\nb\r\n")
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
