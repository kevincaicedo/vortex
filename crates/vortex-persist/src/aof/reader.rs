//! AOF reader and replay engine.
//!
//! Reads a VortexDB AOF file and replays mutation commands into a [`Shard`]
//! to restore state after restart. Handles truncated files (crash during
//! write) by silently discarding the incomplete trailing record.
//!
//! ## Replay Strategy
//!
//! The AOF stores raw RESP bytes. We parse them with the same SIMD-accelerated
//! RESP parser used for live client traffic, then route through
//! `execute_command()`. This guarantees identical semantics between live
//! and replayed commands.

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::time::Instant;

use vortex_common::Timestamp;
use vortex_engine::Shard;
use vortex_engine::commands::execute_command;
use vortex_proto::RespTape;

use super::format::{AOF_HEADER_SIZE, AofHeader};

/// Statistics from an AOF replay.
#[derive(Debug, Clone)]
pub struct ReplayStats {
    /// Number of commands successfully replayed.
    pub commands_replayed: u64,
    /// Number of bytes read from the AOF (excluding header).
    pub bytes_read: u64,
    /// Number of bytes discarded (truncated trailing record).
    pub bytes_truncated: u64,
    /// Wall-clock duration of the replay.
    pub duration_ms: u64,
    /// Shard ID from the AOF header.
    pub shard_id: u16,
    /// Timestamp when the AOF was created.
    pub created_at: u64,
}

/// AOF file reader for replaying persistence on startup.
pub struct AofReader {
    /// Path to the AOF file.
    path: std::path::PathBuf,
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

    /// Replay the AOF file into the given shard.
    ///
    /// Returns replay statistics. If the file doesn't exist, returns
    /// stats with zero commands. If the file is truncated, replays all
    /// valid commands and reports the truncated bytes.
    pub fn replay(&self, shard: &mut Shard) -> io::Result<ReplayStats> {
        let start = Instant::now();

        if !self.path.exists() {
            return Ok(ReplayStats {
                commands_replayed: 0,
                bytes_read: 0,
                bytes_truncated: 0,
                duration_ms: 0,
                shard_id: 0,
                created_at: 0,
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

        // Read and validate header.
        let header = AofHeader::read_from(&mut file)?;

        // Read all record data into memory for fast parsing.
        // For very large AOFs (>1 GB), a streaming approach would be better,
        // but for alpha this is simpler and fast enough (mmap would be ideal).
        let data_len = (file_len - AOF_HEADER_SIZE as u64) as usize;
        let mut data = vec![0u8; data_len];
        file.read_exact(&mut data)?;

        let now_nanos = Timestamp::now().as_nanos();
        let mut offset = 0usize;
        let mut commands_replayed = 0u64;

        while offset < data.len() {
            match RespTape::parse_pipeline(&data[offset..]) {
                Ok(tape) => {
                    let consumed = tape.consumed();
                    if consumed == 0 {
                        // Safety: prevent infinite loop on zero-consume parse.
                        break;
                    }
                    for frame in tape.iter() {
                        // Route through the engine — same path as live commands.
                        let name = match frame.command_name() {
                            Some(n) => n,
                            None => continue,
                        };
                        let _ = execute_command(shard, name, &frame, now_nanos);
                        commands_replayed += 1;
                    }
                    offset += consumed;
                }
                Err(vortex_proto::ParseError::NeedMoreData) => {
                    // Truncated trailing record — discard remaining bytes.
                    break;
                }
                Err(_) => {
                    // Invalid data — stop replay at last valid record.
                    break;
                }
            }
        }

        let bytes_truncated = (data.len() - offset) as u64;

        // If there were truncated bytes, truncate the file to the last valid record.
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
            shard_id: header.shard_id,
            created_at: header.created_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::format::AofFsyncPolicy;
    use crate::aof::writer::AofFileWriter;
    use vortex_common::{ShardId, VortexKey};

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

    #[test]
    fn replay_nonexistent_file() {
        let path = temp_path("nonexistent");
        let reader = AofReader::new(&path);
        let mut shard = Shard::new(ShardId::new(0));
        let stats = reader.replay(&mut shard).unwrap();
        assert_eq!(stats.commands_replayed, 0);
    }

    #[test]
    fn replay_set_and_get() {
        let path = temp_path("set-get");
        // Write AOF with SET commands.
        {
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
            // SET key1 value1
            writer
                .append(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n")
                .unwrap();
            // SET key2 value2
            writer
                .append(b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n")
                .unwrap();
            writer.flush_buffer().unwrap();
        }

        // Replay into a fresh shard.
        let reader = AofReader::new(&path);
        let mut shard = Shard::new_with_time(ShardId::new(0), Timestamp::now().as_nanos());
        let stats = reader.replay(&mut shard).unwrap();

        assert_eq!(stats.commands_replayed, 2);
        assert_eq!(stats.bytes_truncated, 0);

        // Verify data is restored.
        let now = Timestamp::now().as_nanos();
        let key1 = VortexKey::from(b"key1" as &[u8]);
        let key2 = VortexKey::from(b"key2" as &[u8]);
        assert!(shard.get(&key1, now).is_some());
        assert!(shard.get(&key2, now).is_some());

        cleanup(&path);
    }

    #[test]
    fn replay_handles_truncated_file() {
        let path = temp_path("truncated");
        // Write a valid AOF then corrupt the end.
        {
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
            writer
                .append(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n")
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
            f.write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nval")
                .unwrap();
        }

        let reader = AofReader::new(&path);
        let mut shard = Shard::new_with_time(ShardId::new(0), Timestamp::now().as_nanos());
        let stats = reader.replay(&mut shard).unwrap();

        // First command should replay, truncated second should be discarded.
        assert_eq!(stats.commands_replayed, 1);
        assert!(stats.bytes_truncated > 0);

        // The file should be truncated to the valid length.
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
            // SET key1 value1
            writer
                .append(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n")
                .unwrap();
            // DEL key1
            writer.append(b"*2\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n").unwrap();
            writer.flush_buffer().unwrap();
        }

        let reader = AofReader::new(&path);
        let mut shard = Shard::new_with_time(ShardId::new(0), Timestamp::now().as_nanos());
        let stats = reader.replay(&mut shard).unwrap();

        assert_eq!(stats.commands_replayed, 2);

        let now = Timestamp::now().as_nanos();
        let key1 = VortexKey::from(b"key1" as &[u8]);
        assert!(shard.get(&key1, now).is_none()); // Should be deleted.

        cleanup(&path);
    }

    #[test]
    fn replay_large_batch() {
        let path = temp_path("large-batch");
        let num_keys = 10_000;
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
                writer.append(cmd.as_bytes()).unwrap();
            }
            writer.flush_buffer().unwrap();
        }

        let reader = AofReader::new(&path);
        let mut shard = Shard::new_with_time(ShardId::new(0), Timestamp::now().as_nanos());
        let stats = reader.replay(&mut shard).unwrap();

        assert_eq!(stats.commands_replayed, num_keys);
        assert_eq!(stats.bytes_truncated, 0);
        assert_eq!(shard.len(), num_keys as usize);

        cleanup(&path);
    }

    #[test]
    fn replay_incr_counter() {
        let path = temp_path("incr");
        {
            let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
            // SET counter 0
            writer
                .append(b"*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$1\r\n0\r\n")
                .unwrap();
            // INCR counter (×5)
            for _ in 0..5 {
                writer
                    .append(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n")
                    .unwrap();
            }
            writer.flush_buffer().unwrap();
        }

        let reader = AofReader::new(&path);
        let mut shard = Shard::new_with_time(ShardId::new(0), Timestamp::now().as_nanos());
        let stats = reader.replay(&mut shard).unwrap();

        assert_eq!(stats.commands_replayed, 6);

        let now = Timestamp::now().as_nanos();
        let key = VortexKey::from(b"counter" as &[u8]);
        let val = shard.get(&key, now).expect("counter should exist");
        assert_eq!(val.try_as_integer(), Some(5));

        cleanup(&path);
    }
}
