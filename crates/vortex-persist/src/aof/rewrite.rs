//! AOF rewrite (compaction).
//!
//! Rewrites the AOF by dumping the current shard state as a sequence of SET
//! commands. This collapses the entire mutation history into the minimal set
//! of commands needed to reconstruct the current state.
//!
//! ## Rewrite Strategy
//!
//! 1. Create a temporary AOF file (`{path}.rewrite.tmp`).
//! 2. Write the header.
//! 3. For each key in the shard, emit a SET command (with TTL if applicable).
//! 4. Atomically rename the temp file to the real AOF path.
//! 5. Swap the writer's underlying file.
//!
//! During rewrite, new mutations continue writing to the **old** AOF. After
//! the rewrite completes, the old AOF is replaced. Any mutations that
//! occurred during rewrite are already in the old AOF and will be replayed
//! if needed — but since the rewrite captures the state *at start of rewrite
//! plus continued mutations*, we use a two-phase approach: the reactor
//! continues writing to the old file, then swaps atomically.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use vortex_common::{Timestamp, VortexValue};
use vortex_engine::Shard;

use super::format::AofHeader;

/// Buffer size for the rewrite file (128 KB — larger since we write a batch).
const REWRITE_BUF_SIZE: usize = 128 * 1024;

/// AOF rewrite engine.
pub struct AofRewriter;

impl AofRewriter {
    /// Rewrite the AOF by dumping the current shard state.
    ///
    /// Returns the path to the new AOF file and the number of keys written.
    /// The caller should swap the writer to the new file.
    pub fn rewrite(shard: &Shard, aof_path: &Path, shard_id: u16) -> io::Result<(PathBuf, u64)> {
        let tmp_path = aof_path.with_extension("rewrite.tmp");

        // Create temp file with header.
        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::with_capacity(REWRITE_BUF_SIZE, file);

        let header = AofHeader::new(shard_id);
        header.write_to(&mut writer)?;

        let now_nanos = Timestamp::now().as_nanos();
        let mut keys_written = 0u64;

        // Iterate all slots in the Swiss Table.
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
                        // Emit the integer as its string representation.
                        let mut buf = itoa::Buffer::new();
                        let s = buf.format(*n);
                        Self::write_set_cmd(&mut writer, key_bytes.as_bytes(), s.as_bytes())?;
                    }
                    VortexValue::InlineString(_) | VortexValue::String(_) => {
                        let bytes = value.as_string_bytes().unwrap_or(b"");
                        Self::write_set_cmd(&mut writer, key_bytes.as_bytes(), bytes)?;
                    }
                    _ => {
                        // Skip non-string types for now (Phase 4 will extend).
                        continue;
                    }
                }

                // If the key has a TTL, emit PEXPIREAT.
                if ttl_nanos != 0 && ttl_nanos > now_nanos {
                    let deadline_ms = ttl_nanos / 1_000_000;
                    Self::write_pexpireat_cmd(&mut writer, key_bytes.as_bytes(), deadline_ms)?;
                }

                keys_written += 1;
            }
        }

        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);

        // Atomically rename temp → real.
        fs::rename(&tmp_path, aof_path)?;

        Ok((aof_path.to_path_buf(), keys_written))
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
    use crate::aof::reader::AofReader;
    use vortex_common::{ShardId, VortexKey, VortexValue};

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
    }

    #[test]
    fn rewrite_basic() {
        let path = temp_path("basic");
        let now = Timestamp::now().as_nanos();

        let mut shard = Shard::new_with_time(ShardId::new(0), now);
        shard.set(
            VortexKey::from(b"key1" as &[u8]),
            VortexValue::from_bytes(b"value1"),
        );
        shard.set(
            VortexKey::from(b"key2" as &[u8]),
            VortexValue::from_bytes(b"value2"),
        );

        let (_, keys) = AofRewriter::rewrite(&shard, &path, 0).unwrap();
        assert_eq!(keys, 2);

        // Replay the rewritten AOF into a fresh shard.
        let reader = AofReader::new(&path);
        let mut new_shard = Shard::new_with_time(ShardId::new(0), now);
        let stats = reader.replay(&mut new_shard).unwrap();
        assert_eq!(stats.commands_replayed, 2);
        assert_eq!(new_shard.len(), 2);

        cleanup(&path);
    }

    #[test]
    fn rewrite_with_ttl() {
        let path = temp_path("ttl");
        let now = Timestamp::now().as_nanos();
        let future = now + 60_000_000_000; // 60 seconds from now

        let mut shard = Shard::new_with_time(ShardId::new(0), now);
        shard.set_with_ttl(
            VortexKey::from(b"ephemeral" as &[u8]),
            VortexValue::from_bytes(b"data"),
            future,
        );
        shard.set(
            VortexKey::from(b"permanent" as &[u8]),
            VortexValue::from_bytes(b"data"),
        );

        let (_, keys) = AofRewriter::rewrite(&shard, &path, 0).unwrap();
        assert_eq!(keys, 2);

        // Replay — the ephemeral key should have a TTL (PEXPIREAT emitted).
        let reader = AofReader::new(&path);
        let mut new_shard = Shard::new_with_time(ShardId::new(0), now);
        let stats = reader.replay(&mut new_shard).unwrap();
        // 2 SETs + 1 PEXPIREAT = 3 commands
        assert_eq!(stats.commands_replayed, 3);
        assert_eq!(new_shard.len(), 2);

        cleanup(&path);
    }

    #[test]
    fn rewrite_integer_values() {
        let path = temp_path("integers");
        let now = Timestamp::now().as_nanos();

        let mut shard = Shard::new_with_time(ShardId::new(0), now);
        shard.set(
            VortexKey::from(b"counter" as &[u8]),
            VortexValue::Integer(42),
        );

        let (_, keys) = AofRewriter::rewrite(&shard, &path, 0).unwrap();
        assert_eq!(keys, 1);

        let reader = AofReader::new(&path);
        let mut new_shard = Shard::new_with_time(ShardId::new(0), now);
        let stats = reader.replay(&mut new_shard).unwrap();
        assert_eq!(stats.commands_replayed, 1);

        let key = VortexKey::from(b"counter" as &[u8]);
        let val = new_shard.get(&key, now).expect("counter should exist");
        // After SET "42", the value is stored as a string that can be parsed as integer
        // by INCR/DECR. The exact representation depends on value_from_bytes.
        assert!(val.as_string_bytes().is_some() || val.try_as_integer().is_some());

        cleanup(&path);
    }

    #[test]
    fn rewrite_compacts_history() {
        let path = temp_path("compact");
        let now = Timestamp::now().as_nanos();

        // Build shard with mutation history.
        let mut shard = Shard::new_with_time(ShardId::new(0), now);
        // Set then overwrite key1 many times — only final value matters.
        for i in 0..100 {
            shard.set(
                VortexKey::from(b"key1" as &[u8]),
                VortexValue::from_bytes(format!("val{i}").as_bytes()),
            );
        }
        // Set then delete key2 — should not appear in rewrite.
        shard.set(
            VortexKey::from(b"key2" as &[u8]),
            VortexValue::from_bytes(b"gone"),
        );
        shard.del(&VortexKey::from(b"key2" as &[u8]));

        // Shard should have 1 key.
        assert_eq!(shard.len(), 1);

        let (_, keys) = AofRewriter::rewrite(&shard, &path, 0).unwrap();
        assert_eq!(keys, 1); // Only key1 with final value.

        cleanup(&path);
    }
}
