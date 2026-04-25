//! AOF rewrite (compaction).
//!
//! Rewrites the AOF by dumping the current keyspace state as a sequence of SET
//! commands. This collapses the entire mutation history into the minimal set
//! of commands needed to reconstruct the current state.
//!
//! ## Rewrite Strategy
//!
//! 1. Create a temporary AOF file (`{path}.rewrite.tmp`).
//! 2. Write the header.
//! 3. For each shard in the keyspace, emit SET commands for all live keys.
//! 4. Atomically rename the temp file to the real AOF path.
//! 5. Swap the writer's underlying file.
//!
//! During rewrite, new mutations continue writing to the **old** AOF. After
//! the rewrite completes, the old AOF is replaced. The rewrite file uses
//! the v1-style format (no LSN prefix) since it is a snapshot — LSN numbering
//! resumes from the current global LSN after the swap.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use vortex_common::{
    Timestamp, VortexValue, current_unix_time_nanos, deadline_nanos_to_absolute_unix_nanos,
};
use vortex_engine::ConcurrentKeyspace;

use super::format::AofHeader;

/// Buffer size for the rewrite file (128 KB — larger since we write a batch).
const REWRITE_BUF_SIZE: usize = 128 * 1024;

/// AOF rewrite engine.
pub struct AofRewriter;

impl AofRewriter {
    /// Rewrite the AOF by dumping the current keyspace state.
    ///
    /// Iterates all shards in the `ConcurrentKeyspace`, acquiring a read lock
    /// per shard. Returns the path to the new AOF file and the number of keys
    /// written. The caller should swap the writer to the new file.
    pub fn rewrite(
        keyspace: &ConcurrentKeyspace,
        aof_path: &Path,
        reactor_id: u16,
    ) -> io::Result<(PathBuf, u64)> {
        let tmp_path = aof_path.with_extension("rewrite.tmp");

        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::with_capacity(REWRITE_BUF_SIZE, file);

        // Rewrite files use v1 format (no LSN prefix per record) since they
        // are point-in-time snapshots. LSN numbering resumes from the current
        // global LSN after the writer swap.
        let mut header = AofHeader::new(reactor_id);
        header.version = 1;
        header.write_to(&mut writer)?;

        let now_nanos = Timestamp::now().as_nanos();
        let unix_now_nanos = current_unix_time_nanos();
        let mut keys_written = 0u64;

        // Iterate all shards, taking a read lock on each one at a time.
        for shard_idx in 0..keyspace.num_shards() {
            let shard = keyspace.read_shard_by_index(shard_idx);
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
                        let deadline_ms = deadline_nanos_to_absolute_unix_nanos(
                            ttl_nanos,
                            now_nanos,
                            unix_now_nanos,
                        ) / 1_000_000;
                        Self::write_pexpireat_cmd(&mut writer, key_bytes.as_bytes(), deadline_ms)?;
                    }

                    keys_written += 1;
                }
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
    use vortex_engine::commands::{CommandClock, execute_command};
    use vortex_proto::RespTape;

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

    #[test]
    fn rewrite_basic() {
        let path = temp_path("basic");
        let ks = make_keyspace();

        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n");
        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n");

        let (_, keys) = AofRewriter::rewrite(&ks, &path, 0).unwrap();
        assert_eq!(keys, 2);

        // Replay the rewritten AOF into a fresh keyspace.
        let ks2 = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks2).unwrap();
        assert_eq!(stats.commands_replayed, 2);

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

        let (_, keys) = AofRewriter::rewrite(&ks, &path, 0).unwrap();
        assert_eq!(keys, 2);

        // Replay — 2 SETs + 1 PEXPIREAT = 3 commands.
        let ks2 = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks2).unwrap();
        assert_eq!(stats.commands_replayed, 3);

        cleanup(&path);
    }

    #[test]
    fn rewrite_integer_values() {
        let path = temp_path("integers");
        let ks = make_keyspace();

        run_cmd(&ks, b"*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$2\r\n42\r\n");

        let (_, keys) = AofRewriter::rewrite(&ks, &path, 0).unwrap();
        assert_eq!(keys, 1);

        let ks2 = make_keyspace();
        let reader = AofReader::new(&path);
        let stats = reader.replay_into_keyspace(&ks2).unwrap();
        assert_eq!(stats.commands_replayed, 1);

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

        let (_, keys) = AofRewriter::rewrite(&ks, &path, 0).unwrap();
        assert_eq!(keys, 1); // Only key1 with final value.

        cleanup(&path);
    }
}
