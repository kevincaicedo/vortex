//! AOF file writer — buffered, fsync-aware, single-threaded per reactor.
//!
//! The writer is owned by the reactor thread and called inline after each
//! mutation command. Performance is critical: the hot path is a single
//! `BufWriter::write_all()` — a memcpy into the 64 KB userspace buffer.
//!
//! Fsync is controlled by [`AofFsyncPolicy`]:
//! - `Always`: fsync after every mutation (reactor calls `sync()` inline).
//! - `Everysec`: the reactor flushes dirty bytes to the kernel once per second,
//!   then a tiny helper thread performs `sync_data()` off the hot path.
//! - `No`: never explicitly fsync — the OS flushes dirty pages on its own.
//!
//! ## Buffer Size Rationale
//!
//! 64 KB matches the typical Linux `write()` chunk that avoids short writes
//! and aligns with common SSD page sizes. At 1M mutations/sec averaging
//! 100 bytes each, the buffer absorbs ~640 mutations per flush — well within
//! the fsync cadence for `everysec`.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use super::format::{AofFsyncPolicy, AofHeader};

/// Userspace write buffer size (64 KB).
const AOF_BUF_SIZE: usize = 64 * 1024;

struct AsyncFsyncWorker {
    request_tx: SyncSender<()>,
    done_rx: Receiver<io::Result<()>>,
    handle: Option<JoinHandle<()>>,
    inflight: bool,
    syncing_writes: u64,
}

impl AsyncFsyncWorker {
    fn new(file: &File) -> io::Result<Self> {
        let sync_file = file.try_clone()?;
        let (request_tx, request_rx) = sync_channel::<()>(1);
        let (done_tx, done_rx) = sync_channel::<io::Result<()>>(1);
        let handle = thread::spawn(move || {
            while request_rx.recv().is_ok() {
                let result = sync_file.sync_data();
                if done_tx.send(result).is_err() {
                    break;
                }
            }
        });

        Ok(Self {
            request_tx,
            done_rx,
            handle: Some(handle),
            inflight: false,
            syncing_writes: 0,
        })
    }

    fn request_sync(&mut self, syncing_writes: u64) -> io::Result<()> {
        if self.inflight || syncing_writes == 0 {
            return Ok(());
        }

        self.request_tx.try_send(()).map_err(|err| match err {
            std::sync::mpsc::TrySendError::Full(()) => {
                io::Error::new(io::ErrorKind::WouldBlock, "AOF fsync already inflight")
            }
            std::sync::mpsc::TrySendError::Disconnected(()) => {
                io::Error::new(io::ErrorKind::BrokenPipe, "AOF fsync worker stopped")
            }
        })?;

        self.inflight = true;
        self.syncing_writes = syncing_writes;
        Ok(())
    }

    fn poll(&mut self) -> io::Result<Option<u64>> {
        if !self.inflight {
            return Ok(None);
        }

        match self.done_rx.try_recv() {
            Ok(result) => {
                let syncing_writes = self.syncing_writes;
                self.inflight = false;
                self.syncing_writes = 0;
                result?;
                Ok(Some(syncing_writes))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF fsync worker stopped",
            )),
        }
    }

    fn wait(&mut self) -> io::Result<Option<u64>> {
        if !self.inflight {
            return Ok(None);
        }

        let syncing_writes = self.syncing_writes;
        self.inflight = false;
        self.syncing_writes = 0;

        match self.done_rx.recv() {
            Ok(result) => {
                result?;
                Ok(Some(syncing_writes))
            }
            Err(_) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "AOF fsync worker stopped",
            )),
        }
    }

    fn is_inflight(&self) -> bool {
        self.inflight
    }

    fn shutdown(mut self) {
        drop(self.request_tx);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// AOF file writer with buffered I/O and configurable fsync.
///
/// Owned by a single reactor thread — no synchronization needed.
pub struct AofFileWriter {
    /// Buffered file writer.
    writer: BufWriter<File>,
    /// Fsync policy.
    policy: AofFsyncPolicy,
    /// Monotonic timestamp of the last fsync (for `everysec` mode).
    last_fsync: Instant,
    /// Number of mutations written since last fsync.
    pending_writes: u64,
    /// Total mutations written to this AOF since open.
    total_writes: u64,
    /// Total bytes written (including header).
    total_bytes: u64,
    /// File path (for rewrite/rename operations).
    path: PathBuf,
    /// Async fsync worker used by `everysec` to keep sync stalls off-reactor.
    async_fsync: Option<AsyncFsyncWorker>,
}

impl AofFileWriter {
    /// Open or create an AOF file, writing the header if the file is new.
    ///
    /// If the file already exists and has content, we append to it (the header
    /// is already present). If it's empty or doesn't exist, we write a fresh
    /// header.
    pub fn open(path: &Path, shard_id: u16, policy: AofFsyncPolicy) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        let file_len = file.metadata()?.len();
        let mut writer = BufWriter::with_capacity(AOF_BUF_SIZE, file);

        if file_len == 0 {
            // New file — write header.
            let header = AofHeader::new(shard_id);
            header.write_to(&mut writer)?;
            writer.flush()?;
        }

        let async_fsync = if policy == AofFsyncPolicy::Everysec {
            Some(AsyncFsyncWorker::new(writer.get_ref())?)
        } else {
            None
        };

        let total_bytes = if file_len == 0 {
            super::format::AOF_HEADER_SIZE as u64
        } else {
            file_len
        };

        Ok(Self {
            writer,
            policy,
            last_fsync: Instant::now(),
            pending_writes: 0,
            total_writes: 0,
            total_bytes,
            path: path.to_path_buf(),
            async_fsync,
        })
    }

    fn poll_async_fsync(&mut self) -> io::Result<()> {
        if let Some(worker) = self.async_fsync.as_mut() {
            if let Some(synced_writes) = worker.poll()? {
                self.pending_writes = self.pending_writes.saturating_sub(synced_writes);
                self.last_fsync = Instant::now();
            }
        }

        Ok(())
    }

    fn wait_async_fsync(&mut self) -> io::Result<()> {
        if let Some(worker) = self.async_fsync.as_mut() {
            if let Some(synced_writes) = worker.wait()? {
                self.pending_writes = self.pending_writes.saturating_sub(synced_writes);
                self.last_fsync = Instant::now();
            }
        }

        Ok(())
    }

    /// Append raw RESP bytes of a mutation command to the AOF.
    ///
    /// This is the **hot path** — called after every mutation. It must be
    /// as cheap as possible: a single memcpy into the BufWriter's buffer.
    /// No allocation, no formatting, no serialization.
    ///
    /// `resp_bytes` is the exact RESP wire representation of the command,
    /// e.g., `*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n`.
    #[inline]
    pub fn append(&mut self, resp_bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(resp_bytes)?;
        self.pending_writes += 1;
        self.total_writes += 1;
        self.total_bytes += resp_bytes.len() as u64;

        if self.policy == AofFsyncPolicy::Always {
            self.flush_and_sync()?;
        }

        Ok(())
    }

    /// Conditionally fsync based on policy and elapsed time.
    ///
    /// Called once per event-loop iteration by the reactor. For `everysec`
    /// mode, this fsyncs at most once per second. For other modes, this is
    /// a no-op.
    #[inline]
    pub fn maybe_fsync(&mut self) -> io::Result<()> {
        self.poll_async_fsync()?;

        if self.policy != AofFsyncPolicy::Everysec || self.pending_writes == 0 {
            return Ok(());
        }

        if self.last_fsync.elapsed().as_secs() < 1 {
            return Ok(());
        }

        if self
            .async_fsync
            .as_ref()
            .is_some_and(AsyncFsyncWorker::is_inflight)
        {
            return Ok(());
        }

        self.writer.flush()?;

        if let Some(worker) = self.async_fsync.as_mut() {
            worker.request_sync(self.pending_writes)?;
        } else {
            self.writer.get_ref().sync_data()?;
            self.last_fsync = Instant::now();
            self.pending_writes = 0;
        }

        Ok(())
    }

    /// Flush the userspace buffer and fsync to durable storage.
    pub fn flush_and_sync(&mut self) -> io::Result<()> {
        self.poll_async_fsync()?;
        self.wait_async_fsync()?;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.last_fsync = Instant::now();
        self.pending_writes = 0;
        Ok(())
    }

    /// Flush the userspace buffer (without fsync).
    pub fn flush_buffer(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Returns the total number of mutations written.
    pub fn total_writes(&self) -> u64 {
        self.total_writes
    }

    /// Returns the total bytes written to the AOF file.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Returns the number of pending (unfsynced) writes.
    pub fn pending_writes(&self) -> u64 {
        self.pending_writes
    }

    /// Returns the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the fsync policy.
    pub fn policy(&self) -> AofFsyncPolicy {
        self.policy
    }

    /// Replace the underlying file (used by AOF rewrite).
    ///
    /// The caller is responsible for writing the header to the new file
    /// before calling this. Flushes and syncs the current file first.
    pub fn swap_file(&mut self, new_path: &Path) -> io::Result<()> {
        // Flush and sync current file.
        self.flush_and_sync()?;

        if let Some(worker) = self.async_fsync.take() {
            worker.shutdown();
        }

        // Open new file in append mode.
        let new_file = OpenOptions::new().append(true).open(new_path)?;
        let new_len = new_file.metadata()?.len();

        // Replace writer.
        self.writer = BufWriter::with_capacity(AOF_BUF_SIZE, new_file);
        self.async_fsync = if self.policy == AofFsyncPolicy::Everysec {
            Some(AsyncFsyncWorker::new(self.writer.get_ref())?)
        } else {
            None
        };
        self.total_bytes = new_len;
        self.pending_writes = 0;
        self.path = new_path.to_path_buf();

        Ok(())
    }
}

impl Drop for AofFileWriter {
    fn drop(&mut self) {
        if let Some(worker) = self.async_fsync.take() {
            worker.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::format::AOF_HEADER_SIZE;
    use std::io::Read;

    fn temp_aof_path() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut path = std::env::temp_dir();
        path.push(format!(
            "vortex-test-aof-{}-{}.aof",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        path
    }

    #[test]
    fn create_new_aof() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::No).unwrap();
        assert_eq!(writer.total_writes(), 0);
        assert_eq!(writer.total_bytes(), AOF_HEADER_SIZE as u64);
        drop(writer);

        // Verify header was written.
        let mut file = File::open(&path).unwrap();
        let header = AofHeader::read_from(&mut file).unwrap();
        assert_eq!(header.shard_id, 0);
    }

    #[test]
    fn append_and_read_back() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, 7, AofFsyncPolicy::No).unwrap();

        let cmd1 = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n";
        let cmd2 = b"*2\r\n$3\r\nDEL\r\n$1\r\na\r\n";
        writer.append(cmd1).unwrap();
        writer.append(cmd2).unwrap();
        writer.flush_buffer().unwrap();
        drop(writer);

        assert_eq!(writer_total_writes_after_drop(&path, cmd1, cmd2), 2);
    }

    #[test]
    fn append_reopens_existing() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        // Write first session.
        {
            let mut writer = AofFileWriter::open(&path, 3, AofFsyncPolicy::No).unwrap();
            writer.append(b"*1\r\n$4\r\nPING\r\n").unwrap();
            writer.flush_buffer().unwrap();
        }

        // Reopen and append more.
        {
            let mut writer = AofFileWriter::open(&path, 3, AofFsyncPolicy::No).unwrap();
            writer.append(b"*2\r\n$3\r\nDEL\r\n$1\r\nx\r\n").unwrap();
            writer.flush_buffer().unwrap();
        }

        // Read entire file.
        let mut data = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut data).unwrap();

        // Should have exactly one header (16 bytes) + both commands.
        // PING: *1\r\n$4\r\nPING\r\n = 14 bytes
        // DEL x: *2\r\n$3\r\nDEL\r\n$1\r\nx\r\n = 20 bytes
        assert_eq!(data.len(), AOF_HEADER_SIZE + 14 + 20);
        assert_eq!(&data[0..6], b"VXAOF\x00");
    }

    #[test]
    fn always_fsync_policy() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::Always).unwrap();
        writer.append(b"*1\r\n$4\r\nPING\r\n").unwrap();
        // After Always append, pending_writes should be 0 (was fsynced).
        assert_eq!(writer.pending_writes(), 0);
        assert_eq!(writer.total_writes(), 1);
    }

    #[test]
    fn everysec_fsync_completes_off_thread() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::Everysec).unwrap();
        writer
            .append(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n")
            .unwrap();
        writer.last_fsync = Instant::now() - std::time::Duration::from_secs(1);
        writer.maybe_fsync().unwrap();

        for _ in 0..50 {
            writer.maybe_fsync().unwrap();
            if writer.pending_writes() == 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        assert_eq!(writer.pending_writes(), 0);
    }

    #[test]
    fn everysec_fsync_keeps_new_writes_pending() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, 0, AofFsyncPolicy::Everysec).unwrap();
        writer
            .append(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n")
            .unwrap();
        writer.last_fsync = Instant::now() - std::time::Duration::from_secs(1);
        writer.maybe_fsync().unwrap();
        writer
            .append(b"*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\nd\r\n")
            .unwrap();

        for _ in 0..50 {
            writer.maybe_fsync().unwrap();
            if writer.pending_writes() == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        assert_eq!(writer.pending_writes(), 1);
        writer.flush_and_sync().unwrap();
        assert_eq!(writer.pending_writes(), 0);
    }

    // Helper to compute expected writes (avoid unused variable warnings).
    fn writer_total_writes_after_drop(path: &Path, cmd1: &[u8], cmd2: &[u8]) -> u64 {
        let mut data = Vec::new();
        File::open(path).unwrap().read_to_end(&mut data).unwrap();
        assert_eq!(data.len(), AOF_HEADER_SIZE + cmd1.len() + cmd2.len());
        2
    }

    /// RAII guard to clean up temp files.
    fn scopeguard(path: PathBuf) -> impl Drop {
        struct Guard(PathBuf);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        Guard(path)
    }
}
