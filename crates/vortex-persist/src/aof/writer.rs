//! AOF file writer — buffered, fsync-aware, single-threaded per reactor.
//!
//! The writer is owned by the reactor thread and called inline after each
//! mutation command. Performance is critical: the hot path is a single
//! `BufWriter::write_all()` — a memcpy into the 64 KB userspace buffer.
//!
//! Fsync is controlled by [`AofFsyncPolicy`]:
//! - `Always`: fsync after every mutation (reactor calls `sync()` inline).
//! - `Everysec`: the reactor flushes dirty bytes to the kernel once per second,
//!   then a tiny helper thread performs `sync_data()` off the hot path. Pending
//!   bytes are bounded; under disk pressure the reactor blocks before the
//!   backlog can grow without limit.
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
use std::time::{Duration, Instant};

use vortex_engine::keyspace::AofLsn;

use super::contract::{AofAppendOutcome, AofCommitPoint, AofDurabilityRequirement};
use super::error::{AofErrorKind, aof_io_error};
use super::format::{AofFsyncPolicy, AofHeader, AofReactorId, AofWriterMode};

/// Userspace write buffer size (64 KB).
const AOF_BUF_SIZE: usize = 64 * 1024;
/// Default per-reactor `everysec` unsynced-byte ceiling before backpressure.
pub const DEFAULT_EVERYSEC_MAX_PENDING_BYTES: u64 = 64 * 1024 * 1024;
/// Fsync latency histogram bucket count.
pub const AOF_FSYNC_LATENCY_BUCKETS: usize = 8;

#[cfg(feature = "profile-telemetry")]
const AOF_FSYNC_LATENCY_BOUNDS_NANOS: [u64; AOF_FSYNC_LATENCY_BUCKETS - 1] = [
    100_000,
    500_000,
    1_000_000,
    5_000_000,
    10_000_000,
    50_000_000,
    100_000_000,
];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AofMaintenanceOutcome {
    requested_fsync: bool,
    completed_fsync: bool,
    applied_backpressure: bool,
}

impl AofMaintenanceOutcome {
    #[inline]
    const fn requested() -> Self {
        Self {
            requested_fsync: true,
            completed_fsync: false,
            applied_backpressure: false,
        }
    }

    #[inline]
    const fn completed() -> Self {
        Self {
            requested_fsync: false,
            completed_fsync: true,
            applied_backpressure: false,
        }
    }

    #[inline]
    const fn backpressure() -> Self {
        Self {
            requested_fsync: false,
            completed_fsync: false,
            applied_backpressure: true,
        }
    }

    #[inline]
    fn merge(&mut self, other: Self) {
        self.requested_fsync |= other.requested_fsync;
        self.completed_fsync |= other.completed_fsync;
        self.applied_backpressure |= other.applied_backpressure;
    }

    #[inline]
    pub const fn did_work(self) -> bool {
        self.requested_fsync || self.completed_fsync || self.applied_backpressure
    }
}

/// Point-in-time writer telemetry for INFO and benchmark reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AofTelemetrySnapshot {
    pub pending_bytes: u64,
    pub pending_writes: u64,
    pub fsync_requested: u64,
    pub fsync_completed: u64,
    pub fsync_failed: u64,
    pub fsync_worker_saturation: u64,
    pub backpressure_events: u64,
    pub backpressure_nanos_total: u64,
    pub backpressure_nanos_max: u64,
    pub last_appended_lsn: Option<AofLsn>,
    pub last_durable_lsn: Option<AofLsn>,
    pub fsync_latency_nanos_total: u64,
    pub fsync_latency_nanos_max: u64,
    pub fsync_latency_buckets: [u64; AOF_FSYNC_LATENCY_BUCKETS],
}

impl AofTelemetrySnapshot {
    #[inline]
    pub const fn last_appended_lsn_raw(self) -> u64 {
        match self.last_appended_lsn {
            Some(lsn) => lsn.get(),
            None => 0,
        }
    }

    #[inline]
    pub const fn last_durable_lsn_raw(self) -> u64 {
        match self.last_durable_lsn {
            Some(lsn) => lsn.get(),
            None => 0,
        }
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct AofRecordBytes<'a> {
    bytes: &'a [u8],
}

impl<'a> AofRecordBytes<'a> {
    /// Borrow a complete RESP mutation record for journal append.
    ///
    /// This wrapper is intentionally zero-cost: command logging already owns
    /// complete RESP bytes, so the writer must not re-parse, clone, or allocate
    /// on the append hot path.
    #[inline]
    pub fn from_resp(bytes: &'a [u8]) -> Self {
        debug_assert!(!bytes.is_empty(), "AOF records must not be empty");
        Self { bytes }
    }

    #[inline]
    pub fn as_bytes(self) -> &'a [u8] {
        self.bytes
    }

    #[inline]
    pub fn len(self) -> usize {
        self.bytes.len()
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.bytes.is_empty()
    }
}

#[derive(Clone, Copy, Debug)]
struct AsyncFsyncRequest {
    writes: u64,
    bytes: u64,
    durable_lsn: Option<AofLsn>,
    #[cfg(feature = "profile-telemetry")]
    measure_latency: bool,
}

#[derive(Clone, Copy, Debug)]
struct AsyncFsyncCompletion {
    request: AsyncFsyncRequest,
    latency: Option<Duration>,
}

struct AsyncFsyncWorker {
    request_tx: SyncSender<AsyncFsyncRequest>,
    done_rx: Receiver<io::Result<AsyncFsyncCompletion>>,
    handle: Option<JoinHandle<()>>,
    inflight: bool,
}

impl AsyncFsyncWorker {
    fn new(file: &File) -> io::Result<Self> {
        let sync_file = file.try_clone().map_err(|error| {
            aof_io_error(
                AofErrorKind::WorkerStopped,
                "failed to clone AOF file for fsync worker",
                error,
            )
        })?;
        let (request_tx, request_rx) = sync_channel::<AsyncFsyncRequest>(1);
        let (done_tx, done_rx) = sync_channel::<io::Result<AsyncFsyncCompletion>>(1);
        let handle = thread::spawn(move || {
            while let Ok(request) = request_rx.recv() {
                #[cfg(feature = "profile-telemetry")]
                let started = request.measure_latency.then(Instant::now);
                #[cfg(not(feature = "profile-telemetry"))]
                let started: Option<Instant> = None;
                let result = sync_file
                    .sync_data()
                    .map(|()| AsyncFsyncCompletion {
                        request,
                        latency: started.map(|start| start.elapsed()),
                    })
                    .map_err(|error| {
                        aof_io_error(AofErrorKind::Fsync, "AOF async fsync failed", error)
                    });
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
        })
    }

    fn request_sync(&mut self, request: AsyncFsyncRequest) -> io::Result<()> {
        if self.inflight || request.writes == 0 {
            return Ok(());
        }

        self.request_tx.try_send(request).map_err(|err| match err {
            std::sync::mpsc::TrySendError::Full(_) => {
                io::Error::new(io::ErrorKind::WouldBlock, "AOF fsync already inflight")
            }
            std::sync::mpsc::TrySendError::Disconnected(_) => {
                super::error::aof_error(AofErrorKind::WorkerStopped, "AOF fsync worker stopped")
            }
        })?;

        self.inflight = true;
        Ok(())
    }

    fn poll(&mut self) -> io::Result<Option<AsyncFsyncCompletion>> {
        if !self.inflight {
            return Ok(None);
        }

        match self.done_rx.try_recv() {
            Ok(result) => {
                self.inflight = false;
                Ok(Some(result?))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(super::error::aof_error(
                AofErrorKind::WorkerStopped,
                "AOF fsync worker stopped",
            )),
        }
    }

    fn wait(&mut self) -> io::Result<Option<AsyncFsyncCompletion>> {
        if !self.inflight {
            return Ok(None);
        }

        self.inflight = false;

        match self.done_rx.recv() {
            Ok(result) => Ok(Some(result?)),
            Err(_) => Err(super::error::aof_error(
                AofErrorKind::WorkerStopped,
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
    /// Bytes written since last fsync.
    pending_bytes: u64,
    /// Total mutations written to this AOF since open.
    total_writes: u64,
    /// Total bytes written (including header).
    total_bytes: u64,
    /// File path (for rewrite/rename operations).
    path: PathBuf,
    /// Async fsync worker used by `everysec` to keep sync stalls off-reactor.
    async_fsync: Option<AsyncFsyncWorker>,
    /// Max unsynced bytes allowed before `everysec` backpressure engages.
    max_pending_fsync_bytes: u64,
    /// Number of requested fsync operations.
    fsync_requested: u64,
    /// Number of successfully completed fsync operations.
    fsync_completed: u64,
    /// Number of failed fsync operations.
    fsync_failed: u64,
    /// Times the async worker already had an in-flight fsync when more was due.
    fsync_worker_saturation: u64,
    /// Times pending bytes reached the configured backpressure ceiling.
    backpressure_events: u64,
    /// Total time spent blocking for AOF backpressure.
    #[cfg(feature = "profile-telemetry")]
    backpressure_nanos_total: u64,
    /// Longest single AOF backpressure wait.
    #[cfg(feature = "profile-telemetry")]
    backpressure_nanos_max: u64,
    /// Last LSN accepted by this writer.
    last_appended_lsn: Option<AofLsn>,
    /// Last LSN proven durable by an explicit fsync.
    last_durable_lsn: Option<AofLsn>,
    /// Whether profiler-only latency/backpressure timing is collected.
    #[cfg(feature = "profile-telemetry")]
    profile_telemetry: bool,
    /// Total successful fsync syscall latency.
    #[cfg(feature = "profile-telemetry")]
    fsync_latency_nanos_total: u64,
    /// Max successful fsync syscall latency.
    #[cfg(feature = "profile-telemetry")]
    fsync_latency_nanos_max: u64,
    /// Fixed fsync latency histogram.
    #[cfg(feature = "profile-telemetry")]
    fsync_latency_buckets: [u64; AOF_FSYNC_LATENCY_BUCKETS],
}

impl AofFileWriter {
    /// Open or create an AOF file, writing the header if the file is new.
    ///
    /// If the file already exists and has content, we append to it (the header
    /// is already present). If it's empty or doesn't exist, we write a fresh
    /// header.
    pub fn open(path: &Path, reactor_id: AofReactorId, policy: AofFsyncPolicy) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| {
                aof_io_error(AofErrorKind::Append, "failed to open AOF file", error)
            })?;

        let file_len = file
            .metadata()
            .map_err(|error| {
                aof_io_error(
                    AofErrorKind::CorruptHeader,
                    "failed to stat AOF file",
                    error,
                )
            })?
            .len();
        let mut writer = BufWriter::with_capacity(AOF_BUF_SIZE, file);

        if file_len == 0 {
            // New file — write header.
            let header = AofHeader::new(reactor_id, AofWriterMode::Journal);
            header.write_to(&mut writer).map_err(|error| {
                aof_io_error(AofErrorKind::Append, "failed to write AOF header", error)
            })?;
            writer.flush().map_err(|error| {
                aof_io_error(AofErrorKind::Flush, "failed to flush AOF header", error)
            })?;
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
            pending_bytes: 0,
            total_writes: 0,
            total_bytes,
            path: path.to_path_buf(),
            async_fsync,
            max_pending_fsync_bytes: DEFAULT_EVERYSEC_MAX_PENDING_BYTES,
            fsync_requested: 0,
            fsync_completed: 0,
            fsync_failed: 0,
            fsync_worker_saturation: 0,
            backpressure_events: 0,
            #[cfg(feature = "profile-telemetry")]
            backpressure_nanos_total: 0,
            #[cfg(feature = "profile-telemetry")]
            backpressure_nanos_max: 0,
            last_appended_lsn: None,
            last_durable_lsn: None,
            #[cfg(feature = "profile-telemetry")]
            profile_telemetry: false,
            #[cfg(feature = "profile-telemetry")]
            fsync_latency_nanos_total: 0,
            #[cfg(feature = "profile-telemetry")]
            fsync_latency_nanos_max: 0,
            #[cfg(feature = "profile-telemetry")]
            fsync_latency_buckets: [0; AOF_FSYNC_LATENCY_BUCKETS],
        })
    }

    fn poll_async_fsync(&mut self) -> io::Result<AofMaintenanceOutcome> {
        if let Some(worker) = self.async_fsync.as_mut() {
            match worker.poll() {
                Ok(Some(completion)) => {
                    self.apply_fsync_completion(completion);
                    return Ok(AofMaintenanceOutcome::completed());
                }
                Ok(None) => {}
                Err(error) => {
                    self.fsync_failed = self.fsync_failed.saturating_add(1);
                    return Err(error);
                }
            }
        }

        Ok(AofMaintenanceOutcome::default())
    }

    fn wait_async_fsync(&mut self) -> io::Result<AofMaintenanceOutcome> {
        if let Some(worker) = self.async_fsync.as_mut() {
            match worker.wait() {
                Ok(Some(completion)) => {
                    self.apply_fsync_completion(completion);
                    return Ok(AofMaintenanceOutcome::completed());
                }
                Ok(None) => {}
                Err(error) => {
                    self.fsync_failed = self.fsync_failed.saturating_add(1);
                    return Err(error);
                }
            }
        }

        Ok(AofMaintenanceOutcome::default())
    }

    fn apply_fsync_completion(&mut self, completion: AsyncFsyncCompletion) {
        self.pending_writes = self
            .pending_writes
            .saturating_sub(completion.request.writes);
        self.pending_bytes = self.pending_bytes.saturating_sub(completion.request.bytes);
        if let Some(lsn) = completion.request.durable_lsn {
            self.last_durable_lsn = Some(lsn);
        }
        self.last_fsync = Instant::now();
        self.record_fsync_success(completion.latency);
    }

    fn record_fsync_success(&mut self, latency: Option<Duration>) {
        self.fsync_completed = self.fsync_completed.saturating_add(1);
        #[cfg(feature = "profile-telemetry")]
        {
            if let Some(latency) = latency {
                let nanos = latency.as_nanos().min(u128::from(u64::MAX)) as u64;
                if nanos == 0 {
                    return;
                }
                self.fsync_latency_nanos_total =
                    self.fsync_latency_nanos_total.saturating_add(nanos);
                self.fsync_latency_nanos_max = self.fsync_latency_nanos_max.max(nanos);
                let bucket = fsync_latency_bucket(nanos);
                self.fsync_latency_buckets[bucket] =
                    self.fsync_latency_buckets[bucket].saturating_add(1);
                return;
            }
        }
        #[cfg(not(feature = "profile-telemetry"))]
        let _ = latency;
    }

    #[inline]
    fn profile_start(&self) -> Option<Instant> {
        #[cfg(feature = "profile-telemetry")]
        {
            return self.profile_telemetry.then(Instant::now);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            None
        }
    }

    #[inline]
    fn record_backpressure_wait(&mut self, started: Option<Instant>) {
        #[cfg(feature = "profile-telemetry")]
        if let Some(started) = started {
            let waited = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            if waited != 0 {
                self.backpressure_nanos_total =
                    self.backpressure_nanos_total.saturating_add(waited);
                self.backpressure_nanos_max = self.backpressure_nanos_max.max(waited);
            }
        }
        #[cfg(not(feature = "profile-telemetry"))]
        let _ = started;
    }

    fn start_everysec_fsync(&mut self) -> io::Result<AofMaintenanceOutcome> {
        if self.pending_writes == 0 {
            return Ok(AofMaintenanceOutcome::default());
        }

        if self
            .async_fsync
            .as_ref()
            .is_some_and(AsyncFsyncWorker::is_inflight)
        {
            self.fsync_worker_saturation = self.fsync_worker_saturation.saturating_add(1);
            return Ok(AofMaintenanceOutcome::default());
        }

        self.writer.flush().map_err(|error| {
            aof_io_error(AofErrorKind::Flush, "failed to flush AOF buffer", error)
        })?;

        let request = AsyncFsyncRequest {
            writes: self.pending_writes,
            bytes: self.pending_bytes,
            durable_lsn: self.last_appended_lsn,
            #[cfg(feature = "profile-telemetry")]
            measure_latency: self.profile_telemetry,
        };

        if let Some(worker) = self.async_fsync.as_mut() {
            if let Err(error) = worker.request_sync(request) {
                if error.kind() == io::ErrorKind::WouldBlock {
                    self.fsync_worker_saturation = self.fsync_worker_saturation.saturating_add(1);
                } else {
                    self.fsync_failed = self.fsync_failed.saturating_add(1);
                }
                return Err(error);
            }
            self.fsync_requested = self.fsync_requested.saturating_add(1);
        } else {
            let started = self.profile_start();
            self.fsync_requested = self.fsync_requested.saturating_add(1);
            self.writer.get_ref().sync_data().map_err(|error| {
                self.fsync_failed = self.fsync_failed.saturating_add(1);
                aof_io_error(AofErrorKind::Fsync, "failed to sync AOF data", error)
            })?;
            let completion = AsyncFsyncCompletion {
                request,
                latency: started.map(|start| start.elapsed()),
            };
            self.apply_fsync_completion(completion);
        }

        Ok(AofMaintenanceOutcome::requested())
    }

    fn enforce_everysec_backpressure(&mut self) -> io::Result<AofMaintenanceOutcome> {
        if self.policy != AofFsyncPolicy::Everysec
            || self.pending_bytes < self.max_pending_fsync_bytes
        {
            return Ok(AofMaintenanceOutcome::default());
        }

        self.backpressure_events = self.backpressure_events.saturating_add(1);
        let started = self.profile_start();
        let mut outcome = AofMaintenanceOutcome::backpressure();

        if self
            .async_fsync
            .as_ref()
            .is_some_and(AsyncFsyncWorker::is_inflight)
        {
            self.fsync_worker_saturation = self.fsync_worker_saturation.saturating_add(1);
            outcome.merge(self.wait_async_fsync()?);
        }

        if self.pending_bytes >= self.max_pending_fsync_bytes {
            outcome.merge(self.start_everysec_fsync()?);
        }

        self.record_backpressure_wait(started);

        Ok(outcome)
    }

    /// Append a mutation command to the AOF with an LSN prefix (v2 format).
    ///
    /// Writes `[LSN: 8 bytes LE] [RESP bytes]` as a single record.
    /// The LSN is assigned inside the shard write-lock by the reactor,
    /// guaranteeing causal ordering across per-reactor AOF files.
    ///
    /// This is the **primary hot path** for AOF v2.
    #[inline]
    pub fn append_with_lsn(
        &mut self,
        lsn: AofLsn,
        record: AofRecordBytes<'_>,
    ) -> io::Result<AofAppendOutcome> {
        self.writer
            .write_all(&lsn.get().to_le_bytes())
            .map_err(|error| {
                aof_io_error(AofErrorKind::Append, "failed to append AOF LSN", error)
            })?;
        self.writer.write_all(record.as_bytes()).map_err(|error| {
            aof_io_error(AofErrorKind::Append, "failed to append AOF record", error)
        })?;
        self.pending_writes = self.pending_writes.saturating_add(1);
        self.pending_bytes = self
            .pending_bytes
            .saturating_add((super::format::LSN_SIZE + record.len()) as u64);
        self.total_writes = self.total_writes.saturating_add(1);
        self.total_bytes = self
            .total_bytes
            .saturating_add((super::format::LSN_SIZE + record.len()) as u64);
        self.last_appended_lsn = Some(lsn);

        if self.policy == AofFsyncPolicy::Always {
            self.flush_and_sync()?;
            return Ok(AofAppendOutcome::new(
                lsn,
                AofCommitPoint::FsyncDurable,
                Some(lsn),
            ));
        }

        self.enforce_everysec_backpressure()?;

        Ok(AofAppendOutcome::new(
            lsn,
            AofCommitPoint::UserspaceAppend,
            None,
        ))
    }

    /// Conditionally fsync based on policy and elapsed time.
    ///
    /// Called once per event-loop iteration by the reactor. For `everysec`
    /// mode, this fsyncs at most once per second. For other modes, this is
    /// a no-op.
    #[inline]
    pub fn maybe_fsync(&mut self) -> io::Result<AofMaintenanceOutcome> {
        let mut outcome = self.poll_async_fsync()?;

        if self.policy != AofFsyncPolicy::Everysec || self.pending_writes == 0 {
            return Ok(outcome);
        }

        if self.pending_bytes >= self.max_pending_fsync_bytes {
            outcome.merge(self.enforce_everysec_backpressure()?);
            return Ok(outcome);
        }

        if self.last_fsync.elapsed().as_secs() < 1 {
            return Ok(outcome);
        }

        if self
            .async_fsync
            .as_ref()
            .is_some_and(AsyncFsyncWorker::is_inflight)
        {
            self.fsync_worker_saturation = self.fsync_worker_saturation.saturating_add(1);
            return Ok(outcome);
        }

        outcome.merge(self.start_everysec_fsync()?);

        Ok(outcome)
    }

    /// Flush the userspace buffer and fsync to durable storage.
    pub fn flush_and_sync(&mut self) -> io::Result<()> {
        self.poll_async_fsync()?;
        self.wait_async_fsync()?;
        self.writer.flush().map_err(|error| {
            aof_io_error(AofErrorKind::Flush, "failed to flush AOF buffer", error)
        })?;
        let started = self.profile_start();
        self.fsync_requested = self.fsync_requested.saturating_add(1);
        self.writer.get_ref().sync_data().map_err(|error| {
            self.fsync_failed = self.fsync_failed.saturating_add(1);
            aof_io_error(AofErrorKind::Fsync, "failed to sync AOF data", error)
        })?;
        self.record_fsync_success(started.map(|start| start.elapsed()));
        self.last_fsync = Instant::now();
        self.pending_writes = 0;
        self.pending_bytes = 0;
        if let Some(lsn) = self.last_appended_lsn {
            self.last_durable_lsn = Some(lsn);
        }
        Ok(())
    }

    /// Flush the userspace buffer (without fsync).
    pub fn flush_buffer(&mut self) -> io::Result<()> {
        self.writer
            .flush()
            .map_err(|error| aof_io_error(AofErrorKind::Flush, "failed to flush AOF buffer", error))
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

    /// Returns the number of bytes not yet covered by an explicit fsync.
    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes
    }

    /// Returns the latest telemetry snapshot.
    pub fn telemetry(&self) -> AofTelemetrySnapshot {
        AofTelemetrySnapshot {
            pending_bytes: self.pending_bytes,
            pending_writes: self.pending_writes,
            fsync_requested: self.fsync_requested,
            fsync_completed: self.fsync_completed,
            fsync_failed: self.fsync_failed,
            fsync_worker_saturation: self.fsync_worker_saturation,
            backpressure_events: self.backpressure_events,
            backpressure_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.backpressure_nanos_total
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            backpressure_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.backpressure_nanos_max
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            last_appended_lsn: self.last_appended_lsn,
            last_durable_lsn: self.last_durable_lsn,
            fsync_latency_nanos_total: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.fsync_latency_nanos_total
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            fsync_latency_nanos_max: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.fsync_latency_nanos_max
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    0
                }
            },
            fsync_latency_buckets: {
                #[cfg(feature = "profile-telemetry")]
                {
                    self.fsync_latency_buckets
                }
                #[cfg(not(feature = "profile-telemetry"))]
                {
                    [0; AOF_FSYNC_LATENCY_BUCKETS]
                }
            },
        }
    }

    /// Set the `everysec` durability backlog ceiling in bytes.
    ///
    /// This is a cold-path configuration knob used at writer construction time.
    /// Lower values are useful for benchmark and fault-proof runs that need to
    /// force AOF backpressure on fast local disks.
    pub fn set_max_pending_fsync_bytes(&mut self, bytes: u64) {
        self.max_pending_fsync_bytes = bytes.max(1);
    }

    /// Enables or disables profiler-only AOF timing telemetry.
    #[cfg(feature = "profile-telemetry")]
    pub fn set_profile_telemetry(&mut self, enabled: bool) {
        self.profile_telemetry = enabled;
    }

    /// Returns the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the fsync policy.
    pub fn policy(&self) -> AofFsyncPolicy {
        self.policy
    }

    /// Returns the response-release durability required by this writer policy.
    #[inline]
    pub fn durability_requirement(&self) -> AofDurabilityRequirement {
        AofDurabilityRequirement::for_policy(self.policy)
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
        let new_file = OpenOptions::new()
            .append(true)
            .open(new_path)
            .map_err(|error| {
                aof_io_error(
                    AofErrorKind::Append,
                    "failed to open replacement AOF file",
                    error,
                )
            })?;
        let new_len = new_file
            .metadata()
            .map_err(|error| {
                aof_io_error(
                    AofErrorKind::CorruptHeader,
                    "failed to stat replacement AOF file",
                    error,
                )
            })?
            .len();

        // Replace writer.
        self.writer = BufWriter::with_capacity(AOF_BUF_SIZE, new_file);
        self.async_fsync = if self.policy == AofFsyncPolicy::Everysec {
            Some(AsyncFsyncWorker::new(self.writer.get_ref())?)
        } else {
            None
        };
        self.total_bytes = new_len;
        self.pending_writes = 0;
        self.pending_bytes = 0;
        self.path = new_path.to_path_buf();

        Ok(())
    }
}

#[inline]
#[cfg(feature = "profile-telemetry")]
fn fsync_latency_bucket(nanos: u64) -> usize {
    for (idx, bound) in AOF_FSYNC_LATENCY_BOUNDS_NANOS.iter().enumerate() {
        if nanos <= *bound {
            return idx;
        }
    }
    AOF_FSYNC_LATENCY_BUCKETS - 1
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
    use crate::aof::format::{AOF_HEADER_SIZE, LSN_SIZE};
    use crate::aof::{AofPolicyContract, AofReader};
    use std::io::Read;
    use vortex_engine::keyspace::{AofLsn, ConcurrentKeyspace};

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

    fn lsn(raw: u64) -> AofLsn {
        AofLsn::try_from_raw(raw).expect("test LSN fits AOF range")
    }

    fn rid(raw: u16) -> AofReactorId {
        AofReactorId::from_u16(raw)
    }

    fn record(bytes: &[u8]) -> AofRecordBytes<'_> {
        AofRecordBytes::from_resp(bytes)
    }

    fn replayed_commands(path: &Path) -> u64 {
        let keyspace = ConcurrentKeyspace::new(64);
        AofReader::new(path)
            .replay_into_keyspace(&keyspace)
            .expect("test AOF replays")
            .commands_replayed
    }

    #[test]
    fn create_new_aof() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let writer = AofFileWriter::open(&path, rid(0), AofFsyncPolicy::No).unwrap();
        assert_eq!(writer.total_writes(), 0);
        assert_eq!(writer.total_bytes(), AOF_HEADER_SIZE as u64);
        drop(writer);

        // Verify header was written.
        let mut file = File::open(&path).unwrap();
        let header = AofHeader::read_from(&mut file).unwrap();
        assert_eq!(header.reactor_id(), rid(0));
    }

    #[test]
    fn append_and_read_back() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, rid(7), AofFsyncPolicy::No).unwrap();

        let cmd1 = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n";
        let cmd2 = b"*2\r\n$3\r\nDEL\r\n$1\r\na\r\n";
        writer.append_with_lsn(lsn(1), record(cmd1)).unwrap();
        writer.append_with_lsn(lsn(2), record(cmd2)).unwrap();
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
            let mut writer = AofFileWriter::open(&path, rid(3), AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(lsn(1), record(b"*1\r\n$4\r\nPING\r\n"))
                .unwrap();
            writer.flush_buffer().unwrap();
        }

        // Reopen and append more.
        {
            let mut writer = AofFileWriter::open(&path, rid(3), AofFsyncPolicy::No).unwrap();
            writer
                .append_with_lsn(lsn(2), record(b"*2\r\n$3\r\nDEL\r\n$1\r\nx\r\n"))
                .unwrap();
            writer.flush_buffer().unwrap();
        }

        // Read entire file.
        let mut data = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut data).unwrap();

        // Should have exactly one header (16 bytes) + both LSN-prefixed commands.
        // PING: *1\r\n$4\r\nPING\r\n = 14 bytes
        // DEL x: *2\r\n$3\r\nDEL\r\n$1\r\nx\r\n = 20 bytes
        assert_eq!(data.len(), AOF_HEADER_SIZE + 8 + 14 + 8 + 20);
        assert_eq!(&data[0..6], b"VXAOF\x00");
    }

    #[test]
    fn always_fsync_policy() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, rid(0), AofFsyncPolicy::Always).unwrap();
        #[cfg(feature = "profile-telemetry")]
        writer.set_profile_telemetry(true);
        writer
            .append_with_lsn(lsn(1), record(b"*1\r\n$4\r\nPING\r\n"))
            .unwrap();
        // After Always append, pending_writes should be 0 (was fsynced).
        assert_eq!(writer.pending_writes(), 0);
        assert_eq!(writer.total_writes(), 1);
        let telemetry = writer.telemetry();
        assert_eq!(telemetry.pending_bytes, 0);
        assert_eq!(telemetry.fsync_requested, 1);
        assert_eq!(telemetry.fsync_completed, 1);
        assert_eq!(telemetry.fsync_failed, 0);
        assert_eq!(telemetry.last_appended_lsn, Some(lsn(1)));
        assert_eq!(telemetry.last_durable_lsn, Some(lsn(1)));
        #[cfg(feature = "profile-telemetry")]
        assert_eq!(telemetry.fsync_latency_buckets.iter().sum::<u64>(), 1);
        #[cfg(not(feature = "profile-telemetry"))]
        assert_eq!(telemetry.fsync_latency_buckets.iter().sum::<u64>(), 0);
    }

    #[test]
    fn fsync_latency_telemetry_is_profile_gated() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, rid(0), AofFsyncPolicy::Always).unwrap();
        writer
            .append_with_lsn(lsn(1), record(b"*1\r\n$4\r\nPING\r\n"))
            .unwrap();

        let telemetry = writer.telemetry();
        assert_eq!(telemetry.fsync_requested, 1);
        assert_eq!(telemetry.fsync_completed, 1);
        assert_eq!(telemetry.fsync_latency_nanos_total, 0);
        assert_eq!(telemetry.fsync_latency_nanos_max, 0);
        assert_eq!(telemetry.fsync_latency_buckets.iter().sum::<u64>(), 0);
    }

    #[test]
    fn policy_contract_release_points_are_explicit() {
        let no = AofPolicyContract::for_policy(AofFsyncPolicy::No);
        assert_eq!(no.response_release, AofCommitPoint::UserspaceAppend);
        assert!(no.process_crash_may_lose_acknowledged);
        assert!(no.os_crash_may_lose_acknowledged);

        let everysec = AofPolicyContract::for_policy(AofFsyncPolicy::Everysec);
        assert_eq!(everysec.response_release, AofCommitPoint::UserspaceAppend);
        assert!(everysec.process_crash_may_lose_acknowledged);
        assert!(everysec.os_crash_may_lose_acknowledged);

        let always = AofPolicyContract::for_policy(AofFsyncPolicy::Always);
        assert_eq!(always.response_release, AofCommitPoint::FsyncDurable);
        assert!(!always.process_crash_may_lose_acknowledged);
        assert!(!always.os_crash_may_lose_acknowledged);
    }

    #[test]
    fn append_with_lsn_returns_policy_commit_outcome() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, rid(0), AofFsyncPolicy::No).unwrap();
        let outcome = writer
            .append_with_lsn(lsn(1), record(b"*1\r\n$4\r\nPING\r\n"))
            .unwrap();
        assert_eq!(outcome.appended_lsn(), lsn(1));
        assert_eq!(outcome.response_release(), AofCommitPoint::UserspaceAppend);
        assert_eq!(outcome.durable_lsn(), None);
        assert!(outcome.satisfies(writer.durability_requirement()));
    }

    #[test]
    fn replay_visibility_matches_policy_release_window() {
        for policy in [AofFsyncPolicy::No, AofFsyncPolicy::Everysec] {
            let path = temp_aof_path();
            let _cleanup = scopeguard(path.clone());
            let mut writer = AofFileWriter::open(&path, rid(0), policy).unwrap();
            let outcome = writer
                .append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
                .unwrap();
            assert_eq!(outcome.response_release(), AofCommitPoint::UserspaceAppend);
            assert_eq!(replayed_commands(&path), 0);

            writer.flush_buffer().unwrap();
            assert_eq!(replayed_commands(&path), 1);
        }

        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());
        let mut writer = AofFileWriter::open(&path, rid(0), AofFsyncPolicy::Always).unwrap();
        let outcome = writer
            .append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
            .unwrap();
        assert_eq!(outcome.response_release(), AofCommitPoint::FsyncDurable);
        assert_eq!(outcome.durable_lsn(), Some(lsn(1)));
        assert_eq!(replayed_commands(&path), 1);
    }

    #[test]
    fn everysec_fsync_completes_off_thread() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, rid(0), AofFsyncPolicy::Everysec).unwrap();
        #[cfg(feature = "profile-telemetry")]
        writer.set_profile_telemetry(true);
        writer
            .append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"))
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
        let telemetry = writer.telemetry();
        assert_eq!(telemetry.pending_bytes, 0);
        assert_eq!(telemetry.fsync_requested, 1);
        assert_eq!(telemetry.fsync_completed, 1);
        assert_eq!(telemetry.fsync_failed, 0);
        assert_eq!(telemetry.last_appended_lsn, Some(lsn(1)));
        assert_eq!(telemetry.last_durable_lsn, Some(lsn(1)));
        #[cfg(feature = "profile-telemetry")]
        assert_eq!(telemetry.fsync_latency_buckets.iter().sum::<u64>(), 1);
        #[cfg(not(feature = "profile-telemetry"))]
        assert_eq!(telemetry.fsync_latency_buckets.iter().sum::<u64>(), 0);
    }

    #[test]
    fn everysec_fsync_keeps_new_writes_pending() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, rid(0), AofFsyncPolicy::Everysec).unwrap();
        writer
            .append_with_lsn(lsn(1), record(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"))
            .unwrap();
        writer.last_fsync = Instant::now() - std::time::Duration::from_secs(1);
        writer.maybe_fsync().unwrap();
        writer
            .append_with_lsn(lsn(2), record(b"*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\nd\r\n"))
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

    #[test]
    fn everysec_backpressure_requests_sync_at_pending_byte_limit() {
        let path = temp_aof_path();
        let _cleanup = scopeguard(path.clone());

        let mut writer = AofFileWriter::open(&path, rid(0), AofFsyncPolicy::Everysec).unwrap();
        writer.set_max_pending_fsync_bytes(1);
        writer
            .append_with_lsn(lsn(1), record(b"*1\r\n$4\r\nPING\r\n"))
            .unwrap();

        let telemetry = writer.telemetry();
        assert_eq!(telemetry.backpressure_events, 1);
        assert_eq!(telemetry.fsync_requested, 1);
        assert_eq!(telemetry.last_appended_lsn, Some(lsn(1)));

        writer.flush_and_sync().unwrap();
        let telemetry = writer.telemetry();
        assert_eq!(telemetry.pending_bytes, 0);
        assert_eq!(telemetry.pending_writes, 0);
        assert_eq!(telemetry.last_durable_lsn, Some(lsn(1)));
    }

    // Helper to compute expected writes (avoid unused variable warnings).
    fn writer_total_writes_after_drop(path: &Path, cmd1: &[u8], cmd2: &[u8]) -> u64 {
        let mut data = Vec::new();
        File::open(path).unwrap().read_to_end(&mut data).unwrap();
        assert_eq!(
            data.len(),
            AOF_HEADER_SIZE + LSN_SIZE + cmd1.len() + LSN_SIZE + cmd2.len()
        );
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
