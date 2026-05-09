//! # vortex-persist
//!
//! Persistence engine for VortexDB.
//!
//! ## Components
//!
//! - **AOF** — Per-shard append-only file with raw RESP wire-byte logging.
//!   Zero-allocation hot path: mutation bytes are memcpy'd into a 64 KB
//!   `BufWriter`. Three fsync modes: `always`, `everysec`, `no`.
//! - **VXF** — VortexDB native snapshot format (LZ4 compressed, columnar) — Phase 5
//! - **RDB Import** — Read Redis `.rdb` files for migration — Phase 5
//! - **Shadow Page Manager** — Dual-page-table snapshot without fork() — Phase 5
//!
//! ## Feature Flags
//!
//! - `dax` — Enable DAX (Direct Access) memory-mapped persistence

pub mod aof;

use std::path::Path;

use vortex_common::VortexResult;
use vortex_engine::keyspace::AofLsn;

use crate::aof::{AofAppendOutcome, AofCommitPoint, AofRecordBytes};

/// Typed AOF writer trait. Implemented in Phase 5.
pub trait AofWriter {
    /// Append a complete RESP command record with its already-assigned AOF LSN.
    fn append_with_lsn(
        &mut self,
        lsn: AofLsn,
        record: AofRecordBytes<'_>,
    ) -> VortexResult<AofAppendOutcome>;

    /// Flush the AOF buffer to disk.
    fn flush(&mut self) -> VortexResult<()>;

    /// Sync the AOF file to durable storage.
    fn sync(&mut self) -> VortexResult<()>;
}

/// VXF snapshot writer trait. Implemented in Phase 5.
pub trait VxfWriter {
    /// Begin a new snapshot.
    fn begin(&mut self) -> VortexResult<()>;

    /// Write a batch of entries to the snapshot.
    fn write_batch(&mut self, data: &[u8]) -> VortexResult<()>;

    /// Finalize the snapshot with checksum.
    fn finalize(&mut self) -> VortexResult<u32>;
}

/// VXF snapshot reader trait. Implemented in Phase 5.
pub trait VxfReader {
    /// Open a snapshot file.
    fn open(path: &Path) -> VortexResult<Self>
    where
        Self: Sized;

    /// Read the next batch of entries.
    fn read_batch(&mut self) -> VortexResult<Option<Vec<u8>>>;

    /// Verify the snapshot checksum.
    fn verify(&self) -> VortexResult<bool>;
}

/// Placeholder no-op AOF writer.
pub struct NoopAofWriter;

impl AofWriter for NoopAofWriter {
    fn append_with_lsn(
        &mut self,
        lsn: AofLsn,
        _record: AofRecordBytes<'_>,
    ) -> VortexResult<AofAppendOutcome> {
        Ok(AofAppendOutcome::new(
            lsn,
            AofCommitPoint::UserspaceAppend,
            None,
        ))
    }

    fn flush(&mut self) -> VortexResult<()> {
        Ok(())
    }
    fn sync(&mut self) -> VortexResult<()> {
        Ok(())
    }
}
