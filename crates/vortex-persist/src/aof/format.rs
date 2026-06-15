//! AOF file header and format constants.
//!
//! **Format v2 (P1.6):** Each mutation record is prefixed with an 8-byte LSN
//! (Logical Sequence Number, LE u64) for global causal ordering across
//! per-reactor AOF files. Recovery uses K-Way merge on LSN.
//!
//! The AOF uses a minimal 16-byte header for format identification and
//! reactor association. Records are `[LSN: 8 bytes LE] [raw RESP bytes]`.

use std::fmt;
use std::io::{self, Read, Write};

use vortex_common::current_unix_time_nanos;

use super::error::{AofErrorKind, aof_error, aof_io_error};

/// Magic bytes identifying a VortexDB AOF file.
pub const AOF_MAGIC: &[u8; 6] = b"VXAOF\x00";

/// Current AOF format version.
/// v1: raw RESP records (no LSN). v2: LSN-prefixed records.
pub const AOF_VERSION: u16 = 2;

/// Header size in bytes (fixed).
pub const AOF_HEADER_SIZE: usize = 16;

/// Size of the LSN prefix per record (8 bytes, u64 LE).
pub const LSN_SIZE: usize = 8;

/// Internal replay-only command name used for canonical EXEC AOF batches.
pub const AOF_TRANSACTION_BATCH_COMMAND: &[u8] = b"VORTEXTX";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AofReactorId(u16);

impl AofReactorId {
    pub const MAX: usize = u16::MAX as usize;

    #[inline]
    pub const fn from_u16(id: u16) -> Self {
        Self(id)
    }

    #[inline]
    pub fn try_from_usize(id: usize) -> Result<Self, AofReactorIdError> {
        u16::try_from(id)
            .map(Self)
            .map_err(|_| AofReactorIdError { attempted: id })
    }

    #[inline]
    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AofReactorIdError {
    pub attempted: usize,
}

impl fmt::Display for AofReactorIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AOF reactor id {} exceeds u16 header range {}",
            self.attempted,
            AofReactorId::MAX
        )
    }
}

impl std::error::Error for AofReactorIdError {}

impl From<AofReactorIdError> for io::Error {
    fn from(error: AofReactorIdError) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofWriterMode {
    Journal,
    SnapshotRewrite,
}

impl AofWriterMode {
    #[inline]
    pub const fn version(self) -> u16 {
        match self {
            Self::Journal => AOF_VERSION,
            Self::SnapshotRewrite => 1,
        }
    }
}

/// Fsync policy for the AOF writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofFsyncPolicy {
    /// Fsync after every mutation — maximum durability, highest latency.
    Always,
    /// Fsync once per second — default, <3% throughput impact.
    Everysec,
    /// Never explicitly fsync — rely on OS page cache flush.
    No,
}

impl AofFsyncPolicy {
    /// Parse from a config string.
    pub fn parse_policy(s: &str) -> Option<Self> {
        match s {
            "always" => Some(Self::Always),
            "everysec" => Some(Self::Everysec),
            "no" => Some(Self::No),
            _ => None,
        }
    }
}

/// AOF file header (16 bytes).
///
/// ```text
/// Offset  Size  Field
/// 0       6     magic ("VXAOF\0")
/// 6       2     version (u16 LE)  — 1 = legacy (no LSN), 2 = LSN-prefixed
/// 8       2     reactor_id (u16 LE)
/// 10      6     created_at (u48 LE, unix seconds)
/// ```
#[derive(Debug, Clone, Copy)]
pub struct AofHeader {
    version: u16,
    /// Reactor ID that owns this AOF file (was `shard_id` in v1).
    reactor_id: AofReactorId,
    created_at: u64, // truncated to 48 bits on write
}

impl AofHeader {
    /// Create a new header with current timestamp.
    pub fn new(reactor_id: AofReactorId, mode: AofWriterMode) -> Self {
        let created_at = current_unix_time_nanos() / 1_000_000_000;
        Self {
            version: mode.version(),
            reactor_id,
            created_at,
        }
    }

    #[inline]
    pub const fn version(&self) -> u16 {
        self.version
    }

    #[inline]
    pub const fn reactor_id(&self) -> AofReactorId {
        self.reactor_id
    }

    #[inline]
    pub const fn created_at(&self) -> u64 {
        self.created_at
    }

    #[inline]
    pub const fn is_v2(&self) -> bool {
        self.version >= 2
    }

    /// Serialize header to a 16-byte buffer.
    pub fn to_bytes(&self) -> [u8; AOF_HEADER_SIZE] {
        let mut buf = [0u8; AOF_HEADER_SIZE];
        buf[0..6].copy_from_slice(AOF_MAGIC);
        buf[6..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..10].copy_from_slice(&self.reactor_id.get().to_le_bytes());
        // Pack created_at as 6-byte LE (48-bit timestamp — good until year 8919766).
        let ts_bytes = self.created_at.to_le_bytes();
        buf[10..16].copy_from_slice(&ts_bytes[..6]);
        buf
    }

    /// Write header to a writer.
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.to_bytes())
    }

    /// Read and validate header from a reader.
    pub fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; AOF_HEADER_SIZE];
        r.read_exact(&mut buf).map_err(|error| {
            aof_io_error(
                AofErrorKind::CorruptHeader,
                "failed to read AOF header",
                error,
            )
        })?;

        // Validate magic.
        if &buf[0..6] != AOF_MAGIC {
            return Err(aof_error(
                AofErrorKind::CorruptHeader,
                "invalid AOF magic — not a VortexDB AOF file",
            ));
        }

        let version = u16::from_le_bytes([buf[6], buf[7]]);
        if version != AOF_VERSION && version != 1 {
            return Err(aof_error(
                AofErrorKind::UnsupportedFormat,
                format!("unsupported AOF version {version}, expected 1 or {AOF_VERSION}"),
            ));
        }

        let reactor_id = AofReactorId::from_u16(u16::from_le_bytes([buf[8], buf[9]]));

        // Unpack 48-bit timestamp.
        let mut ts_bytes = [0u8; 8];
        ts_bytes[..6].copy_from_slice(&buf[10..16]);
        let created_at = u64::from_le_bytes(ts_bytes);

        Ok(Self {
            version,
            reactor_id,
            created_at,
        })
    }
}

/// Check if a command name (uppercase ASCII) is a mutation that must be logged.
///
/// This is the hot-path gate — must be branchless / minimal overhead.
/// Read-only commands (GET, EXISTS, TTL, SCAN, etc.) return `false`.
#[inline]
pub fn is_mutation(name: &[u8]) -> bool {
    matches!(
        name,
        // String mutations
        b"SET"
            | b"SETNX"
            | b"SETEX"
            | b"PSETEX"
            | b"MSET"
            | b"MSETNX"
            | b"GETSET"
            | b"GETDEL"
            | b"GETEX"
            | b"APPEND"
            | b"SETRANGE"
            | b"INCR"
            | b"DECR"
            | b"INCRBY"
            | b"DECRBY"
            | b"INCRBYFLOAT"
            // Key mutations
            | b"DEL"
            | b"UNLINK"
            | b"EXPIRE"
            | b"PEXPIRE"
            | b"EXPIREAT"
            | b"PEXPIREAT"
            | b"PERSIST"
            | b"RENAME"
            | b"RENAMENX"
            | b"COPY"
            // Server mutations
            | b"FLUSHDB"
            | b"FLUSHALL"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let header = AofHeader {
            version: AOF_VERSION,
            reactor_id: AofReactorId::from_u16(42),
            created_at: 1_711_900_800,
        };
        let bytes = header.to_bytes();
        let mut cursor = std::io::Cursor::new(&bytes);
        let parsed = AofHeader::read_from(&mut cursor).unwrap();
        assert_eq!(parsed.version(), AOF_VERSION);
        assert_eq!(parsed.reactor_id().get(), 42);
        assert_eq!(parsed.created_at(), 1_711_900_800);
    }

    #[test]
    fn header_magic_validation() {
        let mut bad = [0u8; AOF_HEADER_SIZE];
        bad[0..6].copy_from_slice(b"BADMAG");
        let mut cursor = std::io::Cursor::new(&bad);
        let err = AofHeader::read_from(&mut cursor).unwrap_err();
        assert_eq!(
            crate::aof::aof_error_kind(&err),
            Some(crate::aof::AofErrorKind::CorruptHeader)
        );
    }

    #[test]
    fn header_version_validation() {
        let header = AofHeader::new(AofReactorId::from_u16(0), AofWriterMode::Journal);
        let mut bytes = header.to_bytes();
        // Corrupt version to 99.
        bytes[6..8].copy_from_slice(&99u16.to_le_bytes());
        let mut cursor = std::io::Cursor::new(&bytes);
        let err = AofHeader::read_from(&mut cursor).unwrap_err();
        assert_eq!(
            crate::aof::aof_error_kind(&err),
            Some(crate::aof::AofErrorKind::UnsupportedFormat)
        );
    }

    #[test]
    fn writer_mode_selects_journal_or_snapshot_header_version() {
        let journal = AofHeader::new(AofReactorId::from_u16(0), AofWriterMode::Journal);
        assert_eq!(journal.version(), AOF_VERSION);

        let snapshot = AofHeader::new(AofReactorId::from_u16(0), AofWriterMode::SnapshotRewrite);
        assert_eq!(snapshot.version(), 1);
        assert!(!snapshot.is_v2());
    }

    #[test]
    fn reactor_id_rejects_unrepresentable_values() {
        assert_eq!(
            AofReactorId::try_from_usize(u16::MAX as usize)
                .unwrap()
                .get(),
            u16::MAX
        );
        assert!(AofReactorId::try_from_usize(u16::MAX as usize + 1).is_err());
    }

    #[test]
    fn is_mutation_true_for_writes() {
        assert!(is_mutation(b"SET"));
        assert!(is_mutation(b"DEL"));
        assert!(is_mutation(b"INCR"));
        assert!(is_mutation(b"MSET"));
        assert!(is_mutation(b"EXPIRE"));
        assert!(is_mutation(b"FLUSHDB"));
        assert!(is_mutation(b"RENAME"));
        assert!(is_mutation(b"APPEND"));
        assert!(is_mutation(b"COPY"));
    }

    #[test]
    fn is_mutation_false_for_reads() {
        assert!(!is_mutation(b"GET"));
        assert!(!is_mutation(b"MGET"));
        assert!(!is_mutation(b"EXISTS"));
        assert!(!is_mutation(b"TTL"));
        assert!(!is_mutation(b"PTTL"));
        assert!(!is_mutation(b"KEYS"));
        assert!(!is_mutation(b"SCAN"));
        assert!(!is_mutation(b"TYPE"));
        assert!(!is_mutation(b"DBSIZE"));
        assert!(!is_mutation(b"PING"));
        assert!(!is_mutation(b"INFO"));
        assert!(!is_mutation(b"STRLEN"));
        assert!(!is_mutation(b"GETRANGE"));
    }

    #[test]
    fn fsync_policy_from_str() {
        assert_eq!(
            AofFsyncPolicy::parse_policy("always"),
            Some(AofFsyncPolicy::Always)
        );
        assert_eq!(
            AofFsyncPolicy::parse_policy("everysec"),
            Some(AofFsyncPolicy::Everysec)
        );
        assert_eq!(AofFsyncPolicy::parse_policy("no"), Some(AofFsyncPolicy::No));
        assert_eq!(AofFsyncPolicy::parse_policy("invalid"), None);
    }
}
