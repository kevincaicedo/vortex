//! # AOF (Append-Only File) persistence for VortexDB.
//!
//! Provides crash-safe durability by logging every mutation command as raw
//! RESP wire bytes. On restart, the AOF is replayed through the engine to
//! restore state.
//!
//! ## Design Principles
//!
//! - **Zero hot-path allocation:** mutation bytes are memcpy'd into a userspace
//!   `BufWriter` — no serialization, no formatting, no heap allocation.
//! - **RESP-native format:** the AOF stores raw RESP protocol bytes, identical
//!   to what the client sent. Replay uses the existing SIMD RESP parser.
//! - **Single-threaded per reactor:** the AOF writer is owned by the reactor
//!   thread. No locks, no SPSC channel, no extra thread. The 64 KB userspace
//!   buffer absorbs write bursts; fsync is periodic or explicit.
//! - **Three fsync modes:** `always` (durability per command), `everysec`
//!   (default, <3% overhead), `no` (OS-managed).
//!
//! ## File Format
//!
//! ```text
//! ┌────────────────────────────────────┐
//! │ Header (16 bytes)                  │
//! │  magic: "VXAOF\x00" (6 bytes)     │
//! │  version: u16 LE (1)              │
//! │  shard_id: u16 LE                 │
//! │  created_at: u48 LE (unix secs)   │
//! ├────────────────────────────────────┤
//! │ Record 0: raw RESP command bytes   │
//! │ Record 1: raw RESP command bytes   │
//! │ ...                                │
//! └────────────────────────────────────┘
//! ```
//!
//! Each record is a complete RESP array (e.g., `*3\r\n$3\r\nSET\r\n...`).
//! Records are variable-length and self-delimiting via RESP framing.
//! No per-record headers or checksums — the RESP parser validates integrity
//! on replay. Truncated trailing records (from crash mid-write) are detected
//! by the parser returning `NeedMoreData` and safely discarded.

pub mod format;
pub mod reader;
pub mod rewrite;
pub mod writer;

pub use format::{AOF_HEADER_SIZE, AOF_MAGIC, AofFsyncPolicy, AofHeader};
pub use reader::{AofReader, ReplayStats};
pub use rewrite::AofRewriter;
pub use writer::AofFileWriter;
