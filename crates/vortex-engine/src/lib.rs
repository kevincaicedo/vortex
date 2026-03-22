//! # vortex-engine
//!
//! The core data engine for VortexDB. Contains the Swiss Table hash map,
//! shard management, all data structures, command implementations, and the
//! expiry engine. This is the largest crate but contains **zero I/O** — it
//! operates purely on in-memory data.
//!
//! ## Key Types
//!
//! - [`Shard`] — Owns one hash table, one TTL timer wheel, one access-counter sketch
//! - [`SwissTable`] — SIMD-probed open-addressing hash table (Phase 3; HashMap wrapper in Phase 0)
//! - [`Entry`] — 64-byte cache-line-aligned hash table entry

pub mod command;
pub mod entry;
pub mod shard;
pub mod table;

pub use command::{Command, CommandContext};
pub use entry::Entry;
pub use shard::Shard;
pub use table::SwissTable;
