#![cfg_attr(feature = "simd", feature(portable_simd))]
//!
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
//! - [`SwissTable`] — SIMD-probed open-addressing hash table with H₂ fingerprinting
//! - [`Entry`] — 64-byte cache-line-aligned hash table entry (Phase 3.2)

pub mod command;
pub mod entry;
pub mod expiry;
pub mod shard;
pub mod table;

pub use command::{Command, CommandContext};
pub use entry::{Entry, EntryValue};
pub use expiry::{ExpiryEntry, ExpiryWheel};
pub use shard::Shard;
pub use table::SwissTable;
