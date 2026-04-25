#![cfg_attr(all(feature = "simd", target_arch = "aarch64"), feature(portable_simd))]
#![cfg_attr(target_arch = "aarch64", feature(stdarch_aarch64_prefetch))]
//!
//! # vortex-engine
//!
//! The core data engine for VortexDB. Contains the Swiss Table hash map,
//! the concurrent keyspace, all data structures, and command
//! implementations. This is the largest crate but contains **zero I/O** —
//! it operates purely on in-memory data.
//!
//! ## Key Types
//!
//! - [`ConcurrentKeyspace`] — Sharded concurrent key-value store
//! - [`SwissTable`] — SIMD-probed open-addressing hash table with H₂ fingerprinting
//! - [`Entry`] — 64-byte cache-line-aligned hash table entry (Phase 3.2)

pub mod commands;
pub mod entry;
pub mod eviction;
pub mod keyspace;
pub mod morph;
pub mod prefetch;
pub mod table;

pub use entry::{Entry, EntryValue};
pub use eviction::{EvictionConfig, EvictionPolicy};
pub use keyspace::{ConcurrentKeyspace, DEFAULT_SHARD_COUNT};
pub use morph::{AccessProfile, DefaultMorphMonitor, DisabledMorphMonitor, MorphMonitor};
pub use table::SwissTable;
