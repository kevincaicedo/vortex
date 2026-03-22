//! # vortex-sync
//!
//! Lock-free concurrent primitives for inter-reactor communication.
//!
//! This crate provides the data structures for safe, zero-allocation message
//! passing between VortexDB reactor threads. It contains no reactor or I/O
//! logic — purely concurrent data structures.
//!
//! ## Key Types
//!
//! - [`SpscRingBuffer`] — Single-producer, single-consumer lock-free ring buffer
//!   with `CachePadded` head/tail for false-sharing prevention.
//! - [`MpscQueue`] — Multi-producer, single-consumer lock-free queue for
//!   work-stealing across reactors.
//! - [`Backoff`] — Exponential backoff helper for CAS retry loops.
//! - [`ShardedCounter`] — Per-core atomic counters for contention-free metrics.
//!
//! ## Safety
//!
//! The ring buffer internals use `unsafe` code. All unsafe blocks must have:
//! 1. A `// SAFETY:` comment explaining the invariant
//! 2. Miri test coverage via `cargo miri test`
//! 3. ASAN CI runs

pub mod backoff;
pub mod counter;
pub mod mpsc;
pub mod spsc;

pub use backoff::Backoff;
pub use counter::ShardedCounter;
pub use mpsc::MpscQueue;
pub use spsc::SpscRingBuffer;
