//! # vortex-memory
//!
//! Memory allocator and management layer for VortexDB.
//!
//! This crate owns all memory allocation strategy:
//!
//! - **Global allocator**: Sets `tikv-jemallocator` as `#[global_allocator]`
//!   for dramatically better multi-threaded allocation performance vs the
//!   system allocator.
//! - **NUMA-aware arenas**: Creates per-NUMA-node jemalloc arenas so each
//!   reactor allocates from local memory (Phase 1+).
//! - **Buffer pool**: Pre-allocated, page-aligned buffers for io_uring
//!   registration. Lease/return interface with pending-cancellation tracking.
//! - **Arena allocator**: Per-reactor bump allocator for short-lived
//!   allocations, reset each event-loop iteration (Phase 1+).
//!
//! ## Safety
//!
//! This crate contains `unsafe` code for:
//! - FFI calls to jemalloc's `mallctl` API
//! - Raw pointer manipulation in the buffer pool
//!
//! All unsafe blocks have `// SAFETY:` comments and are covered by tests.

pub mod allocator;
pub mod buffer_pool;

pub use allocator::GlobalAllocator;
pub use buffer_pool::{Buffer, BufferPool};
