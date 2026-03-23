//! # vortex-io
//!
//! Per-core I/O reactor and connection management for VortexDB.
//!
//! This crate implements the **thread-per-core** reactor architecture:
//!
//! - Each CPU core runs a dedicated [`Reactor`] with its own event loop,
//!   memory arena, io_uring instance (Linux), and keyspace shard.
//! - Connections are pinned to cores via `SO_REUSEPORT`.
//! - The [`IoBackend`] trait abstracts over io_uring (Linux) and polling
//!   (cross-platform fallback via epoll/kqueue/IOCP).
//!
//! ## Key Types
//!
//! - [`Reactor`] — Single-threaded event loop
//! - [`IoBackend`] — Trait abstracting I/O backend
//! - [`ConnectionSlab`] — Slab-allocated connection tracking
//! - [`Connection`] — Per-client state machine
//!
//! ## Feature Flags
//!
//! - `io-uring` — Enable io_uring backend (Linux only)
//! - `sqpoll` — Enable `IORING_SETUP_SQPOLL` zero-syscall mode
//! - `polling-fallback` — Cross-platform polling backend (default)

pub mod accept;
pub mod backend;
pub mod connection;
pub mod pool;
pub mod reactor;
pub mod timer;

pub use connection::{ConnectionMeta, ConnectionSlab, ConnectionState};
pub use pool::{CrossMessage, ReactorPool, ReactorPoolConfig};
pub use reactor::{Reactor, ReactorConfig};
pub use timer::TimerWheel;
