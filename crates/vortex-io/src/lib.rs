//! # vortex-io
//!
//! Per-core I/O reactor and connection management for VortexDB.
//!
//! This crate implements the **thread-per-core** reactor architecture:
//!
//! - Each CPU core runs a dedicated [`Reactor`] with its own event loop,
//!   memory arena, and io_uring instance (Linux).
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
pub mod shutdown;
pub mod timer;

pub use connection::{ConnectionMeta, ConnectionSlab, ConnectionState};
pub use pool::IoBackendMode;
pub use pool::{ReactorPool, ReactorPoolConfig};
pub use reactor::{AofConfig, Reactor, ReactorConfig};
pub use shutdown::ShutdownCoordinator;
pub use timer::TimerWheel;
