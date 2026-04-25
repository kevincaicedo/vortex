//! # vortex-common
//!
//! Foundation types and contracts for the VortexDB in-memory data engine.
//!
//! This crate sits at the absolute bottom of the dependency tree. It contains
//! the shared types, error enums, constants, and traits that every other
//! VortexDB crate depends on. It has zero external dependencies beyond
//! `thiserror` and `bytes`.
//!
//! ## Key Types
//!
//! - [`VortexKey`] — Small-string-optimized key (inline ≤23 bytes)
//! - [`VortexValue`] — Tagged enum for all value types
//! - [`VortexError`] / [`VortexResult`] — Unified error handling
//! - [`ShardId`] — Shard routing identifier
//! - [`Timestamp`] — Monotonic nanosecond timestamp
//! - [`TTL`] — Expiry representation

pub mod constants;
pub mod encoding;
pub mod error;
pub mod key;
pub mod shard;
pub mod timestamp;
pub mod ttl;
pub mod value;

pub use constants::*;
pub use encoding::Encoding;
pub use error::{VortexError, VortexResult};
pub use key::VortexKey;
pub use shard::ShardId;
pub use timestamp::{
    Timestamp, absolute_unix_nanos_to_deadline_nanos, current_unix_time_nanos,
    deadline_nanos_to_absolute_unix_nanos,
};
pub use ttl::TTL;
pub use value::VortexValue;
