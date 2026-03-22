//! # vortex-proto
//!
//! RESP2/RESP3 wire protocol parser and serializer for VortexDB.
//!
//! This crate is **stateless** and performs **no I/O** — it operates purely
//! on byte slices. The design enables zero-copy parsing where bulk strings
//! are returned as `&[u8]` slices into the input buffer.
//!
//! ## Key Types
//!
//! - [`RespFrame`] — Full RESP3 frame enum (SimpleString, Error, Integer, etc.)
//! - [`RespParser`] — TODO: Scalar RESP parser (SIMD-accelerated in Phase 2)
//! - [`RespSerializer`] — Response encoder with pre-computed common responses
//! - [`CommandMeta`] — Command metadata for perfect-hash dispatch
//!
//! ## Feature Flags
//!
//! - `simd` — Enable SIMD CRLF scanning (TODO: Phase 2 implementation)
//! - `resp3` — Enable RESP3 frame types (Boolean, Double, Map, Set, Push)

pub mod command;
pub mod frame;
pub mod parser;
pub mod serializer;

pub use command::{CommandFlags, CommandMeta};
pub use frame::RespFrame;
pub use parser::{NeedMoreData, RespParser};
pub use serializer::RespSerializer;
