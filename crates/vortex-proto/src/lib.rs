#![cfg_attr(
    all(feature = "simd", any(target_arch = "aarch64", test)),
    feature(portable_simd)
)]
//!
//! # vortex-proto
//!
//! RESP2/RESP3 wire protocol parser and serializer for VortexDB.
//!
//! This crate is **stateless** and performs **no I/O** — it operates purely
//! on byte slices. The parser snapshots the consumed input prefix into shared
//! `Bytes` backing and materializes payload frames with `Bytes::slice()`, which
//! removes per-frame payload allocations without borrowing the caller's buffer.
//!
//! ## Key Types
//!
//! - [`RespFrame`] — Full RESP3 frame enum (SimpleString, Error, Integer, etc.)
//! - [`RespParser`] — RESP parser with SIMD CRLF scan, SWAR integers, and pipeline parsing
//! - [`ParseError`] — Structured parser errors (need more data, frame too large, invalid frame)
//! - [`scan_crlf`] — SIMD/scalar CRLF scanner used by the Phase 2 parser pipeline
//! - [`swar_parse_int`] — SWAR integer parser for RESP lengths and integer frames
//! - [`RespSerializer`] — Response encoder with pre-computed common responses
//! - [`CommandMeta`] — Command metadata for perfect-hash dispatch
//!
//! ## Feature Flags
//!
//! - `simd` — Enable SIMD CRLF scanning
//! - `resp3` — Enable RESP3 frame types (Boolean, Double, Map, Set, Push)

pub mod command;
pub mod frame;
pub mod parser;
pub mod scanner;
pub mod serializer;
pub mod swar;
pub mod tape;

pub use command::{CommandFlags, CommandMeta};
pub use frame::RespFrame;
pub use parser::{NeedMoreData, ParseError, RespParser};
pub use scanner::{CrlfPositions, scan_crlf};
pub use serializer::RespSerializer;
pub use swar::swar_parse_int;
pub use tape::{FrameRef, RespTape, TapeEntry};
