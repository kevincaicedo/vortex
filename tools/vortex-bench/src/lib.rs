//! VortexDB benchmark harness — criterion benchmarks live in `benches/`.
//!
//! This crate provides:
//! - [`LatencyRecorder`] — HdrHistogram wrapper for p50/p99/p999 latency measurement
//! - [`perf_counters`] — Hardware performance counter wrapper (Linux only)

pub mod latency;

#[cfg(target_os = "linux")]
pub mod perf_counters;

pub use latency::{LatencyRecorder, LatencyReport};
