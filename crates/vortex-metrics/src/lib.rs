//! # vortex-metrics
//!
//! Observability layer for VortexDB.
//!
//! Provides per-reactor thread-local metric counters, metric aggregation,
//! a built-in HTTP server for Prometheus `/metrics` endpoint, `SLOWLOG`
//! ring buffer, `LATENCY HISTOGRAM` per command type, and `INFO` command
//! response building.
//!
//! **Status:** Stub — implementation in Phase 4.
