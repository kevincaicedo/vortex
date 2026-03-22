# VortexDB

[![CI](https://github.com/kevincaicedo/vortex/actions/workflows/ci.yml/badge.svg)](https://github.com/kevincaicedo/vortex/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> Next-generation, Redis-compatible in-memory database written in Rust.

VortexDB is a high-performance, drop-in Redis replacement built from the ground up in Rust. It targets sub-100µs p99 latency at 1M+ ops/sec per core using a thread-per-core architecture, io_uring, SIMD-accelerated RESP parsing, and a cache-line-optimized Swiss Table hash map.

## Performance Targets

| Metric | Target |
|--------|--------|
| Throughput (single core) | ≥ 1M ops/sec |
| GET/SET p50 latency | < 10 µs |
| GET/SET p99 latency | < 100 µs |
| GET/SET p999 latency | < 500 µs |
| Memory overhead per key | < 80 bytes (64B entry + metadata) |
| Startup time (1M keys) | < 2 seconds |

## Architecture

### Crate Dependency Graph

```mermaid
graph TD
    common[vortex-common<br><i>types, errors, constants</i>]
    memory[vortex-memory<br><i>jemalloc, buffer pool</i>]
    sync[vortex-sync<br><i>lock-free queues</i>]
    proto[vortex-proto<br><i>RESP2/RESP3 parser</i>]
    engine[vortex-engine<br><i>shard, Swiss Table, commands</i>]
    io[vortex-io<br><i>io_uring / epoll reactor</i>]
    config[vortex-config<br><i>CLI + TOML config</i>]
    persist[vortex-persist<br><i>AOF, snapshots</i>]
    cluster[vortex-cluster<br><i>gossip, slot routing</i>]
    repl[vortex-replication<br><i>leader/follower</i>]
    pubsub[vortex-pubsub<br><i>pub/sub channels</i>]
    scripting[vortex-scripting<br><i>Lua/Functions API</i>]
    acl[vortex-acl<br><i>ACL & auth</i>]
    metrics[vortex-metrics<br><i>Prometheus, SLOWLOG</i>]
    server[vortex-server<br><i>binary entry point</i>]

    proto --> common
    engine --> common
    engine --> proto
    engine --> memory
    io --> common
    io --> memory
    io --> sync
    config --> common
    persist --> common
    persist --> engine
    cluster --> common
    cluster --> proto
    repl --> common
    repl --> engine
    pubsub --> common
    pubsub --> sync
    scripting --> common
    scripting --> engine
    acl --> common
    metrics --> common
    server --> config
    server --> io
    server --> engine
    server --> proto
    server --> persist
    server --> cluster
    server --> repl
    server --> pubsub
    server --> scripting
    server --> acl
    server --> metrics
    server --> memory
```

### Request Lifecycle

```mermaid
sequenceDiagram
    participant C as Client
    participant R as Reactor (vortex-io)
    participant P as Parser (vortex-proto)
    participant E as Engine (vortex-engine)

    C->>R: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    R->>R: io_uring CQE → read into registered buffer
    R->>P: parse(buffer)
    P->>P: SIMD scan for \r\n positions
    P-->>R: RespFrame::Array(["SET", "foo", "bar"])
    R->>E: command_dispatch("SET") → SetCommand
    E->>E: crc16("foo") % num_shards → shard
    E->>E: SwissTable::insert("foo", "bar")
    E-->>R: RespFrame::SimpleString("OK")
    R->>R: serialize → io_uring SQE (write)
    R-->>C: +OK\r\n
```

## Prerequisites

- **Rust nightly** — pinned via `rust-toolchain.toml` (nightly-2026-03-15)
- **Linux** recommended for `io_uring` support; macOS uses `polling` fallback
- **cargo-fuzz** (optional) — for fuzzing: `cargo install cargo-fuzz`
- **cargo-deny** (optional) — for dependency audit: `cargo install cargo-deny`

### Linux (Ubuntu 24.04)

```sh
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# The pinned nightly toolchain installs automatically on first build

# Linux headers (for io_uring, optional)
sudo apt-get install -y linux-headers-$(uname -r) liburing-dev
```

### macOS 15+

```sh
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# No additional dependencies — uses polling fallback
```

## Quick Start

```sh
# Build everything
cargo build --workspace

# Run the server
cargo run --bin vortex-server

# Run all tests
cargo test --workspace

# Run benchmarks
cargo bench -p vortex-bench

# Lint
cargo clippy --workspace --all-targets -- -D warnings

# Format check
cargo fmt --check

# Dependency audit
cargo deny check

# Fuzz the RESP parser (requires cargo-fuzz)
cd fuzz && cargo fuzz run fuzz_resp_parser -- -max_total_time=60
```

## Crate Map

| Crate | Description | Status |
|---|---|---|
| `vortex-common` | Foundation types: keys (SSO), values, errors, TTL, timestamps | ✅ Phase 0 |
| `vortex-memory` | jemalloc global allocator, NUMA-aware arenas, buffer pool | ✅ Phase 0 |
| `vortex-sync` | Lock-free SPSC/MPSC queues, sharded counters, backoff | ✅ Phase 0 |
| `vortex-proto` | RESP2/RESP3 parser & serializer, command dispatch table | ✅ Phase 0 |
| `vortex-io` | Thread-per-core I/O reactor (io_uring / polling fallback) | 📋 Phase 1 |
| `vortex-config` | CLI args + TOML config + environment variables | ✅ Phase 0 |
| `vortex-engine` | Sharded key-value engine, Swiss Table, Command trait | ✅ Phase 0 |
| `vortex-persist` | AOF writer, VXF snapshots, RDB import | 📋 Phase 5 |
| `vortex-cluster` | Cluster topology, gossip, slot routing (16384 slots) | 📋 Phase 7 |
| `vortex-replication` | Leader-follower replication, PSYNC compat | 📋 Phase 6 |
| `vortex-pubsub` | Pub/Sub with cross-reactor message delivery | 📋 Phase 4 |
| `vortex-scripting` | Lua scripting engine (EVAL/EVALSHA) | 📋 Phase 8 |
| `vortex-acl` | Redis ACL-compatible access control | 📋 Phase 6 |
| `vortex-metrics` | Prometheus metrics, SLOWLOG, INFO command | 📋 Phase 4 |
| `vortex-server` | Server binary — startup, signal handling, shutdown | ✅ Phase 0 |
| `vortex-cli` | Interactive CLI client (rustyline REPL) | 📋 Phase 2 |
| `vortex-bench` | Criterion benchmarks, load generator, perf counters | ✅ Phase 0 |

## Project Structure

```
vortex/
├── crates/
│   ├── vortex-common/       # Foundation types & traits
│   ├── vortex-memory/       # Memory allocator
│   ├── vortex-sync/         # Lock-free primitives
│   ├── vortex-proto/        # RESP protocol
│   ├── vortex-engine/       # Data engine & commands
│   ├── vortex-io/           # I/O reactor
│   ├── vortex-config/       # Configuration
│   ├── vortex-persist/      # Persistence
│   ├── vortex-cluster/      # Clustering
│   ├── vortex-replication/  # Replication
│   ├── vortex-pubsub/       # Pub/Sub
│   ├── vortex-scripting/    # Scripting
│   ├── vortex-acl/          # Access control
│   ├── vortex-metrics/      # Observability
│   └── vortex-server/       # Server binary
├── tools/
│   ├── vortex-cli/          # CLI client
│   └── vortex-bench/        # Benchmarks & load generator
├── fuzz/                    # Fuzz targets & corpus
├── scripts/                 # Flamegraph & comparison scripts
└── .github/workflows/       # CI/CD pipelines
```

## License

Apache-2.0
