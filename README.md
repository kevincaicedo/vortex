# VortexDB

> Next-generation, Redis-compatible in-memory database written in Rust.

## Architecture

```
┌─────────────────────────────────────────────┐
│                vortex-server                │
│  ┌───────┐  ┌────────┐  ┌───────────────┐  │
│  │config │  │  I/O   │  │    engine      │  │
│  │       │  │reactor │  │ (sharded map)  │  │
│  └───────┘  └────────┘  └───────────────┘  │
│       │          │              │            │
│  ┌────┴──────────┴──────────────┴────────┐  │
│  │         vortex-proto (RESP parser)    │  │
│  └───────────────────────────────────────┘  │
│  ┌──────────┐ ┌──────────┐ ┌────────────┐  │
│  │ vortex-  │ │ vortex-  │ │  vortex-   │  │
│  │  memory  │ │   sync   │ │  common    │  │
│  └──────────┘ └──────────┘ └────────────┘  │
└─────────────────────────────────────────────┘
```

## Prerequisites

- **Rust nightly** (pinned via `rust-toolchain.toml`)
- Linux recommended for `io_uring` support; macOS uses `polling` fallback

## Build

```sh
cargo build --workspace
```

## Run

```sh
cargo run --bin vortex-server
cargo run --bin vortex-server -- --help
```

## Test

```sh
cargo test --workspace
```

## Benchmarks

```sh
cargo bench -p vortex-bench
```

## Crate Map

| Crate | Purpose |
|---|---|
| `vortex-common` | Shared types: keys, values, errors, TTL |
| `vortex-memory` | jemalloc allocator, buffer pool |
| `vortex-sync` | Lock-free queues, sharded counters |
| `vortex-proto` | RESP2/RESP3 parser & serializer |
| `vortex-io` | I/O reactor (polling / io_uring) |
| `vortex-config` | CLI + TOML configuration |
| `vortex-engine` | Sharded key-value engine |
| `vortex-persist` | AOF & snapshot persistence |
| `vortex-cluster` | Cluster & sharding (Phase 7) |
| `vortex-replication` | Leader-follower replication (Phase 6) |
| `vortex-pubsub` | Pub/Sub (Phase 4) |
| `vortex-scripting` | Lua/WASM scripting (Phase 8) |
| `vortex-acl` | Access control (Phase 6) |
| `vortex-metrics` | Observability & histograms (Phase 4) |
| `vortex-server` | Server binary |
| `vortex-cli` | Interactive CLI client |
| `vortex-bench` | Criterion benchmarks |

## License

Apache-2.0
