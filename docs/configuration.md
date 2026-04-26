# VortexDB Configuration Reference

All configuration options for VortexDB with defaults, types, and descriptions.

---

## Loading Priority

Configuration is loaded from multiple sources with the following precedence (highest to lowest):

1. **CLI arguments** — `vortex-server --bind 0.0.0.0:6379`
2. **Environment variables** — `VORTEX_BIND=0.0.0.0:6379`
3. **TOML config file** — `vortex-server -c vortex.toml`
4. **Compiled defaults**

---

## Network

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Bind address | `--bind` | `VORTEX_BIND` | `127.0.0.1:6379` | Address and port to listen on. Use `0.0.0.0:6379` to accept connections from all interfaces. |
| Max clients | `--max-clients` | `VORTEX_MAX_CLIENTS` | `10000` | Maximum number of simultaneous client connections across all reactors. |
| Connection timeout | `--connection-timeout-secs` | `VORTEX_CONNECTION_TIMEOUT` | `300` | Idle connection timeout in seconds. Connections with no activity are closed after this period. Set to `0` to disable. |

---

## Threading

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Threads | `--threads` | `VORTEX_THREADS` | `0` | Number of reactor threads. `0` auto-detects from CPU count — one reactor per core. Each reactor owns an independent shard, I/O backend, and connection pool. |

---

## I/O Backend

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| I/O backend | `--io-backend` | `VORTEX_IO_BACKEND` | `auto` | I/O multiplexing backend. Values: `auto`, `uring`, `polling`. |
| Ring size | `--ring-size` | `VORTEX_RING_SIZE` | `4096` | io_uring submission queue size. Must be a power of two. Larger values allow more inflight I/O operations. Only applies to `uring` backend. |
| Fixed buffers | `--fixed-buffers` | `VORTEX_FIXED_BUFFERS` | `20000` | Number of fixed I/O buffers pre-registered with io_uring for zero-copy I/O. Must be at least `max_clients * 2` because each live connection uses one read buffer and one write buffer. |
| Buffer size | `--buffer-size` | `VORTEX_BUFFER_SIZE` | `16384` | Size of each I/O buffer in bytes. Minimum: `4096`. Larger buffers reduce syscalls for big values but increase memory usage. |
| SQPOLL idle | `--sqpoll-idle-ms` | `VORTEX_SQPOLL_IDLE_MS` | `1000` | io_uring SQPOLL kernel thread idle timeout in milliseconds. The kernel thread polls SQEs without syscalls; it sleeps after this idle period. |

### I/O Backend Values

| Value | Description |
|-------|-------------|
| `auto` | Use io_uring on Linux 5.6+ if available, otherwise fall back to polling (kqueue/epoll). **Recommended.** |
| `uring` | Force io_uring backend. Fails at startup if io_uring is not available. Linux only. Best performance. |
| `polling` | Force cross-platform polling backend (kqueue on macOS, epoll on Linux). Works everywhere. |

---

## Memory

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Max memory | `--max-memory` | `VORTEX_MAX_MEMORY` | `0` | Maximum memory usage in bytes. `0` means unlimited. When set, the eviction policy determines what happens when the limit is reached. |
| Eviction policy | `--eviction-policy` | `VORTEX_EVICTION_POLICY` | `noeviction` | Memory eviction policy when `max-memory` is reached. |
| Adaptive structures | `--adaptive-structures` | `VORTEX_ADAPTIVE_STRUCTURES` | `true` | Enable adaptive morphing framework. When `true`, data structures can transition between encodings at runtime based on access patterns. When `false`, static Redis-compatible thresholds are used. |

### Eviction Policies

| Policy | Description |
|--------|-------------|
| `noeviction` | Return errors when memory limit is reached. No keys are evicted. |
| `allkeys-lru` | Evict the least recently used key from all keys. |
| `volatile-lru` | Evict the least recently used key among keys with an expiry set. |
| `allkeys-random` | Evict a random key from all keys. |
| `volatile-random` | Evict a random key among keys with an expiry set. |
| `volatile-ttl` | Evict the key with the shortest TTL among keys with an expiry set. |

> **Note:** Eviction policies are recognized and validated in v0.1-alpha but not yet enforced. Memory grows without limit regardless of the policy setting. Enforcement is planned for Phase 5.

---

## Persistence

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| AOF enabled | `--aof-enabled` | `VORTEX_AOF_ENABLED` | `false` | Enable Append-Only File persistence. |
| AOF fsync | `--aof-fsync` | `VORTEX_AOF_FSYNC` | `everysec` | AOF sync policy. |
| AOF path | `--aof-path` | `VORTEX_AOF_PATH` | `vortex.aof` | File path for the AOF log. |
| Snapshot enabled | `--snapshot-enabled` | `VORTEX_SNAPSHOT_ENABLED` | `false` | Enable periodic VXF snapshot persistence. |
| Snapshot interval | `--snapshot-interval` | `VORTEX_SNAPSHOT_INTERVAL` | `3600` | Seconds between automatic snapshots. |
| Snapshot path | `--snapshot-path` | `VORTEX_SNAPSHOT_PATH` | `vortex.vxf` | File path for snapshot files. |

### AOF Sync Policies

| Policy | Description |
|--------|-------------|
| `always` | fsync after every write command. Safest, slowest. |
| `everysec` | fsync once per second. Good balance of safety and performance. **Recommended.** |
| `no` | Let the OS decide when to flush. Fastest, risk of data loss on crash. |

> **Note:** Persistence is configured but not yet operational in v0.1-alpha. Planned for Phase 5.

---

## Security

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Password | `--requirepass` | `VORTEX_REQUIREPASS` | `""` (empty) | Require clients to authenticate with this password via the `AUTH` command. Empty string disables authentication. |

> **Note:** Full ACL support is planned for Phase 6.

---

## Logging

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Log level | `--log-level` | `VORTEX_LOG_LEVEL` | `info` | Minimum log level. Values: `trace`, `debug`, `info`, `warn`, `error`. |

Logs are written to stderr using the `tracing` framework with structured output. Enable JSON logging via the `RUST_LOG` environment variable for production deployments:

```sh
RUST_LOG=info vortex-server  # Standard filter
```

---

## Observability

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Metrics port | `--metrics-port` | `VORTEX_METRICS_PORT` | None | TCP port for Prometheus metrics endpoint. Disabled if not set. |

> **Note:** Prometheus metrics export is planned for Phase 4.

---

## Config File

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Config file | `-c`, `--config` | `VORTEX_CONFIG` | None | Path to a TOML configuration file. |

---

## TOML Config File Example

```toml
# vortex.toml — Full configuration example

# Network
bind = "0.0.0.0:6379"
max_clients = 50000
connection_timeout_secs = 300

# Threading
threads = 0  # auto-detect

# I/O
io_backend = "auto"
ring_size = 4096
fixed_buffers = 2048
buffer_size = 16384
sqpoll_idle_ms = 1000

# Memory
max_memory = 0  # unlimited
eviction_policy = "noeviction"
adaptive_structures = true

# Persistence (not yet active)
aof_enabled = false
aof_fsync = "everysec"
aof_path = "vortex.aof"
snapshot_enabled = false
snapshot_interval = 3600
snapshot_path = "vortex.vxf"

# Security
requirepass = ""

# Logging
log_level = "info"
```

Load: `vortex-server -c vortex.toml`

---

## Validation Rules

The server validates configuration at startup and exits with an error for invalid values:

| Rule | Error |
|------|-------|
| `ring_size` must be a power of two | `ring_size must be a power of two, got N` |
| `buffer_size` must be ≥ 4096 | `buffer_size must be >= 4096, got N` |
| `aof_fsync` must be `always`, `everysec`, or `no` | `invalid aof_fsync value 'X'` |
| `eviction_policy` must be a recognized policy | `invalid eviction_policy 'X'` |
| `threads` resolves to > 0 after auto-detection | `threads must be > 0` |
