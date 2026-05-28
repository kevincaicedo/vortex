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
| Threads | `--threads` | `VORTEX_THREADS` | `0` | Number of reactor threads. `0` auto-detects from CPU count. Each reactor owns its I/O backend and connection pool; all reactors share the concurrent keyspace. |
| Shard count | `--shard-count` | `VORTEX_SHARD_COUNT` | `4096` | Number of engine keyspace shards. Must be a power of two in `64..=131072`. Benchmark matrix sweeps use this to test lock contention and memory overhead tradeoffs. |

---

## I/O Backend

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| I/O backend | `--io-backend` | `VORTEX_IO_BACKEND` | `auto` | I/O multiplexing backend. Values: `auto`, `uring`, `polling`. |
| Ring size | `--ring-size` | `VORTEX_RING_SIZE` | `4096` | io_uring submission queue size. Must be a power of two. Larger values allow more inflight I/O operations. Only applies to `uring` backend. |
| Fixed buffers | `--fixed-buffers` | `VORTEX_FIXED_BUFFERS` | `1024` | Number of reactor I/O staging buffers. Each live connection leases one read buffer; the runtime active-client budget is capped by this pool. |
| Fixed buffer registration | `--fixed-buffer-registration` | `VORTEX_FIXED_BUFFER_REGISTRATION` | `auto` | io_uring fixed-buffer registration policy. Values: `auto`, `on`, `off`. In `auto`, registration is used only when the effective backend and buffer range support it. |
| Buffer size | `--buffer-size` | `VORTEX_BUFFER_SIZE` | `16384` | Size of each I/O buffer in bytes. Minimum: `4096`. Larger buffers reduce syscalls for big values but increase memory usage. |
| SQPOLL idle | `--sqpoll-idle-ms` | `VORTEX_SQPOLL_IDLE_MS` | `0` | io_uring SQPOLL kernel thread idle timeout in milliseconds. `0` disables SQPOLL. The kernel thread polls SQEs without syscalls; it sleeps after this idle period. |

### I/O Backend Values

| Value | Description |
|-------|-------------|
| `auto` | Use io_uring when available and supported, otherwise fall back to the cross-platform polling backend. **Recommended for alpha.** |
| `uring` | Force io_uring backend. Fails at startup if io_uring is not available or strict fixed-buffer registration cannot be satisfied. Linux only. Performance claims require same-workload polling comparison. |
| `polling` | Force the cross-platform polling backend through the `polling` crate. Use this for portability and macOS evidence rows. |

## Per-Connection Retention Limits

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Max request bytes | `--max-request-bytes` | `VORTEX_MAX_REQUEST_BYTES` | `67108864` | Maximum bytes retained for one in-flight request or pipeline. |
| Max parser accumulator bytes | `--max-parser-accumulator-bytes` | `VORTEX_MAX_PARSER_ACCUMULATOR_BYTES` | `67108864` | Maximum bytes retained for one incomplete parser accumulator. |
| Max pending response bytes | `--max-pending-response-bytes` | `VORTEX_MAX_PENDING_RESPONSE_BYTES` | `67108864` | Maximum serialized response bytes pending for one connection. |
| Max MULTI queue commands | `--max-multi-queue-commands` | `VORTEX_MAX_MULTI_QUEUE_COMMANDS` | `128` | Maximum queued MULTI commands retained for one connection. |
| Max MULTI queue bytes | `--max-multi-queue-bytes` | `VORTEX_MAX_MULTI_QUEUE_BYTES` | `16777216` | Maximum serialized MULTI command bytes retained for one connection. |
| Max WATCH registrations | `--max-watch-registrations` | `VORTEX_MAX_WATCH_REGISTRATIONS` | `1024` | Maximum WATCH registrations retained for one connection. |
| Max writev chunks | `--max-writev-chunks` | `VORTEX_MAX_WRITEV_CHUNKS` | `64` | Maximum deferred writev submit chunks retained for one response batch. |

---

## Reactor Budgets And Overload Gates

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Completion budget | `--reactor-completion-budget` | `VORTEX_REACTOR_COMPLETION_BUDGET` | `256` | Max completion events processed per loop activation. |
| Command budget | `--reactor-command-budget` | `VORTEX_REACTOR_COMMAND_BUDGET` | `1024` | Max commands processed per loop activation. |
| Accept budget | `--reactor-accept-budget` | `VORTEX_REACTOR_ACCEPT_BUDGET` | `64` | Max accepts processed per loop activation. |
| Writev budget | `--reactor-writev-budget` | `VORTEX_REACTOR_WRITEV_BUDGET` | `1024` | Max writev segments processed per loop activation. |
| Maintenance budget | `--reactor-maintenance-budget` | `VORTEX_REACTOR_MAINTENANCE_BUDGET` | `4` | Max maintenance slices consumed per loop activation. |
| Time budget | `--reactor-time-budget-us` | `VORTEX_REACTOR_TIME_BUDGET_US` | `0` | Optional command activation time budget in microseconds. `0` disables time budgeting. |
| Overload accept percent | `--reactor-overload-accept-connection-percent` | `VORTEX_REACTOR_OVERLOAD_ACCEPT_CONNECTION_PERCENT` | `95` | Reactor-local connection capacity percent at which accepts throttle. |
| Overload pending response bytes | `--reactor-overload-pending-response-bytes` | `VORTEX_REACTOR_OVERLOAD_PENDING_RESPONSE_BYTES` | `268435456` | Reactor queued response bytes that disable new reads. |
| Overload parser accumulator bytes | `--reactor-overload-parser-accumulator-bytes` | `VORTEX_REACTOR_OVERLOAD_PARSER_ACCUMULATOR_BYTES` | `268435456` | Reactor parser accumulator bytes that disable new reads. |
| Overload AOF pending bytes | `--reactor-overload-aof-pending-bytes` | `VORTEX_REACTOR_OVERLOAD_AOF_PENDING_BYTES` | `67108864` | Reactor AOF pending bytes that defer AOF-bearing write commands. |
| Overload writev backlog bytes | `--reactor-overload-writev-backlog-bytes` | `VORTEX_REACTOR_OVERLOAD_WRITEV_BACKLOG_BYTES` | `268435456` | Reactor writev backlog bytes that disable new reads. |
| Overload maintenance debt | `--reactor-overload-maintenance-debt` | `VORTEX_REACTOR_OVERLOAD_MAINTENANCE_DEBT` | `1024` | Reactor maintenance debt units that defer foreground work. |

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
| `allkeys-lfu` | Evict a least-frequently-used candidate from all keys. |
| `volatile-lfu` | Evict a least-frequently-used candidate among keys with an expiry set. |

> **Note:** Eviction enforcement is implemented for alpha engineering validation, but release-grade eviction pressure evidence is still pending. Public memory and eviction claims must link to the release evidence ledger.

---

## Persistence

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| AOF enabled | `--aof-enabled` | `VORTEX_AOF_ENABLED` | `false` | Enable Append-Only File persistence. |
| AOF fsync | `--aof-fsync` | `VORTEX_AOF_FSYNC` | `everysec` | AOF sync policy. |
| AOF path | `--aof-path` | `VORTEX_AOF_PATH` | `vortex.aof` | File path for the AOF log. |
| AOF max pending fsync bytes | `--aof-max-pending-fsync-bytes` | `VORTEX_AOF_MAX_PENDING_FSYNC_BYTES` | `67108864` | Pending unsynced AOF bytes allowed in `everysec` mode before backpressure. |
| Snapshot enabled | `--snapshot-enabled` | `VORTEX_SNAPSHOT_ENABLED` | `false` | Enable periodic VXF snapshot persistence. |
| Snapshot interval | `--snapshot-interval` | `VORTEX_SNAPSHOT_INTERVAL` | `3600` | Seconds between automatic snapshots. |
| Snapshot path | `--snapshot-path` | `VORTEX_SNAPSHOT_PATH` | `vortex.vxf` | File path for snapshot files. |

### AOF Sync Policies

| Policy | Description |
|--------|-------------|
| `always` | fsync after every write command. Safest, slowest. |
| `everysec` | fsync once per second. Good balance of safety and performance. **Recommended.** |
| `no` | Let the OS decide when to flush. Fastest, risk of data loss on crash. |

> **Note:** AOF append, replay, fsync policy, and write-stop behavior are active alpha surfaces. Live `BGREWRITEAOF` remains disabled for alpha, multi-reactor runtime `appendonly` toggles fail closed, and VXF snapshots are still planned.

---

## Security

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Password | `--requirepass` | `VORTEX_REQUIREPASS` | `""` (empty) | Reserved alpha configuration field for future authentication. The current server does not enforce `AUTH` or ACLs. |

> **Note:** Do not expose alpha builds as authenticated production services. `AUTH` and full ACL support are not release-supported yet.

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
| Metrics port | `--metrics-port` | `VORTEX_METRICS_PORT` | None | Reserved alpha configuration field for a future Prometheus endpoint. No metrics listener is started by the current server. |
| Telemetry mode | `--telemetry-mode` | `VORTEX_TELEMETRY_MODE` | `minimal` | Runtime telemetry cost policy. Normal release builds support `minimal`; profiling builds add `profile`. |

> **Note:** `INFO runtime` is the current runtime observability surface. Profile telemetry is feature-gated and is not accepted by the normal release binary.

---

## Config File

| Option | CLI | Env Var | Default | Description |
|--------|-----|---------|---------|-------------|
| Config file | `-c`, `--config` | `VORTEX_CONFIG` | None | Path to a TOML configuration file. |

---

## TOML Config File Example

```toml
# vortex.toml — Common configuration example

# Network
bind = "0.0.0.0:6379"
max_clients = 50000
connection_timeout_secs = 300

# Threading
threads = 0  # auto-detect
shard_count = 4096

# I/O
io_backend = "auto"
ring_size = 4096
fixed_buffers = 2048
fixed_buffer_registration = "auto"
buffer_size = 16384
sqpoll_idle_ms = 0

# Memory
max_memory = 0  # unlimited
eviction_policy = "noeviction"
adaptive_structures = true

# Persistence
aof_enabled = false
aof_fsync = "everysec"
aof_path = "vortex.aof"
aof_max_pending_fsync_bytes = 67108864
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
| `shard_count` must be a power of two in `64..=131072` | `shard_count must be a power of two in 64..=131072, got N` |
