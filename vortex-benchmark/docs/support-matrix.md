# Support Matrix

Primary operator entrypoints:

| Tool | Help | Purpose |
|------|------|---------|
| `just benchmark` | `just benchmark --help` | Benchmark local, Docker, host-port, and SSH targets, then render neutral reports. |
| `just profiler` | `just profiler --help` | Capture profiler sessions and summaries for local, host-port, and SSH targets. |

## Database Modes

| Database | Native | Container | Runtime Config Notes |
|----------|--------|-----------|----------------------|
| Vortex | Yes | Yes | Supports `maxmemory`, `eviction_policy`, `aof_enabled`, `aof_fsync`, and isolated AOF paths at startup. |
| Redis | Yes | Yes | Supports `appendonly`, `appendfsync`, `maxmemory`, and `maxmemory-policy` at startup. |
| Valkey | No | Yes | Container adapter mirrors the Redis runtime policy surface. |
| Dragonfly | No | Yes | Supports `maxmemory`; AOF automation and non-default eviction policies are rejected explicitly. |

## Target Modes

| Target Mode | Benchmark | Profiler | Notes |
|-------------|-----------|----------|-------|
| `local` | Yes | Yes | Managed local native or Docker service lifecycle. |
| `host-port` | Yes | Yes for local processes with discoverable PID | External service is not stopped or reconfigured. |
| `ssh-managed` | Yes, including optional `--ssh-load-host` remote load execution | Yes, delegated to `scripts/profiler.sh` in a remote checkout | Requires `--ssh-target`; source copy/build and remote load execution require `--ssh-workdir`. |
| `ssh-attach` | Yes, including optional `--ssh-load-host` remote load execution | Yes, delegated to `scripts/profiler.sh` in a remote checkout | External remote service is not stopped or reconfigured. |

Benchmark and profiler SSH paths accept non-default transport with `--ssh-port`,
`--ssh-identity-file`, `--ssh-config`, repeated `--ssh-option`, and
`--ssh-connect-timeout`. Benchmark SSH copy-back writes remote artifacts under
`<artifact-root>/remote/<timestamp>/`.

## Profiler Tool Modes

| Mode / Tool | Platform | Target Question | Permission / Overhead | Output Contract | Fallback |
|-------------|----------|-----------------|-----------------------|-----------------|----------|
| `--perf-stat` | Linux | PMU counters, IPC, cycles/op, cache/TLB rates | May require lower `perf_event_paranoid`; low overhead | `perf-stat.txt`, parsed counter summary where available | Host/process telemetry only |
| `--flamegraph` / `--cpu` | Linux/macOS where tools exist | On-CPU hot functions and stack distribution | Sampling overhead; frequency-controlled | flamegraph SVG, `perf.data` or platform trace | Samply or profiler notes |
| `--scheduler` | Linux | Runnable latency, context switches, scheduler delay | May require BPF/perf permissions; medium overhead | run-queue artifacts and scheduler notes | procfs host telemetry |
| `--lock-offcpu` | Linux | Blocked/off-CPU, futex, fsync, and lock wait attribution | BPF/ftrace tools may require root; medium-high overhead | off-CPU/futex/fsync artifacts and notes | scheduler focus plus manual notes |
| `--c2c` | Linux | HITM and false-sharing escalation | PMU support and permissions required; high overhead | `perf c2c` data/report | cachegrind or layout review |
| `perf trace` / `perf lock` | Linux | Syscall flow and kernel lock contention | tracepoint access may require privileges; medium-high overhead | trace/lock reports | process syscall counters and off-CPU notes |
| `trace-cmd` | Linux | ftrace escalation for named hypotheses | tracefs access may require root; hypothesis-dependent overhead | `trace.dat`, trace reports | perf trace or BPF probes |
| sysstat pack | Linux | Low-overhead CPU/process/disk/network context | ordinary command access; low overhead | `vmstat`, `mpstat`, `pidstat`, `iostat`, `sar` outputs | procfs telemetry |
| `--memory` | Linux/macOS where tools exist | Allocation volume, RSS, resident/retained memory | heaptrack/Instruments may be high overhead | heaptrack/massif/Instruments plus host memory summaries | process RSS and allocator INFO |
| `--cache` | Linux/macOS with Valgrind | Cache simulation and instruction locality | Very high overhead; diagnostic-only | cachegrind/callgrind outputs | PMU cache counters if available |
| `--aof-disk` | Linux | Disk/fsync pressure and writeback attribution | sysstat/perf/BPF availability dependent | disk, writeback, fsync, and AOF artifacts | low-overhead host telemetry |
| `--network` | Linux | TCP retransmits, socket queues, network throughput/errors | low to medium overhead | `ss`, `nstat`, network telemetry | host telemetry only |
| macOS host/process pack | macOS | VM, disk, network, process RSS, and process sampling | ordinary command access; task sampling may need permission | `vm_stat`, `iostat`, `netstat`, `sysctl`, `sample`, `ps` outputs | Instruments/Samply or explicit unavailable fields |

## Backend Coverage

| Backend | Best For | Supports Commands | Supports Workloads |
|---------|----------|-------------------|--------------------|
| `redis-benchmark` | Point-command sweeps and simple command-group comparisons | Yes | No |
| `memtier_benchmark` | Mixed workload latency analysis and thread sweeps | No | Yes, but only non-transactional single-key workloads |
| `custom-rust` | Multi-key and transactional workload families | No | Yes |

## Command Groups

The built-in command groups are:

- `strings`
- `keys`
- `server`
- `transactions`

These groups expand through the catalog in `python/vortex_benchmark/catalog.py` before backend selection happens.

## `redis-benchmark` Command Support

The current adapter supports these explicit commands:

- `APPEND`, `COMMAND`, `COPY`, `DBSIZE`, `DECR`, `DECRBY`, `DEL`, `ECHO`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `EXPIRETIME`, `FLUSHALL`, `FLUSHDB`, `GET`, `GETDEL`, `GETEX`, `GETRANGE`, `GETSET`, `INFO`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `KEYS`, `MGET`, `MSET`, `MSETNX`, `PERSIST`, `PEXPIRE`, `PEXPIREAT`, `PEXPIRETIME`, `PING`, `PSETEX`, `PTTL`, `RANDOMKEY`, `RENAME`, `SCAN`, `SELECT`, `SET`, `SETEX`, `SETNX`, `SETRANGE`, `STRLEN`, `TIME`, `TOUCH`, `TTL`, `TYPE`, `UNLINK`

Known exclusions:

- `RENAMENX` is catalogued but intentionally rejected by the `redis-benchmark` adapter.
- Transaction queue semantics such as `MULTI` and `EXEC` belong on the `custom-rust` path.

## `memtier_benchmark` Workload Support

The `memtier_benchmark` adapter is used for built-in workloads where `multi_key == false` and `transactional == false`:

- `uniform-read_only`
- `uniform-read_heavy`
- `uniform-mixed`
- `uniform-write_heavy`
- `uniform-write_only`
- `zipfian-read_heavy`
- `zipfian-mixed`
- `hot-key`
- `single_key_mixed`

## `custom-rust` Workload Support

The `custom-rust` backend can execute the full built-in workload catalog and is the required backend for:

- `transaction`
- `multi-key operations`
- `multi_key_only`
- `transaction_only`
- `single_key_tx_mixed`
- `multi_key_tx_mixed`

It also remains available for the simpler workloads when deterministic RESP orchestration is more important than using external benchmarking tools.
