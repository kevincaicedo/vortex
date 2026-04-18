# Support Matrix

## Database Modes

| Database | Native | Container | Runtime Config Notes |
|----------|--------|-----------|----------------------|
| Vortex | Yes | Yes | Supports `maxmemory`, `eviction_policy`, `aof_enabled`, `aof_fsync`, and isolated AOF paths at startup. |
| Redis | Yes | Yes | Supports `appendonly`, `appendfsync`, `maxmemory`, and `maxmemory-policy` at startup. |
| Valkey | No | Yes | Container adapter mirrors the Redis runtime policy surface. |
| Dragonfly | No | Yes | Supports `maxmemory`; AOF automation and non-default eviction policies are rejected explicitly. |

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