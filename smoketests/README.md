# Vortex Smoke Tests

`smoketests/` is the dedicated root-level end-to-end compatibility harness for VortexDB.

This project is intentionally separate from crate-local integration tests:

- It uses the official Rust `redis` client, not internal request helpers.
- It can spawn `vortex-server` itself or target an already running Redis-compatible endpoint.
- It can optionally run the supported-command surface against a Redis baseline first, so the suite validates both Vortex behavior and the test cases themselves.
- It tracks command support honestly: `supported`, `partial`, or `stubbed`.
- It keeps one module per command under `smoketests/src/commands/` so coverage stays readable as the command surface grows.

## What This Suite Is For

The goal is correctness and Redis-compatibility tracking, not performance measurement.

The suite is intentionally case-driven and repeatable:

- each case runs against a clean database state
- supported commands can be replayed against Redis as a baseline oracle
- cases can be repeated to catch flakes or state-reset regressions

Each command module documents:

- What is tested now
- What is intentionally not tested yet
- Whether the command is fully supported, partial, or still stubbed

This is the repo’s official smoke/e2e tool for command validation.

## Quick Start

Run against a freshly spawned Vortex server:

```bash
just smoke
```

`just smoke` starts Vortex with `--threads 1` by default. That keeps the suite focused on command correctness and Redis client compatibility instead of multi-reactor scheduling noise. To override it, pass your own thread args:

```bash
just smoke --vortex-arg=--threads --vortex-arg=8
```

Run only selected commands:

```bash
just smoke --command GET,SET,MGET
```

Run an entire group:

```bash
just smoke --group string
just smoke --group key
just smoke --group server
```

Run against an already running server:

```bash
just smoke-existing --server-url redis://127.0.0.1:6379/
```

Or via environment variable:

```bash
VORTEX_SMOKE_SERVER_URL=redis://127.0.0.1:16379/ just smoke-existing
```

List command coverage and support levels:

```bash
just smoke-list
just smoke-list --verbose
```

## CLI Usage

The underlying CLI is the `vortex-smoketests` workspace package.

Examples:

```bash
cargo run -p vortex-smoketests -- run --spawn-vortex
cargo run -p vortex-smoketests -- run --spawn-vortex --command GET --command SET
cargo run -p vortex-smoketests -- run --server-url redis://127.0.0.1:6379/ --group string
cargo run -p vortex-smoketests -- run --spawn-vortex --spawn-redis-baseline --repeat 3 --group string
cargo run -p vortex-smoketests -- list --verbose
```

Useful flags:

- `--command GET,SET,...`: run a command subset
- `--group string|key|server`: run a command family
- `--include-stubbed true|false`: include or skip stubbed commands
- `--fail-fast`: stop on first failure
- `--repeat N`: rerun every selected case `N` times
- `--report PATH`: write a markdown report (default: `smoketests/.artifacts/last-run.md`)
- `--spawn-vortex`: build/start `vortex-server` automatically
- `--baseline-url redis://127.0.0.1:6379/`: compare supported commands against an already running Redis baseline
- `--spawn-redis-baseline`: start `redis-server` automatically and use it as the baseline target for supported commands
- `--redis-bin PATH`: use an explicit `redis-server` binary
- `--redis-arg ARG`: pass extra CLI args through to the spawned Redis baseline
- `--bind 127.0.0.1:16379`: bind address for spawned Vortex
- `--vortex-bin PATH`: use an explicit server binary instead of `target/debug/vortex-server`
- `--vortex-arg ARG`: pass extra CLI args through to the spawned Vortex server

When `--spawn-vortex` is used without an explicit `--threads` override, the runner injects `--threads 1` automatically.
When `--spawn-vortex` uses the default workspace binary, the runner rebuilds `vortex-server` before launch so the smoke suite compares Redis against the current workspace code rather than a stale executable.
When a Redis baseline is configured, only commands marked `supported` are differential-checked against Redis. `partial` and `stubbed` commands still run against Vortex only so expected divergence does not drown the signal.

## Support Matrix

### String Commands

Supported:
`APPEND`, `DECR`, `DECRBY`, `GET`, `GETDEL`, `GETEX`, `GETRANGE`, `GETSET`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `MGET`, `MSET`, `MSETNX`, `PSETEX`, `SET`, `SETEX`, `SETNX`, `SETRANGE`, `STRLEN`

### Key Commands

Supported:
`COPY`, `DEL`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `EXPIRETIME`, `KEYS`, `PERSIST`, `PEXPIRE`, `PEXPIREAT`, `PEXPIRETIME`, `PTTL`, `RANDOMKEY`, `RENAME`, `RENAMENX`, `SCAN`, `TOUCH`, `TTL`, `TYPE`, `UNLINK`

### Server / Connection Commands

Supported:
`DBSIZE`, `ECHO`, `FLUSHALL`, `FLUSHDB`, `PING`, `QUIT`, `TIME`

Partial:
`COMMAND`, `INFO`, `SELECT`, `UNWATCH`

Stubbed:
`DISCARD`, `EXEC`, `MULTI`, `WATCH`

## Artifacts

Smoke test artifacts are written under `smoketests/.artifacts/`.

- `last-run.md`: markdown report for the most recent run
- `vortex-server.log`: server log when the suite spawns Vortex itself
- `redis-server.log`: baseline log when the suite spawns Redis itself

## Extending Coverage

To add a new command:

1. Add a new module under `smoketests/src/commands/<command>.rs`
2. Register it in `smoketests/src/commands/mod.rs`
3. Document `tested` and `not_tested` coverage in that module
4. Mark the command honestly as `supported`, `partial`, or `stubbed`
5. Run `just smoke` and update the phase plan if support status changed
