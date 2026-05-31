# Alpha Correctness Matrix

The alpha correctness matrix is the minimum gate before accepting optimization work that changes IO state, transaction semantics, AOF, table layout, entry metadata, or command execution.

Run the core loop gate from the workspace root:

```bash
just alpha-correctness
```

Run the release gate with real-server smoke rows:

```bash
just alpha-correctness --profile full --fail-on-skip
```

Rows may be skipped only with an explicit reason:

```bash
just alpha-correctness --skip smoketest-command-compat="no command surface changed"
```

Skipping is an engineering shortcut, not a release pass. Release candidates use `--profile full --fail-on-skip`.

## Matrix Rows

| Row | Profile | Purpose |
| --- | --- | --- |
| `cargo-vortex-io` | core | IO lifetime, backend, reactor, shutdown, fixed-buffer, IPv6 bind, timer, and transaction reactor tests. |
| `cargo-vortex-engine` | core | Engine command semantics, WATCH/MULTI/EXEC, deferred effects, optimistic mutation, table cursor, and unsafe entry invariants. |
| `cargo-vortex-proto` | core | RESP parser, serializer, scanner, checked-in fuzz corpus, and deterministic parser mutation tests. |
| `cargo-vortex-persist` | core | AOF record, replay, rewrite, fsync, and failure-injection correctness. |
| `smoketest-build` | core | Ensures the smoke harness still compiles after command or server changes. |
| `smoketest-list` | core | Lists command coverage and keeps smoke registration visible. |
| `smoketest-command-compat` | smoke | Runs command smoke tests through the Redis client against Vortex and a Redis baseline. |
| `smoketest-aof` | smoke | Runs real-server AOF startup, replay, truncation, and backpressure smoke tests. |

## Required Coverage

The report maps every required invariant to a row and representative test names:

- package gates: `cargo test -p vortex-io`, `cargo test -p vortex-engine`, `cargo test -p vortex-proto`, and `cargo test -p vortex-persist`
- invalid completion tokens and cancellation races
- shutdown with in-flight read/write/writev/close/cancel
- fixed-buffer index limits and late accept during drain
- large bulk frames across reads and `IOV_MAX + 1` pipelines
- RESP parser fuzz-corpus replay, deterministic parser mutations, and serializer/iovec length bounds
- high-fd polling registration and IPv6 bind
- timer deadline bounds
- multi-reactor AOF enable/disable/replay
- WATCH/MULTI/EXEC conflict and partial-visibility tests
- table live-slot/eager-entry safety and unsafe table invariants
- deferred-effect publication races for WATCH, TTL, AOF, eviction, and memory accounting
- optimistic prepare/revalidate/swap tests for overwrite, delete, expire, rename/copy, maxmemory, and AOF-visible mutations
- borrowed multi-key duplicate semantics for EXISTS, DEL, MSET, MSETNX, TOUCH, COPY, RENAME, and transactions
- fused table cursor/property tests for TTL, LSN, raw bytes mutation, heap keys, heap values, resize, tombstone reuse, and delete-heavy workloads

`ENG-ALPHA-002` live-slot `MaybeUninit<Entry>` was rejected and reverted. The current matrix therefore protects the eager-entry/live-slot safety boundary that exists today. Any future live-slot initialization retry must add focused Miri/sanitizer-compatible rows before it can replace this note.
