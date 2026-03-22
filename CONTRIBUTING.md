# Contributing to VortexDB

## Build & Test

```bash
# Full workspace build
cargo build --workspace

# Run all tests
cargo test --workspace

# Clippy lint check
cargo clippy --workspace --all-targets -- -D warnings

# Format check
cargo fmt --check

# Dependency audit
cargo deny check
```

## Code Quality

### Workspace Lints

Lints are configured in the root `Cargo.toml` under `[workspace.lints]`:

- **`unsafe_code = "forbid"`** — No `unsafe` blocks anywhere. This is enforced
  at the workspace level. If a future phase requires `unsafe` (e.g. SIMD
  intrinsics, custom allocators), it must be:
  1. Gated behind a feature flag.
  2. Accompanied by a `// SAFETY:` comment explaining the invariants.
  3. Covered by a Miri test (`cargo +nightly miri test`).
  4. Approved in code review.

- **Clippy pedantic** — Enabled as warnings. Key allowed lints:
  `module_name_repetitions`, `must_use_candidate`, `missing_errors_doc`,
  `missing_panics_doc`.

- **`unwrap_used` / `expect_used`** — Warned. Prefer `?` propagation or
  explicit error handling. `expect()` is acceptable in tests and `main()`.

### Formatting

Run `cargo fmt` before committing. CI enforces `cargo fmt --check`.

### Commit Messages

Use conventional commits: `feat:`, `fix:`, `refactor:`, `test:`, `ci:`, `docs:`.

## Architecture

VortexDB is a 17-crate workspace. The dependency hierarchy (bottom → top):

```
vortex-common        ← foundation types, errors, constants
vortex-memory        ← jemalloc allocator
vortex-sync          ← lock-free primitives
vortex-proto         ← RESP2/RESP3 parser & serializer
vortex-engine        ← data engine, commands, shard
vortex-persist       ← AOF, RDB, snapshots
vortex-io            ← io_uring / epoll event loop
vortex-config        ← configuration loading
vortex-cluster       ← cluster topology
vortex-replication   ← leader/follower replication
vortex-pubsub        ← pub/sub channels
vortex-scripting     ← Lua scripting
vortex-acl           ← ACL & auth
vortex-metrics       ← Prometheus metrics
vortex-server        ← main binary, startup, shutdown
```

### Testing

- **Unit tests**: In each crate alongside the code (`#[cfg(test)]` modules).
- **Property tests**: Using `proptest` for invariant-based testing.
- **Benchmarks**: Using `criterion` in `tools/vortex-bench/benches/`.
- **Fuzz targets**: In `fuzz/` using `cargo-fuzz`.

### CI Pipeline

CI runs on every push and PR:

1. **check** — `cargo check --workspace`
2. **test** — `cargo test --workspace`
3. **clippy** — `cargo clippy --workspace --all-targets -- -D warnings`
4. **fmt** — `cargo fmt --check`
5. **deny** — `cargo deny check`
6. **miri** — Memory safety checks on `vortex-common` and `vortex-engine`
7. **tsan** — ThreadSanitizer on `vortex-sync`
8. **asan** — AddressSanitizer on `vortex-engine`
