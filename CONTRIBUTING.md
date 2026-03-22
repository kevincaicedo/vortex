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

# Run fuzzer (requires cargo-fuzz)
cd fuzz && cargo fuzz run fuzz_resp_parser -- -max_total_time=60
```

## Development Workflow

### Branch Naming

- `feat/<short-description>` — New features
- `fix/<short-description>` — Bug fixes
- `refactor/<short-description>` — Code restructuring
- `ci/<short-description>` — CI/CD changes
- `bench/<short-description>` — Benchmark changes

### Pull Request Process

1. Create a branch from `main` with the naming convention above.
2. Ensure all CI checks pass (clippy, fmt, test, deny, miri, sanitizers).
3. Include tests for any new functionality.
4. Update documentation if public APIs change.
5. Request review from at least one maintainer.

### Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add EXPIRE command implementation
fix: handle nil bulk string in pipeline parser
refactor: extract TTL logic into dedicated module
test: add proptest for VortexKey SSO boundary
ci: add ASAN job for vortex-engine
docs: document Command trait usage
bench: add pipeline throughput benchmark
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
  4. Approved in code review by a second maintainer.

- **Clippy pedantic** — Enabled as warnings. Key allowed lints:
  `module_name_repetitions`, `must_use_candidate`, `missing_errors_doc`,
  `missing_panics_doc`.

- **`unwrap_used` / `expect_used`** — Warned. Prefer `?` propagation or
  explicit error handling. `expect()` is acceptable in tests and `main()`.

### Formatting

Run `cargo fmt` before committing. CI enforces `cargo fmt --check`.

## Benchmark Regression Policy

- CI runs criterion benchmarks on every PR and compares against `main`.
- **Any regression > 5% blocks the PR** until investigated.
- If the regression is intentional (e.g. trading throughput for correctness),
  document the tradeoff in the PR description and get maintainer approval.
- Run `cargo bench -p vortex-bench` locally before pushing perf-sensitive changes.

## Fuzz Testing

- The RESP parser is continuously fuzzed via nightly CI (`cargo fuzz`).
- **Fuzz failure response SLA:** Any crash found by the fuzzer must be triaged
  within 24 hours and patched within 72 hours.
- Crash reproducers are automatically uploaded as CI artifacts and a GitHub
  issue is opened with the `bug` and `fuzzing` labels.
- To run locally: `cd fuzz && cargo fuzz run fuzz_resp_parser -- -max_total_time=300`

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
