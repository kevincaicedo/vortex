# Contributing to VortexDB

## Development Setup

### Prerequisites

- **Rust nightly** — auto-installed via `rust-toolchain.toml` (nightly-2026-03-15)
- **Just** — command runner: `cargo install just`
- **cargo-fuzz** — for fuzzing: `cargo install cargo-fuzz`
- **cargo-deny** — for dependency audit: `cargo install cargo-deny`
- **redis-benchmark** — for competitive benchmarks: `brew install redis` (macOS) or `apt install redis-tools` (Linux)
- **Docker** — for competitor database benchmarks

### First Build

```bash
git clone https://github.com/vortexdb/vortex.git
cd vortex
just  # runs: check, test, clippy, fmt-check, deny
```

## Common Commands

```bash
# Full CI check (runs all quality gates)
just

# Individual commands
just build          # cargo build --workspace
just test           # cargo test --workspace (485 tests)
just clippy         # clippy with -D warnings
just fmt            # auto-format
just fmt-check      # format check (CI gate)
just deny           # dependency audit
just bench          # run 69 Criterion benchmarks

# Miri memory safety checks
just miri           # optimized: ~42s (sync + memory + engine)
just miri-fast      # fast subset: ~25s (sync + memory only)
just miri-full      # full: includes proptest (slow)

# Competitive benchmarks
just compare                           # quick comparison vs Redis/Dragonfly/Valkey
just compare --latency --markdown      # with latency percentiles and Markdown output
just compare-docker                    # containerized comparison
just compare-full                      # 3 runs, statistical analysis, all output formats

# Run the server
just run                               # default: 127.0.0.1:6379
just run --bind 0.0.0.0:6380           # custom bind

# Fuzz testing
just fuzz                              # RESP parser fuzzer, 60s
just fuzz 300                          # RESP parser fuzzer, 5 min

# Code coverage
just coverage                          # generates lcov.info

# Docker
just docker-prod                       # build production image
```

## Development Workflow

### Branch Naming

- `feat/<short-description>` — New features
- `fix/<short-description>` — Bug fixes
- `refactor/<short-description>` — Code restructuring
- `ci/<short-description>` — CI/CD changes
- `bench/<short-description>` — Benchmark changes
- `docs/<short-description>` — Documentation changes

### Pull Request Process

1. Create a branch from `main` with the naming convention above
2. Ensure all CI checks pass: `just` (runs check, test, clippy, fmt-check, deny)
3. Include tests for any new functionality
4. Update documentation if public APIs change
5. Run `just miri` if modifying unsafe code
6. Request review from at least one maintainer

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

- **`unsafe_code = "forbid"`** — No `unsafe` blocks in most crates. Crates that require
  `unsafe` (vortex-engine, vortex-io, vortex-memory, vortex-sync) override this in their
  own `Cargo.toml` with `unsafe_code = "allow"`. Every `unsafe` block must have:
  1. A `// SAFETY:` comment explaining the invariants
  2. Miri test coverage (`just miri`)
  3. Approval in code review

- **Clippy pedantic** — Enabled as warnings. Key allowed lints:
  `module_name_repetitions`, `must_use_candidate`, `missing_errors_doc`,
  `missing_panics_doc`

- **`unwrap_used` / `expect_used`** — Warned. Prefer `?` propagation or
  explicit error handling. `expect()` is acceptable in tests and `main()`

### Formatting

Run `cargo fmt` (or `just fmt`) before committing. CI enforces `cargo fmt --check`.

## Testing

### Test Categories

| Type | Count | Command | Description |
|------|-------|---------|-------------|
| Unit tests | ~400 | `cargo test --workspace` | In-crate `#[cfg(test)]` modules |
| Integration tests | ~70 | `cargo test --workspace` | `tests/` directories in crates |
| Property tests | 3 | `cargo test --workspace` | `proptest` in `vortex-engine` (SwissTable invariants) |
| Miri (unsafe safety) | 63 | `just miri` | Memory safety for all `unsafe` code |
| Fuzz (protocol) | 2 targets | `just fuzz` | RESP parser + RDB import |
| Criterion benchmarks | 69 | `just bench` | Performance regression detection |

### Running Specific Tests

```bash
# Single crate
cargo test -p vortex-engine

# Single test
cargo test -p vortex-engine -- cmd_get

# With output
cargo test -p vortex-engine -- --nocapture

# Engine integration tests only
cargo test -p vortex-io --test engine_integration
```

### Miri

Miri verifies that all `unsafe` code is free of undefined behavior. Three crates contain `unsafe`:

- **vortex-sync** — SPSC ring buffer, MPSC queue, sharded counter
- **vortex-memory** — mmap buffer pool, arena allocator
- **vortex-engine** — Swiss Table SIMD probing, cache-line entry layout

```bash
just miri        # All three crates, optimized (~42s)
just miri-fast   # sync + memory only (~25s)
```

**Note:** `vortex-engine` runs Miri without `-Zmiri-strict-provenance` because `entry.rs` uses a deliberate `ptr → usize → bytes → usize → ptr` round-trip for the 64-byte cache-line entry layout. Strict provenance is enabled for all other crates.

### Fuzz Testing

```bash
# Run RESP parser fuzzer
just fuzz 300  # 5 minutes

# List all fuzz targets
just fuzz-list

# Reproduce a crash
cd fuzz && cargo fuzz run fuzz_resp_parser crash-<hash>
```

**Fuzz policy:**
- Crashes must be triaged within 24 hours
- Patches must land within 72 hours
- Crash reproducers are uploaded as CI artifacts

## Benchmark Regression Policy

- CI runs Criterion benchmarks on every PR and compares against `main`
- **Any regression > 5% blocks the PR** until investigated
- If the regression is intentional, document the tradeoff in the PR description
- Run `just bench` locally before pushing performance-sensitive changes

```bash
# Validate against Phase 3 performance targets
just bench-validate

# CI mode (JSON output, fails on missed targets)
just bench-validate-ci
```

## Architecture

VortexDB is a 17-crate workspace. The dependency hierarchy (bottom → top):

```
vortex-common        ← foundation types, errors, constants
vortex-memory        ← jemalloc, mmap buffer pool, arena
vortex-sync          ← lock-free SPSC/MPSC, sharded counters
vortex-proto         ← SIMD RESP2/RESP3 parser & serializer
vortex-config        ← CLI + TOML + env configuration
vortex-engine        ← Swiss Table, shard, commands, TTL, morphing
vortex-io            ← io_uring / kqueue reactor, connection management
vortex-server        ← main binary, startup, shutdown
```

For the full architecture guide: [docs/architecture.md](docs/architecture.md)

### Adding a New Command

1. Add the handler function in the appropriate module under `crates/vortex-engine/src/commands/`:
   - `string.rs` — String commands
   - `key.rs` — Key management commands
   - `server.rs` — Server/connection commands
2. Add the match arm in `crates/vortex-engine/src/commands/mod.rs` → `execute_command()`
3. Add the command metadata in `crates/vortex-proto/build.rs` → PHF table
4. Add unit tests in the command module
5. Add a Criterion benchmark in `tools/vortex-bench/benches/engine.rs`
6. Update [docs/compatibility.md](docs/compatibility.md)

### CI Pipeline

CI runs on every push and PR:

| Check | Tool | Scope |
|-------|------|-------|
| Type check | `cargo check` | Workspace |
| Tests | `cargo test` | Workspace (485 tests) |
| Lints | `cargo clippy` | Workspace, all targets, `-D warnings` |
| Format | `cargo fmt --check` | Workspace |
| Dependencies | `cargo deny check` | License + advisory audit |
| Miri | `cargo miri test` | vortex-sync, vortex-memory, vortex-engine |
| ThreadSanitizer | `cargo test` + TSAN | vortex-sync |
| AddressSanitizer | `cargo test` + ASAN | vortex-engine |
| Benchmarks | `cargo bench` | Regression detection |
