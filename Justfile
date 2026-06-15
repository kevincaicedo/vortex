# VortexDB development commands
# Install: cargo install just

# Default: run all checks
default: check test clippy fmt-check deny

# Build the entire workspace
build:
    cargo build --workspace

# Run all tests
test *ARGS:
    cargo test --workspace {{ ARGS }}

# Run smoke tests against a freshly spawned Vortex server
smoke *ARGS:
    bash smoketests/scripts/run-local.sh {{ ARGS }}

# Run smoke tests against an already running Redis-compatible endpoint
smoke-existing *ARGS:
    bash smoketests/scripts/run-existing.sh {{ ARGS }}

# Run real-server AOF startup/replay/backpressure smoke tests
smoke-aof *ARGS:
    cargo test -p vortex-smoketests --test server_startup -- {{ ARGS }}

# Run the alpha correctness matrix. Use `--profile full --fail-on-skip` for release gating.
alpha-correctness *ARGS:
    python3 scripts/alpha_correctness_matrix.py {{ ARGS }}

# Generate or run the VAL-ALPHA-002 performance matrix manifests.
alpha-performance-matrix *ARGS:
    python3 scripts/alpha_performance_matrix.py {{ ARGS }}

# List smoke-test command coverage and support status
smoke-list *ARGS:
    cargo run -p vortex-smoketests -- list {{ ARGS }}

# Run clippy lints
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Check formatting
fmt-check:
    cargo fmt --check

# Auto-format code
fmt:
    cargo fmt

# Type-check without building
check:
    cargo check --workspace

# Dependency audit
deny:
    cargo deny check

# Run benchmarks
bench:
    cargo bench -p vortex-engine
    cargo bench -p vortex-bench

# Run engine micro-benchmarks only
bench-engine *ARGS:
    cargo bench -p vortex-engine {{ ARGS }}

# Validate benchmarks against Phase 3 performance targets
bench-validate:
    bash scripts/validate-benchmarks.sh

# Validate benchmarks (CI mode — fail on missed targets)
bench-validate-ci:
    bash scripts/validate-benchmarks.sh --ci --json

# Benchmark runner: one public benchmark entrypoint. Run `just benchmark --help`.
benchmark *ARGS:
    @bash vortex-benchmark/bin/vortex_bench {{ ARGS }}

# Compatibility shortcut; prefer `just benchmark ...` with --target-mode/--native/--container.
[private]
benchmark-local *ARGS:
    @bash vortex-benchmark/bin/vortex_bench_local {{ ARGS }}

# Profiling tool manager — see `just profiler --help`
profiler *ARGS:
    @bash scripts/profiler.sh {{ ARGS }}

# Profiling tool manager for engine-only targets such as engine_probe
[private]
profiler-engine *ARGS:
    @bash scripts/profiler.sh --engine-example engine_probe {{ ARGS }}

# Engine/server/Redis memory attribution matrix
memory-attribution *ARGS:
    bash scripts/memory-attribution-matrix.sh {{ ARGS }}

# Run the RESP parser fuzzer for 60 seconds
fuzz duration="60":
    cd fuzz && cargo fuzz run fuzz_resp_parser -- -max_total_time={{ duration }}

# List fuzz targets
fuzz-list:
    cd fuzz && cargo fuzz list

# Run Miri on all unsafe-containing crates (optimized — safe-API tests excluded)
miri:
    @echo "Running Miri on vortex-sync + vortex-memory (strict provenance)..."
    MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check -Zmiri-disable-isolation" \
        cargo miri test -p vortex-sync -p vortex-memory
    @echo "Running Miri on vortex-engine (symbolic alignment)..."
    MIRIFLAGS="-Zmiri-symbolic-alignment-check -Zmiri-disable-isolation" \
        cargo miri test -p vortex-engine

# Run only the fast Miri subset (sync + memory, ~25s)
miri-fast:
    MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check -Zmiri-disable-isolation" \
        cargo miri test -p vortex-sync -p vortex-memory

# Run full Miri including proptest (slow — may take >30 min)
miri-full:
    MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check -Zmiri-disable-isolation" \
        cargo miri test -p vortex-sync -p vortex-memory
    MIRIFLAGS="-Zmiri-symbolic-alignment-check -Zmiri-disable-isolation" \
        PROPTEST_CASES=16 cargo miri test -p vortex-engine

# Code coverage report
coverage:
    cargo llvm-cov --workspace --lcov --output-path lcov.info
    @echo "Coverage report written to lcov.info"

# Generate flamegraph (shorthand for `just profiler --flamegraph`)
[private]
flamegraph *ARGS:
    @bash scripts/profiler.sh --flamegraph {{ ARGS }}

# Compatibility shortcut; prefer `just benchmark ...` with --db/--backend/--target-mode.
[private]
compare *ARGS:
    @bash scripts/compare.sh {{ ARGS }}

# Compatibility shortcut; prefer `just benchmark --backend memtier_benchmark ...`.
[private]
compare-memtier *ARGS:
    @bash scripts/compare.sh --memtier --json --markdown {{ ARGS }}

# Compatibility shortcut; prefer `just benchmark --container ...`.
[private]
compare-docker *ARGS:
    @bash scripts/compare.sh --docker-all --json --markdown --latency --custom --memtier {{ ARGS }}

# Compatibility shortcut; prefer `just benchmark --native ...`.
[private]
compare-native *ARGS:
    @bash scripts/compare.sh --native --json --markdown --latency --custom --memtier {{ ARGS }}

# Compatibility shortcut; prefer `just benchmark ...` and `just benchmark report ...`.
[private]
compare-full:
    @bash scripts/compare.sh --json --markdown --latency --runs 3 --custom --memtier

# Run custom command benchmarks against a running server
bench-commands port="16379":
    bash scripts/bench-commands.sh -p {{ port }}

# Start the server
run *ARGS:
    cargo run --bin vortex-server -- {{ ARGS }}

# Start the CLI client
cli *ARGS:
    cargo run --bin vortex-cli -- {{ ARGS }}

# Build Docker development image
docker:
    docker build -f Dockerfile.dev -t vortex-dev .

# Build Docker production image
docker-prod:
    docker build -t vortexdb:latest .

# Clean build artifacts
clean:
    cargo clean
