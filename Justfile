# VortexDB development commands
# Install: cargo install just

# Default: run all checks
default: check test clippy fmt-check deny

# Build the entire workspace
build:
    cargo build --workspace

# Run all tests
test:
    cargo test --workspace

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
    cargo bench -p vortex-bench

# Validate benchmarks against Phase 3 performance targets
bench-validate:
    bash scripts/validate-benchmarks.sh

# Validate benchmarks (CI mode — fail on missed targets)
bench-validate-ci:
    bash scripts/validate-benchmarks.sh --ci --json

# Run the RESP parser fuzzer for 60 seconds
fuzz duration="60":
    cd fuzz && cargo fuzz run fuzz_resp_parser -- -max_total_time={{duration}}

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

# Generate flamegraph (requires cargo-flamegraph)
flamegraph:
    bash scripts/flamegraph.sh

# Run comparison benchmarks against Redis/Dragonfly/Valkey
compare *ARGS:
    bash scripts/compare.sh {{ARGS}}

# Fair Docker-based comparison (all databases containerized, identical resources)
compare-docker *ARGS:
    bash scripts/compare.sh --docker-all --json --markdown --latency --custom {{ARGS}}

# Native comparison (VortexDB + Redis both native, no Docker)
compare-native *ARGS:
    bash scripts/compare.sh --native --json --markdown --latency --custom {{ARGS}}

# Run comparison with JSON + Markdown output
compare-full:
    bash scripts/compare.sh --json --markdown --latency --runs 3 --custom

# Run comparison with AOF enabled (measures persistence overhead)
compare-aof *ARGS:
    bash scripts/compare.sh --aof --json --markdown --latency --custom {{ARGS}}

# Run comparison with AOF enabled, native mode
compare-aof-native *ARGS:
    bash scripts/compare.sh --native --aof --json --markdown --latency --custom {{ARGS}}

# Run custom command benchmarks against a running server
bench-commands port="16379":
    bash scripts/bench-commands.sh -p {{port}}

# Start the server
run *ARGS:
    cargo run --bin vortex-server -- {{ARGS}}

# Start the CLI client
cli *ARGS:
    cargo run --bin vortex-cli -- {{ARGS}}

# Build Docker development image
docker:
    docker build -f Dockerfile.dev -t vortex-dev .

# Build Docker production image
docker-prod:
    docker build -t vortexdb:latest .

# Clean build artifacts
clean:
    cargo clean
