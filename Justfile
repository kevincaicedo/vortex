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

# Run the RESP parser fuzzer for 60 seconds
fuzz duration="60":
    cd fuzz && cargo fuzz run fuzz_resp_parser -- -max_total_time={{duration}}

# List fuzz targets
fuzz-list:
    cd fuzz && cargo fuzz list

# Run Miri on unsafe-containing crates
miri:
    cargo +nightly miri test -p vortex-sync -p vortex-memory

# Code coverage report
coverage:
    cargo llvm-cov --workspace --lcov --output-path lcov.info
    @echo "Coverage report written to lcov.info"

# Generate flamegraph (requires cargo-flamegraph)
flamegraph:
    bash scripts/flamegraph.sh

# Run comparison benchmarks against Redis/Dragonfly
compare:
    bash scripts/compare.sh

# Start the server
run *ARGS:
    cargo run --bin vortex-server -- {{ARGS}}

# Start the CLI client
cli *ARGS:
    cargo run --bin vortex-cli -- {{ARGS}}

# Build Docker development image
docker:
    docker build -f Dockerfile.dev -t vortex-dev .

# Clean build artifacts
clean:
    cargo clean
