# ──────────────────────────────────────────────────────────────────
#  VortexDB Production Image  —  Multi-stage build
#
#  Build:  docker build -t vortexdb .
#  Run:    docker run -p 6379:6379 vortexdb
# ──────────────────────────────────────────────────────────────────

# ── Stage 1: Build ────────────────────────────────────────────────
FROM rust:1.85-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config linux-headers-generic liburing-dev \
    && rm -rf /var/lib/apt/lists/*

# Install nightly toolchain — override rust-toolchain.toml to skip
# dev-only components (miri, rust-src, llvm-tools-preview).
ENV RUSTUP_TOOLCHAIN=nightly-2026-03-15
RUN rustup toolchain install nightly-2026-03-15

WORKDIR /build
COPY . .

RUN cargo build --release --bin vortex-server \
    && strip target/release/vortex-server

# ── Stage 2: Runtime ──────────────────────────────────────────────
FROM gcr.io/distroless/cc-debian12

COPY --from=builder /build/target/release/vortex-server /usr/local/bin/vortex-server

EXPOSE 6379

ENTRYPOINT ["vortex-server"]
CMD ["--bind", "0.0.0.0:6379"]
