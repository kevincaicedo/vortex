#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/build.sh — build vortex-server with the profiling profile
# ─────────────────────────────────────────────────────────────────────────────

# Expects common.sh to be sourced already.

build_profiling_binary() {
    local bin_override="$1"

    if profiling_target_is_engine; then
        build_engine_profiling_binary
        return 0
    fi

    if [[ -n "$bin_override" ]]; then
        if [[ ! -x "$bin_override" ]]; then
            fatal "--bin path '${bin_override}' does not exist or is not executable"
        fi
        PROFILING_BINARY="$bin_override"
        PROFILING_CARGO_PROFILE="external"
        ensure_macos_debuggable_binary "$PROFILING_BINARY"
        ok "Using pre-built binary: ${PROFILING_BINARY}"
        return 0
    fi

    PROFILING_CARGO_PROFILE="profiling"
    info "Building vortex-server (--profile profiling --features profile-telemetry)..."
    if ! (cd "$REPO_ROOT" && cargo build --profile profiling --features profile-telemetry --bin vortex-server 2>&1); then
        fatal "Build failed. Fix compilation errors and retry."
    fi
    ensure_macos_debuggable_binary "$PROFILING_BINARY"
    ok "Build complete: ${PROFILING_BINARY}"
}

build_engine_profiling_binary() {
    if [[ -n "${ENGINE_BIN_OVERRIDE:-}" ]]; then
        if [[ ! -x "$ENGINE_BIN_OVERRIDE" ]]; then
            fatal "--engine-bin path '${ENGINE_BIN_OVERRIDE}' does not exist or is not executable"
        fi
        PROFILING_BINARY="$ENGINE_BIN_OVERRIDE"
        PROFILING_CARGO_PROFILE="external"
        ensure_macos_debuggable_binary "$PROFILING_BINARY"
        ok "Using pre-built engine binary: ${PROFILING_BINARY}"
        return 0
    fi

    local example_name="${ENGINE_EXAMPLE:-engine_probe}"
    PROFILING_CARGO_PROFILE="profiling"
    PROFILING_BINARY="${REPO_ROOT}/target/profiling/examples/${example_name}"

    info "Building vortex-engine example '${example_name}' (--profile profiling)..."
    if ! (cd "$REPO_ROOT" && cargo build -p vortex-engine --profile profiling --features profiling-tools --example "$example_name" 2>&1); then
        fatal "Engine target build failed. Fix compilation errors and retry."
    fi

    ensure_macos_debuggable_binary "$PROFILING_BINARY"
    ok "Build complete: ${PROFILING_BINARY}"
}
