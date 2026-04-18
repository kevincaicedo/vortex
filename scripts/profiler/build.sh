#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/build.sh — build vortex-server with the profiling profile
# ─────────────────────────────────────────────────────────────────────────────

# Expects common.sh to be sourced already.

build_profiling_binary() {
    local bin_override="$1"

    if [[ -n "$bin_override" ]]; then
        if [[ ! -x "$bin_override" ]]; then
            fatal "--bin path '${bin_override}' does not exist or is not executable"
        fi
        PROFILING_BINARY="$bin_override"
        ensure_macos_debuggable_binary "$PROFILING_BINARY"
        ok "Using pre-built binary: ${PROFILING_BINARY}"
        return 0
    fi

    info "Building vortex-server (--profile profiling)..."
    if ! (cd "$REPO_ROOT" && cargo build --profile profiling --bin vortex-server 2>&1); then
        fatal "Build failed. Fix compilation errors and retry."
    fi
    ensure_macos_debuggable_binary "$PROFILING_BINARY"
    ok "Build complete: ${PROFILING_BINARY}"
}
