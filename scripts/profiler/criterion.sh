#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/criterion.sh — Criterion micro-benchmark mode
# ─────────────────────────────────────────────────────────────────────────────

run_criterion_mode() {
    local session="$1" package="$2" bench_target="$3" filter="$4"

    header "Criterion Micro-Benchmarks"

    local cmd=(cargo bench -p "$package" --profile profiling)

    if [[ -n "$bench_target" ]]; then
        cmd+=(--bench "$bench_target")
    fi

    if [[ -n "$filter" ]]; then
        cmd+=(-- "$filter")
    fi

    info "Running: ${cmd[*]}"
    (cd "$REPO_ROOT" && "${cmd[@]}") 2>&1 | tee "${session}/criterion.log"
    local exit_code=${PIPESTATUS[0]}

    # Copy criterion output if it exists
    if [[ -d "${REPO_ROOT}/target/criterion" ]]; then
        cp -R "${REPO_ROOT}/target/criterion" "${session}/criterion/" 2>/dev/null || true
        ok "Criterion artifacts copied to: ${session}/criterion/"
    fi

    if [[ $exit_code -eq 0 ]]; then
        ok "Criterion complete"
    else
        warn "Criterion exited with code ${exit_code}"
    fi
}
