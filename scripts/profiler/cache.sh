#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/cache.sh — cache locality and call-graph profiling
# ─────────────────────────────────────────────────────────────────────────────

# ── Cachegrind ───────────────────────────────────────────────────────────────
run_cachegrind() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    require_cmd valgrind

    header "Cachegrind"
    warn "Cachegrind runs under Valgrind (20-100x slowdown). Using --threads 1 is recommended."

    local extra_args=()
    [[ "$aof" == "true" ]] && extra_args+=("--aof-enabled")
    [[ -n "$maxmemory" ]] && extra_args+=("--max-memory" "$maxmemory")
    [[ -n "$eviction" ]] && extra_args+=("--eviction-policy" "$eviction")
    [[ -n "${IO_BACKEND:-}" ]] && extra_args+=("--io-backend" "$IO_BACKEND")
    [[ -n "${RING_SIZE:-}" ]] && extra_args+=("--ring-size" "$RING_SIZE")
    [[ -n "${SQPOLL_IDLE_MS:-}" ]] && extra_args+=("--sqpoll-idle-ms" "$SQPOLL_IDLE_MS")

    info "Running: valgrind --tool=cachegrind"
    (
        exec valgrind --tool=cachegrind \
            --cachegrind-out-file="${session}/cachegrind.out" \
            "$PROFILING_BINARY" \
            --bind "${host}:${port}" --threads "$threads" \
            "${extra_args[@]}" \
            >"${session}/server-cachegrind.log" 2>&1
    ) &
    local tool_pid=$!
    local control_pid=""

    control_pid="$(start_profiled_shutdown_controller \
        "cachegrind" "$tool_pid" "$host" "$port" "$command" "$duration" "$clients" 60 \
        "${session}/load-cachegrind.log" "${session}/server-cachegrind.log")"

    wait "$tool_pid" 2>/dev/null || warn "Cachegrind exited with non-zero status"
    [[ -n "$control_pid" ]] && wait "$control_pid" 2>/dev/null || true

    if [[ -f "${session}/cachegrind.out" ]]; then
        ok "Cachegrind data: ${session}/cachegrind.out"
        if has_cmd cg_annotate; then
            cg_annotate "${session}/cachegrind.out" 2>/dev/null \
                | head -80 > "${session}/cachegrind-summary.txt" || true
            info "Cachegrind summary: ${session}/cachegrind-summary.txt"
            echo ""
            head -40 "${session}/cachegrind-summary.txt" 2>/dev/null || true
        fi
    fi
}

# ── Callgrind ────────────────────────────────────────────────────────────────
run_callgrind() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    require_cmd valgrind

    header "Callgrind"
    warn "Callgrind runs under Valgrind (20-100x slowdown). Using --threads 1 is recommended."

    local extra_args=()
    [[ "$aof" == "true" ]] && extra_args+=("--aof-enabled")
    [[ -n "$maxmemory" ]] && extra_args+=("--max-memory" "$maxmemory")
    [[ -n "$eviction" ]] && extra_args+=("--eviction-policy" "$eviction")
    [[ -n "${IO_BACKEND:-}" ]] && extra_args+=("--io-backend" "$IO_BACKEND")
    [[ -n "${RING_SIZE:-}" ]] && extra_args+=("--ring-size" "$RING_SIZE")
    [[ -n "${SQPOLL_IDLE_MS:-}" ]] && extra_args+=("--sqpoll-idle-ms" "$SQPOLL_IDLE_MS")

    info "Running: valgrind --tool=callgrind --simulate-cache=yes --collect-jumps=yes"
    (
        exec valgrind --tool=callgrind \
            --callgrind-out-file="${session}/callgrind.out" \
            --simulate-cache=yes \
            --collect-jumps=yes \
            "$PROFILING_BINARY" \
            --bind "${host}:${port}" --threads "$threads" \
            "${extra_args[@]}" \
            >"${session}/server-callgrind.log" 2>&1
    ) &
    local tool_pid=$!
    local control_pid=""

    control_pid="$(start_profiled_shutdown_controller \
        "callgrind" "$tool_pid" "$host" "$port" "$command" "$duration" "$clients" 60 \
        "${session}/load-callgrind.log" "${session}/server-callgrind.log")"

    wait "$tool_pid" 2>/dev/null || warn "Callgrind exited with non-zero status"
    [[ -n "$control_pid" ]] && wait "$control_pid" 2>/dev/null || true

    if [[ -f "${session}/callgrind.out" ]]; then
        ok "Callgrind data: ${session}/callgrind.out"
        info "Hint: kcachegrind ${session}/callgrind.out"
        if has_cmd callgrind_annotate; then
            callgrind_annotate "${session}/callgrind.out" 2>/dev/null \
                | head -80 > "${session}/callgrind-summary.txt" || true
            info "Callgrind summary: ${session}/callgrind-summary.txt"
        fi
    fi
}
