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

    if [[ -n "$command" ]]; then
        (
            sleep 8
            if wait_for_server_ready "$host" "$port" 120; then
                generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-cachegrind.log"
            else
                warn "Skipping cachegrind load generation because the server never became ready. See ${session}/server-cachegrind.log"
            fi
        ) &
        local bg_load=$!
    fi

    info "Running: valgrind --tool=cachegrind"
    valgrind --tool=cachegrind \
        --cachegrind-out-file="${session}/cachegrind.out" \
        "$PROFILING_BINARY" \
        --bind "${host}:${port}" --threads "$threads" \
        "${extra_args[@]}" \
        >"${session}/server-cachegrind.log" 2>&1 || warn "Cachegrind exited with non-zero status"

    [[ -n "${bg_load:-}" ]] && { kill "$bg_load" 2>/dev/null || true; wait "$bg_load" 2>/dev/null || true; }

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

    if [[ -n "$command" ]]; then
        (
            sleep 8
            if wait_for_server_ready "$host" "$port" 120; then
                generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-callgrind.log"
            else
                warn "Skipping callgrind load generation because the server never became ready. See ${session}/server-callgrind.log"
            fi
        ) &
        local bg_load=$!
    fi

    info "Running: valgrind --tool=callgrind --simulate-cache=yes --collect-jumps=yes"
    valgrind --tool=callgrind \
        --callgrind-out-file="${session}/callgrind.out" \
        --simulate-cache=yes \
        --collect-jumps=yes \
        "$PROFILING_BINARY" \
        --bind "${host}:${port}" --threads "$threads" \
        "${extra_args[@]}" \
        >"${session}/server-callgrind.log" 2>&1 || warn "Callgrind exited with non-zero status"

    [[ -n "${bg_load:-}" ]] && { kill "$bg_load" 2>/dev/null || true; wait "$bg_load" 2>/dev/null || true; }

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
