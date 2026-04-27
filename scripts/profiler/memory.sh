#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/memory.sh — memory profiling modes
# ─────────────────────────────────────────────────────────────────────────────

# ── Heaptrack ────────────────────────────────────────────────────────────────
run_heaptrack() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    require_cmd heaptrack

    header "Heaptrack"

    local extra_args=()
    [[ "$aof" == "true" ]] && extra_args+=("--aof-enabled")
    [[ -n "$maxmemory" ]] && extra_args+=("--max-memory" "$maxmemory")
    [[ -n "$eviction" ]] && extra_args+=("--eviction-policy" "$eviction")
    [[ -n "${IO_BACKEND:-}" ]] && extra_args+=("--io-backend" "$IO_BACKEND")
    [[ -n "${RING_SIZE:-}" ]] && extra_args+=("--ring-size" "$RING_SIZE")
    [[ -n "${SQPOLL_IDLE_MS:-}" ]] && extra_args+=("--sqpoll-idle-ms" "$SQPOLL_IDLE_MS")

    info "Running: heaptrack vortex-server"
    (
        exec heaptrack -o "${session}/heaptrack" \
            "$PROFILING_BINARY" \
            --bind "${host}:${port}" --threads "$threads" \
            "${extra_args[@]}" \
            >"${session}/server-heaptrack.log" 2>&1
    ) &
    local tool_pid=$!
    local control_pid=""

    control_pid="$(start_profiled_shutdown_controller \
        "heaptrack" "$tool_pid" "$host" "$port" "$command" "$duration" "$clients" 60 \
        "${session}/load-heaptrack.log" "${session}/server-heaptrack.log")"

    wait "$tool_pid" 2>/dev/null || warn "Heaptrack exited with non-zero status"
    [[ -n "$control_pid" ]] && wait "$control_pid" 2>/dev/null || true

    local ht_file
    ht_file="$(ls -t "${session}"/heaptrack*.zst "${session}"/heaptrack*.gz 2>/dev/null | head -1 || true)"
    if [[ -n "$ht_file" ]]; then
        ok "Heaptrack data: ${ht_file}"
        if has_cmd heaptrack_print; then
            heaptrack_print "$ht_file" 2>/dev/null | head -80 > "${session}/heaptrack-summary.txt" || true
            info "Heaptrack summary: ${session}/heaptrack-summary.txt"
        fi
        info "Hint: heaptrack_gui ${ht_file}"
    else
        warn "Heaptrack data was not generated"
    fi
}

# ── Massif ───────────────────────────────────────────────────────────────────
run_massif() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    require_cmd valgrind

    header "Massif"

    local extra_args=()
    [[ "$aof" == "true" ]] && extra_args+=("--aof-enabled")
    [[ -n "$maxmemory" ]] && extra_args+=("--max-memory" "$maxmemory")
    [[ -n "$eviction" ]] && extra_args+=("--eviction-policy" "$eviction")
    [[ -n "${IO_BACKEND:-}" ]] && extra_args+=("--io-backend" "$IO_BACKEND")
    [[ -n "${RING_SIZE:-}" ]] && extra_args+=("--ring-size" "$RING_SIZE")
    [[ -n "${SQPOLL_IDLE_MS:-}" ]] && extra_args+=("--sqpoll-idle-ms" "$SQPOLL_IDLE_MS")

    info "Running: valgrind --tool=massif"
    (
        exec valgrind --tool=massif \
            --massif-out-file="${session}/massif.out" \
            "$PROFILING_BINARY" \
            --bind "${host}:${port}" --threads "$threads" \
            "${extra_args[@]}" \
            >"${session}/server-massif.log" 2>&1
    ) &
    local tool_pid=$!
    local control_pid=""

    control_pid="$(start_profiled_shutdown_controller \
        "massif" "$tool_pid" "$host" "$port" "$command" "$duration" "$clients" 60 \
        "${session}/load-massif.log" "${session}/server-massif.log")"

    wait "$tool_pid" 2>/dev/null || warn "Massif exited with non-zero status"
    [[ -n "$control_pid" ]] && wait "$control_pid" 2>/dev/null || true

    if [[ -f "${session}/massif.out" ]]; then
        ok "Massif data: ${session}/massif.out"
        if has_cmd ms_print; then
            ms_print "${session}/massif.out" 2>/dev/null | head -60 > "${session}/massif-summary.txt" || true
            info "Massif summary: ${session}/massif-summary.txt"
        fi
    fi
}

# ── Instruments Memory (macOS) ───────────────────────────────────────────────
run_instruments_memory() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    require_cmd xcrun

    header "Instruments Memory Profiling (xctrace)"
    ensure_macos_debuggable_binary "$PROFILING_BINARY"

    local templates=("Allocations" "Leaks")

    for template in "${templates[@]}"; do
        info "Setting up for template: ${template}"
        local suffix_name="${template// /-}"
        suffix_name="$(echo "$suffix_name" | tr '[:upper:]' '[:lower:]')"
        local trace_log="${session}/instruments-${suffix_name}.log"
        local server_log="${session}/server-instruments-${suffix_name}.log"
        local load_log="${session}/load-alloc-${suffix_name}.log"

        start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "$server_log"

        info "Running: xcrun xctrace record --template '${template}' --attach ${SERVER_PID}"
        (cd "$REPO_ROOT" && exec xcrun xctrace record \
            --template "${template}" \
            --output "${session}/${suffix_name}.trace" \
            --time-limit "${duration}s" \
            --attach "$SERVER_PID" \
            >"${trace_log}" 2>&1) &
        local tool_pid=$!

        sleep 2

        if ! kill -0 "$tool_pid" 2>/dev/null; then
            warn "xctrace exited before load generation started for ${template}. See ${trace_log}"
        elif [[ -n "$command" ]]; then
            generate_load "$host" "$port" "$command" "$duration" "$clients" "$load_log"
        fi

        wait_for_load "$duration"

        info "Stopping xctrace by cleanly shutting down server..."
        pkill -INT -x vortex-server || true
        wait "$tool_pid" 2>/dev/null || true

        if [[ -f "$trace_log" ]] && grep -Eq "Recording failed with errors|Failed to attach to target|Instruments wants permission to analyze other processes" "$trace_log"; then
            fatal "Instruments ${template} capture failed. See ${trace_log}. Ensure the profiling binary remains signed with com.apple.security.get-task-allow."
        fi

        if [[ -d "${session}/${suffix_name}.trace" ]]; then
            ok "Instruments trace: ${session}/${suffix_name}.trace"
            info "Exporting Table of Contents to XML..."
            xcrun xctrace export --input "${session}/${suffix_name}.trace" --toc --output "${session}/${suffix_name}-toc.xml" >/dev/null 2>&1 || true

            local detail_xpath=""
            if [[ "${template}" == "Allocations" ]]; then
                info "Exporting ${template} data to XML..."
                xcrun xctrace export --input "${session}/${suffix_name}.trace" \
                    --xpath '/trace-toc/run[@number="1"]/tracks/track[@name="Allocations"]/details/detail[@name="Allocations List"]' \
                    --output "${session}/${suffix_name}-data.xml" >/dev/null 2>&1 || true
                info "Exporting ${template} statistics to XML..."
                xcrun xctrace export --input "${session}/${suffix_name}.trace" \
                    --xpath '/trace-toc/run[@number="1"]/tracks/track[@name="Allocations"]/details/detail[@name="Statistics"]' \
                    --output "${session}/${suffix_name}-stats.xml" >/dev/null 2>&1 || true
            else
                detail_xpath='/trace-toc/run[@number="1"]/tracks/track[@name="Leaks"]/details/detail[@name="Leaks"]'
                info "Exporting ${template} data to XML..."
                xcrun xctrace export --input "${session}/${suffix_name}.trace" \
                    --xpath "$detail_xpath" \
                    --output "${session}/${suffix_name}-data.xml" >/dev/null 2>&1 || true
            fi
        else
            warn "Instruments trace was not generated for ${template}. See ${trace_log}"
        fi

        _profiler_cleanup
        SERVER_PID=""
        LOAD_PID=""
        sleep 2
    done
}

# ── Composite: --memory runs the best available memory tool ──────────────────
run_memory_all() {
    local session="$1"
    shift

    info "Running memory profiling suite for ${OS}..."

    if has_cmd heaptrack; then
        run_heaptrack "${session}" "$@"
    elif [[ "$OS" == "macos" ]] && has_cmd xcrun; then
        run_instruments_memory "${session}" "$@"
    elif has_cmd valgrind; then
        run_massif "${session}" "$@"
    else
        fatal "No memory profiling tool found. Install heaptrack, valgrind, or Xcode."
    fi
}
