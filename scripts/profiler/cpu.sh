#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/cpu.sh — CPU profiling modes
# ─────────────────────────────────────────────────────────────────────────────

# Expects common.sh, build.sh, server.sh to be sourced already.

# ── Flamegraph ───────────────────────────────────────────────────────────────
run_flamegraph() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}" frequency="${11}"

    require_cmd cargo-flamegraph

    header "Flamegraph"

    local extra_args=("--bind" "${host}:${port}" "--threads" "$threads")
    [[ "$aof" == "true" ]] && extra_args+=("--aof-enabled")
    [[ -n "$maxmemory" ]] && extra_args+=("--max-memory" "$maxmemory")
    [[ -n "$eviction" ]] && extra_args+=("--eviction-policy" "$eviction")
    [[ -n "${IO_BACKEND:-}" ]] && extra_args+=("--io-backend" "$IO_BACKEND")
    [[ -n "${RING_SIZE:-}" ]] && extra_args+=("--ring-size" "$RING_SIZE")
    [[ -n "${SQPOLL_IDLE_MS:-}" ]] && extra_args+=("--sqpoll-idle-ms" "$SQPOLL_IDLE_MS")

    # cargo flamegraph uses elevated profiling on both Linux (--root perf)
    # and macOS (DTrace). Request sudo synchronously before backgrounding so
    # HOST_PASSWORD from .env can prime the sudo session and avoid a blocking prompt.
    if [[ "$OS" == "linux" || "$OS" == "macos" ]]; then
        info "cargo flamegraph requires sudo on ${OS}. Requesting privileges..."
        ensure_sudo_access "sudo access required for Flamegraph on ${OS}"
    fi

    info "Running: cargo flamegraph --profile profiling --bin vortex-server -F ${frequency}"
    (cd "$REPO_ROOT" && exec cargo flamegraph \
        --profile profiling \
        --bin vortex-server \
        --root \
        -F "$frequency" \
        -o "${session}/flamegraph.svg" \
        -- "${extra_args[@]}" \
        >"${session}/server-flamegraph.log" 2>&1) &
    local tool_pid=$!

    local target_ready=false
    sleep 4
    if wait_for_server_ready "$host" "$port" 30; then
        target_ready=true
    fi

    # Foreground handles the load
    if [[ -n "$command" ]]; then
        if [[ "$target_ready" == "true" ]]; then
            generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-flamegraph.log"
        else
            warn "Skipping load generation because the flamegraph target never became ready. See ${session}/server-flamegraph.log"
        fi
    fi

    wait_for_load "$duration"

    info "Stopping flamegraph by cleanly shutting down server..."
    if [[ "$OS" == "linux" || "$OS" == "macos" ]]; then
        run_with_sudo "sudo access required to stop the flamegraph target" pkill -INT -x vortex-server || true
        local attempts=0
        while run_with_sudo "sudo access required to inspect the flamegraph target" pgrep -x vortex-server >/dev/null 2>&1; do
            if [[ $attempts -ge 20 ]]; then
                warn "Flamegraph target is still draining; sending a second signal to force shutdown"
                run_with_sudo "sudo access required to force the flamegraph target to stop" pkill -INT -x vortex-server || true
                break
            fi
            sleep 0.25
            attempts=$((attempts + 1))
        done
    else
        pkill -INT -x vortex-server || true
    fi
    wait "$tool_pid" 2>/dev/null || true

    if [[ -f "${session}/flamegraph.svg" ]]; then
        ok "Flamegraph: ${session}/flamegraph.svg"
    else
        warn "Flamegraph SVG was not generated"
    fi
}

# ── perf record ──────────────────────────────────────────────────────────────
run_perf_record() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}" frequency="${11}"

    require_cmd perf

    header "perf record"

    start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-perf.log"
    generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-perf.log"

    info "Running: perf record -F ${frequency} -g --call-graph fp -p ${SERVER_PID}"
    perf record \
        -F "$frequency" \
        -g --call-graph fp \
        -o "${session}/perf.data" \
        -p "$SERVER_PID" \
        -- sleep "$duration" \
        >"${session}/perf-record.log" 2>&1 || true

    wait_for_load "$duration"

    # Generate a text summary
    if [[ -f "${session}/perf.data" ]]; then
        ok "perf data: ${session}/perf.data"
        perf report -i "${session}/perf.data" --stdio --no-children 2>/dev/null \
            | head -80 > "${session}/perf-report.txt" || true
        info "perf report summary: ${session}/perf-report.txt"
    fi

    # Stop server so next tool gets a clean port
    _profiler_cleanup
    SERVER_PID=""
    LOAD_PID=""
}

# ── perf stat ────────────────────────────────────────────────────────────────
run_perf_stat() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    require_cmd perf

    header "perf stat"

    start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-perf-stat.log"
    generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-perf-stat.log"

    info "Running: perf stat -d -p ${SERVER_PID} (${duration}s)"
    perf stat -d \
        -p "$SERVER_PID" \
        -o "${session}/perf-stat.txt" \
        -- sleep "$duration" \
        2>&1 || true

    wait_for_load "$duration"

    if [[ -f "${session}/perf-stat.txt" ]]; then
        ok "perf stat: ${session}/perf-stat.txt"
        echo ""
        cat "${session}/perf-stat.txt" 2>/dev/null || true
    fi

    _profiler_cleanup
    SERVER_PID=""
    LOAD_PID=""
}

# ── samply ───────────────────────────────────────────────────────────────────
run_samply() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    require_cmd samply

    header "samply"

    local extra_args=("--bind" "${host}:${port}" "--threads" "$threads")
    [[ "$aof" == "true" ]] && extra_args+=("--aof-enabled")
    [[ -n "$maxmemory" ]] && extra_args+=("--max-memory" "$maxmemory")
    [[ -n "$eviction" ]] && extra_args+=("--eviction-policy" "$eviction")
    [[ -n "${IO_BACKEND:-}" ]] && extra_args+=("--io-backend" "$IO_BACKEND")
    [[ -n "${RING_SIZE:-}" ]] && extra_args+=("--ring-size" "$RING_SIZE")
    [[ -n "${SQPOLL_IDLE_MS:-}" ]] && extra_args+=("--sqpoll-idle-ms" "$SQPOLL_IDLE_MS")

    info "Running: samply record"
    (cd "$REPO_ROOT" && exec samply record --save-only \
        -o "${session}/samply-profile.json" \
        -- "$PROFILING_BINARY" \
        "${extra_args[@]}" \
        >"${session}/server-samply.log" 2>&1) &
    local tool_pid=$!

    local target_ready=false
    sleep 4
    if wait_for_server_ready "$host" "$port" 30; then
        target_ready=true
    fi

    if [[ -n "$command" ]]; then
        if [[ "$target_ready" == "true" ]]; then
            generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-samply.log"
        else
            warn "Skipping load generation because the samply target never became ready. See ${session}/server-samply.log"
        fi
    fi

    wait_for_load "$duration"

    info "Stopping samply by cleanly shutting down server..."
    pkill -INT -x vortex-server || true
    wait "$tool_pid" 2>/dev/null || true

    if [[ -f "${session}/samply-profile.json" ]]; then
        ok "samply profile: ${session}/samply-profile.json"
    fi
}

# ── Instruments (macOS) ──────────────────────────────────────────────────────
run_instruments() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    require_cmd xcrun

    header "Instruments CPU Profiling (xctrace)"

    local templates=("Time Profiler" "System Trace")

    for template in "${templates[@]}"; do
        info "Setting up for template: ${template}"
        # Convert template name to safe file suffix
        local suffix_name="${template// /-}"
        suffix_name="$(echo "$suffix_name" | tr '[:upper:]' '[:lower:]')"

        start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-instruments-${suffix_name}.log"
        generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-instruments-${suffix_name}.log"

        info "Running: xcrun xctrace record --template '${template}' --attach ${SERVER_PID} (${duration}s)"
        (cd "$REPO_ROOT" && exec xcrun xctrace record \
            --template "${template}" \
            --attach "$SERVER_PID" \
            --output "${session}/${suffix_name}.trace" \
            --time-limit "${duration}s" \
            >"${session}/instruments-${suffix_name}.log" 2>&1) &
        local tool_pid=$!

        wait_for_load "$duration"

        info "Stopping xctrace by cleanly shutting down server..."
        pkill -INT -x vortex-server || true
        wait "$tool_pid" 2>/dev/null || true

        if [[ -d "${session}/${suffix_name}.trace" ]]; then
            ok "Instruments trace: ${session}/${suffix_name}.trace"
            info "Exporting Table of Contents to XML..."
            xcrun xctrace export --input "${session}/${suffix_name}.trace" --toc --output "${session}/${suffix_name}-toc.xml" >/dev/null 2>&1 || true

            # Export primary data table if known
            if [[ "${template}" == "Time Profiler" ]]; then
                info "Exporting Time Profile data to XML..."
                xcrun xctrace export --input "${session}/${suffix_name}.trace" \
                    --xpath '/trace-toc/run[@number="1"]/data/table[@schema="time-profile"]' \
                    --output "${session}/${suffix_name}-data.xml" >/dev/null 2>&1 || true
            fi
        fi

        _profiler_cleanup
        SERVER_PID=""
        LOAD_PID=""
        # Brief pause between sequential captures
        sleep 2
    done
}



# ── Composite: --cpu runs all available CPU tools ────────────────────────────
run_cpu_all() {
    local session="$1"
    shift

    info "Running full CPU profiling suite for ${OS}..."

    # Always try flamegraph
    if has_cmd cargo-flamegraph; then
        run_flamegraph "${session}" "$@"
    else
        warn "cargo-flamegraph not installed — skipping flamegraph"
    fi

    if [[ "$OS" == "linux" ]]; then
        if has_cmd perf; then
            run_perf_stat "${session}" "$@"
            run_perf_record "${session}" "$@"
        else
            warn "perf not installed — skipping perf stat/record"
        fi
    elif [[ "$OS" == "macos" ]]; then
        if has_cmd samply; then
            run_samply "${session}" "$@"
        else
            warn "samply not installed — skipping samply"
        fi
        if has_cmd xcrun; then
            run_instruments "${session}" "$@"
        else
            warn "Xcode Instruments not installed — skipping"
        fi
    fi
}

run_scheduler_focus() {
    local session="$1"
    shift

    info "Running scheduler diagnostics for ${OS}..."

    if [[ "$OS" == "linux" ]] && has_cmd perf; then
        run_perf_stat "$session" "$@"
    elif [[ "$OS" == "macos" ]] && has_cmd xcrun; then
        run_instruments "$session" "$@"
    elif has_cmd samply; then
        run_samply "$session" "$@"
    else
        fatal "No scheduler-focused profiler tool is available on this host."
    fi
}

run_aof_disk_focus() {
    local session="$1"
    shift

    info "Running AOF/disk diagnostics for ${OS}..."

    if [[ "$OS" == "linux" ]] && has_cmd perf; then
        run_perf_stat "$session" "$@"
    elif [[ "$OS" == "macos" ]] && has_cmd xcrun; then
        run_instruments "$session" "$@"
    elif has_cmd samply; then
        run_samply "$session" "$@"
    else
        fatal "No disk-focused profiler tool is available on this host."
    fi
}

run_network_focus() {
    local session="$1"
    shift

    info "Running network diagnostics for ${OS}..."

    if [[ "$OS" == "linux" ]] && has_cmd perf; then
        run_perf_stat "$session" "$@"
    elif has_cmd samply; then
        run_samply "$session" "$@"
    elif [[ "$OS" == "macos" ]] && has_cmd xcrun; then
        run_instruments "$session" "$@"
    else
        fatal "No network-focused profiler tool is available on this host."
    fi
}
