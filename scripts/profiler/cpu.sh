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

    if profiling_target_is_engine; then
        if [[ -n "${ENGINE_BIN_OVERRIDE:-}" ]]; then
            fatal "--flamegraph requires --engine-example because cargo flamegraph profiles Cargo targets"
        fi

        if [[ "$OS" == "linux" || "$OS" == "macos" ]]; then
            info "cargo flamegraph requires sudo on ${OS}. Requesting privileges..."
            ensure_sudo_access "sudo access required for Flamegraph on ${OS}"
        fi

        local example_name="${ENGINE_EXAMPLE:-engine_probe}"
        info "Running: cargo flamegraph --profile profiling -p vortex-engine --features profiling-tools --example ${example_name} -F ${frequency}"
        (cd "$REPO_ROOT" && exec cargo flamegraph \
            --profile profiling \
            -p vortex-engine \
            --features profiling-tools \
            --example "$example_name" \
            --root \
            -F "$frequency" \
            -o "${session}/flamegraph.svg" \
            -- "${ENGINE_TARGET_ARGS[@]}" \
            >"${session}/engine-flamegraph.log" 2>&1)

        if [[ -f "${session}/flamegraph.svg" ]]; then
            ok "Flamegraph: ${session}/flamegraph.svg"
        else
            warn "Flamegraph SVG was not generated"
        fi
        return 0
    fi

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

    info "Running: cargo flamegraph --profile profiling --features profile-telemetry --bin vortex-server -F ${frequency}"
    (cd "$REPO_ROOT" && exec cargo flamegraph \
        --profile profiling \
        --features profile-telemetry \
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

    if profiling_target_is_engine; then
        info "Running: perf record -F ${frequency} -g --call-graph fp"
        perf record \
            -F "$frequency" \
            -g --call-graph fp \
            -o "${session}/perf.data" \
            -- "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" \
            >"${session}/perf-record.log" 2>&1 || true

        if [[ -f "${session}/perf.data" ]]; then
            ok "perf data: ${session}/perf.data"
            perf report -i "${session}/perf.data" --stdio --no-children 2>/dev/null \
                | head -80 > "${session}/perf-report.txt" || true
            info "perf report summary: ${session}/perf-report.txt"
        fi
        return 0
    fi

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
_perf_stat_linux_value() {
    local path="$1"

    if [[ -r "$path" ]]; then
        tr -d '\n' < "$path"
        return 0
    fi

    printf '%s' "unavailable"
}

_perf_stat_linux_first_line() {
    local path="$1"

    if [[ -r "$path" ]]; then
        head -n 1 "$path"
        return 0
    fi

    printf '%s' "unavailable"
}

_perf_stat_single_line() {
    local value="$1"

    printf '%s' "$value" | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//'
}

perf_stat_virtualization_mode() {
    local detected=""

    if has_cmd systemd-detect-virt; then
        detected="$(systemd-detect-virt 2>/dev/null || true)"
        if [[ -n "$detected" && "$detected" != "none" ]]; then
            printf '%s' "$detected"
            return 0
        fi
    fi

    if grep -qi '^flags.* hypervisor' /proc/cpuinfo 2>/dev/null; then
        printf '%s' "hypervisor"
        return 0
    fi

    printf '%s' "none"
}

perf_stat_event_selector_supported() {
    local event_selector="$1"
    local error_path="$2"
    PERF_STAT_EVENT_SELECTOR_NEEDS_SUDO=0

    perf stat -x , -e "$event_selector" -- sleep 0.01 >/dev/null 2>"$error_path"
    local status=$?
    if [[ $status -eq 0 ]]; then
        return 0
    fi

    if [[ "$(id -u)" == "0" ]] || ! has_cmd sudo; then
        return "$status"
    fi

    if [[ -z "${HOST_PASSWORD:-}" && "$SUDO_SESSION_READY" != "true" && "${VORTEX_PROFILER_TRY_SUDO_PERF_STAT:-}" != "force" ]]; then
        return "$status"
    fi

    ensure_sudo_access "sudo required to probe perf event ${event_selector}"
    if sudo perf stat -x , -e "$event_selector" -- sleep 0.01 >/dev/null 2>"$error_path"; then
        PERF_STAT_EVENT_SELECTOR_NEEDS_SUDO=1
        return 0
    fi
    return "$status"
}

resolve_perf_stat_memory_bandwidth_events() {
    if ! has_cmd perf; then
        return 1
    fi

    if perf list --raw-dump 2>/dev/null | grep -q 'uncore_imc_free_running/data_total/'; then
        printf '%s' 'uncore_imc_free_running/data_read/,uncore_imc_free_running/data_write/,uncore_imc_free_running/data_total/'
        return 0
    fi

    return 1
}

write_perf_stat_context() {
    local session="$1"
    local syscall_status="$2"
    local syscall_event="$3"
    local syscall_note="$4"
    local memory_status="$5"
    local memory_events="$6"
    local memory_note="$7"

    local power_profile="unavailable"
    local power_listing="unavailable"

    if has_cmd powerprofilesctl; then
        power_profile="$(_perf_stat_single_line "$(powerprofilesctl get 2>/dev/null || true)")"
        power_listing="$(_perf_stat_single_line "$(powerprofilesctl list 2>/dev/null || true)")"
        [[ -z "$power_profile" ]] && power_profile="unavailable"
        [[ -z "$power_listing" ]] && power_listing="unavailable"
    fi

    cat >"${session}/perf-stat-context.txt" <<EOF
format=perf-stat-csv
detail_level=counter-pass + topdown-pass
counter_events=task-clock,context-switches,cpu-migrations,page-faults,instructions,cycles,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses,dTLB-loads,dTLB-load-misses
topdown_metric_group=TopdownL1
perf_event_paranoid=$(_perf_stat_linux_value /proc/sys/kernel/perf_event_paranoid)
kptr_restrict=$(_perf_stat_linux_value /proc/sys/kernel/kptr_restrict)
cpu_governor=$(_perf_stat_linux_first_line /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
cpu_scaling_driver=$(_perf_stat_linux_first_line /sys/devices/system/cpu/cpufreq/policy0/scaling_driver)
energy_performance_preference=$(_perf_stat_linux_first_line /sys/devices/system/cpu/cpu0/cpufreq/energy_performance_preference)
power_profile=${power_profile}
power_profile_listing=${power_listing}
virtualization=$(perf_stat_virtualization_mode)
cpu_count=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || printf '%s' 'unavailable')
syscall_counter_status=${syscall_status}
syscall_counter_event=${syscall_event}
syscall_counter_note=${syscall_note}
memory_bandwidth_status=${memory_status}
memory_bandwidth_events=${memory_events}
memory_bandwidth_note=${memory_note}
EOF
}

run_perf_stat_command() {
    local purpose="$1"
    local requires_sudo="$2"
    shift 2

    if [[ "$requires_sudo" == "true" ]]; then
        run_with_sudo "$purpose" perf stat "$@"
    else
        perf stat "$@"
    fi
}

reclaim_perf_stat_output() {
    local requires_sudo="$1"
    local path="$2"

    if [[ "$requires_sudo" != "true" || ! -e "$path" ]]; then
        return 0
    fi

    run_with_sudo "sudo required to reclaim perf stat output" \
        chown "$(id -u):$(id -g)" "$path" >/dev/null 2>&1 || true
}

run_perf_stat() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"
    local extra_events=()
    local syscall_probe="${session}/perf-stat-syscall-probe.log"
    local memory_probe="${session}/perf-stat-memory-probe.log"
    local syscall_status="unavailable"
    local syscall_event="none"
    local syscall_note="not probed"
    local memory_status="unsupported"
    local memory_events="none"
    local memory_note="no supported memory-bandwidth event detected"
    local counter_event_selector="task-clock,context-switches,cpu-migrations,page-faults,instructions,cycles,branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses,dTLB-loads,dTLB-load-misses"
    local perf_stat_counter_args=(-x , -e "$counter_event_selector")
    local perf_stat_topdown_args=(-x , -d -d -M TopdownL1)
    local counter_requires_sudo="false"
    local topdown_requires_sudo="false"

    require_cmd perf

    header "perf stat"

    if perf_stat_event_selector_supported "raw_syscalls:sys_enter" "$syscall_probe"; then
        if [[ "${PERF_STAT_EVENT_SELECTOR_NEEDS_SUDO:-0}" == "1" ]]; then
            syscall_status="supported-sudo"
            syscall_note="tracepoint counter enabled with sudo perf stat"
            counter_requires_sudo="true"
        else
            syscall_status="supported"
            syscall_note="tracepoint counter enabled"
        fi
        syscall_event="raw_syscalls:sys_enter"
        counter_event_selector+=",${syscall_event}"
    else
        syscall_note="$(_perf_stat_single_line "$(cat "$syscall_probe" 2>/dev/null || true)")"
        [[ -z "$syscall_note" ]] && syscall_note="tracepoint counter unavailable"
    fi

    if memory_events="$(resolve_perf_stat_memory_bandwidth_events)"; then
        if perf_stat_event_selector_supported "$memory_events" "$memory_probe"; then
            if [[ "${PERF_STAT_EVENT_SELECTOR_NEEDS_SUDO:-0}" == "1" ]]; then
                memory_status="supported-sudo"
                memory_note="uncore IMC data counters enabled with sudo perf stat"
                topdown_requires_sudo="true"
            else
                memory_status="supported"
                memory_note="uncore IMC data counters enabled"
            fi
            extra_events+=("$memory_events")
        else
            memory_status="unavailable"
            memory_note="$(_perf_stat_single_line "$(cat "$memory_probe" 2>/dev/null || true)")"
            [[ -z "$memory_note" ]] && memory_note="memory bandwidth events unavailable"
        fi
    fi

    write_perf_stat_context \
        "$session" \
        "$syscall_status" \
        "$syscall_event" \
        "$syscall_note" \
        "$memory_status" \
        "$memory_events" \
        "$memory_note"

    perf_stat_counter_args=(-x , -e "$counter_event_selector")

    if [[ "$memory_status" == "supported" || "$memory_status" == "supported-sudo" ]]; then
        perf_stat_topdown_args+=( -e "$memory_events" )
    fi

    if profiling_target_is_engine; then
        info "Running: perf stat -x , -e ${counter_event_selector}"
        run_perf_stat_command "sudo required for perf stat counters" "$counter_requires_sudo" "${perf_stat_counter_args[@]}" \
            -o "${session}/perf-stat-counters.txt" \
            -- "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" \
            >"${session}/perf-stat-counters.log" 2>&1 || true
        reclaim_perf_stat_output "$counter_requires_sudo" "${session}/perf-stat-counters.txt"

        if [[ -f "${session}/perf-stat-counters.txt" ]]; then
            ok "perf stat counters: ${session}/perf-stat-counters.txt"
        fi

        info "Running: perf stat -x , -d -d -M TopdownL1"
        run_perf_stat_command "sudo required for perf stat top-down counters" "$topdown_requires_sudo" "${perf_stat_topdown_args[@]}" \
            -o "${session}/perf-stat.txt" \
            -- "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" \
            >"${session}/perf-stat.log" 2>&1 || true
        reclaim_perf_stat_output "$topdown_requires_sudo" "${session}/perf-stat.txt"

        if [[ -f "${session}/perf-stat.txt" ]]; then
            ok "perf stat: ${session}/perf-stat.txt"
        fi
        return 0
    fi

    start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-perf-stat-counters.log"
    generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-perf-stat.log"

    info "Running: perf stat -x , -e ${counter_event_selector} -p ${SERVER_PID} (${duration}s)"
    run_perf_stat_command "sudo required for perf stat counters" "$counter_requires_sudo" "${perf_stat_counter_args[@]}" \
        -p "$SERVER_PID" \
        -o "${session}/perf-stat-counters.txt" \
        -- sleep "$duration" \
        2>&1 || true
    reclaim_perf_stat_output "$counter_requires_sudo" "${session}/perf-stat-counters.txt"

    wait_for_load "$duration"

    if [[ -f "${session}/perf-stat-counters.txt" ]]; then
        ok "perf stat counters: ${session}/perf-stat-counters.txt"
    fi

    _profiler_cleanup
    SERVER_PID=""
    LOAD_PID=""

    start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-perf-stat.log"
    generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-perf-stat-topdown.log"

    info "Running: perf stat -x , -d -d -M TopdownL1 -p ${SERVER_PID} (${duration}s)"
    run_perf_stat_command "sudo required for perf stat top-down counters" "$topdown_requires_sudo" "${perf_stat_topdown_args[@]}" \
        -p "$SERVER_PID" \
        -o "${session}/perf-stat.txt" \
        -- sleep "$duration" \
        2>&1 || true
    reclaim_perf_stat_output "$topdown_requires_sudo" "${session}/perf-stat.txt"

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

    if profiling_target_is_engine; then
        info "Running: samply record"
        (cd "$REPO_ROOT" && exec samply record --save-only \
            -o "${session}/samply-profile.json" \
            -- "$PROFILING_BINARY" \
            "${ENGINE_TARGET_ARGS[@]}" \
            >"${session}/engine-samply.log" 2>&1)

        if [[ -f "${session}/samply-profile.json" ]]; then
            ok "samply profile: ${session}/samply-profile.json"
        fi
        return 0
    fi

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

    if profiling_target_is_engine; then
        ensure_macos_debuggable_binary "$PROFILING_BINARY"

        for template in "${templates[@]}"; do
            info "Setting up for template: ${template}"
            local suffix_name="${template// /-}"
            suffix_name="$(echo "$suffix_name" | tr '[:upper:]' '[:lower:]')"

            info "Running: xcrun xctrace record --template '${template}' --launch -- ${PROFILING_BINARY}"
            (cd "$REPO_ROOT" && exec xcrun xctrace record \
                --template "${template}" \
                --output "${session}/${suffix_name}.trace" \
                --launch -- "$PROFILING_BINARY" \
                "${ENGINE_TARGET_ARGS[@]}" \
                >"${session}/instruments-${suffix_name}.log" 2>&1) || true

            if [[ -d "${session}/${suffix_name}.trace" ]]; then
                ok "Instruments trace: ${session}/${suffix_name}.trace"
                xcrun xctrace export --input "${session}/${suffix_name}.trace" --toc --output "${session}/${suffix_name}-toc.xml" >/dev/null 2>&1 || true
                if [[ "${template}" == "Time Profiler" ]]; then
                    xcrun xctrace export --input "${session}/${suffix_name}.trace" \
                        --xpath '/trace-toc/run[@number="1"]/data/table[@schema="time-profile"]' \
                        --output "${session}/${suffix_name}-data.xml" >/dev/null 2>&1 || true
                fi
            fi
        done
        return 0
    fi

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

    # BPF escalation: run-queue latency histogram
    if [[ "$OS" == "linux" ]]; then
        run_bpf_runqlat "$session" "$@"
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

    # BPF escalation: block I/O latency histogram
    if [[ "$OS" == "linux" ]]; then
        run_bpf_biolatency "$session" "$@"
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

    # BPF escalation: TCP retransmit tracing
    if [[ "$OS" == "linux" ]]; then
        run_bpf_tcpretrans "$session" "$@"
    fi
}

run_lock_offcpu_focus() {
    local session="$1"
    shift

    write_lock_offcpu_tool_check "$session"
    write_lock_offcpu_context "$session" "$@"

    info "Running lock/off-CPU diagnostics for ${OS}..."

    if [[ "$OS" == "linux" ]]; then
        local offcpu_captured=false

        if has_cmd perf; then
            run_perf_stat "$session" "$@"
        else
            warn "perf not installed — skipping perf stat context for lock/off-CPU mode"
        fi

        run_bpf_runqlat "$session" "$@"
        run_bpf_biolatency "$session" "$@"

        if run_bpf_offwaketime "$session" "$@"; then
            offcpu_captured=true
        fi
        if run_bpf_offcputime "$session" "$@"; then
            offcpu_captured=true
        fi
        if [[ "$offcpu_captured" != "true" ]]; then
            run_perf_sched_capture "$session" "$@" || true
        fi

        run_bpf_futex_waits "$session" "$@" || true
        run_bpf_sync_syscalls "$session" "$@" || true
        write_lock_offcpu_classification "$session"
        return 0
    fi

    if [[ "$OS" == "macos" ]]; then
        if has_cmd xcrun; then
            run_instruments "$session" "$@"
        elif has_cmd samply; then
            run_samply "$session" "$@"
        else
            fatal "No lock/off-CPU profiler tool is available on this host."
        fi

        write_lock_offcpu_platform_note "$session"
        write_lock_offcpu_classification "$session"
        return 0
    fi

    fatal "Lock/off-CPU mode is supported only on Linux and macOS"
}


# ── BPF Tools — Escalation-mode diagnostics (Linux only) ─────────────────────
# These run as optional add-ons inside focus modes. They require bcc-tools
# or bpftrace to be installed and root/CAP_BPF privileges.

find_optional_tool() {
    local candidate=""

    for candidate in "$@"; do
        if has_cmd "$candidate"; then
            command -v "$candidate"
            return 0
        fi
        if [[ -x "$candidate" ]]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done

    return 1
}

resolve_focus_capture_duration() {
    local fallback_duration="${1:-15}"
    local resolved_duration="$fallback_duration"

    if profiling_target_is_engine; then
        resolved_duration="$(engine_target_arg_value "--duration-seconds" || true)"
        if [[ -z "$resolved_duration" ]]; then
            resolved_duration="$(engine_target_arg_value "--duration" || true)"
        fi
    fi

    printf '%s' "${resolved_duration:-$fallback_duration}"
}

write_capture_skip_note() {
    local output_path="$1" reason="$2" details_path="${3:-}"

    {
        printf 'status=skipped\n'
        printf 'reason=%s\n' "$reason"
        if [[ -n "$details_path" ]]; then
            printf 'details=%s\n' "$(basename "$details_path")"
        fi
    } >"$output_path"
}

capture_note_has_signal() {
    local path="$1"

    [[ -s "$path" ]] || return 1
    ! grep -q '^status=skipped$' "$path" 2>/dev/null
}

capture_files_match() {
    local pattern="$1"
    shift
    local path=""

    for path in "$@"; do
        [[ -f "$path" ]] || continue
        if grep -Eiq "$pattern" "$path" 2>/dev/null; then
            return 0
        fi
    done

    return 1
}

lock_offcpu_target_kind() {
    if profiling_target_is_engine; then
        printf 'engine'
        return 0
    fi

    printf 'server'
}

write_lock_offcpu_tool_check() {
    local session="$1"

    {
        printf 'os=%s\n' "$OS"
        printf 'target_kind=%s\n' "$(lock_offcpu_target_kind)"
        printf 'perf_path=%s\n' "$(command -v perf || printf 'unavailable')"
        printf 'runqlat_path=%s\n' "$(find_optional_tool runqlat runqlat-bpfcc /usr/share/bcc/tools/runqlat || printf 'unavailable')"
        printf 'biolatency_path=%s\n' "$(find_optional_tool biolatency biolatency-bpfcc /usr/share/bcc/tools/biolatency || printf 'unavailable')"
        printf 'offcputime_path=%s\n' "$(find_optional_tool offcputime offcputime-bpfcc /usr/share/bcc/tools/offcputime || printf 'unavailable')"
        printf 'offwaketime_path=%s\n' "$(find_optional_tool offwaketime offwaketime-bpfcc /usr/share/bcc/tools/offwaketime || printf 'unavailable')"
        printf 'bpftrace_path=%s\n' "$(command -v bpftrace || printf 'unavailable')"
        printf 'workload_source=%s\n' "${SESSION_WORKLOAD_SOURCE:-}"
        printf 'command_line=%s\n' "${SESSION_COMMAND_LINE:-}"
    } >"${session}/lock-offcpu-tool-check.txt"
}

write_lock_offcpu_context() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"
    local capture_duration=""

    capture_duration="$(resolve_focus_capture_duration "$duration")"

    {
        printf 'binary_path=%s\n' "${PROFILING_BINARY:-}"
        printf 'target_kind=%s\n' "$(lock_offcpu_target_kind)"
        printf 'workload_source=%s\n' "${SESSION_WORKLOAD_SOURCE:-}"
        printf 'workload_description=%s\n' "${SESSION_WORKLOAD_DESCRIPTION:-}"
        printf 'workload_command=%s\n' "${SESSION_WORKLOAD_COMMAND:-}"
        printf 'duration_seconds=%s\n' "$capture_duration"
        printf 'clients=%s\n' "$clients"
        printf 'aof_enabled=%s\n' "$aof"
        printf 'aof_fsync=%s\n' "${VORTEX_AOF_FSYNC:-everysec}"
        printf 'aof_max_pending_fsync_bytes=%s\n' "${VORTEX_AOF_MAX_PENDING_FSYNC_BYTES:-default}"
        printf 'host=%s\n' "$host"
        printf 'port=%s\n' "$port"
        printf 'threads=%s\n' "$threads"
        printf 'maxmemory=%s\n' "$maxmemory"
        printf 'eviction=%s\n' "$eviction"
        printf 'command=%s\n' "$command"
    } >"${session}/lock-offcpu-context.txt"
}

write_lock_offcpu_platform_note() {
    local session="$1"

    {
        printf 'status=partial\n'
        printf 'platform=%s\n' "$OS"
        printf 'reason=Linux-only runqlat, biolatency, offcputime, offwaketime, perf sched, and futex tracing are unavailable on this platform\n'
        printf 'fallback=Instruments or Samply plus host telemetry\n'
    } >"${session}/lock-offcpu-platform-note.txt"
}

write_lock_offcpu_classification() {
    local session="$1"
    local output_path="${session}/lock-offcpu-classification.txt"
    local runnable_offcpu="absent"
    local blocked_offcpu="absent"
    local futex_wait="absent"
    local disk_fsync="absent"
    local classification="unknown"
    local reason="no blocking signal was captured"

    if capture_note_has_signal "${session}/bpf-runqlat.txt"; then
        runnable_offcpu="present"
    fi

    if capture_note_has_signal "${session}/bpf-offcputime.txt" || \
       capture_note_has_signal "${session}/bpf-offwaketime.txt" || \
       capture_note_has_signal "${session}/perf-sched-latency.txt"; then
        blocked_offcpu="present"
    fi

    if capture_note_has_signal "${session}/bpf-futex.txt" || \
       capture_files_match 'futex|parking_lot|pthread_mutex|__lll_lock_wait|lock_offcpu_probe' \
           "${session}/bpf-offcputime.txt" \
           "${session}/bpf-offwaketime.txt" \
           "${session}/perf-sched-timehist.txt"; then
        futex_wait="present"
    fi

    if capture_note_has_signal "${session}/bpf-sync-syscalls.txt" || \
       capture_files_match 'fsync|fdatasync|sync_file_range|submit_bio|blk_|filemap_fdatawait|vortex_persist' \
           "${session}/bpf-offcputime.txt" \
           "${session}/bpf-offwaketime.txt" \
           "${session}/perf-sched-timehist.txt"; then
        disk_fsync="present"
    fi

    if [[ "$disk_fsync" == "present" ]]; then
        classification="disk/fsync"
        reason="disk or fsync blocking evidence is present in the captured artifacts"
    elif [[ "$futex_wait" == "present" ]]; then
        classification="blocked lock/off-CPU"
        reason="futex or parking wait evidence is present in the captured artifacts"
    elif [[ "$runnable_offcpu" == "present" ]]; then
        classification="runnable off-CPU"
        reason="run-queue latency evidence was captured"
    elif [[ "$blocked_offcpu" == "present" ]]; then
        classification="blocked lock/off-CPU"
        reason="off-CPU stacks were captured without a stronger scheduler or disk signal"
    elif [[ -f "${session}/lock-offcpu-platform-note.txt" ]]; then
        reason="the macOS fallback path ran, but Linux-specific blocking signals were unavailable"
    fi

    {
        printf 'classification=%s\n' "$classification"
        printf 'runnable_offcpu=%s\n' "$runnable_offcpu"
        printf 'blocked_offcpu=%s\n' "$blocked_offcpu"
        printf 'futex_wait=%s\n' "$futex_wait"
        printf 'disk_fsync=%s\n' "$disk_fsync"
        printf 'reason=%s\n' "$reason"
    } >"$output_path"

    ok "Lock/off-CPU classification: ${output_path}"
}

bpf_capture_failed() {
    local output_path="$1"
    [[ -s "$output_path" ]] || return 1

    grep -Eq \
    'Failed to compile BPF module|Traceback \(most recent call last\)|use of undeclared identifier|invalid application of .* incomplete type|[0-9]+ errors generated\.|tracepoint not found|^ERROR:' \
        "$output_path"
}

write_bpf_skip_note() {
    local output_path="$1" reason="$2" details_path="${3:-}"

    {
        printf 'status=skipped\n'
        printf 'reason=%s\n' "$reason"
        if [[ -n "$details_path" ]]; then
            printf 'details=%s\n' "$(basename "$details_path")"
        fi
        printf 'fallback=tcp_retrans_segs_delta in host telemetry summary\n'
    } >"$output_path"
}

run_bpf_runqlat() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    # Try bcc-tools runqlat first, then bpftrace fallback
    local runqlat_bin=""
    local output_path="${session}/bpf-runqlat.txt"
    local error_path="${session}/bpf-runqlat-error.log"
    local capture_duration=""
    for candidate in runqlat runqlat-bpfcc /usr/share/bcc/tools/runqlat; do
        if has_cmd "$candidate" || [[ -x "$candidate" ]]; then
            runqlat_bin="$candidate"
            break
        fi
    done

    capture_duration="$(resolve_focus_capture_duration "$duration")"

    if [[ -z "$runqlat_bin" ]]; then
        info "runqlat not found — install bcc-tools for run-queue latency histograms (skipping)"
        write_capture_skip_note "$output_path" "runqlat is not available on this host"
        return 0
    fi

    header "BPF: runqlat (run-queue latency)"

    ensure_sudo_access "sudo required for BPF runqlat"

    rm -f "$output_path" "$error_path"

    if profiling_target_is_engine; then
        info "Starting engine target for runqlat capture"
        "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" >"${session}/engine-runqlat.log" 2>&1 &
        local target_pid=$!
        record_session_pid "$target_pid"
        sleep 0.2

        info "Running: $runqlat_bin ${capture_duration}s 1 (one histogram over full duration)"
        run_with_sudo "BPF runqlat" timeout $((capture_duration + 5)) "$runqlat_bin" "$capture_duration" 1 \
            >"$output_path" 2>&1 || true

        wait "$target_pid" 2>/dev/null || true
    else
        start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-runqlat.log"
        generate_load "$host" "$port" "$command" "$capture_duration" "$clients" "${session}/load-runqlat.log"

        info "Running: $runqlat_bin ${capture_duration}s 1 (one histogram over full duration)"
        run_with_sudo "BPF runqlat" timeout $((capture_duration + 5)) "$runqlat_bin" "$capture_duration" 1 \
            >"$output_path" 2>&1 || true

        wait_for_load "$capture_duration"
        _profiler_cleanup
        SERVER_PID=""
        LOAD_PID=""
    fi

    if bpf_capture_failed "$output_path"; then
        mv "$output_path" "$error_path"
        write_capture_skip_note "$output_path" "runqlat failed on this host or kernel" "$error_path"
        warn "runqlat failed to attach cleanly; wrote a skip note to ${output_path} and saved diagnostics to ${error_path}"
    elif capture_note_has_signal "$output_path"; then
        ok "BPF runqlat histogram: ${output_path}"
    else
        write_capture_skip_note "$output_path" "runqlat produced no output (may need CAP_BPF or kernel headers)"
        warn "runqlat produced no output (may need CAP_BPF or kernel headers)"
    fi
}

run_bpf_biolatency() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    local biolatency_bin=""
    local output_path="${session}/bpf-biolatency.txt"
    local error_path="${session}/bpf-biolatency-error.log"
    local capture_duration=""
    for candidate in biolatency biolatency-bpfcc /usr/share/bcc/tools/biolatency; do
        if has_cmd "$candidate" || [[ -x "$candidate" ]]; then
            biolatency_bin="$candidate"
            break
        fi
    done

    capture_duration="$(resolve_focus_capture_duration "$duration")"

    if [[ -z "$biolatency_bin" ]]; then
        info "biolatency not found — install bcc-tools for block I/O latency histograms (skipping)"
        write_capture_skip_note "$output_path" "biolatency is not available on this host"
        return 0
    fi

    header "BPF: biolatency (block I/O latency)"

    ensure_sudo_access "sudo required for BPF biolatency"

    rm -f "$output_path" "$error_path"

    if profiling_target_is_engine; then
        info "Starting engine target for biolatency capture"
        "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" >"${session}/engine-biolatency.log" 2>&1 &
        local target_pid=$!
        record_session_pid "$target_pid"
        sleep 0.2

        info "Running: $biolatency_bin ${capture_duration}s 1"
        run_with_sudo "BPF biolatency" timeout $((capture_duration + 5)) "$biolatency_bin" "$capture_duration" 1 \
            >"$output_path" 2>&1 || true

        wait "$target_pid" 2>/dev/null || true
    else
        start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-biolatency.log"
        generate_load "$host" "$port" "$command" "$capture_duration" "$clients" "${session}/load-biolatency.log"

        info "Running: $biolatency_bin ${capture_duration}s 1"
        run_with_sudo "BPF biolatency" timeout $((capture_duration + 5)) "$biolatency_bin" "$capture_duration" 1 \
            >"$output_path" 2>&1 || true

        wait_for_load "$capture_duration"
        _profiler_cleanup
        SERVER_PID=""
        LOAD_PID=""
    fi

    if bpf_capture_failed "$output_path"; then
        mv "$output_path" "$error_path"
        write_capture_skip_note "$output_path" "biolatency failed on this host or kernel" "$error_path"
        warn "biolatency failed to attach cleanly; wrote a skip note to ${output_path} and saved diagnostics to ${error_path}"
    elif capture_note_has_signal "$output_path"; then
        ok "BPF biolatency histogram: ${output_path}"
    else
        write_capture_skip_note "$output_path" "biolatency produced no output (may need CAP_BPF or kernel headers)"
        warn "biolatency produced no output (may need CAP_BPF or kernel headers)"
    fi
}

run_bpf_offcputime() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"
    local tool_bin=""
    local output_path="${session}/bpf-offcputime.txt"
    local error_path="${session}/bpf-offcputime-error.log"
    local capture_duration=""

    tool_bin="$(find_optional_tool offcputime offcputime-bpfcc /usr/share/bcc/tools/offcputime || true)"
    capture_duration="$(resolve_focus_capture_duration "$duration")"

    if [[ -z "$tool_bin" ]]; then
        write_capture_skip_note "$output_path" "offcputime is not available on this host"
        info "offcputime not found — skipping off-CPU blocked stack capture"
        return 1
    fi

    header "BPF: offcputime (blocked stack summaries)"

    ensure_sudo_access "sudo required for BPF offcputime"
    rm -f "$output_path" "$error_path"

    if profiling_target_is_engine; then
        info "Starting engine target for offcputime capture"
        "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" >"${session}/engine-offcputime.log" 2>&1 &
        local target_pid=$!
        record_session_pid "$target_pid"
        sleep 0.2

        info "Running: $tool_bin -d -m 1000 -p ${target_pid} ${capture_duration}"
        run_with_sudo "BPF offcputime" timeout --signal=INT $((capture_duration + 5)) \
            "$tool_bin" -d -m 1000 -p "$target_pid" "$capture_duration" \
            >"$output_path" 2>&1 || true

        wait "$target_pid" 2>/dev/null || true
    else
        start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-offcputime.log"
        generate_load "$host" "$port" "$command" "$capture_duration" "$clients" "${session}/load-offcputime.log"

        info "Running: $tool_bin -d -m 1000 -p ${SERVER_PID} ${capture_duration}"
        run_with_sudo "BPF offcputime" timeout --signal=INT $((capture_duration + 5)) \
            "$tool_bin" -d -m 1000 -p "$SERVER_PID" "$capture_duration" \
            >"$output_path" 2>&1 || true

        wait_for_load "$capture_duration"
        _profiler_cleanup
        SERVER_PID=""
        LOAD_PID=""
    fi

    if bpf_capture_failed "$output_path"; then
        mv "$output_path" "$error_path"
        write_capture_skip_note "$output_path" "offcputime failed on this host or kernel" "$error_path"
        warn "offcputime failed to attach cleanly; wrote a skip note to ${output_path} and saved diagnostics to ${error_path}"
        return 1
    fi

    if capture_note_has_signal "$output_path"; then
        ok "BPF offcputime summary: ${output_path}"
        return 0
    fi

    write_capture_skip_note "$output_path" "offcputime produced no output"
    warn "offcputime produced no output"
    return 1
}

run_bpf_offwaketime() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"
    local tool_bin=""
    local output_path="${session}/bpf-offwaketime.txt"
    local error_path="${session}/bpf-offwaketime-error.log"
    local capture_duration=""

    tool_bin="$(find_optional_tool offwaketime offwaketime-bpfcc /usr/share/bcc/tools/offwaketime || true)"
    capture_duration="$(resolve_focus_capture_duration "$duration")"

    if [[ -z "$tool_bin" ]]; then
        write_capture_skip_note "$output_path" "offwaketime is not available on this host"
        info "offwaketime not found — skipping off-CPU waker stack capture"
        return 1
    fi

    header "BPF: offwaketime (blocked plus waker stack summaries)"

    ensure_sudo_access "sudo required for BPF offwaketime"
    rm -f "$output_path" "$error_path"

    if profiling_target_is_engine; then
        info "Starting engine target for offwaketime capture"
        "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" >"${session}/engine-offwaketime.log" 2>&1 &
        local target_pid=$!
        record_session_pid "$target_pid"
        sleep 0.2

        info "Running: $tool_bin -d -m 1000 -p ${target_pid} ${capture_duration}"
        run_with_sudo "BPF offwaketime" timeout --signal=INT $((capture_duration + 5)) \
            "$tool_bin" -d -m 1000 -p "$target_pid" "$capture_duration" \
            >"$output_path" 2>&1 || true

        wait "$target_pid" 2>/dev/null || true
    else
        start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-offwaketime.log"
        generate_load "$host" "$port" "$command" "$capture_duration" "$clients" "${session}/load-offwaketime.log"

        info "Running: $tool_bin -d -m 1000 -p ${SERVER_PID} ${capture_duration}"
        run_with_sudo "BPF offwaketime" timeout --signal=INT $((capture_duration + 5)) \
            "$tool_bin" -d -m 1000 -p "$SERVER_PID" "$capture_duration" \
            >"$output_path" 2>&1 || true

        wait_for_load "$capture_duration"
        _profiler_cleanup
        SERVER_PID=""
        LOAD_PID=""
    fi

    if bpf_capture_failed "$output_path"; then
        mv "$output_path" "$error_path"
        write_capture_skip_note "$output_path" "offwaketime failed on this host or kernel" "$error_path"
        warn "offwaketime failed to attach cleanly; wrote a skip note to ${output_path} and saved diagnostics to ${error_path}"
        return 1
    fi

    if capture_note_has_signal "$output_path"; then
        ok "BPF offwaketime summary: ${output_path}"
        return 0
    fi

    write_capture_skip_note "$output_path" "offwaketime produced no output"
    warn "offwaketime produced no output"
    return 1
}

run_perf_sched_capture() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"
    local capture_duration=""
    local data_path="${session}/perf-sched.data"
    local latency_path="${session}/perf-sched-latency.txt"
    local timehist_path="${session}/perf-sched-timehist.txt"
    local record_log="${session}/perf-sched-record.log"

    capture_duration="$(resolve_focus_capture_duration "$duration")"

    if ! has_cmd perf; then
        write_capture_skip_note "$latency_path" "perf is not available on this host"
        return 1
    fi

    header "perf sched"

    ensure_sudo_access "sudo required for perf sched"
    rm -f "$data_path" "$latency_path" "$timehist_path" "$record_log"

    if profiling_target_is_engine; then
        info "Starting engine target for perf sched capture"
        "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" >"${session}/engine-perf-sched.log" 2>&1 &
        local target_pid=$!
        record_session_pid "$target_pid"
        sleep 0.2

        info "Running: perf sched record -p ${target_pid} -- sleep ${capture_duration}"
        run_with_sudo "perf sched record" perf sched record -o "$data_path" -p "$target_pid" -- sleep "$capture_duration" \
            >"$record_log" 2>&1 || true

        wait "$target_pid" 2>/dev/null || true
    else
        start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-perf-sched.log"
        generate_load "$host" "$port" "$command" "$capture_duration" "$clients" "${session}/load-perf-sched.log"

        info "Running: perf sched record -p ${SERVER_PID} -- sleep ${capture_duration}"
        run_with_sudo "perf sched record" perf sched record -o "$data_path" -p "$SERVER_PID" -- sleep "$capture_duration" \
            >"$record_log" 2>&1 || true

        wait_for_load "$capture_duration"
        _profiler_cleanup
        SERVER_PID=""
        LOAD_PID=""
    fi

    if [[ -f "$data_path" ]]; then
        run_with_sudo "reclaim perf sched data" chown "$(id -u):$(id -g)" "$data_path" >/dev/null 2>&1 || true
        run_with_sudo "perf sched latency" perf sched latency -i "$data_path" >"$latency_path" 2>"${session}/perf-sched-latency.log" || true
        run_with_sudo "perf sched timehist" perf sched timehist -i "$data_path" >"$timehist_path" 2>"${session}/perf-sched-timehist.log" || true
    fi

    if capture_note_has_signal "$latency_path" || capture_note_has_signal "$timehist_path"; then
        ok "perf sched analysis: ${latency_path}"
        return 0
    fi

    write_capture_skip_note "$latency_path" "perf sched produced no usable output" "$record_log"
    warn "perf sched produced no usable output"
    return 1
}

run_bpf_futex_waits() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"
    local output_path="${session}/bpf-futex.txt"
    local error_path="${session}/bpf-futex-error.log"
    local capture_duration=""
    local script=""
    local target_pid=""

    capture_duration="$(resolve_focus_capture_duration "$duration")"

    if ! has_cmd bpftrace; then
        write_capture_skip_note "$output_path" "bpftrace is not available on this host"
        info "bpftrace not found — skipping futex wait tracing"
        return 1
    fi

    header "BPF: futex wait tracing"

    ensure_sudo_access "sudo required for futex wait tracing"
    rm -f "$output_path" "$error_path"

    if profiling_target_is_engine; then
        info "Starting engine target for futex wait tracing"
        "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" >"${session}/engine-futex.log" 2>&1 &
        target_pid=$!
        record_session_pid "$target_pid"
        sleep 0.2
    else
        start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-futex.log"
        generate_load "$host" "$port" "$command" "$capture_duration" "$clients" "${session}/load-futex.log"
        target_pid="$SERVER_PID"
    fi

    script="tracepoint:syscalls:sys_enter_futex /pid == ${target_pid}/ { @futex[comm] = count(); } END { print(@futex); }"
    info "Running: bpftrace futex trace for ${capture_duration}s"
    run_with_sudo "BPF futex wait tracing" timeout --signal=INT $((capture_duration + 5)) bpftrace -e "$script" \
        >"$output_path" 2>&1 || true

    if profiling_target_is_engine; then
        wait "$target_pid" 2>/dev/null || true
    else
        wait_for_load "$capture_duration"
        _profiler_cleanup
        SERVER_PID=""
        LOAD_PID=""
    fi

    if bpf_capture_failed "$output_path"; then
        mv "$output_path" "$error_path"
        write_capture_skip_note "$output_path" "futex wait tracing failed on this host or kernel" "$error_path"
        warn "futex wait tracing failed; wrote a skip note to ${output_path} and saved diagnostics to ${error_path}"
        return 1
    fi

    if grep -q '^@' "$output_path" 2>/dev/null; then
        ok "BPF futex wait trace: ${output_path}"
        return 0
    fi

    write_capture_skip_note "$output_path" "no futex syscalls were observed during the session"
    info "futex wait tracing captured no events"
    return 1
}

run_bpf_sync_syscalls() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"
    local output_path="${session}/bpf-sync-syscalls.txt"
    local error_path="${session}/bpf-sync-syscalls-error.log"
    local capture_duration=""
    local script=""
    local target_pid=""

    capture_duration="$(resolve_focus_capture_duration "$duration")"

    if ! has_cmd bpftrace; then
        write_capture_skip_note "$output_path" "bpftrace is not available on this host"
        info "bpftrace not found — skipping sync syscall tracing"
        return 1
    fi

    header "BPF: fsync and sync syscall tracing"

    ensure_sudo_access "sudo required for fsync and sync syscall tracing"
    rm -f "$output_path" "$error_path"

    if profiling_target_is_engine; then
        info "Starting engine target for sync syscall tracing"
        "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" >"${session}/engine-sync-syscalls.log" 2>&1 &
        target_pid=$!
        record_session_pid "$target_pid"
        sleep 0.2
    else
        start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-sync-syscalls.log"
        generate_load "$host" "$port" "$command" "$capture_duration" "$clients" "${session}/load-sync-syscalls.log"
        target_pid="$SERVER_PID"
    fi

    script="tracepoint:syscalls:sys_enter_fsync /pid == ${target_pid}/ { @sync[\"fsync\"] = count(); } tracepoint:syscalls:sys_enter_fdatasync /pid == ${target_pid}/ { @sync[\"fdatasync\"] = count(); } tracepoint:syscalls:sys_enter_sync_file_range /pid == ${target_pid}/ { @sync[\"sync_file_range\"] = count(); } END { print(@sync); }"
    info "Running: bpftrace sync syscall trace for ${capture_duration}s"
    run_with_sudo "BPF fsync and sync syscall tracing" timeout --signal=INT $((capture_duration + 5)) bpftrace -e "$script" \
        >"$output_path" 2>&1 || true

    if profiling_target_is_engine; then
        wait "$target_pid" 2>/dev/null || true
    else
        wait_for_load "$capture_duration"
        _profiler_cleanup
        SERVER_PID=""
        LOAD_PID=""
    fi

    if bpf_capture_failed "$output_path"; then
        mv "$output_path" "$error_path"
        write_capture_skip_note "$output_path" "sync syscall tracing failed on this host or kernel" "$error_path"
        warn "sync syscall tracing failed; wrote a skip note to ${output_path} and saved diagnostics to ${error_path}"
        return 1
    fi

    if grep -q '^@' "$output_path" 2>/dev/null; then
        ok "BPF sync syscall trace: ${output_path}"
        return 0
    fi

    write_capture_skip_note "$output_path" "no fsync or related sync syscalls were observed during the session"
    info "sync syscall tracing captured no events"
    return 1
}

run_bpf_tcpretrans() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    local output_path="${session}/bpf-tcpretrans.txt"
    local error_path="${session}/bpf-tcpretrans-error.log"
    local bpftrace_script='tracepoint:tcp:tcp_retransmit_skb { @[comm] = count(); } END { print(@); }'
    local tcpretrans_bin=""
    for candidate in tcpretrans tcpretrans-bpfcc /usr/share/bcc/tools/tcpretrans; do
        if has_cmd "$candidate" || [[ -x "$candidate" ]]; then
            tcpretrans_bin="$candidate"
            break
        fi
    done

    if [[ -z "$tcpretrans_bin" ]]; then
        info "tcpretrans not found — install bcc-tools for TCP retransmit tracing (skipping)"
        return 0
    fi

    header "BPF: tcpretrans (TCP retransmit tracing)"

    ensure_sudo_access "sudo required for BPF tcpretrans"

    start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-tcpretrans.log"
    generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-tcpretrans.log"

    rm -f "$output_path" "$error_path"

    if has_cmd bpftrace; then
        info "Running: bpftrace tracepoint:tcp:tcp_retransmit_skb for ${duration}s"
        run_with_sudo "BPF tcpretrans (bpftrace)" \
            timeout --signal=INT $((duration + 5)) bpftrace -e "$bpftrace_script" \
            >"$output_path" 2>&1 || true

        if bpf_capture_failed "$output_path"; then
            mv "$output_path" "$error_path"
        elif grep -q '@' "$output_path"; then
            wait_for_load "$duration"
            _profiler_cleanup
            SERVER_PID=""
            LOAD_PID=""
            ok "BPF tcpretrans trace: ${output_path}"
            return 0
        else
            wait_for_load "$duration"
            _profiler_cleanup
            SERVER_PID=""
            LOAD_PID=""
            write_bpf_skip_note "$output_path" "no TCP retransmits were observed during the session"
            info "tcpretrans captured no events (may indicate zero retransmits during run)"
            return 0
        fi
    fi

    info "Running: $tcpretrans_bin for ${duration}s"
    run_with_sudo "BPF tcpretrans" timeout $((duration + 5)) "$tcpretrans_bin" \
        >"$error_path" 2>&1 || true

    if [[ -s "$error_path" ]] && ! bpf_capture_failed "$error_path"; then
        wait_for_load "$duration"
        _profiler_cleanup
        SERVER_PID=""
        LOAD_PID=""
        mv "$error_path" "$output_path"
        ok "BPF tcpretrans trace: ${output_path}"
        return 0
    fi

    wait_for_load "$duration"
    _profiler_cleanup
    SERVER_PID=""
    LOAD_PID=""

    if [[ -s "$error_path" ]]; then
        write_bpf_skip_note "$output_path" "tcpretrans tracing failed on this host or kernel" "$error_path"
        warn "tcpretrans could not attach cleanly; wrote a skip note to ${output_path} and saved diagnostics to ${error_path}"
    else
        write_bpf_skip_note "$output_path" "no compatible tcpretrans tracer was available"
        info "tcpretrans tracing skipped"
    fi
}


# ── Differential Flamegraph Generation ───────────────────────────────────────
# Compares perf.data from two profiling sessions using Brendan Gregg's
# difffolded.pl + flamegraph.pl methodology, or inferno-diff-folded.

run_diff_flamegraph() {
    local session="$1"
    local compare_to="$2"

    if [[ -z "$compare_to" ]]; then
        return 0
    fi

    # Find perf.data in both sessions
    local current_perf="${session}/perf.data"
    local baseline_perf="${compare_to}/perf.data"

    if [[ ! -f "$current_perf" ]]; then
        info "No perf.data in current session — skipping differential flamegraph"
        return 0
    fi
    if [[ ! -f "$baseline_perf" ]]; then
        info "No perf.data in baseline session (${compare_to}) — skipping differential flamegraph"
        return 0
    fi

    header "Differential Flamegraph"

    # Generate folded stacks from both sessions
    local current_folded="${session}/current-stacks.folded"
    local baseline_folded="${session}/baseline-stacks.folded"

    info "Generating folded stacks from current session..."
    perf script -i "$current_perf" 2>/dev/null \
        | stackcollapse-perf.pl 2>/dev/null \
        > "$current_folded" || true

    info "Generating folded stacks from baseline session..."
    perf script -i "$baseline_perf" 2>/dev/null \
        | stackcollapse-perf.pl 2>/dev/null \
        > "$baseline_folded" || true

    if [[ ! -s "$current_folded" || ! -s "$baseline_folded" ]]; then
        # Try inferno as fallback
        if has_cmd inferno-collapse-perf && has_cmd inferno-diff-folded && has_cmd inferno-flamegraph; then
            info "Falling back to inferno for differential flamegraph..."
            inferno-collapse-perf < <(perf script -i "$current_perf" 2>/dev/null) > "$current_folded" 2>/dev/null || true
            inferno-collapse-perf < <(perf script -i "$baseline_perf" 2>/dev/null) > "$baseline_folded" 2>/dev/null || true

            if [[ -s "$current_folded" && -s "$baseline_folded" ]]; then
                inferno-diff-folded "$baseline_folded" "$current_folded" \
                    | inferno-flamegraph --title "Differential: $(basename "$session") vs $(basename "$compare_to")" \
                    > "${session}/diff-flamegraph.svg" 2>/dev/null || true
            fi
        else
            warn "Neither FlameGraph perl scripts nor inferno are available — install one for diff flamegraphs"
            warn "  Perl: git clone https://github.com/brendangregg/FlameGraph && PATH+=:FlameGraph"
            warn "  Rust: cargo install inferno"
            return 0
        fi
    else
        # Use Brendan Gregg's difffolded.pl + flamegraph.pl
        if has_cmd difffolded.pl && has_cmd flamegraph.pl; then
            info "Generating differential flamegraph (red = regression, blue = improvement)..."
            difffolded.pl "$baseline_folded" "$current_folded" \
                | flamegraph.pl --title "Differential: $(basename "$session") vs $(basename "$compare_to")" \
                    --negate \
                > "${session}/diff-flamegraph.svg" 2>/dev/null || true
        elif has_cmd inferno-diff-folded && has_cmd inferno-flamegraph; then
            info "Generating differential flamegraph via inferno..."
            inferno-diff-folded "$baseline_folded" "$current_folded" \
                | inferno-flamegraph --title "Differential: $(basename "$session") vs $(basename "$compare_to")" \
                > "${session}/diff-flamegraph.svg" 2>/dev/null || true
        else
            warn "difffolded.pl/flamegraph.pl or inferno not found — cannot generate diff flamegraph"
            return 0
        fi
    fi

    if [[ -f "${session}/diff-flamegraph.svg" ]]; then
        ok "Differential flamegraph: ${session}/diff-flamegraph.svg"
        info "  Red stacks = grew since baseline (regression)"
        info "  Blue stacks = shrank since baseline (improvement)"
    else
        warn "Differential flamegraph was not generated"
    fi

    # Clean up intermediate files
    rm -f "$current_folded" "$baseline_folded" 2>/dev/null || true
}
