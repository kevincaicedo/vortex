#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/c2c.sh — cache-line contention and false-sharing profiling
# ─────────────────────────────────────────────────────────────────────────────

perf_supports_c2c() {
    local output=""

    has_cmd perf || return 1
    output="$(perf c2c 2>&1 || true)"
    grep -q 'Usage: perf c2c' <<<"$output"
}

_c2c_linux_value() {
    local path="$1"

    if [[ -r "$path" ]]; then
        tr -d '\n' <"$path"
    else
        printf 'unavailable'
    fi
}

_c2c_target_kind() {
    if profiling_target_is_engine; then
        printf 'engine'
        return 0
    fi

    printf 'server'
}

write_c2c_tool_check() {
    local session="$1"
    local status="${2:-ready}"
    local reason="${3:-}"

    {
        printf 'status=%s\n' "$status"
        printf 'os=%s\n' "$OS"
        printf 'target_kind=%s\n' "$(_c2c_target_kind)"
        printf 'dry_run=%s\n' "${DRY_RUN:-false}"
        printf 'perf_path=%s\n' "$(command -v perf || printf 'unavailable')"
        printf 'perf_c2c_supported=%s\n' "$(if perf_supports_c2c; then printf 'yes'; else printf 'no'; fi)"
        printf 'pahole_path=%s\n' "$(command -v pahole || printf 'unavailable')"
        printf 'taskset_path=%s\n' "$(command -v taskset || printf 'unavailable')"
        printf 'perf_event_paranoid=%s\n' "$(_c2c_linux_value /proc/sys/kernel/perf_event_paranoid)"
        printf 'kptr_restrict=%s\n' "$(_c2c_linux_value /proc/sys/kernel/kptr_restrict)"
        printf 'host_password_env=%s\n' "$(if [[ -n "${HOST_PASSWORD:-}" ]]; then printf 'present'; else printf 'absent'; fi)"
        printf 'profiler_env_file=%s\n' "${PROFILER_ENV_FILE:-unavailable}"
        printf 'command_line=%s\n' "${SESSION_COMMAND_LINE:-}"
        printf 'workload_source=%s\n' "${SESSION_WORKLOAD_SOURCE:-}"
        printf 'workload_description=%s\n' "${SESSION_WORKLOAD_DESCRIPTION:-}"
        printf 'binary_path=%s\n' "${PROFILING_BINARY:-}"
        if [[ -n "$reason" ]]; then
            printf 'reason=%s\n' "$reason"
        fi
    } >"${session}/c2c-tool-check.txt"
}

write_c2c_symbolization_context() {
    local session="$1"

    {
        printf 'perf_version=%s\n' "$(perf version 2>/dev/null || printf 'unavailable')"
        printf 'perf_buildid_dir_env=%s\n' "${PERF_BUILDID_DIR:-default}"
        printf 'debuginfod_urls=%s\n' "${DEBUGINFOD_URLS:-unset}"
        printf 'perf_config_buildid_dir=%s\n' "$(perf config --get buildid.dir 2>/dev/null || printf 'unset')"
        printf 'perf_config_report_demangle=%s\n' "$(perf config --get report.demangle 2>/dev/null || printf 'unset')"
        printf 'perf_config_annotate_source_path=%s\n' "$(perf config --get annotate.source-path 2>/dev/null || printf 'unset')"
    } >"${session}/c2c-symbolization.txt"
}

_c2c_target_affinity() {
    local pid="$1"

    if [[ -z "$pid" ]]; then
        printf 'unavailable'
        return 0
    fi

    if has_cmd taskset; then
        taskset -pc "$pid" 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\+$//'
        return 0
    fi

    if [[ -r "/proc/${pid}/status" ]]; then
        awk -F':' '/^Cpus_allowed_list/ { gsub(/^[[:space:]]+/, "", $2); print $2 }' "/proc/${pid}/status"
        return 0
    fi

    printf 'unavailable'
}

write_c2c_context() {
    local session="$1"
    local pid="${2:-}"

    {
        printf 'binary_path=%s\n' "${PROFILING_BINARY:-}"
        printf 'binary_realpath=%s\n' "$(readlink -f "${PROFILING_BINARY}" 2>/dev/null || printf '%s' "${PROFILING_BINARY}")"
        printf 'target_kind=%s\n' "$(_c2c_target_kind)"
        printf 'target_pid=%s\n' "${pid:-unavailable}"
        printf 'cpu_affinity=%s\n' "$(_c2c_target_affinity "$pid")"
        printf 'workload_source=%s\n' "${SESSION_WORKLOAD_SOURCE:-}"
        printf 'workload_description=%s\n' "${SESSION_WORKLOAD_DESCRIPTION:-}"
        printf 'workload_command=%s\n' "${SESSION_WORKLOAD_COMMAND:-}"
        printf 'duration_seconds=%s\n' "${SESSION_WORKLOAD_DURATION:-}"
        printf 'clients=%s\n' "${SESSION_WORKLOAD_CLIENTS:-}"
        printf 'command_line=%s\n' "${SESSION_COMMAND_LINE:-}"
    } >"${session}/c2c-context.txt"
}

reclaim_root_owned_artifact() {
    local artifact_path="$1"

    [[ -e "$artifact_path" ]] || return 0

    run_with_sudo "sudo required to reclaim perf c2c artifacts" \
        chown "$(id -u):$(id -g)" "$artifact_path" >/dev/null 2>&1 || true
}

write_c2c_build_ids() {
    local session="$1"
    local data_path="${session}/perf-c2c.data"

    [[ -f "$data_path" ]] || return 0

    run_with_sudo "sudo required to inspect perf c2c build IDs" \
        perf buildid-list --force --with-hits -i "$data_path" >"${session}/perf-buildids.txt" 2>"${session}/perf-buildids.log" || true
}

write_c2c_reports() {
    local session="$1"
    local data_path="${session}/perf-c2c.data"

    [[ -f "$data_path" ]] || return 0

    run_with_sudo "sudo required for perf c2c report" \
        perf c2c report --force --stdio --stats --full-symbols --input "$data_path" \
        >"${session}/perf-c2c-stats.txt" 2>"${session}/perf-c2c-stats.log" || true

    run_with_sudo "sudo required for perf c2c report" \
        perf c2c report --force --stdio --show-all --full-symbols --no-source --input "$data_path" \
        >"${session}/perf-c2c-report.txt" 2>"${session}/perf-c2c-report.log" || true

    run_with_sudo "sudo required for perf c2c double-cacheline report" \
        perf c2c report --force --stdio --show-all --full-symbols --no-source --double-cl --input "$data_path" \
        >"${session}/perf-c2c-double-cl.txt" 2>"${session}/perf-c2c-double-cl.log" || true
}

dump_c2c_layout_correlation() {
    local session="$1"
    local layout_dir="${session}/layout-correlation"
    local status_path="${layout_dir}/pahole-status.txt"
    local type_name
    local found_any=false

    if ! has_cmd pahole; then
        info "pahole not found — skipping optional layout correlation dumps"
        return 0
    fi

    mkdir -p "$layout_dir"
    : >"$status_path"

    for type_name in Entry SwissTable ConcurrentKeyspace Shard FrequencySketch Reactor RuntimeMetrics RuntimeMetricsSnapshot; do
        local output_path="${layout_dir}/${type_name}.txt"
        local stderr_path="${output_path}.stderr"

        if pahole -C "$type_name" "$PROFILING_BINARY" >"$output_path" 2>"$stderr_path" && [[ -s "$output_path" ]]; then
            found_any=true
            rm -f "$stderr_path"
            printf 'type=%s status=resolved output=%s\n' "$type_name" "$(basename "$output_path")" >>"$status_path"
        else
            local error_preview=""
            error_preview="$(tr '\n' ' ' <"$stderr_path" 2>/dev/null | sed 's/[[:space:]]\+/ /g' | sed 's/[[:space:]]$//' || true)"
            rm -f "$output_path"
            printf 'type=%s status=missing error=%s\n' "$type_name" "${error_preview:-no output from pahole}" >>"$status_path"
        fi
    done

    if [[ "$found_any" == "true" ]]; then
        ok "Layout correlation dumps: ${layout_dir}"
    else
        warn "pahole did not resolve any requested Vortex types from ${PROFILING_BINARY}; see ${status_path}"
    fi
}

run_c2c_engine_capture() {
    local session="$1"
    local pid_file="${session}/perf-c2c-target.pid"
    local perf_pid=""
    local attempts=0

    header "perf c2c"
    info "Running: perf c2c record -u -- ${PROFILING_BINARY} $(shell_join "${ENGINE_TARGET_ARGS[@]}")"

    run_with_sudo "sudo required for perf c2c engine capture" \
        perf c2c record -u -o "${session}/perf-c2c.data" \
        -- bash -lc 'printf "%s\n" "$$" >"$1"; shift; exec "$@"' _ "$pid_file" "$PROFILING_BINARY" "${ENGINE_TARGET_ARGS[@]}" \
        >"${session}/perf-c2c-record.log" 2>&1 &
    perf_pid=$!

    while [[ ! -s "$pid_file" && $attempts -lt 100 ]]; do
        sleep 0.05
        attempts=$((attempts + 1))
    done

    if [[ -s "$pid_file" ]]; then
        local target_pid
        target_pid="$(tr -d '\n' <"$pid_file")"
        record_session_pid "$target_pid"
        write_c2c_context "$session" "$target_pid"
    else
        write_c2c_context "$session"
    fi

    wait "$perf_pid" 2>/dev/null || warn "perf c2c record exited with a non-zero status"
}

run_c2c_server_capture() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    header "perf c2c"

    start_server "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "${session}/server-c2c.log"
    write_c2c_context "$session" "$SERVER_PID"

    info "Running: perf c2c record -u -p ${SERVER_PID} -- sleep ${duration}"
    ensure_sudo_access "sudo required for perf c2c server capture"
    sudo perf c2c record -u -o "${session}/perf-c2c.data" -p "$SERVER_PID" -- sleep "$duration" \
        >"${session}/perf-c2c-record.log" 2>&1 &
    local perf_pid=$!

    sleep 1
    if [[ -n "$command" ]]; then
        generate_load "$host" "$port" "$command" "$duration" "$clients" "${session}/load-c2c.log"
        wait_for_load "$duration"
    else
        info "No load configured for perf c2c; idling for ${duration}s"
        sleep "$duration"
    fi

    wait "$perf_pid" 2>/dev/null || warn "perf c2c record exited with a non-zero status"
    _profiler_cleanup
    SERVER_PID=""
    LOAD_PID=""
}

run_c2c_focus() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9" clients="${10}"

    write_c2c_tool_check "$session"
    write_c2c_symbolization_context "$session"

    if [[ "$OS" != "linux" ]]; then
        write_c2c_tool_check "$session" "unsupported" "perf c2c is supported only on Linux"
        fatal "--c2c is supported only on Linux"
    fi

    if ! perf_supports_c2c; then
        write_c2c_tool_check "$session" "unsupported" "installed perf does not support c2c"
        fatal "perf c2c is not supported by the installed perf tool"
    fi

    if [[ "${DRY_RUN:-false}" == "true" ]]; then
        write_c2c_tool_check "$session" "dry-run" "validated tooling and permissions only; no capture executed"
        ok "perf c2c dry-run context: ${session}/c2c-tool-check.txt"
        return 0
    fi

    if profiling_target_is_engine; then
        run_c2c_engine_capture "$session"
    else
        run_c2c_server_capture "$session" "$host" "$port" "$threads" "$aof" "$maxmemory" "$eviction" "$command" "$duration" "$clients"
    fi

    reclaim_root_owned_artifact "${session}/perf-c2c.data"
    write_c2c_reports "$session"
    write_c2c_build_ids "$session"
    dump_c2c_layout_correlation "$session"

    if [[ -f "${session}/perf-c2c-stats.txt" ]]; then
        ok "perf c2c stats: ${session}/perf-c2c-stats.txt"
    fi
    if [[ -f "${session}/perf-c2c-report.txt" ]]; then
        ok "perf c2c report: ${session}/perf-c2c-report.txt"
    fi
    if [[ -f "${session}/perf-c2c-double-cl.txt" ]]; then
        ok "perf c2c adjacent-cacheline report: ${session}/perf-c2c-double-cl.txt"
    fi
}