#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/host.sh — lightweight host sampler lifecycle
# ─────────────────────────────────────────────────────────────────────────────

# Expects common.sh to be sourced already.

HOST_SAMPLER_DIR=""
HOST_TELEMETRY_SAMPLES_PATH=""
HOST_TELEMETRY_SUMMARY_PATH=""
HOST_SAMPLER_INTERVAL_SECONDS="${HOST_SAMPLER_INTERVAL_SECONDS:-1}"
HOST_SAMPLER_PIDS=()

_start_host_sampler_process() {
    local output_path="$1"
    shift

    (
        while true; do
            "$@"
            echo ""
            sleep "$HOST_SAMPLER_INTERVAL_SECONDS"
        done
    ) >"$output_path" 2>&1 &

    HOST_SAMPLER_PIDS+=("$!")
}

_sample_socket_summary() {
    local host="$1" port="$2"

    printf 'timestamp=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    if [[ -n "$host" && -n "$port" ]]; then
        printf 'target=%s:%s\n' "$host" "$port"
    else
        printf 'target=none\n'
        return 0
    fi

    if [[ "$OS" == "linux" ]]; then
        if has_cmd ss; then
            ss -s || true
            ss -tin "sport = :${port}" 2>/dev/null || true
        fi
        [[ -r /proc/net/sockstat ]] && cat /proc/net/sockstat
        [[ -r /proc/net/sockstat6 ]] && cat /proc/net/sockstat6
        return 0
    fi

    if [[ "$OS" == "macos" ]] && has_cmd netstat; then
        netstat -s -p tcp 2>/dev/null | head -80 || true
    fi
}

_sample_process_probe() {
    local host="$1" port="$2" explicit_pid="${3:-}"
    local pid="$explicit_pid"

    if [[ -z "$pid" && -n "$port" ]]; then
        pid="$(discover_pid_by_port "$port")"
    fi

    printf 'timestamp=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    if [[ -n "$host" && -n "$port" ]]; then
        printf 'target=%s:%s\n' "$host" "$port"
    else
        printf 'target=none\n'
    fi
    printf 'pid=%s\n' "${pid:-none}"

    if [[ -z "$pid" ]]; then
        return 0
    fi

    if [[ "$OS" == "linux" ]]; then
        [[ -r "/proc/${pid}/status" ]] && cat "/proc/${pid}/status"
        [[ -r "/proc/${pid}/io" ]] && cat "/proc/${pid}/io"
        [[ -r "/proc/${pid}/stat" ]] && cat "/proc/${pid}/stat"
        return 0
    fi

    if [[ "$OS" == "macos" ]] && has_cmd ps; then
        ps -o pid,ppid,pcpu,pmem,rss,vsz,stat,time,command -p "$pid" || true
    fi
}

_sample_low_overhead_tool_pack() {
    local host="$1" port="$2" explicit_pid="${3:-}"
    local pid="$explicit_pid"

    if [[ -z "$pid" && -n "$port" ]]; then
        pid="$(discover_pid_by_port "$port")"
    fi

    printf 'timestamp=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    if [[ -n "$host" && -n "$port" ]]; then
        printf 'target=%s:%s\n' "$host" "$port"
    else
        printf 'target=none\n'
    fi
    printf 'pid=%s\n' "${pid:-none}"

    if [[ "$OS" == "linux" ]]; then
        if has_cmd vmstat; then
            printf '\n--- vmstat ---\n'
            vmstat 1 2 || true
        else
            printf '\n--- vmstat unavailable ---\n'
        fi
        if has_cmd mpstat; then
            printf '\n--- mpstat ---\n'
            mpstat 1 1 || true
        else
            printf '\n--- mpstat unavailable ---\n'
        fi
        if has_cmd pidstat && [[ -n "$pid" ]]; then
            printf '\n--- pidstat ---\n'
            pidstat -p "$pid" -rudw 1 1 || true
        else
            printf '\n--- pidstat unavailable ---\n'
        fi
        if has_cmd iostat; then
            printf '\n--- iostat ---\n'
            iostat -xz 1 1 || true
        else
            printf '\n--- iostat unavailable ---\n'
        fi
        if has_cmd sar; then
            printf '\n--- sar-net ---\n'
            sar -n DEV 1 1 || true
        else
            printf '\n--- sar unavailable ---\n'
        fi
        if has_cmd nstat; then
            printf '\n--- nstat ---\n'
            nstat -az 2>/dev/null || nstat 2>/dev/null || true
        else
            printf '\n--- nstat unavailable ---\n'
        fi
        return 0
    fi

    if [[ "$OS" == "macos" ]]; then
        if has_cmd vm_stat; then
            printf '\n--- vm_stat ---\n'
            vm_stat || true
        else
            printf '\n--- vm_stat unavailable ---\n'
        fi
        if has_cmd iostat; then
            printf '\n--- iostat ---\n'
            iostat -w 1 -c 2 || true
        else
            printf '\n--- iostat unavailable ---\n'
        fi
        if has_cmd netstat; then
            printf '\n--- netstat ---\n'
            netstat -ibn || true
        else
            printf '\n--- netstat unavailable ---\n'
        fi
        if has_cmd sysctl; then
            printf '\n--- sysctl ---\n'
            sysctl hw.ncpu hw.memsize kern.osrelease || true
        else
            printf '\n--- sysctl unavailable ---\n'
        fi
        return 0
    fi
}

start_host_sampler_pack() {
    local session_dir="$1" host="${2:-}" port="${3:-}" pid="${4:-}"
    local session_label

    if [[ ${#HOST_SAMPLER_PIDS[@]} -gt 0 ]]; then
        return 0
    fi

    HOST_SAMPLER_DIR="${session_dir}/host"
    mkdir -p "$HOST_SAMPLER_DIR"

    session_label="$(basename "$session_dir")"
    HOST_TELEMETRY_SAMPLES_PATH="${HOST_SAMPLER_DIR}/${session_label}-host-telemetry.jsonl"
    HOST_TELEMETRY_SUMMARY_PATH="${HOST_SAMPLER_DIR}/${session_label}-host-telemetry-summary.json"

    if has_cmd python3; then
        local args=(
            python3 "${PROFILER_SCRIPT_DIR}/host_telemetry_runner.py"
            --output-dir "$HOST_SAMPLER_DIR"
            --label "$session_label"
            --interval-seconds "$HOST_SAMPLER_INTERVAL_SECONDS"
        )
        if [[ -n "$pid" ]]; then
            args+=(--pid "$pid")
        fi
        if [[ -n "$host" ]]; then
            args+=(--host "$host")
        fi
        if [[ -n "$port" ]]; then
            args+=(--port "$port")
        fi

        "${args[@]}" >"${HOST_SAMPLER_DIR}/host-telemetry.log" 2>&1 &
        HOST_SAMPLER_PIDS+=("$!")
    else
        warn "python3 not found; skipping machine-readable host telemetry capture"
    fi

    _start_host_sampler_process "${HOST_SAMPLER_DIR}/socket-summary.log" _sample_socket_summary "$host" "$port"
    _start_host_sampler_process "${HOST_SAMPLER_DIR}/process-probe.log" _sample_process_probe "$host" "$port" "$pid"
    _start_host_sampler_process "${HOST_SAMPLER_DIR}/low-overhead-tool-pack.log" _sample_low_overhead_tool_pack "$host" "$port" "$pid"
}

stop_host_sampler_pack() {
    local pid

    if [[ ${#HOST_SAMPLER_PIDS[@]} -eq 0 ]]; then
        return 0
    fi

    for pid in "${HOST_SAMPLER_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done

    for pid in "${HOST_SAMPLER_PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    HOST_SAMPLER_PIDS=()
}
