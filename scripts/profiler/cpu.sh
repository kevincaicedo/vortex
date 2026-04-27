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


# ── BPF Tools — Escalation-mode diagnostics (Linux only) ─────────────────────
# These run as optional add-ons inside focus modes. They require bcc-tools
# or bpftrace to be installed and root/CAP_BPF privileges.

bpf_capture_failed() {
    local output_path="$1"
    [[ -s "$output_path" ]] || return 1

    grep -Eq \
        'Failed to compile BPF module|Traceback \(most recent call last\)|use of undeclared identifier|invalid application of .* incomplete type|[0-9]+ errors generated\.' \
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
    local command="$8" duration="$9"

    # Try bcc-tools runqlat first, then bpftrace fallback
    local runqlat_bin=""
    for candidate in runqlat runqlat-bpfcc /usr/share/bcc/tools/runqlat; do
        if has_cmd "$candidate" || [[ -x "$candidate" ]]; then
            runqlat_bin="$candidate"
            break
        fi
    done

    if [[ -z "$runqlat_bin" ]]; then
        info "runqlat not found — install bcc-tools for run-queue latency histograms (skipping)"
        return 0
    fi

    header "BPF: runqlat (run-queue latency)"

    ensure_sudo_access "sudo required for BPF runqlat"

    info "Running: $runqlat_bin ${duration}s 1 (one histogram over full duration)"
    run_with_sudo "BPF runqlat" timeout $((duration + 5)) "$runqlat_bin" "$duration" 1 \
        >"${session}/bpf-runqlat.txt" 2>&1 || true

    if [[ -s "${session}/bpf-runqlat.txt" ]]; then
        ok "BPF runqlat histogram: ${session}/bpf-runqlat.txt"
    else
        warn "runqlat produced no output (may need CAP_BPF or kernel headers)"
    fi
}

run_bpf_biolatency() {
    local session="$1" host="$2" port="$3" threads="$4" aof="$5" maxmemory="$6" eviction="$7"
    local command="$8" duration="$9"

    local biolatency_bin=""
    for candidate in biolatency biolatency-bpfcc /usr/share/bcc/tools/biolatency; do
        if has_cmd "$candidate" || [[ -x "$candidate" ]]; then
            biolatency_bin="$candidate"
            break
        fi
    done

    if [[ -z "$biolatency_bin" ]]; then
        info "biolatency not found — install bcc-tools for block I/O latency histograms (skipping)"
        return 0
    fi

    header "BPF: biolatency (block I/O latency)"

    ensure_sudo_access "sudo required for BPF biolatency"

    info "Running: $biolatency_bin ${duration}s 1"
    run_with_sudo "BPF biolatency" timeout $((duration + 5)) "$biolatency_bin" "$duration" 1 \
        >"${session}/bpf-biolatency.txt" 2>&1 || true

    if [[ -s "${session}/bpf-biolatency.txt" ]]; then
        ok "BPF biolatency histogram: ${session}/bpf-biolatency.txt"
    else
        warn "biolatency produced no output (may need CAP_BPF or kernel headers)"
    fi
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
