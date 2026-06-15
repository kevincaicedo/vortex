#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/check.sh — tool availability diagnostics
# ─────────────────────────────────────────────────────────────────────────────

_check_tool() {
    local name="$1" desc="${2:-}"
    if has_cmd "$name"; then
        printf "  ${C_GREEN}✓${C_RESET} %-24s %s\n" "$name" "$(command -v "$name")"
    else
        printf "  ${C_DIM}✗ %-24s not found${C_RESET}\n" "$name"
    fi
}

_check_any_tool() {
    local label="$1"
    shift
    local candidate=""

    for candidate in "$@"; do
        if has_cmd "$candidate"; then
            printf "  ${C_GREEN}✓${C_RESET} %-24s %s\n" "$label" "$(command -v "$candidate")"
            return 0
        fi
        if [[ -x "$candidate" ]]; then
            printf "  ${C_GREEN}✓${C_RESET} %-24s %s\n" "$label" "$candidate"
            return 0
        fi
    done

    printf "  ${C_DIM}✗ %-24s not found${C_RESET}\n" "$label"
}

_print_platform_preflight() {
    printf "${C_BOLD}Platform evidence boundary:${C_RESET}\n"
    if has_cmd python3; then
        PYTHONPATH="${REPO_ROOT}/vortex-benchmark/python" \
            python3 "${PROFILER_SCRIPT_DIR}/platform_preflight.py" --format text \
            | sed 's/^/  /'
    else
        printf "  platform=%s\n" "$OS"
        printf "  python3 not found; machine-readable platform preflight unavailable\n"
    fi
    echo ""
}

_write_platform_preflight_artifacts() {
    has_cmd python3 || return 0

    local check_dir
    check_dir="$(make_session_dir check)"
    PYTHONPATH="${REPO_ROOT}/vortex-benchmark/python" \
        python3 "${PROFILER_SCRIPT_DIR}/platform_preflight.py" --format json \
        >"${check_dir}/profiler-platform-preflight.json"
    PYTHONPATH="${REPO_ROOT}/vortex-benchmark/python" \
        python3 "${PROFILER_SCRIPT_DIR}/platform_preflight.py" --system Darwin --format json \
        >"${check_dir}/profiler-platform-preflight-darwin-simulated.json"
    ok "Platform preflight artifacts: ${check_dir}"
}

run_check_mode() {
    header "Vortex Profiler — Environment Check"

    printf "${C_BOLD}OS:${C_RESET} %s (%s)\n\n" "$OS" "$(uname -m)"
    _print_platform_preflight

    printf "${C_BOLD}CPU Profiling:${C_RESET}\n"
    _check_tool cargo-flamegraph
    _check_tool perf
    if perf_supports_c2c; then
        printf "  ${C_GREEN}✓${C_RESET} %-24s %s\n" "perf c2c" "supported by $(command -v perf)"
    else
        printf "  ${C_DIM}✗ %-24s unsupported by installed perf${C_RESET}\n" "perf c2c"
    fi
    _check_tool samply
    if [[ "$OS" == "macos" ]]; then
        _check_tool xcrun "(Instruments via xcrun xctrace)"
    fi
    echo ""

    printf "${C_BOLD}Memory Profiling:${C_RESET}\n"
    _check_tool heaptrack
    _check_tool valgrind "(massif via valgrind --tool=massif)"
    if [[ "$OS" == "macos" ]]; then
        _check_tool xcrun "(Instruments Allocations via xcrun xctrace)"
    fi
    echo ""

    printf "${C_BOLD}Cache / Call-Graph:${C_RESET}\n"
    _check_tool valgrind "(cachegrind, callgrind)"
    _check_tool pahole
    _check_tool taskset
    _check_tool cg_annotate
    _check_tool callgrind_annotate
    _check_tool kcachegrind
    echo ""

    printf "${C_BOLD}Blocking / Off-CPU:${C_RESET}\n"
    _check_any_tool runqlat runqlat runqlat-bpfcc /usr/share/bcc/tools/runqlat
    _check_any_tool biolatency biolatency biolatency-bpfcc /usr/share/bcc/tools/biolatency
    _check_any_tool offcputime offcputime offcputime-bpfcc /usr/share/bcc/tools/offcputime
    _check_any_tool offwaketime offwaketime offwaketime-bpfcc /usr/share/bcc/tools/offwaketime
    _check_tool bpftrace
    if has_cmd perf; then
        printf "  ${C_GREEN}✓${C_RESET} %-24s %s\n" "perf sched" "available via $(command -v perf)"
    else
        printf "  ${C_DIM}✗ %-24s perf not found${C_RESET}\n" "perf sched"
    fi
    echo ""

    if [[ "$OS" == "linux" ]]; then
        printf "${C_BOLD}Linux perf permissions:${C_RESET}\n"
        if [[ -r /proc/sys/kernel/perf_event_paranoid ]]; then
            printf "  %-26s %s\n" "perf_event_paranoid" "$(tr -d '\n' </proc/sys/kernel/perf_event_paranoid)"
        fi
        if [[ -r /proc/sys/kernel/kptr_restrict ]]; then
            printf "  %-26s %s\n" "kptr_restrict" "$(tr -d '\n' </proc/sys/kernel/kptr_restrict)"
        fi
        echo ""
    fi

    printf "${C_BOLD}Load Generation:${C_RESET}\n"
    _check_tool redis-benchmark
    echo ""

    printf "${C_BOLD}Profiling Binary:${C_RESET}\n"
    if [[ -x "$PROFILING_BINARY" ]]; then
        printf "  ${C_GREEN}✓${C_RESET} %s\n" "$PROFILING_BINARY"
        if [[ "$OS" == "macos" ]]; then
            if binary_has_get_task_allow "$PROFILING_BINARY"; then
                printf "  ${C_GREEN}✓${C_RESET} com.apple.security.get-task-allow enabled\n"
            else
                printf "  ${C_YELLOW}⚠${C_RESET} binary is not currently attachable by Instruments (missing get-task-allow)\n"
            fi
        fi
    else
        printf "  ${C_DIM}✗ %s (not built — will build automatically)${C_RESET}\n" "$PROFILING_BINARY"
    fi
    echo ""

    printf "${C_BOLD}Cargo Profile:${C_RESET}\n"
    if grep -q "profile.profiling" "${REPO_ROOT}/Cargo.toml" 2>/dev/null; then
        printf "  ${C_GREEN}✓${C_RESET} [profile.profiling] found in Cargo.toml\n"
    else
        printf "  ${C_RED}✗${C_RESET} [profile.profiling] NOT found in Cargo.toml\n"
    fi

    if [[ -f "${REPO_ROOT}/.cargo/config.toml" ]]; then
        printf "  ${C_GREEN}✓${C_RESET} .cargo/config.toml exists\n"
        if grep -q "force-frame-pointers" "${REPO_ROOT}/.cargo/config.toml" 2>/dev/null; then
            printf "  ${C_GREEN}✓${C_RESET} force-frame-pointers=yes configured\n"
        else
            printf "  ${C_YELLOW}⚠${C_RESET} force-frame-pointers not configured\n"
        fi
        if grep -q "symbol-mangling-version" "${REPO_ROOT}/.cargo/config.toml" 2>/dev/null; then
            printf "  ${C_GREEN}✓${C_RESET} symbol-mangling-version=v0 configured\n"
        else
            printf "  ${C_YELLOW}⚠${C_RESET} symbol-mangling-version not configured\n"
        fi
    else
        printf "  ${C_YELLOW}⚠${C_RESET} .cargo/config.toml not found\n"
    fi

    if [[ "${DRY_RUN:-false}" == "true" ]]; then
        echo ""
        _write_platform_preflight_artifacts
    fi
}
