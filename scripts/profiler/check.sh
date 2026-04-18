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

run_check_mode() {
    header "Vortex Profiler — Environment Check"

    printf "${C_BOLD}OS:${C_RESET} %s (%s)\n\n" "$OS" "$(uname -m)"

    printf "${C_BOLD}CPU Profiling:${C_RESET}\n"
    _check_tool cargo-flamegraph
    _check_tool perf
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
    _check_tool cg_annotate
    _check_tool callgrind_annotate
    _check_tool kcachegrind
    echo ""

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
}
