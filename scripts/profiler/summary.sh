#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/profiler/summary.sh — post-session summary helpers
# ─────────────────────────────────────────────────────────────────────────────

generate_post_session_summary() {
    local session_dir="$1"
    local compare_to="${2:-}"

    if ! has_cmd python3; then
        warn "python3 not found; skipping profiler session summary generation"
        return 0
    fi

    SESSION_SUMMARY_PATH="${session_dir}/summary.json"

    local args=(
        "${PROFILER_SCRIPT_DIR}/write_session_summary.py"
        --session-dir "$session_dir"
        --output-path "$SESSION_SUMMARY_PATH"
    )

    if [[ -n "$compare_to" ]]; then
        args+=(--compare-to "$compare_to")
    fi

    python3 "${args[@]}" >"${session_dir}/summary.log" 2>&1 \
        || warn "Profiler summary generation failed. See ${session_dir}/summary.log"
}