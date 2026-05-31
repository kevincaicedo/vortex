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

    if [[ -f "$SESSION_SUMMARY_PATH" ]]; then
        python3 - "$SESSION_SUMMARY_PATH" "${session_dir}/summary.md" <<'PY' \
            >"${session_dir}/summary-markdown.log" 2>&1 \
            || warn "Profiler Markdown summary generation failed. See ${session_dir}/summary-markdown.log"
from __future__ import annotations

import json
import sys
from pathlib import Path

summary_path = Path(sys.argv[1])
markdown_path = Path(sys.argv[2])
payload = json.loads(summary_path.read_text(encoding="utf-8"))

session = payload.get("session") or payload
tools = payload.get("tools") or payload.get("tools_executed") or session.get("tools_executed") or []
if isinstance(tools, str):
    tools = [tools]

lines = [
    "# Profiler Session Report",
    "",
    "## Summary",
    "",
    f"- **Session:** `{session.get('session_id') or summary_path.parent.name}`",
    f"- **Status:** `{session.get('status') or payload.get('status') or 'unknown'}`",
    f"- **Mode:** `{session.get('mode') or payload.get('mode') or 'n/a'}`",
    f"- **Target mode:** `{session.get('target_mode') or (session.get('target') or {}).get('mode') or 'n/a'}`",
    f"- **Tools:** `{', '.join(str(tool) for tool in tools) or 'n/a'}`",
    f"- **Summary JSON:** `{summary_path}`",
    "",
]

workload = session.get("workload") or payload.get("workload") or {}
if workload:
    lines.extend(
        [
            "## Workload",
            "",
            f"- **Source:** `{workload.get('source') or 'n/a'}`",
            f"- **Command:** `{workload.get('command') or 'n/a'}`",
            f"- **Duration:** `{workload.get('duration_seconds') or 'n/a'}` seconds",
            f"- **Clients:** `{workload.get('clients') or 'n/a'}`",
            "",
        ]
    )

warnings = payload.get("warnings") or payload.get("reliability_warnings") or []
if warnings:
    lines.extend(["## Warnings", ""])
    for warning in warnings:
        lines.append(f"- {warning}")
    lines.append("")

artifacts = payload.get("artifacts") or payload.get("artifact_paths") or session.get("artifact_paths") or []
if isinstance(artifacts, dict):
    artifact_values = [value for value in artifacts.values() if value]
else:
    artifact_values = artifacts
if artifact_values:
    lines.extend(["## Artifacts", ""])
    for artifact in artifact_values[:40]:
        lines.append(f"- `{artifact}`")
    if len(artifact_values) > 40:
        lines.append(f"- ... {len(artifact_values) - 40} more in summary JSON")
    lines.append("")

markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
    fi
}
