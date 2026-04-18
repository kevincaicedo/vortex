from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .markdown import render_markdown_report


def _report_id(path: Path) -> str:
    stem = path.stem
    return stem[:-7] if stem.endswith("_report") else stem


def build_publication_artifacts(
    reports_dir: Path,
    chart_paths: dict[str, Path],
) -> dict[str, Any]:
    latest_dir = reports_dir / "latest"
    latest_assets_dir = latest_dir / "assets"
    return {
        "latest": {
            "json": str(latest_dir / "report.json"),
            "csv": str(latest_dir / "report.csv"),
            "markdown": str(latest_dir / "report.md"),
            "charts": {
                name: str(latest_assets_dir / path.name) for name, path in chart_paths.items()
            },
        },
        "index": str(reports_dir / "index.json"),
    }


def _history_entry(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows") or []
    artifacts = payload.get("artifacts") or {}
    return {
        "report_id": _report_id(path),
        "generated_at": payload.get("generated_at"),
        "title": payload.get("title"),
        "json": str(path),
        "csv": artifacts.get("csv"),
        "markdown": artifacts.get("markdown"),
        "charts": artifacts.get("charts") or {},
        "source_run_count": len(payload.get("source_runs") or []),
        "row_count": len(rows),
        "databases": [item.get("database") for item in payload.get("databases") or []],
        "backends": sorted({row.get("backend") for row in rows if row.get("backend")}),
        "has_aof_overhead": bool(payload.get("aof_overhead")),
        "has_evictions": bool(payload.get("eviction_rows")),
    }


def _build_history_index(
    reports_dir: Path,
    latest_payload: dict[str, Any],
    current_json_path: Path,
) -> dict[str, Any]:
    history = [_history_entry(path) for path in sorted(reports_dir.glob("*_report.json"))]
    latest = (latest_payload.get("artifacts") or {}).get("latest") or {}
    rows = latest_payload.get("rows") or []
    return {
        "schema_version": 1,
        "generated_at": latest_payload.get("generated_at"),
        "latest": {
            "report_id": _report_id(current_json_path),
            "title": latest_payload.get("title"),
            "generated_at": latest_payload.get("generated_at"),
            "json": latest.get("json"),
            "csv": latest.get("csv"),
            "markdown": latest.get("markdown"),
            "charts": latest.get("charts") or {},
            "source_run_count": len(latest_payload.get("source_runs") or []),
            "row_count": len(rows),
            "databases": [item.get("database") for item in latest_payload.get("databases") or []],
            "backends": sorted({row.get("backend") for row in rows if row.get("backend")}),
        },
        "reports": history,
    }


def publish_report_artifacts(
    payload: dict[str, Any],
    json_path: Path,
    csv_path: Path,
    chart_paths: dict[str, Path],
) -> Path:
    artifacts = payload.get("artifacts") or {}
    latest = artifacts.get("latest") or {}
    latest_json_path = Path(latest["json"])
    latest_csv_path = Path(latest["csv"])
    latest_markdown_path = Path(latest["markdown"])
    latest_chart_paths = {name: Path(path) for name, path in (latest.get("charts") or {}).items()}

    latest_json_path.parent.mkdir(parents=True, exist_ok=True)
    latest_csv_path.parent.mkdir(parents=True, exist_ok=True)
    if latest_chart_paths:
        shutil.rmtree(next(iter(latest_chart_paths.values())).parent, ignore_errors=True)
        next(iter(latest_chart_paths.values())).parent.mkdir(parents=True, exist_ok=True)

    shutil.copy2(json_path, latest_json_path)
    shutil.copy2(csv_path, latest_csv_path)
    for name, source in chart_paths.items():
        shutil.copy2(source, latest_chart_paths[name])

    latest_markdown_path.write_text(
        render_markdown_report(payload, latest_markdown_path, latest_chart_paths),
        encoding="utf-8",
    )

    index_path = Path(artifacts["index"])
    index_path.write_text(
        json.dumps(_build_history_index(json_path.parent, payload, json_path), indent=2) + "\n",
        encoding="utf-8",
    )
    return index_path