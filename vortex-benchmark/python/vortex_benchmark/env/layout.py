from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class ArtifactLayout:
    root: Path
    environments_dir: Path
    logs_dir: Path
    requests_dir: Path
    results_dir: Path
    backend_runs_dir: Path
    reports_dir: Path
    runtime_dir: Path


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def resolve_benchmark_root() -> Path:
    return Path(__file__).resolve().parents[3]


def build_layout(output_dir: Optional[str]) -> ArtifactLayout:
    root = (
        Path(output_dir).expanduser().resolve()
        if output_dir
        else resolve_benchmark_root() / ".artifacts"
    )
    layout = ArtifactLayout(
        root=root,
        environments_dir=root / "environments",
        logs_dir=root / "logs",
        requests_dir=root / "requests",
        results_dir=root / "results",
        backend_runs_dir=root / "backend-runs",
        reports_dir=root / "reports",
        runtime_dir=root / "runtime",
    )
    for path in (
        layout.root,
        layout.environments_dir,
        layout.logs_dir,
        layout.requests_dir,
        layout.results_dir,
        layout.backend_runs_dir,
        layout.reports_dir,
        layout.runtime_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return layout
