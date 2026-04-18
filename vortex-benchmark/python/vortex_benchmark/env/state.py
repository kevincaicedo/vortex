from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from vortex_benchmark.models import EnvironmentState


def save_environment_state(state: EnvironmentState, path: Optional[Path] = None) -> Path:
    target = path or Path(state.state_file)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(state.to_dict(), indent=2) + "\n", encoding="utf-8")
    return target


def load_environment_state(path: Path) -> EnvironmentState:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return EnvironmentState.from_dict(payload)
