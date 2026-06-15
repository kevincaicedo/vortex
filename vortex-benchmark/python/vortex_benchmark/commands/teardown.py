from __future__ import annotations

from pathlib import Path
from typing import Optional

from vortex_benchmark.db import stop_service
from vortex_benchmark.env import load_environment_state, save_environment_state
from vortex_benchmark.models import EnvironmentState, utc_now


def _resolve_state(args, preloaded_state: Optional[EnvironmentState]) -> EnvironmentState:
    if preloaded_state is not None:
        return preloaded_state
    if not args.state_file:
        raise ValueError("teardown requires --state-file")
    return load_environment_state(Path(args.state_file).expanduser().resolve())


def execute_teardown(args, preloaded_state: Optional[EnvironmentState] = None) -> EnvironmentState:
    state = _resolve_state(args, preloaded_state)
    errors: list[str] = []
    had_external = False

    for service in reversed(state.services):
        try:
            stop_service(service)
            had_external = had_external or service.status == "detached"
        except Exception as error:
            errors.append(f"{service.database}: {error}")

    if errors:
        state.status = "teardown-partial"
    elif had_external:
        state.status = "detached"
    else:
        state.status = "stopped"
    state.updated_at = utc_now()
    save_environment_state(state, Path(state.state_file))

    print(f"environment stopped: {state.environment_id}")
    print(f"state file updated: {state.state_file}")
    if errors:
        raise RuntimeError("teardown completed with errors: " + "; ".join(errors))
    return state
