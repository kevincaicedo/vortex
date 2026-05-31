from __future__ import annotations

import json
import os
import platform
import shlex
import subprocess
from pathlib import Path
from typing import Any, Iterable, Optional

from vortex_benchmark.models import timestamp_slug, utc_now


TARGET_MODES = ("local", "host-port", "ssh-managed", "ssh-attach")


def command_line(argv: Optional[Iterable[str]] = None) -> str:
    values = list(argv if argv is not None else [])
    return " ".join(shlex.quote(str(value)) for value in values)


def redact(value: str, redactions: Iterable[str] = ()) -> str:
    rendered = value
    secrets = [item for item in redactions if item]
    for env_name in ("HOST_PASSWORD", "PASSWORD", "TOKEN", "SECRET"):
        env_value = os.environ.get(env_name)
        if env_value:
            secrets.append(env_value)
    for secret in secrets:
        rendered = rendered.replace(secret, "[redacted]")
    return rendered


def build_ssh_options(
    *,
    port: int | None = None,
    identity_file: str | None = None,
    config_file: str | None = None,
    extra_options: Iterable[str] = (),
    connect_timeout: int | None = None,
    scp: bool = False,
) -> list[str]:
    options: list[str] = []
    if config_file:
        options.extend(["-F", str(config_file)])
    if identity_file:
        options.extend(["-i", str(identity_file)])
    if port is not None:
        if port <= 0:
            raise ValueError("--ssh-port must be positive")
        options.extend(["-P" if scp else "-p", str(port)])
    if connect_timeout is not None:
        if connect_timeout <= 0:
            raise ValueError("--ssh-connect-timeout must be positive")
        options.extend(["-o", f"ConnectTimeout={connect_timeout}"])
    for option in extra_options:
        if option:
            options.extend(["-o", str(option)])
    return options


def make_session_dir(root: Path, target_mode: str, stamp: str | None = None) -> Path:
    session_id = stamp or timestamp_slug()
    path = root / "sessions" / target_mode / session_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_session_header(
    *,
    session_dir: Path,
    artifact_root: Path,
    target_mode: str,
    command: str,
    profile: str | None,
    target: dict[str, Any],
    workload_contract: dict[str, Any],
    artifact_paths: dict[str, Any],
    status: str = "running",
    exit_code: int | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "tool": "vortex_bench",
        "tool_role": "benchmark",
        "version": "0.1.0a0",
        "status": status,
        "exit_code": exit_code,
        "started_at": started_at or utc_now(),
        "ended_at": ended_at,
        "host": {
            "os": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "hostname": platform.node(),
        },
        "command_line": command,
        "target_mode": target_mode,
        "profile": profile,
        "target": target,
        "workload_contract": workload_contract,
        "artifact_root": str(artifact_root),
        "session_dir": str(session_dir),
        "artifact_paths": artifact_paths,
    }


def write_session_json(session_dir: Path, payload: dict[str, Any]) -> Path:
    session_dir.mkdir(parents=True, exist_ok=True)
    path = session_dir / "session.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def update_session_json(session_dir: Path, **updates: Any) -> Path:
    path = session_dir / "session.json"
    payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    payload.update(updates)
    return write_session_json(session_dir, payload)


def run_ssh(
    target: str,
    remote_command: str,
    *,
    timeout: float = 30.0,
    redactions: Iterable[str] = (),
    ssh_options: Iterable[str] = (),
) -> dict[str, Any]:
    argv = ["ssh", *list(ssh_options), target, remote_command]
    started_at = utc_now()
    try:
        completed = subprocess.run(
            argv,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        return {
            "command": redact(command_line(argv), redactions),
            "started_at": started_at,
            "completed_at": utc_now(),
            "exit_code": completed.returncode,
            "stdout": redact(completed.stdout.strip(), redactions),
            "stderr": redact(completed.stderr.strip(), redactions),
        }
    except (OSError, subprocess.SubprocessError) as error:
        return {
            "command": redact(command_line(argv), redactions),
            "started_at": started_at,
            "completed_at": utc_now(),
            "exit_code": 127,
            "stdout": "",
            "stderr": redact(str(error), redactions),
        }


def copy_ssh_path(
    target: str,
    remote_path: str,
    local_dir: Path,
    *,
    timeout: float = 120.0,
    redactions: Iterable[str] = (),
    scp_options: Iterable[str] = (),
) -> dict[str, Any]:
    local_dir.mkdir(parents=True, exist_ok=True)
    argv = ["scp", *list(scp_options), "-r", f"{target}:{remote_path}", str(local_dir)]
    started_at = utc_now()
    try:
        completed = subprocess.run(
            argv,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        return {
            "command": redact(command_line(argv), redactions),
            "started_at": started_at,
            "completed_at": utc_now(),
            "exit_code": completed.returncode,
            "stdout": redact(completed.stdout.strip(), redactions),
            "stderr": redact(completed.stderr.strip(), redactions),
            "local_dir": str(local_dir),
        }
    except (OSError, subprocess.SubprocessError) as error:
        return {
            "command": redact(command_line(argv), redactions),
            "started_at": started_at,
            "completed_at": utc_now(),
            "exit_code": 127,
            "stdout": "",
            "stderr": redact(str(error), redactions),
            "local_dir": str(local_dir),
        }


def copy_source_to_ssh(
    target: str,
    source_dir: Path,
    remote_dir: str,
    *,
    timeout: float = 300.0,
    redactions: Iterable[str] = (),
    ssh_options: Iterable[str] = (),
) -> dict[str, Any]:
    """Copy the local checkout to a remote working directory with rsync."""

    remote_shell = ["ssh", *list(ssh_options)]
    argv = [
        "rsync",
        "-az",
        "-e",
        command_line(remote_shell),
        "--exclude",
        ".git",
        "--exclude",
        "target",
        "--exclude",
        ".artifacts",
        f"{source_dir.resolve()}/",
        f"{target}:{remote_dir.rstrip('/')}/",
    ]
    started_at = utc_now()
    try:
        completed = subprocess.run(
            argv,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        return {
            "command": redact(command_line(argv), redactions),
            "started_at": started_at,
            "completed_at": utc_now(),
            "exit_code": completed.returncode,
            "stdout": redact(completed.stdout.strip(), redactions),
            "stderr": redact(completed.stderr.strip(), redactions),
            "remote_dir": remote_dir,
        }
    except (OSError, subprocess.SubprocessError) as error:
        return {
            "command": redact(command_line(argv), redactions),
            "started_at": started_at,
            "completed_at": utc_now(),
            "exit_code": 127,
            "stdout": "",
            "stderr": redact(str(error), redactions),
            "remote_dir": remote_dir,
        }
