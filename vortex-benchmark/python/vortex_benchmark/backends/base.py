from __future__ import annotations

import json
import math
import re
import shlex
import shutil
import socket
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from vortex_benchmark.env import ArtifactLayout
    from vortex_benchmark.manifests import ResolvedBenchmarkSpec
    from vortex_benchmark.models import EnvironmentState, ServiceState


DEFAULT_REDIS_REQUESTS = 100_000
DEFAULT_REDIS_CLIENTS = 100
DEFAULT_REDIS_PIPELINE = 1
DEFAULT_REDIS_KEYSPACE = 1_000

DEFAULT_MEMTIER_REQUESTS = 2_000
DEFAULT_MEMTIER_CLIENTS = 100
DEFAULT_MEMTIER_PIPELINE = 1
DEFAULT_MEMTIER_THREAD_SWEEP = (1, 2, 4, 8)
DEFAULT_MEMTIER_KEYSPACE = 10_000
DEFAULT_MEMTIER_DATA_SIZE = 384

DEFAULT_CUSTOM_THREAD_SWEEP = (1, 2, 4, 8)
DEFAULT_CUSTOM_KEYSPACE = 10_000
DEFAULT_CUSTOM_OPS_PER_THREAD = 50_000
DEFAULT_CUSTOM_WARMUP_OPS = 5_000
DEFAULT_CUSTOM_VALUE_SIZE = 64


class BackendError(RuntimeError):
    pass


@dataclass
class BackendRunContext:
    spec: "ResolvedBenchmarkSpec"
    state: "EnvironmentState"
    service: "ServiceState"
    layout: "ArtifactLayout"
    request_path: Path
    run_id: str
    selected_workloads: list[str]
    selected_commands: list[str]


@dataclass
class BackendExecutionRecord:
    backend: str
    database: str
    status: str
    started_at: str
    completed_at: str
    duration_seconds: float
    selection: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)
    items: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def quote_command(command: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def ensure_command_available(name: str) -> None:
    if shutil.which(name) is None:
        raise BackendError(f"required command not found in PATH: {name}")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def tail_text(path: Path, *, max_lines: int = 20) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-max_lines:]).strip()


def run_process(
    command: list[str],
    *,
    stdout_path: Path,
    stderr_path: Path,
    cwd: Optional[Path] = None,
    check: bool = True,
) -> tuple[subprocess.CompletedProcess[str], float]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)

    started = time.perf_counter()
    try:
        with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
            "w", encoding="utf-8"
        ) as stderr_handle:
            result = subprocess.run(
                command,
                cwd=cwd,
                check=False,
                text=True,
                stdout=stdout_handle,
                stderr=stderr_handle,
            )
    except FileNotFoundError as error:
        raise BackendError(f"required command not found in PATH: {command[0]}") from error

    elapsed = time.perf_counter() - started
    if check and result.returncode != 0:
        detail = tail_text(stderr_path) or tail_text(stdout_path) or (
            f"command exited with status {result.returncode}"
        )
        raise BackendError(f"command failed: {quote_command(command)}\n{detail}")
    return result, elapsed


def _setting_aliases(value: str) -> tuple[str, ...]:
    normalized = value.strip()
    return (
        normalized,
        normalized.replace("-", "_"),
        normalized.replace("_", "-"),
    )


def backend_settings(settings: dict[str, Any], backend_name: str) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for alias in _setting_aliases(backend_name):
        value = settings.get(alias)
        if isinstance(value, dict):
            resolved.update(value)
    return resolved


def get_setting(
    settings: dict[str, Any],
    backend_name: str,
    key: str,
    default: Any = None,
) -> Any:
    nested = backend_settings(settings, backend_name)
    for alias in _setting_aliases(key):
        if alias in nested:
            return nested[alias]
    for alias in _setting_aliases(key):
        if alias in settings and not isinstance(settings[alias], dict):
            return settings[alias]
    return default


def parse_duration_seconds(duration: Optional[str]) -> Optional[int]:
    if not duration:
        return None

    match = re.fullmatch(r"(\d+)(ms|s|m|h)", duration.strip())
    if not match:
        raise BackendError(
            f"unsupported duration literal for backend execution: {duration}. Use forms like 500ms, 60s, 5m, or 1h."
        )

    amount = int(match.group(1))
    unit = match.group(2)
    if unit == "ms":
        return max(1, int(math.ceil(amount / 1000.0)))
    if unit == "s":
        return amount
    if unit == "m":
        return amount * 60
    return amount * 3600


def dedupe_ordered(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def encode_resp_command(parts: list[str]) -> str:
    payload = [f"*{len(parts)}\r\n"]
    for part in parts:
        encoded = str(part)
        payload.append(f"${len(encoded.encode('utf-8'))}\r\n{encoded}\r\n")
    return "".join(payload)


def encode_resp_batch(commands: list[list[str]]) -> str:
    return "".join(encode_resp_command(command) for command in commands)


def _read_resp_line(handle) -> bytes:
    line = handle.readline()
    if not line:
        raise BackendError("connection closed while reading RESP reply")
    if not line.endswith(b"\r\n"):
        raise BackendError("received malformed RESP line without CRLF terminator")
    return line[:-2]


def _read_resp_reply(handle) -> None:
    prefix = handle.read(1)
    if not prefix:
        raise BackendError("connection closed while reading RESP reply prefix")
    if prefix in {b"+", b":"}:
        _read_resp_line(handle)
        return
    if prefix == b"-":
        message = _read_resp_line(handle).decode("utf-8", errors="replace")
        raise BackendError(f"server returned error while preloading seed data: {message}")
    if prefix == b"$":
        length = int(_read_resp_line(handle))
        if length >= 0:
            payload = handle.read(length + 2)
            if len(payload) != length + 2 or not payload.endswith(b"\r\n"):
                raise BackendError("received malformed RESP bulk-string reply")
        return
    if prefix == b"*":
        length = int(_read_resp_line(handle))
        if length >= 0:
            for _ in range(length):
                _read_resp_reply(handle)
        return
    raise BackendError(f"received unsupported RESP reply prefix while preloading seed data: {prefix!r}")


def send_resp_pipeline(
    host: str,
    port: int,
    commands: list[list[str]],
    *,
    batch_size: int = 1000,
    connect_timeout: float = 5.0,
    io_timeout: float = 30.0,
) -> None:
    if not commands:
        return

    try:
        with socket.create_connection((host, port), timeout=connect_timeout) as connection:
            connection.settimeout(io_timeout)
            with connection.makefile("rb") as reader:
                for index in range(0, len(commands), batch_size):
                    batch = commands[index : index + batch_size]
                    payload = encode_resp_batch(batch).encode("utf-8")
                    connection.sendall(payload)
                    for _ in batch:
                        _read_resp_reply(reader)
    except OSError as error:
        raise BackendError(f"failed to preload seed data via RESP pipeline to {host}:{port}") from error


def coerce_int_list(value: Any, *, label: str, default: tuple[int, ...]) -> list[int]:
    if value is None:
        return list(default)

    parts: list[Any]
    if isinstance(value, int):
        parts = [value]
    elif isinstance(value, str):
        parts = [part.strip() for part in value.split(",") if part.strip()]
    elif isinstance(value, (list, tuple)):
        parts = list(value)
    else:
        raise BackendError(f"{label} must be an integer or a comma-separated integer list")

    resolved: list[int] = []
    for part in parts:
        try:
            number = int(part)
        except (TypeError, ValueError) as error:
            raise BackendError(f"{label} contains a non-integer value: {part}") from error
        if number <= 0:
            raise BackendError(f"{label} entries must be positive integers")
        resolved.append(number)
    return resolved


def coerce_positive_int(value: Any, *, label: str, default: int) -> int:
    if value is None:
        return default
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise BackendError(f"{label} must be a positive integer") from error
    if number <= 0:
        raise BackendError(f"{label} must be a positive integer")
    return number


def coerce_nonnegative_int(value: Any, *, label: str, default: Optional[int]) -> Optional[int]:
    if value is None:
        return default
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise BackendError(f"{label} must be a non-negative integer") from error
    if number < 0:
        raise BackendError(f"{label} must be a non-negative integer")
    return number
