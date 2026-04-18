from __future__ import annotations

import os
import re
import shlex
import shutil
import signal
import subprocess
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Optional

from vortex_benchmark.env.readiness import wait_until_ready
from vortex_benchmark.models import (
    DEFAULT_AOF_FSYNC,
    DEFAULT_EVICTION_POLICY,
    ServiceState,
    utc_now,
)


class SetupError(RuntimeError):
    pass


SIZE_MULTIPLIERS = {
    "": 1,
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "mib": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
    "gib": 1024**3,
    "t": 1024**4,
    "tb": 1024**4,
    "tib": 1024**4,
}
SIZE_LITERAL_RE = re.compile(r"^\s*(\d+)\s*([a-zA-Z]*)\s*$")


def format_command(command: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in command)


def require_command(name: str) -> None:
    if shutil.which(name) is None:
        raise SetupError(f"required command not found in PATH: {name}")


def run_checked(
    command: list[str],
    *,
    cwd: Optional[Path] = None,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            check=True,
            text=True,
            capture_output=capture_output,
        )
    except FileNotFoundError as error:
        raise SetupError(f"required command not found in PATH: {command[0]}") from error
    except subprocess.CalledProcessError as error:
        stderr = (error.stderr or "").strip()
        stdout = (error.stdout or "").strip()
        detail = stderr or stdout or f"command exited with status {error.returncode}"
        raise SetupError(f"command failed: {format_command(command)}\n{detail}") from error


def best_effort_command_output(
    command: list[str], *, cwd: Optional[Path] = None
) -> Optional[str]:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            text=True,
            capture_output=True,
        )
    except FileNotFoundError:
        return None

    if result.returncode != 0:
        return None

    output = (result.stdout or result.stderr or "").strip()
    if not output:
        return None
    return output.splitlines()[0]


def parse_size_literal_to_bytes(value: str, *, label: str) -> int:
    match = SIZE_LITERAL_RE.fullmatch(value)
    if not match:
        raise SetupError(
            f"{label} must be a size literal such as 4194304, 4mb, 2g, or 512k"
        )

    amount = int(match.group(1))
    suffix = match.group(2).lower()
    try:
        multiplier = SIZE_MULTIPLIERS[suffix]
    except KeyError as error:
        supported = ", ".join(sorted(key or "bytes" for key in SIZE_MULTIPLIERS))
        raise SetupError(
            f"unsupported size suffix for {label}: {suffix or 'bytes'} ({supported})"
        ) from error
    return amount * multiplier


def docker_image_exists(image: str) -> bool:
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        check=False,
        text=True,
        capture_output=True,
    )
    return result.returncode == 0


def append_log_header(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{utc_now()}] {message}\n")


def capture_container_logs(container_id: str, log_path: Path) -> None:
    append_log_header(log_path, f"docker logs snapshot for {container_id}")
    with log_path.open("a", encoding="utf-8") as handle:
        subprocess.run(
            ["docker", "logs", container_id],
            check=False,
            text=True,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )


@dataclass
class StartRequest:
    database: str
    mode: str
    host: str
    port: int
    cpus: int
    memory: str
    threads: Optional[int]
    environment_id: str
    project_root: Path
    benchmark_root: Path
    artifact_root: Path
    runtime_dir: Path
    log_path: Path
    build_vortex: bool
    runtime_config: dict[str, object] = field(default_factory=dict)


@dataclass
class NativeLaunch:
    command: list[str]
    cwd: Optional[Path]
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class ContainerLaunch:
    image: str
    command: list[str]
    docker_args: list[str]
    container_name: str
    metadata: dict[str, object] = field(default_factory=dict)


class DatabaseAdapter:
    name = ""
    native_supported = False
    container_supported = False

    def resolve_threads(self, request: StartRequest) -> int:
        return request.threads or max(1, request.cpus)

    def resolve_runtime_config(self, request: StartRequest) -> dict[str, object]:
        return dict(request.runtime_config)

    def validate_runtime_config(self, request: StartRequest) -> None:
        runtime_config = request.runtime_config
        if runtime_config.get("aof_enabled"):
            raise SetupError(f"{self.name} does not support AOF setup automation in {request.mode} mode")
        if "aof_fsync" in runtime_config and runtime_config.get("aof_fsync") != DEFAULT_AOF_FSYNC:
            raise SetupError(f"{self.name} does not support AOF fsync configuration in {request.mode} mode")
        if "eviction_policy" in runtime_config and runtime_config.get("eviction_policy") not in {
            None,
            DEFAULT_EVICTION_POLICY,
        }:
            raise SetupError(
                f"{self.name} does not support eviction policy '{runtime_config.get('eviction_policy')}' in {request.mode} mode"
            )

    def prepare_native(self, request: StartRequest) -> None:
        raise SetupError(f"native mode is not supported for {self.name}")

    def prepare_container(self, request: StartRequest) -> None:
        raise SetupError(f"container mode is not supported for {self.name}")

    def build_native_launch(self, request: StartRequest) -> NativeLaunch:
        raise SetupError(f"native mode is not supported for {self.name}")

    def build_container_launch(self, request: StartRequest) -> ContainerLaunch:
        raise SetupError(f"container mode is not supported for {self.name}")

    def native_version(self, request: StartRequest) -> Optional[str]:
        return None

    def container_version(self, request: StartRequest) -> Optional[str]:
        return None


def start_service(adapter: DatabaseAdapter, request: StartRequest) -> ServiceState:
    if request.mode == "native":
        return _start_native_service(adapter, request)
    return _start_container_service(adapter, request)


def _start_native_service(adapter: DatabaseAdapter, request: StartRequest) -> ServiceState:
    resolved_request = replace(request, runtime_config=adapter.resolve_runtime_config(request))
    resolved_request.runtime_dir.mkdir(parents=True, exist_ok=True)
    adapter.validate_runtime_config(resolved_request)
    adapter.prepare_native(resolved_request)
    launch = adapter.build_native_launch(resolved_request)
    append_log_header(request.log_path, f"launching native service: {format_command(launch.command)}")

    with request.log_path.open("a", encoding="utf-8") as handle:
        process = subprocess.Popen(
            launch.command,
            cwd=launch.cwd,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            text=True,
        )

    try:
        wait_until_ready(request.host, request.port)
    except TimeoutError as error:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        raise SetupError(
            f"{adapter.name} did not become ready on {request.host}:{request.port}; inspect {request.log_path}"
        ) from error

    now = utc_now()
    return ServiceState(
        database=adapter.name,
        mode=request.mode,
        host=request.host,
        port=request.port,
        ready=True,
        log_path=str(request.log_path),
        started_at=now,
        ready_at=now,
        pid=process.pid,
        process_group=process.pid,
        stop_signal="SIGTERM",
        resource_config={
            "cpus": request.cpus,
            "memory": request.memory,
            "threads": adapter.resolve_threads(resolved_request),
        },
        runtime_config=dict(resolved_request.runtime_config),
        metadata={
            **launch.metadata,
            "command": format_command(launch.command),
            "version": adapter.native_version(resolved_request),
        },
    )


def _start_container_service(adapter: DatabaseAdapter, request: StartRequest) -> ServiceState:
    require_command("docker")
    resolved_request = replace(request, runtime_config=adapter.resolve_runtime_config(request))
    resolved_request.runtime_dir.mkdir(parents=True, exist_ok=True)
    adapter.validate_runtime_config(resolved_request)
    adapter.prepare_container(resolved_request)
    launch = adapter.build_container_launch(resolved_request)
    command = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        launch.container_name,
        "-p",
        f"{request.port}:6379",
        *launch.docker_args,
        launch.image,
        *launch.command,
    ]
    append_log_header(request.log_path, f"launching container: {format_command(command)}")
    result = run_checked(command, cwd=request.project_root)
    container_id = result.stdout.strip()

    try:
        wait_until_ready(request.host, request.port)
    except TimeoutError as error:
        capture_container_logs(container_id, request.log_path)
        subprocess.run(["docker", "rm", "-f", container_id], check=False, capture_output=True)
        raise SetupError(
            f"{adapter.name} container did not become ready on {request.host}:{request.port}; inspect {request.log_path}"
        ) from error

    capture_container_logs(container_id, request.log_path)
    now = utc_now()
    return ServiceState(
        database=adapter.name,
        mode=request.mode,
        host=request.host,
        port=request.port,
        ready=True,
        log_path=str(request.log_path),
        started_at=now,
        ready_at=now,
        container_id=container_id,
        stop_signal="docker rm -f",
        resource_config={
            "cpus": request.cpus,
            "memory": request.memory,
            "threads": adapter.resolve_threads(resolved_request),
        },
        runtime_config=dict(resolved_request.runtime_config),
        metadata={
            **launch.metadata,
            "image": launch.image,
            "container_name": launch.container_name,
            "command": format_command(command),
            "version": adapter.container_version(resolved_request),
        },
    )


def stop_service(service: ServiceState) -> ServiceState:
    if service.status == "stopped":
        return service

    is_external = service.external or bool((service.metadata or {}).get("external"))
    if is_external:
        service.ready = False
        service.status = "detached"
        service.stopped_at = utc_now()
        if not service.stop_signal:
            service.stop_signal = "external-managed"
        return service

    if service.container_id:
        capture_container_logs(service.container_id, Path(service.log_path))
        subprocess.run(
            ["docker", "rm", "-f", service.container_id],
            check=False,
            text=True,
            capture_output=True,
        )
    elif service.process_group:
        try:
            os.killpg(service.process_group, signal.SIGTERM)
        except ProcessLookupError:
            pass
    elif service.pid:
        try:
            os.kill(service.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    service.ready = False
    service.status = "stopped"
    service.stopped_at = utc_now()
    return service
