from __future__ import annotations

from pathlib import Path
from typing import Optional

from .base import (
    DEFAULT_AOF_FSYNC,
    DEFAULT_EVICTION_POLICY,
    ContainerLaunch,
    DatabaseAdapter,
    NativeLaunch,
    SetupError,
    StartRequest,
    best_effort_command_output,
    docker_image_exists,
    parse_size_literal_to_bytes,
    run_checked,
)


DEFAULT_VORTEX_MAXMEMORY = "1800mb"
DEFAULT_AOF_MAX_PENDING_FSYNC_BYTES = 64 * 1024 * 1024
CONTAINER_RUNTIME_DIR = "/benchmark-runtime"


def _append_reactor_budget_args(command: list[str], runtime: dict[str, object]) -> None:
    for key, flag in (
        ("reactor_completion_budget", "--reactor-completion-budget"),
        ("reactor_command_budget", "--reactor-command-budget"),
        ("reactor_accept_budget", "--reactor-accept-budget"),
        ("reactor_writev_budget", "--reactor-writev-budget"),
        ("reactor_maintenance_budget", "--reactor-maintenance-budget"),
        ("reactor_time_budget_us", "--reactor-time-budget-us"),
    ):
        if runtime.get(key) is not None:
            command.extend([flag, str(runtime[key])])


class VortexAdapter(DatabaseAdapter):
    name = "vortex"
    native_supported = True
    container_supported = True
    image = "vortexdb:latest"

    def _binary_path(self, request: StartRequest) -> Path:
        if request.runtime_config.get("telemetry_mode") == "profile":
            return request.project_root / "target" / "profiling" / "vortex-server"
        return request.project_root / "target" / "release" / "vortex-server"

    def prepare_native(self, request: StartRequest) -> None:
        binary = self._binary_path(request)
        profile_telemetry = request.runtime_config.get("telemetry_mode") == "profile"
        sqpoll_requested = int(request.runtime_config.get("sqpoll_idle_ms") or 0) > 0
        if not request.build_vortex and binary.exists():
            return
        if not request.build_vortex:
            raise SetupError(
                f"native Vortex binary not found at {binary}; rerun without --no-build-vortex or build it first"
            )
        if profile_telemetry:
            features = "profile-telemetry,sqpoll" if sqpoll_requested else "profile-telemetry"
            run_checked(
                [
                    "cargo",
                    "build",
                    "--profile",
                    "profiling",
                    "--features",
                    features,
                    "--bin",
                    "vortex-server",
                ],
                cwd=request.project_root,
                capture_output=False,
            )
            return
        command = ["cargo", "build", "--release"]
        if sqpoll_requested:
            command.extend(["--features", "sqpoll"])
        command.extend(["--bin", "vortex-server"])
        run_checked(command, cwd=request.project_root, capture_output=False)

    def prepare_container(self, request: StartRequest) -> None:
        if not request.build_vortex and docker_image_exists(self.image):
            return
        if not request.build_vortex and not docker_image_exists(self.image):
            raise SetupError(
                f"Docker image {self.image} is missing; rerun without --no-build-vortex or build it first"
            )
        run_checked(
            ["docker", "build", "-t", self.image, "."],
            cwd=request.project_root,
            capture_output=False,
        )

    def resolve_runtime_config(self, request: StartRequest) -> dict[str, object]:
        maxmemory = str(
            request.runtime_config.get("maxmemory", DEFAULT_VORTEX_MAXMEMORY)
        )
        resolved: dict[str, object] = {
            "aof_enabled": bool(request.runtime_config.get("aof_enabled", False)),
            "aof_fsync": str(request.runtime_config.get("aof_fsync", DEFAULT_AOF_FSYNC)),
            "aof_max_pending_fsync_bytes": int(
                request.runtime_config.get(
                    "aof_max_pending_fsync_bytes", DEFAULT_AOF_MAX_PENDING_FSYNC_BYTES
                )
            ),
            "eviction_policy": str(
                request.runtime_config.get("eviction_policy", DEFAULT_EVICTION_POLICY)
            ),
            "telemetry_mode": str(request.runtime_config.get("telemetry_mode", "minimal")),
            "maxmemory": maxmemory,
            "maxmemory_bytes": parse_size_literal_to_bytes(
                maxmemory, label="runtime_config.maxmemory"
            ),
            "aof_path": (
                str((request.runtime_dir / "vortex.aof").resolve())
                if request.mode == "native"
                else f"{CONTAINER_RUNTIME_DIR}/vortex.aof"
            ),
        }
        for key in (
            "io_backend",
            "telemetry_mode",
            "shard_count",
            "ring_size",
            "fixed_buffers",
            "fixed_buffer_registration",
            "max_request_bytes",
            "sqpoll_idle_ms",
            "reactor_completion_budget",
            "reactor_command_budget",
            "reactor_accept_budget",
            "reactor_writev_budget",
            "reactor_maintenance_budget",
            "reactor_time_budget_us",
        ):
            value = request.runtime_config.get(key)
            if value is not None:
                resolved[key] = value
        return resolved

    def validate_runtime_config(self, request: StartRequest) -> None:
        runtime = request.runtime_config
        if runtime.get("aof_fsync") and runtime.get("aof_fsync") not in {
            "always",
            "everysec",
            "no",
        }:
            raise SetupError(f"unsupported Vortex AOF fsync policy: {runtime.get('aof_fsync')}")
        if runtime.get("io_backend") not in {None, "auto", "uring", "polling"}:
            raise SetupError(f"unsupported Vortex io backend: {runtime.get('io_backend')}")
        if runtime.get("fixed_buffer_registration") not in {None, "auto", "on", "off"}:
            raise SetupError(
                "unsupported Vortex fixed-buffer registration policy: "
                f"{runtime.get('fixed_buffer_registration')}"
            )
        if runtime.get("telemetry_mode") not in {None, "minimal", "profile"}:
            raise SetupError(
                f"unsupported Vortex telemetry mode: {runtime.get('telemetry_mode')}"
            )
        shard_count = runtime.get("shard_count")
        if shard_count is not None and (
            not isinstance(shard_count, int)
            or shard_count <= 0
            or shard_count & (shard_count - 1) != 0
        ):
            raise SetupError("Vortex shard_count must be a positive power of two")
        if request.mode != "native" and runtime.get("telemetry_mode") == "profile":
            raise SetupError("Vortex profile telemetry requires a native profiling build")
        pending_limit = runtime.get("aof_max_pending_fsync_bytes")
        if pending_limit is not None and (not isinstance(pending_limit, int) or pending_limit <= 0):
            raise SetupError("Vortex aof_max_pending_fsync_bytes must be a positive integer")
        for key in (
            "reactor_completion_budget",
            "reactor_command_budget",
            "reactor_accept_budget",
            "reactor_writev_budget",
            "reactor_maintenance_budget",
            "max_request_bytes",
        ):
            value = runtime.get(key)
            if value is not None and (not isinstance(value, int) or value <= 0):
                raise SetupError(f"Vortex {key} must be a positive integer")
        time_budget = runtime.get("reactor_time_budget_us")
        if time_budget is not None and (not isinstance(time_budget, int) or time_budget < 0):
            raise SetupError("Vortex reactor_time_budget_us must be a non-negative integer")

    def build_native_launch(self, request: StartRequest) -> NativeLaunch:
        binary = self._binary_path(request)
        command = [
            str(binary),
            "--bind",
            f"{request.host}:{request.port}",
            "--threads",
            str(self.resolve_threads(request)),
        ]
        runtime = request.runtime_config
        if runtime.get("maxmemory_bytes") is not None:
            command.extend(["--max-memory", str(runtime["maxmemory_bytes"])])
        if runtime.get("shard_count") is not None:
            command.extend(["--shard-count", str(runtime["shard_count"])])
        if runtime.get("eviction_policy"):
            command.extend(["--eviction-policy", str(runtime["eviction_policy"])])
        if runtime.get("io_backend"):
            command.extend(["--io-backend", str(runtime["io_backend"])])
        if runtime.get("telemetry_mode"):
            command.extend(["--telemetry-mode", str(runtime["telemetry_mode"])])
        if runtime.get("ring_size") is not None:
            command.extend(["--ring-size", str(runtime["ring_size"])])
        if runtime.get("fixed_buffers") is not None:
            command.extend(["--fixed-buffers", str(runtime["fixed_buffers"])])
        if runtime.get("fixed_buffer_registration"):
            command.extend(
                [
                    "--fixed-buffer-registration",
                    str(runtime["fixed_buffer_registration"]),
                ]
            )
        if runtime.get("max_request_bytes") is not None:
            command.extend(["--max-request-bytes", str(runtime["max_request_bytes"])])
        if runtime.get("sqpoll_idle_ms") is not None:
            command.extend(["--sqpoll-idle-ms", str(runtime["sqpoll_idle_ms"])])
        _append_reactor_budget_args(command, runtime)
        if runtime.get("aof_enabled"):
            command.extend(
                [
                    "--aof-enabled",
                    "--aof-fsync",
                    str(runtime.get("aof_fsync", DEFAULT_AOF_FSYNC)),
                    "--aof-path",
                    str(runtime["aof_path"]),
                ]
            )
            if runtime.get("aof_max_pending_fsync_bytes") is not None:
                command.extend(
                    [
                        "--aof-max-pending-fsync-bytes",
                        str(runtime["aof_max_pending_fsync_bytes"]),
                    ]
                )
        return NativeLaunch(
            command=command,
            cwd=request.project_root,
            metadata={"bind": f"{request.host}:{request.port}", "binary": str(binary)},
        )

    def build_container_launch(self, request: StartRequest) -> ContainerLaunch:
        runtime = request.runtime_config
        command = [
            "--bind",
            "0.0.0.0:6379",
            "--threads",
            str(self.resolve_threads(request)),
        ]
        if runtime.get("maxmemory_bytes") is not None:
            command.extend(["--max-memory", str(runtime["maxmemory_bytes"])])
        if runtime.get("shard_count") is not None:
            command.extend(["--shard-count", str(runtime["shard_count"])])
        if runtime.get("eviction_policy"):
            command.extend(["--eviction-policy", str(runtime["eviction_policy"])])
        if runtime.get("io_backend"):
            command.extend(["--io-backend", str(runtime["io_backend"])])
        if runtime.get("telemetry_mode"):
            command.extend(["--telemetry-mode", str(runtime["telemetry_mode"])])
        if runtime.get("ring_size") is not None:
            command.extend(["--ring-size", str(runtime["ring_size"])])
        if runtime.get("fixed_buffers") is not None:
            command.extend(["--fixed-buffers", str(runtime["fixed_buffers"])])
        if runtime.get("fixed_buffer_registration"):
            command.extend(
                [
                    "--fixed-buffer-registration",
                    str(runtime["fixed_buffer_registration"]),
                ]
            )
        if runtime.get("max_request_bytes") is not None:
            command.extend(["--max-request-bytes", str(runtime["max_request_bytes"])])
        if runtime.get("sqpoll_idle_ms") is not None:
            command.extend(["--sqpoll-idle-ms", str(runtime["sqpoll_idle_ms"])])
        _append_reactor_budget_args(command, runtime)
        if runtime.get("aof_enabled"):
            command.extend(
                [
                    "--aof-enabled",
                    "--aof-fsync",
                    str(runtime.get("aof_fsync", DEFAULT_AOF_FSYNC)),
                    "--aof-path",
                    str(runtime["aof_path"]),
                ]
            )
            if runtime.get("aof_max_pending_fsync_bytes") is not None:
                command.extend(
                    [
                        "--aof-max-pending-fsync-bytes",
                        str(runtime["aof_max_pending_fsync_bytes"]),
                    ]
                )
        return ContainerLaunch(
            image=self.image,
            command=command,
            docker_args=[
                f"--memory={request.memory}",
                f"--cpus={request.cpus}",
                "-v",
                f"{request.runtime_dir}:{CONTAINER_RUNTIME_DIR}",
                "--security-opt",
                "seccomp=unconfined",
                "--ulimit",
                "memlock=-1",
            ],
            container_name=f"{request.environment_id}-{self.name}",
            metadata={"bind": "0.0.0.0:6379"},
        )

    def native_version(self, request: StartRequest) -> Optional[str]:
        return best_effort_command_output([str(self._binary_path(request)), "--version"])

    def container_version(self, request: StartRequest) -> Optional[str]:
        return self.image
