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


CONTAINER_RUNTIME_DIR = "/benchmark-runtime"


class VortexAdapter(DatabaseAdapter):
    name = "vortex"
    native_supported = True
    container_supported = True
    image = "vortexdb:latest"

    def _binary_path(self, request: StartRequest) -> Path:
        return request.project_root / "target" / "release" / "vortex-server"

    def prepare_native(self, request: StartRequest) -> None:
        binary = self._binary_path(request)
        if binary.exists():
            return
        if not request.build_vortex:
            raise SetupError(
                f"native Vortex binary not found at {binary}; rerun without --no-build-vortex or build it first"
            )
        run_checked(
            ["cargo", "build", "--release", "--bin", "vortex-server"],
            cwd=request.project_root,
            capture_output=False,
        )

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
        maxmemory = request.runtime_config.get("maxmemory")
        resolved: dict[str, object] = {
            "aof_enabled": bool(request.runtime_config.get("aof_enabled", False)),
            "aof_fsync": str(request.runtime_config.get("aof_fsync", DEFAULT_AOF_FSYNC)),
            "eviction_policy": str(
                request.runtime_config.get("eviction_policy", DEFAULT_EVICTION_POLICY)
            ),
            "aof_path": (
                str((request.runtime_dir / "vortex.aof").resolve())
                if request.mode == "native"
                else f"{CONTAINER_RUNTIME_DIR}/vortex.aof"
            ),
        }
        if maxmemory is not None:
            resolved["maxmemory"] = str(maxmemory)
            resolved["maxmemory_bytes"] = parse_size_literal_to_bytes(
                str(maxmemory), label="runtime_config.maxmemory"
            )
        return resolved

    def validate_runtime_config(self, request: StartRequest) -> None:
        runtime = request.runtime_config
        if runtime.get("aof_fsync") and runtime.get("aof_fsync") not in {
            "always",
            "everysec",
            "no",
        }:
            raise SetupError(f"unsupported Vortex AOF fsync policy: {runtime.get('aof_fsync')}")

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
        if runtime.get("eviction_policy"):
            command.extend(["--eviction-policy", str(runtime["eviction_policy"])])
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
        if runtime.get("eviction_policy"):
            command.extend(["--eviction-policy", str(runtime["eviction_policy"])])
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
