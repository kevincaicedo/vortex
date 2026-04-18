from __future__ import annotations

from pathlib import Path
from typing import Optional

from .base import (
    DEFAULT_AOF_FSYNC,
    DEFAULT_EVICTION_POLICY,
    ContainerLaunch,
    DatabaseAdapter,
    NativeLaunch,
    StartRequest,
    best_effort_command_output,
    require_command,
)


DEFAULT_REDIS_MAXMEMORY = "1800mb"
CONTAINER_RUNTIME_DIR = "/data"


class RedisAdapter(DatabaseAdapter):
    name = "redis"
    native_supported = True
    container_supported = True
    image = "redis:8.6.2-alpine3.23"

    def resolve_threads(self, request: StartRequest) -> int:
        return request.threads or max(1, min(request.cpus, 4))

    def prepare_native(self, request: StartRequest) -> None:
        require_command("redis-server")

    def validate_runtime_config(self, request: StartRequest) -> None:
        return None

    def resolve_runtime_config(self, request: StartRequest) -> dict[str, object]:
        aof_enabled = bool(request.runtime_config.get("aof_enabled", False))
        aof_fsync = str(request.runtime_config.get("aof_fsync", DEFAULT_AOF_FSYNC))
        maxmemory = str(request.runtime_config.get("maxmemory", DEFAULT_REDIS_MAXMEMORY))
        eviction_policy = str(
            request.runtime_config.get("eviction_policy", DEFAULT_EVICTION_POLICY)
        )
        aof_path = (
            str((request.runtime_dir / "appendonly.aof").resolve())
            if request.mode == "native"
            else f"{CONTAINER_RUNTIME_DIR}/appendonly.aof"
        )
        return {
            "aof_enabled": aof_enabled,
            "aof_fsync": aof_fsync,
            "maxmemory": maxmemory,
            "eviction_policy": eviction_policy,
            "aof_path": aof_path,
        }

    def build_native_launch(self, request: StartRequest) -> NativeLaunch:
        runtime = request.runtime_config
        command = [
            "redis-server",
            "--port",
            str(request.port),
            "--save",
            "",
            "--appendonly",
            "yes" if runtime.get("aof_enabled") else "no",
            "--appendfsync",
            str(runtime.get("aof_fsync", DEFAULT_AOF_FSYNC)),
            "--dir",
            str(request.runtime_dir),
            "--appendfilename",
            Path(str(runtime.get("aof_path", request.runtime_dir / "appendonly.aof"))).name,
            "--io-threads",
            str(self.resolve_threads(request)),
            "--io-threads-do-reads",
            "yes",
            "--hz",
            "100",
            "--loglevel",
            "warning",
            "--protected-mode",
            "no",
            "--maxmemory",
            str(runtime.get("maxmemory", DEFAULT_REDIS_MAXMEMORY)),
            "--maxmemory-policy",
            str(runtime.get("eviction_policy", DEFAULT_EVICTION_POLICY)),
            "--daemonize",
            "no",
        ]
        return NativeLaunch(
            command=command,
            cwd=request.project_root,
            metadata={"bind": f"{request.host}:{request.port}", "binary": "redis-server"},
        )

    def build_container_launch(self, request: StartRequest) -> ContainerLaunch:
        runtime = request.runtime_config
        command = [
            "redis-server",
            "--save",
            "",
            "--appendonly",
            "yes" if runtime.get("aof_enabled") else "no",
            "--appendfsync",
            str(runtime.get("aof_fsync", DEFAULT_AOF_FSYNC)),
            "--dir",
            CONTAINER_RUNTIME_DIR,
            "--appendfilename",
            Path(str(runtime.get("aof_path", f"{CONTAINER_RUNTIME_DIR}/appendonly.aof"))).name,
            "--io-threads",
            str(self.resolve_threads(request)),
            "--io-threads-do-reads",
            "yes",
            "--hz",
            "100",
            "--loglevel",
            "warning",
            "--protected-mode",
            "no",
            "--maxmemory",
            str(runtime.get("maxmemory", DEFAULT_REDIS_MAXMEMORY)),
            "--maxmemory-policy",
            str(runtime.get("eviction_policy", DEFAULT_EVICTION_POLICY)),
        ]
        return ContainerLaunch(
            image=self.image,
            command=command,
            docker_args=[
                f"--memory={request.memory}",
                f"--cpus={request.cpus}",
                "-v",
                f"{request.runtime_dir}:{CONTAINER_RUNTIME_DIR}",
            ],
            container_name=f"{request.environment_id}-{self.name}",
            metadata={"bind": "0.0.0.0:6379"},
        )

    def native_version(self, request: StartRequest) -> Optional[str]:
        return best_effort_command_output(["redis-server", "--version"])

    def container_version(self, request: StartRequest) -> Optional[str]:
        return self.image
