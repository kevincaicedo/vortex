from __future__ import annotations

from pathlib import Path
from typing import Optional

from .base import (
    DEFAULT_AOF_FSYNC,
    DEFAULT_EVICTION_POLICY,
    ContainerLaunch,
    DatabaseAdapter,
    StartRequest,
)


DEFAULT_VALKEY_MAXMEMORY = "1800mb"
CONTAINER_RUNTIME_DIR = "/data"


class ValkeyAdapter(DatabaseAdapter):
    name = "valkey"
    native_supported = False
    container_supported = True
    image = "valkey/valkey:9.1-alpine3.23"

    def resolve_threads(self, request: StartRequest) -> int:
        return request.threads or max(1, min(request.cpus, 4))

    def validate_runtime_config(self, request: StartRequest) -> None:
        return None

    def resolve_runtime_config(self, request: StartRequest) -> dict[str, object]:
        aof_enabled = bool(request.runtime_config.get("aof_enabled", False))
        aof_fsync = str(request.runtime_config.get("aof_fsync", DEFAULT_AOF_FSYNC))
        maxmemory = str(request.runtime_config.get("maxmemory", DEFAULT_VALKEY_MAXMEMORY))
        eviction_policy = str(
            request.runtime_config.get("eviction_policy", DEFAULT_EVICTION_POLICY)
        )
        return {
            "aof_enabled": aof_enabled,
            "aof_fsync": aof_fsync,
            "maxmemory": maxmemory,
            "eviction_policy": eviction_policy,
            "aof_path": f"{CONTAINER_RUNTIME_DIR}/appendonly.aof",
        }

    def build_container_launch(self, request: StartRequest) -> ContainerLaunch:
        runtime = request.runtime_config
        command = [
            "valkey-server",
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
            str(runtime.get("maxmemory", DEFAULT_VALKEY_MAXMEMORY)),
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

    def container_version(self, request: StartRequest) -> Optional[str]:
        return self.image
