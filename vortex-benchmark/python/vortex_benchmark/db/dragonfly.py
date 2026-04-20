from __future__ import annotations

from typing import Optional

from .base import ContainerLaunch, DatabaseAdapter, SetupError, StartRequest


DEFAULT_DRAGONFLY_MAXMEMORY = "1800mb"


class DragonflyAdapter(DatabaseAdapter):
    name = "dragonfly"
    native_supported = False
    container_supported = True
    image = "docker.dragonflydb.io/dragonflydb/dragonfly:latest"

    def prepare_container(self, request: StartRequest) -> None:
        return None

    def resolve_runtime_config(self, request: StartRequest) -> dict[str, object]:
        runtime_config = {
            "maxmemory": str(request.runtime_config.get("maxmemory", DEFAULT_DRAGONFLY_MAXMEMORY))
        }
        if request.runtime_config.get("eviction_policy") is not None:
            runtime_config["eviction_policy"] = str(request.runtime_config["eviction_policy"])
        return runtime_config

    def validate_runtime_config(self, request: StartRequest) -> None:
        runtime = request.runtime_config
        if runtime.get("aof_enabled"):
            raise SetupError("dragonfly setup automation does not support AOF-enabled scenarios")
        if "aof_fsync" in runtime:
            raise SetupError("dragonfly setup automation does not support AOF fsync configuration")
        if runtime.get("eviction_policy") not in {None, "noeviction"}:
            raise SetupError(
                "dragonfly setup automation currently only supports the default noeviction policy"
            )

    def build_container_launch(self, request: StartRequest) -> ContainerLaunch:
        runtime = request.runtime_config
        command = [
            "dragonfly",
            "--logtostdout",
            "--proactor_threads",
            str(self.resolve_threads(request)),
            "--hz",
            "100",
            "--dbfilename",
            "",
            "--pipeline_squash",
            "10",
            "--version_check",
            "false",
            "--maxmemory",
            str(runtime.get("maxmemory", DEFAULT_DRAGONFLY_MAXMEMORY)),
        ]
        return ContainerLaunch(
            image=self.image,
            command=command,
            docker_args=[
                f"--memory={request.memory}",
                f"--cpus={request.cpus}",
                "--ulimit",
                "memlock=-1",
            ],
            container_name=f"{request.environment_id}-{self.name}",
            metadata={"bind": "0.0.0.0:6379"},
        )

    def container_version(self, request: StartRequest) -> Optional[str]:
        return self.image
