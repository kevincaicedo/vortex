from __future__ import annotations

from vortex_benchmark.catalog import get_workload_definition

from .base import BackendExecutionRecord, BackendRunContext
from .custom_rust import run_custom_rust_backend
from .memtier import run_memtier_backend
from .redis_benchmark import run_redis_benchmark_backend


BACKEND_RUNNERS = {
    "redis-benchmark": run_redis_benchmark_backend,
    "memtier_benchmark": run_memtier_backend,
    "custom-rust": run_custom_rust_backend,
}


def resolve_backend_names(spec) -> list[str]:
    if spec.backends:
        return list(spec.backends)

    resolved: list[str] = []
    if spec.resolved_commands:
        resolved.append("redis-benchmark")

    if spec.workloads:
        workload_definitions = [get_workload_definition(name) for name in spec.workloads]
        if any(definition.multi_key or definition.transactional for definition in workload_definitions):
            resolved.append("custom-rust")
        else:
            resolved.append("memtier_benchmark")

    if not resolved:
        resolved.append("redis-benchmark")

    ordered: list[str] = []
    seen: set[str] = set()
    for backend in resolved:
        if backend in seen:
            continue
        seen.add(backend)
        ordered.append(backend)
    return ordered


def execute_backend(name: str, context: BackendRunContext) -> BackendExecutionRecord:
    try:
        runner = BACKEND_RUNNERS[name]
    except KeyError as error:
        raise ValueError(f"unknown backend runner: {name}") from error
    return runner(context)


__all__ = ["BackendExecutionRecord", "BackendRunContext", "execute_backend", "resolve_backend_names"]