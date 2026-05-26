from __future__ import annotations

import json
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Any


class CounterReplayCheckError(RuntimeError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_text_status(path: Path, payload: dict[str, Any]) -> None:
    lines = [f"{key}={value}" for key, value in payload.items() if not isinstance(value, (dict, list))]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _find_state_file(artifact_root: Path) -> Path:
    candidates = sorted((artifact_root / "environments").glob("*.json"))
    if not candidates:
        raise CounterReplayCheckError(f"no environment state file found under {artifact_root / 'environments'}")
    return candidates[-1]


def _vortex_service(state: dict[str, Any]) -> dict[str, Any]:
    for service in state.get("services", []):
        if service.get("database") == "vortex":
            return service
    raise CounterReplayCheckError("environment state has no vortex service")


def _counter_result_candidates(artifact_root: Path, workload: str) -> list[dict[str, Any]]:
    rows = []
    for path in sorted((artifact_root / "backend-runs").glob(f"**/vortex-{workload}-*t.json")):
        payload = _read_json(path)
        counter = payload.get("counter") or {}
        validation = counter.get("validation") or {}
        expected = validation.get("expected_final_value")
        if expected is None:
            continue
        rows.append(
            {
                "path": path,
                "threads": int(payload.get("num_threads") or 0),
                "expected": int(expected),
                "live_final": validation.get("final_value"),
                "validation_status": validation.get("status"),
                "workload": payload.get("workload"),
            }
        )
    return rows


def _aof_shard_glob(base: Path) -> str:
    stem = base.stem
    suffix = base.suffix
    return f"{stem}-shard*{suffix}"


def _replay_shard_path(source_base: Path, destination_base: Path, source_shard: Path) -> Path:
    source_prefix = f"{source_base.stem}-"
    source_name = source_shard.name
    if not source_name.startswith(source_prefix):
        return destination_base.with_name(source_name)
    suffix = source_name[len(source_prefix) :]
    return destination_base.with_name(f"{destination_base.stem}-{suffix}")


def _clear_replay_aof_files(destination_base: Path) -> None:
    for path in destination_base.parent.glob(_aof_shard_glob(destination_base)):
        if path.is_file():
            path.unlink()
    if destination_base.exists():
        destination_base.unlink()


def _copy_replay_aof_files(source_base: Path, destination_base: Path) -> list[Path]:
    _clear_replay_aof_files(destination_base)
    copied = [destination_base]
    shutil.copy2(source_base, destination_base)
    for source_shard in sorted(source_base.parent.glob(_aof_shard_glob(source_base))):
        destination_shard = _replay_shard_path(source_base, destination_base, source_shard)
        shutil.copy2(source_shard, destination_shard)
        copied.append(destination_shard)
    return copied


def _select_counter_result(artifact_root: Path, workload: str, threads: int | None) -> dict[str, Any]:
    candidates = _counter_result_candidates(artifact_root, workload)
    if not candidates:
        raise CounterReplayCheckError(f"no counter result JSON found for workload {workload}")
    if threads is not None:
        matches = [item for item in candidates if item["threads"] == threads]
        if not matches:
            raise CounterReplayCheckError(f"no counter result found for workload {workload} at {threads} threads")
        return matches[-1]
    return max(candidates, key=lambda item: item["threads"])


def _encode_command(parts: list[str]) -> bytes:
    chunks = [f"*{len(parts)}\r\n".encode("ascii")]
    for part in parts:
        encoded = part.encode("utf-8")
        chunks.append(f"${len(encoded)}\r\n".encode("ascii"))
        chunks.append(encoded)
        chunks.append(b"\r\n")
    return b"".join(chunks)


def _read_line(sock_file) -> str:
    line = sock_file.readline()
    if not line.endswith(b"\r\n"):
        raise CounterReplayCheckError("RESP line did not end with CRLF")
    return line[:-2].decode("utf-8")


def _read_resp(sock_file) -> Any:
    prefix = sock_file.read(1)
    if not prefix:
        raise CounterReplayCheckError("server closed connection before RESP response")
    if prefix == b"+":
        return _read_line(sock_file)
    if prefix == b":":
        return int(_read_line(sock_file))
    if prefix == b"-":
        raise CounterReplayCheckError(f"server returned error: {_read_line(sock_file)}")
    if prefix == b"$":
        length = int(_read_line(sock_file))
        if length < 0:
            return None
        payload = sock_file.read(length)
        crlf = sock_file.read(2)
        if crlf != b"\r\n":
            raise CounterReplayCheckError("bulk response was not terminated by CRLF")
        return payload.decode("utf-8")
    if prefix == b"*":
        length = int(_read_line(sock_file))
        if length < 0:
            return None
        return [_read_resp(sock_file) for _ in range(length)]
    raise CounterReplayCheckError(f"unsupported RESP prefix byte: {prefix!r}")


def _query_counter(host: str, port: int, key: str, timeout_seconds: float) -> int | None:
    with socket.create_connection((host, port), timeout=timeout_seconds) as sock:
        sock.settimeout(timeout_seconds)
        sock.sendall(_encode_command(["GET", key]))
        value = _read_resp(sock.makefile("rb"))
    if value is None:
        return None
    try:
        return int(value)
    except ValueError as error:
        raise CounterReplayCheckError(f"counter value was not an integer: {value!r}") from error


def _wait_for_counter(host: str, port: int, key: str, timeout_seconds: float) -> int | None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return _query_counter(host, port, key, timeout_seconds=1.0)
        except (OSError, CounterReplayCheckError) as error:
            last_error = error
            time.sleep(0.2)
    raise CounterReplayCheckError(f"replay server was not ready before timeout: {last_error}")


def _replay_command(
    *,
    binary: Path,
    copied_aof: Path,
    port: int,
    threads: int,
    maxmemory_bytes: int,
    eviction_policy: str,
    telemetry_mode: str,
    aof_fsync: str,
) -> list[str]:
    return [
        str(binary),
        "--bind",
        f"127.0.0.1:{port}",
        "--threads",
        str(threads),
        "--max-memory",
        str(maxmemory_bytes),
        "--eviction-policy",
        eviction_policy,
        "--io-backend",
        "polling",
        "--telemetry-mode",
        telemetry_mode,
        "--aof-enabled",
        "--aof-fsync",
        aof_fsync,
        "--aof-path",
        str(copied_aof),
        "--log-level",
        "warn",
    ]


def execute_counter_replay_check(args) -> Path:
    artifact_root = Path(args.artifact_root).expanduser().resolve()
    state = _read_json(_find_state_file(artifact_root))
    service = _vortex_service(state)
    runtime = service.get("runtime_config") or {}
    metadata = service.get("metadata") or {}
    aof_path = Path(str(runtime.get("aof_path") or "")).expanduser()
    if not aof_path.exists():
        raise CounterReplayCheckError(f"AOF file does not exist: {aof_path}")

    selected = _select_counter_result(artifact_root, args.workload, args.threads)
    binary = Path(str(args.vortex_binary or metadata.get("binary") or "")).expanduser()
    if not binary.exists():
        raise CounterReplayCheckError(f"vortex binary does not exist: {binary}")

    output_dir = artifact_root / "replay-check"
    copied_aof = output_dir / "vortex-replay.aof"
    log_path = output_dir / "vortex-replay.log"
    output_dir.mkdir(parents=True, exist_ok=True)
    copied_aof_paths = _copy_replay_aof_files(aof_path, copied_aof)

    command = _replay_command(
        binary=binary,
        copied_aof=copied_aof,
        port=args.port,
        threads=args.threads_for_replay,
        maxmemory_bytes=int(runtime.get("maxmemory_bytes") or 1_887_436_800),
        eviction_policy=str(runtime.get("eviction_policy") or "noeviction"),
        telemetry_mode=str(runtime.get("telemetry_mode") or "minimal"),
        aof_fsync=str(runtime.get("aof_fsync") or "everysec"),
    )

    with log_path.open("wb") as log_file:
        process = subprocess.Popen(command, stdout=log_file, stderr=subprocess.STDOUT)
    replayed: int | None = None
    status = "failed"
    error_message: str | None = None
    try:
        replayed = _wait_for_counter("127.0.0.1", args.port, args.key, args.timeout_seconds)
        status = "passed" if replayed == selected["expected"] else "failed"
    except Exception as error:
        error_message = str(error)
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    payload = {
        "schema_version": 1,
        "status": status,
        "artifact_root": str(artifact_root),
        "workload": selected["workload"],
        "threads": selected["threads"],
        "key": args.key,
        "expected_final_value": selected["expected"],
        "live_final_value": selected["live_final"],
        "replayed_value": replayed,
        "source_counter_json": str(selected["path"]),
        "source_aof_path": str(aof_path),
        "copied_aof_path": str(copied_aof),
        "copied_aof_paths": [str(path) for path in copied_aof_paths],
        "aof_fsync": runtime.get("aof_fsync"),
        "replay_command": command,
        "replay_log_path": str(log_path),
        "error": error_message,
    }
    json_path = output_dir / "counter-replay-check.json"
    status_path = output_dir / "counter-replay-check.txt"
    _write_json(json_path, payload)
    _write_text_status(status_path, payload)

    if status != "passed":
        raise CounterReplayCheckError(
            f"counter replay check failed: expected {selected['expected']}, replayed {replayed}; status written to {json_path}"
        )

    print(f"counter replay check passed: {json_path}")
    return json_path
