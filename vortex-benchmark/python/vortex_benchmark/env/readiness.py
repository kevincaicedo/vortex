from __future__ import annotations

import socket
import time


def probe_redis_endpoint(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout) as connection:
            connection.settimeout(timeout)
            connection.sendall(b"*1\r\n$4\r\nPING\r\n")
            response = connection.recv(32)
    except OSError:
        return False

    return response.startswith(b"+PONG")


def wait_until_ready(
    host: str,
    port: int,
    attempts: int = 30,
    delay_seconds: float = 1.0,
    timeout: float = 1.0,
) -> None:
    for _ in range(attempts):
        if probe_redis_endpoint(host, port, timeout=timeout):
            return
        time.sleep(delay_seconds)
    raise TimeoutError(f"service on {host}:{port} did not become ready after {attempts} attempts")
