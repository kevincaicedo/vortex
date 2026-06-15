from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, TextIO


class ProgressReporter:
    """Streams stable progress events for interactive and CI runs."""

    def __init__(
        self,
        *,
        json_mode: bool = False,
        no_color: bool = False,
        total_steps: int | None = None,
        stream: TextIO | None = None,
    ) -> None:
        self.json_mode = json_mode
        self.stream = stream or sys.stderr
        self.started = time.monotonic()
        self.total_steps = total_steps if total_steps and total_steps > 0 else None
        self.completed_steps = 0
        self._spinner_index = 0
        self._live_line = False
        self.interactive = self.stream.isatty() and not json_mode
        self.use_color = (
            not no_color
            and not json_mode
            and self.interactive
            and os.environ.get("CI", "").lower() not in {"1", "true", "yes"}
        )

    def set_total_steps(self, total_steps: int | None) -> None:
        if total_steps and total_steps > 0:
            self.total_steps = total_steps

    def _is_terminal_status(self, status: str) -> bool:
        return status in {"ok", "warn", "fail", "skip", "dry-run", "completed"}

    def _progress_fields(self, elapsed: float) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "step_index": self.completed_steps,
            "total_steps": self.total_steps,
        }
        if not self.total_steps:
            return fields
        if self.completed_steps <= 0:
            fields["eta_seconds"] = None
            fields["progress_percent"] = 0.0
            return fields
        rate = elapsed / self.completed_steps
        remaining = max(self.total_steps - self.completed_steps, 0)
        fields["eta_seconds"] = round(rate * remaining, 3)
        fields["progress_percent"] = round(
            min(self.completed_steps / self.total_steps, 1.0) * 100.0,
            1,
        )
        return fields

    def _progress_suffix(self, elapsed: float) -> str:
        if not self.total_steps:
            return ""
        percent = min(self.completed_steps / self.total_steps, 1.0)
        filled = int(percent * 16)
        bar = "#" * filled + "-" * (16 - filled)
        if self.completed_steps <= 0:
            eta = "eta n/a"
        else:
            rate = elapsed / self.completed_steps
            remaining = max(self.total_steps - self.completed_steps, 0)
            eta = f"eta {rate * remaining:0.0f}s"
        return f" [{bar}] {self.completed_steps}/{self.total_steps} {eta}"

    def _colorize(self, status: str, label: str) -> str:
        if not self.use_color:
            return label
        color = {
            "start": "\033[36m",
            "ok": "\033[32m",
            "warn": "\033[33m",
            "fail": "\033[31m",
            "skip": "\033[2m",
        }.get(status, "")
        reset = "\033[0m" if color else ""
        return f"{color}{label}{reset}"

    def _write_status(self, label: str, *, terminal: bool) -> None:
        if self.interactive:
            rendered = f"\r\033[K{label}"
            if terminal:
                print(rendered, file=self.stream, flush=True)
                self._live_line = False
            else:
                self.stream.write(rendered)
                self.stream.flush()
                self._live_line = True
            return

        if self._live_line:
            print("", file=self.stream, flush=True)
            self._live_line = False
        print(label, file=self.stream, flush=True)

    def phase(self, phase: str, status: str, detail: str | None = None, **fields: Any) -> None:
        elapsed = time.monotonic() - self.started
        if self._is_terminal_status(status) and (
            self.total_steps is None or self.completed_steps < self.total_steps
        ):
            self.completed_steps += 1
        progress_fields = self._progress_fields(elapsed)
        if self.json_mode:
            payload = {
                "type": "progress",
                "phase": phase,
                "status": status,
                "elapsed_seconds": round(elapsed, 3),
                **{key: value for key, value in progress_fields.items() if value is not None},
                **{key: value for key, value in fields.items() if value is not None},
            }
            if detail is not None:
                payload["detail"] = detail
            print(json.dumps(payload, sort_keys=True), file=self.stream, flush=True)
            return

        terminal = self._is_terminal_status(status)
        spinner = ""
        if self.interactive:
            frames = ("|", "/", "-", "\\")
            spinner = f"{frames[self._spinner_index % len(frames)]} "
            self._spinner_index += 1

        if self.interactive:
            label = f"{spinner}[{elapsed:7.2f}s] {phase:<15} {status:<7}"
            if detail:
                label = f"{label} {detail}"
            label = f"{label}{self._progress_suffix(elapsed)}"
        else:
            label = f"{elapsed:7.2f}s  {phase:<15} {status:<7}"
            if detail:
                label = f"{label}  {detail}"
            suffix = self._progress_suffix(elapsed)
            if suffix:
                label = f"{label}  {suffix.strip()}"

        self._write_status(self._colorize(status, label), terminal=terminal)
