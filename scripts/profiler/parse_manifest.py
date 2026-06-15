#!/usr/bin/env python3
"""
Minimal YAML manifest parser for the Vortex profiler.

Reads a profiling manifest YAML and outputs shell-friendly KEY=VALUE pairs
that profiler.sh can eval. Stateless — reads stdin or a file, writes to stdout.

Usage:
    python3 parse_manifest.py /path/to/manifest.yaml
"""
from __future__ import annotations

import sys
from pathlib import Path


def parse_simple_yaml(text: str) -> dict:
    """
    Parse a flat/shallow YAML file without requiring PyYAML.
    Handles top-level scalars and one level of nesting (key.subkey).
    Lists are joined with commas.
    """
    result: dict[str, str] = {}
    current_section = ""

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        # Skip empty lines and comments
        if not line or line.lstrip().startswith("#"):
            continue

        # Detect indentation
        stripped = line.lstrip()
        indent = len(line) - len(stripped)

        if indent == 0 and ":" in stripped:
            # Top-level key
            key, _, value = stripped.partition(":")
            key = key.strip()
            value = value.strip()

            if value:
                result[key] = value
            else:
                current_section = key
        elif indent > 0 and current_section:
            if stripped.startswith("- "):
                # List item
                item = stripped[2:].strip()
                existing = result.get(current_section, "")
                result[current_section] = f"{existing},{item}" if existing else item
            elif ":" in stripped:
                # Nested key
                subkey, _, subvalue = stripped.partition(":")
                subkey = subkey.strip()
                subvalue = subvalue.strip()
                if subvalue and subvalue != "null":
                    result[f"{current_section}.{subkey}"] = subvalue

    return result


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: parse_manifest.py <manifest.yaml>", file=sys.stderr)
        sys.exit(1)

    manifest_path = Path(sys.argv[1]).expanduser().resolve()
    if not manifest_path.exists():
        print(f"error: manifest not found: {manifest_path}", file=sys.stderr)
        sys.exit(1)

    text = manifest_path.read_text(encoding="utf-8")
    data = parse_simple_yaml(text)

    # Output shell-safe KEY=VALUE pairs
    for key, value in sorted(data.items()):
        # Sanitize key for shell: replace dots and dashes with underscores
        shell_key = "MANIFEST_" + key.upper().replace(".", "_").replace("-", "_")
        # Quote the value
        safe_value = value.replace("'", "'\\''")
        print(f"{shell_key}='{safe_value}'")


if __name__ == "__main__":
    main()
