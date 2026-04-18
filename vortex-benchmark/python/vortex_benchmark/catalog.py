from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Iterable


@dataclass(frozen=True)
class BackendDefinition:
    name: str
    description: str


@dataclass(frozen=True)
class CommandGroupDefinition:
    name: str
    description: str
    commands: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class WorkloadDefinition:
    name: str
    description: str
    distribution: str
    key_pattern: str
    read_ratio: int
    write_ratio: int
    multi_key: bool
    transactional: bool
    hot_key: bool
    default_command_groups: tuple[str, ...]
    default_commands: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _normalize_lookup_key(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"\s+", " ", value)
    return value


def _candidate_aliases(value: str) -> set[str]:
    normalized = _normalize_lookup_key(value)
    compact = normalized.replace(" ", "")
    hyphen = normalized.replace(" ", "-").replace("_", "-")
    underscore = normalized.replace(" ", "_").replace("-", "_")
    spaced = normalized.replace("-", " ").replace("_", " ")
    return {normalized, compact, hyphen, underscore, spaced}


def _build_alias_map(items: Iterable[tuple[str, Iterable[str]]]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for canonical, values in items:
        for value in values:
            for alias in _candidate_aliases(value):
                aliases[alias] = canonical
    return aliases


BACKEND_DEFINITIONS = (
    BackendDefinition(
        name="redis-benchmark",
        description="Use the official redis-benchmark client for point-command workloads.",
    ),
    BackendDefinition(
        name="memtier_benchmark",
        description="Use memtier_benchmark for mixed workloads, scaling sweeps, and richer latency analysis.",
    ),
    BackendDefinition(
        name="custom-rust",
        description="Use the future custom Rust RESP backend for deterministic Vortex-specific workloads.",
    ),
)

BACKEND_ALIASES = _build_alias_map(
    (
        definition.name,
        (
            definition.name,
            definition.name.replace("_", "-"),
            definition.name.replace("-", "_"),
            "custom rust" if definition.name == "custom-rust" else definition.name,
            "custom_resp" if definition.name == "custom-rust" else definition.name,
            "custom-resp" if definition.name == "custom-rust" else definition.name,
        ),
    )
    for definition in BACKEND_DEFINITIONS
)

COMMAND_GROUP_DEFINITIONS = (
    CommandGroupDefinition(
        name="strings",
        description="Single-key and multi-key string operations.",
        commands=(
            "APPEND",
            "DECR",
            "DECRBY",
            "GET",
            "GETDEL",
            "GETEX",
            "GETRANGE",
            "GETSET",
            "INCR",
            "INCRBY",
            "INCRBYFLOAT",
            "MGET",
            "MSET",
            "MSETNX",
            "PSETEX",
            "SET",
            "SETEX",
            "SETNX",
            "SETRANGE",
            "STRLEN",
        ),
    ),
    CommandGroupDefinition(
        name="keys",
        description="Key management, expiry, and keyspace traversal commands.",
        commands=(
            "COPY",
            "DEL",
            "EXISTS",
            "EXPIRE",
            "EXPIREAT",
            "EXPIRETIME",
            "KEYS",
            "PERSIST",
            "PEXPIRE",
            "PEXPIREAT",
            "PEXPIRETIME",
            "PTTL",
            "RANDOMKEY",
            "RENAME",
            "RENAMENX",
            "SCAN",
            "TOUCH",
            "TTL",
            "TYPE",
            "UNLINK",
        ),
    ),
    CommandGroupDefinition(
        name="server",
        description="Connection, admin, and server-level commands.",
        commands=(
            "COMMAND",
            "DBSIZE",
            "ECHO",
            "FLUSHALL",
            "FLUSHDB",
            "INFO",
            "PING",
            "QUIT",
            "SELECT",
            "TIME",
        ),
    ),
    CommandGroupDefinition(
        name="transactions",
        description="Transaction queueing and optimistic-locking commands.",
        commands=(
            "DISCARD",
            "EXEC",
            "MULTI",
            "UNWATCH",
            "WATCH",
        ),
    ),
)

COMMAND_GROUP_ALIASES = _build_alias_map(
    (
        definition.name,
        (
            definition.name,
            definition.name[:-1] if definition.name.endswith("s") else definition.name,
        ),
    )
    for definition in COMMAND_GROUP_DEFINITIONS
)

KNOWN_COMMANDS = frozenset(
    command
    for definition in COMMAND_GROUP_DEFINITIONS
    for command in definition.commands
)

WORKLOAD_DEFINITIONS = (
    WorkloadDefinition(
        name="uniform-read_only",
        description="Uniformly distributed pure reads against the current keyspace.",
        distribution="uniform",
        key_pattern="single-key",
        read_ratio=100,
        write_ratio=0,
        multi_key=False,
        transactional=False,
        hot_key=False,
        default_command_groups=("strings", "keys"),
        default_commands=("GET", "MGET", "EXISTS", "TTL"),
    ),
    WorkloadDefinition(
        name="uniform-read_heavy",
        description="Uniform access with read-heavy traffic for general cache behavior.",
        distribution="uniform",
        key_pattern="single-key",
        read_ratio=80,
        write_ratio=20,
        multi_key=False,
        transactional=False,
        hot_key=False,
        default_command_groups=("strings", "keys"),
        default_commands=("GET", "SET", "EXISTS", "TTL", "INCR"),
    ),
    WorkloadDefinition(
        name="uniform-mixed",
        description="Uniformly distributed balanced read-write traffic.",
        distribution="uniform",
        key_pattern="single-key",
        read_ratio=50,
        write_ratio=50,
        multi_key=False,
        transactional=False,
        hot_key=False,
        default_command_groups=("strings", "keys"),
        default_commands=("GET", "SET", "INCR", "DEL", "EXPIRE"),
    ),
    WorkloadDefinition(
        name="uniform-write_heavy",
        description="Uniform access with write-heavy traffic to stress mutation paths.",
        distribution="uniform",
        key_pattern="single-key",
        read_ratio=20,
        write_ratio=80,
        multi_key=False,
        transactional=False,
        hot_key=False,
        default_command_groups=("strings", "keys"),
        default_commands=("SET", "INCR", "DECR", "DEL", "EXPIRE"),
    ),
    WorkloadDefinition(
        name="uniform-write_only",
        description="Uniformly distributed pure writes and key mutations.",
        distribution="uniform",
        key_pattern="single-key",
        read_ratio=0,
        write_ratio=100,
        multi_key=False,
        transactional=False,
        hot_key=False,
        default_command_groups=("strings", "keys"),
        default_commands=("SET", "SETEX", "SETNX", "DEL", "EXPIRE"),
    ),
    WorkloadDefinition(
        name="zipfian-read_heavy",
        description="Skewed hot-key access with read-heavy traffic.",
        distribution="zipfian",
        key_pattern="hot-key",
        read_ratio=90,
        write_ratio=10,
        multi_key=False,
        transactional=False,
        hot_key=True,
        default_command_groups=("strings", "keys"),
        default_commands=("GET", "MGET", "EXISTS", "TTL"),
    ),
    WorkloadDefinition(
        name="zipfian-mixed",
        description="Skewed hot-key traffic with both reads and writes.",
        distribution="zipfian",
        key_pattern="hot-key",
        read_ratio=70,
        write_ratio=30,
        multi_key=False,
        transactional=False,
        hot_key=True,
        default_command_groups=("strings", "keys"),
        default_commands=("GET", "SET", "INCR", "EXPIRE"),
    ),
    WorkloadDefinition(
        name="transaction",
        description="General transaction-oriented flow using MULTI/EXEC semantics.",
        distribution="uniform",
        key_pattern="single-key",
        read_ratio=50,
        write_ratio=50,
        multi_key=False,
        transactional=True,
        hot_key=False,
        default_command_groups=("transactions",),
        default_commands=("MULTI", "EXEC", "WATCH", "UNWATCH"),
    ),
    WorkloadDefinition(
        name="multi-key operations",
        description="Multi-key request mixes that emphasize batch and fan-out patterns.",
        distribution="uniform",
        key_pattern="multi-key",
        read_ratio=50,
        write_ratio=50,
        multi_key=True,
        transactional=False,
        hot_key=False,
        default_command_groups=("strings", "keys"),
        default_commands=("MGET", "MSET", "MSETNX", "DEL", "EXISTS", "TOUCH", "UNLINK"),
    ),
    WorkloadDefinition(
        name="hot-key",
        description="A focused hot-key workload for cache-efficiency and contention analysis.",
        distribution="zipfian",
        key_pattern="hot-key",
        read_ratio=85,
        write_ratio=15,
        multi_key=False,
        transactional=False,
        hot_key=True,
        default_command_groups=("strings",),
        default_commands=("GET", "SET", "INCR", "MGET"),
    ),
    WorkloadDefinition(
        name="single_key_mixed",
        description="Single-key mixed operations without transaction boundaries.",
        distribution="uniform",
        key_pattern="single-key",
        read_ratio=60,
        write_ratio=40,
        multi_key=False,
        transactional=False,
        hot_key=False,
        default_command_groups=("strings", "keys"),
        default_commands=("GET", "SET", "INCR", "DEL", "EXPIRE"),
    ),
    WorkloadDefinition(
        name="multi_key_only",
        description="Pure multi-key operations for fan-out and aggregation paths.",
        distribution="uniform",
        key_pattern="multi-key",
        read_ratio=50,
        write_ratio=50,
        multi_key=True,
        transactional=False,
        hot_key=False,
        default_command_groups=("strings", "keys"),
        default_commands=("MGET", "MSET", "MSETNX", "DEL", "UNLINK"),
    ),
    WorkloadDefinition(
        name="transaction_only",
        description="Only transaction commands and transaction-controlled flows.",
        distribution="uniform",
        key_pattern="single-key",
        read_ratio=50,
        write_ratio=50,
        multi_key=False,
        transactional=True,
        hot_key=False,
        default_command_groups=("transactions",),
        default_commands=("MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"),
    ),
    WorkloadDefinition(
        name="single_key_tx_mixed",
        description="Single-key traffic wrapped in optimistic transaction flows.",
        distribution="uniform",
        key_pattern="single-key",
        read_ratio=55,
        write_ratio=45,
        multi_key=False,
        transactional=True,
        hot_key=False,
        default_command_groups=("strings", "transactions"),
        default_commands=("WATCH", "MULTI", "GET", "SET", "INCR", "EXEC"),
    ),
    WorkloadDefinition(
        name="multi_key_tx_mixed",
        description="Multi-key traffic combined with optimistic transaction boundaries.",
        distribution="uniform",
        key_pattern="multi-key",
        read_ratio=55,
        write_ratio=45,
        multi_key=True,
        transactional=True,
        hot_key=False,
        default_command_groups=("strings", "keys", "transactions"),
        default_commands=("WATCH", "MULTI", "MGET", "MSET", "DEL", "EXEC"),
    ),
)

WORKLOAD_ALIASES = _build_alias_map(
    (
        definition.name,
        (
            definition.name,
            definition.name.replace(" ", "_"),
            definition.name.replace(" ", "-"),
            definition.name.replace("-", " "),
            definition.name.replace("-", "_"),
            definition.name.replace("_", " "),
        ),
    )
    for definition in WORKLOAD_DEFINITIONS
)

WORKLOAD_MAP = {definition.name: definition for definition in WORKLOAD_DEFINITIONS}
COMMAND_GROUP_MAP = {definition.name: definition for definition in COMMAND_GROUP_DEFINITIONS}
BACKEND_MAP = {definition.name: definition for definition in BACKEND_DEFINITIONS}


def resolve_backend_name(name: str) -> str:
    key = _normalize_lookup_key(name)
    try:
        return BACKEND_ALIASES[key]
    except KeyError as error:
        supported = ", ".join(sorted(BACKEND_MAP))
        raise ValueError(f"unsupported backend '{name}'. Supported backends: {supported}") from error


def resolve_command_group_name(name: str) -> str:
    key = _normalize_lookup_key(name)
    try:
        return COMMAND_GROUP_ALIASES[key]
    except KeyError as error:
        supported = ", ".join(sorted(COMMAND_GROUP_MAP))
        raise ValueError(
            f"unsupported command group '{name}'. Supported command groups: {supported}"
        ) from error


def resolve_workload_name(name: str) -> str:
    key = _normalize_lookup_key(name)
    try:
        return WORKLOAD_ALIASES[key]
    except KeyError as error:
        supported = ", ".join(sorted(WORKLOAD_MAP))
        raise ValueError(f"unsupported workload '{name}'. Supported workloads: {supported}") from error


def normalize_command_name(name: str) -> str:
    normalized = name.strip().upper()
    if not normalized:
        raise ValueError("command names must not be empty")
    if normalized not in KNOWN_COMMANDS:
        supported = ", ".join(sorted(KNOWN_COMMANDS))
        raise ValueError(f"unsupported command '{name}'. Supported commands: {supported}")
    return normalized


def get_workload_definition(name: str) -> WorkloadDefinition:
    return WORKLOAD_MAP[resolve_workload_name(name)]


def expand_command_groups(group_names: list[str]) -> list[str]:
    expanded: list[str] = []
    seen: set[str] = set()
    for group_name in group_names:
        definition = COMMAND_GROUP_MAP[group_name]
        for command in definition.commands:
            if command in seen:
                continue
            seen.add(command)
            expanded.append(command)
    return expanded
