//! Server command handlers.
//!
//! DBSIZE, FLUSHDB, FLUSHALL, INFO, COMMAND, HELLO, CLIENT, and TIME.

use vortex_proto::{CommandFlags, CommandMeta, FrameRef, RespFrame};

use super::{
    CmdResult, CommandArgs, ERR_SYNTAX, ExecutedCommand, NS_PER_SEC, RESP_EMPTY_ARRAY, RESP_OK,
    mutation_error_response, resolve_unix_time_now_nanos,
};
use crate::{ConcurrentKeyspace, effects::MutationErrorKind, keyspace::RuntimeMetricsSnapshot};

const ERR_CLIENT_UNSUPPORTED: &[u8] = b"-ERR unknown subcommand or wrong number of arguments\r\n";
const ERR_HELLO_RESP3_UNSUPPORTED: &[u8] = b"-NOPROTO RESP3 is not supported by VortexDB alpha\r\n";
const ERR_HELLO_PROTO_UNSUPPORTED: &[u8] = b"-NOPROTO unsupported protocol version\r\n";

/// Alpha-visible command set. This excludes commands that are still stubs,
/// unsupported, or intentionally disabled for the alpha release.
const SUPPORTED_COMMANDS: &[&str] = &[
    // Connection
    "PING",
    "ECHO",
    "QUIT",
    "SELECT",
    "HELLO",
    "CLIENT",
    // Server
    "COMMAND",
    "INFO",
    "DBSIZE",
    "FLUSHDB",
    "FLUSHALL",
    "CONFIG",
    "TIME",
    "MULTI",
    "EXEC",
    "DISCARD",
    "WATCH",
    "UNWATCH",
    // String
    "SET",
    "GET",
    "SETNX",
    "SETEX",
    "PSETEX",
    "MSET",
    "MSETNX",
    "MGET",
    "GETSET",
    "GETDEL",
    "GETEX",
    "INCR",
    "DECR",
    "INCRBY",
    "DECRBY",
    "INCRBYFLOAT",
    "APPEND",
    "STRLEN",
    "GETRANGE",
    "SETRANGE",
    // Key
    "DEL",
    "UNLINK",
    "EXISTS",
    "EXPIRE",
    "PEXPIRE",
    "EXPIREAT",
    "PEXPIREAT",
    "PERSIST",
    "TTL",
    "PTTL",
    "EXPIRETIME",
    "PEXPIRETIME",
    "RENAME",
    "RENAMENX",
    "KEYS",
    "SCAN",
    "RANDOMKEY",
    "TOUCH",
    "COPY",
    "TYPE",
];

#[inline]
fn lookup_supported_command(name: &str) -> Option<&'static CommandMeta> {
    SUPPORTED_COMMANDS
        .iter()
        .copied()
        .find(|candidate| *candidate == name)
        .and_then(vortex_proto::command::lookup_command)
}

#[inline]
fn lookup_supported_command_bytes(name: &[u8]) -> Option<&'static CommandMeta> {
    SUPPORTED_COMMANDS
        .iter()
        .copied()
        .find(|candidate| name.eq_ignore_ascii_case(candidate.as_bytes()))
        .and_then(vortex_proto::command::lookup_command)
}

/// DBSIZE
///
/// Returns the number of keys in the shard as an integer.
#[inline]
pub fn cmd_dbsize(
    keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    super::int_resp(keyspace.cmd_dbsize(now_nanos) as i64)
}

/// FLUSHDB [ASYNC|SYNC]
///
/// Removes all keys from the shard. Phase 3: ASYNC is accepted but executes
/// synchronously.
#[inline]
pub fn cmd_flushdb(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> ExecutedCommand {
    if !valid_flush_args(frame) {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    }
    match keyspace.cmd_flush_all() {
        Ok(aof_lsn) => ExecutedCommand::with_aof_lsn(CmdResult::Static(RESP_OK), aof_lsn),
        Err(error) => ExecutedCommand::from(CmdResult::Static(mutation_error_response(
            MutationErrorKind::from(error),
        ))),
    }
}

/// FLUSHALL [ASYNC|SYNC]
///
/// Same as FLUSHDB for the shared-keyspace alpha.
#[inline]
pub fn cmd_flushall(
    keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> ExecutedCommand {
    if !valid_flush_args(frame) {
        return ExecutedCommand::from(CmdResult::Static(ERR_SYNTAX));
    }
    match keyspace.cmd_flush_all() {
        Ok(aof_lsn) => ExecutedCommand::with_aof_lsn(CmdResult::Static(RESP_OK), aof_lsn),
        Err(error) => ExecutedCommand::from(CmdResult::Static(mutation_error_response(
            MutationErrorKind::from(error),
        ))),
    }
}

fn valid_flush_args(frame: &FrameRef<'_>) -> bool {
    let Some(args) = CommandArgs::collect(frame) else {
        return false;
    };

    match args.len() {
        1 => true,
        2 => args
            .get(1)
            .is_some_and(|mode| eq_ci(mode, b"sync") || eq_ci(mode, b"async")),
        _ => false,
    }
}

/// HELLO [protover [SETNAME name]]
///
/// VortexDB alpha remains a RESP2 server. `HELLO` without a protocol version,
/// and `HELLO 2`, return Redis-shaped RESP2 handshake metadata. RESP3 is
/// rejected explicitly so clients do not assume RESP3 push/map semantics.
#[inline]
pub fn cmd_hello(
    _keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    let Some(args) = CommandArgs::collect(frame) else {
        return CmdResult::Static(ERR_SYNTAX);
    };

    if args.len() == 1 {
        return hello_resp2();
    }

    let Some(proto) = args.get(1).and_then(parse_i64_arg) else {
        return CmdResult::Static(ERR_HELLO_PROTO_UNSUPPORTED);
    };
    match proto {
        2 if valid_hello_resp2_options(&args) => hello_resp2(),
        2 => CmdResult::Static(ERR_SYNTAX),
        3 => CmdResult::Static(ERR_HELLO_RESP3_UNSUPPORTED),
        _ => CmdResult::Static(ERR_HELLO_PROTO_UNSUPPORTED),
    }
}

/// CLIENT SETINFO LIB-NAME name|LIB-VER version
///
/// Accepts modern client-library metadata setup as an alpha no-op. Broader
/// CLIENT subcommands remain unsupported because the engine does not own
/// per-connection identity, listing, kill, tracking, or naming state.
#[inline]
pub fn cmd_client(
    _keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    let Some(args) = CommandArgs::collect(frame) else {
        return CmdResult::Static(ERR_CLIENT_UNSUPPORTED);
    };

    if args.len() == 4
        && args.get(1).is_some_and(|sub| eq_ci(sub, b"setinfo"))
        && args
            .get(2)
            .is_some_and(|field| eq_ci(field, b"lib-name") || eq_ci(field, b"lib-ver"))
    {
        return CmdResult::Static(RESP_OK);
    }

    CmdResult::Static(ERR_CLIENT_UNSUPPORTED)
}

fn valid_hello_resp2_options(args: &CommandArgs<'_>) -> bool {
    let mut index = 2usize;
    while index < args.len() {
        let Some(option) = args.get(index) else {
            return false;
        };
        if eq_ci(option, b"setname") {
            if args.get(index + 1).is_none() {
                return false;
            }
            index += 2;
            continue;
        }
        return false;
    }
    true
}

fn hello_resp2() -> CmdResult {
    CmdResult::Resp(RespFrame::Array(Some(vec![
        bulk_static(b"server"),
        bulk_static(b"vortex"),
        bulk_static(b"version"),
        bulk_static(env!("CARGO_PKG_VERSION").as_bytes()),
        bulk_static(b"proto"),
        RespFrame::integer(2),
        bulk_static(b"id"),
        RespFrame::integer(0),
        bulk_static(b"mode"),
        bulk_static(b"standalone"),
        bulk_static(b"role"),
        bulk_static(b"master"),
        bulk_static(b"modules"),
        RespFrame::Array(Some(Vec::new())),
    ])))
}

#[inline]
fn bulk_static(value: &'static [u8]) -> RespFrame {
    RespFrame::bulk_string(bytes::Bytes::from_static(value))
}

#[inline]
fn parse_i64_arg(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// INFO [section]
///
/// Returns a bulk string with server statistics.
/// Sections: server, clients, memory, runtime, keyspace. Default = all.
pub fn cmd_info(keyspace: &ConcurrentKeyspace, frame: &FrameRef<'_>, now_nanos: u64) -> CmdResult {
    let section = CommandArgs::collect(frame)
        .and_then(|args| args.get(1))
        .unwrap_or(b"all");
    let all = eq_ci(section, b"all") || eq_ci(section, b"everything");
    let (keys, expires) = keyspace.info_keyspace(now_nanos);
    let runtime = keyspace.runtime_metrics();

    let mut buf = Vec::with_capacity(512);

    if all || eq_ci(section, b"server") {
        write_info_server(&mut buf);
    }
    if all || eq_ci(section, b"clients") {
        write_info_clients(&mut buf);
    }
    if all || eq_ci(section, b"memory") {
        write_info_memory(&mut buf, keyspace);
    }
    if all || eq_ci(section, b"runtime") {
        write_info_runtime(&mut buf, runtime);
    }
    if all || eq_ci(section, b"keyspace") {
        write_info_keyspace(&mut buf, keys, expires);
    }

    CmdResult::Resp(RespFrame::bulk_string(bytes::Bytes::from(buf)))
}

fn write_info_server(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"# Server\r\n");
    buf.extend_from_slice(b"vortex_version:");
    buf.extend_from_slice(env!("CARGO_PKG_VERSION").as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"redis_version:7.4.0\r\n");
    #[cfg(target_arch = "aarch64")]
    buf.extend_from_slice(b"arch_bits:64\r\nserver_arch:aarch64\r\n");
    #[cfg(target_arch = "x86_64")]
    buf.extend_from_slice(b"arch_bits:64\r\nserver_arch:x86_64\r\n");
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    buf.extend_from_slice(b"arch_bits:64\r\n");
    buf.extend_from_slice(b"tcp_port:6379\r\n");
    buf.extend_from_slice(b"process_id:");
    itoa_append(buf, std::process::id() as i64);
    buf.extend_from_slice(b"\r\n\r\n");
}

fn write_info_clients(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"# Clients\r\n");
    buf.extend_from_slice(b"connected_clients:0\r\n\r\n");
}

fn write_info_memory(buf: &mut Vec<u8>, keyspace: &ConcurrentKeyspace) {
    buf.extend_from_slice(b"# Memory\r\n");

    // jemalloc epoch advance — ensures stats are current
    let epoch: u64 = 1;
    // SAFETY: Writing the epoch is a standard jemalloc API operation to refresh stats.
    let _ = unsafe { tikv_jemalloc_ctl::raw::write(b"epoch\0", epoch) };

    // Read jemalloc stats
    let allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0);
    let active = tikv_jemalloc_ctl::stats::active::read().unwrap_or(0);
    let resident = tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0);
    let mapped = tikv_jemalloc_ctl::stats::mapped::read().unwrap_or(0);
    let retained = tikv_jemalloc_ctl::stats::retained::read().unwrap_or(0);
    let dataset_bytes = keyspace.memory_used();
    let engine = keyspace.engine_memory_attribution();
    let server = keyspace.server_memory_attribution();
    let runtime = keyspace.runtime_metrics();

    // Read process RSS from /proc/self/statm
    let rss_bytes = read_proc_rss_bytes();

    // Fragmentation ratio: resident / allocated (or 0 if allocated is 0)
    let frag_ratio = if allocated > 0 {
        resident as f64 / allocated as f64
    } else {
        0.0
    };
    let frag_bytes = resident.saturating_sub(allocated);

    // used_memory = jemalloc allocated (what the application asked for)
    buf.extend_from_slice(b"used_memory:");
    itoa_append(buf, allocated as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"used_memory_human:");
    write_human_size(buf, allocated);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"used_memory_rss:");
    itoa_append(buf, rss_bytes as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"used_memory_peak:");
    itoa_append(buf, resident as i64);
    buf.extend_from_slice(b"\r\n");

    // Expose live keyspace bytes separately from allocator state so benchmark
    // reports can distinguish logical dataset growth from allocator churn.
    buf.extend_from_slice(b"used_memory_dataset:");
    itoa_append(buf, dataset_bytes as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"used_memory_overhead:");
    itoa_append(buf, allocated.saturating_sub(dataset_bytes) as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"used_memory_startup:0\r\n");
    buf.extend_from_slice(b"used_memory_scripts:0\r\n");

    buf.extend_from_slice(b"memory_attribution_engine_scope:engine_only\r\n");
    buf.extend_from_slice(b"memory_attribution_full_server_scope:process_rss_plus_io\r\n");

    write_info_usize(buf, b"engine_live_keys:", engine.live_keys);
    write_info_usize(
        buf,
        b"engine_logical_dataset_bytes:",
        engine.logical_dataset_bytes,
    );
    write_info_usize(
        buf,
        b"engine_table_allocated_bytes:",
        engine.table_allocated_bytes,
    );
    write_info_usize(buf, b"engine_table_total_slots:", engine.table_total_slots);
    write_info_usize(
        buf,
        b"engine_capacity_slack_slots:",
        engine.capacity_slack_slots,
    );
    write_info_usize(buf, b"engine_tombstone_slots:", engine.tombstone_slots);
    write_info_float(buf, b"engine_load_factor:", engine.load_factor);
    write_info_float(
        buf,
        b"engine_bytes_per_live_key:",
        engine.bytes_per_live_key.unwrap_or(0.0),
    );
    write_info_usize(buf, b"engine_shard_count:", engine.shard_count);
    write_info_usize(
        buf,
        b"io_fixed_buffer_reserved_bytes:",
        server.io_fixed_buffer_reserved_bytes,
    );
    write_info_usize(
        buf,
        b"io_fixed_buffer_committed_bytes:",
        server.io_fixed_buffer_committed_bytes,
    );
    write_info_usize(
        buf,
        b"io_fixed_buffer_active_bytes:",
        server.io_fixed_buffer_active_bytes,
    );
    write_info_usize(buf, b"io_fixed_buffer_count:", server.fixed_buffer_count);
    write_info_usize(buf, b"io_fixed_buffer_size:", server.fixed_buffer_size);
    write_info_usize(
        buf,
        b"per_connection_state_bytes:",
        server.per_connection_state_bytes,
    );
    write_info_u64(
        buf,
        b"client_retained_bytes:",
        runtime.client_retained_bytes,
    );
    write_info_u64(
        buf,
        b"client_retained_bytes_max:",
        runtime.client_retained_bytes_max,
    );
    write_info_u64(
        buf,
        b"client_retained_bytes_peak:",
        runtime.client_retained_bytes_peak,
    );
    write_info_usize(buf, b"connection_capacity:", server.connection_capacity);
    write_info_usize(buf, b"full_server_process_rss_bytes:", rss_bytes);

    // Allocator stats — consumed by benchmark telemetry
    buf.extend_from_slice(b"allocator_allocated:");
    itoa_append(buf, allocated as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"allocator_active:");
    itoa_append(buf, active as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"allocator_resident:");
    itoa_append(buf, resident as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"allocator_mapped:");
    itoa_append(buf, mapped as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"allocator_retained:");
    itoa_append(buf, retained as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"allocator_frag_ratio:");
    write_float(buf, frag_ratio);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"allocator_frag_bytes:");
    itoa_append(buf, frag_bytes as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"allocator_rss_ratio:");
    let rss_ratio = if active > 0 {
        resident as f64 / active as f64
    } else {
        0.0
    };
    write_float(buf, rss_ratio);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"allocator_rss_bytes:");
    itoa_append(buf, resident.saturating_sub(active) as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"mem_fragmentation_ratio:");
    let mem_frag = if allocated > 0 {
        rss_bytes as f64 / allocated as f64
    } else {
        0.0
    };
    write_float(buf, mem_frag);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"mem_fragmentation_bytes:");
    itoa_append(buf, (rss_bytes as i64).saturating_sub(allocated as i64));
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"allocator_mapped:");
    itoa_append(buf, mapped as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"allocator_retained:");
    itoa_append(buf, retained as i64);
    buf.extend_from_slice(b"\r\n");

    buf.extend_from_slice(b"total_system_memory:");
    itoa_append(buf, read_total_system_memory() as i64);
    buf.extend_from_slice(b"\r\n\r\n");
}

/// Read process RSS from /proc/self/statm (Linux).
fn read_proc_rss_bytes() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            let parts: Vec<&str> = statm.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(rss_pages) = parts[1].parse::<usize>() {
                    // SAFETY: sysconf reads the process page-size setting and
                    // does not mutate Rust memory.
                    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
                    if page_size > 0 {
                        return rss_pages.saturating_mul(page_size as usize);
                    }
                }
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// Read total system memory from /proc/meminfo (Linux).
fn read_total_system_memory() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    let rest = rest.trim();
                    if let Some(kb_str) =
                        rest.strip_suffix("kB").or_else(|| rest.strip_suffix("KB"))
                    {
                        if let Ok(kb) = kb_str.trim().parse::<usize>() {
                            return kb * 1024;
                        }
                    }
                }
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// Write a float with 2 decimal places.
fn write_float(buf: &mut Vec<u8>, v: f64) {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(16);
    let _ = write!(s, "{:.2}", v);
    buf.extend_from_slice(s.as_bytes());
}

/// Write a human-readable size (e.g., "1.23M").
fn write_human_size(buf: &mut Vec<u8>, bytes: usize) {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(16);
    if bytes >= 1024 * 1024 * 1024 {
        let _ = write!(s, "{:.2}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0));
    } else if bytes >= 1024 * 1024 {
        let _ = write!(s, "{:.2}M", bytes as f64 / (1024.0 * 1024.0));
    } else if bytes >= 1024 {
        let _ = write!(s, "{:.2}K", bytes as f64 / 1024.0);
    } else {
        let _ = write!(s, "{}B", bytes);
    }
    buf.extend_from_slice(s.as_bytes());
}

fn write_info_u64(buf: &mut Vec<u8>, name: &[u8], value: u64) {
    buf.extend_from_slice(name);
    let mut tmp = itoa::Buffer::new();
    buf.extend_from_slice(tmp.format(value).as_bytes());
    buf.extend_from_slice(b"\r\n");
}

fn write_info_usize(buf: &mut Vec<u8>, name: &[u8], value: usize) {
    write_info_u64(buf, name, value as u64);
}

fn write_info_float(buf: &mut Vec<u8>, name: &[u8], value: f64) {
    buf.extend_from_slice(name);
    write_float(buf, value);
    buf.extend_from_slice(b"\r\n");
}

fn write_info_str(buf: &mut Vec<u8>, name: &[u8], value: &str) {
    buf.extend_from_slice(name);
    buf.extend_from_slice(value.as_bytes());
    buf.extend_from_slice(b"\r\n");
}

fn write_info_bool(buf: &mut Vec<u8>, name: &[u8], value: bool) {
    write_info_u64(buf, name, u64::from(value));
}

fn write_info_runtime(buf: &mut Vec<u8>, runtime: RuntimeMetricsSnapshot) {
    buf.extend_from_slice(b"# Runtime\r\n");
    write_info_str(
        buf,
        b"runtime_telemetry_mode:",
        runtime.telemetry_mode.as_str(),
    );
    write_info_bool(
        buf,
        b"runtime_profile_timers_available:",
        runtime.profile_timers_available,
    );
    write_info_bool(
        buf,
        b"runtime_local_flush_metrics_available:",
        runtime.local_flush_metrics_available,
    );
    write_info_u64(
        buf,
        b"runtime_local_flush_sample_rate:",
        runtime.local_flush_sample_rate,
    );
    write_info_u64(
        buf,
        b"runtime_metrics_flush_interval_ms:",
        runtime.metrics_flush_interval_millis,
    );
    write_info_str(
        buf,
        b"backend_requested:",
        runtime.backend.requested.as_str(),
    );
    write_info_str(
        buf,
        b"backend_effective:",
        runtime.backend.effective.as_str(),
    );
    write_info_bool(buf, b"backend_plan_mixed:", runtime.backend.mixed);
    write_info_bool(
        buf,
        b"backend_fixed_buffers_capable:",
        runtime.backend.fixed_buffers_capable,
    );
    write_info_bool(
        buf,
        b"backend_fixed_buffers_registered:",
        runtime.backend.fixed_buffers_registered,
    );
    write_info_bool(buf, b"backend_sqpoll:", runtime.backend.sqpoll);
    write_info_bool(
        buf,
        b"backend_multishot_accept:",
        runtime.backend.multishot_accept,
    );
    write_info_bool(buf, b"backend_accept4:", runtime.backend.accept4);
    write_info_bool(buf, b"backend_close_opcode:", runtime.backend.close_opcode);
    write_info_bool(
        buf,
        b"backend_cancel_support:",
        runtime.backend.cancel_support,
    );
    write_info_bool(
        buf,
        b"backend_nonblocking_drain:",
        runtime.backend.nonblocking_drain,
    );
    write_info_u64(
        buf,
        b"backend_requested_ring_size:",
        runtime.backend.requested_ring_size,
    );
    write_info_u64(
        buf,
        b"backend_effective_ring_size:",
        runtime.backend.effective_ring_size,
    );
    write_info_u64(
        buf,
        b"backend_submit_syscalls:",
        runtime.backend_submit_syscalls,
    );
    write_info_u64(
        buf,
        b"backend_sq_occupancy_max:",
        runtime.backend_sq_occupancy_max,
    );
    write_info_u64(buf, b"backend_sq_capacity:", runtime.backend_sq_capacity);
    write_info_u64(
        buf,
        b"backend_cq_occupancy_max:",
        runtime.backend_cq_occupancy_max,
    );
    write_info_u64(buf, b"backend_cq_capacity:", runtime.backend_cq_capacity);
    write_info_u64(buf, b"backend_cq_overflows:", runtime.backend_cq_overflows);
    write_info_u64(
        buf,
        b"backend_completions_per_submit_syscall_x1000:",
        runtime.backend_completions_per_submit_syscall_x1000,
    );
    write_info_u64(
        buf,
        b"backend_sq_pressure_events:",
        runtime.submit_sq_full_retries,
    );
    write_info_u64(
        buf,
        b"backend_cq_pressure_events:",
        runtime.completion_budget_exhaustions,
    );
    buf.extend_from_slice(b"runtime_reactor_slots:");
    itoa_append(buf, runtime.reactor_slots as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_loop_iterations:");
    itoa_append(buf, runtime.loop_iterations as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_accept_eagain_rearms:");
    itoa_append(buf, runtime.accept_eagain_rearms as i64);
    buf.extend_from_slice(b"\r\n");
    write_info_u64(
        buf,
        b"reactor_accept_drain_runs:",
        runtime.accept_drain_runs,
    );
    write_info_u64(
        buf,
        b"reactor_accept_drain_accepted:",
        runtime.accept_drain_accepted,
    );
    write_info_u64(
        buf,
        b"reactor_accept_drain_accepted_max:",
        runtime.accept_drain_accepted_max,
    );
    buf.extend_from_slice(b"reactor_submit_sq_full_retries:");
    itoa_append(buf, runtime.submit_sq_full_retries as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_submit_failures:");
    itoa_append(buf, runtime.submit_failures as i64);
    buf.extend_from_slice(b"\r\n");
    write_info_u64(
        buf,
        b"reactor_completion_budget_exhaustions:",
        runtime.completion_budget_exhaustions,
    );
    write_info_u64(
        buf,
        b"reactor_command_budget_exhaustions:",
        runtime.command_budget_exhaustions,
    );
    write_info_u64(
        buf,
        b"reactor_accept_budget_exhaustions:",
        runtime.accept_budget_exhaustions,
    );
    write_info_u64(
        buf,
        b"reactor_writev_budget_exhaustions:",
        runtime.writev_budget_exhaustions,
    );
    write_info_u64(
        buf,
        b"reactor_maintenance_budget_exhaustions:",
        runtime.maintenance_budget_exhaustions,
    );
    write_info_u64(
        buf,
        b"reactor_yielded_connections:",
        runtime.yielded_connections,
    );
    write_info_u64(buf, b"reactor_parser_resumes:", runtime.parser_resumes);
    buf.extend_from_slice(b"reactor_completion_batches:");
    itoa_append(buf, runtime.completion_batch_count as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_completion_batch_total:");
    itoa_append(buf, runtime.completion_batch_total as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_completion_batch_max:");
    itoa_append(buf, runtime.completion_batch_max as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_completion_batch_avg:");
    write_float(buf, runtime.completion_batch_avg);
    buf.extend_from_slice(b"\r\n");
    write_info_u64(
        buf,
        b"reactor_completion_nanos_total:",
        runtime.completion_nanos_total,
    );
    write_info_u64(
        buf,
        b"reactor_completion_nanos_max:",
        runtime.completion_nanos_max,
    );
    buf.extend_from_slice(b"reactor_command_batches:");
    itoa_append(buf, runtime.command_batch_count as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_command_batch_total:");
    itoa_append(buf, runtime.command_batch_total as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_command_batch_max:");
    itoa_append(buf, runtime.command_batch_max as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_command_batch_avg:");
    write_float(buf, runtime.command_batch_avg);
    buf.extend_from_slice(b"\r\n");
    write_info_u64(buf, b"reactor_writev_chunks:", runtime.writev_chunks);
    write_info_u64(
        buf,
        b"reactor_writev_iovecs_total:",
        runtime.writev_iovecs_total,
    );
    write_info_u64(
        buf,
        b"reactor_writev_iovecs_max:",
        runtime.writev_iovecs_max,
    );
    write_info_u64(
        buf,
        b"reactor_queued_response_bytes_total:",
        runtime.queued_response_bytes_total,
    );
    write_info_u64(
        buf,
        b"reactor_queued_response_bytes_max:",
        runtime.queued_response_bytes_max,
    );
    write_info_u64(
        buf,
        b"reactor_client_retained_bytes:",
        runtime.client_retained_bytes,
    );
    write_info_u64(
        buf,
        b"reactor_client_retained_bytes_max:",
        runtime.client_retained_bytes_max,
    );
    write_info_u64(
        buf,
        b"reactor_client_retained_bytes_peak:",
        runtime.client_retained_bytes_peak,
    );
    write_info_u64(
        buf,
        b"reactor_request_cap_exceeded:",
        runtime.request_cap_exceeded,
    );
    write_info_u64(
        buf,
        b"reactor_response_cap_exceeded:",
        runtime.response_cap_exceeded,
    );
    write_info_u64(
        buf,
        b"reactor_multi_queue_command_cap_exceeded:",
        runtime.multi_queue_command_cap_exceeded,
    );
    write_info_u64(
        buf,
        b"reactor_multi_queue_bytes_cap_exceeded:",
        runtime.multi_queue_bytes_cap_exceeded,
    );
    write_info_u64(
        buf,
        b"reactor_watch_cap_exceeded:",
        runtime.watch_cap_exceeded,
    );
    write_info_u64(
        buf,
        b"reactor_writev_chunk_cap_exceeded:",
        runtime.writev_chunk_cap_exceeded,
    );
    write_info_u64(
        buf,
        b"reactor_overload_accept_throttled:",
        runtime.overload_accept_throttled,
    );
    write_info_u64(
        buf,
        b"reactor_overload_accept_resumed:",
        runtime.overload_accept_resumed,
    );
    write_info_u64(
        buf,
        b"reactor_overload_read_disabled:",
        runtime.overload_read_disabled,
    );
    write_info_u64(
        buf,
        b"reactor_overload_read_resumed:",
        runtime.overload_read_resumed,
    );
    write_info_u64(
        buf,
        b"reactor_overload_command_deferred:",
        runtime.overload_command_deferred,
    );
    write_info_u64(
        buf,
        b"reactor_overload_command_resumed:",
        runtime.overload_command_resumed,
    );
    write_info_u64(
        buf,
        b"reactor_overload_connections_dropped:",
        runtime.overload_connections_dropped,
    );
    write_info_u64(
        buf,
        b"reactor_overload_pending_response_bytes:",
        runtime.overload_pending_response_bytes,
    );
    write_info_u64(
        buf,
        b"reactor_overload_pending_response_bytes_peak:",
        runtime.overload_pending_response_bytes_peak,
    );
    write_info_u64(
        buf,
        b"reactor_overload_parser_accumulator_bytes:",
        runtime.overload_parser_accumulator_bytes,
    );
    write_info_u64(
        buf,
        b"reactor_overload_parser_accumulator_bytes_peak:",
        runtime.overload_parser_accumulator_bytes_peak,
    );
    write_info_u64(
        buf,
        b"reactor_overload_writev_backlog_bytes:",
        runtime.overload_writev_backlog_bytes,
    );
    write_info_u64(
        buf,
        b"reactor_overload_writev_backlog_bytes_peak:",
        runtime.overload_writev_backlog_bytes_peak,
    );
    write_info_u64(
        buf,
        b"reactor_overload_aof_pending_bytes:",
        runtime.overload_aof_pending_bytes,
    );
    write_info_u64(
        buf,
        b"reactor_overload_aof_pending_bytes_peak:",
        runtime.overload_aof_pending_bytes_peak,
    );
    write_info_u64(
        buf,
        b"reactor_overload_maintenance_debt:",
        runtime.overload_maintenance_debt,
    );
    write_info_u64(
        buf,
        b"reactor_overload_maintenance_debt_peak:",
        runtime.overload_maintenance_debt_peak,
    );
    write_info_u64(
        buf,
        b"reactor_overload_read_disabled_connections:",
        runtime.overload_read_disabled_connections,
    );
    write_info_u64(
        buf,
        b"reactor_overload_read_disabled_connections_peak:",
        runtime.overload_read_disabled_connections_peak,
    );
    write_info_u64(
        buf,
        b"reactor_overload_deferred_commands:",
        runtime.overload_deferred_commands,
    );
    write_info_u64(
        buf,
        b"reactor_overload_deferred_commands_peak:",
        runtime.overload_deferred_commands_peak,
    );
    write_info_u64(
        buf,
        b"reactor_close_drain_nanos_total:",
        runtime.close_drain_nanos_total,
    );
    write_info_u64(
        buf,
        b"reactor_close_drain_nanos_max:",
        runtime.close_drain_nanos_max,
    );
    buf.extend_from_slice(b"reactor_active_expiry_runs:");
    itoa_append(buf, runtime.active_expiry_runs as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_active_expiry_sampled:");
    itoa_append(buf, runtime.active_expiry_sampled as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_active_expiry_expired:");
    itoa_append(buf, runtime.active_expiry_expired as i64);
    buf.extend_from_slice(b"\r\n");
    write_info_u64(
        buf,
        b"reactor_active_expiry_nanos_total:",
        runtime.active_expiry_nanos_total,
    );
    write_info_u64(
        buf,
        b"reactor_active_expiry_nanos_max:",
        runtime.active_expiry_nanos_max,
    );
    write_info_u64(
        buf,
        b"reactor_aof_append_nanos_total:",
        runtime.aof_append_nanos_total,
    );
    write_info_u64(
        buf,
        b"reactor_aof_append_nanos_max:",
        runtime.aof_append_nanos_max,
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_nanos_total:",
        runtime.aof_fsync_nanos_total,
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_nanos_max:",
        runtime.aof_fsync_nanos_max,
    );
    write_info_u64(
        buf,
        b"reactor_aof_pending_bytes:",
        runtime.aof_pending_bytes,
    );
    write_info_u64(
        buf,
        b"reactor_aof_pending_bytes_max:",
        runtime.aof_pending_bytes_max,
    );
    write_info_u64(
        buf,
        b"reactor_aof_pending_writes:",
        runtime.aof_pending_writes,
    );
    write_info_u64(
        buf,
        b"reactor_aof_pending_writes_max:",
        runtime.aof_pending_writes_max,
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_requested:",
        runtime.aof_fsync_requested,
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_completed:",
        runtime.aof_fsync_completed,
    );
    write_info_u64(buf, b"reactor_aof_fsync_failed:", runtime.aof_fsync_failed);
    write_info_u64(
        buf,
        b"reactor_aof_fsync_worker_saturation:",
        runtime.aof_fsync_worker_saturation,
    );
    write_info_u64(
        buf,
        b"reactor_aof_backpressure_events:",
        runtime.aof_backpressure_events,
    );
    write_info_u64(
        buf,
        b"reactor_aof_backpressure_nanos_total:",
        runtime.aof_backpressure_nanos_total,
    );
    write_info_u64(
        buf,
        b"reactor_aof_backpressure_nanos_max:",
        runtime.aof_backpressure_nanos_max,
    );
    write_info_u64(
        buf,
        b"reactor_aof_last_appended_lsn:",
        runtime.aof_last_appended_lsn,
    );
    write_info_u64(
        buf,
        b"reactor_aof_last_durable_lsn:",
        runtime.aof_last_durable_lsn,
    );
    write_info_u64(
        buf,
        b"reactor_aof_durable_lsn_lag:",
        runtime.aof_durable_lsn_lag,
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_latency_nanos_total:",
        runtime.aof_fsync_latency_nanos_total,
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_latency_nanos_max:",
        runtime.aof_fsync_latency_nanos_max,
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_latency_le_100us:",
        runtime.aof_fsync_latency_buckets[0],
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_latency_le_500us:",
        runtime.aof_fsync_latency_buckets[1],
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_latency_le_1ms:",
        runtime.aof_fsync_latency_buckets[2],
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_latency_le_5ms:",
        runtime.aof_fsync_latency_buckets[3],
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_latency_le_10ms:",
        runtime.aof_fsync_latency_buckets[4],
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_latency_le_50ms:",
        runtime.aof_fsync_latency_buckets[5],
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_latency_le_100ms:",
        runtime.aof_fsync_latency_buckets[6],
    );
    write_info_u64(
        buf,
        b"reactor_aof_fsync_latency_gt_100ms:",
        runtime.aof_fsync_latency_buckets[7],
    );
    write_info_u64(
        buf,
        b"reactor_maintenance_nanos_total:",
        runtime.maintenance_nanos_total,
    );
    write_info_u64(
        buf,
        b"reactor_maintenance_nanos_max:",
        runtime.maintenance_nanos_max,
    );
    write_info_u64(
        buf,
        b"reactor_metrics_flush_nanos_total:",
        runtime.metrics_flush_nanos_total,
    );
    write_info_u64(
        buf,
        b"reactor_metrics_flush_nanos_max:",
        runtime.metrics_flush_nanos_max,
    );
    buf.extend_from_slice(b"eviction_admissions:");
    itoa_append(buf, runtime.eviction_admissions as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"eviction_shards_scanned:");
    itoa_append(buf, runtime.eviction_shards_scanned as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"eviction_slots_sampled:");
    itoa_append(buf, runtime.eviction_slots_sampled as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"eviction_bytes_freed:");
    itoa_append(buf, runtime.eviction_bytes_freed as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"eviction_oom_after_scan:");
    itoa_append(buf, runtime.eviction_oom_after_scan as i64);
    buf.extend_from_slice(b"\r\n");
    write_info_u64(buf, b"eviction_nanos_total:", runtime.eviction_nanos_total);
    write_info_u64(buf, b"eviction_nanos_max:", runtime.eviction_nanos_max);
    buf.extend_from_slice(b"\r\n");
}

fn write_info_keyspace(buf: &mut Vec<u8>, keys: usize, expires: usize) {
    buf.extend_from_slice(b"# Keyspace\r\n");
    if keys > 0 {
        buf.extend_from_slice(b"db0:keys=");
        itoa_append(buf, keys as i64);
        buf.extend_from_slice(b",expires=");
        itoa_append(buf, expires as i64);
        buf.extend_from_slice(b",avg_ttl=0\r\n");
    }
    buf.extend_from_slice(b"\r\n");
}

#[inline]
fn itoa_append(buf: &mut Vec<u8>, n: i64) {
    let mut tmp = itoa::Buffer::new();
    buf.extend_from_slice(tmp.format(n).as_bytes());
}

/// COMMAND [subcommand [args...]]
///
/// - COMMAND (no args): returns metadata for all commands.
/// - COMMAND COUNT: returns number of registered commands.
/// - COMMAND INFO cmd [cmd ...]: returns metadata for specific commands.
pub fn cmd_command(
    _keyspace: &ConcurrentKeyspace,
    frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> CmdResult {
    let Some(args) = CommandArgs::collect(frame) else {
        return cmd_command_all();
    };
    let argc = args.len();

    if argc <= 1 {
        return cmd_command_all();
    }

    let sub = args.get(1).unwrap_or(b"");
    if eq_ci(sub, b"count") {
        return cmd_command_count();
    }
    if eq_ci(sub, b"info") {
        return cmd_command_info(&args);
    }
    if eq_ci(sub, b"docs") || eq_ci(sub, b"list") || eq_ci(sub, b"getkeys") {
        return CmdResult::Static(RESP_EMPTY_ARRAY);
    }

    CmdResult::Static(b"-ERR unknown subcommand or wrong number of arguments\r\n")
}

fn cmd_command_all() -> CmdResult {
    let entries = collect_all_command_metas();
    let mut frames = Vec::with_capacity(entries.len());
    for meta in &entries {
        frames.push(command_meta_to_frame(meta));
    }
    CmdResult::Resp(RespFrame::Array(Some(frames)))
}

fn cmd_command_count() -> CmdResult {
    super::int_resp(collect_all_command_metas().len() as i64)
}

fn cmd_command_info(args: &CommandArgs<'_>) -> CmdResult {
    let argc = args.len();
    let mut frames = Vec::with_capacity(argc.saturating_sub(2));
    for i in 2..argc {
        if let Some(name_bytes) = args.get(i) {
            match lookup_supported_command_bytes(name_bytes) {
                Some(meta) => frames.push(command_meta_to_frame(meta)),
                None => frames.push(RespFrame::Null),
            }
        } else {
            frames.push(RespFrame::Null);
        }
    }
    CmdResult::Resp(RespFrame::Array(Some(frames)))
}

fn command_meta_to_frame(meta: &CommandMeta) -> RespFrame {
    let name = RespFrame::bulk_string(bytes::Bytes::from_static(meta.name.as_bytes()));
    let arity = RespFrame::integer(i64::from(meta.arity));

    let mut flags = Vec::with_capacity(4);
    if meta.flags.contains(CommandFlags::READ) {
        flags.push(RespFrame::simple_string("readonly"));
    }
    if meta.flags.contains(CommandFlags::WRITE) {
        flags.push(RespFrame::simple_string("write"));
    }
    if meta.flags.contains(CommandFlags::FAST) {
        flags.push(RespFrame::simple_string("fast"));
    }
    if meta.flags.contains(CommandFlags::SLOW) {
        flags.push(RespFrame::simple_string("slow"));
    }
    if meta.flags.contains(CommandFlags::ADMIN) {
        flags.push(RespFrame::simple_string("admin"));
    }
    if meta.flags.contains(CommandFlags::BLOCKING) {
        flags.push(RespFrame::simple_string("blocking"));
    }
    if meta.flags.contains(CommandFlags::PUBSUB) {
        flags.push(RespFrame::simple_string("pubsub"));
    }
    if meta.flags.contains(CommandFlags::SCRIPTING) {
        flags.push(RespFrame::simple_string("scripting"));
    }
    let flags_arr = RespFrame::Array(Some(flags));

    let first_key = RespFrame::integer(i64::from(meta.key_range.first));
    let last_key = RespFrame::integer(i64::from(meta.key_range.last));
    let step = RespFrame::integer(i64::from(meta.key_range.step));

    RespFrame::Array(Some(vec![
        name, arity, flags_arr, first_key, last_key, step,
    ]))
}

fn collect_all_command_metas() -> Vec<&'static CommandMeta> {
    let mut metas = Vec::with_capacity(SUPPORTED_COMMANDS.len());
    for &name in SUPPORTED_COMMANDS {
        if let Some(meta) = lookup_supported_command(name) {
            metas.push(meta);
        }
    }
    metas
}

// ── 3.6.4 — TIME ───────────────────────────────────────────────────

/// TIME
///
/// Returns [unix_seconds_string, microseconds_string] as a two-element array.
#[inline]
#[allow(dead_code)]
pub fn cmd_time(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    now_nanos: u64,
) -> CmdResult {
    cmd_time_with_clock(_keyspace, _frame, now_nanos, 0)
}

#[inline]
pub(crate) fn cmd_time_with_clock(
    _keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
    unix_now_nanos: u64,
) -> CmdResult {
    let unix_now_nanos = resolve_unix_time_now_nanos(unix_now_nanos);
    let secs = unix_now_nanos / NS_PER_SEC;
    let usecs = (unix_now_nanos % NS_PER_SEC) / 1_000;

    let mut sec_buf = itoa::Buffer::new();
    let sec_str = sec_buf.format(secs);
    let mut usec_buf = itoa::Buffer::new();
    let usec_str = usec_buf.format(usecs);

    CmdResult::Resp(RespFrame::Array(Some(vec![
        RespFrame::bulk_string(bytes::Bytes::copy_from_slice(sec_str.as_bytes())),
        RespFrame::bulk_string(bytes::Bytes::copy_from_slice(usec_str.as_bytes())),
    ])))
}

// ── Helpers ─────────────────────────────────────────────────────────

/// ASCII case-insensitive comparison.
#[inline]
fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::commands::test_harness::TestHarness;
    use vortex_proto::RespTape;

    fn make_resp(parts: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            buf.extend_from_slice(part);
            buf.extend_from_slice(b"\r\n");
        }
        buf
    }

    fn exec(h: &TestHarness, parts: &[&[u8]]) -> CmdResult {
        exec_at(h, parts, 0)
    }

    fn exec_at(h: &TestHarness, parts: &[&[u8]], now_nanos: u64) -> CmdResult {
        let wire = make_resp(parts);
        let tape = RespTape::parse_pipeline(&wire).expect("valid RESP");
        let frame = tape.iter().next().unwrap();
        let name_upper: Vec<u8> = parts[0].iter().map(|b| b.to_ascii_uppercase()).collect();
        crate::commands::execute_command(&h.keyspace, &name_upper, &frame, now_nanos)
            .expect("command should be recognized")
            .response
    }

    fn assert_static(r: &CmdResult, expected: &[u8]) {
        match r {
            CmdResult::Static(s) => assert_eq!(*s, expected, "static mismatch"),
            CmdResult::Inline(_) => panic!("expected Static, got Inline"),
            CmdResult::Owned(b) => panic!("expected Static, got Owned: {b:?}"),
            CmdResult::Resp(f) => panic!("expected Static, got Resp: {f:?}"),
        }
    }

    fn assert_integer(r: &CmdResult, expected: i64) {
        match r {
            CmdResult::Resp(RespFrame::Integer(n)) => {
                assert_eq!(*n, expected, "integer mismatch");
            }
            CmdResult::Static(s) => {
                let expected_bytes: &[u8] = match expected {
                    0 => b":0\r\n",
                    1 => b":1\r\n",
                    -1 => b":-1\r\n",
                    -2 => b":-2\r\n",
                    _ => panic!(
                        "expected Integer({expected}), got Static({:?})",
                        std::str::from_utf8(s)
                    ),
                };
                assert_eq!(*s, expected_bytes, "static integer mismatch for {expected}");
            }
            other => panic!("expected Integer({expected}), got {other:?}"),
        }
    }

    fn assert_bulk_contains(r: &CmdResult, needle: &[u8]) {
        match r {
            CmdResult::Resp(RespFrame::BulkString(Some(b))) => {
                let bytes: &[u8] = b.as_ref();
                assert!(
                    bytes.windows(needle.len()).any(|w| w == needle),
                    "bulk string does not contain {:?}",
                    std::str::from_utf8(needle).unwrap_or("<binary>")
                );
            }
            other => panic!("expected BulkString, got {other:?}"),
        }
    }

    fn bulk_string_text(r: &CmdResult) -> &str {
        match r {
            CmdResult::Resp(RespFrame::BulkString(Some(b))) => {
                std::str::from_utf8(b.as_ref()).expect("INFO response is UTF-8")
            }
            other => panic!("expected BulkString, got {other:?}"),
        }
    }

    fn production_region(source: &str) -> &str {
        source.split("\n#[cfg(test").next().unwrap_or(source)
    }

    fn production_function_body<'a>(source_name: &str, source: &'a str, function: &str) -> &'a str {
        let source = production_region(source);
        let start = source
            .find(&format!("fn {function}"))
            .unwrap_or_else(|| panic!("{source_name} must define `{function}`"));
        let body = &source[start..];
        body.split("\n    pub").next().unwrap_or(body)
    }

    fn production_function_after<'a>(
        source_name: &str,
        source: &'a str,
        anchor: &str,
        function: &str,
    ) -> &'a str {
        let source = production_region(source);
        let anchor_start = source
            .find(anchor)
            .unwrap_or_else(|| panic!("{source_name} must contain `{anchor}`"));
        let search = &source[anchor_start..];
        let function_start = search
            .find(&format!("fn {function}"))
            .unwrap_or_else(|| panic!("{source_name} must define `{function}` after `{anchor}`"));
        let body = &search[function_start..];
        let open = body
            .find('{')
            .unwrap_or_else(|| panic!("{source_name} `{function}` must have a body"));
        let mut depth = 0usize;
        for (offset, ch) in body[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return &body[..open + offset + 1];
                    }
                }
                _ => {}
            }
        }
        panic!("{source_name} `{function}` body is not balanced");
    }

    fn assert_contains_token(source: &str, context: &str, token: &str) {
        assert!(source.contains(token), "{context} must contain `{token}`");
    }

    fn assert_ordered_tokens(source: &str, context: &str, tokens: &[&str]) {
        let mut offset = 0usize;
        for token in tokens {
            let Some(relative_start) = source[offset..].find(token) else {
                panic!("{context} must contain `{token}` after byte offset {offset}");
            };
            offset += relative_start + token.len();
        }
    }

    fn assert_array_len(r: &CmdResult, expected: usize) {
        match r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert_eq!(
                    arr.len(),
                    expected,
                    "array length mismatch: got {}, expected {}",
                    arr.len(),
                    expected
                );
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    fn array_contains_command_name(r: &CmdResult, needle: &[u8]) -> bool {
        match r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => arr.iter().any(|frame| match frame {
                RespFrame::Array(Some(meta)) => match meta.first() {
                    Some(RespFrame::BulkString(Some(name))) => name.as_ref() == needle,
                    _ => false,
                },
                _ => false,
            }),
            other => panic!("expected Array, got {other:?}"),
        }
    }

    fn array_contains_bulk_pair(arr: &[RespFrame], key: &[u8], value: &[u8]) -> bool {
        arr.windows(2).any(|pair| {
            matches!(
                (&pair[0], &pair[1]),
                (RespFrame::BulkString(Some(k)), RespFrame::BulkString(Some(v)))
                    if k.as_ref() == key && v.as_ref() == value
            )
        })
    }

    fn array_contains_integer_pair(arr: &[RespFrame], key: &[u8], value: i64) -> bool {
        arr.windows(2).any(|pair| {
            matches!(
                (&pair[0], &pair[1]),
                (RespFrame::BulkString(Some(k)), RespFrame::Integer(n))
                    if k.as_ref() == key && *n == value
            )
        })
    }

    // ── DBSIZE ──

    #[test]
    fn dbsize_empty() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"DBSIZE"]);
        assert_integer(&r, 0);
    }

    #[test]
    fn dbsize_with_keys() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("a"), VortexValue::from_bytes(b"1"));
        h.set(VortexKey::from("b"), VortexValue::from_bytes(b"2"));
        h.set(VortexKey::from("c"), VortexValue::from_bytes(b"3"));
        let r = exec(&h, &[b"DBSIZE"]);
        assert_integer(&r, 3);
    }

    #[test]
    fn dbsize_excludes_expired_keys() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};

        let deadline = NS_PER_SEC;
        let now = deadline + 1;
        h.set(VortexKey::from("live"), VortexValue::from_bytes(b"1"));
        h.set_with_ttl(
            VortexKey::from("expired"),
            VortexValue::from_bytes(b"2"),
            deadline,
        );

        let r = exec_at(&h, &[b"DBSIZE"], now);
        assert_integer(&r, 1);
    }

    // ── FLUSHDB ──

    #[test]
    fn flushdb_empties_shard() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("a"), VortexValue::from_bytes(b"1"));
        h.set(VortexKey::from("b"), VortexValue::from_bytes(b"2"));
        assert_eq!(h.len(), 2);
        let r = exec(&h, &[b"FLUSHDB"]);
        assert_static(&r, RESP_OK);
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn flushdb_rejects_malformed_options_without_mutating() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("a"), VortexValue::from_bytes(b"1"));

        let r = exec(&h, &[b"FLUSHDB", b"later"]);
        assert_static(&r, ERR_SYNTAX);
        assert_eq!(h.len(), 1);

        let r = exec(&h, &[b"FLUSHDB", b"SYNC", b"extra"]);
        assert_static(&r, ERR_SYNTAX);
        assert_eq!(h.len(), 1);

        let r = exec(&h, &[b"FLUSHDB", b"ASYNC"]);
        assert_static(&r, RESP_OK);
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn flushall_empties_shard() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("x"), VortexValue::from_bytes(b"val"));
        let r = exec(&h, &[b"FLUSHALL"]);
        assert_static(&r, RESP_OK);
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn flushall_rejects_malformed_options_without_mutating() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("x"), VortexValue::from_bytes(b"val"));

        let r = exec(&h, &[b"FLUSHALL", b"eventually"]);
        assert_static(&r, ERR_SYNTAX);
        assert_eq!(h.len(), 1);

        let r = exec(&h, &[b"FLUSHALL", b"async", b"extra"]);
        assert_static(&r, ERR_SYNTAX);
        assert_eq!(h.len(), 1);

        let r = exec(&h, &[b"FLUSHALL", b"SYNC"]);
        assert_static(&r, RESP_OK);
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn engine_command_hot_paths_do_not_publish_runtime_telemetry() {
        let forbidden = [
            "record_reactor_",
            "publish_reactor_",
            "publish_runtime_backend(",
            ".runtime_metrics()",
        ];
        let files = [
            ("commands/connection.rs", include_str!("connection.rs")),
            ("commands/generic.rs", include_str!("generic.rs")),
            ("commands/mod.rs", include_str!("mod.rs")),
            ("commands/pattern.rs", include_str!("pattern.rs")),
            ("commands/string.rs", include_str!("string.rs")),
            ("commands/transaction.rs", include_str!("transaction.rs")),
            ("effects.rs", include_str!("../effects.rs")),
            ("engine/domain.rs", include_str!("../engine/domain.rs")),
            (
                "engine/domain/admin_ops.rs",
                include_str!("../engine/domain/admin_ops.rs"),
            ),
            (
                "engine/domain/key_ops.rs",
                include_str!("../engine/domain/key_ops.rs"),
            ),
            (
                "engine/domain/mutation.rs",
                include_str!("../engine/domain/mutation.rs"),
            ),
            (
                "engine/domain/scan_ops.rs",
                include_str!("../engine/domain/scan_ops.rs"),
            ),
            (
                "engine/domain/string_ops.rs",
                include_str!("../engine/domain/string_ops.rs"),
            ),
            (
                "engine/domain/string_tables.rs",
                include_str!("../engine/domain/string_tables.rs"),
            ),
            ("engine/mod.rs", include_str!("../engine/mod.rs")),
            ("entry.rs", include_str!("../entry.rs")),
            ("eviction.rs", include_str!("../eviction.rs")),
            ("executor.rs", include_str!("../executor.rs")),
            ("keyspace.rs", include_str!("../keyspace.rs")),
            ("keyspace/admin.rs", include_str!("../keyspace/admin.rs")),
            (
                "keyspace/eviction_sweep.rs",
                include_str!("../keyspace/eviction_sweep.rs"),
            ),
            ("keyspace/expiry.rs", include_str!("../keyspace/expiry.rs")),
            (
                "keyspace/features.rs",
                include_str!("../keyspace/features.rs"),
            ),
            ("keyspace/gate.rs", include_str!("../keyspace/gate.rs")),
            (
                "keyspace/lock_profile.rs",
                include_str!("../keyspace/lock_profile.rs"),
            ),
            ("keyspace/memory.rs", include_str!("../keyspace/memory.rs")),
            (
                "keyspace/persistence.rs",
                include_str!("../keyspace/persistence.rs"),
            ),
            ("keyspace/shards.rs", include_str!("../keyspace/shards.rs")),
            ("keyspace/watch.rs", include_str!("../keyspace/watch.rs")),
            ("morph.rs", include_str!("../morph.rs")),
            ("prefetch.rs", include_str!("../prefetch.rs")),
            ("table.rs", include_str!("../table.rs")),
        ];

        for (name, source) in files {
            let source = production_region(source);
            for token in forbidden {
                assert!(
                    !source.contains(token),
                    "{name} must not publish reactor/runtime telemetry from engine command hot paths via `{token}`"
                );
            }
        }
    }

    #[test]
    fn engine_eviction_profile_timer_publications_stay_profile_gated() {
        let profile_start = production_function_body(
            "keyspace/metrics.rs",
            include_str!("../keyspace/metrics.rs"),
            "runtime_profile_metric_start",
        );
        assert_ordered_tokens(
            profile_start,
            "engine runtime profile timer start gate",
            &[
                "#[cfg(feature = \"profile-telemetry\")]",
                "runtime_profile_timers_enabled()",
                "Some(Timestamp::now().as_nanos())",
                "None",
            ],
        );

        let profile_elapsed = production_function_body(
            "keyspace/metrics.rs",
            include_str!("../keyspace/metrics.rs"),
            "runtime_profile_metric_elapsed_nanos",
        );
        assert_ordered_tokens(
            profile_elapsed,
            "engine runtime profile timer elapsed gate",
            &[
                "#[cfg(feature = \"profile-telemetry\")]",
                "Timestamp::now().as_nanos()",
                "#[cfg(not(feature = \"profile-telemetry\"))]",
                "0",
            ],
        );

        let admission = production_function_body(
            "keyspace.rs",
            include_str!("../keyspace.rs"),
            "ensure_memory_for_snapshot",
        );
        assert_ordered_tokens(
            admission,
            "admission eviction scan timer metric",
            &[
                "let eviction_scan_start = self.runtime_profile_metric_start();",
                "self.runtime_profile_metric_elapsed_nanos(eviction_scan_start)",
                "record_with_duration",
            ],
        );
        assert!(
            !admission.contains("Timestamp::now"),
            "admission eviction timing must use the keyspace profile-timer helper"
        );

        let maintenance = production_function_body(
            "keyspace/eviction_sweep.rs",
            include_str!("../keyspace/eviction_sweep.rs"),
            "run_eviction_maintenance_on_shard",
        );
        assert_ordered_tokens(
            maintenance,
            "maintenance eviction scan timer metric",
            &[
                "let scan_start = self.runtime_profile_metric_start();",
                "let scan_nanos = self.runtime_profile_metric_elapsed_nanos(scan_start);",
                "record_with_duration",
            ],
        );
        assert!(
            !maintenance.contains("Timestamp::now"),
            "maintenance eviction timing must use the keyspace profile-timer helper"
        );
    }

    #[test]
    fn runtime_profile_only_accumulators_stay_feature_gated() {
        let metrics = production_region(include_str!("../keyspace/metrics.rs"));
        let profile_fields = [
            "completion_nanos_total: ShardedCounter,",
            "close_drain_nanos_total: ShardedCounter,",
            "active_expiry_nanos_total: ShardedCounter,",
            "aof_append_nanos_total: ShardedCounter,",
            "aof_fsync_nanos_total: ShardedCounter,",
            "aof_backpressure_nanos_total: RuntimeGaugeSlots,",
            "aof_fsync_latency_nanos_total: RuntimeGaugeSlots,",
            "aof_fsync_latency_buckets: [RuntimeGaugeSlots; RUNTIME_AOF_FSYNC_LATENCY_BUCKETS],",
            "maintenance_nanos_total: ShardedCounter,",
            "metrics_flush_nanos_total: ShardedCounter,",
            "completion_nanos_max: RuntimeMaxSlots,",
            "close_drain_nanos_max: RuntimeMaxSlots,",
            "active_expiry_nanos_max: RuntimeMaxSlots,",
            "aof_append_nanos_max: RuntimeMaxSlots,",
            "aof_fsync_nanos_max: RuntimeMaxSlots,",
            "aof_backpressure_nanos_max: RuntimeMaxSlots,",
            "aof_fsync_latency_nanos_max: RuntimeMaxSlots,",
            "maintenance_nanos_max: RuntimeMaxSlots,",
            "metrics_flush_nanos_max: RuntimeMaxSlots,",
        ];
        for field in profile_fields {
            let token = format!("#[cfg(feature = \"profile-telemetry\")]\n    {field}");
            assert_contains_token(metrics, "runtime profile accumulator storage", &token);
        }

        let profile_initializers = [
            "completion_nanos_total: ShardedCounter::new(slot_count),",
            "close_drain_nanos_total: ShardedCounter::new(slot_count),",
            "active_expiry_nanos_total: ShardedCounter::new(slot_count),",
            "aof_append_nanos_total: ShardedCounter::new(slot_count),",
            "aof_fsync_nanos_total: ShardedCounter::new(slot_count),",
            "aof_backpressure_nanos_total: RuntimeGaugeSlots::new(slot_count),",
            "aof_fsync_latency_nanos_total: RuntimeGaugeSlots::new(slot_count),",
            "aof_fsync_latency_buckets: std::array::from_fn(|_| RuntimeGaugeSlots::new(slot_count)),",
            "maintenance_nanos_total: ShardedCounter::new(slot_count),",
            "metrics_flush_nanos_total: ShardedCounter::new(slot_count),",
            "completion_nanos_max: RuntimeMaxSlots::new(slot_count),",
            "close_drain_nanos_max: RuntimeMaxSlots::new(slot_count),",
            "active_expiry_nanos_max: RuntimeMaxSlots::new(slot_count),",
            "aof_append_nanos_max: RuntimeMaxSlots::new(slot_count),",
            "aof_fsync_nanos_max: RuntimeMaxSlots::new(slot_count),",
            "aof_backpressure_nanos_max: RuntimeMaxSlots::new(slot_count),",
            "aof_fsync_latency_nanos_max: RuntimeMaxSlots::new(slot_count),",
            "maintenance_nanos_max: RuntimeMaxSlots::new(slot_count),",
            "metrics_flush_nanos_max: RuntimeMaxSlots::new(slot_count),",
        ];
        for initializer in profile_initializers {
            let token =
                format!("#[cfg(feature = \"profile-telemetry\")]\n            {initializer}");
            assert_contains_token(
                metrics,
                "runtime profile accumulator initialization",
                &token,
            );
        }

        let profile_recorders = [
            (
                "record_completion_nanos",
                "completion_nanos_total",
                "completion_nanos_max",
            ),
            (
                "record_close_drain_nanos",
                "close_drain_nanos_total",
                "close_drain_nanos_max",
            ),
            (
                "record_active_expiry_nanos",
                "active_expiry_nanos_total",
                "active_expiry_nanos_max",
            ),
            (
                "record_aof_append_nanos",
                "aof_append_nanos_total",
                "aof_append_nanos_max",
            ),
            (
                "record_aof_fsync_nanos",
                "aof_fsync_nanos_total",
                "aof_fsync_nanos_max",
            ),
            (
                "record_maintenance_nanos",
                "maintenance_nanos_total",
                "maintenance_nanos_max",
            ),
            (
                "record_metrics_flush_nanos",
                "metrics_flush_nanos_total",
                "metrics_flush_nanos_max",
            ),
        ];
        for (function, total_field, max_field) in profile_recorders {
            let body = production_function_after(
                "keyspace/metrics.rs",
                metrics,
                "impl RuntimeMetrics {",
                function,
            );
            let context = format!("runtime profile accumulator recorder `{function}`");
            assert_ordered_tokens(
                body,
                &context,
                &[
                    "#[cfg(feature = \"profile-telemetry\")]",
                    "if nanos == 0",
                    total_field,
                    max_field,
                    "#[cfg(not(feature = \"profile-telemetry\"))]",
                    "let _ = (slot, nanos);",
                ],
            );
        }

        let aof_publish = production_function_after(
            "keyspace/metrics.rs",
            metrics,
            "impl RuntimeMetrics {",
            "publish_aof_telemetry",
        );
        assert_ordered_tokens(
            aof_publish,
            "runtime AOF profile telemetry publication",
            &[
                "self.aof_backpressure_events",
                "#[cfg(feature = \"profile-telemetry\")]",
                "self.aof_backpressure_nanos_total",
                "self.aof_backpressure_nanos_max",
                "self.aof_last_appended_lsn",
                "#[cfg(feature = \"profile-telemetry\")]",
                "self.aof_fsync_latency_nanos_total",
                "self.aof_fsync_latency_nanos_max",
                "self.aof_fsync_latency_buckets",
            ],
        );

        let snapshot = production_function_after(
            "keyspace/metrics.rs",
            metrics,
            "impl RuntimeMetrics {",
            "snapshot",
        );
        let profile_snapshot_fields = [
            (
                "completion_nanos_total",
                "self.completion_nanos_total.total()",
                "0",
            ),
            (
                "completion_nanos_max",
                "self.completion_nanos_max.max()",
                "0",
            ),
            (
                "close_drain_nanos_total",
                "self.close_drain_nanos_total.total()",
                "0",
            ),
            (
                "close_drain_nanos_max",
                "self.close_drain_nanos_max.max()",
                "0",
            ),
            (
                "active_expiry_nanos_total",
                "self.active_expiry_nanos_total.total()",
                "0",
            ),
            (
                "active_expiry_nanos_max",
                "self.active_expiry_nanos_max.max()",
                "0",
            ),
            (
                "aof_append_nanos_total",
                "self.aof_append_nanos_total.total()",
                "0",
            ),
            (
                "aof_append_nanos_max",
                "self.aof_append_nanos_max.max()",
                "0",
            ),
            (
                "aof_fsync_nanos_total",
                "self.aof_fsync_nanos_total.total()",
                "0",
            ),
            ("aof_fsync_nanos_max", "self.aof_fsync_nanos_max.max()", "0"),
            (
                "aof_backpressure_nanos_total",
                "self.aof_backpressure_nanos_total.sum()",
                "0",
            ),
            (
                "aof_backpressure_nanos_max",
                "self.aof_backpressure_nanos_max.max()",
                "0",
            ),
            (
                "aof_fsync_latency_nanos_total",
                "self.aof_fsync_latency_nanos_total.sum()",
                "0",
            ),
            (
                "aof_fsync_latency_nanos_max",
                "self.aof_fsync_latency_nanos_max.max()",
                "0",
            ),
            (
                "aof_fsync_latency_buckets",
                "std::array::from_fn(|idx| self.aof_fsync_latency_buckets[idx].sum())",
                "[0; RUNTIME_AOF_FSYNC_LATENCY_BUCKETS]",
            ),
            (
                "maintenance_nanos_total",
                "self.maintenance_nanos_total.total()",
                "0",
            ),
            (
                "maintenance_nanos_max",
                "self.maintenance_nanos_max.max()",
                "0",
            ),
            (
                "metrics_flush_nanos_total",
                "self.metrics_flush_nanos_total.total()",
                "0",
            ),
            (
                "metrics_flush_nanos_max",
                "self.metrics_flush_nanos_max.max()",
                "0",
            ),
        ];
        for (field, profile_value, release_value) in profile_snapshot_fields {
            let field_start = format!("{field}: {{");
            let context = format!("runtime profile snapshot field `{field}`");
            assert_ordered_tokens(
                snapshot,
                &context,
                &[
                    field_start.as_str(),
                    "#[cfg(feature = \"profile-telemetry\")]",
                    profile_value,
                    "#[cfg(not(feature = \"profile-telemetry\"))]",
                    release_value,
                ],
            );
        }
    }

    // ── INFO ──

    #[test]
    fn info_all_contains_sections() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"INFO"]);
        assert_bulk_contains(&r, b"# Server");
        assert_bulk_contains(&r, b"# Clients");
        assert_bulk_contains(&r, b"# Memory");
        assert_bulk_contains(&r, b"# Runtime");
        assert_bulk_contains(&r, b"# Keyspace");
    }

    #[test]
    fn info_runtime_section_contains_runtime_fields() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"INFO", b"runtime"]);
        assert_bulk_contains(&r, b"# Runtime");
        assert_bulk_contains(&r, b"runtime_telemetry_mode:minimal");
        assert_bulk_contains(&r, b"runtime_profile_timers_available:0");
        assert_bulk_contains(&r, b"runtime_local_flush_metrics_available:0");
        assert_bulk_contains(&r, b"runtime_local_flush_sample_rate:");
        assert_bulk_contains(&r, b"runtime_metrics_flush_interval_ms:");
        assert_bulk_contains(&r, b"backend_requested:");
        assert_bulk_contains(&r, b"backend_effective:");
        assert_bulk_contains(&r, b"backend_fixed_buffers_registered:");
        assert_bulk_contains(&r, b"backend_sqpoll:");
        assert_bulk_contains(&r, b"backend_multishot_accept:");
        assert_bulk_contains(&r, b"backend_accept4:");
        assert_bulk_contains(&r, b"backend_close_opcode:");
        assert_bulk_contains(&r, b"backend_cancel_support:");
        assert_bulk_contains(&r, b"backend_requested_ring_size:");
        assert_bulk_contains(&r, b"backend_effective_ring_size:");
        assert_bulk_contains(&r, b"backend_submit_syscalls:");
        assert_bulk_contains(&r, b"backend_sq_occupancy_max:");
        assert_bulk_contains(&r, b"backend_sq_capacity:");
        assert_bulk_contains(&r, b"backend_cq_occupancy_max:");
        assert_bulk_contains(&r, b"backend_cq_capacity:");
        assert_bulk_contains(&r, b"backend_cq_overflows:");
        assert_bulk_contains(&r, b"backend_completions_per_submit_syscall_x1000:");
        assert_bulk_contains(&r, b"backend_sq_pressure_events:");
        assert_bulk_contains(&r, b"backend_cq_pressure_events:");
        assert_bulk_contains(&r, b"reactor_loop_iterations:");
        assert_bulk_contains(&r, b"reactor_submit_sq_full_retries:");
        assert_bulk_contains(&r, b"reactor_submit_failures:");
        assert_bulk_contains(&r, b"reactor_completion_budget_exhaustions:");
        assert_bulk_contains(&r, b"reactor_command_budget_exhaustions:");
        assert_bulk_contains(&r, b"reactor_accept_budget_exhaustions:");
        assert_bulk_contains(&r, b"reactor_writev_budget_exhaustions:");
        assert_bulk_contains(&r, b"reactor_maintenance_budget_exhaustions:");
        assert_bulk_contains(&r, b"reactor_parser_resumes:");
        assert_bulk_contains(&r, b"reactor_completion_batch_avg:");
        assert_bulk_contains(&r, b"reactor_writev_chunks:");
        assert_bulk_contains(&r, b"reactor_queued_response_bytes_max:");
        assert_bulk_contains(&r, b"reactor_client_retained_bytes:");
        assert_bulk_contains(&r, b"reactor_response_cap_exceeded:");
        assert_bulk_contains(&r, b"reactor_multi_queue_bytes_cap_exceeded:");
        assert_bulk_contains(&r, b"reactor_watch_cap_exceeded:");
        assert_bulk_contains(&r, b"reactor_overload_accept_throttled:");
        assert_bulk_contains(&r, b"reactor_overload_read_disabled:");
        assert_bulk_contains(&r, b"reactor_overload_command_deferred:");
        assert_bulk_contains(&r, b"reactor_overload_pending_response_bytes_peak:");
        assert_bulk_contains(&r, b"reactor_overload_maintenance_debt_peak:");
        assert_bulk_contains(&r, b"reactor_close_drain_nanos_max:");
        assert_bulk_contains(&r, b"reactor_aof_fsync_nanos_total:");
        assert_bulk_contains(&r, b"reactor_aof_pending_bytes:");
        assert_bulk_contains(&r, b"reactor_aof_fsync_requested:");
        assert_bulk_contains(&r, b"reactor_aof_fsync_completed:");
        assert_bulk_contains(&r, b"reactor_aof_fsync_failed:");
        assert_bulk_contains(&r, b"reactor_aof_fsync_worker_saturation:");
        assert_bulk_contains(&r, b"reactor_aof_last_appended_lsn:");
        assert_bulk_contains(&r, b"reactor_aof_last_durable_lsn:");
        assert_bulk_contains(&r, b"reactor_aof_durable_lsn_lag:");
        assert_bulk_contains(&r, b"reactor_aof_fsync_latency_le_1ms:");
        assert_bulk_contains(&r, b"reactor_aof_fsync_latency_gt_100ms:");
        assert_bulk_contains(&r, b"reactor_metrics_flush_nanos_total:");
        assert_bulk_contains(&r, b"eviction_shards_scanned:");
        assert_bulk_contains(&r, b"eviction_nanos_total:");
    }

    #[test]
    fn info_runtime_fields_are_documented_in_metric_catalogs() {
        let docs = concat!(
            include_str!("../../docs/metrics.md"),
            "\n",
            include_str!("../../docs/profiling.md"),
            "\n",
            include_str!("../../../vortex-io/docs/metrics.md"),
            "\n",
            include_str!("../../../vortex-io/docs/profiling.md"),
            "\n",
            include_str!("../../../vortex-persist/docs/metrics.md"),
            "\n",
            include_str!("../../../vortex-persist/docs/profiling.md")
        );
        let h = TestHarness::new();
        let r = exec(&h, &[b"INFO", b"runtime"]);
        let info = bulk_string_text(&r);
        let mut missing = Vec::new();

        for line in info.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((field, _value)) = line.split_once(':') else {
                continue;
            };
            let marker = format!("`{field}`");
            if !docs.contains(&marker) {
                missing.push(field.to_owned());
            }
        }

        assert!(
            missing.is_empty(),
            "INFO runtime fields missing from metric catalogs: {missing:?}"
        );
    }

    #[derive(Debug)]
    struct MetricCatalogRow<'a> {
        file: &'a str,
        line: usize,
        metric: &'a str,
        group: &'a str,
        modes: &'a str,
        hot_path: &'a str,
        update_path: &'a str,
        reason: &'a str,
    }

    fn release_metric_catalog_rows<'a>(file: &'a str, docs: &'a str) -> Vec<MetricCatalogRow<'a>> {
        let mut in_catalog = false;
        let mut rows = Vec::new();

        for (index, line) in docs.lines().enumerate() {
            let line_number = index + 1;
            if line == "## Metric Catalog" {
                in_catalog = true;
                continue;
            }
            if in_catalog && line.starts_with("## ") {
                break;
            }
            if !in_catalog || !line.starts_with('|') {
                continue;
            }
            if line.contains("---") || line.contains("Metric | Group") {
                continue;
            }

            let columns: Vec<_> = line.trim_matches('|').split('|').map(str::trim).collect();
            assert_eq!(
                columns.len(),
                6,
                "{file}:{line_number}: metric catalog rows must keep the six-column schema"
            );

            rows.push(MetricCatalogRow {
                file,
                line: line_number,
                metric: columns[0],
                group: columns[1],
                modes: columns[2],
                hot_path: columns[3],
                update_path: columns[4],
                reason: columns[5],
            });
        }

        assert!(
            !rows.is_empty(),
            "{file}: release metric catalog must contain rows"
        );
        rows
    }

    fn supported_modes_are_explicit(modes: &str) -> bool {
        modes.split(';').all(|mode_set| {
            let Some((binary, modes)) = mode_set.trim().split_once(':') else {
                return false;
            };
            matches!(binary, "release" | "profiling")
                && modes.split('/').all(|mode| {
                    matches!(
                        mode.trim(),
                        "minimal" | "standard" | "profile" | "minimal/standard"
                    )
                })
        })
    }

    fn hot_path_cost_is_classified(row: &MetricCatalogRow<'_>) -> bool {
        let hot_path = row.hot_path.to_ascii_lowercase();
        if hot_path == "yes" || hot_path == "unknown" || hot_path == "tbd" {
            return false;
        }

        let cost = format!(
            "{} {}",
            row.hot_path.to_ascii_lowercase(),
            row.update_path.to_ascii_lowercase()
        );
        [
            "cold",
            "startup",
            "static",
            "derived",
            "disabled in minimal",
            "sampled",
            "exact in profile",
            "profile",
            "not compiled",
            "error path",
            "only when",
            "only on",
            "relaxed counter",
            "sharded counter",
            "counter",
            "timestamp",
            "instant",
            "max update",
            "bucket atomic",
            "atomic update",
            "state",
            "gauge",
            "snapshot",
            "flush",
            "boundary",
            "budget exhausted",
            "append path",
            "fsync",
            "backpressure",
            "max publication",
        ]
        .iter()
        .any(|needle| cost.contains(needle))
    }

    fn profiling_modes_are_explicit(modes: &str) -> bool {
        modes.contains("profiling:")
            && modes.contains("release:not compiled")
            && !modes.contains("release:minimal")
            && !modes.contains("release:standard")
    }

    #[test]
    fn release_metric_catalogs_keep_schema_and_cost_decisions() {
        let docs = [
            (
                "crates/vortex-engine/docs/metrics.md",
                include_str!("../../docs/metrics.md"),
            ),
            (
                "crates/vortex-io/docs/metrics.md",
                include_str!("../../../vortex-io/docs/metrics.md"),
            ),
            (
                "crates/vortex-persist/docs/metrics.md",
                include_str!("../../../vortex-persist/docs/metrics.md"),
            ),
        ];

        let mut failures = Vec::new();
        for (file, docs) in docs {
            for row in release_metric_catalog_rows(file, docs) {
                let metric_is_quoted = row.metric.starts_with('`') && row.metric.ends_with('`');
                if !metric_is_quoted {
                    failures.push(format!(
                        "{}:{} metric must be backtick-quoted",
                        row.file, row.line
                    ));
                }
                if row.group.is_empty() || row.reason.is_empty() {
                    failures.push(format!(
                        "{}:{} metric group and release reason must be non-empty",
                        row.file, row.line
                    ));
                }
                if !supported_modes_are_explicit(row.modes) {
                    failures.push(format!(
                        "{}:{} supported modes must use release:/profiling: minimal/standard/profile vocabulary",
                        row.file, row.line
                    ));
                }
                if !hot_path_cost_is_classified(&row) {
                    failures.push(format!(
                        "{}:{} hot-path cost decision is missing or too vague for {}",
                        row.file, row.line, row.metric
                    ));
                }
            }
        }

        assert!(
            failures.is_empty(),
            "release metric catalog schema/cost failures: {failures:#?}"
        );
    }

    #[test]
    fn profiling_metric_catalogs_stay_profile_only() {
        let docs = [
            (
                "crates/vortex-engine/docs/profiling.md",
                include_str!("../../docs/profiling.md"),
            ),
            (
                "crates/vortex-io/docs/profiling.md",
                include_str!("../../../vortex-io/docs/profiling.md"),
            ),
            (
                "crates/vortex-persist/docs/profiling.md",
                include_str!("../../../vortex-persist/docs/profiling.md"),
            ),
        ];

        let mut failures = Vec::new();
        for (file, docs) in docs {
            for row in release_metric_catalog_rows(file, docs) {
                if !(row.metric.starts_with('`') && row.metric.ends_with('`')) {
                    failures.push(format!(
                        "{}:{} profile metric must be backtick-quoted",
                        row.file, row.line
                    ));
                }
                if !profiling_modes_are_explicit(row.modes) {
                    failures.push(format!(
                        "{}:{} profile metric must stay profiling-only and release:not compiled",
                        row.file, row.line
                    ));
                }
                if !hot_path_cost_is_classified(&row) {
                    failures.push(format!(
                        "{}:{} profile metric cost is missing or too vague for {}",
                        row.file, row.line, row.metric
                    ));
                }
                if row.reason.is_empty() {
                    failures.push(format!(
                        "{}:{} profile metric use case must be non-empty",
                        row.file, row.line
                    ));
                }
            }
        }

        assert!(
            failures.is_empty(),
            "profiling metric catalog schema/cost failures: {failures:#?}"
        );
    }

    #[test]
    fn info_memory_section_contains_attribution_fields() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"INFO", b"memory"]);
        assert_bulk_contains(&r, b"# Memory");
        assert_bulk_contains(&r, b"memory_attribution_engine_scope:engine_only");
        assert_bulk_contains(
            &r,
            b"memory_attribution_full_server_scope:process_rss_plus_io",
        );
        assert_bulk_contains(&r, b"engine_logical_dataset_bytes:");
        assert_bulk_contains(&r, b"engine_table_allocated_bytes:");
        assert_bulk_contains(&r, b"engine_table_total_slots:");
        assert_bulk_contains(&r, b"engine_capacity_slack_slots:");
        assert_bulk_contains(&r, b"engine_tombstone_slots:");
        assert_bulk_contains(&r, b"engine_load_factor:");
        assert_bulk_contains(&r, b"engine_bytes_per_live_key:");
        assert_bulk_contains(&r, b"io_fixed_buffer_reserved_bytes:");
        assert_bulk_contains(&r, b"io_fixed_buffer_committed_bytes:");
        assert_bulk_contains(&r, b"per_connection_state_bytes:");
        assert_bulk_contains(&r, b"client_retained_bytes:");
        assert_bulk_contains(&r, b"client_retained_bytes_peak:");
        assert_bulk_contains(&r, b"full_server_process_rss_bytes:");
    }

    #[test]
    fn info_server_section() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"INFO", b"server"]);
        assert_bulk_contains(&r, b"# Server");
        assert_bulk_contains(&r, b"vortex_version:");
        assert_bulk_contains(&r, b"redis_version:");
    }

    #[test]
    fn info_keyspace_with_keys() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("k1"), VortexValue::from_bytes(b"v1"));
        h.set(VortexKey::from("k2"), VortexValue::from_bytes(b"v2"));
        let r = exec(&h, &[b"INFO", b"keyspace"]);
        assert_bulk_contains(&r, b"db0:keys=2");
    }

    #[test]
    fn info_keyspace_excludes_expired_keys() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};

        let deadline = NS_PER_SEC;
        let now = deadline + 1;
        h.set(VortexKey::from("live"), VortexValue::from_bytes(b"v1"));
        h.set_with_ttl(
            VortexKey::from("expired"),
            VortexValue::from_bytes(b"v2"),
            deadline,
        );

        let r = exec_at(&h, &[b"INFO", b"keyspace"], now);
        assert_bulk_contains(&r, b"db0:keys=1,expires=0");
    }

    // ── CLIENT / HELLO setup compatibility ──

    #[test]
    fn hello_resp2_returns_handshake_metadata() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"HELLO", b"2"]);

        match &r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert!(array_contains_bulk_pair(arr, b"server", b"vortex"));
                assert!(array_contains_bulk_pair(
                    arr,
                    b"version",
                    env!("CARGO_PKG_VERSION").as_bytes()
                ));
                assert!(array_contains_integer_pair(arr, b"proto", 2));
                assert!(array_contains_bulk_pair(arr, b"mode", b"standalone"));
                assert!(array_contains_bulk_pair(arr, b"role", b"master"));
            }
            other => panic!("expected HELLO array, got {other:?}"),
        }
    }

    #[test]
    fn hello_resp2_accepts_setname_as_noop() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"HELLO", b"2", b"SETNAME", b"client-a"]);

        match &r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert!(array_contains_integer_pair(arr, b"proto", 2));
            }
            other => panic!("expected HELLO array, got {other:?}"),
        }
    }

    #[test]
    fn hello_resp3_is_rejected_explicitly() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"HELLO", b"3"]);
        assert_static(&r, ERR_HELLO_RESP3_UNSUPPORTED);
    }

    #[test]
    fn client_setinfo_is_alpha_noop() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"CLIENT", b"SETINFO", b"LIB-NAME", b"redis-py"]);
        assert_static(&r, RESP_OK);

        let r = exec(&h, &[b"CLIENT", b"SETINFO", b"LIB-VER", b"8.0.0"]);
        assert_static(&r, RESP_OK);
    }

    #[test]
    fn unsupported_client_subcommands_remain_closed() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"CLIENT", b"ID"]);
        assert_static(&r, ERR_CLIENT_UNSUPPORTED);
    }

    // ── COMMAND ──

    #[test]
    fn command_count_returns_positive() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"COMMAND", b"COUNT"]);
        match &r {
            CmdResult::Resp(RespFrame::Integer(n)) => {
                assert_eq!(*n, SUPPORTED_COMMANDS.len() as i64);
            }
            other => panic!("expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn command_all_omits_unsupported_alpha_commands() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"COMMAND"]);

        assert!(array_contains_command_name(&r, b"GET"));
        assert!(array_contains_command_name(&r, b"CONFIG"));
        assert!(array_contains_command_name(&r, b"HELLO"));
        assert!(array_contains_command_name(&r, b"CLIENT"));
        assert!(!array_contains_command_name(&r, b"HSET"));
        assert!(!array_contains_command_name(&r, b"BGREWRITEAOF"));
    }

    #[test]
    fn command_info_known() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"COMMAND", b"INFO", b"GET"]);
        // Should return a 1-element array with GET metadata.
        assert_array_len(&r, 1);
    }

    #[test]
    fn command_info_unknown() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"COMMAND", b"INFO", b"NONEXISTENT"]);
        // Should return array with Null entry.
        assert_array_len(&r, 1);
    }

    #[test]
    fn command_info_unsupported_returns_null() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"COMMAND", b"INFO", b"HSET", b"BGREWRITEAOF"]);
        match &r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert!(matches!(arr.first(), Some(RespFrame::Null)));
                assert!(matches!(arr.get(1), Some(RespFrame::Null)));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn command_info_invalid_utf8_returns_null() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"COMMAND", b"INFO", b"\xffGET"]);
        match &r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                assert!(matches!(arr.first(), Some(RespFrame::Null)));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    // ── TIME ──

    #[test]
    fn time_returns_two_element_array() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"TIME"]);
        assert_array_len(&r, 2);
    }

    #[test]
    fn time_values_are_numeric() {
        let h = TestHarness::new();
        let r = exec(&h, &[b"TIME"]);
        match &r {
            CmdResult::Resp(RespFrame::Array(Some(arr))) => {
                // Both should be bulk strings containing numeric values.
                for frame in arr {
                    match frame {
                        RespFrame::BulkString(Some(b)) => {
                            let s = std::str::from_utf8(b).unwrap();
                            assert!(s.parse::<u64>().is_ok(), "not numeric: {s}");
                        }
                        other => panic!("expected BulkString in TIME array, got {other:?}"),
                    }
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }
}
