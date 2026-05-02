//! Server command handlers.
//!
//! DBSIZE, FLUSHDB, FLUSHALL, INFO, COMMAND, and TIME.

use vortex_proto::{CommandFlags, CommandMeta, FrameRef, RespFrame};

use super::{
    CmdResult, CommandArgs, ExecutedCommand, NS_PER_SEC, RESP_EMPTY_ARRAY, RESP_OK,
    resolve_unix_time_now_nanos,
};
use crate::{ConcurrentKeyspace, keyspace::RuntimeMetricsSnapshot};

/// Alpha-visible command set. This excludes commands that are still stubs,
/// unsupported, or intentionally disabled for the alpha release.
const SUPPORTED_COMMANDS: &[&str] = &[
    // Connection
    "PING",
    "ECHO",
    "QUIT",
    "SELECT",
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
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> ExecutedCommand {
    ExecutedCommand::with_aof_lsn(CmdResult::Static(RESP_OK), keyspace.cmd_flush_all())
}

/// FLUSHALL [ASYNC|SYNC]
///
/// Same as FLUSHDB for the shared-keyspace alpha.
#[inline]
pub fn cmd_flushall(
    keyspace: &ConcurrentKeyspace,
    _frame: &FrameRef<'_>,
    _now_nanos: u64,
) -> ExecutedCommand {
    ExecutedCommand::with_aof_lsn(CmdResult::Static(RESP_OK), keyspace.cmd_flush_all())
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
                    return rss_pages * page_size();
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

fn write_info_runtime(buf: &mut Vec<u8>, runtime: RuntimeMetricsSnapshot) {
    buf.extend_from_slice(b"# Runtime\r\n");
    buf.extend_from_slice(b"runtime_reactor_slots:");
    itoa_append(buf, runtime.reactor_slots as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_loop_iterations:");
    itoa_append(buf, runtime.loop_iterations as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_accept_eagain_rearms:");
    itoa_append(buf, runtime.accept_eagain_rearms as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_submit_sq_full_retries:");
    itoa_append(buf, runtime.submit_sq_full_retries as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_submit_failures:");
    itoa_append(buf, runtime.submit_failures as i64);
    buf.extend_from_slice(b"\r\n");
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
    buf.extend_from_slice(b"reactor_active_expiry_runs:");
    itoa_append(buf, runtime.active_expiry_runs as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_active_expiry_sampled:");
    itoa_append(buf, runtime.active_expiry_sampled as i64);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(b"reactor_active_expiry_expired:");
    itoa_append(buf, runtime.active_expiry_expired as i64);
    buf.extend_from_slice(b"\r\n");
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
    buf.extend_from_slice(b"\r\n\r\n");
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
    fn flushall_empties_shard() {
        let h = TestHarness::new();
        use vortex_common::{VortexKey, VortexValue};
        h.set(VortexKey::from("x"), VortexValue::from_bytes(b"val"));
        let r = exec(&h, &[b"FLUSHALL"]);
        assert_static(&r, RESP_OK);
        assert_eq!(h.len(), 0);
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
        assert_bulk_contains(&r, b"reactor_loop_iterations:");
        assert_bulk_contains(&r, b"reactor_submit_sq_full_retries:");
        assert_bulk_contains(&r, b"reactor_submit_failures:");
        assert_bulk_contains(&r, b"reactor_completion_batch_avg:");
        assert_bulk_contains(&r, b"eviction_shards_scanned:");
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
