use std::fmt::Write as _;
use std::fs;
use std::hint::black_box;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use tikv_jemallocator::Jemalloc;
use vortex_common::{MAX_INLINE_VALUE_LEN, VortexKey, VortexValue};
use vortex_engine::commands::{CommandClock, execute_command};
use vortex_engine::keyspace::LockProfileSnapshot;
use vortex_engine::{ConcurrentKeyspace, EvictionPolicy};
use vortex_proto::RespTape;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DEFAULT_DURATION_SECS: u64 = 5;
const DEFAULT_WORKERS: usize = 8;
const DEFAULT_SHARDS: usize = 64;
const DEFAULT_KEYS: usize = 65_536;
const DEFAULT_VALUE_SIZE: usize = 16;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Workload {
    LfuRead,
    AofWrite,
    MaxmemoryReserve,
    RuntimeExpiry,
    WatchCounters,
}

#[derive(Debug, Parser)]
#[command(about = "Concurrent engine probe for global atomic contention profiling")]
struct Args {
    #[arg(long, value_enum)]
    workload: Workload,

    #[arg(long, default_value_t = DEFAULT_WORKERS)]
    workers: usize,

    #[arg(long, default_value_t = DEFAULT_SHARDS)]
    shards: usize,

    #[arg(long, default_value_t = DEFAULT_KEYS)]
    keys: usize,

    #[arg(long, default_value_t = DEFAULT_DURATION_SECS)]
    duration: u64,

    #[arg(long, default_value_t = DEFAULT_VALUE_SIZE)]
    value_size: usize,

    #[arg(long)]
    json: Option<PathBuf>,

    #[arg(long)]
    lock_profile: bool,
}

struct CommandFixture {
    name: &'static [u8],
    tape: RespTape,
}

impl CommandFixture {
    fn new(name: &'static [u8], parts: &[&[u8]]) -> Result<Self, String> {
        let wire = make_resp(parts);
        let tape = RespTape::parse_pipeline(&wire)
            .map_err(|error| format!("failed to parse generated command: {error:?}"))?;
        Ok(Self { name, tape })
    }
}

#[derive(Default)]
struct WorkerResult {
    operations: u64,
    errors: u64,
}

struct ProbeSummary {
    workload: Workload,
    workers: usize,
    shards: usize,
    keys: usize,
    duration_seconds: f64,
    operations: u64,
    errors: u64,
    throughput_ops_per_second: f64,
    live_keys: usize,
    expiring_keys: usize,
    memory_used_bytes: usize,
    runtime_active_expiry_runs: u64,
    runtime_active_expiry_sampled: u64,
    runtime_active_expiry_expired: u64,
    eviction_admissions: u64,
    eviction_slots_sampled: u64,
    lock_profile: Option<LockProfileSnapshot>,
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    validate_args(&args)?;

    let summary = run_probe(&args)?;
    let json = summary.to_json();
    if let Some(path) = &args.json {
        fs::write(path, json.as_bytes())
            .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    }
    print!("{json}");
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.workers == 0 {
        return Err("--workers must be greater than zero".to_string());
    }
    if args.shards == 0 || !args.shards.is_power_of_two() {
        return Err("--shards must be a non-zero power of two".to_string());
    }
    if args.keys == 0 {
        return Err("--keys must be greater than zero".to_string());
    }
    if args.duration == 0 {
        return Err("--duration must be greater than zero".to_string());
    }
    Ok(())
}

fn run_probe(args: &Args) -> Result<ProbeSummary, String> {
    let keys_per_worker = keys_per_worker(args);
    let capacity = args
        .workers
        .checked_mul(keys_per_worker)
        .and_then(|keys| keys.checked_mul(2))
        .ok_or_else(|| "probe capacity overflow".to_string())?;
    let keyspace = Arc::new(ConcurrentKeyspace::with_capacity(args.shards, capacity));
    setup_workload(args, &keyspace, keys_per_worker)?;
    let lock_profile_enabled =
        args.lock_profile || std::env::var_os("VORTEX_LOCK_PROFILE").is_some();
    if lock_profile_enabled {
        keyspace.set_lock_profile_enabled(true);
        keyspace.reset_lock_profile();
    }

    let barrier = Arc::new(Barrier::new(args.workers + 1));
    let duration = Duration::from_secs(args.duration);
    let mut handles = Vec::with_capacity(args.workers);

    for worker_id in 0..args.workers {
        let keyspace = Arc::clone(&keyspace);
        let barrier = Arc::clone(&barrier);
        let workload = args.workload;
        let value_size = args.value_size;
        handles.push(thread::spawn(move || {
            run_worker(
                keyspace,
                barrier,
                workload,
                worker_id,
                keys_per_worker,
                value_size,
                duration,
            )
        }));
    }

    barrier.wait();
    let started = Instant::now();

    let mut result = WorkerResult::default();
    for handle in handles {
        let worker = handle
            .join()
            .map_err(|_| "atomic contention worker panicked".to_string())??;
        result.operations = result.operations.saturating_add(worker.operations);
        result.errors = result.errors.saturating_add(worker.errors);
    }

    let elapsed = started.elapsed().as_secs_f64().max(f64::EPSILON);
    let runtime = keyspace.runtime_metrics();
    let eviction = keyspace.eviction_metrics();

    Ok(ProbeSummary {
        workload: args.workload,
        workers: args.workers,
        shards: args.shards,
        keys: args.keys,
        duration_seconds: elapsed,
        operations: result.operations,
        errors: result.errors,
        throughput_ops_per_second: result.operations as f64 / elapsed,
        live_keys: keyspace.engine_memory_attribution().live_keys,
        expiring_keys: keyspace.approx_expiring_keys(),
        memory_used_bytes: keyspace.memory_used(),
        runtime_active_expiry_runs: runtime.active_expiry_runs,
        runtime_active_expiry_sampled: runtime.active_expiry_sampled,
        runtime_active_expiry_expired: runtime.active_expiry_expired,
        eviction_admissions: eviction.admissions,
        eviction_slots_sampled: eviction.slots_sampled,
        lock_profile: lock_profile_enabled.then(|| keyspace.lock_profile_snapshot()),
    })
}

fn setup_workload(
    args: &Args,
    keyspace: &ConcurrentKeyspace,
    keys_per_worker: usize,
) -> Result<(), String> {
    match args.workload {
        Workload::LfuRead => {
            prefill_worker_keys(keyspace, args.workers, keys_per_worker, args.value_size);
            keyspace.configure_eviction(usize::MAX / 4, EvictionPolicy::AllKeysLfu);
        }
        Workload::AofWrite => {
            prefill_worker_keys(keyspace, args.workers, keys_per_worker, args.value_size);
            keyspace.enable_aof_recording();
        }
        Workload::MaxmemoryReserve => {
            prefill_worker_keys(keyspace, args.workers, keys_per_worker, args.value_size);
            keyspace.configure_eviction(
                keyspace.memory_used().saturating_add(usize::MAX / 8),
                EvictionPolicy::NoEviction,
            );
        }
        Workload::RuntimeExpiry => {
            prefill_worker_keys(keyspace, args.workers, keys_per_worker, args.value_size);
        }
        Workload::WatchCounters => {}
    }
    Ok(())
}

fn run_worker(
    keyspace: Arc<ConcurrentKeyspace>,
    barrier: Arc<Barrier>,
    workload: Workload,
    worker_id: usize,
    keys_per_worker: usize,
    value_size: usize,
    duration: Duration,
) -> Result<WorkerResult, String> {
    let fixtures = worker_fixtures(workload, worker_id, keys_per_worker, value_size)?;
    let mut result = WorkerResult::default();

    barrier.wait();
    let started = Instant::now();
    let mut index = 0usize;
    while started.elapsed() < duration {
        match workload {
            Workload::LfuRead | Workload::AofWrite => {
                let fixture = &fixtures[index % fixtures.len()];
                result.operations += 1;
                result.errors += u64::from(!execute_fixture(&keyspace, fixture));
            }
            Workload::MaxmemoryReserve => {
                let delete = &fixtures[(index * 2) % fixtures.len()];
                let set = &fixtures[(index * 2 + 1) % fixtures.len()];
                result.operations += 2;
                result.errors += u64::from(!execute_fixture(&keyspace, delete));
                result.errors += u64::from(!execute_fixture(&keyspace, set));
            }
            Workload::RuntimeExpiry => {
                let expire = &fixtures[(index * 2) % fixtures.len()];
                let persist = &fixtures[(index * 2 + 1) % fixtures.len()];
                let reactor_id = worker_id;
                keyspace.record_reactor_loop_iteration(reactor_id);
                keyspace.record_reactor_active_expiry(reactor_id, 1, 0);
                result.operations += 3;
                result.errors += u64::from(!execute_fixture(&keyspace, expire));
                result.errors += u64::from(!execute_fixture(&keyspace, persist));
            }
            Workload::WatchCounters => {
                let key = watch_key(worker_id, index % keys_per_worker);
                let watched = keyspace.watch_key(VortexKey::from_bytes(key.as_slice()));
                let epoch = keyspace.current_watch_epoch();
                let changed = keyspace.watched_keys_changed(epoch, std::slice::from_ref(&watched));
                keyspace.unwatch_keys(std::iter::once(watched));
                black_box(changed);
                result.operations += 1;
            }
        }
        index = index.wrapping_add(1);
    }

    Ok(result)
}

fn worker_fixtures(
    workload: Workload,
    worker_id: usize,
    keys_per_worker: usize,
    value_size: usize,
) -> Result<Vec<CommandFixture>, String> {
    let value_size = value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let mut fixtures = match workload {
        Workload::MaxmemoryReserve | Workload::RuntimeExpiry => {
            Vec::with_capacity(keys_per_worker * 2)
        }
        Workload::WatchCounters => Vec::new(),
        _ => Vec::with_capacity(keys_per_worker),
    };

    for index in 0..keys_per_worker {
        let key = data_key(worker_id, index);
        let value = value_bytes(worker_id, index, value_size);
        match workload {
            Workload::LfuRead => {
                fixtures.push(CommandFixture::new(b"GET", &[b"GET", key.as_slice()])?);
            }
            Workload::AofWrite => {
                fixtures.push(CommandFixture::new(
                    b"SET",
                    &[b"SET", key.as_slice(), value.as_slice()],
                )?);
            }
            Workload::MaxmemoryReserve => {
                fixtures.push(CommandFixture::new(b"DEL", &[b"DEL", key.as_slice()])?);
                fixtures.push(CommandFixture::new(
                    b"SET",
                    &[b"SET", key.as_slice(), value.as_slice()],
                )?);
            }
            Workload::RuntimeExpiry => {
                fixtures.push(CommandFixture::new(
                    b"PEXPIRE",
                    &[b"PEXPIRE", key.as_slice(), b"60000"],
                )?);
                fixtures.push(CommandFixture::new(
                    b"PERSIST",
                    &[b"PERSIST", key.as_slice()],
                )?);
            }
            Workload::WatchCounters => {}
        }
    }

    Ok(fixtures)
}

fn execute_fixture(keyspace: &ConcurrentKeyspace, fixture: &CommandFixture) -> bool {
    let Some(frame) = fixture.tape.iter().next() else {
        return false;
    };
    let Some(executed) = execute_command(keyspace, fixture.name, &frame, CommandClock::from(0u64))
    else {
        return false;
    };
    !executed.response.is_error()
}

fn prefill_worker_keys(
    keyspace: &ConcurrentKeyspace,
    workers: usize,
    keys_per_worker: usize,
    value_size: usize,
) {
    let value_size = value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    for worker_id in 0..workers {
        for index in 0..keys_per_worker {
            // SAFETY: This is deterministic single-threaded setup before the
            // measured workload starts. Command-side AOF/WATCH/maxmemory effects
            // are enabled only after setup when the workload requires them.
            unsafe {
                keyspace.benchmark_insert_unchecked(
                    VortexKey::from_bytes(data_key(worker_id, index).as_slice()),
                    VortexValue::from_bytes(value_bytes(worker_id, index, value_size).as_slice()),
                );
            }
        }
    }
}

fn keys_per_worker(args: &Args) -> usize {
    args.keys.div_ceil(args.workers).max(1)
}

fn data_key(worker_id: usize, index: usize) -> Vec<u8> {
    format!("atomic:data:{worker_id:04}:{index:08}").into_bytes()
}

fn watch_key(worker_id: usize, index: usize) -> Vec<u8> {
    format!("atomic:watch:{worker_id:04}:{index:08}").into_bytes()
}

fn value_bytes(worker_id: usize, index: usize, len: usize) -> Vec<u8> {
    let mut value = format!("v:{worker_id:04}:{index:08}").into_bytes();
    value.resize(len, b'x');
    value
}

fn make_resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(parts.len().saturating_mul(32));
    let _ = write!(&mut buf, "*{}\r\n", parts.len());
    for part in parts {
        let _ = write!(&mut buf, "${}\r\n", part.len());
        buf.extend_from_slice(part);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

impl ProbeSummary {
    fn to_json(&self) -> String {
        let mut json = String::with_capacity(1024);
        let _ = writeln!(&mut json, "{{");
        write_json_string(&mut json, "workload", workload_name(self.workload), true);
        write_json_usize(&mut json, "workers", self.workers, true);
        write_json_usize(&mut json, "shards", self.shards, true);
        write_json_usize(&mut json, "keys", self.keys, true);
        write_json_f64(&mut json, "duration_seconds", self.duration_seconds, true);
        write_json_u64(&mut json, "operations", self.operations, true);
        write_json_u64(&mut json, "errors", self.errors, true);
        write_json_f64(
            &mut json,
            "throughput_ops_per_second",
            self.throughput_ops_per_second,
            true,
        );
        write_json_usize(&mut json, "live_keys", self.live_keys, true);
        write_json_usize(&mut json, "expiring_keys", self.expiring_keys, true);
        write_json_usize(&mut json, "memory_used_bytes", self.memory_used_bytes, true);
        write_json_u64(
            &mut json,
            "runtime_active_expiry_runs",
            self.runtime_active_expiry_runs,
            true,
        );
        write_json_u64(
            &mut json,
            "runtime_active_expiry_sampled",
            self.runtime_active_expiry_sampled,
            true,
        );
        write_json_u64(
            &mut json,
            "runtime_active_expiry_expired",
            self.runtime_active_expiry_expired,
            true,
        );
        write_json_u64(
            &mut json,
            "eviction_admissions",
            self.eviction_admissions,
            true,
        );
        write_json_u64(
            &mut json,
            "eviction_slots_sampled",
            self.eviction_slots_sampled,
            self.lock_profile.is_some(),
        );
        if let Some(snapshot) = &self.lock_profile {
            let profile_json = snapshot.to_json();
            let mut lines = profile_json.lines();
            if lines.next().is_some() {
                let _ = writeln!(&mut json, "  \"lock_profile\": {{");
                for line in lines {
                    if line == "}" {
                        break;
                    }
                    let _ = writeln!(&mut json, "  {line}");
                }
                let _ = writeln!(&mut json, "  }}");
            }
        }
        let _ = writeln!(&mut json, "}}");
        json
    }
}

fn workload_name(workload: Workload) -> &'static str {
    match workload {
        Workload::LfuRead => "lfu-read",
        Workload::AofWrite => "aof-write",
        Workload::MaxmemoryReserve => "maxmemory-reserve",
        Workload::RuntimeExpiry => "runtime-expiry",
        Workload::WatchCounters => "watch-counters",
    }
}

fn write_json_string(buf: &mut String, name: &str, value: &str, trailing_comma: bool) {
    let _ = writeln!(
        buf,
        "  \"{}\": \"{}\"{}",
        name,
        value,
        comma(trailing_comma)
    );
}

fn write_json_usize(buf: &mut String, name: &str, value: usize, trailing_comma: bool) {
    let _ = writeln!(buf, "  \"{}\": {}{}", name, value, comma(trailing_comma));
}

fn write_json_u64(buf: &mut String, name: &str, value: u64, trailing_comma: bool) {
    let _ = writeln!(buf, "  \"{}\": {}{}", name, value, comma(trailing_comma));
}

fn write_json_f64(buf: &mut String, name: &str, value: f64, trailing_comma: bool) {
    let _ = writeln!(buf, "  \"{}\": {:.6}{}", name, value, comma(trailing_comma));
}

const fn comma(enabled: bool) -> &'static str {
    if enabled { "," } else { "" }
}
