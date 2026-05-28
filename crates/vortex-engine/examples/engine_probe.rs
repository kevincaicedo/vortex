use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::mem::MaybeUninit;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use tikv_jemallocator::Jemalloc;
use vortex_common::{MAX_INLINE_VALUE_LEN, VortexKey, VortexValue};
use vortex_engine::commands::{CommandClock, NS_PER_MS, execute_command};
use vortex_engine::keyspace::{LockProfileSnapshot, ShardCount};
use vortex_engine::{ConcurrentKeyspace, EvictionPolicy};
use vortex_proto::RespTape;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DEFAULT_MULTI_KEY_WIDTH: usize = 100;
const DEFAULT_LATENCY_SAMPLE_RATE: u64 = 64;
const DEFAULT_LATENCY_MAX_SAMPLES: usize = 20_000;
const DEFAULT_TTL_MS: u64 = 5;
const DEFAULT_ACTIVE_EXPIRY_EFFORT: usize = 256;
const DEFAULT_EVICTION_HEADROOM_BYTES: usize = 1 << 20;
const EVICTION_PREHEAT_READS: usize = 32;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Workload {
    SetInlineInt,
    SetInlineString,
    SetInlineStringCommand,
    SetInlineStringWatched,
    SetInlineStringTtl,
    SetHeapString,
    GetHit,
    GetMiss,
    Mget,
    Mset,
    Msetnx,
    Delete,
    Append,
    Setrange,
    Incrbyfloat,
    ValueMutationMixed,
    ExpirePersist,
    TtlExpire,
    EvictionHeadroom,
    EvictionPressure,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProbeEvictionPolicy {
    AllKeysLru,
    AllKeysLfu,
    NoEviction,
}

impl ProbeEvictionPolicy {
    const fn into_engine(self) -> EvictionPolicy {
        match self {
            Self::AllKeysLru => EvictionPolicy::AllKeysLru,
            Self::AllKeysLfu => EvictionPolicy::AllKeysLfu,
            Self::NoEviction => EvictionPolicy::NoEviction,
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Run engine-only workload probes without vortex-io")]
struct Args {
    #[arg(long, value_enum)]
    workload: Workload,

    #[arg(long, default_value_t = 100_000)]
    keys: usize,

    #[arg(long, default_value_t = 16)]
    value_size: usize,

    #[arg(long, default_value_t = 64)]
    shards: usize,

    #[arg(long)]
    duration: Option<u64>,

    #[arg(long, default_value_t = DEFAULT_MULTI_KEY_WIDTH)]
    multi_key_width: usize,

    #[arg(long, default_value_t = DEFAULT_LATENCY_SAMPLE_RATE)]
    latency_sample_rate: u64,

    #[arg(long, default_value_t = DEFAULT_LATENCY_MAX_SAMPLES)]
    latency_max_samples: usize,

    #[arg(long, default_value_t = DEFAULT_TTL_MS)]
    ttl_ms: u64,

    #[arg(long, default_value_t = DEFAULT_ACTIVE_EXPIRY_EFFORT)]
    active_expiry_effort: usize,

    #[arg(long, value_enum, default_value_t = ProbeEvictionPolicy::AllKeysLru)]
    eviction_policy: ProbeEvictionPolicy,

    #[arg(long, default_value_t = DEFAULT_EVICTION_HEADROOM_BYTES)]
    eviction_headroom_bytes: usize,

    #[arg(long, default_value_t = false)]
    aof_recording: bool,

    #[arg(long, default_value_t = false)]
    lock_profile: bool,

    #[arg(long)]
    json: Option<PathBuf>,
}

struct LatencyRecorder {
    sample_rate: u64,
    max_samples: usize,
    samples_ns: Vec<u64>,
}

impl LatencyRecorder {
    fn new(sample_rate: u64, max_samples: usize) -> Self {
        Self {
            sample_rate: sample_rate.max(1),
            max_samples,
            samples_ns: Vec::with_capacity(max_samples.min(4096)),
        }
    }

    fn record(&mut self, operation_index: u64, started_at: Instant) {
        if self.samples_ns.len() >= self.max_samples {
            return;
        }
        if operation_index % self.sample_rate != 0 {
            return;
        }

        let elapsed = started_at.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.samples_ns.push(elapsed);
    }

    fn summarize(&self) -> LatencySummary {
        if self.samples_ns.is_empty() {
            return LatencySummary::empty();
        }

        let mut sorted = self.samples_ns.clone();
        sorted.sort_unstable();

        LatencySummary {
            sample_count: sorted.len(),
            p50_ns: Some(select_percentile(&sorted, 50)),
            p95_ns: Some(select_percentile(&sorted, 95)),
            p99_ns: Some(select_percentile(&sorted, 99)),
            p999_ns: Some(select_permyriad(&sorted, 9_990)),
        }
    }
}

#[derive(Clone, Copy)]
struct LatencySummary {
    sample_count: usize,
    p50_ns: Option<u64>,
    p95_ns: Option<u64>,
    p99_ns: Option<u64>,
    p999_ns: Option<u64>,
}

impl LatencySummary {
    const fn empty() -> Self {
        Self {
            sample_count: 0,
            p50_ns: None,
            p95_ns: None,
            p99_ns: None,
            p999_ns: None,
        }
    }
}

struct ProbeSummary {
    workload: Workload,
    driver: &'static str,
    operations: u64,
    duration_seconds: f64,
    throughput_ops_per_second: f64,
    keys_requested: usize,
    value_size_bytes: usize,
    shards: usize,
    aof_recording: bool,
    live_keys: usize,
    expiring_keys: usize,
    table_total_slots: usize,
    capacity_slack_slots: usize,
    tombstone_slots: usize,
    load_factor: f64,
    table_logical_bytes: usize,
    table_allocated_bytes: usize,
    bytes_per_live_key: Option<f64>,
    process_rss_bytes: Option<usize>,
    allocator_allocated_bytes: Option<usize>,
    allocator_active_bytes: Option<usize>,
    allocator_resident_bytes: Option<usize>,
    allocator_mapped_bytes: Option<usize>,
    allocator_retained_bytes: Option<usize>,
    active_expiry_runs: u64,
    active_expiry_sampled: u64,
    active_expiry_expired: u64,
    eviction_admissions: u64,
    eviction_shards_scanned: u64,
    eviction_slots_sampled: u64,
    eviction_bytes_freed: u64,
    latency: LatencySummary,
    lock_profile: Option<LockProfileSnapshot>,
}

#[derive(Clone, Copy)]
struct AllocatorStats {
    allocated: Option<usize>,
    active: Option<usize>,
    resident: Option<usize>,
    mapped: Option<usize>,
    retained: Option<usize>,
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

    println!("{json}");
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.keys == 0 {
        return Err("--keys must be greater than zero".to_string());
    }
    ShardCount::try_new(args.shards).map_err(|error| format!("--shards {error}"))?;
    if args.multi_key_width == 0 {
        return Err("--multi-key-width must be greater than zero".to_string());
    }
    if args.active_expiry_effort == 0 {
        return Err("--active-expiry-effort must be greater than zero".to_string());
    }
    if matches!(args.workload, Workload::EvictionPressure)
        && matches!(args.eviction_policy, ProbeEvictionPolicy::NoEviction)
    {
        return Err(
            "eviction-pressure requires an eviction policy that can reclaim keys".to_string(),
        );
    }
    Ok(())
}

fn run_probe(args: &Args) -> Result<ProbeSummary, String> {
    match args.workload {
        Workload::SetInlineInt => run_set_inline_int(args),
        Workload::SetInlineString => run_set_inline_string(args),
        Workload::SetInlineStringCommand => run_set_inline_string_command(args),
        Workload::SetInlineStringWatched => run_set_inline_string_watched(args),
        Workload::SetInlineStringTtl => run_set_inline_string_ttl(args),
        Workload::SetHeapString => run_set_heap_string(args),
        Workload::GetHit => run_get_hit(args),
        Workload::GetMiss => run_get_miss(args),
        Workload::Mget => run_mget(args),
        Workload::Mset => run_mset(args),
        Workload::Msetnx => run_msetnx(args),
        Workload::Delete => run_delete(args),
        Workload::Append => run_append(args),
        Workload::Setrange => run_setrange(args),
        Workload::Incrbyfloat => run_incrbyfloat(args),
        Workload::ValueMutationMixed => run_value_mutation_mixed(args),
        Workload::ExpirePersist => run_expire_persist(args),
        Workload::TtlExpire => run_ttl_expire(args),
        Workload::EvictionHeadroom => run_eviction_headroom(args),
        Workload::EvictionPressure => run_eviction_pressure(args),
    }
}

fn run_set_inline_int(args: &Args) -> Result<ProbeSummary, String> {
    let keyspace = new_keyspace(args, args.keys)?;
    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);

    for index in 0..args.keys {
        let op_started = Instant::now();
        benchmark_insert(
            &keyspace,
            VortexKey::from(make_key(index)),
            VortexValue::Integer(index as i64),
        );
        latency.record(index as u64, op_started);
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "storage-direct",
        args.keys as u64,
        started.elapsed(),
        latency.summarize(),
        std::mem::size_of::<i64>(),
    ))
}

fn run_set_inline_string(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);

    for index in 0..args.keys {
        let op_started = Instant::now();
        benchmark_insert(
            &keyspace,
            VortexKey::from(make_key(index)),
            VortexValue::from_bytes(make_value_bytes(index, value_size).as_slice()),
        );
        latency.record(index as u64, op_started);
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "storage-direct",
        args.keys as u64,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_set_inline_string_command(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);

    for index in 0..args.keys {
        let key = make_key(index);
        let value = make_value_bytes(index, value_size);
        let parts = [b"SET".as_slice(), key.as_slice(), value.as_slice()];
        let op_started = Instant::now();
        execute_parts(&keyspace, b"SET", &parts, 0u64.into())?;
        latency.record(index as u64, op_started);
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        args.keys as u64,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_set_inline_string_watched(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_inline_strings(&keyspace, args.keys, value_size);
    let watched = (0..args.keys)
        .map(|index| keyspace.watch_key(VortexKey::from(make_key(index))))
        .collect::<Vec<_>>();
    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);

    for index in 0..args.keys {
        let key = make_key(index);
        let value = make_value_bytes(index.wrapping_add(args.keys), value_size);
        let parts = [b"SET".as_slice(), key.as_slice(), value.as_slice()];
        let op_started = Instant::now();
        execute_parts(&keyspace, b"SET", &parts, 0u64.into())?;
        latency.record(index as u64, op_started);
    }

    let summary = finalize_summary(
        args,
        &keyspace,
        "command-path",
        args.keys as u64,
        started.elapsed(),
        latency.summarize(),
        value_size,
    );
    keyspace.unwatch_keys(watched);
    Ok(summary)
}

fn run_set_inline_string_ttl(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    let ttl_text = args.ttl_ms.to_string();
    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);

    for index in 0..args.keys {
        let key = make_key(index);
        let value = make_value_bytes(index, value_size);
        let parts = [
            b"SET".as_slice(),
            key.as_slice(),
            value.as_slice(),
            b"PX".as_slice(),
            ttl_text.as_bytes(),
        ];
        let op_started = Instant::now();
        execute_parts(&keyspace, b"SET", &parts, 1u64.into())?;
        latency.record(index as u64, op_started);
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        args.keys as u64,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_set_heap_string(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.max(MAX_INLINE_VALUE_LEN + 1);
    let keyspace = new_keyspace(args, args.keys)?;
    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);

    for index in 0..args.keys {
        let op_started = Instant::now();
        benchmark_insert(
            &keyspace,
            VortexKey::from(make_key(index)),
            VortexValue::from_bytes(make_value_bytes(index, value_size).as_slice()),
        );
        latency.record(index as u64, op_started);
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "storage-direct",
        args.keys as u64,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_get_hit(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_inline_strings(&keyspace, args.keys, value_size);

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;

    match args.duration {
        Some(seconds) => {
            let deadline = Duration::from_secs(seconds);
            let mut index = 0usize;
            while started.elapsed() < deadline {
                let key_bytes = make_key(index % args.keys);
                let key = VortexKey::from_bytes(key_bytes.as_slice());
                let op_started = Instant::now();
                let found = keyspace.read(key_bytes.as_slice(), |table| table.get(&key).is_some());
                if !found {
                    return Err(format!("GET hit missed key {}", index % args.keys));
                }
                latency.record(operations, op_started);
                operations += 1;
                index += 1;
            }
        }
        None => {
            for index in 0..args.keys {
                let key_bytes = make_key(index);
                let key = VortexKey::from_bytes(key_bytes.as_slice());
                let op_started = Instant::now();
                let found = keyspace.read(key_bytes.as_slice(), |table| table.get(&key).is_some());
                if !found {
                    return Err(format!("GET hit missed key {index}"));
                }
                latency.record(operations, op_started);
                operations += 1;
            }
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "storage-direct",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_get_miss(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_inline_strings(&keyspace, args.keys, value_size);

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;

    match args.duration {
        Some(seconds) => {
            let deadline = Duration::from_secs(seconds);
            let mut index = 0usize;
            while started.elapsed() < deadline {
                let key_bytes = make_missing_key(index);
                let key = VortexKey::from_bytes(key_bytes.as_slice());
                let op_started = Instant::now();
                let found = keyspace.read(key_bytes.as_slice(), |table| table.get(&key).is_some());
                if found {
                    return Err(format!("GET miss unexpectedly found key {index}"));
                }
                latency.record(operations, op_started);
                operations += 1;
                index += 1;
            }
        }
        None => {
            for index in 0..args.keys {
                let key_bytes = make_missing_key(index);
                let key = VortexKey::from_bytes(key_bytes.as_slice());
                let op_started = Instant::now();
                let found = keyspace.read(key_bytes.as_slice(), |table| table.get(&key).is_some());
                if found {
                    return Err(format!("GET miss unexpectedly found key {index}"));
                }
                latency.record(operations, op_started);
                operations += 1;
            }
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "storage-direct",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_mget(args: &Args) -> Result<ProbeSummary, String> {
    let width = args.multi_key_width.min(args.keys).max(1);
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_inline_strings(&keyspace, args.keys, value_size);

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;

    match args.duration {
        Some(seconds) => {
            let deadline = Duration::from_secs(seconds);
            let mut batch = 0usize;
            while started.elapsed() < deadline {
                let op_started = Instant::now();
                execute_mget_batch(&keyspace, width, batch, args.keys)?;
                latency.record(operations, op_started);
                operations += 1;
                batch += 1;
            }
        }
        None => {
            for batch in (0..args.keys).step_by(width) {
                let op_started = Instant::now();
                execute_mget_batch(&keyspace, width, batch / width, args.keys)?;
                latency.record(operations, op_started);
                operations += 1;
            }
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_mset(args: &Args) -> Result<ProbeSummary, String> {
    let width = args.multi_key_width.min(args.keys).max(1);
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_inline_strings(&keyspace, args.keys, value_size);

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;

    match args.duration {
        Some(seconds) => {
            let deadline = Duration::from_secs(seconds);
            let mut batch = 0usize;
            while started.elapsed() < deadline {
                let op_started = Instant::now();
                execute_mset_batch(&keyspace, width, batch, args.keys, value_size)?;
                latency.record(operations, op_started);
                operations += 1;
                batch += 1;
            }
        }
        None => {
            for batch in (0..args.keys).step_by(width) {
                let op_started = Instant::now();
                execute_mset_batch(&keyspace, width, batch / width, args.keys, value_size)?;
                latency.record(operations, op_started);
                operations += 1;
            }
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_msetnx(args: &Args) -> Result<ProbeSummary, String> {
    let width = args.multi_key_width.min(args.keys).max(1);
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys.saturating_mul(2))?;

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;

    match args.duration {
        Some(seconds) => {
            let deadline = Duration::from_secs(seconds);
            let mut batch = 0usize;
            while started.elapsed() < deadline {
                let op_started = Instant::now();
                execute_msetnx_batch(&keyspace, width, batch, args.keys, value_size)?;
                latency.record(operations, op_started);
                operations += 1;
                batch += 1;
            }
        }
        None => {
            for batch in (0..args.keys).step_by(width) {
                let op_started = Instant::now();
                execute_msetnx_batch(&keyspace, width, batch / width, args.keys, value_size)?;
                latency.record(operations, op_started);
                operations += 1;
            }
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_delete(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let width = args.multi_key_width.min(args.keys).max(1);
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_inline_strings(&keyspace, args.keys, value_size);

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let batch_count = args.keys.div_ceil(width);
    let mut operations = 0u64;

    for batch in 0..batch_count {
        if let Some(seconds) = args.duration {
            if started.elapsed() >= Duration::from_secs(seconds) {
                break;
            }
        }
        let op_started = Instant::now();
        execute_delete_batch(&keyspace, width, batch, args.keys)?;
        latency.record(operations, op_started);
        operations += 1;
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_append(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_inline_strings(&keyspace, args.keys, value_size);
    let append = make_value_bytes(0, value_size.clamp(1, 8));

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;

    match args.duration {
        Some(seconds) => {
            let deadline = Duration::from_secs(seconds);
            let mut index = 0usize;
            while started.elapsed() < deadline {
                let key = make_key(index % args.keys);
                let parts = [b"APPEND".as_slice(), key.as_slice(), append.as_slice()];
                let op_started = Instant::now();
                execute_parts(&keyspace, b"APPEND", &parts, 0u64.into())?;
                latency.record(operations, op_started);
                operations += 1;
                index += 1;
            }
        }
        None => {
            for index in 0..args.keys {
                let key = make_key(index);
                let parts = [b"APPEND".as_slice(), key.as_slice(), append.as_slice()];
                let op_started = Instant::now();
                execute_parts(&keyspace, b"APPEND", &parts, 0u64.into())?;
                latency.record(operations, op_started);
                operations += 1;
            }
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_setrange(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_inline_strings(&keyspace, args.keys, value_size);
    let offset = value_size.saturating_sub(1).min(value_size / 2);
    let offset_text = offset.to_string();
    let replacement = make_value_bytes(1, value_size.clamp(1, 8));

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;

    match args.duration {
        Some(seconds) => {
            let deadline = Duration::from_secs(seconds);
            let mut index = 0usize;
            while started.elapsed() < deadline {
                let key = make_key(index % args.keys);
                let parts = [
                    b"SETRANGE".as_slice(),
                    key.as_slice(),
                    offset_text.as_bytes(),
                    replacement.as_slice(),
                ];
                let op_started = Instant::now();
                execute_parts(&keyspace, b"SETRANGE", &parts, 0u64.into())?;
                latency.record(operations, op_started);
                operations += 1;
                index += 1;
            }
        }
        None => {
            for index in 0..args.keys {
                let key = make_key(index);
                let parts = [
                    b"SETRANGE".as_slice(),
                    key.as_slice(),
                    offset_text.as_bytes(),
                    replacement.as_slice(),
                ];
                let op_started = Instant::now();
                execute_parts(&keyspace, b"SETRANGE", &parts, 0u64.into())?;
                latency.record(operations, op_started);
                operations += 1;
            }
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_incrbyfloat(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = b"1.25".len();
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_float_strings(&keyspace, args.keys);

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;

    match args.duration {
        Some(seconds) => {
            let deadline = Duration::from_secs(seconds);
            let mut index = 0usize;
            while started.elapsed() < deadline {
                let key = make_key(index % args.keys);
                let parts = [b"INCRBYFLOAT".as_slice(), key.as_slice(), b"0.5".as_slice()];
                let op_started = Instant::now();
                execute_parts(&keyspace, b"INCRBYFLOAT", &parts, 0u64.into())?;
                latency.record(operations, op_started);
                operations += 1;
                index += 1;
            }
        }
        None => {
            for index in 0..args.keys {
                let key = make_key(index);
                let parts = [b"INCRBYFLOAT".as_slice(), key.as_slice(), b"0.5".as_slice()];
                let op_started = Instant::now();
                execute_parts(&keyspace, b"INCRBYFLOAT", &parts, 0u64.into())?;
                latency.record(operations, op_started);
                operations += 1;
            }
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_value_mutation_mixed(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let partition_keys = (args.keys / 3).max(1);
    let keyspace = new_keyspace(args, partition_keys * 3)?;
    prefill_named_strings(&keyspace, b"a", partition_keys, value_size);
    prefill_named_strings(&keyspace, b"r", partition_keys, value_size);
    prefill_named_float_strings(&keyspace, b"f", partition_keys);

    let append = make_value_bytes(0, value_size.clamp(1, 4));
    let offset = value_size.saturating_sub(1).min(value_size / 2);
    let offset_text = offset.to_string();
    let replacement = make_value_bytes(1, value_size.clamp(1, 4));

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;

    let execute_one = |operation_index: usize| -> Result<(), String> {
        let key_index = (operation_index / 3) % partition_keys;
        match operation_index % 3 {
            0 => {
                let key = make_named_key(b"a", key_index);
                let parts = [b"APPEND".as_slice(), key.as_slice(), append.as_slice()];
                execute_parts(&keyspace, b"APPEND", &parts, 0u64.into())
            }
            1 => {
                let key = make_named_key(b"r", key_index);
                let parts = [
                    b"SETRANGE".as_slice(),
                    key.as_slice(),
                    offset_text.as_bytes(),
                    replacement.as_slice(),
                ];
                execute_parts(&keyspace, b"SETRANGE", &parts, 0u64.into())
            }
            _ => {
                let key = make_named_key(b"f", key_index);
                let parts = [b"INCRBYFLOAT".as_slice(), key.as_slice(), b"0.5".as_slice()];
                execute_parts(&keyspace, b"INCRBYFLOAT", &parts, 0u64.into())
            }
        }
    };

    match args.duration {
        Some(seconds) => {
            let deadline = Duration::from_secs(seconds);
            let mut index = 0usize;
            while started.elapsed() < deadline {
                let op_started = Instant::now();
                execute_one(index)?;
                latency.record(operations, op_started);
                operations += 1;
                index += 1;
            }
        }
        None => {
            for index in 0..(partition_keys * 3) {
                let op_started = Instant::now();
                execute_one(index)?;
                latency.record(operations, op_started);
                operations += 1;
            }
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_expire_persist(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_inline_strings(&keyspace, args.keys, value_size);
    let ttl_text = args.ttl_ms.to_string();

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;

    match args.duration {
        Some(seconds) => {
            let deadline = Duration::from_secs(seconds);
            let mut index = 0usize;
            while started.elapsed() < deadline {
                let key = make_key(index % args.keys);
                let op_started = Instant::now();
                let expire = [b"PEXPIRE".as_slice(), key.as_slice(), ttl_text.as_bytes()];
                execute_parts(&keyspace, b"PEXPIRE", &expire, 0u64.into())?;
                let persist = [b"PERSIST".as_slice(), key.as_slice()];
                execute_parts(&keyspace, b"PERSIST", &persist, 0u64.into())?;
                latency.record(operations, op_started);
                operations += 1;
                index += 1;
            }
        }
        None => {
            for index in 0..args.keys {
                let key = make_key(index);
                let op_started = Instant::now();
                let expire = [b"PEXPIRE".as_slice(), key.as_slice(), ttl_text.as_bytes()];
                execute_parts(&keyspace, b"PEXPIRE", &expire, 0u64.into())?;
                let persist = [b"PERSIST".as_slice(), key.as_slice()];
                execute_parts(&keyspace, b"PERSIST", &persist, 0u64.into())?;
                latency.record(operations, op_started);
                operations += 1;
            }
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_ttl_expire(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys)?;
    prefill_inline_strings(&keyspace, args.keys, value_size);

    let ttl_text = args.ttl_ms.to_string();
    let setup_clock = CommandClock::from(1u64);
    for index in 0..args.keys {
        let key = make_key(index);
        let parts = [b"PEXPIRE".as_slice(), key.as_slice(), ttl_text.as_bytes()];
        execute_parts(&keyspace, b"PEXPIRE", &parts, setup_clock)?;
    }

    let expiry_now = args.ttl_ms.saturating_mul(NS_PER_MS).saturating_add(1);
    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);
    let mut operations = 0u64;
    let mut start_slot = 0usize;

    while keyspace.has_expiring_keys() {
        for shard in 0..args.shards {
            let op_started = Instant::now();
            let (expired, sampled) = keyspace.run_active_expiry_on_shard(
                shard,
                start_slot,
                args.active_expiry_effort,
                expiry_now,
            );
            keyspace.record_reactor_active_expiry(0, sampled, expired);
            latency.record(operations, op_started);
            operations += 1;
            start_slot = start_slot.wrapping_add(args.active_expiry_effort);
        }
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "hybrid",
        operations,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_eviction_headroom(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys.saturating_mul(2))?;
    prefill_inline_strings(&keyspace, args.keys, value_size);

    let estimated_growth = args.keys.saturating_mul(estimate_insert_bytes(value_size));
    let headroom = args.eviction_headroom_bytes.max(estimated_growth);
    keyspace.configure_eviction(
        keyspace.memory_used().saturating_add(headroom),
        args.eviction_policy.into_engine(),
    );

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);

    for index in 0..args.keys {
        let key = make_new_key(index);
        let value = make_value_bytes(index, value_size);
        let parts = [b"SET".as_slice(), key.as_slice(), value.as_slice()];
        let op_started = Instant::now();
        execute_parts(&keyspace, b"SET", &parts, 0u64.into())?;
        latency.record(index as u64, op_started);
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        args.keys as u64,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn run_eviction_pressure(args: &Args) -> Result<ProbeSummary, String> {
    let value_size = args.value_size.clamp(1, MAX_INLINE_VALUE_LEN);
    let keyspace = new_keyspace(args, args.keys.saturating_mul(2))?;
    prefill_inline_strings(&keyspace, args.keys, value_size);
    keyspace.configure_eviction(keyspace.memory_used(), args.eviction_policy.into_engine());
    preheat_eviction_candidates(&keyspace, args.keys)?;

    let mut latency = LatencyRecorder::new(args.latency_sample_rate, args.latency_max_samples);
    let started = begin_measurement(args, &keyspace);

    for index in 0..args.keys {
        let key = make_pressure_key(index);
        let value = make_value_bytes(index, value_size);
        let parts = [b"SET".as_slice(), key.as_slice(), value.as_slice()];
        let op_started = Instant::now();
        execute_parts(&keyspace, b"SET", &parts, 0u64.into())?;
        latency.record(index as u64, op_started);
    }

    Ok(finalize_summary(
        args,
        &keyspace,
        "command-path",
        args.keys as u64,
        started.elapsed(),
        latency.summarize(),
        value_size,
    ))
}

fn new_keyspace(args: &Args, capacity: usize) -> Result<ConcurrentKeyspace, String> {
    let keyspace = ConcurrentKeyspace::try_with_capacity(args.shards, capacity)
        .map_err(|error| format!("failed to create keyspace: {error}"))?;
    if args.aof_recording {
        keyspace.enable_aof_recording();
    }
    if args.lock_profile || std::env::var_os("VORTEX_LOCK_PROFILE").is_some() {
        keyspace.set_lock_profile_enabled(true);
    }
    Ok(keyspace)
}

fn begin_measurement(args: &Args, keyspace: &ConcurrentKeyspace) -> Instant {
    if args.lock_profile || std::env::var_os("VORTEX_LOCK_PROFILE").is_some() {
        keyspace.reset_lock_profile();
    }
    Instant::now()
}

fn benchmark_insert(keyspace: &ConcurrentKeyspace, key: VortexKey, value: VortexValue) {
    // SAFETY: engine_probe uses this only for deterministic setup or storage-only
    // workload loops where command-layer side effects are intentionally excluded.
    unsafe {
        keyspace.benchmark_insert_unchecked(key, value);
    }
}

fn prefill_inline_strings(keyspace: &ConcurrentKeyspace, keys: usize, value_size: usize) {
    for index in 0..keys {
        benchmark_insert(
            keyspace,
            VortexKey::from(make_key(index)),
            VortexValue::from_bytes(make_value_bytes(index, value_size).as_slice()),
        );
    }
}

fn prefill_float_strings(keyspace: &ConcurrentKeyspace, keys: usize) {
    for index in 0..keys {
        benchmark_insert(
            keyspace,
            VortexKey::from(make_key(index)),
            VortexValue::from_bytes(make_float_value(index).as_slice()),
        );
    }
}

fn prefill_named_strings(
    keyspace: &ConcurrentKeyspace,
    prefix: &[u8],
    keys: usize,
    value_size: usize,
) {
    for index in 0..keys {
        benchmark_insert(
            keyspace,
            VortexKey::from(make_named_key(prefix, index)),
            VortexValue::from_bytes(make_value_bytes(index, value_size).as_slice()),
        );
    }
}

fn prefill_named_float_strings(keyspace: &ConcurrentKeyspace, prefix: &[u8], keys: usize) {
    for index in 0..keys {
        benchmark_insert(
            keyspace,
            VortexKey::from(make_named_key(prefix, index)),
            VortexValue::from_bytes(make_float_value(index).as_slice()),
        );
    }
}

fn execute_parts(
    keyspace: &ConcurrentKeyspace,
    name: &[u8],
    parts: &[&[u8]],
    clock: CommandClock,
) -> Result<(), String> {
    let wire = make_resp(parts);
    let tape = RespTape::parse_pipeline(&wire)
        .map_err(|error| format!("failed to parse command payload: {error:?}"))?;
    let frame = tape
        .iter()
        .next()
        .ok_or_else(|| "expected exactly one command frame".to_string())?;
    let executed = execute_command(keyspace, name, &frame, clock)
        .ok_or_else(|| format!("unknown engine command: {}", String::from_utf8_lossy(name)))?;

    if executed.response.is_error() {
        return Err(format!(
            "command {} returned an error response",
            String::from_utf8_lossy(name)
        ));
    }

    Ok(())
}

fn execute_mget_batch(
    keyspace: &ConcurrentKeyspace,
    width: usize,
    batch_index: usize,
    key_count: usize,
) -> Result<(), String> {
    let mut parts = Vec::with_capacity(width + 1);
    parts.push(b"MGET".to_vec());

    let start = (batch_index * width) % key_count;
    for offset in 0..width {
        parts.push(make_key((start + offset) % key_count));
    }

    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    execute_parts(keyspace, b"MGET", refs.as_slice(), 0u64.into())
}

fn execute_mset_batch(
    keyspace: &ConcurrentKeyspace,
    width: usize,
    batch_index: usize,
    key_count: usize,
    value_size: usize,
) -> Result<(), String> {
    let mut parts = Vec::with_capacity((width * 2) + 1);
    parts.push(b"MSET".to_vec());

    let start = (batch_index * width) % key_count;
    for offset in 0..width {
        let key_index = (start + offset) % key_count;
        parts.push(make_key(key_index));
        parts.push(make_value_bytes(key_index, value_size));
    }

    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    execute_parts(keyspace, b"MSET", refs.as_slice(), 0u64.into())
}

fn execute_msetnx_batch(
    keyspace: &ConcurrentKeyspace,
    width: usize,
    batch_index: usize,
    key_count: usize,
    value_size: usize,
) -> Result<(), String> {
    let mut parts = Vec::with_capacity((width * 2) + 1);
    parts.push(b"MSETNX".to_vec());

    let start = (batch_index * width) % key_count;
    for offset in 0..width {
        let key_index = (start + offset) % key_count;
        parts.push(make_new_key(key_index + batch_index.saturating_mul(width)));
        parts.push(make_value_bytes(key_index, value_size));
    }

    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    execute_parts(keyspace, b"MSETNX", refs.as_slice(), 0u64.into())
}

fn execute_delete_batch(
    keyspace: &ConcurrentKeyspace,
    width: usize,
    batch_index: usize,
    key_count: usize,
) -> Result<(), String> {
    let mut parts = Vec::with_capacity(width + 1);
    parts.push(b"DEL".to_vec());

    let start = (batch_index * width) % key_count;
    for offset in 0..width {
        parts.push(make_key((start + offset) % key_count));
    }

    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    execute_parts(keyspace, b"DEL", refs.as_slice(), 0u64.into())
}

fn preheat_eviction_candidates(
    keyspace: &ConcurrentKeyspace,
    key_count: usize,
) -> Result<(), String> {
    for index in 0..EVICTION_PREHEAT_READS.min(key_count) {
        let key = make_key(index);
        let parts = [b"GET".as_slice(), key.as_slice()];
        execute_parts(keyspace, b"GET", &parts, 0u64.into())?;
    }
    Ok(())
}

fn finalize_summary(
    args: &Args,
    keyspace: &ConcurrentKeyspace,
    driver: &'static str,
    operations: u64,
    duration: Duration,
    latency: LatencySummary,
    value_size_bytes: usize,
) -> ProbeSummary {
    let engine = keyspace.engine_memory_attribution();
    let allocator = read_allocator_stats();
    let runtime = keyspace.runtime_metrics();
    let eviction = keyspace.eviction_metrics();
    let duration_seconds = duration.as_secs_f64();

    ProbeSummary {
        workload: args.workload,
        driver,
        operations,
        duration_seconds,
        throughput_ops_per_second: if duration_seconds > 0.0 {
            operations as f64 / duration_seconds
        } else {
            0.0
        },
        keys_requested: args.keys,
        value_size_bytes,
        shards: args.shards,
        aof_recording: args.aof_recording,
        live_keys: engine.live_keys,
        expiring_keys: keyspace.approx_expiring_keys(),
        table_total_slots: engine.table_total_slots,
        capacity_slack_slots: engine.capacity_slack_slots,
        tombstone_slots: engine.tombstone_slots,
        load_factor: engine.load_factor,
        table_logical_bytes: engine.logical_dataset_bytes,
        table_allocated_bytes: engine.table_allocated_bytes,
        bytes_per_live_key: engine.bytes_per_live_key,
        process_rss_bytes: current_process_rss_bytes(),
        allocator_allocated_bytes: allocator.allocated,
        allocator_active_bytes: allocator.active,
        allocator_resident_bytes: allocator.resident,
        allocator_mapped_bytes: allocator.mapped,
        allocator_retained_bytes: allocator.retained,
        active_expiry_runs: runtime.active_expiry_runs,
        active_expiry_sampled: runtime.active_expiry_sampled,
        active_expiry_expired: runtime.active_expiry_expired,
        eviction_admissions: eviction.admissions,
        eviction_shards_scanned: eviction.shards_scanned,
        eviction_slots_sampled: eviction.slots_sampled,
        eviction_bytes_freed: eviction.bytes_freed,
        latency,
        lock_profile: (args.lock_profile || std::env::var_os("VORTEX_LOCK_PROFILE").is_some())
            .then(|| keyspace.lock_profile_snapshot()),
    }
}

fn read_allocator_stats() -> AllocatorStats {
    let epoch: u64 = 1;
    // SAFETY: Writing the jemalloc epoch is the standard way to refresh stats.
    let _ = unsafe { tikv_jemalloc_ctl::raw::write(b"epoch\0", epoch) };

    AllocatorStats {
        allocated: tikv_jemalloc_ctl::stats::allocated::read().ok(),
        active: tikv_jemalloc_ctl::stats::active::read().ok(),
        resident: tikv_jemalloc_ctl::stats::resident::read().ok(),
        mapped: tikv_jemalloc_ctl::stats::mapped::read().ok(),
        retained: tikv_jemalloc_ctl::stats::retained::read().ok(),
    }
}

fn current_process_rss_bytes() -> Option<usize> {
    current_process_rss_bytes_procfs()
        .or_else(current_process_peak_rss_bytes)
        .or_else(current_process_rss_bytes_ps)
}

#[cfg(target_os = "linux")]
fn current_process_rss_bytes_procfs() -> Option<usize> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<usize>().ok()?;
    // SAFETY: sysconf reads the process page-size setting and does not mutate Rust memory.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    resident_pages.checked_mul(page_size as usize)
}

#[cfg(not(target_os = "linux"))]
fn current_process_rss_bytes_procfs() -> Option<usize> {
    None
}

fn current_process_peak_rss_bytes() -> Option<usize> {
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes `usage` when it returns 0 for RUSAGE_SELF.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return None;
    }

    // SAFETY: status 0 means libc initialized the rusage structure.
    let max_rss = unsafe { usage.assume_init() }.ru_maxrss;
    if max_rss <= 0 {
        return None;
    }

    #[cfg(target_os = "macos")]
    {
        Some(max_rss as usize)
    }

    #[cfg(not(target_os = "macos"))]
    {
        (max_rss as usize).checked_mul(1024)
    }
}

fn current_process_rss_bytes_ps() -> Option<usize> {
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", pid.as_str()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let kb = String::from_utf8(output.stdout).ok()?;
    let rss_kb = kb.trim().parse::<usize>().ok()?;
    Some(rss_kb.saturating_mul(1024))
}

fn select_percentile(sorted: &[u64], percentile: usize) -> u64 {
    let last_index = sorted.len().saturating_sub(1);
    let index = ((last_index * percentile) / 100).min(last_index);
    sorted[index]
}

fn select_permyriad(sorted: &[u64], permyriad: usize) -> u64 {
    let last_index = sorted.len().saturating_sub(1);
    let index = ((last_index * permyriad) / 10_000).min(last_index);
    sorted[index]
}

fn estimate_insert_bytes(value_size: usize) -> usize {
    64 + 32 + 24 + value_size
}

fn make_key(index: usize) -> Vec<u8> {
    format!("k:{index:016x}").into_bytes()
}

fn make_named_key(prefix: &[u8], index: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 1 + 16);
    key.extend_from_slice(prefix);
    key.push(b':');
    write!(&mut key, "{index:016x}").expect("writing to Vec should not fail");
    key
}

fn make_missing_key(index: usize) -> Vec<u8> {
    format!("m:{index:016x}").into_bytes()
}

fn make_new_key(index: usize) -> Vec<u8> {
    format!("n:{index:016x}").into_bytes()
}

fn make_pressure_key(index: usize) -> Vec<u8> {
    format!("p:{index:016x}").into_bytes()
}

fn make_value_bytes(index: usize, size: usize) -> Vec<u8> {
    let mut value = format!("v:{index:016x}").into_bytes();
    if value.len() > size {
        value.truncate(size);
    } else {
        value.resize(size, b'x');
    }
    value
}

fn make_float_value(index: usize) -> Vec<u8> {
    format!("{}.25", index % 1000).into_bytes()
}

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

impl ProbeSummary {
    fn to_json(&self) -> String {
        let mut json = String::with_capacity(1024);
        let _ = writeln!(&mut json, "{{");
        write_json_string_field(&mut json, "workload", workload_name(self.workload), true);
        write_json_string_field(&mut json, "driver", self.driver, true);
        write_json_u64_field(&mut json, "operations", self.operations, true);
        write_json_f64_field(&mut json, "duration_seconds", self.duration_seconds, true);
        write_json_f64_field(
            &mut json,
            "throughput_ops_per_second",
            self.throughput_ops_per_second,
            true,
        );
        write_json_usize_field(&mut json, "keys_requested", self.keys_requested, true);
        write_json_usize_field(&mut json, "value_size_bytes", self.value_size_bytes, true);
        write_json_usize_field(&mut json, "shards", self.shards, true);
        write_json_bool_field(&mut json, "aof_recording", self.aof_recording, true);
        write_json_usize_field(&mut json, "live_keys", self.live_keys, true);
        write_json_usize_field(&mut json, "expiring_keys", self.expiring_keys, true);
        write_json_usize_field(&mut json, "table_total_slots", self.table_total_slots, true);
        write_json_usize_field(
            &mut json,
            "capacity_slack_slots",
            self.capacity_slack_slots,
            true,
        );
        write_json_usize_field(&mut json, "tombstone_slots", self.tombstone_slots, true);
        write_json_f64_field(&mut json, "load_factor", self.load_factor, true);
        write_json_usize_field(
            &mut json,
            "table_logical_bytes",
            self.table_logical_bytes,
            true,
        );
        write_json_usize_field(
            &mut json,
            "table_allocated_bytes",
            self.table_allocated_bytes,
            true,
        );
        write_json_option_f64_field(
            &mut json,
            "bytes_per_live_key",
            self.bytes_per_live_key,
            true,
        );
        write_json_option_usize_field(&mut json, "process_rss_bytes", self.process_rss_bytes, true);
        write_json_option_usize_field(
            &mut json,
            "allocator_allocated_bytes",
            self.allocator_allocated_bytes,
            true,
        );
        write_json_option_usize_field(
            &mut json,
            "allocator_active_bytes",
            self.allocator_active_bytes,
            true,
        );
        write_json_option_usize_field(
            &mut json,
            "allocator_resident_bytes",
            self.allocator_resident_bytes,
            true,
        );
        write_json_option_usize_field(
            &mut json,
            "allocator_mapped_bytes",
            self.allocator_mapped_bytes,
            true,
        );
        write_json_option_usize_field(
            &mut json,
            "allocator_retained_bytes",
            self.allocator_retained_bytes,
            true,
        );
        write_json_u64_field(
            &mut json,
            "active_expiry_runs",
            self.active_expiry_runs,
            true,
        );
        write_json_u64_field(
            &mut json,
            "active_expiry_sampled",
            self.active_expiry_sampled,
            true,
        );
        write_json_u64_field(
            &mut json,
            "active_expiry_expired",
            self.active_expiry_expired,
            true,
        );
        write_json_u64_field(
            &mut json,
            "eviction_admissions",
            self.eviction_admissions,
            true,
        );
        write_json_u64_field(
            &mut json,
            "eviction_shards_scanned",
            self.eviction_shards_scanned,
            true,
        );
        write_json_u64_field(
            &mut json,
            "eviction_slots_sampled",
            self.eviction_slots_sampled,
            true,
        );
        write_json_u64_field(
            &mut json,
            "eviction_bytes_freed",
            self.eviction_bytes_freed,
            true,
        );

        let _ = writeln!(&mut json, "  \"latency_ns\": {{");
        let _ = writeln!(
            &mut json,
            "    \"sample_count\": {},",
            self.latency.sample_count
        );
        write_json_option_u64_field(&mut json, "p50", self.latency.p50_ns, true);
        write_json_option_u64_field(&mut json, "p95", self.latency.p95_ns, true);
        write_json_option_u64_field(&mut json, "p99", self.latency.p99_ns, true);
        write_json_option_u64_field(&mut json, "p999", self.latency.p999_ns, false);
        let _ = writeln!(
            &mut json,
            "  }}{}",
            if self.lock_profile.is_some() { "," } else { "" }
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
        Workload::SetInlineInt => "set-inline-int",
        Workload::SetInlineString => "set-inline-string",
        Workload::SetInlineStringCommand => "set-inline-string-command",
        Workload::SetInlineStringWatched => "set-inline-string-watched",
        Workload::SetInlineStringTtl => "set-inline-string-ttl",
        Workload::SetHeapString => "set-heap-string",
        Workload::GetHit => "get-hit",
        Workload::GetMiss => "get-miss",
        Workload::Mget => "mget",
        Workload::Mset => "mset",
        Workload::Msetnx => "msetnx",
        Workload::Delete => "delete",
        Workload::Append => "append",
        Workload::Setrange => "setrange",
        Workload::Incrbyfloat => "incrbyfloat",
        Workload::ValueMutationMixed => "value-mutation-mixed",
        Workload::ExpirePersist => "expire-persist",
        Workload::TtlExpire => "ttl-expire",
        Workload::EvictionHeadroom => "eviction-headroom",
        Workload::EvictionPressure => "eviction-pressure",
    }
}

fn write_json_string_field(buf: &mut String, name: &str, value: &str, trailing_comma: bool) {
    let _ = writeln!(
        buf,
        "  \"{}\": \"{}\"{}",
        name,
        escape_json(value),
        comma(trailing_comma)
    );
}

fn write_json_usize_field(buf: &mut String, name: &str, value: usize, trailing_comma: bool) {
    let _ = writeln!(buf, "  \"{}\": {}{}", name, value, comma(trailing_comma));
}

fn write_json_u64_field(buf: &mut String, name: &str, value: u64, trailing_comma: bool) {
    let _ = writeln!(buf, "  \"{}\": {}{}", name, value, comma(trailing_comma));
}

fn write_json_bool_field(buf: &mut String, name: &str, value: bool, trailing_comma: bool) {
    let _ = writeln!(buf, "  \"{}\": {}{}", name, value, comma(trailing_comma));
}

fn write_json_f64_field(buf: &mut String, name: &str, value: f64, trailing_comma: bool) {
    let _ = writeln!(buf, "  \"{}\": {:.6}{}", name, value, comma(trailing_comma));
}

fn write_json_option_usize_field(
    buf: &mut String,
    name: &str,
    value: Option<usize>,
    trailing_comma: bool,
) {
    match value {
        Some(value) => write_json_usize_field(buf, name, value, trailing_comma),
        None => {
            let _ = writeln!(buf, "  \"{}\": null{}", name, comma(trailing_comma));
        }
    }
}

fn write_json_option_u64_field(
    buf: &mut String,
    name: &str,
    value: Option<u64>,
    trailing_comma: bool,
) {
    match value {
        Some(value) => {
            let _ = writeln!(buf, "    \"{}\": {}{}", name, value, comma(trailing_comma));
        }
        None => {
            let _ = writeln!(buf, "    \"{}\": null{}", name, comma(trailing_comma));
        }
    }
}

fn write_json_option_f64_field(
    buf: &mut String,
    name: &str,
    value: Option<f64>,
    trailing_comma: bool,
) {
    match value {
        Some(value) => {
            let _ = writeln!(buf, "  \"{}\": {:.6}{}", name, value, comma(trailing_comma));
        }
        None => {
            let _ = writeln!(buf, "  \"{}\": null{}", name, comma(trailing_comma));
        }
    }
}

const fn comma(enabled: bool) -> &'static str {
    if enabled { "," } else { "" }
}

fn escape_json(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(ch),
        }
    }
    escaped
}
