use std::error::Error;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;

const SEED: u64 = 0xDEADBEEF_CAFE_BABE;
const KEY_PREFIX: &str = "bench:key:";
const MSETNX_KEY_PREFIX: &str = "bench:msetnx:";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MultiKeyWriteCommand {
    Mset,
    Msetnx,
}

#[derive(Parser, Debug, Clone)]
#[command(name = "custom-loadgen")]
#[command(
    about = "Run deterministic RESP/TCP workloads against an external Redis-compatible server"
)]
struct Cli {
    #[arg(long, default_value = "custom-rust")]
    server: String,
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long)]
    port: u16,
    #[arg(long = "workload", required = true)]
    workloads: Vec<String>,
    #[arg(long, value_delimiter = ',', default_values_t = vec![1, 2, 4, 8])]
    threads: Vec<usize>,
    #[arg(long, default_value_t = 10_000)]
    num_keys: u64,
    #[arg(long, default_value_t = 50_000)]
    ops_per_thread: u64,
    #[arg(long, default_value_t = 5_000)]
    warmup_ops: u64,
    #[arg(long, default_value_t = 64)]
    value_size: usize,
    #[arg(long, default_value_t = 1)]
    pipeline_depth: usize,
    #[arg(long, default_value_t = 3)]
    multi_key_width: usize,
    #[arg(long)]
    duration_ms: Option<u64>,
    #[arg(long)]
    output_dir: PathBuf,
}

#[derive(Clone, Copy)]
struct WorkloadSpec {
    canonical_name: &'static str,
    read_weight: u32,
    write_weight: u32,
    multi_key: bool,
    multi_key_write: MultiKeyWriteCommand,
    transactional: bool,
    hot_key: bool,
    counter: Option<CounterKind>,
    pressure: Option<PressureKind>,
}

impl WorkloadSpec {
    fn standard(
        canonical_name: &'static str,
        read_weight: u32,
        write_weight: u32,
        multi_key: bool,
        transactional: bool,
        hot_key: bool,
    ) -> Self {
        Self {
            canonical_name,
            read_weight,
            write_weight,
            multi_key,
            multi_key_write: MultiKeyWriteCommand::Mset,
            transactional,
            hot_key,
            counter: None,
            pressure: None,
        }
    }

    fn counter(
        canonical_name: &'static str,
        read_weight: u32,
        write_weight: u32,
        multi_key: bool,
        transactional: bool,
        counter: CounterKind,
    ) -> Self {
        Self {
            canonical_name,
            read_weight,
            write_weight,
            multi_key,
            multi_key_write: MultiKeyWriteCommand::Mset,
            transactional,
            hot_key: true,
            counter: Some(counter),
            pressure: None,
        }
    }

    fn pressure(canonical_name: &'static str, pressure: PressureKind) -> Self {
        Self {
            canonical_name,
            read_weight: 100,
            write_weight: 0,
            multi_key: false,
            multi_key_write: MultiKeyWriteCommand::Mset,
            transactional: false,
            hot_key: false,
            counter: None,
            pressure: Some(pressure),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CounterKind {
    Hot,
    HotWithGet,
    HotWithSetDel,
    HotTtl,
    HotTransaction,
    HotWatchTransaction,
    HotMultiKey,
}

impl CounterKind {
    fn as_str(self) -> &'static str {
        match self {
            CounterKind::Hot => "hot_counter",
            CounterKind::HotWithGet => "hot_counter_with_get",
            CounterKind::HotWithSetDel => "hot_counter_with_set_del",
            CounterKind::HotTtl => "hot_counter_ttl",
            CounterKind::HotTransaction => "hot_counter_transaction",
            CounterKind::HotWatchTransaction => "hot_counter_watch_transaction",
            CounterKind::HotMultiKey => "hot_counter_multikey",
        }
    }

    fn validates_final_value(self) -> bool {
        matches!(
            self,
            CounterKind::Hot
                | CounterKind::HotWithGet
                | CounterKind::HotTtl
                | CounterKind::HotTransaction
                | CounterKind::HotWatchTransaction
        )
    }

    fn validates_live_ttl(self) -> bool {
        matches!(self, CounterKind::HotTtl)
    }
}

#[derive(Clone, Copy)]
enum PressureKind {
    SlowReader,
    DeepPipeline,
    LargeBulk,
    AofBacklog,
    TtlExpiry,
    EvictionPressure,
    ConnectionStorm,
    CloseStorm,
}

#[derive(Debug, Clone, Serialize)]
struct ThreadResult {
    thread_id: usize,
    ops_completed: u64,
    duration_ns: u64,
    throughput_ops_sec: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    p99_9_ns: u64,
    p99_999_ns: u64,
    max_ns: u64,
    mean_ns: f64,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkResult {
    server: String,
    workload: String,
    host: String,
    port: u16,
    num_threads: usize,
    num_keys: u64,
    ops_per_thread: u64,
    warmup_ops: u64,
    value_size: usize,
    pipeline_depth: usize,
    multi_key_width: usize,
    latency_sample_unit: &'static str,
    total_ops: u64,
    total_duration_ns: u64,
    aggregate_throughput_ops_sec: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    p99_9_ns: u64,
    p99_999_ns: u64,
    max_ns: u64,
    mean_ns: f64,
    thread_results: Vec<ThreadResult>,
    counter: Option<CounterBenchmarkStats>,
}

struct ThreadOutcome {
    result: ThreadResult,
    latencies_ns: Vec<u64>,
    counter_warmup: CounterOperationStats,
    counter_measured: CounterOperationStats,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
struct CounterOperationStats {
    incrby_ops: u64,
    get_ops: u64,
    set_ops: u64,
    del_ops: u64,
    ttl_ops: u64,
    mget_ops: u64,
    mset_ops: u64,
    transaction_attempts: u64,
    transaction_commits: u64,
    transaction_aborts: u64,
    applied_increments: u64,
    barrier_ops: u64,
}

impl CounterOperationStats {
    fn add_assign(&mut self, other: &Self) {
        self.incrby_ops += other.incrby_ops;
        self.get_ops += other.get_ops;
        self.set_ops += other.set_ops;
        self.del_ops += other.del_ops;
        self.ttl_ops += other.ttl_ops;
        self.mget_ops += other.mget_ops;
        self.mset_ops += other.mset_ops;
        self.transaction_attempts += other.transaction_attempts;
        self.transaction_commits += other.transaction_commits;
        self.transaction_aborts += other.transaction_aborts;
        self.applied_increments += other.applied_increments;
        self.barrier_ops += other.barrier_ops;
    }
}

#[derive(Debug, Clone, Serialize)]
struct CounterBenchmarkStats {
    kind: &'static str,
    warmup: CounterOperationStats,
    measured: CounterOperationStats,
    total: CounterOperationStats,
    validation: CounterValidation,
}

#[derive(Debug, Clone, Serialize)]
struct CounterValidation {
    status: &'static str,
    expected_final_value: Option<i64>,
    final_value: Option<i64>,
    final_value_matches: Option<bool>,
    ttl_seconds: Option<i64>,
    ttl_live: Option<bool>,
    notes: Vec<String>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    if cli.threads.is_empty() {
        return Err("at least one thread count is required".into());
    }
    if cli.pipeline_depth == 0 {
        return Err("pipeline depth must be positive".into());
    }
    if cli.multi_key_width == 0 {
        return Err("multi-key width must be positive".into());
    }

    std::fs::create_dir_all(&cli.output_dir)?;

    for workload_name in &cli.workloads {
        let spec = resolve_workload(workload_name)?;
        for &num_threads in &cli.threads {
            run_workload(&cli, spec, num_threads)?;
        }
    }

    Ok(())
}

fn run_workload(cli: &Cli, spec: WorkloadSpec, num_threads: usize) -> Result<(), Box<dyn Error>> {
    if spec.pressure.is_some() {
        return run_pressure_workload(cli, spec, num_threads);
    }
    if cli.pipeline_depth > 1 && spec.counter != Some(CounterKind::Hot) {
        return Err("pipeline depth greater than 1 is only supported for hot_counter".into());
    }

    prepare_workload(&cli.host, cli.port, spec, cli.num_keys, cli.value_size)?;

    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = Vec::with_capacity(num_threads);
    let port = cli.port;
    for thread_id in 0..num_threads {
        let barrier = Arc::clone(&barrier);
        let host = cli.host.clone();
        let value_size = cli.value_size;
        let ops_per_thread = cli.ops_per_thread;
        let warmup_ops = cli.warmup_ops;
        let num_keys = cli.num_keys;
        let pipeline_depth = cli.pipeline_depth;
        let multi_key_width = cli.multi_key_width;
        handles.push(thread::spawn(move || {
            run_thread(
                &host,
                port,
                spec,
                num_keys,
                value_size,
                ops_per_thread,
                warmup_ops,
                pipeline_depth,
                multi_key_width,
                thread_id,
                barrier,
            )
        }));
    }

    let mut thread_results = Vec::with_capacity(num_threads);
    let mut all_latencies = Vec::new();
    let mut counter_warmup = CounterOperationStats::default();
    let mut counter_measured = CounterOperationStats::default();
    for handle in handles {
        let outcome = handle
            .join()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "benchmark worker panicked"))??;
        all_latencies.extend_from_slice(&outcome.latencies_ns);
        counter_warmup.add_assign(&outcome.counter_warmup);
        counter_measured.add_assign(&outcome.counter_measured);
        thread_results.push(outcome.result);
    }

    all_latencies.sort_unstable();
    let total_ops = thread_results
        .iter()
        .map(|item| item.ops_completed)
        .sum::<u64>();
    let total_duration_ns = thread_results
        .iter()
        .map(|item| item.duration_ns)
        .max()
        .unwrap_or(0);
    let aggregate_throughput_ops_sec = if total_duration_ns == 0 {
        0.0
    } else {
        total_ops as f64 / (total_duration_ns as f64 / 1_000_000_000.0)
    };
    let mean_ns = mean_latency(&all_latencies);
    let counter = build_counter_benchmark_stats(
        &cli.host,
        cli.port,
        spec.counter,
        counter_warmup,
        counter_measured,
    )?;
    let counter_failed = counter
        .as_ref()
        .is_some_and(|stats| stats.validation.status == "failed");

    let result = BenchmarkResult {
        server: cli.server.clone(),
        workload: spec.canonical_name.to_string(),
        host: cli.host.clone(),
        port: cli.port,
        num_threads,
        num_keys: cli.num_keys,
        ops_per_thread: cli.ops_per_thread,
        warmup_ops: cli.warmup_ops,
        value_size: cli.value_size,
        pipeline_depth: cli.pipeline_depth,
        multi_key_width: cli.multi_key_width,
        latency_sample_unit: if cli.pipeline_depth > 1 {
            "pipeline_batch_assigned_to_each_operation"
        } else {
            "operation"
        },
        total_ops,
        total_duration_ns,
        aggregate_throughput_ops_sec,
        p50_ns: percentile(&all_latencies, 0.50),
        p95_ns: percentile(&all_latencies, 0.95),
        p99_ns: percentile(&all_latencies, 0.99),
        p99_9_ns: percentile(&all_latencies, 0.999),
        p99_999_ns: percentile(&all_latencies, 0.99999),
        max_ns: all_latencies.last().copied().unwrap_or(0),
        mean_ns,
        thread_results,
        counter,
    };

    let output_path = cli.output_dir.join(format!(
        "{}-{}-{}t-p{}.json",
        sanitize_identifier(&cli.server),
        sanitize_identifier(spec.canonical_name),
        num_threads,
        cli.pipeline_depth
    ));
    std::fs::write(output_path, serde_json::to_string_pretty(&result)?)?;
    if counter_failed {
        return Err(format!(
            "counter correctness validation failed for {} with {} thread(s)",
            spec.canonical_name, num_threads
        )
        .into());
    }
    Ok(())
}

fn run_pressure_workload(
    cli: &Cli,
    spec: WorkloadSpec,
    num_threads: usize,
) -> Result<(), Box<dyn Error>> {
    let pressure = spec.pressure.expect("pressure workload has kind");
    flush_and_preload(&cli.host, cli.port, cli.num_keys, cli.value_size)?;

    let barrier = Arc::new(Barrier::new(num_threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let pressure_barrier = Arc::clone(&barrier);
    let pressure_stop = Arc::clone(&stop);
    let pressure_host = cli.host.clone();
    let pressure_port = cli.port;
    let pressure_value_size = cli.value_size;
    let pressure_num_keys = cli.num_keys;
    let pressure_handle = thread::spawn(move || {
        pressure_barrier.wait();
        run_pressure_client(
            &pressure_host,
            pressure_port,
            pressure,
            pressure_num_keys,
            pressure_value_size,
            pressure_stop,
        )
    });

    let mut handles = Vec::with_capacity(num_threads);
    for thread_id in 0..num_threads {
        let barrier = Arc::clone(&barrier);
        let host = cli.host.clone();
        let port = cli.port;
        let ops_per_thread = cli.ops_per_thread;
        let warmup_ops = cli.warmup_ops;
        let num_keys = cli.num_keys;
        let duration = cli.duration_ms.map(Duration::from_millis);
        handles.push(thread::spawn(move || {
            run_pressure_latency_thread(
                &host,
                port,
                num_keys,
                ops_per_thread,
                warmup_ops,
                duration,
                thread_id,
                barrier,
            )
        }));
    }

    let mut thread_results = Vec::with_capacity(num_threads);
    let mut all_latencies = Vec::new();
    for handle in handles {
        let outcome = handle
            .join()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "benchmark worker panicked"))??;
        all_latencies.extend_from_slice(&outcome.latencies_ns);
        thread_results.push(outcome.result);
    }

    stop.store(true, Ordering::Relaxed);
    pressure_handle
        .join()
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "pressure worker panicked"))??;

    all_latencies.sort_unstable();
    let total_ops = thread_results
        .iter()
        .map(|item| item.ops_completed)
        .sum::<u64>();
    let total_duration_ns = thread_results
        .iter()
        .map(|item| item.duration_ns)
        .max()
        .unwrap_or(0);
    let aggregate_throughput_ops_sec = if total_duration_ns == 0 {
        0.0
    } else {
        total_ops as f64 / (total_duration_ns as f64 / 1_000_000_000.0)
    };
    let mean_ns = mean_latency(&all_latencies);
    let result = BenchmarkResult {
        server: cli.server.clone(),
        workload: spec.canonical_name.to_string(),
        host: cli.host.clone(),
        port: cli.port,
        num_threads,
        num_keys: cli.num_keys,
        ops_per_thread: cli.ops_per_thread,
        warmup_ops: cli.warmup_ops,
        value_size: cli.value_size,
        pipeline_depth: 1,
        multi_key_width: cli.multi_key_width,
        latency_sample_unit: "operation",
        total_ops,
        total_duration_ns,
        aggregate_throughput_ops_sec,
        p50_ns: percentile(&all_latencies, 0.50),
        p95_ns: percentile(&all_latencies, 0.95),
        p99_ns: percentile(&all_latencies, 0.99),
        p99_9_ns: percentile(&all_latencies, 0.999),
        p99_999_ns: percentile(&all_latencies, 0.99999),
        max_ns: all_latencies.last().copied().unwrap_or(0),
        mean_ns,
        thread_results,
        counter: None,
    };

    let output_path = cli.output_dir.join(format!(
        "{}-{}-{}t.json",
        sanitize_identifier(&cli.server),
        sanitize_identifier(spec.canonical_name),
        num_threads
    ));
    std::fs::write(output_path, serde_json::to_string_pretty(&result)?)?;
    Ok(())
}

fn run_pressure_latency_thread(
    host: &str,
    port: u16,
    num_keys: u64,
    ops_per_thread: u64,
    warmup_ops: u64,
    duration: Option<Duration>,
    thread_id: usize,
    barrier: Arc<Barrier>,
) -> io::Result<ThreadOutcome> {
    let mut writer = TcpStream::connect((host, port))?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    let mut rng = StdRng::seed_from_u64(SEED.wrapping_add(thread_id as u64 * 131_071));

    for _ in 0..warmup_ops {
        let key = key_name(next_key_for_latency(num_keys, &mut rng));
        execute_command(&mut writer, &mut reader, &["GET".to_string(), key])?;
    }

    barrier.wait();
    let start = Instant::now();
    let mut latencies_ns = Vec::with_capacity(ops_per_thread as usize);
    let mut ops_completed = 0;
    while pressure_latency_should_continue(start, duration, ops_completed, ops_per_thread) {
        let key = key_name(next_key_for_latency(num_keys, &mut rng));
        let op_start = Instant::now();
        execute_command(&mut writer, &mut reader, &["GET".to_string(), key])?;
        latencies_ns.push(op_start.elapsed().as_nanos() as u64);
        ops_completed += 1;
    }
    let duration_ns = start.elapsed().as_nanos() as u64;
    let metrics = latency_summary(thread_id, ops_completed, duration_ns, &mut latencies_ns);

    Ok(ThreadOutcome {
        result: metrics,
        latencies_ns,
        counter_warmup: CounterOperationStats::default(),
        counter_measured: CounterOperationStats::default(),
    })
}

fn pressure_latency_should_continue(
    start: Instant,
    duration: Option<Duration>,
    ops_completed: u64,
    ops_per_thread: u64,
) -> bool {
    match duration {
        Some(duration) => start.elapsed() < duration,
        None => ops_completed < ops_per_thread,
    }
}

fn run_pressure_client(
    host: &str,
    port: u16,
    pressure: PressureKind,
    num_keys: u64,
    value_size: usize,
    stop: Arc<AtomicBool>,
) -> io::Result<()> {
    match pressure {
        PressureKind::SlowReader => pressure_slow_reader(host, port, stop),
        PressureKind::DeepPipeline => pressure_deep_pipeline(host, port, stop),
        PressureKind::LargeBulk => pressure_large_bulk(host, port, num_keys, value_size, stop),
        PressureKind::AofBacklog => pressure_write_backlog(host, port, num_keys, value_size, stop),
        PressureKind::TtlExpiry => pressure_ttl_expiry(host, port, num_keys, value_size, stop),
        PressureKind::EvictionPressure => pressure_eviction(host, port, num_keys, value_size, stop),
        PressureKind::ConnectionStorm => pressure_connection_storm(host, port, stop),
        PressureKind::CloseStorm => pressure_close_storm(host, port, stop),
    }
}

fn pressure_slow_reader(host: &str, port: u16, stop: Arc<AtomicBool>) -> io::Result<()> {
    let mut stream = TcpStream::connect((host, port))?;
    stream.set_nodelay(true)?;
    stream.set_write_timeout(Some(Duration::from_millis(10)))?;
    let command = encode_command(&["PING"]);
    while !stop.load(Ordering::Relaxed) {
        for _ in 0..256 {
            if let Err(error) = stream.write_all(&command) {
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) {
                    break;
                }
                return Ok(());
            }
        }
        let _ = stream.flush();
    }
    Ok(())
}

fn pressure_deep_pipeline(host: &str, port: u16, stop: Arc<AtomicBool>) -> io::Result<()> {
    let mut writer = TcpStream::connect((host, port))?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    let command = encode_command(&["PING"]);
    while !stop.load(Ordering::Relaxed) {
        for _ in 0..512 {
            writer.write_all(&command)?;
        }
        writer.flush()?;
        for _ in 0..512 {
            read_response(&mut reader)?;
        }
    }
    Ok(())
}

fn pressure_large_bulk(
    host: &str,
    port: u16,
    num_keys: u64,
    value_size: usize,
    stop: Arc<AtomicBool>,
) -> io::Result<()> {
    let large_value = "x".repeat(value_size.max(128 * 1024));
    pressure_set_loop(host, port, num_keys, &large_value, "pressure:large", stop)
}

fn pressure_write_backlog(
    host: &str,
    port: u16,
    num_keys: u64,
    value_size: usize,
    stop: Arc<AtomicBool>,
) -> io::Result<()> {
    let large_value = "x".repeat(value_size.max(16 * 1024));
    pressure_set_loop(host, port, num_keys, &large_value, "pressure:aof", stop)
}

fn pressure_ttl_expiry(
    host: &str,
    port: u16,
    num_keys: u64,
    value_size: usize,
    stop: Arc<AtomicBool>,
) -> io::Result<()> {
    let mut writer = TcpStream::connect((host, port))?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    let value = "x".repeat(value_size.max(64));
    let mut key = 0u64;
    while !stop.load(Ordering::Relaxed) {
        key = key.wrapping_add(1);
        let name = format!("pressure:ttl:{}", key % num_keys.max(1));
        execute_command(
            &mut writer,
            &mut reader,
            &[
                "SET".to_string(),
                name,
                value.clone(),
                "PX".to_string(),
                "1".to_string(),
            ],
        )?;
    }
    Ok(())
}

fn pressure_eviction(
    host: &str,
    port: u16,
    num_keys: u64,
    value_size: usize,
    stop: Arc<AtomicBool>,
) -> io::Result<()> {
    let large_value = "x".repeat(value_size.max(256 * 1024));
    pressure_set_loop(
        host,
        port,
        num_keys.max(100_000),
        &large_value,
        "pressure:evict",
        stop,
    )
}

fn pressure_set_loop(
    host: &str,
    port: u16,
    num_keys: u64,
    value: &str,
    prefix: &str,
    stop: Arc<AtomicBool>,
) -> io::Result<()> {
    let mut writer = TcpStream::connect((host, port))?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    let mut key = 0u64;
    while !stop.load(Ordering::Relaxed) {
        key = key.wrapping_add(1);
        let name = format!("{prefix}:{}", key % num_keys.max(1));
        execute_pressure_command(
            &mut writer,
            &mut reader,
            &["SET".to_string(), name, value.to_string()],
        )?;
    }
    Ok(())
}

fn pressure_connection_storm(host: &str, port: u16, stop: Arc<AtomicBool>) -> io::Result<()> {
    let command = encode_command(&["PING"]);
    while !stop.load(Ordering::Relaxed) {
        let mut writer = TcpStream::connect((host, port))?;
        writer.set_nodelay(true)?;
        let mut reader = BufReader::new(writer.try_clone()?);
        writer.write_all(&command)?;
        writer.flush()?;
        read_response(&mut reader)?;
    }
    Ok(())
}

fn pressure_close_storm(host: &str, port: u16, stop: Arc<AtomicBool>) -> io::Result<()> {
    while !stop.load(Ordering::Relaxed) {
        let mut sockets = Vec::with_capacity(128);
        for _ in 0..128 {
            match TcpStream::connect((host, port)) {
                Ok(stream) => sockets.push(stream),
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => break,
                Err(error) => return Err(error),
            }
        }
        drop(sockets);
    }
    Ok(())
}

fn run_thread(
    host: &str,
    port: u16,
    spec: WorkloadSpec,
    num_keys: u64,
    value_size: usize,
    ops_per_thread: u64,
    warmup_ops: u64,
    pipeline_depth: usize,
    multi_key_width: usize,
    thread_id: usize,
    barrier: Arc<Barrier>,
) -> io::Result<ThreadOutcome> {
    let mut writer = TcpStream::connect((host, port))?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    let value = "x".repeat(value_size);
    let mut rng = StdRng::seed_from_u64(SEED.wrapping_add(thread_id as u64 * 104_729));
    let mut command_buffer = Vec::with_capacity(estimate_multi_key_command_bytes(
        multi_key_width,
        value_size,
    ));
    let mut key_buffer = Vec::with_capacity(multi_key_width.max(1));
    let mut counter_warmup = CounterOperationStats::default();
    let mut counter_measured = CounterOperationStats::default();

    if spec.counter == Some(CounterKind::Hot) && pipeline_depth > 1 {
        return run_pipelined_hot_counter_thread(
            writer,
            reader,
            ops_per_thread,
            warmup_ops,
            pipeline_depth,
            thread_id,
            barrier,
        );
    }

    for _ in 0..warmup_ops {
        execute_operation(
            &mut writer,
            &mut reader,
            spec,
            &value,
            num_keys,
            multi_key_width,
            &mut key_buffer,
            &mut command_buffer,
            &mut rng,
            Some(&mut counter_warmup),
        )?;
    }

    barrier.wait();
    let start = Instant::now();
    let mut latencies_ns = Vec::with_capacity(ops_per_thread as usize);
    for _ in 0..ops_per_thread {
        let op_start = Instant::now();
        execute_operation(
            &mut writer,
            &mut reader,
            spec,
            &value,
            num_keys,
            multi_key_width,
            &mut key_buffer,
            &mut command_buffer,
            &mut rng,
            Some(&mut counter_measured),
        )?;
        latencies_ns.push(op_start.elapsed().as_nanos() as u64);
    }
    let duration_ns = start.elapsed().as_nanos() as u64;
    let metrics = latency_summary(thread_id, ops_per_thread, duration_ns, &mut latencies_ns);

    Ok(ThreadOutcome {
        result: metrics,
        latencies_ns,
        counter_warmup,
        counter_measured,
    })
}

fn run_pipelined_hot_counter_thread(
    mut writer: TcpStream,
    mut reader: BufReader<TcpStream>,
    ops_per_thread: u64,
    warmup_ops: u64,
    pipeline_depth: usize,
    thread_id: usize,
    barrier: Arc<Barrier>,
) -> io::Result<ThreadOutcome> {
    let mut counter_warmup = CounterOperationStats::default();
    let mut counter_measured = CounterOperationStats::default();

    execute_counter_incr_pipeline(&mut writer, &mut reader, warmup_ops, pipeline_depth)?;
    counter_warmup.incrby_ops = warmup_ops;
    counter_warmup.applied_increments = warmup_ops;

    barrier.wait();
    let start = Instant::now();
    let mut latencies_ns = Vec::with_capacity(ops_per_thread as usize);
    let mut remaining = ops_per_thread;
    while remaining > 0 {
        let batch = remaining.min(pipeline_depth as u64);
        let op_start = Instant::now();
        execute_counter_incr_pipeline(&mut writer, &mut reader, batch, pipeline_depth)?;
        let elapsed = op_start.elapsed().as_nanos() as u64;
        latencies_ns.extend(std::iter::repeat_n(elapsed, batch as usize));
        remaining -= batch;
    }
    let duration_ns = start.elapsed().as_nanos() as u64;
    let metrics = latency_summary(thread_id, ops_per_thread, duration_ns, &mut latencies_ns);

    counter_measured.incrby_ops = ops_per_thread;
    counter_measured.applied_increments = ops_per_thread;

    Ok(ThreadOutcome {
        result: metrics,
        latencies_ns,
        counter_warmup,
        counter_measured,
    })
}

fn execute_operation(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    spec: WorkloadSpec,
    value: &str,
    num_keys: u64,
    multi_key_width: usize,
    key_buffer: &mut Vec<u64>,
    command_buffer: &mut Vec<u8>,
    rng: &mut StdRng,
    counter_stats: Option<&mut CounterOperationStats>,
) -> io::Result<()> {
    if let Some(counter) = spec.counter {
        return execute_counter_operation(
            writer,
            reader,
            counter,
            value,
            rng,
            counter_stats.expect("counter workload records counter stats"),
        );
    }

    if spec.transactional {
        return execute_transaction(writer, reader, spec, value, num_keys, multi_key_width, rng);
    }
    if spec.multi_key {
        let width = multi_key_width.max(1);
        fill_next_keys(spec, num_keys, rng, width, key_buffer);
        if is_read(spec, rng) {
            return execute_mget_keys(writer, reader, key_buffer.as_slice(), command_buffer);
        }
        return match spec.multi_key_write {
            MultiKeyWriteCommand::Mset => execute_mset_keys(
                writer,
                reader,
                key_buffer.as_slice(),
                value.as_bytes(),
                command_buffer,
            ),
            MultiKeyWriteCommand::Msetnx => execute_msetnx_keys(
                writer,
                reader,
                key_buffer.as_slice(),
                value.as_bytes(),
                command_buffer,
            ),
        };
    }
    let key = next_key(spec, num_keys, rng);
    if is_read(spec, rng) {
        execute_command(writer, reader, &["GET".to_string(), key_name(key)])
    } else {
        execute_command(
            writer,
            reader,
            &["SET".to_string(), key_name(key), value.to_string()],
        )
    }
}

fn execute_counter_operation(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    counter: CounterKind,
    value: &str,
    rng: &mut StdRng,
    stats: &mut CounterOperationStats,
) -> io::Result<()> {
    let key = counter_key(0);
    match counter {
        CounterKind::Hot => {
            execute_counter_incr(writer, reader, key)?;
            stats.incrby_ops += 1;
            stats.applied_increments += 1;
            Ok(())
        }
        CounterKind::HotWithGet => {
            if rng.gen_ratio(1, 10) {
                execute_command(writer, reader, &["GET".to_string(), key])?;
                stats.get_ops += 1;
            } else {
                execute_counter_incr(writer, reader, key)?;
                stats.incrby_ops += 1;
                stats.applied_increments += 1;
            }
            Ok(())
        }
        CounterKind::HotWithSetDel => {
            let roll = rng.gen_range(0..100);
            if roll < 80 {
                execute_counter_incr(writer, reader, key)?;
                stats.incrby_ops += 1;
                stats.applied_increments += 1;
            } else if roll < 90 {
                execute_command(writer, reader, &["SET".to_string(), key, "0".to_string()])?;
                stats.set_ops += 1;
                stats.barrier_ops += 1;
            } else {
                execute_command(writer, reader, &["DEL".to_string(), key])?;
                stats.del_ops += 1;
                stats.barrier_ops += 1;
            }
            Ok(())
        }
        CounterKind::HotTtl => {
            if rng.gen_ratio(1, 10) {
                execute_command(writer, reader, &["TTL".to_string(), key])?;
                stats.ttl_ops += 1;
            } else {
                execute_counter_incr(writer, reader, key)?;
                stats.incrby_ops += 1;
                stats.applied_increments += 1;
            }
            Ok(())
        }
        CounterKind::HotTransaction => execute_counter_transaction(writer, reader, false, stats),
        CounterKind::HotWatchTransaction => {
            execute_counter_transaction(writer, reader, true, stats)
        }
        CounterKind::HotMultiKey => {
            let roll = rng.gen_range(0..100);
            let side_key = counter_key(1);
            if roll < 80 {
                execute_counter_incr(writer, reader, key)?;
                stats.incrby_ops += 1;
                stats.applied_increments += 1;
            } else if roll < 90 {
                execute_command(writer, reader, &["MGET".to_string(), key, side_key])?;
                stats.mget_ops += 1;
            } else {
                execute_command(
                    writer,
                    reader,
                    &[
                        "MSET".to_string(),
                        key,
                        "0".to_string(),
                        side_key,
                        value.to_string(),
                    ],
                )?;
                stats.mset_ops += 1;
                stats.barrier_ops += 1;
            }
            Ok(())
        }
    }
}

fn execute_counter_incr(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    key: String,
) -> io::Result<()> {
    execute_command(
        writer,
        reader,
        &["INCRBY".to_string(), key, "1".to_string()],
    )
}

fn execute_counter_incr_pipeline(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    total: u64,
    pipeline_depth: usize,
) -> io::Result<()> {
    if total == 0 {
        return Ok(());
    }

    let key = counter_key(0);
    let command = ["INCRBY", key.as_str(), "1"];
    let mut remaining = total;
    while remaining > 0 {
        let batch = remaining.min(pipeline_depth as u64);
        for _ in 0..batch {
            write_command_unflushed(writer, &command)?;
        }
        writer.flush()?;
        for _ in 0..batch {
            read_response(reader)?;
        }
        remaining -= batch;
    }
    Ok(())
}

fn execute_counter_transaction(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    watch: bool,
    stats: &mut CounterOperationStats,
) -> io::Result<()> {
    let key = counter_key(0);
    stats.transaction_attempts += 1;
    if watch {
        execute_command(writer, reader, &["WATCH".to_string(), key.clone()])?;
    }
    execute_command(writer, reader, &["MULTI".to_string()])?;
    execute_command(
        writer,
        reader,
        &["INCRBY".to_string(), key, "1".to_string()],
    )?;
    match execute_command_value(writer, reader, &["EXEC".to_string()])? {
        RespValue::Array(None) => {
            stats.transaction_aborts += 1;
            Ok(())
        }
        RespValue::Array(Some(_)) => {
            stats.transaction_commits += 1;
            stats.applied_increments += 1;
            Ok(())
        }
        response => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected EXEC response for counter transaction: {response:?}"),
        )),
    }
}

fn execute_transaction(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    spec: WorkloadSpec,
    value: &str,
    num_keys: u64,
    multi_key_width: usize,
    rng: &mut StdRng,
) -> io::Result<()> {
    if spec.multi_key {
        let width = multi_key_width.max(1);
        let keys = next_keys(spec, num_keys, rng, width);
        let mut watch_parts = Vec::with_capacity(width + 1);
        watch_parts.push("WATCH".to_string());
        for key in &keys {
            watch_parts.push(key_name(*key));
        }
        execute_command(writer, reader, &watch_parts)?;
        execute_command(writer, reader, &["MULTI".to_string()])?;
        let mut mget_parts = Vec::with_capacity(width + 1);
        mget_parts.push("MGET".to_string());
        for key in &keys {
            mget_parts.push(key_name(*key));
        }
        execute_command(writer, reader, &mget_parts)?;

        let mut mset_parts = Vec::with_capacity(width * 2 + 1);
        mset_parts.push("MSET".to_string());
        for key in keys {
            mset_parts.push(key_name(key));
            mset_parts.push(value.to_string());
        }
        execute_command(writer, reader, &mset_parts)?;
        return execute_command(writer, reader, &["EXEC".to_string()]);
    }

    let key = next_key(spec, num_keys, rng);
    execute_command(writer, reader, &["WATCH".to_string(), key_name(key)])?;
    execute_command(writer, reader, &["MULTI".to_string()])?;
    execute_command(writer, reader, &["GET".to_string(), key_name(key)])?;
    execute_command(
        writer,
        reader,
        &["SET".to_string(), key_name(key), value.to_string()],
    )?;
    execute_command(writer, reader, &["EXEC".to_string()])
}

fn prepare_workload(
    host: &str,
    port: u16,
    spec: WorkloadSpec,
    num_keys: u64,
    value_size: usize,
) -> io::Result<()> {
    match spec.counter {
        Some(counter) => flush_and_preload_counter(host, port, counter),
        None => flush_and_preload(host, port, num_keys, value_size),
    }
}

fn flush_and_preload_counter(host: &str, port: u16, counter: CounterKind) -> io::Result<()> {
    let mut writer = TcpStream::connect((host, port))?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    execute_command(&mut writer, &mut reader, &["FLUSHALL".to_string()])?;

    match counter {
        CounterKind::HotTtl => execute_command(
            &mut writer,
            &mut reader,
            &[
                "SETEX".to_string(),
                counter_key(0),
                "3600".to_string(),
                "0".to_string(),
            ],
        )?,
        CounterKind::HotMultiKey => {
            execute_command(
                &mut writer,
                &mut reader,
                &["SET".to_string(), counter_key(0), "0".to_string()],
            )?;
            execute_command(
                &mut writer,
                &mut reader,
                &["SET".to_string(), counter_key(1), "side".to_string()],
            )?;
        }
        _ => execute_command(
            &mut writer,
            &mut reader,
            &["SET".to_string(), counter_key(0), "0".to_string()],
        )?,
    }

    Ok(())
}

fn build_counter_benchmark_stats(
    host: &str,
    port: u16,
    counter: Option<CounterKind>,
    warmup: CounterOperationStats,
    measured: CounterOperationStats,
) -> io::Result<Option<CounterBenchmarkStats>> {
    let Some(kind) = counter else {
        return Ok(None);
    };
    let mut total = warmup.clone();
    total.add_assign(&measured);
    let validation = validate_counter_workload(host, port, kind, &total)?;
    Ok(Some(CounterBenchmarkStats {
        kind: kind.as_str(),
        warmup,
        measured,
        total,
        validation,
    }))
}

fn validate_counter_workload(
    host: &str,
    port: u16,
    kind: CounterKind,
    total: &CounterOperationStats,
) -> io::Result<CounterValidation> {
    let mut status = "passed";
    let mut notes = Vec::new();
    let mut expected_final_value = None;
    let mut final_value = None;
    let mut final_value_matches = None;
    let mut ttl_seconds = None;
    let mut ttl_live = None;

    if kind.validates_final_value() {
        let expected = i64::try_from(total.applied_increments).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "applied increment count exceeded i64 range",
            )
        })?;
        let actual = read_counter_value(host, port)?;
        let matches = actual == Some(expected);
        expected_final_value = Some(expected);
        final_value = actual;
        final_value_matches = Some(matches);
        if !matches {
            status = "failed";
            notes.push("final counter value does not match applied increment count".to_string());
        }
    } else {
        status = "not_applicable";
        notes.push(
            "final counter value is not deterministic because this workload includes barrier writes"
                .to_string(),
        );
    }

    if kind.validates_live_ttl() {
        let ttl = read_counter_ttl(host, port)?;
        let live = ttl > 0;
        ttl_seconds = Some(ttl);
        ttl_live = Some(live);
        if !live {
            status = "failed";
            notes.push("counter key TTL was not live after the run".to_string());
        }
    }

    Ok(CounterValidation {
        status,
        expected_final_value,
        final_value,
        final_value_matches,
        ttl_seconds,
        ttl_live,
        notes,
    })
}

fn read_counter_value(host: &str, port: u16) -> io::Result<Option<i64>> {
    let mut writer = TcpStream::connect((host, port))?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    match execute_command_value(
        &mut writer,
        &mut reader,
        &["GET".to_string(), counter_key(0)],
    )? {
        RespValue::Bulk(Some(value)) => value.parse::<i64>().map(Some).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("counter value was not an integer: {error}"),
            )
        }),
        RespValue::Bulk(None) => Ok(None),
        response => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected GET response for counter value: {response:?}"),
        )),
    }
}

fn read_counter_ttl(host: &str, port: u16) -> io::Result<i64> {
    let mut writer = TcpStream::connect((host, port))?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    match execute_command_value(
        &mut writer,
        &mut reader,
        &["TTL".to_string(), counter_key(0)],
    )? {
        RespValue::Integer(value) => Ok(value),
        response => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected TTL response for counter key: {response:?}"),
        )),
    }
}

fn flush_and_preload(host: &str, port: u16, num_keys: u64, value_size: usize) -> io::Result<()> {
    let mut writer = TcpStream::connect((host, port))?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    execute_command(&mut writer, &mut reader, &["FLUSHALL".to_string()])?;

    let value = "x".repeat(value_size);
    for key_id in 1..=num_keys {
        execute_command(
            &mut writer,
            &mut reader,
            &["SET".to_string(), key_name(key_id), value.clone()],
        )?;
    }
    Ok(())
}

fn encode_command(parts: &[&str]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(b"*");
    output.extend_from_slice(parts.len().to_string().as_bytes());
    output.extend_from_slice(b"\r\n");
    for part in parts {
        output.extend_from_slice(b"$");
        output.extend_from_slice(part.len().to_string().as_bytes());
        output.extend_from_slice(b"\r\n");
        output.extend_from_slice(part.as_bytes());
        output.extend_from_slice(b"\r\n");
    }
    output
}

fn execute_command(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    parts: &[String],
) -> io::Result<()> {
    write_command(writer, parts)?;
    read_response(reader)
}

fn execute_command_value(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    parts: &[String],
) -> io::Result<RespValue> {
    write_command(writer, parts)?;
    read_response_value(reader)
}

fn execute_pressure_command(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    parts: &[String],
) -> io::Result<()> {
    write_command(writer, parts)?;
    read_response_allow_error(reader)
}

fn write_command(writer: &mut TcpStream, parts: &[String]) -> io::Result<()> {
    write_command_unflushed(writer, parts)?;
    writer.flush()
}

fn write_command_unflushed<S: AsRef<str>>(writer: &mut TcpStream, parts: &[S]) -> io::Result<()> {
    write!(writer, "*{}\r\n", parts.len())?;
    for part in parts {
        let part = part.as_ref();
        write!(writer, "${}\r\n", part.as_bytes().len())?;
        writer.write_all(part.as_bytes())?;
        writer.write_all(b"\r\n")?;
    }
    Ok(())
}

fn execute_mget_keys(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    keys: &[u64],
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    encode_mget_command(keys, buffer);
    writer.write_all(buffer)?;
    writer.flush()?;
    read_response(reader)
}

fn execute_mset_keys(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    keys: &[u64],
    value: &[u8],
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    encode_mset_command(keys, value, buffer);
    writer.write_all(buffer)?;
    writer.flush()?;
    read_response(reader)
}

fn execute_msetnx_keys(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    keys: &[u64],
    value: &[u8],
    buffer: &mut Vec<u8>,
) -> io::Result<()> {
    encode_msetnx_command(keys, value, buffer);
    writer.write_all(buffer)?;
    writer.flush()?;
    read_response(reader)
}

fn encode_mget_command(keys: &[u64], buffer: &mut Vec<u8>) {
    buffer.clear();
    append_array_header(buffer, keys.len() + 1);
    append_bulk_bytes(buffer, b"MGET");
    for key in keys {
        append_prefixed_key_bulk(buffer, KEY_PREFIX, *key);
    }
}

fn encode_mset_command(keys: &[u64], value: &[u8], buffer: &mut Vec<u8>) {
    encode_multiset_command(b"MSET", KEY_PREFIX, keys, value, buffer);
}

fn encode_msetnx_command(keys: &[u64], value: &[u8], buffer: &mut Vec<u8>) {
    encode_multiset_command(b"MSETNX", MSETNX_KEY_PREFIX, keys, value, buffer);
}

fn encode_multiset_command(
    command: &[u8],
    key_prefix: &str,
    keys: &[u64],
    value: &[u8],
    buffer: &mut Vec<u8>,
) {
    buffer.clear();
    append_array_header(buffer, keys.len() * 2 + 1);
    append_bulk_bytes(buffer, command);
    for key in keys {
        append_prefixed_key_bulk(buffer, key_prefix, *key);
        append_bulk_bytes(buffer, value);
    }
}

fn append_array_header(buffer: &mut Vec<u8>, item_count: usize) {
    write!(buffer, "*{item_count}\r\n").expect("writing to Vec cannot fail");
}

fn append_bulk_bytes(buffer: &mut Vec<u8>, value: &[u8]) {
    write!(buffer, "${}\r\n", value.len()).expect("writing to Vec cannot fail");
    buffer.extend_from_slice(value);
    buffer.extend_from_slice(b"\r\n");
}

fn append_prefixed_key_bulk(buffer: &mut Vec<u8>, prefix: &str, key_id: u64) {
    write!(
        buffer,
        "${}\r\n{prefix}{key_id}\r\n",
        prefix.len() + decimal_len(key_id)
    )
    .expect("writing to Vec cannot fail");
}

fn decimal_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 10 {
        value /= 10;
        len += 1;
    }
    len
}

fn estimate_multi_key_command_bytes(width: usize, value_size: usize) -> usize {
    let width = width.max(1);
    32 + width * (KEY_PREFIX.len() + value_size + 48)
}

#[derive(Debug)]
enum RespValue {
    Simple,
    Integer(i64),
    Bulk(Option<String>),
    Array(Option<Vec<RespValue>>),
}

fn read_response(reader: &mut BufReader<TcpStream>) -> io::Result<()> {
    let mut prefix = [0_u8; 1];
    reader.read_exact(&mut prefix)?;
    match prefix[0] {
        b'+' | b':' => {
            let _ = read_line(reader)?;
            Ok(())
        }
        b'-' => {
            let message = read_line(reader)?;
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("server returned error: {message}"),
            ))
        }
        b'$' => {
            let length = parse_i64(read_line(reader)?)?;
            if length >= 0 {
                let mut payload = vec![0_u8; length as usize + 2];
                reader.read_exact(&mut payload)?;
            }
            Ok(())
        }
        b'*' => {
            let length = parse_i64(read_line(reader)?)?;
            if length >= 0 {
                for _ in 0..length {
                    read_response(reader)?;
                }
            }
            Ok(())
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported RESP prefix byte: {}", other as char),
        )),
    }
}

fn read_response_value(reader: &mut BufReader<TcpStream>) -> io::Result<RespValue> {
    let mut prefix = [0_u8; 1];
    reader.read_exact(&mut prefix)?;
    match prefix[0] {
        b'+' => {
            let _ = read_line(reader)?;
            Ok(RespValue::Simple)
        }
        b':' => Ok(RespValue::Integer(parse_i64(read_line(reader)?)?)),
        b'-' => {
            let message = read_line(reader)?;
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("server returned error: {message}"),
            ))
        }
        b'$' => {
            let length = parse_i64(read_line(reader)?)?;
            if length < 0 {
                return Ok(RespValue::Bulk(None));
            }
            let mut payload = vec![0_u8; length as usize];
            reader.read_exact(&mut payload)?;
            let mut crlf = [0_u8; 2];
            reader.read_exact(&mut crlf)?;
            if crlf != *b"\r\n" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bulk string was not terminated by CRLF",
                ));
            }
            let value = String::from_utf8(payload).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("bulk string was not valid UTF-8: {error}"),
                )
            })?;
            Ok(RespValue::Bulk(Some(value)))
        }
        b'*' => {
            let length = parse_i64(read_line(reader)?)?;
            if length < 0 {
                return Ok(RespValue::Array(None));
            }
            let mut items = Vec::with_capacity(length as usize);
            for _ in 0..length {
                items.push(read_response_value(reader)?);
            }
            Ok(RespValue::Array(Some(items)))
        }
        b'_' => {
            let _ = read_line(reader)?;
            Ok(RespValue::Bulk(None))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported RESP prefix byte: {}", other as char),
        )),
    }
}

fn read_response_allow_error(reader: &mut BufReader<TcpStream>) -> io::Result<()> {
    let mut prefix = [0_u8; 1];
    reader.read_exact(&mut prefix)?;
    match prefix[0] {
        b'+' | b':' | b'-' => {
            let _ = read_line(reader)?;
            Ok(())
        }
        b'$' => {
            let length = parse_i64(read_line(reader)?)?;
            if length >= 0 {
                let mut payload = vec![0_u8; length as usize + 2];
                reader.read_exact(&mut payload)?;
            }
            Ok(())
        }
        b'*' => {
            let length = parse_i64(read_line(reader)?)?;
            if length >= 0 {
                for _ in 0..length {
                    read_response_allow_error(reader)?;
                }
            }
            Ok(())
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported RESP prefix byte: {}", other as char),
        )),
    }
}

fn read_line(reader: &mut BufReader<TcpStream>) -> io::Result<String> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if !line.ends_with("\r\n") {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "RESP line did not end with CRLF",
        ));
    }
    line.truncate(line.len() - 2);
    Ok(line)
}

fn parse_i64(value: String) -> io::Result<i64> {
    value.parse::<i64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse integer '{value}': {error}"),
        )
    })
}

fn is_read(spec: WorkloadSpec, rng: &mut StdRng) -> bool {
    if spec.read_weight == 0 {
        return false;
    }
    if spec.write_weight == 0 {
        return true;
    }
    let threshold = spec.read_weight + spec.write_weight;
    rng.gen_range(0..threshold) < spec.read_weight
}

fn next_key(spec: WorkloadSpec, num_keys: u64, rng: &mut StdRng) -> u64 {
    if spec.hot_key && num_keys > 1 && rng.gen_ratio(4, 5) {
        let hot_limit = std::cmp::max(1, num_keys / 100);
        rng.gen_range(1..=hot_limit)
    } else {
        rng.gen_range(1..=num_keys)
    }
}

fn next_keys(spec: WorkloadSpec, num_keys: u64, rng: &mut StdRng, count: usize) -> Vec<u64> {
    (0..count)
        .map(|_| next_key(spec, num_keys, rng))
        .collect::<Vec<u64>>()
}

fn fill_next_keys(
    spec: WorkloadSpec,
    num_keys: u64,
    rng: &mut StdRng,
    count: usize,
    output: &mut Vec<u64>,
) {
    output.clear();
    output.reserve(count);
    for _ in 0..count {
        output.push(next_key(spec, num_keys, rng));
    }
}

fn next_key_for_latency(num_keys: u64, rng: &mut StdRng) -> u64 {
    rng.gen_range(1..=num_keys.max(1))
}

fn key_name(id: u64) -> String {
    format!("{KEY_PREFIX}{id}")
}

fn counter_key(id: u64) -> String {
    format!("bench:counter:{id}")
}

fn latency_summary(
    thread_id: usize,
    ops_completed: u64,
    duration_ns: u64,
    latencies_ns: &mut [u64],
) -> ThreadResult {
    latencies_ns.sort_unstable();
    let throughput_ops_sec = if duration_ns == 0 {
        0.0
    } else {
        ops_completed as f64 / (duration_ns as f64 / 1_000_000_000.0)
    };

    ThreadResult {
        thread_id,
        ops_completed,
        duration_ns,
        throughput_ops_sec,
        p50_ns: percentile(latencies_ns, 0.50),
        p95_ns: percentile(latencies_ns, 0.95),
        p99_ns: percentile(latencies_ns, 0.99),
        p99_9_ns: percentile(latencies_ns, 0.999),
        p99_999_ns: percentile(latencies_ns, 0.99999),
        max_ns: latencies_ns.last().copied().unwrap_or(0),
        mean_ns: mean_latency(latencies_ns),
    }
}

fn percentile(values: &[u64], quantile: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    values[index.min(values.len() - 1)]
}

fn mean_latency(values: &[u64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let total = values.iter().map(|value| *value as u128).sum::<u128>();
    total as f64 / values.len() as f64
}

fn normalize_workload_name(name: &str) -> String {
    name.trim()
        .to_ascii_lowercase()
        .replace('_', "-")
        .replace(' ', "-")
}

fn resolve_workload(name: &str) -> Result<WorkloadSpec, Box<dyn Error>> {
    let normalized = normalize_workload_name(name);
    let spec = match normalized.as_str() {
        "uniform-read-only" => {
            WorkloadSpec::standard("uniform-read_only", 100, 0, false, false, false)
        }
        "uniform-read-heavy" => {
            WorkloadSpec::standard("uniform-read_heavy", 80, 20, false, false, false)
        }
        "uniform-mixed" => WorkloadSpec::standard("uniform-mixed", 50, 50, false, false, false),
        "uniform-write-heavy" => {
            WorkloadSpec::standard("uniform-write_heavy", 20, 80, false, false, false)
        }
        "uniform-write-only" => {
            WorkloadSpec::standard("uniform-write_only", 0, 100, false, false, false)
        }
        "zipfian-read-heavy" => {
            WorkloadSpec::standard("zipfian-read_heavy", 90, 10, false, false, true)
        }
        "zipfian-mixed" => WorkloadSpec::standard("zipfian-mixed", 70, 30, false, false, true),
        "hot-key" => WorkloadSpec::standard("hot-key", 85, 15, false, false, true),
        "hot-counter" => {
            WorkloadSpec::counter("hot_counter", 0, 100, false, false, CounterKind::Hot)
        }
        "hot-counter-with-get" => WorkloadSpec::counter(
            "hot_counter_with_get",
            10,
            90,
            false,
            false,
            CounterKind::HotWithGet,
        ),
        "hot-counter-with-set-del" => WorkloadSpec::counter(
            "hot_counter_with_set_del",
            0,
            100,
            false,
            false,
            CounterKind::HotWithSetDel,
        ),
        "hot-counter-ttl" => {
            WorkloadSpec::counter("hot_counter_ttl", 10, 90, false, false, CounterKind::HotTtl)
        }
        "hot-counter-transaction" => WorkloadSpec::counter(
            "hot_counter_transaction",
            0,
            100,
            false,
            true,
            CounterKind::HotTransaction,
        ),
        "hot-counter-watch-transaction" => WorkloadSpec::counter(
            "hot_counter_watch_transaction",
            0,
            100,
            false,
            true,
            CounterKind::HotWatchTransaction,
        ),
        "hot-counter-multikey" => WorkloadSpec::counter(
            "hot_counter_multikey",
            10,
            90,
            true,
            false,
            CounterKind::HotMultiKey,
        ),
        "single-key-mixed" => {
            WorkloadSpec::standard("single_key_mixed", 60, 40, false, false, false)
        }
        "multi-key-operations" => {
            WorkloadSpec::standard("multi-key operations", 50, 50, true, false, false)
        }
        "multi-key-only" => WorkloadSpec::standard("multi_key_only", 50, 50, true, false, false),
        "mget-only" => WorkloadSpec::standard("mget_only", 100, 0, true, false, false),
        "mset-only" => WorkloadSpec::standard("mset_only", 0, 100, true, false, false),
        "msetnx-only" => WorkloadSpec {
            multi_key_write: MultiKeyWriteCommand::Msetnx,
            ..WorkloadSpec::standard("msetnx_only", 0, 100, true, false, false)
        },
        "transaction" => WorkloadSpec::standard("transaction", 50, 50, false, true, false),
        "transaction-only" => {
            WorkloadSpec::standard("transaction_only", 50, 50, false, true, false)
        }
        "single-key-tx-mixed" => {
            WorkloadSpec::standard("single_key_tx_mixed", 55, 45, false, true, false)
        }
        "multi-key-tx-mixed" => {
            WorkloadSpec::standard("multi_key_tx_mixed", 55, 45, true, true, false)
        }
        "pressure-slow-reader" => {
            WorkloadSpec::pressure("pressure-slow_reader", PressureKind::SlowReader)
        }
        "pressure-deep-pipeline" => {
            WorkloadSpec::pressure("pressure-deep_pipeline", PressureKind::DeepPipeline)
        }
        "pressure-large-bulk" => {
            WorkloadSpec::pressure("pressure-large_bulk", PressureKind::LargeBulk)
        }
        "pressure-aof-backlog" => {
            WorkloadSpec::pressure("pressure-aof_backlog", PressureKind::AofBacklog)
        }
        "pressure-ttl-expiry" => {
            WorkloadSpec::pressure("pressure-ttl_expiry", PressureKind::TtlExpiry)
        }
        "pressure-eviction-pressure" | "pressure-eviction" => {
            WorkloadSpec::pressure("pressure-eviction_pressure", PressureKind::EvictionPressure)
        }
        "pressure-connection-storm" => {
            WorkloadSpec::pressure("pressure-connection_storm", PressureKind::ConnectionStorm)
        }
        "pressure-close-storm" => {
            WorkloadSpec::pressure("pressure-close_storm", PressureKind::CloseStorm)
        }
        _ => {
            return Err(format!("unsupported workload for custom-loadgen: {name}").into());
        }
    };
    Ok(spec)
}

fn sanitize_identifier(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_phase7_counter_workloads() {
        let cases = [
            ("hot_counter", "hot_counter", CounterKind::Hot),
            (
                "hot_counter_with_get",
                "hot_counter_with_get",
                CounterKind::HotWithGet,
            ),
            (
                "hot_counter_with_set_del",
                "hot_counter_with_set_del",
                CounterKind::HotWithSetDel,
            ),
            ("hot_counter_ttl", "hot_counter_ttl", CounterKind::HotTtl),
            (
                "hot_counter_transaction",
                "hot_counter_transaction",
                CounterKind::HotTransaction,
            ),
            (
                "hot_counter_watch_transaction",
                "hot_counter_watch_transaction",
                CounterKind::HotWatchTransaction,
            ),
            (
                "hot_counter_multikey",
                "hot_counter_multikey",
                CounterKind::HotMultiKey,
            ),
        ];

        for (input, canonical, counter) in cases {
            let spec = resolve_workload(input).expect("counter workload should resolve");
            assert_eq!(spec.canonical_name, canonical);
            assert!(matches!(spec.counter, Some(actual) if actual == counter));
            assert!(spec.pressure.is_none());
            assert!(spec.hot_key);
        }
    }

    #[test]
    fn resolves_width_configurable_multikey_point_workloads() {
        let cases = [
            (
                "mget_only",
                "mget_only",
                100,
                0,
                MultiKeyWriteCommand::Mset,
            ),
            ("mset_only", "mset_only", 0, 100, MultiKeyWriteCommand::Mset),
            (
                "msetnx_only",
                "msetnx_only",
                0,
                100,
                MultiKeyWriteCommand::Msetnx,
            ),
        ];

        for (input, canonical, read_weight, write_weight, write_command) in cases {
            let spec = resolve_workload(input).expect("multi-key point workload should resolve");
            assert_eq!(spec.canonical_name, canonical);
            assert_eq!(spec.read_weight, read_weight);
            assert_eq!(spec.write_weight, write_weight);
            assert_eq!(spec.multi_key_write, write_command);
            assert!(spec.multi_key);
            assert!(!spec.transactional);
            assert!(spec.counter.is_none());
            assert!(spec.pressure.is_none());
        }
    }

    #[test]
    fn encodes_multikey_commands_into_single_resp_buffer() {
        let mut buffer = Vec::new();

        encode_mset_command(&[1, 23], b"xx", &mut buffer);
        assert_eq!(
            std::str::from_utf8(&buffer).expect("RESP should be utf8 for this test"),
            "*5\r\n$4\r\nMSET\r\n$11\r\nbench:key:1\r\n$2\r\nxx\r\n$12\r\nbench:key:23\r\n$2\r\nxx\r\n"
        );

        encode_mget_command(&[1, 23], &mut buffer);
        assert_eq!(
            std::str::from_utf8(&buffer).expect("RESP should be utf8 for this test"),
            "*3\r\n$4\r\nMGET\r\n$11\r\nbench:key:1\r\n$12\r\nbench:key:23\r\n"
        );

        encode_msetnx_command(&[1], b"v", &mut buffer);
        assert_eq!(
            std::str::from_utf8(&buffer).expect("RESP should be utf8 for this test"),
            "*3\r\n$6\r\nMSETNX\r\n$14\r\nbench:msetnx:1\r\n$1\r\nv\r\n"
        );
    }

    #[test]
    fn aggregates_counter_stats_without_shared_state() {
        let mut left = CounterOperationStats {
            incrby_ops: 1,
            get_ops: 2,
            transaction_commits: 3,
            applied_increments: 4,
            ..CounterOperationStats::default()
        };
        let right = CounterOperationStats {
            incrby_ops: 5,
            get_ops: 6,
            transaction_aborts: 7,
            applied_increments: 8,
            barrier_ops: 9,
            ..CounterOperationStats::default()
        };

        left.add_assign(&right);

        assert_eq!(left.incrby_ops, 6);
        assert_eq!(left.get_ops, 8);
        assert_eq!(left.transaction_commits, 3);
        assert_eq!(left.transaction_aborts, 7);
        assert_eq!(left.applied_increments, 12);
        assert_eq!(left.barrier_ops, 9);
    }
}
