use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use crossbeam_utils::CachePadded;
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DEFAULT_DURATION_SECONDS: u64 = 5;
const DEFAULT_THREADS: usize = 4;
const COUNTER_BATCH_SIZE: u64 = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CounterLayout {
    Unpadded,
    Padded,
}

impl CounterLayout {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Unpadded => "unpadded",
            Self::Padded => "padded",
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Run a synthetic false-sharing probe for perf c2c")]
struct Args {
    #[arg(long, value_enum, default_value_t = CounterLayout::Unpadded)]
    variant: CounterLayout,

    #[arg(long, default_value_t = DEFAULT_THREADS)]
    threads: usize,

    #[arg(long, default_value_t = DEFAULT_DURATION_SECONDS)]
    duration_seconds: u64,

    #[arg(long)]
    no_pin: bool,

    #[arg(long)]
    json: Option<PathBuf>,
}

trait CounterCell: Send + Sync + 'static {
    fn new_cell() -> Self;
    fn add_batch(&self, batch: u64);
    fn load(&self) -> u64;
    fn bytes_per_counter() -> usize;
}

impl CounterCell for AtomicU64 {
    fn new_cell() -> Self {
        Self::new(0)
    }

    fn add_batch(&self, batch: u64) {
        self.fetch_add(batch, Ordering::Relaxed);
    }

    fn load(&self) -> u64 {
        self.load(Ordering::Relaxed)
    }

    fn bytes_per_counter() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl CounterCell for CachePadded<AtomicU64> {
    fn new_cell() -> Self {
        Self::new(AtomicU64::new(0))
    }

    fn add_batch(&self, batch: u64) {
        AtomicU64::fetch_add(self, batch, Ordering::Relaxed);
    }

    fn load(&self) -> u64 {
        AtomicU64::load(self, Ordering::Relaxed)
    }

    fn bytes_per_counter() -> usize {
        std::mem::size_of::<Self>()
    }
}

#[derive(Clone, Copy)]
struct WorkerSummary {
    worker_index: usize,
    cpu: usize,
    pin_succeeded: bool,
    operations: u64,
}

struct ProbeSummary {
    variant: CounterLayout,
    threads: usize,
    duration_seconds: u64,
    pinning_enabled: bool,
    pinned_workers: usize,
    total_operations: u64,
    throughput_ops_per_second: f64,
    counters_total: u64,
    bytes_per_counter: usize,
    available_parallelism: usize,
    workers: Vec<WorkerSummary>,
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    validate_args(&args)?;

    let summary = match args.variant {
        CounterLayout::Unpadded => run_probe::<AtomicU64>(&args),
        CounterLayout::Padded => run_probe::<CachePadded<AtomicU64>>(&args),
    }?;
    let json = summary.to_json();

    if let Some(path) = &args.json {
        fs::write(path, json.as_bytes())
            .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    }

    println!("{json}");
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.threads < 2 {
        return Err(
            "--threads must be at least 2 so the probe can create inter-core sharing".into(),
        );
    }
    if args.duration_seconds == 0 {
        return Err("--duration-seconds must be greater than zero".into());
    }

    Ok(())
}

fn run_probe<T>(args: &Args) -> Result<ProbeSummary, String>
where
    T: CounterCell,
{
    let counters = build_counters::<T>(args.threads);
    let barrier = Arc::new(Barrier::new(args.threads));
    let available_parallelism = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(args.threads.max(1));
    let mut handles = Vec::with_capacity(args.threads);

    for worker_index in 0..args.threads {
        let counters = Arc::clone(&counters);
        let barrier = Arc::clone(&barrier);
        let pin_thread = !args.no_pin;
        let duration = Duration::from_secs(args.duration_seconds);
        let cpu = worker_index % available_parallelism.max(1);
        handles.push(thread::spawn(move || {
            run_worker(counters, barrier, worker_index, cpu, pin_thread, duration)
        }));
    }

    let mut workers = Vec::with_capacity(args.threads);
    for handle in handles {
        workers.push(
            handle
                .join()
                .map_err(|_| "false-sharing worker thread panicked".to_string())?,
        );
    }

    let total_operations = workers.iter().map(|worker| worker.operations).sum::<u64>();
    let counters_total = counters.iter().map(T::load).sum::<u64>();
    let pinned_workers = workers.iter().filter(|worker| worker.pin_succeeded).count();

    Ok(ProbeSummary {
        variant: args.variant,
        threads: args.threads,
        duration_seconds: args.duration_seconds,
        pinning_enabled: !args.no_pin,
        pinned_workers,
        total_operations,
        throughput_ops_per_second: total_operations as f64 / args.duration_seconds as f64,
        counters_total,
        bytes_per_counter: T::bytes_per_counter(),
        available_parallelism,
        workers,
    })
}

fn build_counters<T>(count: usize) -> Arc<[T]>
where
    T: CounterCell,
{
    let counters: Vec<T> = (0..count).map(|_| T::new_cell()).collect();
    Arc::from(counters.into_boxed_slice())
}

fn run_worker<T>(
    counters: Arc<[T]>,
    barrier: Arc<Barrier>,
    worker_index: usize,
    cpu: usize,
    pin_thread: bool,
    duration: Duration,
) -> WorkerSummary
where
    T: CounterCell,
{
    let pin_succeeded = if pin_thread {
        pin_current_thread(cpu).is_ok()
    } else {
        false
    };
    let counter = &counters[worker_index];

    barrier.wait();

    let started = Instant::now();
    let deadline = started + duration;
    let mut operations = 0_u64;

    while Instant::now() < deadline {
        counter.add_batch(COUNTER_BATCH_SIZE);
        operations += COUNTER_BATCH_SIZE;
        std::hint::spin_loop();
    }

    WorkerSummary {
        worker_index,
        cpu,
        pin_succeeded,
        operations,
    }
}

#[cfg(target_os = "linux")]
fn pin_current_thread(cpu: usize) -> Result<(), String> {
    let mut cpu_set = std::mem::MaybeUninit::<libc::cpu_set_t>::zeroed();

    // SAFETY: `cpu_set_t` is zero-initialized before the libc helpers mutate it,
    // and `sched_setaffinity` only reads the memory described by the provided size.
    unsafe {
        let cpu_set = cpu_set.assume_init_mut();
        libc::CPU_ZERO(cpu_set);
        libc::CPU_SET(cpu, cpu_set);

        let result = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), cpu_set);
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error().to_string())
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_cpu: usize) -> Result<(), String> {
    Err("thread affinity is only supported on Linux".into())
}

impl ProbeSummary {
    fn to_json(&self) -> String {
        let mut json = String::new();
        let _ = writeln!(json, "{{");
        let _ = writeln!(json, "  \"variant\": \"{}\",", self.variant.as_str());
        let _ = writeln!(json, "  \"threads\": {},", self.threads);
        let _ = writeln!(json, "  \"duration_seconds\": {},", self.duration_seconds);
        let _ = writeln!(json, "  \"pinning_enabled\": {},", self.pinning_enabled);
        let _ = writeln!(json, "  \"pinned_workers\": {},", self.pinned_workers);
        let _ = writeln!(json, "  \"total_operations\": {},", self.total_operations);
        let _ = writeln!(
            json,
            "  \"throughput_ops_per_second\": {:.2},",
            self.throughput_ops_per_second
        );
        let _ = writeln!(json, "  \"counters_total\": {},", self.counters_total);
        let _ = writeln!(json, "  \"bytes_per_counter\": {},", self.bytes_per_counter);
        let _ = writeln!(
            json,
            "  \"available_parallelism\": {},",
            self.available_parallelism
        );
        let _ = writeln!(json, "  \"workers\": [");

        for (index, worker) in self.workers.iter().enumerate() {
            let comma = if index + 1 == self.workers.len() {
                ""
            } else {
                ","
            };
            let _ = writeln!(
                json,
                "    {{ \"worker_index\": {}, \"cpu\": {}, \"pin_succeeded\": {}, \"operations\": {} }}{}",
                worker.worker_index, worker.cpu, worker.pin_succeeded, worker.operations, comma,
            );
        }

        let _ = writeln!(json, "  ]");
        let _ = write!(json, "}}");
        json
    }
}
