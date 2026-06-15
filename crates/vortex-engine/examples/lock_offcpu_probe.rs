use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DEFAULT_DURATION_SECONDS: u64 = 5;
const DEFAULT_THREADS: usize = 4;
const DEFAULT_HOLD_MICROS: u64 = 2_000;

#[derive(Debug, Parser)]
#[command(about = "Run a synthetic lock-wait probe for lock/off-CPU profiling")]
struct Args {
    #[arg(long, default_value_t = DEFAULT_THREADS)]
    threads: usize,

    #[arg(long, default_value_t = DEFAULT_DURATION_SECONDS)]
    duration_seconds: u64,

    #[arg(long, default_value_t = DEFAULT_HOLD_MICROS)]
    hold_micros: u64,

    #[arg(long)]
    json: Option<PathBuf>,
}

#[derive(Clone, Copy)]
struct WorkerSummary {
    worker_index: usize,
    role: WorkerRole,
    operations: u64,
}

#[derive(Clone, Copy)]
enum WorkerRole {
    Holder,
    Contender,
}

impl WorkerRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Holder => "holder",
            Self::Contender => "contender",
        }
    }
}

struct ProbeSummary {
    threads: usize,
    duration_seconds: u64,
    hold_micros: u64,
    expected_classification: &'static str,
    total_operations: u64,
    holder_operations: u64,
    contender_operations: u64,
    workers: Vec<WorkerSummary>,
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
    if args.threads < 2 {
        return Err("--threads must be at least 2 so the probe can create lock contention".into());
    }
    if args.duration_seconds == 0 {
        return Err("--duration-seconds must be greater than zero".into());
    }
    if args.hold_micros == 0 {
        return Err("--hold-micros must be greater than zero".into());
    }

    Ok(())
}

fn run_probe(args: &Args) -> Result<ProbeSummary, String> {
    let shared_counter = Arc::new(Mutex::new(0_u64));
    let barrier = Arc::new(Barrier::new(args.threads));
    let mut handles = Vec::with_capacity(args.threads);

    for worker_index in 0..args.threads {
        let shared_counter = Arc::clone(&shared_counter);
        let barrier = Arc::clone(&barrier);
        let role = if worker_index == 0 {
            WorkerRole::Holder
        } else {
            WorkerRole::Contender
        };
        let duration = Duration::from_secs(args.duration_seconds);
        let hold_duration = Duration::from_micros(args.hold_micros);

        handles.push(thread::spawn(move || {
            run_worker(
                shared_counter,
                barrier,
                worker_index,
                role,
                duration,
                hold_duration,
            )
        }));
    }

    let mut workers = Vec::with_capacity(args.threads);
    for handle in handles {
        workers.push(
            handle
                .join()
                .map_err(|_| "lock_offcpu_probe worker thread panicked".to_string())?,
        );
    }

    let total_operations = workers.iter().map(|worker| worker.operations).sum::<u64>();
    let holder_operations = workers
        .iter()
        .filter(|worker| matches!(worker.role, WorkerRole::Holder))
        .map(|worker| worker.operations)
        .sum::<u64>();
    let contender_operations = workers
        .iter()
        .filter(|worker| matches!(worker.role, WorkerRole::Contender))
        .map(|worker| worker.operations)
        .sum::<u64>();

    Ok(ProbeSummary {
        threads: args.threads,
        duration_seconds: args.duration_seconds,
        hold_micros: args.hold_micros,
        expected_classification: "blocked lock/off-CPU",
        total_operations,
        holder_operations,
        contender_operations,
        workers,
    })
}

fn run_worker(
    shared_counter: Arc<Mutex<u64>>,
    barrier: Arc<Barrier>,
    worker_index: usize,
    role: WorkerRole,
    duration: Duration,
    hold_duration: Duration,
) -> WorkerSummary {
    barrier.wait();

    let deadline = Instant::now() + duration;
    let mut operations = 0_u64;

    while Instant::now() < deadline {
        let mut guard = match shared_counter.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard += 1;
        operations += 1;

        if matches!(role, WorkerRole::Holder) {
            thread::sleep(hold_duration);
        }
    }

    WorkerSummary {
        worker_index,
        role,
        operations,
    }
}

impl ProbeSummary {
    fn to_json(&self) -> String {
        let mut json = String::new();
        let _ = writeln!(json, "{{");
        let _ = writeln!(json, "  \"threads\": {},", self.threads);
        let _ = writeln!(json, "  \"duration_seconds\": {},", self.duration_seconds);
        let _ = writeln!(json, "  \"hold_micros\": {},", self.hold_micros);
        let _ = writeln!(
            json,
            "  \"expected_classification\": \"{}\",",
            self.expected_classification
        );
        let _ = writeln!(json, "  \"total_operations\": {},", self.total_operations);
        let _ = writeln!(json, "  \"holder_operations\": {},", self.holder_operations);
        let _ = writeln!(
            json,
            "  \"contender_operations\": {},",
            self.contender_operations
        );
        let _ = writeln!(json, "  \"workers\": [");

        for (index, worker) in self.workers.iter().enumerate() {
            let trailing_comma = if index + 1 == self.workers.len() {
                ""
            } else {
                ","
            };
            let _ = writeln!(
                json,
                "    {{ \"worker_index\": {}, \"role\": \"{}\", \"operations\": {} }}{}",
                worker.worker_index,
                worker.role.as_str(),
                worker.operations,
                trailing_comma,
            );
        }

        let _ = writeln!(json, "  ]");
        let _ = writeln!(json, "}}");
        json
    }
}
