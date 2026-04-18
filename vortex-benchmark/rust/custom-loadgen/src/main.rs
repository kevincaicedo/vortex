use std::error::Error;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;

const SEED: u64 = 0xDEADBEEF_CAFE_BABE;

#[derive(Parser, Debug, Clone)]
#[command(name = "custom-loadgen")]
#[command(about = "Run deterministic RESP/TCP workloads against an external Redis-compatible server")]
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
    #[arg(long)]
    output_dir: PathBuf,
}

#[derive(Clone, Copy)]
struct WorkloadSpec {
    canonical_name: &'static str,
    read_weight: u32,
    write_weight: u32,
    multi_key: bool,
    transactional: bool,
    hot_key: bool,
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
}

struct ThreadOutcome {
    result: ThreadResult,
    latencies_ns: Vec<u64>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    if cli.threads.is_empty() {
        return Err("at least one thread count is required".into());
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

fn run_workload(
    cli: &Cli,
    spec: WorkloadSpec,
    num_threads: usize,
) -> Result<(), Box<dyn Error>> {
    flush_and_preload(&cli.host, cli.port, cli.num_keys, cli.value_size)?;

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
        handles.push(thread::spawn(move || {
            run_thread(
                &host,
                port,
                spec,
                num_keys,
                value_size,
                ops_per_thread,
                warmup_ops,
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

    all_latencies.sort_unstable();
    let total_ops = thread_results.iter().map(|item| item.ops_completed).sum::<u64>();
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

fn run_thread(
    host: &str,
    port: u16,
    spec: WorkloadSpec,
    num_keys: u64,
    value_size: usize,
    ops_per_thread: u64,
    warmup_ops: u64,
    thread_id: usize,
    barrier: Arc<Barrier>,
) -> io::Result<ThreadOutcome> {
    let mut writer = TcpStream::connect((host, port))?;
    writer.set_nodelay(true)?;
    let mut reader = BufReader::new(writer.try_clone()?);
    let value = "x".repeat(value_size);
    let mut rng = StdRng::seed_from_u64(SEED.wrapping_add(thread_id as u64 * 104_729));

    for _ in 0..warmup_ops {
        execute_operation(&mut writer, &mut reader, spec, &value, num_keys, &mut rng)?;
    }

    barrier.wait();
    let start = Instant::now();
    let mut latencies_ns = Vec::with_capacity(ops_per_thread as usize);
    for _ in 0..ops_per_thread {
        let op_start = Instant::now();
        execute_operation(&mut writer, &mut reader, spec, &value, num_keys, &mut rng)?;
        latencies_ns.push(op_start.elapsed().as_nanos() as u64);
    }
    let duration_ns = start.elapsed().as_nanos() as u64;
    let metrics = latency_summary(thread_id, ops_per_thread, duration_ns, &mut latencies_ns);

    Ok(ThreadOutcome {
        result: metrics,
        latencies_ns,
    })
}

fn execute_operation(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    spec: WorkloadSpec,
    value: &str,
    num_keys: u64,
    rng: &mut StdRng,
) -> io::Result<()> {
    if spec.transactional {
        return execute_transaction(writer, reader, spec, value, num_keys, rng);
    }
    if spec.multi_key {
        if is_read(spec, rng) {
            let keys = next_keys(spec, num_keys, rng, 3);
            return execute_command(
                writer,
                reader,
                &[
                    "MGET".to_string(),
                    key_name(keys[0]),
                    key_name(keys[1]),
                    key_name(keys[2]),
                ],
            );
        }
        let keys = next_keys(spec, num_keys, rng, 3);
        return execute_command(
            writer,
            reader,
            &[
                "MSET".to_string(),
                key_name(keys[0]),
                value.to_string(),
                key_name(keys[1]),
                value.to_string(),
                key_name(keys[2]),
                value.to_string(),
            ],
        );
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

fn execute_transaction(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    spec: WorkloadSpec,
    value: &str,
    num_keys: u64,
    rng: &mut StdRng,
) -> io::Result<()> {
    if spec.multi_key {
        let keys = next_keys(spec, num_keys, rng, 3);
        execute_command(
            writer,
            reader,
            &[
                "WATCH".to_string(),
                key_name(keys[0]),
                key_name(keys[1]),
                key_name(keys[2]),
            ],
        )?;
        execute_command(writer, reader, &["MULTI".to_string()])?;
        execute_command(
            writer,
            reader,
            &[
                "MGET".to_string(),
                key_name(keys[0]),
                key_name(keys[1]),
                key_name(keys[2]),
            ],
        )?;
        execute_command(
            writer,
            reader,
            &[
                "MSET".to_string(),
                key_name(keys[0]),
                value.to_string(),
                key_name(keys[1]),
                value.to_string(),
                key_name(keys[2]),
                value.to_string(),
            ],
        )?;
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

fn execute_command(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    parts: &[String],
) -> io::Result<()> {
    write!(writer, "*{}\r\n", parts.len())?;
    for part in parts {
        write!(writer, "${}\r\n", part.as_bytes().len())?;
        writer.write_all(part.as_bytes())?;
        writer.write_all(b"\r\n")?;
    }
    writer.flush()?;
    read_response(reader)
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

fn key_name(id: u64) -> String {
    format!("bench:key:{id}")
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
        "uniform-read-only" => WorkloadSpec {
            canonical_name: "uniform-read_only",
            read_weight: 100,
            write_weight: 0,
            multi_key: false,
            transactional: false,
            hot_key: false,
        },
        "uniform-read-heavy" => WorkloadSpec {
            canonical_name: "uniform-read_heavy",
            read_weight: 80,
            write_weight: 20,
            multi_key: false,
            transactional: false,
            hot_key: false,
        },
        "uniform-mixed" => WorkloadSpec {
            canonical_name: "uniform-mixed",
            read_weight: 50,
            write_weight: 50,
            multi_key: false,
            transactional: false,
            hot_key: false,
        },
        "uniform-write-heavy" => WorkloadSpec {
            canonical_name: "uniform-write_heavy",
            read_weight: 20,
            write_weight: 80,
            multi_key: false,
            transactional: false,
            hot_key: false,
        },
        "uniform-write-only" => WorkloadSpec {
            canonical_name: "uniform-write_only",
            read_weight: 0,
            write_weight: 100,
            multi_key: false,
            transactional: false,
            hot_key: false,
        },
        "zipfian-read-heavy" => WorkloadSpec {
            canonical_name: "zipfian-read_heavy",
            read_weight: 90,
            write_weight: 10,
            multi_key: false,
            transactional: false,
            hot_key: true,
        },
        "zipfian-mixed" => WorkloadSpec {
            canonical_name: "zipfian-mixed",
            read_weight: 70,
            write_weight: 30,
            multi_key: false,
            transactional: false,
            hot_key: true,
        },
        "hot-key" => WorkloadSpec {
            canonical_name: "hot-key",
            read_weight: 85,
            write_weight: 15,
            multi_key: false,
            transactional: false,
            hot_key: true,
        },
        "single-key-mixed" => WorkloadSpec {
            canonical_name: "single_key_mixed",
            read_weight: 60,
            write_weight: 40,
            multi_key: false,
            transactional: false,
            hot_key: false,
        },
        "multi-key-operations" => WorkloadSpec {
            canonical_name: "multi-key operations",
            read_weight: 50,
            write_weight: 50,
            multi_key: true,
            transactional: false,
            hot_key: false,
        },
        "multi-key-only" => WorkloadSpec {
            canonical_name: "multi_key_only",
            read_weight: 50,
            write_weight: 50,
            multi_key: true,
            transactional: false,
            hot_key: false,
        },
        "transaction" => WorkloadSpec {
            canonical_name: "transaction",
            read_weight: 50,
            write_weight: 50,
            multi_key: false,
            transactional: true,
            hot_key: false,
        },
        "transaction-only" => WorkloadSpec {
            canonical_name: "transaction_only",
            read_weight: 50,
            write_weight: 50,
            multi_key: false,
            transactional: true,
            hot_key: false,
        },
        "single-key-tx-mixed" => WorkloadSpec {
            canonical_name: "single_key_tx_mixed",
            read_weight: 55,
            write_weight: 45,
            multi_key: false,
            transactional: true,
            hot_key: false,
        },
        "multi-key-tx-mixed" => WorkloadSpec {
            canonical_name: "multi_key_tx_mixed",
            read_weight: 55,
            write_weight: 45,
            multi_key: true,
            transactional: true,
            hot_key: false,
        },
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