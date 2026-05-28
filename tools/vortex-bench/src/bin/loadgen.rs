//! `vortex-loadgen` — Redis-benchmark compatible load generator.
//!
//! Supports a subset of `redis-benchmark` flags:
//!   -h <hostname>   Server hostname (default: 127.0.0.1)
//!   -p <port>       Server port (default: 6379)
//!   -c <clients>    Number of parallel connections (default: 50)
//!   -n <requests>   Total number of requests (default: 100000)
//!   -P <pipeline>   Pipeline depth (default: 1)
//!   -t <tests>      Comma-separated list of tests: SET,GET,INCR,PING

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use clap::Parser;
use vortex_bench::LatencyRecorder;

#[derive(Parser)]
#[command(
    name = "vortex-loadgen",
    about = "Redis-benchmark compatible load generator"
)]
struct Args {
    /// Server hostname
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Server port
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Number of parallel connections
    #[arg(short, long, default_value_t = 50)]
    clients: usize,

    /// Total number of requests
    #[arg(short, long, default_value_t = 100_000)]
    requests: u64,

    /// Pipeline depth
    #[arg(short = 'P', long, default_value_t = 1)]
    pipeline: usize,

    /// Comma-separated test list (SET,GET,INCR,PING)
    #[arg(short, long, default_value = "SET,GET,PING")]
    tests: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoadTest {
    Set,
    Get,
    Incr,
    Ping,
}

impl LoadTest {
    fn parse(test: &str) -> Option<Self> {
        match test.trim().to_ascii_uppercase().as_str() {
            "SET" => Some(Self::Set),
            "GET" => Some(Self::Get),
            "INCR" => Some(Self::Incr),
            "PING" => Some(Self::Ping),
            _ => None,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Set => "SET",
            Self::Get => "GET",
            Self::Incr => "INCR",
            Self::Ping => "PING",
        }
    }

    fn build_command(self, idx: u64) -> Vec<u8> {
        match self {
            Self::Set => {
                format!("*3\r\n$3\r\nSET\r\n$12\r\nkey:{idx:08}\r\n$3\r\nval\r\n").into_bytes()
            }
            Self::Get => format!("*2\r\n$3\r\nGET\r\n$12\r\nkey:{idx:08}\r\n").into_bytes(),
            Self::Incr => b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n".to_vec(),
            Self::Ping => b"*1\r\n$4\r\nPING\r\n".to_vec(),
        }
    }
}

fn parse_tests(tests: &str) -> Result<Vec<LoadTest>, String> {
    let parsed = tests
        .split(',')
        .map(str::trim)
        .filter(|test| !test.is_empty())
        .map(|test| LoadTest::parse(test).ok_or_else(|| format!("unknown test: {test}")))
        .collect::<Result<Vec<_>, _>>()?;

    if parsed.is_empty() {
        return Err("at least one test must be specified".to_string());
    }

    Ok(parsed)
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.clients == 0 {
        return Err("--clients must be greater than zero".to_string());
    }
    if args.requests == 0 {
        return Err("--requests must be greater than zero".to_string());
    }
    if args.pipeline == 0 {
        return Err("--pipeline must be greater than zero".to_string());
    }
    Ok(())
}

fn client_request_range(total: u64, clients: usize, client_idx: usize) -> (u64, u64) {
    debug_assert!(clients > 0);
    debug_assert!(client_idx < clients);

    let base = total / clients as u64;
    let extra = total % clients as u64;
    let idx = client_idx as u64;
    let count = base + u64::from(idx < extra);
    let start = (base * idx) + idx.min(extra);
    (start, count)
}

fn drain_responses(stream: &mut TcpStream, count: usize) {
    // Simple approach: read until we've seen `count` \r\n-terminated lines.
    // This works for simple RESP replies (+OK, :1, $-1, etc.)
    let mut buf = [0u8; 8192];
    let mut lines_seen = 0;
    while lines_seen < count {
        let n = stream.read(&mut buf).unwrap_or(0);
        if n == 0 {
            break;
        }
        for &b in &buf[..n] {
            if b == b'\n' {
                lines_seen += 1;
            }
        }
    }
}

fn run_test(args: &Args, test: LoadTest) -> Result<(), String> {
    let total = args.requests;
    let clients = args.clients;
    let pipeline = args.pipeline;
    let completed = Arc::new(AtomicU64::new(0));
    let test_name = test.name();

    println!("===== {test_name} =====");
    println!(
        "  {} requests, {} clients, pipeline {}",
        total, clients, pipeline
    );

    let start = Instant::now();
    let mut handles = Vec::new();

    for client_idx in 0..clients {
        let host = args.host.clone();
        let port = args.port;
        let completed = Arc::clone(&completed);
        let (request_start, requests_for_client) = client_request_range(total, clients, client_idx);

        handles.push(thread::spawn(move || {
            let addr = format!("{host}:{port}");
            let mut stream = match TcpStream::connect(&addr) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Connection failed to {addr}: {e}");
                    return LatencyRecorder::new();
                }
            };

            let mut recorder = LatencyRecorder::new();
            let mut sent = 0u64;

            while sent < requests_for_client {
                let batch = pipeline.min((requests_for_client - sent) as usize);
                let mut payload = Vec::new();
                for j in 0..batch {
                    payload.extend_from_slice(&test.build_command(request_start + sent + j as u64));
                }

                let t0 = Instant::now();
                if stream.write_all(&payload).is_err() {
                    break;
                }
                drain_responses(&mut stream, batch);
                let elapsed_ns = t0.elapsed().as_nanos() as u64;

                // Record per-request latency
                let per_req = elapsed_ns / batch as u64;
                for _ in 0..batch {
                    recorder.record(per_req);
                }

                sent += batch as u64;
                completed.fetch_add(batch as u64, Ordering::Relaxed);
            }

            recorder
        }));
    }

    let mut combined = LatencyRecorder::new();
    for h in handles {
        let recorder = h
            .join()
            .map_err(|_| "loadgen worker thread panicked".to_string())?;
        let report = recorder.report();
        // Merge by re-recording summary stats (approximate)
        if report.count > 0 {
            combined.record(report.p50);
            combined.record(report.p99);
            combined.record(report.p999);
        }
    }

    let elapsed = start.elapsed();
    let total_done = completed.load(Ordering::Relaxed);
    let throughput = total_done as f64 / elapsed.as_secs_f64();
    let report = combined.report();

    println!(
        "  {total_done} requests completed in {:.3}s",
        elapsed.as_secs_f64()
    );
    println!("  Throughput: {throughput:.0} ops/sec");
    println!(
        "  Latency — p50: {}ns, p99: {}ns, p999: {}ns",
        report.p50, report.p99, report.p999
    );
    println!();
    Ok(())
}

fn main() -> ExitCode {
    let args = Args::parse();
    if let Err(error) = run(args) {
        eprintln!("error: {error}");
        return ExitCode::from(2);
    }
    ExitCode::SUCCESS
}

fn run(args: Args) -> Result<(), String> {
    validate_args(&args)?;
    let tests = parse_tests(&args.tests)?;
    println!("vortex-loadgen — connecting to {}:{}", args.host, args.port);
    println!();

    for test in tests {
        run_test(&args, test)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_with(clients: usize, requests: u64, pipeline: usize, tests: &str) -> Args {
        Args {
            host: "127.0.0.1".to_string(),
            port: 6379,
            clients,
            requests,
            pipeline,
            tests: tests.to_string(),
        }
    }

    #[test]
    fn parses_tests_case_insensitively() {
        assert_eq!(
            parse_tests("set, GET, ping").unwrap(),
            vec![LoadTest::Set, LoadTest::Get, LoadTest::Ping]
        );
    }

    #[test]
    fn rejects_unknown_or_empty_tests() {
        assert!(parse_tests("SET,NOPE").is_err());
        assert!(parse_tests(" , ").is_err());
    }

    #[test]
    fn rejects_zero_clients_or_pipeline() {
        assert!(validate_args(&args_with(0, 100, 1, "GET")).is_err());
        assert!(validate_args(&args_with(1, 100, 0, "GET")).is_err());
        assert!(validate_args(&args_with(1, 0, 1, "GET")).is_err());
        assert!(validate_args(&args_with(1, 100, 1, "GET")).is_ok());
    }

    #[test]
    fn client_request_ranges_cover_exact_total() {
        for (total, clients) in [(10, 6), (3, 8), (100, 4), (1, 1)] {
            let mut next_start = 0;
            let mut covered = 0;
            for client_idx in 0..clients {
                let (start, count) = client_request_range(total, clients, client_idx);
                assert_eq!(start, next_start);
                next_start += count;
                covered += count;
            }
            assert_eq!(covered, total);
        }
    }
}
