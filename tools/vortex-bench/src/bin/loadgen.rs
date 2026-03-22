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

fn build_command(test: &str, idx: u64) -> Vec<u8> {
    match test {
        "SET" => format!("*3\r\n$3\r\nSET\r\n$12\r\nkey:{idx:08}\r\n$3\r\nval\r\n").into_bytes(),
        "GET" => format!("*2\r\n$3\r\nGET\r\n$12\r\nkey:{idx:08}\r\n").into_bytes(),
        "INCR" => "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"
            .to_string()
            .into_bytes(),
        "PING" => b"*1\r\n$4\r\nPING\r\n".to_vec(),
        other => {
            eprintln!("Unknown test: {other}");
            std::process::exit(1);
        }
    }
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

fn run_test(args: &Args, test: &str) {
    let total = args.requests;
    let clients = args.clients;
    let pipeline = args.pipeline;
    let requests_per_client = total / clients as u64;
    let completed = Arc::new(AtomicU64::new(0));

    println!("===== {test} =====");
    println!(
        "  {} requests, {} clients, pipeline {}",
        total, clients, pipeline
    );

    let start = Instant::now();
    let mut handles = Vec::new();

    for _ in 0..clients {
        let host = args.host.clone();
        let port = args.port;
        let test = test.to_string();
        let completed = Arc::clone(&completed);

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

            while sent < requests_per_client {
                let batch = pipeline.min((requests_per_client - sent) as usize);
                let mut payload = Vec::new();
                for j in 0..batch {
                    payload.extend_from_slice(&build_command(&test, sent + j as u64));
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
        let recorder = h.join().unwrap();
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
}

fn main() {
    let args = Args::parse();
    let tests: Vec<&str> = args.tests.split(',').map(|s| s.trim()).collect();

    println!("vortex-loadgen — connecting to {}:{}", args.host, args.port);
    println!();

    for test in &tests {
        run_test(&args, test);
    }
}
