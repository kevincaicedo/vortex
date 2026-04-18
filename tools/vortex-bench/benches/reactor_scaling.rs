//! Benchmark: reactor scaling — measure PING throughput vs reactor count.
//!
//! Varies reactor count from 1 to N (physical cores) and measures total
//! throughput (PING/PONG ops/sec) across 100 concurrent clients pipelining
//! 10 PINGs each.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use vortex_io::{ReactorPool, ReactorPoolConfig};

/// Find a free port by binding to :0, extracting the port, and closing.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to :0");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

fn reactor_scaling(c: &mut Criterion) {
    let max_reactors = core_affinity::get_core_ids()
        .map(|ids| ids.len())
        .unwrap_or(1)
        .min(4); // Cap at 4 for benchmark sanity.

    let mut group = c.benchmark_group("reactor_scaling");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    for num_reactors in 1..=max_reactors {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_reactors),
            &num_reactors,
            |b, &n| {
                let port = free_port();
                let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

                let config = ReactorPoolConfig {
                    bind_addr: addr,
                    threads: n,
                    max_connections: 1024,
                    buffer_size: 16_384,
                    buffer_count: 1024,
                    connection_timeout: 0,
                    aof_config: None,
                    shard_count: 64,
                };

                let mut pool = ReactorPool::spawn(config).expect("pool creation");
                std::thread::sleep(Duration::from_millis(200));

                b.iter(|| {
                    let num_clients = 20;
                    let pings_per_client = 10;
                    let ping_cmd = b"*1\r\n$4\r\nPING\r\n";

                    let mut streams: Vec<TcpStream> = (0..num_clients)
                        .map(|_| {
                            let s = TcpStream::connect(addr).expect("connect");
                            s.set_read_timeout(Some(Duration::from_secs(5)))
                                .expect("timeout");
                            s.set_nodelay(true).expect("nodelay");
                            s
                        })
                        .collect();

                    // Pipeline PINGs.
                    for stream in &mut streams {
                        for _ in 0..pings_per_client {
                            stream.write_all(ping_cmd).expect("write");
                        }
                    }

                    // Read all responses.
                    let expected_len = "+PONG\r\n".len() * pings_per_client;
                    let mut buf = vec![0u8; expected_len + 64];
                    for stream in &mut streams {
                        let mut total = 0;
                        while total < expected_len {
                            let n = stream.read(&mut buf[total..]).expect("read");
                            if n == 0 {
                                break;
                            }
                            total += n;
                        }
                    }

                    drop(streams);
                });

                pool.shutdown();
                pool.join();
            },
        );
    }
    group.finish();
}

criterion_group!(benches, reactor_scaling);
criterion_main!(benches);
