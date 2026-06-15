//! Benchmark: transaction round-trip through the production reactor.
//!
//! Measures the full single-connection MULTI/SET/GET/EXEC path over TCP. This
//! intentionally includes parser, reactor dispatch, transaction queueing,
//! keyspace mutation, response serialization, and socket I/O.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use vortex_io::{IoBackendMode, ReactorPool, ReactorPoolConfig};

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to :0");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

fn connect_stream(addr: SocketAddr) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("timeout");
                stream.set_nodelay(true).expect("nodelay");
                return stream;
            }
            Err(err)
                if Instant::now() < deadline
                    && matches!(
                        err.kind(),
                        std::io::ErrorKind::AddrNotAvailable
                            | std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::TimedOut
                    ) =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => panic!("connect {addr} failed: {err}"),
        }
    }
}

fn read_exact_response(stream: &mut TcpStream, expected_len: usize, buf: &mut [u8]) {
    let mut total = 0;
    while total < expected_len {
        let n = stream.read(&mut buf[total..expected_len]).expect("read");
        assert!(
            n != 0,
            "connection closed while reading transaction response"
        );
        total += n;
    }
}

fn transaction_roundtrip(c: &mut Criterion) {
    let port = free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let config = ReactorPoolConfig {
        bind_addr: addr,
        threads: 1,
        max_connections: 128,
        buffer_size: 16_384,
        buffer_count: 128,
        connection_timeout: 0,
        aof_config: None,
        shard_count: 64,
        io_backend: IoBackendMode::Auto,
        ring_size: 1024,
        sqpoll_idle_ms: 4096,
        max_memory: 0,
        eviction_policy: vortex_engine::EvictionPolicy::NoEviction,
    };
    let mut pool = ReactorPool::spawn(config).expect("pool creation");
    let mut stream = connect_stream(addr);

    let command = b"*1\r\n$5\r\nMULTI\r\n\
*3\r\n$3\r\nSET\r\n$6\r\nbenchk\r\n$5\r\nvalue\r\n\
*2\r\n$3\r\nGET\r\n$6\r\nbenchk\r\n\
*1\r\n$4\r\nEXEC\r\n";
    let expected = b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n$5\r\nvalue\r\n";
    let mut buf = vec![0u8; expected.len()];

    let mut group = c.benchmark_group("transaction");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.bench_function("multi_set_get_exec_tcp", |b| {
        b.iter(|| {
            stream.write_all(command).expect("write transaction");
            read_exact_response(&mut stream, expected.len(), &mut buf);
            assert_eq!(buf.as_slice(), expected);
            black_box(&buf);
        });
    });
    group.finish();

    drop(stream);
    pool.shutdown();
    pool.join();
}

criterion_group!(benches, transaction_roundtrip);
criterion_main!(benches);
