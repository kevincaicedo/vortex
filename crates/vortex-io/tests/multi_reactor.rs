//! Integration test: spawn a multi-reactor pool and verify PING → PONG from
//! multiple concurrent clients.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use vortex_engine::EvictionPolicy;
use vortex_io::{IoBackendMode, ReactorPool, ReactorPoolConfig};

/// Find a free port by binding to :0, extracting the port, and closing.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to :0");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

#[test]
fn multi_reactor_ping_pong() {
    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let config = ReactorPoolConfig {
        bind_addr: addr,
        threads: 2,
        max_connections: 128,
        buffer_size: 4096,
        buffer_count: 256,
        connection_timeout: 0,
        aof_config: None,
        shard_count: 64,
        io_backend: IoBackendMode::Polling,
        ring_size: 4096,
        sqpoll_idle_ms: 1000,
        max_memory: 0,
        eviction_policy: EvictionPolicy::NoEviction,
    };

    let mut pool = ReactorPool::spawn(config).expect("pool creation");

    // Give reactors time to start and bind.
    std::thread::sleep(Duration::from_millis(200));

    // Connect multiple clients and verify each gets PONG.
    let num_clients = 10;
    let mut streams = Vec::with_capacity(num_clients);

    for _ in 0..num_clients {
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set read timeout");

        stream
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .expect("write PING");

        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).expect("read PONG");
        let response = std::str::from_utf8(&buf[..n]).expect("valid utf8");
        assert_eq!(response, "+PONG\r\n", "expected PONG response");

        streams.push(stream);
    }

    // Send a second round of PINGs to verify persistent connections.
    for stream in &mut streams {
        stream
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .expect("write PING 2");

        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).expect("read PONG 2");
        let response = std::str::from_utf8(&buf[..n]).expect("valid utf8");
        assert_eq!(response, "+PONG\r\n", "expected second PONG");
    }

    // Clean up.
    drop(streams);
    pool.shutdown();
    pool.join();
}
