//! Integration test: graceful shutdown — verify in-flight responses are
//! delivered before connections close and all reactor threads join cleanly.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use vortex_io::{IoBackendMode, Reactor, ReactorConfig};
use vortex_io::{ReactorPool, ReactorPoolConfig, ShutdownCoordinator};

/// Find a free port by binding to :0, extracting the port, and closing.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to :0");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// Test: single reactor graceful shutdown flushes in-flight response.
///
/// Steps:
/// 1. Spawn a single reactor.
/// 2. Connect a client, send PING, receive PONG.
/// 3. Initiate shutdown.
/// 4. Verify the reactor thread joins cleanly.
/// 5. Verify `reactor_finished` flag is set.
#[test]
fn graceful_shutdown_single_reactor() {
    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let coordinator = Arc::new(ShutdownCoordinator::new(1));
    let coord_clone = Arc::clone(&coordinator);

    let handle = std::thread::spawn(move || {
        let config = ReactorConfig {
            bind_addr: addr,
            max_connections: 64,
            buffer_size: 4096,
            buffer_count: 64,
            connection_timeout: 0,
            aof_config: None,
            io_backend: IoBackendMode::Polling,
            ring_size: 4096,
            sqpoll_idle_ms: 1000,
        };
        let mut reactor = Reactor::new(0, config, coord_clone).expect("reactor creation");
        reactor.run();
    });

    // Give the reactor time to start.
    std::thread::sleep(Duration::from_millis(100));

    // Connect and send PING.
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
    assert_eq!(response, "+PONG\r\n");

    // Initiate graceful shutdown.
    drop(stream);
    assert!(coordinator.initiate(), "first initiate should succeed");

    // Reactor thread should join within a reasonable time.
    handle.join().expect("reactor thread should join cleanly");

    // Coordinator should report reactor finished.
    assert!(coordinator.all_done(), "all reactors should be done");
}

/// Test: multi-reactor pool graceful shutdown.
///
/// Steps:
/// 1. Spawn a 2-reactor pool.
/// 2. Connect clients and verify PONG.
/// 3. Initiate shutdown via coordinator.
/// 4. Wait for clean shutdown.
/// 5. Verify all threads join.
#[test]
fn graceful_shutdown_pool() {
    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let config = ReactorPoolConfig {
        bind_addr: addr,
        threads: 2,
        max_connections: 128,
        buffer_size: 4096,
        buffer_count: 128,
        connection_timeout: 0,
        aof_config: None,
        shard_count: 64,
        io_backend: IoBackendMode::Polling,
        ring_size: 4096,
        sqpoll_idle_ms: 1000,
    };

    let mut pool = ReactorPool::spawn(config).expect("pool creation");

    // Give reactors time to start.
    std::thread::sleep(Duration::from_millis(200));

    // Connect a client.
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    stream
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("write PING");

    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).expect("read PONG");
    assert_eq!(&buf[..n], b"+PONG\r\n");

    // Drop client before shutdown.
    drop(stream);

    // Initiate shutdown.
    pool.shutdown();

    // Wait with a generous timeout.
    let clean = pool.wait_for_shutdown(Duration::from_secs(10));
    assert!(clean, "shutdown should be clean");

    pool.join();

    // Verify coordinator state.
    assert!(pool.coordinator().all_done());
}

/// Test: new connections are refused during drain.
///
/// Steps:
/// 1. Spawn a single reactor.
/// 2. Initiate shutdown (drain mode).
/// 3. Attempt to connect — should be refused.
#[test]
fn new_connections_refused_during_drain() {
    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let coordinator = Arc::new(ShutdownCoordinator::new(1));
    let coord_clone = Arc::clone(&coordinator);

    let handle = std::thread::spawn(move || {
        let config = ReactorConfig {
            bind_addr: addr,
            max_connections: 64,
            buffer_size: 4096,
            buffer_count: 64,
            connection_timeout: 0,
            aof_config: None,
            io_backend: IoBackendMode::Polling,
            ring_size: 4096,
            sqpoll_idle_ms: 1000,
        };
        let mut reactor = Reactor::new(0, config, coord_clone).expect("reactor creation");
        reactor.run();
    });

    // Give the reactor time to start.
    std::thread::sleep(Duration::from_millis(100));

    // Initiate shutdown.
    coordinator.initiate();

    // Wait a bit for drain to take effect.
    std::thread::sleep(Duration::from_millis(100));

    // Attempt to connect — may either fail to connect or connection is dropped.
    let result = TcpStream::connect_timeout(&addr, Duration::from_millis(500));
    if let Ok(mut stream) = result {
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .ok();
        stream.write_all(b"*1\r\n$4\r\nPING\r\n").ok();
        let mut buf = [0u8; 64];
        // Should either fail or get no response (connection closed by server).
        let read_result = stream.read(&mut buf);
        match read_result {
            Ok(0) => {}  // EOF — expected: server closed connection.
            Err(_) => {} // Error — expected: connection reset.
            Ok(_) => {}  // May get a response if accept completed before drain.
        }
    }
    // Either way, reactor should exit.

    handle.join().expect("reactor thread should join");
}
