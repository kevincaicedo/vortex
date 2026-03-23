//! Integration test: spawn a reactor and verify PING → PONG over TCP.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use vortex_io::{Reactor, ReactorConfig, ShutdownCoordinator};

/// Find a free port by binding to :0, extracting the port, and closing.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to :0");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

#[test]
fn ping_pong_resp() {
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
        };
        let mut reactor = Reactor::new(0, config, coord_clone).expect("reactor creation");
        reactor.run();
    });

    // Give the reactor a moment to start and bind.
    std::thread::sleep(Duration::from_millis(100));

    // Connect and send a RESP-encoded PING: *1\r\n$4\r\nPING\r\n
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

    // Send a second PING to verify persistent connections work.
    stream
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("write PING 2");
    let n = stream.read(&mut buf).expect("read PONG 2");
    let response = std::str::from_utf8(&buf[..n]).expect("valid utf8");
    assert_eq!(response, "+PONG\r\n", "expected second PONG");

    // Shut down the reactor.
    drop(stream);
    coordinator.initiate();
    handle.join().expect("reactor thread join");
}

#[test]
fn ping_pong_inline() {
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
        };
        let mut reactor = Reactor::new(0, config, coord_clone).expect("reactor creation");
        reactor.run();
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    // Inline PING command.
    stream.write_all(b"PING\r\n").expect("write inline PING");

    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).expect("read PONG");
    let response = std::str::from_utf8(&buf[..n]).expect("valid utf8");
    assert_eq!(response, "+PONG\r\n", "expected PONG for inline PING");

    drop(stream);
    coordinator.initiate();
    handle.join().expect("reactor thread join");
}

#[test]
fn unknown_command_returns_error() {
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
        };
        let mut reactor = Reactor::new(0, config, coord_clone).expect("reactor creation");
        reactor.run();
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");

    // Send unknown command.
    stream.write_all(b"*1\r\n$3\r\nGET\r\n").expect("write GET");

    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).expect("read error");
    let response = std::str::from_utf8(&buf[..n]).expect("valid utf8");
    assert_eq!(
        response, "-ERR unknown command\r\n",
        "expected ERR for unknown command"
    );

    drop(stream);
    coordinator.initiate();
    handle.join().expect("reactor thread join");
}
