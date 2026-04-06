//! Integration test: spawn a multi-reactor pool and verify PING → PONG from
//! multiple concurrent clients.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use vortex_io::{ReactorPool, ReactorPoolConfig};

/// Find a free port by binding to :0, extracting the port, and closing.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to :0");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

fn resp_cmd(parts: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        buf.extend_from_slice(part.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

fn cmd(stream: &mut TcpStream, parts: &[&str]) -> String {
    stream.write_all(&resp_cmd(parts)).expect("write command");
    let mut buf = [0u8; 8192];
    let n = stream.read(&mut buf).expect("read response");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

fn connect(addr: std::net::SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");
    stream.set_nodelay(true).expect("set nodelay");
    stream
}

fn key_owner(key: &str, num_reactors: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.as_bytes().hash(&mut hasher);
    hasher.finish() as usize % num_reactors
}

fn cross_shard_keys(num_reactors: usize) -> (String, String) {
    let mut first = None;
    let mut second = None;

    for index in 0..10_000 {
        let key = format!("mr:key:{index}");
        match key_owner(&key, num_reactors) {
            0 if first.is_none() => first = Some(key),
            1 if second.is_none() => second = Some(key),
            _ => {}
        }
        if first.is_some() && second.is_some() {
            break;
        }
    }

    (
        first.expect("key for reactor 0"),
        second.expect("key for reactor 1"),
    )
}

fn scan_collect(stream: &mut TcpStream, pattern: &str) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut keys = Vec::new();

    for _ in 0..32 {
        let response = cmd(stream, &["SCAN", &cursor, "MATCH", pattern, "COUNT", "1"]);
        let parts: Vec<&str> = response.split("\r\n").collect();
        assert_eq!(parts[0], "*2", "SCAN returns two top-level elements");
        cursor = parts[2].to_string();

        let key_array_header = parts[3];
        let count = key_array_header
            .strip_prefix('*')
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        for index in 0..count {
            let key_idx = 4 + index * 2 + 1;
            if key_idx < parts.len() {
                keys.push(parts[key_idx].to_string());
            }
        }

        if cursor == "0" {
            break;
        }
    }

    keys.sort();
    keys.dedup();
    keys
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
        buffer_count: 128,
        connection_timeout: 0,
        aof_config: None,
        max_memory: 0,
        ..ReactorPoolConfig::default()
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

#[test]
fn multi_reactor_cross_shard_mset_mget_del() {
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
        max_memory: 0,
        ..ReactorPoolConfig::default()
    };

    let mut pool = ReactorPool::spawn(config).expect("pool creation");
    std::thread::sleep(Duration::from_millis(200));

    let (key_a, key_b) = cross_shard_keys(2);
    let mut stream = connect(addr);

    let response = cmd(&mut stream, &["MSET", &key_a, "va", &key_b, "vb"]);
    assert_eq!(response, "+OK\r\n");

    let response = cmd(&mut stream, &["GET", &key_a]);
    assert_eq!(response, "$2\r\nva\r\n");

    let response = cmd(&mut stream, &["GET", &key_b]);
    assert_eq!(response, "$2\r\nvb\r\n");

    let response = cmd(&mut stream, &["MGET", &key_a, &key_b]);
    assert!(response.starts_with("*2\r\n"));
    assert!(response.contains("$2\r\nva\r\n"));
    assert!(response.contains("$2\r\nvb\r\n"));

    let response = cmd(&mut stream, &["DEL", &key_a, &key_b]);
    assert_eq!(response, ":2\r\n");

    let response = cmd(&mut stream, &["EXISTS", &key_a, &key_b]);
    assert_eq!(response, ":0\r\n");

    drop(stream);
    pool.shutdown();
    pool.join();
}

#[test]
fn multi_reactor_keys_and_scan_cover_global_keyspace() {
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
        max_memory: 0,
        ..ReactorPoolConfig::default()
    };

    let mut pool = ReactorPool::spawn(config).expect("pool creation");
    std::thread::sleep(Duration::from_millis(200));

    let (key_a, key_b) = cross_shard_keys(2);
    let mut stream = connect(addr);
    let prefixed_a = format!("scan:{}", key_a);
    let prefixed_b = format!("scan:{}", key_b);

    assert_eq!(cmd(&mut stream, &["SET", &prefixed_a, "1"]), "+OK\r\n");
    assert_eq!(cmd(&mut stream, &["SET", &prefixed_b, "2"]), "+OK\r\n");

    let response = cmd(&mut stream, &["KEYS", "scan:*"]);
    assert!(response.contains(&prefixed_a));
    assert!(response.contains(&prefixed_b));

    let scanned = scan_collect(&mut stream, "scan:*");
    assert!(scanned.contains(&prefixed_a));
    assert!(scanned.contains(&prefixed_b));

    drop(stream);
    pool.shutdown();
    pool.join();
}
