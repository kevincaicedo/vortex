//! End-to-end integration tests for Task 3.7 — Reactor ↔ Engine Integration.
//!
//! These tests spin up a real reactor on a random port, connect via raw TCP,
//! send RESP-encoded commands, and verify the wire responses. This validates
//! the full request lifecycle: read → parse → dispatch → engine execute →
//! serialize → write.

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

/// RESP-encode a command from parts: `resp_cmd(&["SET", "foo", "bar"])`.
fn resp_cmd(parts: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        buf.extend_from_slice(part.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

/// Spawn a reactor and return (handle, port, coordinator).
fn spawn_reactor() -> (std::thread::JoinHandle<()>, u16, Arc<ShutdownCoordinator>) {
    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let coordinator = Arc::new(ShutdownCoordinator::new(1));
    let coord_clone = Arc::clone(&coordinator);

    let handle = std::thread::spawn(move || {
        let config = ReactorConfig {
            bind_addr: addr,
            max_connections: 64,
            buffer_size: 4096,
            buffer_count: 128,
            connection_timeout: 0,
        };
        let mut reactor = Reactor::new(0, config, coord_clone).expect("reactor creation");
        reactor.run();
    });

    // Give the reactor time to bind. Retry connection to avoid race.
    std::thread::sleep(Duration::from_millis(100));

    (handle, port, coordinator)
}

/// Connect to the reactor; returns a configured TcpStream.
fn connect(port: u16) -> TcpStream {
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");
    stream.set_nodelay(true).expect("set nodelay");
    stream
}

/// Send a RESP-encoded command and read the full response.
fn send_recv(stream: &mut TcpStream, cmd: &[u8]) -> String {
    stream.write_all(cmd).expect("write");
    let mut buf = [0u8; 8192];
    let n = stream.read(&mut buf).expect("read");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// Send a RESP command built from parts and read the response.
fn cmd(stream: &mut TcpStream, parts: &[&str]) -> String {
    send_recv(stream, &resp_cmd(parts))
}

/// Shutdown the reactor cleanly.
fn shutdown(handle: std::thread::JoinHandle<()>, coordinator: &Arc<ShutdownCoordinator>) {
    coordinator.initiate();
    handle.join().expect("reactor thread join");
}

// ── SET / GET round-trip ───────────────────────────────────────────

#[test]
fn set_get_roundtrip() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    let resp = cmd(&mut s, &["SET", "hello", "world"]);
    assert_eq!(resp, "+OK\r\n");

    let resp = cmd(&mut s, &["GET", "hello"]);
    assert_eq!(resp, "$5\r\nworld\r\n");

    // GET missing key → nil
    let resp = cmd(&mut s, &["GET", "missing"]);
    assert_eq!(resp, "$-1\r\n");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── INCR counter ────────────────────────────────────────────────

#[test]
fn incr_counter() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    let resp = cmd(&mut s, &["INCR", "counter"]);
    assert_eq!(resp, ":1\r\n");

    let resp = cmd(&mut s, &["INCR", "counter"]);
    assert_eq!(resp, ":2\r\n");

    let resp = cmd(&mut s, &["INCR", "counter"]);
    assert_eq!(resp, ":3\r\n");

    let resp = cmd(&mut s, &["GET", "counter"]);
    assert_eq!(resp, "$1\r\n3\r\n");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── EXPIRE / TTL ────────────────────────────────────────────────

#[test]
fn expire_ttl_roundtrip() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    cmd(&mut s, &["SET", "ex_key", "data"]);

    // TTL on non-expiring key → -1
    let resp = cmd(&mut s, &["TTL", "ex_key"]);
    assert_eq!(resp, ":-1\r\n");

    // EXPIRE 60 seconds
    let resp = cmd(&mut s, &["EXPIRE", "ex_key", "60"]);
    assert_eq!(resp, ":1\r\n");

    // TTL should be positive (≤60)
    let resp = cmd(&mut s, &["TTL", "ex_key"]);
    assert!(resp.starts_with(':'));
    let ttl: i64 = resp
        .trim_start_matches(':')
        .trim_end_matches("\r\n")
        .parse()
        .unwrap();
    assert!(ttl > 0 && ttl <= 60, "TTL should be 0 < {ttl} <= 60");

    // PERSIST removes TTL
    let resp = cmd(&mut s, &["PERSIST", "ex_key"]);
    assert_eq!(resp, ":1\r\n");

    let resp = cmd(&mut s, &["TTL", "ex_key"]);
    assert_eq!(resp, ":-1\r\n");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── DEL / EXISTS ────────────────────────────────────────────────

#[test]
fn del_exists() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    cmd(&mut s, &["SET", "a", "1"]);
    cmd(&mut s, &["SET", "b", "2"]);

    let resp = cmd(&mut s, &["EXISTS", "a"]);
    assert_eq!(resp, ":1\r\n");

    let resp = cmd(&mut s, &["DEL", "a", "b", "c"]);
    assert_eq!(resp, ":2\r\n"); // 2 deleted (c didn't exist)

    let resp = cmd(&mut s, &["EXISTS", "a"]);
    assert_eq!(resp, ":0\r\n");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── MGET pipeline ───────────────────────────────────────────────

#[test]
fn mget_pipeline() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    cmd(&mut s, &["SET", "k1", "v1"]);
    cmd(&mut s, &["SET", "k2", "v2"]);

    let resp = cmd(&mut s, &["MGET", "k1", "k2", "k3"]);
    // *3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$-1\r\n
    assert!(resp.starts_with("*3\r\n"));
    assert!(resp.contains("$2\r\nv1\r\n"));
    assert!(resp.contains("$2\r\nv2\r\n"));
    assert!(resp.contains("$-1\r\n")); // k3 is nil

    drop(s);
    shutdown(handle, &coordinator);
}

// ── SCAN full iteration ─────────────────────────────────────────

#[test]
fn scan_full_iteration() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    // Insert 20 keys.
    for i in 0..20 {
        cmd(
            &mut s,
            &["SET", &format!("scan_key_{i}"), &format!("val_{i}")],
        );
    }

    // Full SCAN — collect all keys.
    let mut all_keys = Vec::new();
    let mut cursor = "0".to_string();
    for _ in 0..100 {
        // Prevent infinite loop.
        let resp = cmd(&mut s, &["SCAN", &cursor, "COUNT", "5"]);
        // Response: *2\r\n$<cursor_len>\r\n<cursor>\r\n*<count>\r\n...
        let parts: Vec<&str> = resp.split("\r\n").collect();
        // parts[0] = "*2", parts[1] = "$<len>", parts[2] = new_cursor, ...
        assert!(parts[0] == "*2", "SCAN returns 2-element array");
        let new_cursor = parts[2].to_string();
        // Count the keys in the sub-array.
        let key_array_header = parts[3]; // "*N"
        if let Some(count_text) = key_array_header.strip_prefix('*') {
            let count: usize = count_text.parse().unwrap_or(0);
            for i in 0..count {
                // Each key is: $<len>\r\n<key>
                let key_idx = 4 + i * 2 + 1; // skip $<len>, get value
                if key_idx < parts.len() {
                    all_keys.push(parts[key_idx].to_string());
                }
            }
        }
        cursor = new_cursor;
        if cursor == "0" {
            break;
        }
    }

    assert_eq!(cursor, "0", "SCAN must complete (cursor back to 0)");
    // Deduplicate — SCAN may return keys multiple times.
    all_keys.sort();
    all_keys.dedup();
    assert_eq!(all_keys.len(), 20, "SCAN must return all 20 keys");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── DBSIZE / FLUSHDB ────────────────────────────────────────────

#[test]
fn dbsize_and_flushdb() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    let resp = cmd(&mut s, &["DBSIZE"]);
    assert_eq!(resp, ":0\r\n");

    cmd(&mut s, &["SET", "x", "1"]);
    cmd(&mut s, &["SET", "y", "2"]);
    cmd(&mut s, &["SET", "z", "3"]);

    let resp = cmd(&mut s, &["DBSIZE"]);
    assert_eq!(resp, ":3\r\n");

    let resp = cmd(&mut s, &["FLUSHDB"]);
    assert_eq!(resp, "+OK\r\n");

    let resp = cmd(&mut s, &["DBSIZE"]);
    assert_eq!(resp, ":0\r\n");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── PING through engine ────────────────────────────────────────

#[test]
fn ping_through_engine() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    let resp = cmd(&mut s, &["PING"]);
    assert_eq!(resp, "+PONG\r\n");

    let resp = cmd(&mut s, &["PING", "hello"]);
    assert_eq!(resp, "$5\r\nhello\r\n");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── INFO through engine ────────────────────────────────────────

#[test]
fn info_through_engine() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    let resp = cmd(&mut s, &["INFO"]);
    // INFO returns a bulk string starting with $<len>\r\n...
    assert!(resp.starts_with('$'), "INFO returns bulk string");
    assert!(resp.contains("# Server"), "INFO contains Server section");
    assert!(
        resp.contains("# Keyspace"),
        "INFO contains Keyspace section"
    );

    drop(s);
    shutdown(handle, &coordinator);
}

// ── QUIT closes connection after response ──────────────────────

#[test]
fn quit_closes_connection() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    // SET something to prove the connection was live.
    let resp = cmd(&mut s, &["SET", "qk", "qv"]);
    assert_eq!(resp, "+OK\r\n");

    // QUIT → +OK, then the server closes the connection.
    s.write_all(&resp_cmd(&["QUIT"])).expect("write QUIT");
    let mut buf = [0u8; 256];
    let n = s.read(&mut buf).expect("read QUIT response");
    let response = std::str::from_utf8(&buf[..n]).unwrap();
    assert_eq!(response, "+OK\r\n");

    // Next read should return 0 (EOF) because the server closed.
    // Use a short timeout to detect closure.
    s.set_read_timeout(Some(Duration::from_millis(500)))
        .expect("set read timeout");
    let n = s.read(&mut buf).unwrap_or(0);
    assert_eq!(n, 0, "connection should be closed after QUIT");

    shutdown(handle, &coordinator);
}

// ── Multiple concurrent clients ────────────────────────────────

#[test]
fn multiple_concurrent_clients() {
    let (handle, port, coordinator) = spawn_reactor();

    let mut handles = Vec::new();
    for client_id in 0..4 {
        let p = port;
        handles.push(std::thread::spawn(move || {
            let mut s = connect(p);
            for i in 0..10 {
                let key = format!("c{client_id}_k{i}");
                let val = format!("c{client_id}_v{i}");
                let resp = cmd(&mut s, &["SET", &key, &val]);
                assert_eq!(resp, "+OK\r\n", "client {client_id} SET {key}");
                let resp = cmd(&mut s, &["GET", &key]);
                let expected = format!("${}\r\n{val}\r\n", val.len());
                assert_eq!(resp, expected, "client {client_id} GET {key}");
            }
            drop(s);
        }));
    }

    for h in handles {
        h.join().expect("client thread join");
    }

    // All 4 clients × 10 keys = 40 keys.
    let mut s = connect(port);
    let resp = cmd(&mut s, &["DBSIZE"]);
    assert_eq!(resp, ":40\r\n");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── Pipeline batch (multiple commands in one write) ─────────────

#[test]
fn pipeline_batch() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    // Send SET + GET + DBSIZE in a single write (pipeline).
    let mut wire = resp_cmd(&["SET", "pipe_k", "pipe_v"]);
    wire.extend_from_slice(&resp_cmd(&["GET", "pipe_k"]));
    wire.extend_from_slice(&resp_cmd(&["DBSIZE"]));

    s.write_all(&wire).expect("write pipeline");

    // Read all responses at once.
    let mut buf = [0u8; 4096];
    let n = s.read(&mut buf).expect("read pipeline");
    let resp = std::str::from_utf8(&buf[..n]).unwrap();

    // Expected: +OK\r\n$6\r\npipe_v\r\n:1\r\n
    assert!(resp.starts_with("+OK\r\n"), "SET response");
    assert!(resp.contains("$6\r\npipe_v\r\n"), "GET response");
    assert!(resp.ends_with(":1\r\n"), "DBSIZE response");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── TYPE command ────────────────────────────────────────────────

#[test]
fn type_command() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    cmd(&mut s, &["SET", "str_key", "value"]);

    let resp = cmd(&mut s, &["TYPE", "str_key"]);
    assert_eq!(resp, "+string\r\n");

    let resp = cmd(&mut s, &["TYPE", "nonexistent"]);
    assert_eq!(resp, "+none\r\n");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── SELECT command ──────────────────────────────────────────────

#[test]
fn select_command() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    let resp = cmd(&mut s, &["SELECT", "0"]);
    assert_eq!(resp, "+OK\r\n");

    let resp = cmd(&mut s, &["SELECT", "1"]);
    assert!(resp.starts_with("-ERR"), "SELECT 1 should error");

    drop(s);
    shutdown(handle, &coordinator);
}

// ── TIME command ────────────────────────────────────────────────

#[test]
fn time_command() {
    let (handle, port, coordinator) = spawn_reactor();
    let mut s = connect(port);

    let resp = cmd(&mut s, &["TIME"]);
    // Returns *2\r\n array of two bulk strings: seconds and microseconds
    assert!(resp.starts_with("*2\r\n"), "TIME returns 2-element array");

    drop(s);
    shutdown(handle, &coordinator);
}
