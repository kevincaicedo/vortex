#![cfg(all(target_os = "linux", feature = "io-uring"))]

//! Native io_uring integration coverage.
//!
//! These tests are intentionally gated behind the `io-uring` feature because
//! some CI sandboxes forbid `io_uring_setup(2)`. Set
//! `VORTEX_REQUIRE_IO_URING_TESTS=1` on validation hosts where native coverage
//! is required instead of skipped.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use vortex_engine::EvictionPolicy;
use vortex_io::{
    ConnectionMemoryCaps, FixedBufferRegistrationMode, IoBackendMode, ReactorPool,
    ReactorPoolConfig,
};

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

fn crlf_pos(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|window| window == b"\r\n")
}

fn resp_frame_complete(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }

    match bytes[0] {
        b'+' | b'-' | b':' => crlf_pos(bytes).is_some(),
        b'$' => {
            let Some(header_end) = crlf_pos(bytes) else {
                return false;
            };
            let Ok(len) = std::str::from_utf8(&bytes[1..header_end])
                .unwrap_or_default()
                .parse::<isize>()
            else {
                return false;
            };
            if len < 0 {
                return bytes.len() >= header_end + 2;
            }
            bytes.len() >= header_end + 2 + len as usize + 2
        }
        _ => bytes.ends_with(b"\r\n"),
    }
}

fn read_resp_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut data = Vec::with_capacity(16 * 1024);

    loop {
        let mut buf = [0u8; 4096];
        match stream.read(&mut buf) {
            Ok(0) if data.is_empty() => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed before response",
                ));
            }
            Ok(0) => return Ok(data),
            Ok(n) => data.extend_from_slice(&buf[..n]),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if resp_frame_complete(&data) {
                    return Ok(data);
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for complete RESP frame",
                    ));
                }
                continue;
            }
            Err(error) => return Err(error),
        }

        if resp_frame_complete(&data) {
            return Ok(data);
        }
    }
}

fn read_until_eof(stream: &mut TcpStream, timeout: Duration) -> io::Result<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    let mut data = Vec::with_capacity(4096);

    loop {
        let mut buf = [0u8; 4096];
        match stream.read(&mut buf) {
            Ok(0) => return Ok(data),
            Ok(n) => data.extend_from_slice(&buf[..n]),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for connection close",
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset && !data.is_empty() => {
                return Ok(data);
            }
            Err(error) => return Err(error),
        }
    }
}

fn cmd(stream: &mut TcpStream, parts: &[&str]) -> String {
    stream.write_all(&resp_cmd(parts)).expect("write command");
    let frame = read_resp_frame(stream).expect("read response");
    String::from_utf8(frame).expect("response is UTF-8")
}

fn info_field_u64(info: &str, field: &str) -> u64 {
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("INFO runtime missing {field}: {info}"))
        .parse()
        .unwrap_or_else(|error| panic!("INFO runtime field {field} is not u64: {error}; {info}"))
}

fn connect_until(addr: SocketAddr) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_millis(500)))
                    .expect("set read timeout");
                stream.set_nodelay(true).expect("set TCP_NODELAY");
                return stream;
            }
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("reactor did not accept connections on {addr}: {error}"),
        }
    }
}

fn native_uring_required() -> bool {
    std::env::var_os("VORTEX_REQUIRE_IO_URING_TESTS").is_some()
}

fn skippable_uring_startup_error(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::Unsupported | io::ErrorKind::PermissionDenied
    ) {
        return true;
    }

    let message = error.to_string();
    message.contains("Operation not permitted")
        || message.contains("io_uring backend is not available")
}

struct RunningPool {
    pool: ReactorPool,
}

impl RunningPool {
    fn shutdown_clean(&mut self) {
        self.pool.shutdown();
        assert!(
            self.pool.wait_for_shutdown(Duration::from_secs(10)),
            "io_uring reactor pool should drain cleanly"
        );
        self.pool.join();
    }
}

impl Drop for RunningPool {
    fn drop(&mut self) {
        self.pool.shutdown();
        let _ = self.pool.wait_for_shutdown(Duration::from_secs(2));
        self.pool.join();
    }
}

fn spawn_strict_uring_pool_with_caps(
    addr: SocketAddr,
    max_connections: usize,
    connection_caps: ConnectionMemoryCaps,
) -> Option<RunningPool> {
    let config = ReactorPoolConfig {
        bind_addr: addr,
        threads: 1,
        max_connections,
        buffer_size: 4096,
        max_request_bytes: 64 * 1024 * 1024,
        connection_caps,
        overload_policy: Default::default(),
        buffer_count: max_connections,
        fixed_buffer_registration: FixedBufferRegistrationMode::On,
        connection_timeout: 0,
        aof_config: None,
        shard_count: 64,
        max_memory: 0,
        eviction_policy: EvictionPolicy::NoEviction,
        io_backend: IoBackendMode::Uring,
        ring_size: 256,
        sqpoll_idle_ms: 0,
        budgets: Default::default(),
        telemetry_mode: Default::default(),
        ..Default::default()
    };

    match ReactorPool::spawn(config) {
        Ok(pool) => Some(RunningPool { pool }),
        Err(error) if skippable_uring_startup_error(&error) && !native_uring_required() => {
            eprintln!("skipping native io_uring runtime test: {error}");
            None
        }
        Err(error) => panic!("strict io_uring reactor pool startup failed: {error}"),
    }
}

fn spawn_strict_uring_pool(addr: SocketAddr, max_connections: usize) -> Option<RunningPool> {
    spawn_strict_uring_pool_with_caps(addr, max_connections, ConnectionMemoryCaps::default())
}

#[test]
fn strict_uring_ping_pong_reports_backend_contract() {
    let port = free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let Some(mut running) = spawn_strict_uring_pool(addr, 64) else {
        return;
    };

    let mut stream = connect_until(addr);

    assert_eq!(cmd(&mut stream, &["PING"]), "+PONG\r\n");
    assert_eq!(cmd(&mut stream, &["SET", "native", "ok"]), "+OK\r\n");
    assert_eq!(cmd(&mut stream, &["GET", "native"]), "$2\r\nok\r\n");

    let info = cmd(&mut stream, &["INFO", "runtime"]);
    assert!(
        info.contains("# Runtime"),
        "INFO runtime should include the Runtime section: {info}"
    );
    assert!(
        info.contains("backend_requested:io_uring\r\n"),
        "strict startup should publish requested io_uring backend: {info}"
    );
    assert!(
        info.contains("backend_effective:io_uring\r\n"),
        "strict startup should publish effective io_uring backend: {info}"
    );
    assert!(
        info.contains("backend_fixed_buffers_capable:1\r\n"),
        "native backend should advertise fixed-buffer capability: {info}"
    );
    assert!(
        info.contains("backend_fixed_buffers_registered:1\r\n"),
        "strict fixed-buffer registration should be visible through INFO runtime: {info}"
    );
    assert!(
        info.contains("backend_close_opcode:1\r\n"),
        "native backend should advertise close opcode support: {info}"
    );
    assert!(
        info.contains("backend_cancel_support:1\r\n"),
        "native backend should advertise async cancel support: {info}"
    );
    assert!(
        info.contains("backend_nonblocking_drain:1\r\n"),
        "native backend should advertise nonblocking CQ drain support: {info}"
    );

    drop(stream);
    running.shutdown_clean();
}

#[test]
fn strict_uring_churn_and_idle_shutdown_drain_complete() {
    let port = free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let Some(mut running) = spawn_strict_uring_pool(addr, 128) else {
        return;
    };

    for index in 0..96 {
        let mut stream = connect_until(addr);
        if index % 4 == 0 {
            stream
                .write_all(b"*1\r\n$4\r\nPING\r\n")
                .expect("write PING before disconnect");
            drop(stream);
            continue;
        }

        assert_eq!(cmd(&mut stream, &["PING"]), "+PONG\r\n");
        drop(stream);
    }

    let mut idle_streams = Vec::with_capacity(16);
    for _ in 0..16 {
        idle_streams.push(connect_until(addr));
    }

    running.shutdown_clean();
    drop(idle_streams);
}

#[test]
fn strict_uring_slow_reader_cap_closes_and_reuses_single_slot() {
    let port = free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let connection_caps = ConnectionMemoryCaps {
        max_pending_response_bytes: 16 * 1024,
        ..ConnectionMemoryCaps::default()
    };
    let Some(mut running) = spawn_strict_uring_pool_with_caps(addr, 1, connection_caps) else {
        return;
    };

    let mut slow_stream = connect_until(addr);
    let oversized_value = "v".repeat(connection_caps.max_pending_response_bytes + 1024);
    assert_eq!(
        cmd(&mut slow_stream, &["SET", "native:large", &oversized_value]),
        "+OK\r\n"
    );
    slow_stream
        .write_all(&resp_cmd(&["GET", "native:large"]))
        .expect("write capped slow-reader GET");

    let capped_response =
        read_until_eof(&mut slow_stream, Duration::from_secs(3)).expect("slow reader closes");
    assert!(
        !capped_response.is_empty(),
        "server should flush bounded responses before closing"
    );
    assert!(
        capped_response.len() <= connection_caps.max_pending_response_bytes,
        "response cap should bound queued bytes: {} > {}",
        capped_response.len(),
        connection_caps.max_pending_response_bytes
    );
    assert!(
        capped_response.starts_with(b"-ERR response memory limit exceeded\r\n"),
        "slow-reader response should explain the capped close"
    );

    for _ in 0..8 {
        let mut stream = connect_until(addr);
        assert_eq!(cmd(&mut stream, &["PING"]), "+PONG\r\n");
        drop(stream);
    }

    let mut info_stream = connect_until(addr);
    let info = cmd(&mut info_stream, &["INFO", "runtime"]);
    assert!(
        info.contains("backend_effective:io_uring\r\n"),
        "strict startup should keep the native backend after slow-reader close: {info}"
    );
    assert!(
        info.contains("backend_fixed_buffers_registered:1\r\n"),
        "single-slot strict startup should keep fixed-buffer registration: {info}"
    );
    assert!(
        info_field_u64(&info, "reactor_response_cap_exceeded") >= 1,
        "slow-reader cap should be visible in INFO runtime: {info}"
    );
    assert_eq!(
        info_field_u64(&info, "backend_cq_overflows"),
        0,
        "bounded slow-reader/reuse row should not require CQ overflow"
    );
    assert_eq!(
        info_field_u64(&info, "reactor_submit_failures"),
        0,
        "bounded slow-reader/reuse row should not report submit failures"
    );

    drop(info_stream);
    running.shutdown_clean();
}
