use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use vortex_smoketests::server::{SpawnOptions, spawn_vortex};

#[test]
fn raw_resp_protocol_edges() -> Result<()> {
    let server = spawn_vortex(&SpawnOptions {
        vortex_args: vec![
            "--threads".to_string(),
            "1".to_string(),
            "--telemetry-mode".to_string(),
            "minimal".to_string(),
        ],
        ready_timeout: Duration::from_secs(20),
        ..SpawnOptions::default()
    })?;
    let addr = addr_from_redis_url(server.url())?;
    let mut stream =
        TcpStream::connect(addr).with_context(|| format!("failed to connect to {addr}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    send_array(&mut stream, &[b"PING"])?;
    expect_line(&mut stream, b"+PONG")?;

    send_array(&mut stream, &[b"GET", b"raw:missing"])?;
    expect_line(&mut stream, b"$-1")?;

    let binary_key = b"raw:\0:key";
    let binary_value = b"a\0b\r\nc";
    send_array(&mut stream, &[b"SET", binary_key, binary_value])?;
    expect_line(&mut stream, b"+OK")?;

    send_array(&mut stream, &[b"GET", binary_key])?;
    expect_bulk(&mut stream, binary_value)?;

    send_array(&mut stream, &[b"GET", b"raw:too", b"many"])?;
    expect_error_contains(&mut stream, b"wrong number of arguments")?;

    send_array(&mut stream, &[b"MULTI"])?;
    expect_line(&mut stream, b"+OK")?;
    send_array(&mut stream, &[b"SET", b"raw:tx", b"value"])?;
    expect_line(&mut stream, b"+QUEUED")?;
    send_array(&mut stream, &[b"INCR", b"raw:tx", b"extra"])?;
    expect_error_contains(&mut stream, b"wrong number of arguments")?;
    send_array(&mut stream, &[b"EXEC"])?;
    expect_error_contains(&mut stream, b"EXECABORT")?;
    send_array(&mut stream, &[b"GET", b"raw:tx"])?;
    expect_line(&mut stream, b"$-1")?;

    let mut pipelined = Vec::new();
    append_array(&mut pipelined, &[b"SET", b"raw:pipeline", b"ok"]);
    append_array(&mut pipelined, &[b"GET", b"raw:pipeline"]);
    stream.write_all(&pipelined)?;
    expect_line(&mut stream, b"+OK")?;
    expect_bulk(&mut stream, b"ok")?;

    Ok(())
}

fn addr_from_redis_url(url: &str) -> Result<&str> {
    url.strip_prefix("redis://")
        .and_then(|rest| rest.split('/').next())
        .filter(|addr| !addr.is_empty())
        .with_context(|| format!("unsupported Redis URL: {url}"))
}

fn send_array(stream: &mut TcpStream, parts: &[&[u8]]) -> Result<()> {
    let mut buf = Vec::new();
    append_array(&mut buf, parts);
    stream.write_all(&buf)?;
    Ok(())
}

fn append_array(buf: &mut Vec<u8>, parts: &[&[u8]]) {
    buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        buf.extend_from_slice(part);
        buf.extend_from_slice(b"\r\n");
    }
}

fn expect_line(stream: &mut TcpStream, expected: &[u8]) -> Result<()> {
    let line = read_line(stream)?;
    ensure!(
        line == expected,
        "unexpected response line: expected {:?}, got {:?}",
        String::from_utf8_lossy(expected),
        String::from_utf8_lossy(&line)
    );
    Ok(())
}

fn expect_error_contains(stream: &mut TcpStream, needle: &[u8]) -> Result<()> {
    let line = read_line(stream)?;
    ensure!(
        line.starts_with(b"-"),
        "expected RESP error, got {:?}",
        String::from_utf8_lossy(&line)
    );
    ensure!(
        line.windows(needle.len()).any(|window| window == needle),
        "expected error containing {:?}, got {:?}",
        String::from_utf8_lossy(needle),
        String::from_utf8_lossy(&line)
    );
    Ok(())
}

fn expect_bulk(stream: &mut TcpStream, expected: &[u8]) -> Result<()> {
    let header = read_line(stream)?;
    ensure!(
        header.starts_with(b"$"),
        "expected bulk response, got {:?}",
        String::from_utf8_lossy(&header)
    );
    let len: usize = std::str::from_utf8(&header[1..])
        .context("bulk length was not utf8")?
        .parse()
        .context("bulk length was not numeric")?;
    ensure!(
        len == expected.len(),
        "unexpected bulk length: expected {}, got {}",
        expected.len(),
        len
    );

    let mut payload = vec![0u8; len + 2];
    stream.read_exact(&mut payload)?;
    ensure!(
        &payload[..len] == expected && &payload[len..] == b"\r\n",
        "unexpected bulk payload: expected {:?}, got {:?}",
        String::from_utf8_lossy(expected),
        String::from_utf8_lossy(&payload)
    );
    Ok(())
}

fn read_line(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte)?;
        if n == 0 {
            bail!("server closed raw RESP connection while reading line");
        }
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            line.truncate(line.len() - 2);
            return Ok(line);
        }
        ensure!(line.len() <= 8192, "raw RESP line exceeded smoke limit");
    }
}
