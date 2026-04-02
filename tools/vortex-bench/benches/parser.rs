use bytes::{Bytes, BytesMut};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use vortex_proto::frame::RespFrame;
use vortex_proto::{IovecWriter, RespParser, RespSerializer, RespTape, scan_crlf, swar_parse_int};

fn next_sample<'a, T>(samples: &'a [T], index: &mut usize) -> &'a T {
    let sample = &samples[*index];
    *index += 1;
    if *index == samples.len() {
        *index = 0;
    }
    sample
}

fn make_set_pipeline(count: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(count * 48);
    for i in 0..count {
        let key = format!("key:{i}");
        let value = format!("val:{i}");
        buf.extend_from_slice(format!("*3\r\n$3\r\nSET\r\n${}\r\n", key.len()).as_bytes());
        buf.extend_from_slice(key.as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
        buf.extend_from_slice(value.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

fn make_nested_array(depth: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(depth * 4 + 4);
    for _ in 0..depth {
        buf.extend_from_slice(b"*1\r\n");
    }
    buf.extend_from_slice(b":1\r\n");
    buf
}

fn make_mixed_pipeline() -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"+OK\r\n");
    buf.extend_from_slice(b":42\r\n");
    buf.extend_from_slice(b"$5\r\nhello\r\n");
    buf.extend_from_slice(b"#t\r\n");
    buf.extend_from_slice(b"%1\r\n+role\r\n+primary\r\n");
    buf
}

fn make_sparse_crlf_buffer(len: usize, stride: usize) -> Vec<u8> {
    let mut buf = vec![b'x'; len];
    let mut offset = stride.saturating_sub(2);

    while offset + 1 < len {
        buf[offset] = b'\r';
        buf[offset + 1] = b'\n';
        offset += stride;
    }

    buf
}

fn make_all_crlf_buffer(len: usize) -> Vec<u8> {
    let mut buf = vec![b'\r'; len];
    for index in (1..len).step_by(2) {
        buf[index] = b'\n';
    }
    buf
}

fn bench_crlf_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("crlf_scan");

    let cases = [
        ("crlf_scan_1kb", make_sparse_crlf_buffer(1024, 48)),
        ("crlf_scan_16kb", make_sparse_crlf_buffer(16 * 1024, 54)),
        ("crlf_scan_64kb", make_sparse_crlf_buffer(64 * 1024, 96)),
        ("crlf_scan_no_crlf", vec![0_u8; 16 * 1024]),
        ("crlf_scan_all_crlf", make_all_crlf_buffer(16 * 1024)),
    ];

    for (name, input) in &cases {
        group.throughput(Throughput::Bytes(input.len() as u64));
        group.bench_function(*name, |b| {
            b.iter(|| black_box(scan_crlf(black_box(input.as_slice())).len()));
        });
    }

    group.finish();
}

fn bench_swar_integer_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("swar_integer_parse");

    let cases = [
        (
            "1digit",
            &[
                b"0".as_slice(),
                b"1",
                b"2",
                b"3",
                b"4",
                b"5",
                b"6",
                b"7",
                b"8",
                b"9",
            ][..],
        ),
        (
            "3digit",
            &[
                b"101".as_slice(),
                b"123",
                b"245",
                b"367",
                b"489",
                b"501",
                b"623",
                b"745",
                b"867",
                b"989",
            ][..],
        ),
        (
            "5digit",
            &[
                b"10001".as_slice(),
                b"12345",
                b"23456",
                b"34567",
                b"45678",
                b"56789",
                b"67890",
                b"78901",
                b"89012",
                b"90123",
            ][..],
        ),
        (
            "10digit",
            &[
                b"1000000001".as_slice(),
                b"1234567890",
                b"1987654321",
                b"2147483647",
                b"2222222222",
                b"2718281828",
                b"3141592653",
                b"4000000000",
                b"9876543210",
                b"9999999999",
            ][..],
        ),
        (
            "18digit",
            &[
                b"100000000000000001".as_slice(),
                b"123456789012345678",
                b"222222222222222222",
                b"333333333333333333",
                b"444444444444444444",
                b"555555555555555555",
                b"666666666666666666",
                b"777777777777777777",
                b"888888888888888888",
                b"900000000000000000",
            ][..],
        ),
        (
            "negative",
            &[
                b"-10".as_slice(),
                b"-42",
                b"-75",
                b"-99",
                b"-123",
                b"-456",
                b"-789",
                b"-1024",
                b"-4096",
                b"-16384",
            ][..],
        ),
    ];

    for (label, samples) in cases {
        group.throughput(Throughput::Elements(1));

        let swar_name = format!("swar_integer_{label}");
        group.bench_function(swar_name, |b| {
            let mut index = 0;
            b.iter(|| {
                let input = black_box(*next_sample(samples, &mut index));
                black_box(swar_parse_int(input).unwrap())
            });
        });

        let baseline_name = format!("baseline_integer_{label}");
        group.bench_function(baseline_name, |b| {
            let mut index = 0;
            b.iter(|| {
                let input = black_box(*next_sample(samples, &mut index));
                let text = std::str::from_utf8(input).unwrap();
                black_box(text.parse::<i64>().unwrap())
            });
        });
    }

    group.finish();
}

// ── 0.3.1 — RESP Parse Benchmarks ──────────────────────────────────

fn bench_resp_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("resp_parse");

    // +OK\r\n
    let simple_string = b"+OK\r\n";
    group.throughput(Throughput::Bytes(simple_string.len() as u64));
    group.bench_function("simple_string", |b| {
        b.iter(|| black_box(RespParser::parse(black_box(simple_string)).unwrap()));
    });

    // $5\r\nhello\r\n
    let bulk_string = b"$5\r\nhello\r\n";
    group.throughput(Throughput::Bytes(bulk_string.len() as u64));
    group.bench_function("bulk_string", |b| {
        b.iter(|| black_box(RespParser::parse(black_box(bulk_string)).unwrap()));
    });

    // *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    let array = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    group.throughput(Throughput::Bytes(array.len() as u64));
    group.bench_function("array_set_cmd", |b| {
        b.iter(|| black_box(RespParser::parse(black_box(array)).unwrap()));
    });

    let pipeline_10 = make_set_pipeline(10);
    group.throughput(Throughput::Bytes(pipeline_10.len() as u64));
    group.bench_function("pipeline_10", |b| {
        b.iter(|| {
            let (frames, consumed) =
                RespParser::parse_pipeline(black_box(pipeline_10.as_slice())).unwrap();
            black_box((frames.len(), consumed));
        });
    });

    let pipeline_100 = make_set_pipeline(100);
    group.throughput(Throughput::Bytes(pipeline_100.len() as u64));
    group.bench_function("pipeline_100", |b| {
        b.iter(|| {
            let (frames, consumed) =
                RespParser::parse_pipeline(black_box(pipeline_100.as_slice())).unwrap();
            black_box((frames.len(), consumed));
        });
    });

    let pipeline_1000 = make_set_pipeline(1000);
    group.throughput(Throughput::Bytes(pipeline_1000.len() as u64));
    group.bench_function("pipeline_1000", |b| {
        b.iter(|| {
            let (frames, consumed) =
                RespParser::parse_pipeline(black_box(pipeline_1000.as_slice())).unwrap();
            black_box((frames.len(), consumed));
        });
    });

    // Zero-copy variants: measure parse-only throughput without the upfront
    // Bytes::copy_from_slice. Each Bytes::clone is a single atomic increment.

    let pipeline_10_bytes = Bytes::from(pipeline_10.clone());
    group.throughput(Throughput::Bytes(pipeline_10_bytes.len() as u64));
    group.bench_function("pipeline_10_zerocopy", |b| {
        b.iter(|| {
            let (frames, consumed) =
                RespParser::parse_pipeline_bytes(black_box(pipeline_10_bytes.clone())).unwrap();
            black_box((frames.len(), consumed));
        });
    });

    let pipeline_100_bytes = Bytes::from(pipeline_100.clone());
    group.throughput(Throughput::Bytes(pipeline_100_bytes.len() as u64));
    group.bench_function("pipeline_100_zerocopy", |b| {
        b.iter(|| {
            let (frames, consumed) =
                RespParser::parse_pipeline_bytes(black_box(pipeline_100_bytes.clone())).unwrap();
            black_box((frames.len(), consumed));
        });
    });

    let pipeline_1000_bytes = Bytes::from(pipeline_1000.clone());
    group.throughput(Throughput::Bytes(pipeline_1000_bytes.len() as u64));
    group.bench_function("pipeline_1000_zerocopy", |b| {
        b.iter(|| {
            let (frames, consumed) =
                RespParser::parse_pipeline_bytes(black_box(pipeline_1000_bytes.clone())).unwrap();
            black_box((frames.len(), consumed));
        });
    });

    let mixed = make_mixed_pipeline();
    group.throughput(Throughput::Bytes(mixed.len() as u64));
    group.bench_function("mixed_frame_types", |b| {
        b.iter(|| {
            let (frames, consumed) =
                RespParser::parse_pipeline(black_box(mixed.as_slice())).unwrap();
            black_box((frames.len(), consumed));
        });
    });

    let large_bulk = {
        let payload = vec![b'x'; 64 * 1024];
        let mut buf = format!("${}\r\n", payload.len()).into_bytes();
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(b"\r\n");
        buf
    };
    group.throughput(Throughput::Bytes(large_bulk.len() as u64));
    group.bench_function("large_bulk_string_64kb", |b| {
        b.iter(|| black_box(RespParser::parse(black_box(large_bulk.as_slice())).unwrap()));
    });

    let nested = make_nested_array(8);
    group.throughput(Throughput::Bytes(nested.len() as u64));
    group.bench_function("nested_array_depth_8", |b| {
        b.iter(|| black_box(RespParser::parse(black_box(nested.as_slice())).unwrap()));
    });

    group.finish();
}

// ── 0.3.2 — Tape Parse Benchmarks ──────────────────────────────────

fn bench_tape_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("tape_parse");

    let pipeline_10 = make_set_pipeline(10);
    group.throughput(Throughput::Bytes(pipeline_10.len() as u64));
    group.bench_function("tape_pipeline_10", |b| {
        b.iter(|| {
            let tape = RespTape::parse_pipeline(black_box(pipeline_10.as_slice())).unwrap();
            black_box((tape.frame_count(), tape.consumed()));
        });
    });

    let pipeline_100 = make_set_pipeline(100);
    group.throughput(Throughput::Bytes(pipeline_100.len() as u64));
    group.bench_function("tape_pipeline_100", |b| {
        b.iter(|| {
            let tape = RespTape::parse_pipeline(black_box(pipeline_100.as_slice())).unwrap();
            black_box((tape.frame_count(), tape.consumed()));
        });
    });

    let pipeline_1000 = make_set_pipeline(1000);
    group.throughput(Throughput::Bytes(pipeline_1000.len() as u64));
    group.bench_function("tape_pipeline_1000", |b| {
        b.iter(|| {
            let tape = RespTape::parse_pipeline(black_box(pipeline_1000.as_slice())).unwrap();
            black_box((tape.frame_count(), tape.consumed()));
        });
    });

    let pipeline_1000_bytes = Bytes::from(pipeline_1000);
    group.throughput(Throughput::Bytes(pipeline_1000_bytes.len() as u64));
    group.bench_function("tape_pipeline_1000_zerocopy", |b| {
        b.iter(|| {
            let tape =
                RespTape::parse_pipeline_bytes(black_box(pipeline_1000_bytes.clone())).unwrap();
            black_box((tape.frame_count(), tape.consumed()));
        });
    });

    group.finish();
}

// ── 0.3.1 — RESP Serialize Benchmarks ──────────────────────────────

fn bench_resp_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("resp_serialize");

    // ── Pre-computed path: +OK\r\n ──
    let ok_frame = RespFrame::ok();
    let ok_bytes = b"+OK\r\n";
    group.throughput(Throughput::Bytes(ok_bytes.len() as u64));
    group.bench_function("ok", |b| {
        let mut buf = BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            RespSerializer::serialize(&ok_frame, &mut buf);
        });
    });

    // ── LUT integer path: :42\r\n ──
    let int42 = RespFrame::Integer(42);
    let int42_bytes = b":42\r\n";
    group.throughput(Throughput::Bytes(int42_bytes.len() as u64));
    group.bench_function("integer_lut_42", |b| {
        let mut buf = BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            RespSerializer::serialize(&int42, &mut buf);
        });
    });

    // ── LUT large integer: :9999\r\n (LUT boundary) ──
    let int9999 = RespFrame::Integer(9999);
    group.throughput(Throughput::Bytes(b":9999\r\n".len() as u64));
    group.bench_function("integer_lut_9999", |b| {
        let mut buf = BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            RespSerializer::serialize(&int9999, &mut buf);
        });
    });

    // ── itoa fallback integer: :123456\r\n ──
    let int_large = RespFrame::Integer(123_456);
    group.throughput(Throughput::Bytes(b":123456\r\n".len() as u64));
    group.bench_function("integer_itoa_123456", |b| {
        let mut buf = BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            RespSerializer::serialize(&int_large, &mut buf);
        });
    });

    // ── Bulk string ──
    let bulk = RespFrame::bulk_string("hello world this is a value");
    let bulk_wire = b"$27\r\nhello world this is a value\r\n";
    group.throughput(Throughput::Bytes(bulk_wire.len() as u64));
    group.bench_function("bulk_string", |b| {
        let mut buf = BytesMut::with_capacity(128);
        b.iter(|| {
            buf.clear();
            RespSerializer::serialize(&bulk, &mut buf);
        });
    });

    // ── 3-element array (SET command) ──
    let arr = RespFrame::Array(Some(vec![
        RespFrame::bulk_string("SET"),
        RespFrame::bulk_string("mykey"),
        RespFrame::bulk_string("myvalue"),
    ]));
    let arr_wire = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
    group.throughput(Throughput::Bytes(arr_wire.len() as u64));
    group.bench_function("array_3", |b| {
        let mut buf = BytesMut::with_capacity(128);
        b.iter(|| {
            buf.clear();
            RespSerializer::serialize(&arr, &mut buf);
        });
    });

    // ── Pipeline: 100 OK responses ──
    let ok_100: Vec<_> = (0..100).map(|_| RespFrame::ok()).collect();
    let ok_100_total = ok_100.len() * ok_bytes.len();
    group.throughput(Throughput::Bytes(ok_100_total as u64));
    group.bench_function("pipeline_ok_100", |b| {
        let mut buf = BytesMut::with_capacity(ok_100_total);
        b.iter(|| {
            buf.clear();
            for frame in &ok_100 {
                RespSerializer::serialize(frame, &mut buf);
            }
        });
    });

    // ── Pipeline: 100 integer responses ──
    let int_100: Vec<_> = (0..100).map(RespFrame::Integer).collect();
    // Estimate total output size.
    let mut int_100_buf = BytesMut::with_capacity(1024);
    for f in &int_100 {
        RespSerializer::serialize(f, &mut int_100_buf);
    }
    let int_100_total = int_100_buf.len();
    group.throughput(Throughput::Bytes(int_100_total as u64));
    group.bench_function("pipeline_integer_100", |b| {
        let mut buf = BytesMut::with_capacity(int_100_total);
        b.iter(|| {
            buf.clear();
            for frame in &int_100 {
                RespSerializer::serialize(frame, &mut buf);
            }
        });
    });

    // ── serialize_to_slice: +OK\r\n ──
    group.throughput(Throughput::Bytes(ok_bytes.len() as u64));
    group.bench_function("slice_ok", |b| {
        let mut buf = [0u8; 64];
        b.iter(|| {
            black_box(RespSerializer::serialize_to_slice(&ok_frame, &mut buf).unwrap());
        });
    });

    // ── serialize_to_slice: :42\r\n ──
    group.throughput(Throughput::Bytes(int42_bytes.len() as u64));
    group.bench_function("slice_integer_42", |b| {
        let mut buf = [0u8; 64];
        b.iter(|| {
            black_box(RespSerializer::serialize_to_slice(&int42, &mut buf).unwrap());
        });
    });

    // ── serialize_to_slice: array_3 ──
    group.throughput(Throughput::Bytes(arr_wire.len() as u64));
    group.bench_function("slice_array_3", |b| {
        let mut buf = [0u8; 256];
        b.iter(|| {
            black_box(RespSerializer::serialize_to_slice(&arr, &mut buf).unwrap());
        });
    });

    group.finish();
}

// ── 0.3.2 — RESP Serialize-to-Iovec Benchmarks ────────────────────

fn bench_resp_serialize_iovecs(c: &mut Criterion) {
    let mut group = c.benchmark_group("resp_serialize_iovecs");

    // ── iovec: +OK\r\n ──
    let ok_frame = RespFrame::ok();
    let ok_bytes = b"+OK\r\n";
    group.throughput(Throughput::Bytes(ok_bytes.len() as u64));
    group.bench_function("iovec_ok", |b| {
        let mut w = IovecWriter::new();
        b.iter(|| {
            w.clear();
            RespSerializer::serialize_to_iovecs(black_box(&ok_frame), &mut w);
            black_box(w.total_len());
        });
    });

    // ── iovec: :42\r\n ──
    let int42 = RespFrame::Integer(42);
    let int42_bytes = b":42\r\n";
    group.throughput(Throughput::Bytes(int42_bytes.len() as u64));
    group.bench_function("iovec_integer_42", |b| {
        let mut w = IovecWriter::new();
        b.iter(|| {
            w.clear();
            RespSerializer::serialize_to_iovecs(black_box(&int42), &mut w);
            black_box(w.total_len());
        });
    });

    // ── iovec: bulk string ──
    let bulk = RespFrame::bulk_string("hello world this is a value");
    let bulk_wire = b"$27\r\nhello world this is a value\r\n";
    group.throughput(Throughput::Bytes(bulk_wire.len() as u64));
    group.bench_function("iovec_bulk_string", |b| {
        let mut w = IovecWriter::new();
        b.iter(|| {
            w.clear();
            RespSerializer::serialize_to_iovecs(black_box(&bulk), &mut w);
            black_box(w.total_len());
        });
    });

    // ── iovec: 3-element array ──
    let arr3 = RespFrame::Array(Some(vec![
        RespFrame::bulk_string("SET"),
        RespFrame::bulk_string("mykey"),
        RespFrame::bulk_string("myvalue"),
    ]));
    let arr3_wire = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
    group.throughput(Throughput::Bytes(arr3_wire.len() as u64));
    group.bench_function("iovec_array_3", |b| {
        let mut w = IovecWriter::new();
        b.iter(|| {
            w.clear();
            RespSerializer::serialize_to_iovecs(black_box(&arr3), &mut w);
            black_box(w.total_len());
        });
    });

    // ── iovec: 10-element array (AC target: >6 GB/s) ──
    let arr10 = RespFrame::Array(Some(
        (0..10)
            .map(|_| RespFrame::bulk_string("value12345"))
            .collect(),
    ));
    // Pre-compute expected wire size.
    let mut arr10_buf = BytesMut::with_capacity(256);
    RespSerializer::serialize(&arr10, &mut arr10_buf);
    let arr10_len = arr10_buf.len();
    group.throughput(Throughput::Bytes(arr10_len as u64));
    group.bench_function("iovec_array_10", |b| {
        let mut w = IovecWriter::new();
        b.iter(|| {
            w.clear();
            RespSerializer::serialize_to_iovecs(black_box(&arr10), &mut w);
            black_box(w.total_len());
        });
    });

    // ── iovec: pipeline 100 OK responses ──
    let ok_100_total = 100 * ok_bytes.len();
    group.throughput(Throughput::Bytes(ok_100_total as u64));
    group.bench_function("iovec_pipeline_ok_100", |b| {
        let mut w = IovecWriter::new();
        let ok = RespFrame::ok();
        b.iter(|| {
            w.clear();
            for _ in 0..100 {
                RespSerializer::serialize_to_iovecs(black_box(&ok), &mut w);
            }
            black_box(w.total_len());
        });
    });

    // ── iovec flatten: 10-element array (measures full path: serialize + flatten) ──
    group.throughput(Throughput::Bytes(arr10_len as u64));
    group.bench_function("iovec_array_10_flatten", |b| {
        let mut w = IovecWriter::new();
        b.iter(|| {
            w.clear();
            RespSerializer::serialize_to_iovecs(black_box(&arr10), &mut w);
            black_box(w.flatten());
        });
    });

    // ── iovec: large 1KB bulk string (scatter-gather sweet spot) ──
    let large_value = Bytes::from("x".repeat(1024));
    let large_bulk = RespFrame::BulkString(Some(large_value.clone()));
    let mut large_buf = BytesMut::with_capacity(1100);
    RespSerializer::serialize(&large_bulk, &mut large_buf);
    let large_len = large_buf.len();
    group.throughput(Throughput::Bytes(large_len as u64));
    group.bench_function("iovec_bulk_1kb", |b| {
        let mut w = IovecWriter::new();
        b.iter(|| {
            w.clear();
            RespSerializer::serialize_to_iovecs(black_box(&large_bulk), &mut w);
            black_box(w.total_len());
        });
    });

    // ── iovec: LRANGE-style 100-element array with 64-byte values ──
    let lrange_value = Bytes::from("v".repeat(64));
    let lrange = RespFrame::Array(Some(
        (0..100)
            .map(|_| RespFrame::BulkString(Some(lrange_value.clone())))
            .collect(),
    ));
    let mut lrange_buf = BytesMut::with_capacity(8192);
    RespSerializer::serialize(&lrange, &mut lrange_buf);
    let lrange_len = lrange_buf.len();
    group.throughput(Throughput::Bytes(lrange_len as u64));
    group.bench_function("iovec_lrange_100", |b| {
        let mut w = IovecWriter::new();
        b.iter(|| {
            w.clear();
            RespSerializer::serialize_to_iovecs(black_box(&lrange), &mut w);
            black_box(w.total_len());
        });
    });

    group.finish();
}

// ── Command Dispatch Benchmarks ─────────────────────────────────────────────

fn bench_command_dispatch(c: &mut Criterion) {
    use vortex_proto::{CommandRouter, uppercase_inplace};

    let mut group = c.benchmark_group("command_dispatch");

    // ── SWAR uppercase (4 bytes — PING) ────
    group.bench_function("swar_upper_4b", |b| {
        let mut buf = *b"ping";
        b.iter(|| {
            buf.copy_from_slice(b"ping");
            uppercase_inplace(black_box(&mut buf));
            black_box(&buf);
        });
    });

    // ── SWAR uppercase (11 bytes — INCRBYFLOAT crosses 8-byte boundary) ────
    group.bench_function("swar_upper_11b", |b| {
        let mut buf = *b"incrbyfloat";
        b.iter(|| {
            buf.copy_from_slice(b"incrbyfloat");
            uppercase_inplace(black_box(&mut buf));
            black_box(&buf);
        });
    });

    // ── PHF lookup only (no tape overhead) ────
    group.bench_function("phf_lookup_ping", |b| {
        b.iter(|| {
            black_box(vortex_proto::command::lookup_command(black_box("PING")));
        });
    });

    // ── PHF lookup + uppercase (core dispatch, no tape) ────
    group.bench_function("upper_and_lookup_set", |b| {
        let mut scratch = [0u8; 8];
        let cmd = b"set";
        b.iter(|| {
            scratch[..cmd.len()].copy_from_slice(cmd);
            uppercase_inplace(&mut scratch[..cmd.len()]);
            let name = unsafe { std::str::from_utf8_unchecked(&scratch[..cmd.len()]) };
            black_box(vortex_proto::command::lookup_command(black_box(name)));
        });
    });

    // ── Full dispatch: PING (1 arg, arity -1) ────
    let ping_wire = b"*1\r\n$4\r\nPING\r\n";
    let ping_tape = RespTape::parse_pipeline(ping_wire).unwrap();
    let ping_frame = ping_tape.iter().next().unwrap();
    group.bench_function("dispatch_ping", |b| {
        let mut router = CommandRouter::new();
        b.iter(|| {
            black_box(router.dispatch(black_box(&ping_frame)));
        });
    });

    // ── Full dispatch: SET foo bar (3 args, arity -3) ────
    let set_wire = b"*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let set_tape = RespTape::parse_pipeline(set_wire).unwrap();
    let set_frame = set_tape.iter().next().unwrap();
    group.bench_function("dispatch_set", |b| {
        let mut router = CommandRouter::new();
        b.iter(|| {
            black_box(router.dispatch(black_box(&set_frame)));
        });
    });

    // ── Full dispatch: HSET (long command name, 4 args) ────
    let hset_wire = b"*4\r\n$4\r\nhset\r\n$6\r\nmyhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let hset_tape = RespTape::parse_pipeline(hset_wire).unwrap();
    let hset_frame = hset_tape.iter().next().unwrap();
    group.bench_function("dispatch_hset", |b| {
        let mut router = CommandRouter::new();
        b.iter(|| {
            black_box(router.dispatch(black_box(&hset_frame)));
        });
    });

    // ── Full dispatch: unknown command ────
    let unk_wire = b"*1\r\n$6\r\nFOOBAR\r\n";
    let unk_tape = RespTape::parse_pipeline(unk_wire).unwrap();
    let unk_frame = unk_tape.iter().next().unwrap();
    group.bench_function("dispatch_unknown", |b| {
        let mut router = CommandRouter::new();
        b.iter(|| {
            black_box(router.dispatch(black_box(&unk_frame)));
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_crlf_scan,
    bench_swar_integer_parse,
    bench_resp_parse,
    bench_tape_parse,
    bench_resp_serialize,
    bench_resp_serialize_iovecs,
    bench_command_dispatch
);
criterion_main!(benches);
