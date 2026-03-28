use bytes::{Bytes, BytesMut};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use vortex_proto::frame::RespFrame;
use vortex_proto::{RespParser, RespSerializer, RespTape, scan_crlf, swar_parse_int};

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
        ("1digit", &[b"0".as_slice(), b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8", b"9"][..]),
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
            let (frames, consumed) = RespParser::parse_pipeline(black_box(pipeline_10.as_slice())).unwrap();
            black_box((frames.len(), consumed));
        });
    });

    let pipeline_100 = make_set_pipeline(100);
    group.throughput(Throughput::Bytes(pipeline_100.len() as u64));
    group.bench_function("pipeline_100", |b| {
        b.iter(|| {
            let (frames, consumed) = RespParser::parse_pipeline(black_box(pipeline_100.as_slice())).unwrap();
            black_box((frames.len(), consumed));
        });
    });

    let pipeline_1000 = make_set_pipeline(1000);
    group.throughput(Throughput::Bytes(pipeline_1000.len() as u64));
    group.bench_function("pipeline_1000", |b| {
        b.iter(|| {
            let (frames, consumed) = RespParser::parse_pipeline(black_box(pipeline_1000.as_slice())).unwrap();
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
            let (frames, consumed) = RespParser::parse_pipeline(black_box(mixed.as_slice())).unwrap();
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

    let ok_frame = RespFrame::ok();
    group.bench_function("ok", |b| {
        let mut buf = BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            RespSerializer::serialize(&ok_frame, &mut buf);
        });
    });

    let bulk = RespFrame::bulk_string("hello world this is a value");
    group.bench_function("bulk_string", |b| {
        let mut buf = BytesMut::with_capacity(128);
        b.iter(|| {
            buf.clear();
            RespSerializer::serialize(&bulk, &mut buf);
        });
    });

    let arr = RespFrame::Array(Some(vec![
        RespFrame::bulk_string("SET"),
        RespFrame::bulk_string("mykey"),
        RespFrame::bulk_string("myvalue"),
    ]));
    group.bench_function("array_3", |b| {
        let mut buf = BytesMut::with_capacity(128);
        b.iter(|| {
            buf.clear();
            RespSerializer::serialize(&arr, &mut buf);
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
    bench_resp_serialize
);
criterion_main!(benches);
