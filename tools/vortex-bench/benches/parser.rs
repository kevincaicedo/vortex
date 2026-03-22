use bytes::BytesMut;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use vortex_proto::frame::RespFrame;
use vortex_proto::{RespParser, RespSerializer};

// ── 0.3.1 — RESP Parse Benchmarks ──────────────────────────────────

fn bench_resp_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("resp_parse");

    // +OK\r\n
    let simple_string = b"+OK\r\n";
    group.throughput(Throughput::Bytes(simple_string.len() as u64));
    group.bench_function("simple_string", |b| {
        b.iter(|| RespParser::parse(simple_string).unwrap());
    });

    // $5\r\nhello\r\n
    let bulk_string = b"$5\r\nhello\r\n";
    group.throughput(Throughput::Bytes(bulk_string.len() as u64));
    group.bench_function("bulk_string", |b| {
        b.iter(|| RespParser::parse(bulk_string).unwrap());
    });

    // *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    let array = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    group.throughput(Throughput::Bytes(array.len() as u64));
    group.bench_function("array_set_cmd", |b| {
        b.iter(|| RespParser::parse(array).unwrap());
    });

    // Pipeline of 10 SET commands
    let mut pipeline = Vec::new();
    for i in 0..10 {
        pipeline.extend_from_slice(
            format!("*3\r\n$3\r\nSET\r\n$5\r\nkey:{i}\r\n$5\r\nval:{i}\r\n").as_bytes(),
        );
    }
    group.throughput(Throughput::Bytes(pipeline.len() as u64));
    group.bench_function("pipeline_10", |b| {
        b.iter(|| {
            let mut buf = pipeline.as_slice();
            while !buf.is_empty() {
                let (_, consumed) = RespParser::parse(buf).unwrap();
                buf = &buf[consumed..];
            }
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

criterion_group!(benches, bench_resp_parse, bench_resp_serialize);
criterion_main!(benches);
