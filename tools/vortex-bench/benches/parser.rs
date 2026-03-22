use criterion::{Criterion, criterion_group, criterion_main};
use vortex_proto::RespParser;

fn bench_parse_inline(c: &mut Criterion) {
    let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    c.bench_function("parse_resp_set", |b| {
        b.iter(|| {
            let _ = RespParser::parse(input);
        });
    });
}

criterion_group!(benches, bench_parse_inline);
criterion_main!(benches);
