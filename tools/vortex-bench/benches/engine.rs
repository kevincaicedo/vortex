use criterion::{Criterion, criterion_group, criterion_main};
use vortex_common::{VortexKey, VortexValue};
use vortex_engine::Shard;

fn bench_set_get(c: &mut Criterion) {
    let mut shard = Shard::new(0);
    let key = VortexKey::from("bench_key");
    c.bench_function("shard_set", |b| {
        b.iter(|| {
            shard.set(key.clone(), VortexValue::Integer(42));
        });
    });
    c.bench_function("shard_get", |b| {
        b.iter(|| {
            let _ = shard.get(&key);
        });
    });
}

criterion_group!(benches, bench_set_get);
criterion_main!(benches);
