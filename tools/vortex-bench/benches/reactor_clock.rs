use criterion::{Criterion, black_box, criterion_group, criterion_main};
use vortex_common::Timestamp;

#[inline]
fn current_unix_time_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn reactor_clock_overhead(c: &mut Criterion) {
    let mono_anchor = Timestamp::now().as_nanos();
    let unix_anchor = current_unix_time_nanos();
    let unix_offset = unix_anchor.saturating_sub(mono_anchor);

    c.bench_function("reactor_clock_mono_only", |b| {
        b.iter(|| {
            let mono_now = Timestamp::now().as_nanos();
            black_box(mono_now);
        });
    });

    c.bench_function("reactor_clock_mono_plus_unix", |b| {
        b.iter(|| {
            let mono_now = Timestamp::now().as_nanos();
            let unix_now = current_unix_time_nanos();
            black_box((mono_now, unix_now));
        });
    });

    c.bench_function("reactor_clock_mono_plus_derived_unix", |b| {
        b.iter(|| {
            let mono_now = Timestamp::now().as_nanos();
            let unix_now = mono_now.saturating_add(black_box(unix_offset));
            black_box((mono_now, unix_now));
        });
    });
}

criterion_group!(benches, reactor_clock_overhead);
criterion_main!(benches);
