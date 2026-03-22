use std::sync::Arc;
use std::thread;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use vortex_sync::{MpscQueue, SpscRingBuffer};

// ── 0.3.3 — Ring Buffer / Queue Benchmarks ──────────────────────────

const ITEMS: u64 = 100_000;

fn bench_spsc_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_throughput");
    group.throughput(Throughput::Elements(ITEMS));

    group.bench_function("spsc_100k", |b| {
        b.iter(|| {
            let ring: Arc<SpscRingBuffer<u64, 1024>> = Arc::new(SpscRingBuffer::new());
            let producer = Arc::clone(&ring);
            let consumer = Arc::clone(&ring);

            let p = thread::spawn(move || {
                for i in 0..ITEMS {
                    while producer.push(i).is_err() {
                        std::hint::spin_loop();
                    }
                }
            });

            let c = thread::spawn(move || {
                let mut count = 0u64;
                while count < ITEMS {
                    if consumer.pop().is_some() {
                        count += 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            });

            p.join().unwrap();
            c.join().unwrap();
        });
    });

    group.finish();
}

fn bench_mpsc_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc_throughput");

    for &num_producers in &[2, 8] {
        let items_per_producer = ITEMS / num_producers;
        group.throughput(Throughput::Elements(items_per_producer * num_producers));

        group.bench_function(format!("{num_producers}_producers"), |b| {
            b.iter(|| {
                let q: Arc<MpscQueue<u64>> = Arc::new(MpscQueue::new());
                let total = items_per_producer * num_producers;

                let mut handles = Vec::new();
                for t in 0..num_producers {
                    let q = Arc::clone(&q);
                    handles.push(thread::spawn(move || {
                        for i in 0..items_per_producer {
                            q.push(t * items_per_producer + i);
                        }
                    }));
                }

                // Consumer: drain all items
                let consumer_q = Arc::clone(&q);
                let consumer = thread::spawn(move || {
                    let mut count = 0u64;
                    while count < total {
                        if consumer_q.pop().is_some() {
                            count += 1;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                });

                for h in handles {
                    h.join().unwrap();
                }
                consumer.join().unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_spsc_throughput, bench_mpsc_throughput);
criterion_main!(benches);
