use std::mem::size_of;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use vortex_common::{VortexKey, VortexValue};
use vortex_engine::{Entry, SwissTable};

// ── 0.3.2 — Swiss Table Benchmarks ─────────────────────────────────

fn prefill_table(n: usize) -> SwissTable {
    let mut table = SwissTable::with_capacity(n);
    for i in 0..n {
        let key = VortexKey::from(format!("key:{i:08}").as_str());
        table.insert(key, VortexValue::Integer(i as i64));
    }
    table
}

fn bench_swiss_table_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("swiss_table_insert");
    for &size in &[100, 10_000, 1_000_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || SwissTable::with_capacity(size),
                |mut table| {
                    for i in 0..size {
                        let key = VortexKey::from(format!("key:{i:08}").as_str());
                        table.insert(key, VortexValue::Integer(i as i64));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_swiss_table_lookup_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("swiss_table_lookup_hit");
    for &size in &[100, 10_000, 1_000_000] {
        let table = prefill_table(size);
        let key = VortexKey::from(format!("key:{:08}", size / 2).as_str());
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| table.get(&key));
        });
    }
    group.finish();
}

fn bench_swiss_table_lookup_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("swiss_table_lookup_miss");
    for &size in &[100, 10_000, 1_000_000] {
        let table = prefill_table(size);
        let key = VortexKey::from("nonexistent_key");
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| table.get(&key));
        });
    }
    group.finish();
}

fn bench_swiss_table_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("swiss_table_delete");
    for &size in &[100, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || prefill_table(size),
                |mut table| {
                    for i in 0..size {
                        let key = VortexKey::from(format!("key:{i:08}").as_str());
                        table.remove(&key);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Benchmark single-key insert into a pre-sized table with pre-generated keys.
/// This isolates the hash table insert cost from key allocation.
fn bench_swiss_table_insert_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("swiss_table_insert_single");
    for &size in &[100, 10_000, 1_000_000] {
        let keys: Vec<VortexKey> = (0..size)
            .map(|i| VortexKey::from(format!("key:{i:08}").as_str()))
            .collect();
        let values: Vec<VortexValue> = (0..size).map(|i| VortexValue::Integer(i as i64)).collect();
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    (
                        SwissTable::with_capacity(size),
                        keys.clone(),
                        values.clone(),
                    )
                },
                |(mut table, keys, values)| {
                    for (k, v) in keys.into_iter().zip(values) {
                        table.insert(k, v);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Benchmark single-key delete with pre-generated keys.
fn bench_swiss_table_delete_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("swiss_table_delete_single");
    for &size in &[100, 10_000, 1_000_000] {
        let keys: Vec<VortexKey> = (0..size)
            .map(|i| VortexKey::from(format!("key:{i:08}").as_str()))
            .collect();
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let table = prefill_table(size);
                    (table, keys.clone())
                },
                |(mut table, keys)| {
                    for k in &keys {
                        table.remove(k);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ── 3.2 — Entry Benchmarks ────────────────────────────────────────

fn bench_entry_write_inline(c: &mut Criterion) {
    let key = *b"0123456789";
    let value = *b"abcdefghij";

    c.bench_function("entry_write_inline", |b| {
        b.iter_batched(
            Entry::empty,
            |mut entry| {
                entry.write_inline(
                    black_box(0x91),
                    black_box(&key),
                    black_box(&value),
                    black_box(42),
                );
                black_box(entry);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_entry_read_inline(c: &mut Criterion) {
    let mut entry = Entry::empty();
    entry.write_inline(0x91, b"0123456789", b"abcdefghij", 42);

    c.bench_function("entry_read_inline", |b| {
        b.iter(|| black_box(entry.read_key()));
    });
}

fn bench_entry_matches_key(c: &mut Criterion) {
    let mut entry = Entry::empty();
    let key = b"0123456789";
    entry.write_inline(0x91, key, b"abcdefghij", 42);

    c.bench_function("entry_matches_key", |b| {
        b.iter(|| black_box(entry.matches_key_with_ctrl(black_box(key), black_box(0x91))));
    });
}

fn bench_entry_is_expired(c: &mut Criterion) {
    let mut entry = Entry::empty();
    entry.write_inline(0x91, b"0123456789", b"abcdefghij", 1_000);

    c.bench_function("entry_is_expired", |b| {
        b.iter(|| black_box(entry.is_expired(black_box(1_001))));
    });
}

fn bench_entry_write_integer(c: &mut Criterion) {
    let key = *b"0123456789";

    c.bench_function("entry_write_integer", |b| {
        b.iter_batched(
            Entry::empty,
            |mut entry| {
                entry.write_integer(
                    black_box(0x91),
                    black_box(&key),
                    black_box(123_456_i64),
                    black_box(42),
                );
                black_box(entry);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

// ── Task 3.9 — Prefetch Pipeline Benchmarks ─────────────────────────

fn bench_table_batch_100_1m_prefetch(c: &mut Criterion) {
    let table = prefill_table(1_000_000);

    let key_sets: Vec<Vec<VortexKey>> = (0..100)
        .map(|set| {
            (0..100)
                .map(|i| {
                    let idx = (set * 100 + i * 9_973) % 1_000_000;
                    VortexKey::from(format!("key:{idx:08}").as_bytes())
                })
                .collect()
        })
        .collect();

    let mut set_idx = 0usize;
    c.bench_function("table_batch_100_1m_prefetch", |b| {
        b.iter(|| {
            let keys = &key_sets[set_idx % key_sets.len()];
            set_idx += 1;

            let hashes: Vec<u64> = keys
                .iter()
                .map(|k| {
                    let h = table.hash_key_bytes(k.as_bytes());
                    table.prefetch_group(h);
                    h
                })
                .collect();
            let mut count = 0u64;
            for key in keys {
                if table.get(key).is_some() {
                    count += 1;
                }
            }
            black_box((count, hashes.len()));
        });
    });
}

fn bench_table_batch_100_1m_no_prefetch(c: &mut Criterion) {
    let table = prefill_table(1_000_000);

    let key_sets: Vec<Vec<VortexKey>> = (0..100)
        .map(|set| {
            (0..100)
                .map(|i| {
                    let idx = (set * 100 + i * 9_973) % 1_000_000;
                    VortexKey::from(format!("key:{idx:08}").as_bytes())
                })
                .collect()
        })
        .collect();

    let mut set_idx = 0usize;
    c.bench_function("table_batch_100_1m_no_prefetch", |b| {
        b.iter(|| {
            let keys = &key_sets[set_idx % key_sets.len()];
            set_idx += 1;

            let mut count = 0u64;
            for key in keys {
                if table.get(key).is_some() {
                    count += 1;
                }
            }
            black_box(count);
        });
    });
}

// ── 3.10.1 — Missing Swiss Table Micro-Benchmarks ──────────────────

fn bench_swiss_table_resize_amortized(c: &mut Criterion) {
    let mut group = c.benchmark_group("swiss_table_resize_amortized");
    for &target in &[1_000, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(target),
            &target,
            |b, &target| {
                b.iter_batched(
                    SwissTable::new,
                    |mut table| {
                        for i in 0..target {
                            let key = VortexKey::from(format!("key:{i:08}").as_str());
                            table.insert(key, VortexValue::Integer(i as i64));
                        }
                        black_box(table.len());
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_swiss_table_mixed_50_50(c: &mut Criterion) {
    let mut group = c.benchmark_group("swiss_table_mixed_50_50");
    for &size in &[10_000, 1_000_000] {
        let keys: Vec<VortexKey> = (0..size)
            .map(|i| VortexKey::from(format!("key:{i:08}").as_str()))
            .collect();
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || prefill_table(size),
                |mut table| {
                    for i in 0..1000 {
                        if i & 1 == 0 {
                            black_box(table.get(&keys[i % size]));
                        } else {
                            table.insert(keys[i % size].clone(), VortexValue::Integer(i as i64));
                        }
                    }
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

// ── 3.10.2 — Missing Entry Benchmark ───────────────────────────────

fn bench_entry_read_integer(c: &mut Criterion) {
    let mut entry = Entry::empty();
    entry.write_integer(0x91, b"counter\x00\x00\x00", 42_i64, 0);

    c.bench_function("entry_read_integer", |b| {
        b.iter(|| black_box(entry.read_value()));
    });
}

// ── 3.10.5 — Memory Efficiency ─────────────────────────────────────

fn bench_memory_per_entry(c: &mut Criterion) {
    c.bench_function("memory_per_entry_1m", |b| {
        b.iter_batched(
            || (),
            |()| {
                let n = 1_000_000usize;
                let mut table = SwissTable::with_capacity(n);
                for i in 0..n {
                    let key = VortexKey::from(format!("k:{i:07}").as_bytes());
                    table.insert(key, VortexValue::from_bytes(b"v:12345678"));
                }

                let total_slots = table.total_slots();
                let entry_bytes = total_slots * 64;
                let ctrl_bytes = total_slots + 16;
                let key_vec_bytes = total_slots * size_of::<Option<VortexKey>>();
                let value_vec_bytes = total_slots * size_of::<Option<VortexValue>>();
                let total = entry_bytes + ctrl_bytes + key_vec_bytes + value_vec_bytes;
                let per_entry = total / n;
                black_box(per_entry)
            },
            criterion::BatchSize::LargeInput,
        );
    });
}

// ── 3.10.4 — Latency Distribution ─────────────────────────────────

criterion_group!(
    benches,
    bench_swiss_table_insert,
    bench_swiss_table_insert_single,
    bench_swiss_table_lookup_hit,
    bench_swiss_table_lookup_miss,
    bench_swiss_table_delete,
    bench_swiss_table_delete_single,
    bench_swiss_table_resize_amortized,
    bench_swiss_table_mixed_50_50,
    bench_entry_write_inline,
    bench_entry_read_inline,
    bench_entry_matches_key,
    bench_entry_is_expired,
    bench_entry_write_integer,
    bench_entry_read_integer,
    bench_table_batch_100_1m_prefetch,
    bench_table_batch_100_1m_no_prefetch,
    bench_memory_per_entry,
);
criterion_main!(benches);
