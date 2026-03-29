use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use vortex_common::{ShardId, VortexKey, VortexValue};
use vortex_engine::commands::execute_command;
use vortex_engine::{Entry, ExpiryEntry, ExpiryWheel, Shard, SwissTable};
use vortex_proto::RespTape;

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
        // Pre-generate all keys and values.
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

// ── 3.3 — Expiry Benchmarks ───────────────────────────────────────

const NS_PER_SEC: u64 = 1_000_000_000;

fn bench_expiry_register(c: &mut Criterion) {
    c.bench_function("expiry_register", |b| {
        let mut wheel = ExpiryWheel::new();
        wheel.set_time(NS_PER_SEC);
        let mut counter = 0u64;

        b.iter(|| {
            counter = counter.wrapping_add(1);
            wheel.register(black_box(counter), black_box(10 * NS_PER_SEC));
        });
    });
}

fn bench_expiry_tick_20(c: &mut Criterion) {
    // Measure the core drain of 20 expired entries from a slot.
    let deadline = 5 * NS_PER_SEC;
    let now = 6 * NS_PER_SEC;

    c.bench_function("expiry_tick_20", |b| {
        b.iter_batched(
            || {
                // Setup: a Vec with 20 expired entries (simulates one slot).
                (0u64..20)
                    .map(|i| ExpiryEntry {
                        key_hash: i,
                        deadline_nanos: deadline,
                    })
                    .collect::<Vec<_>>()
            },
            |mut entries| {
                let mut expired = Vec::with_capacity(20);
                let mut i = 0;
                while i < entries.len() {
                    if entries[i].deadline_nanos <= now {
                        expired.push(entries.swap_remove(i));
                    } else {
                        i += 1;
                    }
                }
                black_box(&expired);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_lazy_expiry_overhead(c: &mut Criterion) {
    // Measure shard.get() with lazy expiry on a non-expired key.
    // The overhead vs plain Swiss Table GET (~5 ns) is the is_expired branch.
    c.bench_function("lazy_expiry_overhead", |b| {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from("bench_key");
        shard.set_with_ttl(key.clone(), VortexValue::from("value"), 999 * NS_PER_SEC);

        b.iter(|| {
            black_box(shard.get(black_box(&key), black_box(NS_PER_SEC)));
        });
    });
}

fn bench_active_sweep_100k(c: &mut Criterion) {
    c.bench_function("active_sweep_100K", |b| {
        b.iter_batched_ref(
            || {
                let mut shard = Shard::new_with_time(ShardId::new(0), 0);
                // Place 20 entries in each of 5000 second-slots (slots 1..=5000).
                for slot in 1u64..=5000 {
                    let deadline = slot * NS_PER_SEC;
                    for j in 0..20u64 {
                        let idx = (slot - 1) * 20 + j;
                        let key = VortexKey::from(format!("key:{idx:08}").as_str());
                        shard.set_with_ttl(key, VortexValue::Integer(idx as i64), deadline);
                    }
                }
                (shard, NS_PER_SEC) // start sweeping from t=1s
            },
            |(shard, now)| {
                // Sweep 20 entries from one second-slot.
                let result = shard.run_active_expiry(black_box(*now), black_box(20));
                *now += NS_PER_SEC;
                black_box(result);
            },
            criterion::BatchSize::LargeInput,
        );
    });
}

// ── 3.4 — String Command Benchmarks ────────────────────────────────

/// Build a RESP array command from parts.
fn make_resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        buf.extend_from_slice(part);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

fn bench_cmd_get_inline(c: &mut Criterion) {
    let mut shard = Shard::new(ShardId::new(0));
    // Pre-populate with inline-sized key+value.
    let key = VortexKey::from(b"mykey" as &[u8]);
    shard.set(key, VortexValue::from_bytes(b"myvalue"));

    let cmd = make_resp(&[b"GET", b"mykey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_get_inline", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&mut shard), b"GET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_set_inline(c: &mut Criterion) {
    let cmd = make_resp(&[b"SET", b"mykey", b"myvalue"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_set_inline", |b| {
        // Pre-create shard: benchmark measures overwrite (hot-path SET).
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"mykey" as &[u8]);
        shard.set(key, VortexValue::from_bytes(b"old"));

        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&mut shard), b"SET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_get_miss(c: &mut Criterion) {
    let mut shard = Shard::new(ShardId::new(0));
    let cmd = make_resp(&[b"GET", b"nosuchkey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_get_miss", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&mut shard), b"GET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_incr(c: &mut Criterion) {
    let cmd = make_resp(&[b"INCR", b"counter"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_incr", |b| {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"counter" as &[u8]);
        shard.set(key, VortexValue::Integer(0));

        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&mut shard), b"INCR", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_mget_100(c: &mut Criterion) {
    let mut shard = Shard::new(ShardId::new(0));
    // Pre-populate 100 keys.
    let mut parts: Vec<Vec<u8>> = vec![b"MGET".to_vec()];
    for i in 0..100 {
        let key_str = format!("key:{i:04}");
        let key = VortexKey::from(key_str.as_bytes());
        shard.set(key, VortexValue::Integer(i));
        parts.push(key_str.into_bytes());
    }

    let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    let cmd = make_resp(&refs);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_mget_100", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&mut shard), b"MGET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_mset_100(c: &mut Criterion) {
    let mut parts: Vec<Vec<u8>> = vec![b"MSET".to_vec()];
    for i in 0..100 {
        parts.push(format!("key:{i:04}").into_bytes());
        parts.push(format!("val:{i:04}").into_bytes());
    }

    let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    let cmd = make_resp(&refs);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_mset_100", |b| {
        // Pre-create shard with capacity; measures overwrite path.
        let mut shard = Shard::new(ShardId::new(0));
        for i in 0..100 {
            let key = VortexKey::from(format!("key:{i:04}").as_bytes());
            shard.set(key, VortexValue::from_bytes(b"old"));
        }

        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&mut shard), b"MSET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_append_inline(c: &mut Criterion) {
    let cmd = make_resp(&[b"APPEND", b"mykey", b"abc"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_append_inline", |b| {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from(b"mykey" as &[u8]);
        shard.set(key.clone(), VortexValue::from_bytes(b"hello"));

        b.iter(|| {
            // Reset value to inline before each append (keeps it short).
            shard.set(key.clone(), VortexValue::from_bytes(b"hello"));
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&mut shard), b"APPEND", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_throughput_get_set_mix(c: &mut Criterion) {
    // 50/50 GET/SET mix over 1M-key shard, measuring single-core throughput.
    let n_keys = 1_000_000usize;
    let n_tapes = 1000usize;
    let batch = 100_000usize;

    let set_cmds: Vec<Vec<u8>> = (0..n_tapes)
        .map(|i| make_resp(&[b"SET", format!("k:{i:08}").as_bytes(), b"val"]))
        .collect();
    let get_cmds: Vec<Vec<u8>> = (0..n_tapes)
        .map(|i| make_resp(&[b"GET", format!("k:{i:08}").as_bytes()]))
        .collect();
    let set_tapes: Vec<RespTape> = set_cmds
        .iter()
        .map(|c| RespTape::parse_pipeline(c).unwrap())
        .collect();
    let get_tapes: Vec<RespTape> = get_cmds
        .iter()
        .map(|c| RespTape::parse_pipeline(c).unwrap())
        .collect();

    // Pre-fill shard once (reused across iterations).
    let mut shard = Shard::new(ShardId::new(0));
    for i in 0..n_keys {
        let key = VortexKey::from(format!("k:{i:08}").as_bytes());
        shard.set(key, VortexValue::from_bytes(b"val"));
    }

    c.bench_function("throughput_get_set_mix", |b| {
        b.iter(|| {
            for i in 0..batch {
                let idx = i % n_tapes;
                if i & 1 == 0 {
                    let frame = set_tapes[idx].iter().next().unwrap();
                    let r = execute_command(&mut shard, b"SET", &frame, 0);
                    black_box(r);
                } else {
                    let frame = get_tapes[idx].iter().next().unwrap();
                    let r = execute_command(&mut shard, b"GET", &frame, 0);
                    black_box(r);
                }
            }
        });
    });
}

criterion_group!(
    benches,
    bench_swiss_table_insert,
    bench_swiss_table_insert_single,
    bench_swiss_table_lookup_hit,
    bench_swiss_table_lookup_miss,
    bench_swiss_table_delete,
    bench_swiss_table_delete_single,
    bench_entry_write_inline,
    bench_entry_read_inline,
    bench_entry_matches_key,
    bench_entry_is_expired,
    bench_entry_write_integer,
    bench_expiry_register,
    bench_expiry_tick_20,
    bench_lazy_expiry_overhead,
    bench_active_sweep_100k,
    bench_cmd_get_inline,
    bench_cmd_set_inline,
    bench_cmd_get_miss,
    bench_cmd_incr,
    bench_cmd_mget_100,
    bench_cmd_mset_100,
    bench_cmd_append_inline,
    bench_throughput_get_set_mix,
);
criterion_main!(benches);
