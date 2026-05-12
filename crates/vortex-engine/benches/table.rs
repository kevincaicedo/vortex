use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use vortex_common::{MAX_INLINE_KEY_LEN, MAX_INLINE_VALUE_LEN, VortexKey, VortexValue};
use vortex_engine::{ConcurrentKeyspace, Entry, SwissTable};

const TABLE_MEMORY_REQUESTED_CAPACITY: usize = 16_384;
const KEYSPACE_MEMORY_TOTAL_CAPACITY: usize = 65_536;
const INLINE_LAYOUT_TARGET_MULTIPLIER: f64 = 0.75;
const LOAD_FACTORS_BPS: &[usize] = &[250, 500, 750, 875];
const KEYSPACE_SHARD_COUNTS: &[usize] = &[64, 128, 256, 512];

static TABLE_MEMORY_ARTIFACTS_WRITTEN: OnceLock<()> = OnceLock::new();

#[derive(Clone, Copy)]
struct DatasetSpec {
    scenario: &'static str,
    key_layout: &'static str,
    value_layout: &'static str,
    target_multiplier: Option<f64>,
    make_key: fn(usize) -> VortexKey,
    make_value: fn(usize) -> VortexValue,
}

struct TableMemoryRecord {
    kind: &'static str,
    scenario: &'static str,
    key_layout: &'static str,
    value_layout: &'static str,
    requested_capacity: usize,
    shard_count: usize,
    target_load_factor: Option<f64>,
    actual_load_factor: f64,
    total_slots: usize,
    live_keys: usize,
    deleted_keys: usize,
    allocated_bytes: usize,
    logical_bytes: usize,
    bytes_per_live_key: Option<f64>,
    target_bytes_per_live_key: Option<f64>,
    notes: &'static str,
}

#[derive(Clone, Copy)]
struct TableRecordSpec {
    scenario: &'static str,
    key_layout: &'static str,
    value_layout: &'static str,
    requested_capacity: usize,
    shard_count: usize,
    target_load_factor: Option<f64>,
    deleted_keys: usize,
    target_multiplier: Option<f64>,
    notes: &'static str,
}

struct TableMemoryReport {
    generated_at_unix_seconds: u64,
    records: Vec<TableMemoryRecord>,
}

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

// rust HashMap lookup hit is very fast, so we want to see how SwissTable compares.
fn bench_hashmap_lookup_hit(c: &mut Criterion) {
    use std::collections::HashMap;

    let mut group = c.benchmark_group("hashmap_lookup_hit");
    for &size in &[100, 10_000, 1_000_000] {
        let mut map = HashMap::with_capacity(size);
        for i in 0..size {
            let key = VortexKey::from(format!("key:{i:08}").as_str());
            map.insert(key, VortexValue::Integer(i as i64));
        }
        let key = VortexKey::from(format!("key:{:08}", size / 2).as_str());
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| map.get(&key));
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

// rust HashMap lookup miss is very fast, so we want to see how SwissTable compares.
fn bench_hashmap_lookup_miss(c: &mut Criterion) {
    use std::collections::HashMap;

    let mut group = c.benchmark_group("hashmap_lookup_miss");
    for &size in &[100, 10_000, 1_000_000] {
        let mut map = HashMap::with_capacity(size);
        for i in 0..size {
            let key = VortexKey::from(format!("key:{i:08}").as_str());
            map.insert(key, VortexValue::Integer(i as i64));
        }
        let key = VortexKey::from("nonexistent_key");
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| map.get(&key));
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

/// Benchmark single-key replacement into a pre-filled table with pre-generated keys.
/// This isolates the existing-key SET path from table construction and key allocation.
fn bench_swiss_table_replace_existing_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("swiss_table_replace_existing_single");
    for &size in &[100, 10_000, 1_000_000] {
        let keys: Vec<VortexKey> = (0..size)
            .map(|i| VortexKey::from(format!("key:{i:08}").as_str()))
            .collect();
        let values: Vec<VortexValue> = (0..size)
            .map(|i| VortexValue::Integer(-(i as i64) - 1))
            .collect();
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || (prefill_table(size), keys.clone(), values.clone()),
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

/// Compare with rust HashMap insert single
fn bench_hashmap_insert_single(c: &mut Criterion) {
    use std::collections::HashMap;

    let mut group = c.benchmark_group("hashmap_insert_single");
    for &size in &[100, 10_000, 1_000_000] {
        let keys: Vec<VortexKey> = (0..size)
            .map(|i| VortexKey::from(format!("key:{i:08}").as_str()))
            .collect();
        let values: Vec<VortexValue> = (0..size).map(|i| VortexValue::Integer(i as i64)).collect();
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || (HashMap::with_capacity(size), keys.clone(), values.clone()),
                |(mut map, keys, values)| {
                    for (k, v) in keys.into_iter().zip(values) {
                        map.insert(k, v);
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
                entry.write_inline_string(
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
    entry.write_inline_string(0x91, b"0123456789", b"abcdefghij", 42);

    c.bench_function("entry_read_inline", |b| {
        b.iter(|| black_box(entry.read_key()));
    });
}

fn bench_entry_matches_key(c: &mut Criterion) {
    let mut entry = Entry::empty();
    let key = b"0123456789";
    entry.write_inline_string(0x91, key, b"abcdefghij", 42);

    c.bench_function("entry_matches_key", |b| {
        b.iter(|| black_box(entry.matches_key(black_box(key))));
    });
}

fn bench_entry_is_expired(c: &mut Criterion) {
    let mut entry = Entry::empty();
    entry.write_inline_string(0x91, b"0123456789", b"abcdefghij", 1_000);

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
                entry.write_inline_integer(
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
    entry.write_inline_integer(0x91, b"counter\x00\x00\x00", 42_i64, 0);

    c.bench_function("entry_read_integer", |b| {
        b.iter(|| black_box(entry.read_value()));
    });
}

// ── 3.10.5 — Memory Efficiency ─────────────────────────────────────

fn dataset_specs() -> [DatasetSpec; 4] {
    [
        DatasetSpec {
            scenario: "inline-key-integer",
            key_layout: "inline",
            value_layout: "integer",
            target_multiplier: Some(INLINE_LAYOUT_TARGET_MULTIPLIER),
            make_key: make_inline_key,
            make_value: make_integer_value,
        },
        DatasetSpec {
            scenario: "inline-key-inline-string",
            key_layout: "inline",
            value_layout: "inline-string",
            target_multiplier: Some(INLINE_LAYOUT_TARGET_MULTIPLIER),
            make_key: make_inline_key,
            make_value: make_inline_string_value,
        },
        DatasetSpec {
            scenario: "inline-key-heap-string",
            key_layout: "inline",
            value_layout: "heap-string",
            target_multiplier: None,
            make_key: make_inline_key,
            make_value: make_heap_string_value,
        },
        DatasetSpec {
            scenario: "heap-key-heap-string",
            key_layout: "heap",
            value_layout: "heap-string",
            target_multiplier: None,
            make_key: make_heap_key,
            make_value: make_heap_string_value,
        },
    ]
}

fn bench_table_memory_report_generation(c: &mut Criterion) {
    ensure_table_memory_artifacts();

    let mut group = c.benchmark_group("table_memory_report_generation");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("all_scenarios", |b| {
        b.iter(|| {
            let report = collect_table_memory_report();
            black_box(report.records.len())
        });
    });
    group.finish();
}

fn ensure_table_memory_artifacts() {
    TABLE_MEMORY_ARTIFACTS_WRITTEN.get_or_init(|| {
        let report = collect_table_memory_report();
        write_table_memory_artifacts(&report).expect("table memory sidecar artifact generation");
    });
}

fn collect_table_memory_report() -> TableMemoryReport {
    let mut records = Vec::new();

    records.push(measure_empty_pre_sized_table());

    for spec in dataset_specs() {
        for &load_factor_bps in LOAD_FACTORS_BPS {
            records.push(measure_table_load_factor(spec, load_factor_bps));
        }
    }

    records.extend(measure_tombstone_heavy_table());

    for &shard_count in KEYSPACE_SHARD_COUNTS {
        records.push(measure_pre_sized_keyspace(shard_count));
    }

    TableMemoryReport {
        generated_at_unix_seconds: unix_timestamp_now(),
        records,
    }
}

fn measure_empty_pre_sized_table() -> TableMemoryRecord {
    let table = SwissTable::with_capacity(TABLE_MEMORY_REQUESTED_CAPACITY);
    let total_slots = table.total_slots();

    TableMemoryRecord {
        kind: "table",
        scenario: "empty-pre-sized",
        key_layout: "none",
        value_layout: "none",
        requested_capacity: TABLE_MEMORY_REQUESTED_CAPACITY,
        shard_count: 1,
        target_load_factor: None,
        actual_load_factor: 0.0,
        total_slots,
        live_keys: 0,
        deleted_keys: 0,
        allocated_bytes: table.allocated_bytes(),
        logical_bytes: table.memory_used(),
        bytes_per_live_key: None,
        target_bytes_per_live_key: None,
        notes: "allocation-baseline",
    }
}

fn measure_table_load_factor(spec: DatasetSpec, load_factor_bps: usize) -> TableMemoryRecord {
    let mut table = SwissTable::with_capacity(TABLE_MEMORY_REQUESTED_CAPACITY);
    let total_slots = table.total_slots();
    let live_keys = total_slots * load_factor_bps / 1000;

    for index in 0..live_keys {
        table.insert((spec.make_key)(index), (spec.make_value)(index));
    }

    build_table_record(
        TableRecordSpec {
            scenario: spec.scenario,
            key_layout: spec.key_layout,
            value_layout: spec.value_layout,
            requested_capacity: TABLE_MEMORY_REQUESTED_CAPACITY,
            shard_count: 1,
            target_load_factor: Some(load_factor_bps as f64 / 1000.0),
            deleted_keys: 0,
            target_multiplier: spec.target_multiplier,
            notes: if spec.target_multiplier.is_some() {
                "inline-baseline-25pct-target"
            } else {
                "layout-baseline"
            },
        },
        &table,
    )
}

fn measure_tombstone_heavy_table() -> [TableMemoryRecord; 2] {
    let mut table = SwissTable::with_capacity(TABLE_MEMORY_REQUESTED_CAPACITY);
    let seed_keys = table.total_slots() * 3 / 4;
    let mut keys = Vec::with_capacity(seed_keys);

    for index in 0..seed_keys {
        let key = make_inline_key(index);
        table.insert(key.clone(), make_inline_string_value(index));
        keys.push(key);
    }

    let mut deleted_keys = 0usize;
    for key in keys.iter().step_by(2) {
        if table.remove(key).is_some() {
            deleted_keys += 1;
        }
    }

    let before_resize = build_table_record(
        TableRecordSpec {
            scenario: "tombstone-heavy-before-resize",
            key_layout: "inline",
            value_layout: "inline-string",
            requested_capacity: TABLE_MEMORY_REQUESTED_CAPACITY,
            shard_count: 1,
            target_load_factor: None,
            deleted_keys,
            target_multiplier: None,
            notes: "delete-heavy-before-resize",
        },
        &table,
    );

    let slots_before_resize = table.total_slots();
    let mut next_index = seed_keys + 1_000_000;
    while table.total_slots() == slots_before_resize {
        table.insert(
            make_inline_key(next_index),
            make_inline_string_value(next_index),
        );
        next_index += 1;
    }

    let after_resize = build_table_record(
        TableRecordSpec {
            scenario: "tombstone-heavy-after-resize",
            key_layout: "inline",
            value_layout: "inline-string",
            requested_capacity: TABLE_MEMORY_REQUESTED_CAPACITY,
            shard_count: 1,
            target_load_factor: None,
            deleted_keys: 0,
            target_multiplier: None,
            notes: "resize-clears-tombstones",
        },
        &table,
    );

    [before_resize, after_resize]
}

fn measure_pre_sized_keyspace(shard_count: usize) -> TableMemoryRecord {
    let keyspace = ConcurrentKeyspace::with_capacity(shard_count, KEYSPACE_MEMORY_TOTAL_CAPACITY);
    let mut total_slots = 0usize;
    let mut allocated_bytes = 0usize;
    let mut logical_bytes = 0usize;

    for (slots, allocated, logical) in keyspace.scan_all_shards(|_, table| {
        (
            table.total_slots(),
            table.allocated_bytes(),
            table.memory_used(),
        )
    }) {
        total_slots += slots;
        allocated_bytes += allocated;
        logical_bytes += logical;
    }

    TableMemoryRecord {
        kind: "keyspace",
        scenario: "pre-sized-shards",
        key_layout: "none",
        value_layout: "none",
        requested_capacity: KEYSPACE_MEMORY_TOTAL_CAPACITY,
        shard_count,
        target_load_factor: None,
        actual_load_factor: 0.0,
        total_slots,
        live_keys: 0,
        deleted_keys: 0,
        allocated_bytes,
        logical_bytes,
        bytes_per_live_key: None,
        target_bytes_per_live_key: None,
        notes: "empty-keyspace-shard-overhead",
    }
}

fn build_table_record(spec: TableRecordSpec, table: &SwissTable) -> TableMemoryRecord {
    let live_keys = table.len();
    let bytes_per_live_key = bytes_per_live_key(table.allocated_bytes(), live_keys);

    TableMemoryRecord {
        kind: "table",
        scenario: spec.scenario,
        key_layout: spec.key_layout,
        value_layout: spec.value_layout,
        requested_capacity: spec.requested_capacity,
        shard_count: spec.shard_count,
        target_load_factor: spec.target_load_factor,
        actual_load_factor: actual_load_factor(live_keys, table.total_slots()),
        total_slots: table.total_slots(),
        live_keys,
        deleted_keys: spec.deleted_keys,
        allocated_bytes: table.allocated_bytes(),
        logical_bytes: table.memory_used(),
        bytes_per_live_key,
        target_bytes_per_live_key: bytes_per_live_key
            .zip(spec.target_multiplier)
            .map(|(value, multiplier)| value * multiplier),
        notes: spec.notes,
    }
}

fn bytes_per_live_key(allocated_bytes: usize, live_keys: usize) -> Option<f64> {
    if live_keys == 0 {
        None
    } else {
        Some(allocated_bytes as f64 / live_keys as f64)
    }
}

fn actual_load_factor(live_keys: usize, total_slots: usize) -> f64 {
    if total_slots == 0 {
        0.0
    } else {
        live_keys as f64 / total_slots as f64
    }
}

fn make_inline_key(index: usize) -> VortexKey {
    VortexKey::from(format!("k:{index:08}").as_bytes())
}

fn make_heap_key(index: usize) -> VortexKey {
    let mut bytes = format!("heap-key:{index:08}:").into_bytes();
    bytes.resize(MAX_INLINE_KEY_LEN + 8, b'k');
    VortexKey::from(bytes.as_slice())
}

fn make_integer_value(index: usize) -> VortexValue {
    VortexValue::Integer(index as i64)
}

fn make_inline_string_value(index: usize) -> VortexValue {
    let mut bytes = format!("v:{index:08}").into_bytes();
    bytes.truncate(MAX_INLINE_VALUE_LEN);
    VortexValue::from_bytes(bytes.as_slice())
}

fn make_heap_string_value(index: usize) -> VortexValue {
    let mut bytes = format!("value:{index:08}:").into_bytes();
    bytes.resize(MAX_INLINE_VALUE_LEN + 24, b'v');
    VortexValue::from_bytes(bytes.as_slice())
}

fn write_table_memory_artifacts(report: &TableMemoryReport) -> std::io::Result<()> {
    let artifact_dir = table_memory_artifact_dir();
    fs::create_dir_all(&artifact_dir)?;
    fs::write(
        artifact_dir.join("table-memory-report.json"),
        render_table_memory_report_json(report),
    )?;
    fs::write(
        artifact_dir.join("table-memory-report.csv"),
        render_table_memory_report_csv(report),
    )?;
    Ok(())
}

fn table_memory_artifact_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/benchmarks/table-memory/latest")
}

fn unix_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn render_table_memory_report_json(report: &TableMemoryReport) -> String {
    let mut json = String::new();
    let _ = writeln!(&mut json, "{{");
    let _ = writeln!(
        &mut json,
        "  \"generated_at_unix_seconds\": {},",
        report.generated_at_unix_seconds
    );
    let _ = writeln!(&mut json, "  \"records\": [");

    for (index, record) in report.records.iter().enumerate() {
        let trailing = if index + 1 == report.records.len() {
            ""
        } else {
            ","
        };
        let _ = writeln!(&mut json, "    {{");
        let _ = writeln!(&mut json, "      \"kind\": \"{}\",", record.kind);
        let _ = writeln!(&mut json, "      \"scenario\": \"{}\",", record.scenario);
        let _ = writeln!(
            &mut json,
            "      \"key_layout\": \"{}\",",
            record.key_layout
        );
        let _ = writeln!(
            &mut json,
            "      \"value_layout\": \"{}\",",
            record.value_layout
        );
        let _ = writeln!(
            &mut json,
            "      \"requested_capacity\": {},",
            record.requested_capacity
        );
        let _ = writeln!(&mut json, "      \"shard_count\": {},", record.shard_count);
        write_json_option_f64(
            &mut json,
            "target_load_factor",
            record.target_load_factor,
            true,
            6,
        );
        let _ = writeln!(
            &mut json,
            "      \"actual_load_factor\": {:.6},",
            record.actual_load_factor
        );
        let _ = writeln!(&mut json, "      \"total_slots\": {},", record.total_slots);
        let _ = writeln!(&mut json, "      \"live_keys\": {},", record.live_keys);
        let _ = writeln!(
            &mut json,
            "      \"deleted_keys\": {},",
            record.deleted_keys
        );
        let _ = writeln!(
            &mut json,
            "      \"allocated_bytes\": {},",
            record.allocated_bytes
        );
        let _ = writeln!(
            &mut json,
            "      \"logical_bytes\": {},",
            record.logical_bytes
        );
        write_json_option_f64(
            &mut json,
            "bytes_per_live_key",
            record.bytes_per_live_key,
            true,
            4,
        );
        write_json_option_f64(
            &mut json,
            "target_bytes_per_live_key",
            record.target_bytes_per_live_key,
            true,
            4,
        );
        let _ = writeln!(
            &mut json,
            "      \"notes\": \"{}\"",
            escape_json(record.notes)
        );
        let _ = writeln!(&mut json, "    }}{trailing}");
    }

    let _ = writeln!(&mut json, "  ]");
    let _ = writeln!(&mut json, "}}");
    json
}

fn render_table_memory_report_csv(report: &TableMemoryReport) -> String {
    let mut csv = String::from(
        "kind,scenario,key_layout,value_layout,requested_capacity,shard_count,target_load_factor,actual_load_factor,total_slots,live_keys,deleted_keys,allocated_bytes,logical_bytes,bytes_per_live_key,target_bytes_per_live_key,notes\n",
    );

    for record in &report.records {
        let _ = writeln!(
            &mut csv,
            "{},{},{},{},{},{},{},{:.6},{},{},{},{},{},{},{},{}",
            record.kind,
            record.scenario,
            record.key_layout,
            record.value_layout,
            record.requested_capacity,
            record.shard_count,
            csv_option_f64(record.target_load_factor, 6),
            record.actual_load_factor,
            record.total_slots,
            record.live_keys,
            record.deleted_keys,
            record.allocated_bytes,
            record.logical_bytes,
            csv_option_f64(record.bytes_per_live_key, 4),
            csv_option_f64(record.target_bytes_per_live_key, 4),
            record.notes,
        );
    }

    csv
}

fn write_json_option_f64(
    buf: &mut String,
    name: &str,
    value: Option<f64>,
    trailing_comma: bool,
    precision: usize,
) {
    let trailing = if trailing_comma { "," } else { "" };
    match value {
        Some(value) => {
            let _ = writeln!(
                buf,
                "      \"{}\": {:.*}{}",
                name, precision, value, trailing
            );
        }
        None => {
            let _ = writeln!(buf, "      \"{}\": null{}", name, trailing);
        }
    }
}

fn csv_option_f64(value: Option<f64>, precision: usize) -> String {
    match value {
        Some(value) => format!("{value:.precision$}"),
        None => String::new(),
    }
}

fn escape_json(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

// ── 3.10.4 — Latency Distribution ─────────────────────────────────

criterion_group!(
    benches,
    bench_swiss_table_insert,
    bench_swiss_table_insert_single,
    bench_swiss_table_replace_existing_single,
    bench_hashmap_insert_single,
    bench_hashmap_lookup_miss,
    bench_hashmap_lookup_hit,
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
    bench_table_memory_report_generation,
);
criterion_main!(benches);
