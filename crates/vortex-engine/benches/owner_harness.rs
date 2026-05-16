use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use hdrhistogram::Histogram;
use vortex_common::{VortexKey, VortexValue};
use vortex_engine::ConcurrentKeyspace;
use vortex_engine::owner::{
    EngineOwnerHarness, OwnerHarnessCommand, OwnerId, ThreadedOwnerHarness, shared_keyspace_get,
    shared_keyspace_set,
};

const OWNER_COUNT: usize = 8;
const CAPSULE_COUNT: usize = 1024;
const DATASET_KEYS: usize = 16_384;
const OWNER_CAPACITY: usize = DATASET_KEYS / OWNER_COUNT * 2;
const SHARED_KEYSPACE_SHARDS: usize = 64;
const REMOTE_RING_SLOTS: usize = 1024;
const QUEUE_WAIT_SAMPLES: usize = 50_000;
const ARTIFACT_RELATIVE_ROOT: &str =
    ".artifacts/benchmarks/shared-nothing-sn003-owner-harness-20260514";

static QUEUE_WAIT_ARTIFACT: OnceLock<()> = OnceLock::new();

fn make_key(index: usize) -> VortexKey {
    VortexKey::from(format!("sn003-key:{index:08}"))
}

fn make_value(index: usize) -> VortexValue {
    VortexValue::Integer(index as i64)
}

fn keys() -> Vec<VortexKey> {
    (0..DATASET_KEYS).map(make_key).collect()
}

fn owner_harness(keys: &[VortexKey]) -> EngineOwnerHarness {
    let mut harness =
        EngineOwnerHarness::new(OWNER_COUNT, CAPSULE_COUNT, OWNER_CAPACITY).expect("valid harness");
    for (index, key) in keys.iter().enumerate() {
        harness.insert_routed(key.clone(), make_value(index));
    }
    harness
}

fn shared_keyspace(keys: &[VortexKey]) -> ConcurrentKeyspace {
    let keyspace = ConcurrentKeyspace::with_capacity(SHARED_KEYSPACE_SHARDS, DATASET_KEYS);
    for (index, key) in keys.iter().enumerate() {
        shared_keyspace_set(&keyspace, key.clone(), make_value(index));
    }
    keyspace
}

fn routed_keys(harness: &EngineOwnerHarness, keys: &[VortexKey]) -> Vec<(OwnerId, VortexKey)> {
    keys.iter()
        .map(|key| (harness.route_owner(key), key.clone()))
        .collect()
}

fn keys_for_owner(
    harness: &EngineOwnerHarness,
    keys: &[VortexKey],
    owner: OwnerId,
    count: usize,
) -> Vec<VortexKey> {
    keys.iter()
        .filter(|key| harness.route_owner(key) == owner)
        .take(count)
        .cloned()
        .collect()
}

fn keys_not_owner(
    harness: &EngineOwnerHarness,
    keys: &[VortexKey],
    owner: OwnerId,
    count: usize,
) -> Vec<VortexKey> {
    keys.iter()
        .filter(|key| harness.route_owner(key) != owner)
        .take(count)
        .cloned()
        .collect()
}

fn bench_shared_keyspace_get(c: &mut Criterion) {
    let keys = keys();
    let keyspace = shared_keyspace(&keys);
    let mut index = 0usize;

    c.bench_function("owner_harness_shared_get_hit_8", |b| {
        b.iter(|| {
            let key = &keys[index & (DATASET_KEYS - 1)];
            let result = shared_keyspace_get(black_box(&keyspace), black_box(key));
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_owner_local_get(c: &mut Criterion) {
    let keys = keys();
    let harness = owner_harness(&keys);
    let routed = routed_keys(&harness, &keys);
    let mut index = 0usize;

    c.bench_function("owner_harness_local_get_hit_8", |b| {
        b.iter(|| {
            let (owner, key) = &routed[index & (DATASET_KEYS - 1)];
            let result = harness.get_local(*owner, black_box(key));
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_shared_keyspace_set(c: &mut Criterion) {
    let keys = keys();
    let keyspace = shared_keyspace(&keys);
    let mut index = 0usize;

    c.bench_function("owner_harness_shared_set_replace_8", |b| {
        b.iter(|| {
            let key = keys[index & (DATASET_KEYS - 1)].clone();
            let result =
                shared_keyspace_set(black_box(&keyspace), black_box(key), make_value(index));
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_owner_local_set(c: &mut Criterion) {
    let keys = keys();
    let mut harness = owner_harness(&keys);
    let routed = routed_keys(&harness, &keys);
    let mut index = 0usize;

    c.bench_function("owner_harness_local_set_replace_8", |b| {
        b.iter(|| {
            let (owner, key) = &routed[index & (DATASET_KEYS - 1)];
            let result = harness.set_local(*owner, black_box(key.clone()), make_value(index));
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_owner_remote_get(c: &mut Criterion) {
    write_queue_wait_artifact();

    let keys = keys();
    let harness = ThreadedOwnerHarness::<REMOTE_RING_SLOTS>::start(owner_harness(&keys));
    let mut index = 0usize;

    c.bench_function("owner_harness_remote_get_hit_8", |b| {
        b.iter(|| {
            let key = keys[index & (DATASET_KEYS - 1)].clone();
            let result = harness.execute(OwnerHarnessCommand::get(black_box(key)));
            black_box(result.reply);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_owner_remote_set(c: &mut Criterion) {
    let keys = keys();
    let harness = ThreadedOwnerHarness::<REMOTE_RING_SLOTS>::start(owner_harness(&keys));
    let mut index = 0usize;

    c.bench_function("owner_harness_remote_set_replace_8", |b| {
        b.iter(|| {
            let key = keys[index & (DATASET_KEYS - 1)].clone();
            let result =
                harness.execute(OwnerHarnessCommand::set(black_box(key), make_value(index)));
            black_box(result.reply);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_owner_mixed_get(c: &mut Criterion) {
    let keys = keys();
    let local_harness = owner_harness(&keys);
    let remote_harness = ThreadedOwnerHarness::<REMOTE_RING_SLOTS>::start(owner_harness(&keys));
    let owner_zero = local_harness.owner_id(0).expect("owner zero exists");
    let local_keys = keys_for_owner(
        &local_harness,
        &keys,
        owner_zero,
        DATASET_KEYS / OWNER_COUNT,
    );
    let remote_keys = keys_not_owner(
        &local_harness,
        &keys,
        owner_zero,
        DATASET_KEYS / OWNER_COUNT,
    );
    let mut index = 0usize;

    c.bench_function("owner_harness_mixed_get_hit_8", |b| {
        b.iter(|| {
            if index & 1 == 0 {
                let key = &local_keys[(index / 2) % local_keys.len()];
                let result = local_harness.get_local(owner_zero, black_box(key));
                black_box(result);
            } else {
                let key = remote_keys[(index / 2) % remote_keys.len()].clone();
                let result = remote_harness.execute(OwnerHarnessCommand::get(black_box(key)));
                black_box(result.reply);
            }
            index = index.wrapping_add(1);
        });
    });
}

fn bench_owner_mixed_set(c: &mut Criterion) {
    let keys = keys();
    let mut local_harness = owner_harness(&keys);
    let remote_harness = ThreadedOwnerHarness::<REMOTE_RING_SLOTS>::start(owner_harness(&keys));
    let owner_zero = local_harness.owner_id(0).expect("owner zero exists");
    let local_keys = keys_for_owner(
        &local_harness,
        &keys,
        owner_zero,
        DATASET_KEYS / OWNER_COUNT,
    );
    let remote_keys = keys_not_owner(
        &local_harness,
        &keys,
        owner_zero,
        DATASET_KEYS / OWNER_COUNT,
    );
    let mut index = 0usize;

    c.bench_function("owner_harness_mixed_set_replace_8", |b| {
        b.iter(|| {
            if index & 1 == 0 {
                let key = local_keys[(index / 2) % local_keys.len()].clone();
                let result = local_harness.set_local(owner_zero, black_box(key), make_value(index));
                black_box(result);
            } else {
                let key = remote_keys[(index / 2) % remote_keys.len()].clone();
                let result = remote_harness
                    .execute(OwnerHarnessCommand::set(black_box(key), make_value(index)));
                black_box(result.reply);
            }
            index = index.wrapping_add(1);
        });
    });
}

fn write_queue_wait_artifact() {
    QUEUE_WAIT_ARTIFACT.get_or_init(|| {
        let keys = keys();
        let harness = ThreadedOwnerHarness::<REMOTE_RING_SLOTS>::start(owner_harness(&keys));
        let mut get_hist = Histogram::<u64>::new(3).expect("histogram builds");
        let mut set_hist = Histogram::<u64>::new(3).expect("histogram builds");

        for index in 0..QUEUE_WAIT_SAMPLES {
            let key = keys[index & (DATASET_KEYS - 1)].clone();
            let result = harness.execute_timed(OwnerHarnessCommand::get(key));
            let nanos = result.queue_wait.as_nanos().min(u64::MAX as u128) as u64;
            get_hist.record(nanos).expect("queue wait recorded");
        }

        for index in 0..QUEUE_WAIT_SAMPLES {
            let key = keys[index & (DATASET_KEYS - 1)].clone();
            let result = harness.execute_timed(OwnerHarnessCommand::set(key, make_value(index)));
            let nanos = result.queue_wait.as_nanos().min(u64::MAX as u128) as u64;
            set_hist.record(nanos).expect("queue wait recorded");
        }

        let root = workspace_artifact_root();
        fs::create_dir_all(&root).expect("artifact root created");
        let mut report = String::new();
        writeln!(
            report,
            "workload,samples,p50_ns,p95_ns,p99_ns,p999_ns,max_ns"
        )
        .unwrap();
        write_histogram_row(&mut report, "remote_get", &get_hist);
        write_histogram_row(&mut report, "remote_set", &set_hist);
        fs::write(root.join("queue-wait.csv"), report).expect("queue wait artifact written");
    });
}

fn workspace_artifact_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .join(ARTIFACT_RELATIVE_ROOT)
}

fn write_histogram_row(report: &mut String, workload: &str, histogram: &Histogram<u64>) {
    writeln!(
        report,
        "{},{},{},{},{},{},{}",
        workload,
        histogram.len(),
        histogram.value_at_quantile(0.50),
        histogram.value_at_quantile(0.95),
        histogram.value_at_quantile(0.99),
        histogram.value_at_quantile(0.999),
        histogram.max()
    )
    .unwrap();
}

criterion_group!(
    benches,
    bench_shared_keyspace_get,
    bench_owner_local_get,
    bench_shared_keyspace_set,
    bench_owner_local_set,
    bench_owner_remote_get,
    bench_owner_remote_set,
    bench_owner_mixed_get,
    bench_owner_mixed_set
);
criterion_main!(benches);
