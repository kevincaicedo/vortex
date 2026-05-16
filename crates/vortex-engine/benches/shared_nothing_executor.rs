use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use hdrhistogram::Histogram;
use vortex_common::{VortexKey, VortexValue};
use vortex_engine::commands::{CommandClock, execute_command};
use vortex_engine::owner::{OwnerId, OwnerTopology, SharedNothingExecutionResult, TopologyConfig};
use vortex_engine::{ConcurrentKeyspace, SharedNothingExecutor};
use vortex_proto::RespTape;

const OWNER_COUNT: usize = 8;
const CAPSULE_COUNT: usize = 1024;
const DATASET_KEYS: usize = 16_384;
const OWNER_CAPACITY: usize = DATASET_KEYS / OWNER_COUNT * 2;
const COMMAND_KEY_COUNT: usize = 1024;
const SHARED_KEYSPACE_SHARDS: usize = 64;
const REMOTE_RING_SLOTS: usize = 1024;
const QUEUE_WAIT_SAMPLES: usize = 50_000;
const NOW_NANOS: u64 = 10_000_000;
const ARTIFACT_RELATIVE_ROOT: &str = ".artifacts/benchmarks/shared-nothing-sn004-executor-20260514";

static QUEUE_WAIT_ARTIFACT: OnceLock<()> = OnceLock::new();

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

fn tape(parts: &[&[u8]]) -> RespTape {
    RespTape::parse_pipeline(&make_resp(parts)).expect("valid RESP")
}

fn topology() -> OwnerTopology {
    OwnerTopology::new(TopologyConfig::new(OWNER_COUNT, CAPSULE_COUNT).expect("valid topology"))
}

fn owner_zero() -> OwnerId {
    topology().owner_id(0).expect("owner zero exists")
}

fn dataset_keys() -> Vec<Vec<u8>> {
    (0..DATASET_KEYS)
        .map(|index| format!("sn004-key:{index:08}").into_bytes())
        .collect()
}

fn keys_matching(
    keys: &[Vec<u8>],
    predicate: impl Fn(OwnerId) -> bool,
    limit: usize,
) -> Vec<Vec<u8>> {
    let topology = topology();
    keys.iter()
        .filter(|key| predicate(topology.route_key(key).owner()))
        .take(limit)
        .cloned()
        .collect()
}

fn shared_keyspace(keys: &[Vec<u8>]) -> ConcurrentKeyspace {
    let keyspace = ConcurrentKeyspace::with_capacity(SHARED_KEYSPACE_SHARDS, DATASET_KEYS);
    for (index, key) in keys.iter().enumerate() {
        unsafe {
            keyspace.benchmark_insert_unchecked(
                VortexKey::from_bytes(key),
                VortexValue::Integer(index as i64),
            );
        }
    }
    keyspace
}

fn shared_nothing_executor(keys: &[Vec<u8>]) -> SharedNothingExecutor<REMOTE_RING_SLOTS> {
    let mut executor = SharedNothingExecutor::start(OWNER_COUNT, CAPSULE_COUNT, OWNER_CAPACITY, 0)
        .expect("valid executor");
    for (index, key) in keys.iter().enumerate() {
        executor.insert_routed(
            VortexKey::from_bytes(key),
            VortexValue::Integer(index as i64),
        );
    }
    executor
}

fn get_tapes(keys: &[Vec<u8>]) -> Vec<RespTape> {
    keys.iter().map(|key| tape(&[b"GET", key])).collect()
}

fn set_tapes(keys: &[Vec<u8>]) -> Vec<RespTape> {
    keys.iter()
        .map(|key| tape(&[b"SET", key, b"value2"]))
        .collect()
}

fn bench_shared_get(c: &mut Criterion) {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner == local_owner, COMMAND_KEY_COUNT);
    let keyspace = shared_keyspace(&keys);
    let tapes = get_tapes(&command_keys);
    let mut index = 0usize;

    c.bench_function("sn004_shared_keyspace_get_hit", |b| {
        b.iter(|| {
            let frame = tapes[index & (tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            let result = execute_command(
                black_box(&keyspace),
                b"GET",
                &frame,
                CommandClock::new(NOW_NANOS, NOW_NANOS),
            );
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_local_get(c: &mut Criterion) {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner == local_owner, COMMAND_KEY_COUNT);
    let mut executor = shared_nothing_executor(&keys);
    let tapes = get_tapes(&command_keys);
    let mut index = 0usize;

    c.bench_function("sn004_shared_nothing_local_get_hit_8", |b| {
        b.iter(|| {
            let frame = tapes[index & (tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            let result = executor.execute(b"GET", &frame, CommandClock::new(NOW_NANOS, NOW_NANOS));
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_remote_get(c: &mut Criterion) {
    write_queue_wait_artifact();

    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner != local_owner, COMMAND_KEY_COUNT);
    let mut executor = shared_nothing_executor(&keys);
    let tapes = get_tapes(&command_keys);
    let mut index = 0usize;

    c.bench_function("sn004_shared_nothing_remote_get_hit_8", |b| {
        b.iter(|| {
            let frame = tapes[index & (tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            let result = executor.execute(b"GET", &frame, CommandClock::new(NOW_NANOS, NOW_NANOS));
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_shared_set(c: &mut Criterion) {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner == local_owner, COMMAND_KEY_COUNT);
    let keyspace = shared_keyspace(&keys);
    let tapes = set_tapes(&command_keys);
    let mut index = 0usize;

    c.bench_function("sn004_shared_keyspace_set_replace", |b| {
        b.iter(|| {
            let frame = tapes[index & (tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            let result = execute_command(
                black_box(&keyspace),
                b"SET",
                &frame,
                CommandClock::new(NOW_NANOS, NOW_NANOS),
            );
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_local_set(c: &mut Criterion) {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner == local_owner, COMMAND_KEY_COUNT);
    let mut executor = shared_nothing_executor(&keys);
    let tapes = set_tapes(&command_keys);
    let mut index = 0usize;

    c.bench_function("sn004_shared_nothing_local_set_replace_8", |b| {
        b.iter(|| {
            let frame = tapes[index & (tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            let result = executor.execute(b"SET", &frame, CommandClock::new(NOW_NANOS, NOW_NANOS));
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_remote_set(c: &mut Criterion) {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner != local_owner, COMMAND_KEY_COUNT);
    let mut executor = shared_nothing_executor(&keys);
    let tapes = set_tapes(&command_keys);
    let mut index = 0usize;

    c.bench_function("sn004_shared_nothing_remote_set_replace_8", |b| {
        b.iter(|| {
            let frame = tapes[index & (tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            let result = executor.execute(b"SET", &frame, CommandClock::new(NOW_NANOS, NOW_NANOS));
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn write_queue_wait_artifact() {
    QUEUE_WAIT_ARTIFACT.get_or_init(|| {
        let keys = dataset_keys();
        let local_owner = owner_zero();
        let command_keys = keys_matching(&keys, |owner| owner != local_owner, COMMAND_KEY_COUNT);
        let mut executor = shared_nothing_executor(&keys);
        let get_tapes = get_tapes(&command_keys);
        let set_tapes = set_tapes(&command_keys);
        let mut get_hist = Histogram::<u64>::new(3).expect("histogram builds");
        let mut set_hist = Histogram::<u64>::new(3).expect("histogram builds");

        for index in 0..QUEUE_WAIT_SAMPLES {
            let frame = get_tapes[index & (get_tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            if let SharedNothingExecutionResult::Ready { queue_wait, .. } =
                executor.execute_timed(b"GET", &frame, CommandClock::new(NOW_NANOS, NOW_NANOS))
            {
                let nanos = queue_wait.as_nanos().min(u64::MAX as u128) as u64;
                get_hist.record(nanos).expect("queue wait recorded");
            }
        }

        for index in 0..QUEUE_WAIT_SAMPLES {
            let frame = set_tapes[index & (set_tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            if let SharedNothingExecutionResult::Ready { queue_wait, .. } =
                executor.execute_timed(b"SET", &frame, CommandClock::new(NOW_NANOS, NOW_NANOS))
            {
                let nanos = queue_wait.as_nanos().min(u64::MAX as u128) as u64;
                set_hist.record(nanos).expect("queue wait recorded");
            }
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
    bench_shared_get,
    bench_local_get,
    bench_remote_get,
    bench_shared_set,
    bench_local_set,
    bench_remote_set
);
criterion_main!(benches);
