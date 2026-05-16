use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use vortex_common::{VortexKey, VortexValue};
use vortex_engine::commands::{CommandClock, execute_command};
use vortex_engine::owner::{
    OwnerId, OwnerTopology, SharedNothingConnectionGeneration, SharedNothingConnectionId,
    SharedNothingConnectionToken, SharedNothingExecutorCostProbe, TopologyConfig,
};
use vortex_engine::{ConcurrentKeyspace, SharedNothingExecutor};
use vortex_proto::RespTape;

const OWNER_COUNT: usize = 8;
const CAPSULE_COUNT: usize = 1024;
const DATASET_KEYS: usize = 16_384;
const OWNER_CAPACITY: usize = DATASET_KEYS / OWNER_COUNT * 2;
const COMMAND_KEY_COUNT: usize = 1024;
const SHARED_KEYSPACE_SHARDS: usize = 64;
const REMOTE_RING_SLOTS: usize = 1024;
const NOW_NANOS: u64 = 10_000_000;

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
        .map(|index| format!("sn004a-key:{index:08}").into_bytes())
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

fn local_command_keys() -> Vec<Vec<u8>> {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    keys_matching(&keys, |owner| owner == local_owner, COMMAND_KEY_COUNT)
}

fn get_tapes(keys: &[Vec<u8>]) -> Vec<RespTape> {
    keys.iter().map(|key| tape(&[b"GET", key])).collect()
}

fn set_tapes(keys: &[Vec<u8>]) -> Vec<RespTape> {
    keys.iter()
        .map(|key| tape(&[b"SET", key, b"value2"]))
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

fn cost_probe(keys: &[Vec<u8>]) -> SharedNothingExecutorCostProbe {
    let mut probe =
        SharedNothingExecutorCostProbe::new(OWNER_COUNT, CAPSULE_COUNT, OWNER_CAPACITY, 0)
            .expect("valid cost probe");
    for (index, key) in keys.iter().enumerate() {
        probe.insert_local(
            VortexKey::from_bytes(key),
            VortexValue::Integer(index as i64),
        );
    }
    probe
}

fn bench_route_only(c: &mut Criterion) {
    let keys = local_command_keys();
    let topology = topology();
    let mut index = 0usize;

    c.bench_function("sn004a_route_owner_local_key_8", |b| {
        b.iter(|| {
            let owner = topology
                .route_key(black_box(&keys[index & (keys.len() - 1)]))
                .owner();
            black_box(owner);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_parse_get(c: &mut Criterion) {
    let keys = local_command_keys();
    let tapes = get_tapes(&keys);
    let probe = cost_probe(&keys);
    let mut index = 0usize;

    c.bench_function("sn004a_parse_get_frame", |b| {
        b.iter(|| {
            let frame = tapes[index & (tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            let parsed = probe.parse_kind(b"GET", &frame, CommandClock::new(NOW_NANOS, NOW_NANOS));
            black_box(parsed);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_parse_set(c: &mut Criterion) {
    let keys = local_command_keys();
    let tapes = set_tapes(&keys);
    let probe = cost_probe(&keys);
    let mut index = 0usize;

    c.bench_function("sn004a_parse_set_plain_frame", |b| {
        b.iter(|| {
            let frame = tapes[index & (tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            let parsed = probe.parse_kind(b"SET", &frame, CommandClock::new(NOW_NANOS, NOW_NANOS));
            black_box(parsed);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_storage_get(c: &mut Criterion) {
    let keys = local_command_keys();
    let mut probe = cost_probe(&keys);
    let mut index = 0usize;

    c.bench_function("sn004a_owner_partition_get_storage_hit", |b| {
        b.iter(|| {
            let found = probe.storage_get_hit(
                black_box(&keys[index & (keys.len() - 1)]),
                black_box(NOW_NANOS),
            );
            black_box(found);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_storage_set(c: &mut Criterion) {
    let keys = local_command_keys();
    let mut probe = cost_probe(&keys);
    let mut index = 0usize;

    c.bench_function("sn004a_owner_partition_set_storage_replace", |b| {
        b.iter(|| {
            probe.storage_set_plain(
                black_box(&keys[index & (keys.len() - 1)]),
                black_box(b"value2"),
            );
            index = index.wrapping_add(1);
        });
    });
}

fn bench_preparsed_get(c: &mut Criterion) {
    let keys = local_command_keys();
    let mut probe = cost_probe(&keys);
    let mut index = 0usize;

    c.bench_function("sn004a_local_preparsed_get_response", |b| {
        b.iter(|| {
            let result = probe.execute_preparsed_get(
                black_box(&keys[index & (keys.len() - 1)]),
                CommandClock::new(NOW_NANOS, NOW_NANOS),
            );
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_routed_preparsed_get(c: &mut Criterion) {
    let keys = local_command_keys();
    let mut probe = cost_probe(&keys);
    let mut index = 0usize;

    c.bench_function("sn004a_local_routed_preparsed_get_response", |b| {
        b.iter(|| {
            let result = probe.execute_routed_preparsed_get(
                black_box(&keys[index & (keys.len() - 1)]),
                CommandClock::new(NOW_NANOS, NOW_NANOS),
            );
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_parsed_no_route_get(c: &mut Criterion) {
    let keys = local_command_keys();
    let tapes = get_tapes(&keys);
    let mut probe = cost_probe(&keys);
    let mut index = 0usize;

    c.bench_function("sn004a_local_parse_execute_no_route_get", |b| {
        b.iter(|| {
            let frame = tapes[index & (tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            let result = probe.execute_parsed_no_route(
                b"GET",
                &frame,
                CommandClock::new(NOW_NANOS, NOW_NANOS),
            );
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_preparsed_set(c: &mut Criterion) {
    let keys = local_command_keys();
    let mut probe = cost_probe(&keys);
    let mut index = 0usize;

    c.bench_function("sn004a_local_preparsed_set_response", |b| {
        b.iter(|| {
            let result = probe.execute_preparsed_set_plain(
                black_box(&keys[index & (keys.len() - 1)]),
                black_box(b"value2"),
                CommandClock::new(NOW_NANOS, NOW_NANOS),
            );
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_routed_preparsed_set(c: &mut Criterion) {
    let keys = local_command_keys();
    let mut probe = cost_probe(&keys);
    let mut index = 0usize;

    c.bench_function("sn004a_local_routed_preparsed_set_response", |b| {
        b.iter(|| {
            let result = probe.execute_routed_preparsed_set_plain(
                black_box(&keys[index & (keys.len() - 1)]),
                black_box(b"value2"),
                CommandClock::new(NOW_NANOS, NOW_NANOS),
            );
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_parsed_no_route_set(c: &mut Criterion) {
    let keys = local_command_keys();
    let tapes = set_tapes(&keys);
    let mut probe = cost_probe(&keys);
    let mut index = 0usize;

    c.bench_function("sn004a_local_parse_execute_no_route_set", |b| {
        b.iter(|| {
            let frame = tapes[index & (tapes.len() - 1)]
                .iter()
                .next()
                .expect("one frame");
            let result = probe.execute_parsed_no_route(
                b"SET",
                &frame,
                CommandClock::new(NOW_NANOS, NOW_NANOS),
            );
            black_box(result);
            index = index.wrapping_add(1);
        });
    });
}

fn bench_generation_guard(c: &mut Criterion) {
    let token = SharedNothingConnectionToken::new(
        SharedNothingConnectionId::new(7),
        SharedNothingConnectionGeneration::new(42),
    );
    let current = SharedNothingConnectionGeneration::new(42);

    c.bench_function("sn004a_generation_guard_current", |b| {
        b.iter(|| {
            let is_current = black_box(token).is_current(black_box(current));
            black_box(is_current);
        });
    });
}

fn bench_full_local_get(c: &mut Criterion) {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner == local_owner, COMMAND_KEY_COUNT);
    let mut executor = shared_nothing_executor(&keys);
    let tapes = get_tapes(&command_keys);
    let mut index = 0usize;

    c.bench_function("sn004a_full_local_get_hit", |b| {
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

fn bench_full_local_set(c: &mut Criterion) {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner == local_owner, COMMAND_KEY_COUNT);
    let mut executor = shared_nothing_executor(&keys);
    let tapes = set_tapes(&command_keys);
    let mut index = 0usize;

    c.bench_function("sn004a_full_local_set_replace", |b| {
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

fn bench_shared_get(c: &mut Criterion) {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner == local_owner, COMMAND_KEY_COUNT);
    let keyspace = shared_keyspace(&keys);
    let tapes = get_tapes(&command_keys);
    let mut index = 0usize;

    c.bench_function("sn004a_shared_keyspace_get_hit", |b| {
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

fn bench_shared_set(c: &mut Criterion) {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner == local_owner, COMMAND_KEY_COUNT);
    let keyspace = shared_keyspace(&keys);
    let tapes = set_tapes(&command_keys);
    let mut index = 0usize;

    c.bench_function("sn004a_shared_keyspace_set_replace", |b| {
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

fn bench_pipeline_widths(c: &mut Criterion) {
    let keys = dataset_keys();
    let local_owner = owner_zero();
    let command_keys = keys_matching(&keys, |owner| owner == local_owner, COMMAND_KEY_COUNT);
    let get_tapes = get_tapes(&command_keys);
    let set_tapes = set_tapes(&command_keys);
    let mut group = c.benchmark_group("sn004a_local_full_pipeline_batch");

    for width in [1usize, 16, 64] {
        group.throughput(Throughput::Elements(width as u64));

        let mut executor = shared_nothing_executor(&keys);
        let mut index = 0usize;
        group.bench_function(format!("get_width_{width}"), |b| {
            b.iter(|| {
                for _ in 0..width {
                    let frame = get_tapes[index & (get_tapes.len() - 1)]
                        .iter()
                        .next()
                        .expect("one frame");
                    let result =
                        executor.execute(b"GET", &frame, CommandClock::new(NOW_NANOS, NOW_NANOS));
                    black_box(result);
                    index = index.wrapping_add(1);
                }
            });
        });

        let mut executor = shared_nothing_executor(&keys);
        let mut index = 0usize;
        group.bench_function(format!("set_width_{width}"), |b| {
            b.iter(|| {
                for _ in 0..width {
                    let frame = set_tapes[index & (set_tapes.len() - 1)]
                        .iter()
                        .next()
                        .expect("one frame");
                    let result =
                        executor.execute(b"SET", &frame, CommandClock::new(NOW_NANOS, NOW_NANOS));
                    black_box(result);
                    index = index.wrapping_add(1);
                }
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_route_only,
    bench_parse_get,
    bench_parse_set,
    bench_storage_get,
    bench_storage_set,
    bench_preparsed_get,
    bench_routed_preparsed_get,
    bench_parsed_no_route_get,
    bench_preparsed_set,
    bench_routed_preparsed_set,
    bench_parsed_no_route_set,
    bench_generation_guard,
    bench_full_local_get,
    bench_full_local_set,
    bench_shared_get,
    bench_shared_set,
    bench_pipeline_widths,
);
criterion_main!(benches);
