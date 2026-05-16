use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Instant;

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use hdrhistogram::Histogram;
use vortex_common::{VortexKey, VortexValue};
use vortex_engine::owner::{
    OwnerId, PublishedScatterGather, SCATTER_INGRESS_REPLY_BUDGET_PARTIALS,
    SCATTER_OWNER_READ_BUDGET_KEYS, ScatterGatherHarness, ScatterGatherMetrics,
    ScatterGatherReadKind, SharedNothingConnectionGeneration, SharedNothingConnectionId,
    SharedNothingConnectionToken,
};

const OWNER_COUNT: usize = 8;
const CAPSULE_COUNT: usize = 1024;
const OWNER_CAPACITY: usize = 4096;
const CONNECTION_COUNT: usize = 1024;
const RING_SLOTS: usize = 1024;
const NOW_NANOS: u64 = 10_000_000;
const ARTIFACT_RELATIVE_ROOT: &str =
    ".artifacts/benchmarks/shared-nothing-sn006-scatter-gather-20260514";

static REPORT_ARTIFACT: OnceLock<()> = OnceLock::new();

#[derive(Clone, Copy)]
enum Distribution {
    LocalOnly,
    RemoteOnly,
    Mixed,
    HotRemote,
}

impl Distribution {
    const fn label(self) -> &'static str {
        match self {
            Self::LocalOnly => "local_only",
            Self::RemoteOnly => "remote_only",
            Self::Mixed => "mixed_uniform",
            Self::HotRemote => "hot_remote_owner",
        }
    }
}

struct ScenarioInput {
    source: OwnerId,
    kind: ScatterGatherReadKind,
    keys: Vec<Vec<u8>>,
    value_size: usize,
}

struct PreparedScenario {
    harness: ScatterGatherHarness<RING_SLOTS>,
    input: ScenarioInput,
}

#[derive(Default)]
struct RunTiming {
    accept_ns: u128,
    owner_drain_ns: u128,
    ingress_drain_ns: u128,
}

struct ScenarioResult {
    label: String,
    kind: ScatterGatherReadKind,
    width: usize,
    operations: usize,
    value_size: usize,
    distribution: Distribution,
    elapsed_ns: u128,
    accept_ns: u128,
    owner_drain_ns: u128,
    ingress_drain_ns: u128,
    wait: Histogram<u64>,
    metrics: ScatterGatherMetrics,
}

impl ScenarioResult {
    fn ns_per_command(&self) -> f64 {
        self.elapsed_ns as f64 / self.operations.max(1) as f64
    }

    fn ns_per_key(&self) -> f64 {
        self.elapsed_ns as f64 / (self.operations.max(1) * self.width.max(1)) as f64
    }
}

fn bench_mget_mixed_width_16(c: &mut Criterion) {
    write_report_artifact();
    c.bench_function("sn006_mget_mixed_width_16", |b| {
        b.iter_batched(
            || {
                prepare_scenario(scenario_input(
                    ScatterGatherReadKind::Mget,
                    Distribution::Mixed,
                    16,
                    16,
                ))
            },
            |mut prepared| black_box(run_prepared_operations_fast(&mut prepared, 1)),
            BatchSize::SmallInput,
        );
    });
}

fn bench_mget_mixed_width_64(c: &mut Criterion) {
    c.bench_function("sn006_mget_mixed_width_64", |b| {
        b.iter_batched(
            || {
                prepare_scenario(scenario_input(
                    ScatterGatherReadKind::Mget,
                    Distribution::Mixed,
                    64,
                    16,
                ))
            },
            |mut prepared| black_box(run_prepared_operations_fast(&mut prepared, 1)),
            BatchSize::SmallInput,
        );
    });
}

fn bench_mget_hot_remote_width_256(c: &mut Criterion) {
    c.bench_function("sn006_mget_hot_remote_width_256", |b| {
        b.iter_batched(
            || {
                prepare_scenario(scenario_input(
                    ScatterGatherReadKind::Mget,
                    Distribution::HotRemote,
                    256,
                    16,
                ))
            },
            |mut prepared| black_box(run_prepared_operations_fast(&mut prepared, 1)),
            BatchSize::SmallInput,
        );
    });
}

fn bench_exists_mixed_width_64(c: &mut Criterion) {
    c.bench_function("sn006_exists_mixed_width_64", |b| {
        b.iter_batched(
            || {
                prepare_scenario(scenario_input(
                    ScatterGatherReadKind::Exists,
                    Distribution::Mixed,
                    64,
                    16,
                ))
            },
            |mut prepared| black_box(run_prepared_operations_fast(&mut prepared, 1)),
            BatchSize::SmallInput,
        );
    });
}

fn write_report_artifact() {
    REPORT_ARTIFACT.get_or_init(|| {
        let root = workspace_artifact_root();
        fs::create_dir_all(&root).expect("artifact root created");

        let mut csv = String::new();
        writeln!(
            csv,
            "scenario,kind,distribution,width,operations,value_size,elapsed_ns,ns_per_command,ns_per_key,accept_ns,owner_drain_ns,reply_assembly_ns,accepted,published,remote_subplans,local_subplans,remote_executed,partials_drained,stale_generation,stale_epoch,orphan_replies,max_pending,max_width,max_subplans,max_remote_subplans,max_command_slots,max_command_bytes,max_reply_slots,max_reply_bytes,max_reply_credits,owned_response_bytes,p50_wait_ns,p95_wait_ns,p99_wait_ns,p999_wait_ns,max_wait_ns"
        )
        .unwrap();

        let widths = [1usize, 16, 64, 256];
        for width in widths {
            for distribution in [
                Distribution::LocalOnly,
                Distribution::RemoteOnly,
                Distribution::Mixed,
                Distribution::HotRemote,
            ] {
                let input = scenario_input(ScatterGatherReadKind::Mget, distribution, width, 16);
                let result = run_scenario(
                    &format!("mget_{}_width_{width}_16b", distribution.label()),
                    input,
                    operations_for(width, 16),
                );
                write_scenario_row(&mut csv, &result);
            }

            let input = scenario_input(ScatterGatherReadKind::Exists, Distribution::Mixed, width, 16);
            let result = run_scenario(
                &format!("exists_mixed_uniform_width_{width}"),
                input,
                operations_for(width, 16),
            );
            write_scenario_row(&mut csv, &result);
        }

        for value_size in [1024usize, 4096] {
            for width in [16usize, 64] {
                let input =
                    scenario_input(ScatterGatherReadKind::Mget, Distribution::Mixed, width, value_size);
                let result = run_scenario(
                    &format!("mget_mixed_uniform_width_{width}_{value_size}b"),
                    input,
                    operations_for(width, value_size),
                );
                write_scenario_row(&mut csv, &result);
            }
        }

        fs::write(root.join("scatter-gather-metrics.csv"), csv)
            .expect("scatter/gather metrics artifact written");
    });
}

fn write_scenario_row(csv: &mut String, result: &ScenarioResult) {
    let metrics = &result.metrics;
    writeln!(
        csv,
        "{},{:?},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        result.label,
        result.kind,
        result.distribution.label(),
        result.width,
        result.operations,
        result.value_size,
        result.elapsed_ns,
        result.ns_per_command(),
        result.ns_per_key(),
        result.accept_ns,
        result.owner_drain_ns,
        result.ingress_drain_ns,
        metrics.accepted_aggregates,
        metrics.published_aggregates,
        metrics.remote_subplans_enqueued,
        metrics.local_subplans_executed,
        metrics.remote_subplans_executed,
        metrics.partial_replies_drained,
        metrics.stale_generation_drops,
        metrics.stale_epoch_drops,
        metrics.orphan_replies,
        metrics.max_pending_aggregates,
        metrics.max_width,
        metrics.max_subplans_per_aggregate,
        metrics.max_remote_subplans_per_aggregate,
        metrics.max_command_queue_slots,
        metrics.max_command_queue_bytes,
        metrics.max_reply_queue_slots,
        metrics.max_reply_queue_bytes,
        metrics.max_reply_credits_used,
        metrics.owned_response_bytes,
        result.wait.value_at_quantile(0.50),
        result.wait.value_at_quantile(0.95),
        result.wait.value_at_quantile(0.99),
        result.wait.value_at_quantile(0.999),
        result.wait.max(),
    )
    .unwrap();
}

fn scenario_input(
    kind: ScatterGatherReadKind,
    distribution: Distribution,
    width: usize,
    value_size: usize,
) -> ScenarioInput {
    let probe = ScatterGatherHarness::<RING_SLOTS>::new(OWNER_COUNT, CAPSULE_COUNT, 1, 1)
        .expect("probe builds");
    let source = probe.owner_id(0).expect("owner zero");
    let hot_remote = probe.owner_id(1).expect("owner one");
    let mut keys = Vec::with_capacity(width);
    let mut local_index = 0usize;
    let mut remote_index = 0usize;
    let mut hot_index = 0usize;

    for index in 0..width {
        let key = match distribution {
            Distribution::LocalOnly => {
                local_index += 1;
                key_for(&probe, &format!("sn006-local-{local_index}"), |owner| {
                    owner == source
                })
            }
            Distribution::RemoteOnly => {
                remote_index += 1;
                key_for(&probe, &format!("sn006-remote-{remote_index}"), |owner| {
                    owner != source
                })
            }
            Distribution::Mixed if index % 2 == 0 => {
                local_index += 1;
                key_for(
                    &probe,
                    &format!("sn006-mixed-local-{local_index}"),
                    |owner| owner == source,
                )
            }
            Distribution::Mixed => {
                remote_index += 1;
                key_for(
                    &probe,
                    &format!("sn006-mixed-remote-{remote_index}"),
                    |owner| owner != source,
                )
            }
            Distribution::HotRemote => {
                hot_index += 1;
                key_for(&probe, &format!("sn006-hot-{hot_index}"), |owner| {
                    owner == hot_remote
                })
            }
        };
        keys.push(key);
    }

    ScenarioInput {
        source,
        kind,
        keys,
        value_size,
    }
}

fn run_scenario(label: &str, input: ScenarioInput, operations: usize) -> ScenarioResult {
    let mut prepared = prepare_scenario(input);
    let kind = prepared.input.kind;
    let width = prepared.input.keys.len();
    let value_size = prepared.input.value_size;
    let distribution = scenario_distribution_from_label(label);
    let start = Instant::now();
    let timing = run_prepared_operations(&mut prepared, operations);
    let elapsed_ns = start.elapsed().as_nanos();

    let wait = aggregate_wait_histogram(&prepared.harness);
    ScenarioResult {
        label: label.to_owned(),
        kind,
        width,
        operations,
        value_size,
        distribution,
        elapsed_ns,
        accept_ns: timing.accept_ns,
        owner_drain_ns: timing.owner_drain_ns,
        ingress_drain_ns: timing.ingress_drain_ns,
        wait,
        metrics: prepared.harness.metrics().clone(),
    }
}

fn prepare_scenario(input: ScenarioInput) -> PreparedScenario {
    let mut harness = ScatterGatherHarness::<RING_SLOTS>::new(
        OWNER_COUNT,
        CAPSULE_COUNT,
        OWNER_CAPACITY,
        CONNECTION_COUNT,
    )
    .expect("scatter harness builds");
    let value = vec![b'v'; input.value_size];
    for key in &input.keys {
        harness.insert_routed(VortexKey::from_bytes(key), VortexValue::from_bytes(&value));
    }
    PreparedScenario { harness, input }
}

fn run_prepared_operations(prepared: &mut PreparedScenario, operations: usize) -> RunTiming {
    let key_refs: Vec<&[u8]> = prepared.input.keys.iter().map(Vec::as_slice).collect();
    let mut timing = RunTiming::default();
    for op_index in 0..operations {
        let accept_start = Instant::now();
        prepared
            .harness
            .try_accept_read(
                prepared.input.source,
                prepared.input.kind,
                &key_refs,
                token((op_index % CONNECTION_COUNT) as u32),
                NOW_NANOS,
            )
            .expect("scenario aggregate accepted");
        timing.accept_ns += accept_start.elapsed().as_nanos();

        if op_index % 64 == 63 {
            let (owner_ns, ingress_ns) = drain_all_once(&mut prepared.harness);
            timing.owner_drain_ns += owner_ns;
            timing.ingress_drain_ns += ingress_ns;
        }
    }
    let (owner_ns, ingress_ns) = drain_until_idle_timed(&mut prepared.harness, operations.max(1));
    timing.owner_drain_ns += owner_ns;
    timing.ingress_drain_ns += ingress_ns;
    timing
}

fn run_prepared_operations_fast(prepared: &mut PreparedScenario, operations: usize) -> u64 {
    let key_refs: Vec<&[u8]> = prepared.input.keys.iter().map(Vec::as_slice).collect();
    for op_index in 0..operations {
        prepared
            .harness
            .try_accept_read(
                prepared.input.source,
                prepared.input.kind,
                &key_refs,
                token((op_index % CONNECTION_COUNT) as u32),
                NOW_NANOS,
            )
            .expect("scenario aggregate accepted");

        if op_index % 64 == 63 {
            drain_all_once_fast(&mut prepared.harness);
        }
    }
    drain_until_idle_fast(&mut prepared.harness, operations.max(1));
    prepared.harness.metrics().published_aggregates
}

fn drain_until_idle_fast(harness: &mut ScatterGatherHarness<RING_SLOTS>, max_turns: usize) {
    for _ in 0..max_turns {
        if drain_all_once_fast(harness) == 0 {
            break;
        }
    }
}

fn drain_all_once_fast(harness: &mut ScatterGatherHarness<RING_SLOTS>) -> usize {
    let mut drained = 0usize;
    for owner_index in 0..OWNER_COUNT {
        let owner = harness.owner_id(owner_index).expect("owner exists");
        drained += harness
            .drain_owner_subplans(owner, SCATTER_OWNER_READ_BUDGET_KEYS, NOW_NANOS)
            .expect("owner drain succeeds");
    }
    for owner_index in 0..OWNER_COUNT {
        let owner = harness.owner_id(owner_index).expect("owner exists");
        drained += harness
            .drain_ingress_replies(owner, SCATTER_INGRESS_REPLY_BUDGET_PARTIALS)
            .expect("ingress drain succeeds");
    }
    drained
}

fn drain_until_idle_timed(
    harness: &mut ScatterGatherHarness<RING_SLOTS>,
    max_turns: usize,
) -> (u128, u128) {
    let mut owner_ns = 0u128;
    let mut ingress_ns = 0u128;
    for _ in 0..max_turns {
        let (owner_now, ingress_now) = drain_all_once(harness);
        owner_ns += owner_now;
        ingress_ns += ingress_now;
        if owner_now == 0 && ingress_now == 0 {
            break;
        }
    }
    (owner_ns, ingress_ns)
}

fn drain_all_once(harness: &mut ScatterGatherHarness<RING_SLOTS>) -> (u128, u128) {
    let owner_start = Instant::now();
    let mut owner_drained = 0usize;
    for owner_index in 0..OWNER_COUNT {
        let owner = harness.owner_id(owner_index).expect("owner exists");
        owner_drained += harness
            .drain_owner_subplans(owner, SCATTER_OWNER_READ_BUDGET_KEYS, NOW_NANOS)
            .expect("owner drain succeeds");
    }
    let owner_ns = if owner_drained == 0 {
        0
    } else {
        owner_start.elapsed().as_nanos()
    };

    let ingress_start = Instant::now();
    let mut ingress_drained = 0usize;
    for owner_index in 0..OWNER_COUNT {
        let owner = harness.owner_id(owner_index).expect("owner exists");
        ingress_drained += harness
            .drain_ingress_replies(owner, SCATTER_INGRESS_REPLY_BUDGET_PARTIALS)
            .expect("ingress drain succeeds");
    }
    let ingress_ns = if ingress_drained == 0 {
        0
    } else {
        ingress_start.elapsed().as_nanos()
    };

    (owner_ns, ingress_ns)
}

fn aggregate_wait_histogram(harness: &ScatterGatherHarness<RING_SLOTS>) -> Histogram<u64> {
    let mut hist = Histogram::<u64>::new(3).expect("histogram builds");
    for connection_index in 0..CONNECTION_COUNT {
        let connection = SharedNothingConnectionId::new(connection_index as u32);
        let Some(published) = harness.published_for_connection(connection) else {
            continue;
        };
        for aggregate in published {
            record_wait(&mut hist, aggregate);
        }
    }
    hist
}

fn record_wait(hist: &mut Histogram<u64>, aggregate: &PublishedScatterGather) {
    let nanos = aggregate.aggregate_wait.as_nanos().min(u64::MAX as u128) as u64;
    hist.record(nanos).expect("aggregate wait recorded");
}

fn key_for(
    harness: &ScatterGatherHarness<RING_SLOTS>,
    prefix: &str,
    is_match: impl Fn(OwnerId) -> bool,
) -> Vec<u8> {
    (0usize..100_000)
        .map(|index| format!("{prefix}:{index:05}").into_bytes())
        .find(|key| is_match(harness.route_owner_bytes(key)))
        .expect("routed key found")
}

fn token(connection_id: u32) -> SharedNothingConnectionToken {
    SharedNothingConnectionToken::new(
        SharedNothingConnectionId::new(connection_id),
        SharedNothingConnectionGeneration::INITIAL,
    )
}

fn operations_for(width: usize, value_size: usize) -> usize {
    match (width, value_size) {
        (256, _) => 128,
        (_, 4096) => 64,
        (_, 1024) => 128,
        _ => 256,
    }
}

fn scenario_distribution_from_label(label: &str) -> Distribution {
    if label.contains("local_only") {
        Distribution::LocalOnly
    } else if label.contains("remote_only") {
        Distribution::RemoteOnly
    } else if label.contains("hot_remote") {
        Distribution::HotRemote
    } else {
        Distribution::Mixed
    }
}

fn workspace_artifact_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .join(ARTIFACT_RELATIVE_ROOT)
}

criterion_group!(
    benches,
    bench_mget_mixed_width_16,
    bench_mget_mixed_width_64,
    bench_mget_hot_remote_width_256,
    bench_exists_mixed_width_64,
);
criterion_main!(benches);
