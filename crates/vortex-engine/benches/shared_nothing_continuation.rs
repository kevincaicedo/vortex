use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Instant;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use hdrhistogram::Histogram;
use vortex_engine::owner::{
    OwnerId, OwnerReplyStatus, ReactorContinuationHarness, ReactorContinuationMetrics,
    SharedNothingConnectionId,
};

const OWNER_COUNT: usize = 8;
const CAPSULE_COUNT: usize = 1024;
const CONNECTION_COUNT: usize = 1024;
const BALANCED_OPERATIONS: usize = 50_000;
const BENCH_OPERATIONS: usize = 4096;
const ARTIFACT_RELATIVE_ROOT: &str =
    ".artifacts/benchmarks/shared-nothing-sn004c-continuation-20260514";

static REPORT_ARTIFACT: OnceLock<()> = OnceLock::new();

struct ScenarioResult {
    label: &'static str,
    operations: usize,
    elapsed_ns: u128,
    queue_wait: Histogram<u64>,
    metrics: ReactorContinuationMetrics,
}

impl ScenarioResult {
    fn ns_per_command(&self) -> f64 {
        self.elapsed_ns as f64 / self.operations.max(1) as f64
    }

    fn ns_per_hop(&self) -> f64 {
        self.ns_per_command() / 2.0
    }
}

fn bench_continuation_batch(c: &mut Criterion) {
    write_report_artifact();

    c.bench_function("sn004c_reactor_continuation_batch_64", |b| {
        b.iter(|| {
            let result = run_balanced_scenario::<1024>("criterion_batch_64", BENCH_OPERATIONS);
            black_box(result.metrics.published_replies);
        });
    });
}

fn write_report_artifact() {
    REPORT_ARTIFACT.get_or_init(|| {
        let root = workspace_artifact_root();
        fs::create_dir_all(&root).expect("artifact root created");

        let balanced = run_balanced_scenario::<1024>("balanced_batch_64", BALANCED_OPERATIONS);
        let pressure = run_pressure_scenario::<16>("pressure_ring_16_budget_4", 128);

        let mut csv = String::new();
        writeln!(
            csv,
            "scenario,operations,elapsed_ns,ns_per_command,ns_per_hop,accepted,published,rejected,command_rejections,reply_credit_rejections,owner_overruns,ingress_overruns,stale_epoch,stale_generation_drops,orphan_replies,disconnect_cleanups,max_pending,max_command_slots,max_command_bytes,max_reply_slots,max_reply_bytes,max_reply_credits,p50_queue_wait_ns,p95_queue_wait_ns,p99_queue_wait_ns,p999_queue_wait_ns,max_queue_wait_ns"
        )
        .unwrap();
        write_scenario_row(&mut csv, &balanced);
        write_scenario_row(&mut csv, &pressure);
        fs::write(root.join("continuation-metrics.csv"), csv)
            .expect("continuation metrics artifact written");
    });
}

fn write_scenario_row(csv: &mut String, result: &ScenarioResult) {
    let metrics = &result.metrics;
    writeln!(
        csv,
        "{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        result.label,
        result.operations,
        result.elapsed_ns,
        result.ns_per_command(),
        result.ns_per_hop(),
        metrics.accepted_commands,
        metrics.published_replies,
        metrics.rejected_commands,
        metrics.command_queue_rejections,
        metrics.reply_credit_rejections,
        metrics.owner_turn_budget_overruns,
        metrics.ingress_turn_budget_overruns,
        metrics.stale_epoch_replies,
        metrics.stale_generation_drops,
        metrics.orphan_replies,
        metrics.disconnect_cleanups,
        metrics.max_pending_continuations,
        metrics.max_command_queue_slots,
        metrics.max_command_queue_bytes,
        metrics.max_reply_queue_slots,
        metrics.max_reply_queue_bytes,
        metrics.max_reply_credits_used,
        result.queue_wait.value_at_quantile(0.50),
        result.queue_wait.value_at_quantile(0.95),
        result.queue_wait.value_at_quantile(0.99),
        result.queue_wait.value_at_quantile(0.999),
        result.queue_wait.max(),
    )
    .unwrap();
}

fn run_balanced_scenario<const N: usize>(label: &'static str, operations: usize) -> ScenarioResult {
    let mut harness =
        ReactorContinuationHarness::<N>::new(OWNER_COUNT, CAPSULE_COUNT, CONNECTION_COUNT)
            .expect("harness builds");
    let source = owner(&harness, 0);
    let start = Instant::now();

    let mut accepted = 0usize;
    while accepted < operations {
        let destination = owner(&harness, 1 + accepted % (OWNER_COUNT - 1));
        let capsule = harness
            .capsule_id(accepted & (CAPSULE_COUNT - 1))
            .expect("capsule exists");
        let connection = SharedNothingConnectionId::new((accepted % CONNECTION_COUNT) as u32);

        match harness.try_accept_remote(source, destination, capsule, connection) {
            Ok(_) => {
                accepted += 1;
                if accepted % 64 == 0 {
                    drain_all_once(&mut harness, 64, 64);
                }
            }
            Err(_) => {
                drain_all_once(&mut harness, 128, 128);
            }
        }
    }

    harness
        .drain_until_idle(128, 128, operations)
        .expect("harness drains");
    finish_scenario(label, operations, start.elapsed().as_nanos(), harness)
}

fn run_pressure_scenario<const N: usize>(label: &'static str, attempts: usize) -> ScenarioResult {
    let mut harness =
        ReactorContinuationHarness::<N>::new(OWNER_COUNT, CAPSULE_COUNT, CONNECTION_COUNT)
            .expect("harness builds");
    let source = owner(&harness, 0);
    let destination = owner(&harness, 1);
    let capsule = harness.capsule_id(0).expect("capsule exists");
    let start = Instant::now();

    for attempt in 0..attempts {
        let connection = SharedNothingConnectionId::new((attempt % CONNECTION_COUNT) as u32);
        if harness
            .try_accept_remote(source, destination, capsule, connection)
            .is_err()
        {
            drain_all_once(&mut harness, 4, 4);
        }
    }

    harness
        .drain_until_idle(4, 4, attempts)
        .expect("harness drains");
    finish_scenario(label, attempts, start.elapsed().as_nanos(), harness)
}

fn finish_scenario<const N: usize>(
    label: &'static str,
    operations: usize,
    elapsed_ns: u128,
    harness: ReactorContinuationHarness<N>,
) -> ScenarioResult {
    let mut queue_wait = Histogram::<u64>::new(3).expect("histogram builds");
    for connection_index in 0..CONNECTION_COUNT {
        let connection = SharedNothingConnectionId::new(connection_index as u32);
        let Some(published) = harness.published_for_connection(connection) else {
            continue;
        };
        for reply in published {
            debug_assert!(matches!(
                reply.status,
                OwnerReplyStatus::Ok | OwnerReplyStatus::StaleEpoch
            ));
            let nanos = reply.queue_wait.as_nanos().min(u64::MAX as u128) as u64;
            queue_wait.record(nanos).expect("queue wait recorded");
        }
    }

    ScenarioResult {
        label,
        operations,
        elapsed_ns,
        queue_wait,
        metrics: harness.metrics().clone(),
    }
}

fn drain_all_once<const N: usize>(
    harness: &mut ReactorContinuationHarness<N>,
    owner_budget: usize,
    ingress_budget: usize,
) {
    for owner_index in 0..OWNER_COUNT {
        let owner = owner(harness, owner_index);
        harness
            .drain_owner_commands(owner, owner_budget)
            .expect("owner command drain succeeds");
    }
    for owner_index in 0..OWNER_COUNT {
        let owner = owner(harness, owner_index);
        harness
            .drain_ingress_replies(owner, ingress_budget)
            .expect("ingress reply drain succeeds");
    }
}

fn owner<const N: usize>(harness: &ReactorContinuationHarness<N>, index: usize) -> OwnerId {
    harness.owner_id(index).expect("owner exists")
}

fn workspace_artifact_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .join(ARTIFACT_RELATIVE_ROOT)
}

criterion_group!(benches, bench_continuation_batch);
criterion_main!(benches);
