use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use vortex_engine::owner::locality::{
    ConnectionSteering, KeyWorkload, LocalitySimulationConfig, LocalitySimulationResult,
    simulate_connection_locality,
};

const OWNER_COUNT: usize = 8;
const CAPSULE_COUNT: usize = 1024;
const KEY_COUNT: usize = 16_384;
const OPERATION_COUNT: usize = 1_048_576;
const BENCH_OPERATION_COUNT: usize = 65_536;
const CONNECTION_COUNT: usize = 1024;
const ARTIFACT_RELATIVE_ROOT: &str =
    ".artifacts/benchmarks/shared-nothing-sn004b-locality-20260514";

const SHARED_GET_NS: f64 = 28.492;
const LOCAL_GET_NS: f64 = 30.827;
const REMOTE_GET_NS: f64 = 788.49;
const SHARED_SET_NS: f64 = 49.014;
const LOCAL_SET_NS: f64 = 44.719;
const REMOTE_SET_NS: f64 = 818.79;
const REMOTE_GET_QUEUE_WAIT_P99_NS: f64 = 500.0;
const REMOTE_SET_QUEUE_WAIT_P99_NS: f64 = 500.0;

static REPORT_ARTIFACT: OnceLock<()> = OnceLock::new();

#[derive(Clone, Copy)]
struct WorkloadSpec {
    label: &'static str,
    workload: KeyWorkload,
    pipeline_width: usize,
}

#[derive(Clone, Copy)]
struct SteeringSpec {
    label: &'static str,
    steering: ConnectionSteering,
}

fn workloads() -> [WorkloadSpec; 9] {
    [
        WorkloadSpec {
            label: "uniform",
            workload: KeyWorkload::Uniform,
            pipeline_width: 1,
        },
        WorkloadSpec {
            label: "zipf_0_80",
            workload: KeyWorkload::Zipf { theta: 0.80 },
            pipeline_width: 1,
        },
        WorkloadSpec {
            label: "zipf_0_95",
            workload: KeyWorkload::Zipf { theta: 0.95 },
            pipeline_width: 1,
        },
        WorkloadSpec {
            label: "zipf_1_20",
            workload: KeyWorkload::Zipf { theta: 1.20 },
            pipeline_width: 1,
        },
        WorkloadSpec {
            label: "hot_key_90",
            workload: KeyWorkload::HotKey {
                hot_key_index: 0,
                hot_permille: 900,
            },
            pipeline_width: 1,
        },
        WorkloadSpec {
            label: "uniform_pipeline_16",
            workload: KeyWorkload::Uniform,
            pipeline_width: 16,
        },
        WorkloadSpec {
            label: "uniform_pipeline_64",
            workload: KeyWorkload::Uniform,
            pipeline_width: 64,
        },
        WorkloadSpec {
            label: "capsule_local_90",
            workload: KeyWorkload::CapsuleLocalUniform {
                capsule_local_permille: 900,
            },
            pipeline_width: 1,
        },
        WorkloadSpec {
            label: "capsule_local_99_5",
            workload: KeyWorkload::CapsuleLocalUniform {
                capsule_local_permille: 995,
            },
            pipeline_width: 1,
        },
    ]
}

fn steering_modes() -> [SteeringSpec; 4] {
    [
        SteeringSpec {
            label: "random",
            steering: ConnectionSteering::Random,
        },
        SteeringSpec {
            label: "round_robin",
            steering: ConnectionSteering::RoundRobin,
        },
        SteeringSpec {
            label: "owner_aware",
            steering: ConnectionSteering::OwnerAware,
        },
        SteeringSpec {
            label: "sticky_capsule",
            steering: ConnectionSteering::StickyCapsule,
        },
    ]
}

fn config(
    workload: WorkloadSpec,
    steering: SteeringSpec,
    operation_count: usize,
) -> LocalitySimulationConfig {
    LocalitySimulationConfig {
        owner_count: OWNER_COUNT,
        capsule_count: CAPSULE_COUNT,
        key_count: KEY_COUNT,
        operation_count,
        connection_count: CONNECTION_COUNT,
        pipeline_width: workload.pipeline_width,
        steering: steering.steering,
        workload: workload.workload,
        seed: 0x5004_b00d ^ ((workload.pipeline_width as u64) << 32),
    }
}

fn bench_locality_simulation(c: &mut Criterion) {
    write_report_artifact();

    let workload = WorkloadSpec {
        label: "uniform",
        workload: KeyWorkload::Uniform,
        pipeline_width: 1,
    };
    let steering = SteeringSpec {
        label: "owner_aware",
        steering: ConnectionSteering::OwnerAware,
    };
    let config = config(workload, steering, BENCH_OPERATION_COUNT);

    c.bench_function("sn004b_locality_simulate_uniform_owner_aware", |b| {
        b.iter(|| {
            let result = simulate_connection_locality(black_box(config))
                .expect("locality simulation succeeds");
            black_box(result);
        });
    });
}

fn write_report_artifact() {
    REPORT_ARTIFACT.get_or_init(|| {
        let root = workspace_artifact_root();
        fs::create_dir_all(&root).expect("artifact root created");
        fs::write(root.join("locality-matrix.csv"), locality_matrix_csv())
            .expect("locality matrix artifact written");
    });
}

fn locality_matrix_csv() -> String {
    let mut csv = String::new();
    writeln!(
        csv,
        "workload,steering,pipeline_width,operations,connections,local_ratio_pct,remote_ratio_pct,target_owner_saturation,ingress_owner_saturation,expected_get_ns,shared_get_ns,get_delta_pct,expected_set_ns,shared_set_ns,set_delta_pct,remote_get_queue_wait_p99_ns,remote_set_queue_wait_p99_ns"
    )
    .unwrap();

    for workload in workloads() {
        for steering in steering_modes() {
            let result = simulate_connection_locality(config(workload, steering, OPERATION_COUNT))
                .expect("locality simulation succeeds");
            write_result_row(&mut csv, workload, steering, &result);
        }
    }
    csv
}

fn write_result_row(
    csv: &mut String,
    workload: WorkloadSpec,
    steering: SteeringSpec,
    result: &LocalitySimulationResult,
) {
    let local = result.local_ratio;
    let remote = result.remote_ratio;
    let expected_get = expected_latency(local, remote, LOCAL_GET_NS, REMOTE_GET_NS);
    let expected_set = expected_latency(local, remote, LOCAL_SET_NS, REMOTE_SET_NS);

    writeln!(
        csv,
        "{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
        workload.label,
        steering.label,
        workload.pipeline_width,
        result.operations,
        CONNECTION_COUNT,
        local * 100.0,
        remote * 100.0,
        result.target_owner_saturation,
        result.ingress_owner_saturation,
        expected_get,
        SHARED_GET_NS,
        percent_delta(expected_get, SHARED_GET_NS),
        expected_set,
        SHARED_SET_NS,
        percent_delta(expected_set, SHARED_SET_NS),
        REMOTE_GET_QUEUE_WAIT_P99_NS,
        REMOTE_SET_QUEUE_WAIT_P99_NS,
    )
    .unwrap();
}

#[inline]
fn expected_latency(local_ratio: f64, remote_ratio: f64, local_ns: f64, remote_ns: f64) -> f64 {
    local_ratio * local_ns + remote_ratio * remote_ns
}

#[inline]
fn percent_delta(value: f64, baseline: f64) -> f64 {
    ((value - baseline) / baseline) * 100.0
}

fn workspace_artifact_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .join(ARTIFACT_RELATIVE_ROOT)
}

criterion_group!(benches, bench_locality_simulation);
criterion_main!(benches);
