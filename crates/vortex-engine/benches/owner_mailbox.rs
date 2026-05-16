use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use vortex_engine::owner::{
    KeyCapsuleId, MailboxDrainCursor, MailboxLane, OwnerCommand, OwnerId, OwnerMailboxMesh,
    OwnerMessage, OwnerReplyStatus, OwnerTopology, TopologyConfig, TopologyEpoch,
};

const RING_SLOTS: usize = 4096;
const BENCH_CAPSULE_COUNT: usize = 64;

fn topology(owner_count: usize) -> OwnerTopology {
    OwnerTopology::new(
        TopologyConfig::new(owner_count, BENCH_CAPSULE_COUNT).expect("valid topology"),
    )
}

fn owner(topology: &OwnerTopology, index: usize) -> OwnerId {
    topology.owner_id(index).expect("owner exists")
}

fn command_message(
    request_id: u64,
    source: OwnerId,
    destination: OwnerId,
    capsule: KeyCapsuleId,
) -> OwnerMessage {
    OwnerMessage::one_way_command(
        request_id,
        source,
        destination,
        capsule,
        TopologyEpoch::INITIAL,
    )
}

fn credited_command(
    request_id: u64,
    source: OwnerId,
    destination: OwnerId,
    capsule: KeyCapsuleId,
) -> OwnerMessage {
    OwnerMessage::command(
        request_id,
        source,
        destination,
        capsule,
        TopologyEpoch::INITIAL,
    )
}

fn bench_pair_enqueue_dequeue(c: &mut Criterion) {
    let topology = topology(2);
    let source = owner(&topology, 0);
    let destination = owner(&topology, 1);
    let capsule = topology.capsule_id(0).expect("capsule exists");
    let mesh = OwnerMailboxMesh::<RING_SLOTS>::new(topology.config()).expect("mesh builds");

    let mut group = c.benchmark_group("owner_mailbox_pair_enqueue_dequeue");
    for batch in [1usize, 8, 64, 256, 1024] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            b.iter(|| {
                for request_id in 0..batch as u64 {
                    mesh.try_send(command_message(
                        black_box(request_id),
                        source,
                        destination,
                        capsule,
                    ))
                    .expect("enqueue succeeds");
                }

                let mut checksum = 0u64;
                let drained = mesh
                    .drain_lane(
                        source,
                        destination,
                        MailboxLane::Command,
                        batch,
                        |message| {
                            checksum ^= black_box(message.request_id());
                        },
                    )
                    .expect("drain succeeds");
                assert_eq!(drained, batch);
                black_box(checksum);
            });
        });
    }
    group.finish();
}

fn bench_credit_roundtrip_pair(c: &mut Criterion) {
    let topology = topology(2);
    let source = owner(&topology, 0);
    let destination = owner(&topology, 1);
    let capsule = topology.capsule_id(0).expect("capsule exists");
    let mesh = OwnerMailboxMesh::<RING_SLOTS>::new(topology.config()).expect("mesh builds");
    const BATCH: usize = 256;

    c.benchmark_group("owner_mailbox_credit_roundtrip_pair")
        .throughput(Throughput::Elements(BATCH as u64))
        .bench_function("batch_256", |b| {
            b.iter(|| {
                for request_id in 0..BATCH as u64 {
                    mesh.try_send(credited_command(
                        black_box(request_id),
                        source,
                        destination,
                        capsule,
                    ))
                    .expect("credited command enqueue succeeds");
                }

                let mut commands: Vec<OwnerCommand> = Vec::with_capacity(BATCH);
                let drained_commands = mesh
                    .drain_lane(
                        source,
                        destination,
                        MailboxLane::Command,
                        BATCH,
                        |message| {
                            let OwnerMessage::Command(command) = message else {
                                panic!("expected command");
                            };
                            commands.push(command);
                        },
                    )
                    .expect("command drain succeeds");
                assert_eq!(drained_commands, BATCH);

                for command in commands {
                    mesh.try_send(OwnerMessage::reply_to(command, OwnerReplyStatus::Ok))
                        .expect("reply credit guarantees enqueue");
                }

                let mut checksum = 0u64;
                let drained_replies = mesh
                    .drain_lane(destination, source, MailboxLane::Reply, BATCH, |message| {
                        checksum ^= black_box(message.request_id());
                    })
                    .expect("reply drain succeeds");
                assert_eq!(drained_replies, BATCH);
                black_box(checksum);
            });
        });
}

fn bench_all_to_all_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("owner_mailbox_all_to_all_batch");
    const MESSAGES_PER_PAIR: usize = 16;

    for owner_count in [4usize, 8, 16, 32] {
        let topology = topology(owner_count);
        let owners: Vec<OwnerId> = (0..owner_count)
            .map(|index| owner(&topology, index))
            .collect();
        let capsule = topology.capsule_id(0).expect("capsule exists");
        let mesh = OwnerMailboxMesh::<RING_SLOTS>::new(topology.config()).expect("mesh builds");
        let total_messages = owner_count * (owner_count - 1) * MESSAGES_PER_PAIR;

        group.throughput(Throughput::Elements(total_messages as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(owner_count),
            &owner_count,
            |b, &_owner_count| {
                b.iter(|| {
                    for &source in &owners {
                        for &destination in &owners {
                            if source == destination {
                                continue;
                            }

                            for request_id in 0..MESSAGES_PER_PAIR as u64 {
                                mesh.try_send(command_message(
                                    black_box(request_id),
                                    source,
                                    destination,
                                    capsule,
                                ))
                                .expect("enqueue succeeds");
                            }
                        }
                    }

                    let mut total_drained = 0usize;
                    let mut checksum = 0u64;
                    for &destination in &owners {
                        let mut cursor = MailboxDrainCursor::new();
                        let target = (owner_count - 1) * MESSAGES_PER_PAIR;
                        let mut drained_for_owner = 0usize;

                        while drained_for_owner < target {
                            let drained = mesh
                                .drain_owner_lane_with_cursor(
                                    destination,
                                    MailboxLane::Command,
                                    &mut cursor,
                                    256,
                                    |message| {
                                        checksum ^= black_box(message.request_id());
                                    },
                                )
                                .expect("owner drain succeeds");
                            drained_for_owner += drained;
                        }

                        total_drained += drained_for_owner;
                    }

                    assert_eq!(total_drained, total_messages);
                    black_box(checksum);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_pair_enqueue_dequeue,
    bench_credit_roundtrip_pair,
    bench_all_to_all_batch
);
criterion_main!(benches);
