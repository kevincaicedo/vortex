use criterion::{Criterion, black_box, criterion_group, criterion_main};
use vortex_common::{VortexKey, VortexValue};
use vortex_engine::commands::execute_command;
use vortex_engine::{ConcurrentKeyspace, EvictionPolicy};
use vortex_proto::RespTape;

const BENCH_CONCURRENT_SHARDS: usize = 64;

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

fn insert_keyspace_value(keyspace: &ConcurrentKeyspace, key: VortexKey, value: VortexValue) {
    // SAFETY: benchmark fixtures use this only to seed data around the
    // measured command path; command invariants are not being validated here.
    unsafe {
        keyspace.benchmark_insert_unchecked(key, value);
    }
}

fn prefill_keyspace(n: usize) -> ConcurrentKeyspace {
    let keyspace = ConcurrentKeyspace::with_capacity(BENCH_CONCURRENT_SHARDS, n);
    for i in 0..n {
        let key = VortexKey::from(format!("key:{i:08}").as_bytes());
        insert_keyspace_value(&keyspace, key, VortexValue::Integer(i as i64));
    }
    keyspace
}

fn same_shard_keys(keyspace: &ConcurrentKeyspace, count: usize) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(count);
    let target = keyspace.shard_index(b"bench-evict-seed");
    for index in 0..10_000usize {
        let key = format!("bench-evict:{index:04}").into_bytes();
        if keyspace.shard_index(&key) != target {
            continue;
        }
        keys.push(key);
        if keys.len() == count {
            return keys;
        }
    }
    panic!("failed to find {count} same-shard benchmark keys");
}

fn bench_cmd_get_inline(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let key = VortexKey::from(b"mykey" as &[u8]);
    insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"myvalue"));

    let cmd = make_resp(&[b"GET", b"mykey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_get_inline", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"GET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_get_inline_allkeys_lfu_hot(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let key = VortexKey::from(b"mykey" as &[u8]);
    insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"myvalue"));
    keyspace.configure_eviction(1 << 20, EvictionPolicy::AllKeysLfu);

    let cmd = make_resp(&[b"GET", b"mykey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_get_inline_allkeys_lfu_hot", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"GET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_set_inline(c: &mut Criterion) {
    let cmd = make_resp(&[b"SET", b"mykey", b"myvalue"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_set_inline", |b| {
        let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
        let key = VortexKey::from(b"mykey" as &[u8]);
        insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"old"));

        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"SET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_set_inline_allkeys_lru_headroom(c: &mut Criterion) {
    let cmd = make_resp(&[b"SET", b"mykey", b"myvalue"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_set_inline_allkeys_lru_headroom", |b| {
        let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
        let key = VortexKey::from(b"mykey" as &[u8]);
        insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"oldval"));
        keyspace.configure_eviction(1 << 20, EvictionPolicy::AllKeysLru);

        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"SET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_set_inline_allkeys_lru_evict_same_shard(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let keys = same_shard_keys(&keyspace, 3);
    let get_cmd = make_resp(&[b"GET", keys[0].as_slice()]);
    let get_tape = RespTape::parse_pipeline(&get_cmd).unwrap();
    let set_cmd = make_resp(&[b"SET", keys[2].as_slice(), b"mild"]);
    let set_tape = RespTape::parse_pipeline(&set_cmd).unwrap();

    c.bench_function("cmd_set_inline_allkeys_lru_evict_same_shard", |b| {
        b.iter_batched(
            || {
                let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
                insert_keyspace_value(
                    &keyspace,
                    VortexKey::from(keys[0].as_slice()),
                    VortexValue::from_bytes(b"warm"),
                );
                insert_keyspace_value(
                    &keyspace,
                    VortexKey::from(keys[1].as_slice()),
                    VortexValue::from_bytes(b"cool"),
                );
                keyspace.configure_eviction(keyspace.memory_used(), EvictionPolicy::AllKeysLru);
                for _ in 0..16 {
                    let frame = get_tape.iter().next().unwrap();
                    let _ = execute_command(&keyspace, b"GET", &frame, 0);
                }
                keyspace
            },
            |keyspace| {
                let frame = set_tape.iter().next().unwrap();
                let r = execute_command(black_box(&keyspace), b"SET", &frame, 0);
                black_box(r);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_cmd_set_inline_allkeys_lfu_headroom(c: &mut Criterion) {
    let cmd = make_resp(&[b"SET", b"mykey", b"myvalue"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_set_inline_allkeys_lfu_headroom", |b| {
        let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
        let key = VortexKey::from(b"mykey" as &[u8]);
        insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"oldval"));
        keyspace.configure_eviction(1 << 20, EvictionPolicy::AllKeysLfu);

        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"SET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_set_inline_allkeys_lfu_evict_same_shard(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let keys = same_shard_keys(&keyspace, 3);
    let get_cmd = make_resp(&[b"GET", keys[0].as_slice()]);
    let get_tape = RespTape::parse_pipeline(&get_cmd).unwrap();
    let set_cmd = make_resp(&[b"SET", keys[2].as_slice(), b"mild"]);
    let set_tape = RespTape::parse_pipeline(&set_cmd).unwrap();

    c.bench_function("cmd_set_inline_allkeys_lfu_evict_same_shard", |b| {
        b.iter_batched(
            || {
                let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
                insert_keyspace_value(
                    &keyspace,
                    VortexKey::from(keys[0].as_slice()),
                    VortexValue::from_bytes(b"warm"),
                );
                insert_keyspace_value(
                    &keyspace,
                    VortexKey::from(keys[1].as_slice()),
                    VortexValue::from_bytes(b"cool"),
                );
                keyspace.configure_eviction(keyspace.memory_used(), EvictionPolicy::AllKeysLfu);
                for _ in 0..32 {
                    let frame = get_tape.iter().next().unwrap();
                    let _ = execute_command(&keyspace, b"GET", &frame, 0);
                }
                keyspace
            },
            |keyspace| {
                let frame = set_tape.iter().next().unwrap();
                let r = execute_command(black_box(&keyspace), b"SET", &frame, 0);
                black_box(r);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_cmd_set_inline_volatile_lru_no_ttl_oom(c: &mut Criterion) {
    let cmd = make_resp(&[b"SET", b"probe", b"value"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_set_inline_volatile_lru_no_ttl_oom", |b| {
        b.iter_batched(
            || {
                let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
                insert_keyspace_value(
                    &keyspace,
                    VortexKey::from(b"resident" as &[u8]),
                    VortexValue::from_bytes(b"resident"),
                );
                keyspace.configure_eviction(keyspace.memory_used(), EvictionPolicy::VolatileLru);
                keyspace
            },
            |keyspace| {
                let frame = tape.iter().next().unwrap();
                let r = execute_command(black_box(&keyspace), b"SET", &frame, 0);
                black_box(r);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_cmd_get_miss(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let cmd = make_resp(&[b"GET", b"nosuchkey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_get_miss", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"GET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_incr(c: &mut Criterion) {
    let cmd = make_resp(&[b"INCR", b"counter"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_incr", |b| {
        let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
        let key = VortexKey::from(b"counter" as &[u8]);
        insert_keyspace_value(&keyspace, key, VortexValue::Integer(0));

        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"INCR", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_mget_100(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::with_capacity(BENCH_CONCURRENT_SHARDS, 100);
    let mut parts: Vec<Vec<u8>> = vec![b"MGET".to_vec()];
    for i in 0..100 {
        let key_str = format!("key:{i:04}");
        let key = VortexKey::from(key_str.as_bytes());
        insert_keyspace_value(&keyspace, key, VortexValue::Integer(i));
        parts.push(key_str.into_bytes());
    }

    let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    let cmd = make_resp(&refs);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_mget_100", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"MGET", &frame, 0);
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
        let keyspace = ConcurrentKeyspace::with_capacity(BENCH_CONCURRENT_SHARDS, 100);
        for i in 0..100 {
            let key = VortexKey::from(format!("key:{i:04}").as_bytes());
            insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"old"));
        }

        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"MSET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_append_inline(c: &mut Criterion) {
    let cmd = make_resp(&[b"APPEND", b"mykey", b"abc"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_append_inline", |b| {
        let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
        let key = VortexKey::from(b"mykey" as &[u8]);
        insert_keyspace_value(&keyspace, key.clone(), VortexValue::from_bytes(b"hello"));

        b.iter(|| {
            insert_keyspace_value(&keyspace, key.clone(), VortexValue::from_bytes(b"hello"));
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"APPEND", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_dbsize_10k(c: &mut Criterion) {
    let keyspace = prefill_keyspace(10_000);
    let cmd = make_resp(&[b"DBSIZE"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_dbsize_10k", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"DBSIZE", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_throughput_get_set_mix(c: &mut Criterion) {
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

    let keyspace = ConcurrentKeyspace::with_capacity(BENCH_CONCURRENT_SHARDS, n_keys);
    for i in 0..n_keys {
        let key = VortexKey::from(format!("k:{i:08}").as_bytes());
        insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"val"));
    }

    c.bench_function("throughput_get_set_mix", |b| {
        b.iter(|| {
            for i in 0..batch {
                let idx = i % n_tapes;
                if i & 1 == 0 {
                    let frame = set_tapes[idx].iter().next().unwrap();
                    let r = execute_command(&keyspace, b"SET", &frame, 0);
                    black_box(r);
                } else {
                    let frame = get_tapes[idx].iter().next().unwrap();
                    let r = execute_command(&keyspace, b"GET", &frame, 0);
                    black_box(r);
                }
            }
        });
    });
}

// ── Task 3.5 — Key Management Benchmarks ────────────────────────────

fn bench_cmd_del_inline(c: &mut Criterion) {
    let cmd = make_resp(&[b"DEL", b"mykey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();
    let key = VortexKey::from(b"mykey" as &[u8]);

    c.bench_function("cmd_del_inline", |b| {
        b.iter_batched_ref(
            || {
                let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
                insert_keyspace_value(&keyspace, key.clone(), VortexValue::from_bytes(b"val"));
                keyspace
            },
            |keyspace| {
                let frame = tape.iter().next().unwrap();
                let r = execute_command(black_box(&*keyspace), b"DEL", &frame, 0);
                black_box(r);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_cmd_exists(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let key = VortexKey::from(b"mykey" as &[u8]);
    insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"val"));

    let cmd = make_resp(&[b"EXISTS", b"mykey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_exists", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"EXISTS", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_del_small_batches(c: &mut Criterion) {
    for size in [2usize, 4, 8, 16] {
        let keyspace = prefill_keyspace(10_000);
        let keys: Vec<Vec<u8>> = (0..size)
            .map(|i| format!("key:{:08}", i * 17).into_bytes())
            .collect();
        let mut parts: Vec<Vec<u8>> = vec![b"DEL".to_vec()];
        parts.extend(keys.iter().cloned());
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        let cmd = make_resp(&refs);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();

        let name = format!("cmd_del_{size}_keys");
        c.bench_function(&name, |b| {
            b.iter(|| {
                for key_bytes in &keys {
                    insert_keyspace_value(
                        &keyspace,
                        VortexKey::from(key_bytes.as_slice()),
                        VortexValue::Integer(0),
                    );
                }
                let frame = tape.iter().next().unwrap();
                let r = execute_command(black_box(&keyspace), b"DEL", &frame, 0);
                black_box(r);
            });
        });
    }
}

fn bench_cmd_exists_small_batches(c: &mut Criterion) {
    for size in [2usize, 4, 8, 16] {
        let keyspace = prefill_keyspace(10_000);
        let mut parts: Vec<Vec<u8>> = vec![b"EXISTS".to_vec()];
        for i in 0..size {
            parts.push(format!("key:{:08}", i * 17).into_bytes());
        }
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        let cmd = make_resp(&refs);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();

        let name = format!("cmd_exists_{size}_keys");
        c.bench_function(&name, |b| {
            b.iter(|| {
                let frame = tape.iter().next().unwrap();
                let r = execute_command(black_box(&keyspace), b"EXISTS", &frame, 0);
                black_box(r);
            });
        });
    }
}

fn bench_cmd_expire(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let key = VortexKey::from(b"mykey" as &[u8]);
    insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"val"));

    let cmd = make_resp(&[b"EXPIRE", b"mykey", b"3600"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_expire", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"EXPIRE", &frame, 1_000_000_000_000);
            black_box(r);
        });
    });
}

fn bench_cmd_ttl(c: &mut Criterion) {
    let now: u64 = 1_000_000_000_000;
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let key = VortexKey::from(b"mykey" as &[u8]);
    insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"val"));
    {
        let expire_cmd = make_resp(&[b"EXPIRE", b"mykey", b"3600"]);
        let expire_tape = RespTape::parse_pipeline(&expire_cmd).unwrap();
        let frame = expire_tape.iter().next().unwrap();
        execute_command(&keyspace, b"EXPIRE", &frame, now);
    }

    let cmd = make_resp(&[b"TTL", b"mykey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_ttl", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"TTL", &frame, now);
            black_box(r);
        });
    });
}

fn bench_active_expiry_empty_shard(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);

    c.bench_function("active_expiry_empty_shard", |b| {
        b.iter(|| {
            let result = keyspace.run_active_expiry_on_shard(0, 0, 64, 0);
            black_box(result);
        });
    });
}

fn bench_cmd_type(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let key = VortexKey::from(b"mykey" as &[u8]);
    insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"val"));

    let cmd = make_resp(&[b"TYPE", b"mykey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_type", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"TYPE", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_scan_10k(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::with_capacity(BENCH_CONCURRENT_SHARDS, 10_000);
    for i in 0..10_000 {
        let key = VortexKey::from(format!("key:{i:06}").as_str());
        insert_keyspace_value(&keyspace, key, VortexValue::Integer(i));
    }

    c.bench_function("cmd_scan_10k", |b| {
        b.iter(|| {
            let mut cursor: u64 = 0;
            let mut total = 0usize;
            loop {
                let cur_str = cursor.to_string();
                let cmd = make_resp(&[b"SCAN", cur_str.as_bytes(), b"COUNT", b"100"]);
                let tape = RespTape::parse_pipeline(&cmd).unwrap();
                let frame = tape.iter().next().unwrap();
                let r = execute_command(black_box(&keyspace), b"SCAN", &frame, 0);
                if let Some(vortex_engine::commands::ExecutedCommand {
                    response:
                        vortex_engine::commands::CmdResult::Resp(vortex_proto::RespFrame::Array(
                            Some(ref arr),
                        )),
                    ..
                }) = r
                {
                    if let vortex_proto::RespFrame::BulkString(Some(c)) = &arr[0] {
                        cursor = std::str::from_utf8(c).unwrap().parse().unwrap();
                    }
                    if let vortex_proto::RespFrame::Array(Some(keys)) = &arr[1] {
                        total += keys.len();
                    }
                }
                if cursor == 0 {
                    break;
                }
            }
            black_box(total);
        });
    });
}

fn bench_cmd_keys_star_10k(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::with_capacity(BENCH_CONCURRENT_SHARDS, 10_000);
    for i in 0..10_000 {
        let key = VortexKey::from(format!("key:{i:06}").as_str());
        insert_keyspace_value(&keyspace, key, VortexValue::Integer(i));
    }

    let cmd = make_resp(&[b"KEYS", b"*"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_keys_star_10k", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"KEYS", &frame, 0);
            black_box(r);
        });
    });
}

// ── 3.6 — Server & Connection Command Benchmarks ────────────────────────

// ── Task 3.9 — Prefetch Pipeline Benchmarks ─────────────────────────

fn bench_cmd_mget_100_1m(c: &mut Criterion) {
    let keyspace = prefill_keyspace(1_000_000);

    let mut parts: Vec<Vec<u8>> = vec![b"MGET".to_vec()];
    for i in 0..100 {
        parts.push(format!("key:{:08}", i * 10_000).into_bytes());
    }
    let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    let cmd = make_resp(&refs);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_mget_100_1m", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"MGET", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_del_100_1m(c: &mut Criterion) {
    let keyspace = prefill_keyspace(1_000_000);

    let del_keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("key:{:08}", i * 10_000).into_bytes())
        .collect();

    let mut parts: Vec<Vec<u8>> = vec![b"DEL".to_vec()];
    for k in &del_keys {
        parts.push(k.clone());
    }
    let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    let cmd = make_resp(&refs);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_del_100_1m", |b| {
        b.iter(|| {
            for k in &del_keys {
                let key = VortexKey::from(k.as_slice());
                insert_keyspace_value(&keyspace, key, VortexValue::Integer(0));
            }
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"DEL", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_exists_100_1m(c: &mut Criterion) {
    let keyspace = prefill_keyspace(1_000_000);

    let mut parts: Vec<Vec<u8>> = vec![b"EXISTS".to_vec()];
    for i in 0..100 {
        parts.push(format!("key:{:08}", i * 10_000).into_bytes());
    }
    let refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();
    let cmd = make_resp(&refs);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_exists_100_1m", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"EXISTS", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_ping(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let cmd = make_resp(&[b"PING"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_ping", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"PING", &frame, 0);
            black_box(r);
        });
    });
}

fn bench_cmd_info(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::with_capacity(BENCH_CONCURRENT_SHARDS, 1_000);
    for i in 0..1_000 {
        let key = VortexKey::from(format!("key:{i:06}").as_str());
        insert_keyspace_value(&keyspace, key, VortexValue::Integer(i));
    }
    let cmd = make_resp(&[b"INFO"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("cmd_info", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"INFO", &frame, 0);
            black_box(r);
        });
    });
}

// ── 3.10.4 — Throughput at 1M Keys ─────────────────────────────────

fn bench_throughput_get_set_1m(c: &mut Criterion) {
    let n_keys = 1_000_000usize;
    let batch = 100_000usize;

    let n_tapes = 10_000usize;
    let set_cmds: Vec<Vec<u8>> = (0..n_tapes)
        .map(|i| make_resp(&[b"SET", format!("k:{:08}", i * 100).as_bytes(), b"val"]))
        .collect();
    let get_cmds: Vec<Vec<u8>> = (0..n_tapes)
        .map(|i| make_resp(&[b"GET", format!("k:{:08}", i * 100).as_bytes()]))
        .collect();
    let set_tapes: Vec<RespTape> = set_cmds
        .iter()
        .map(|c| RespTape::parse_pipeline(c).unwrap())
        .collect();
    let get_tapes: Vec<RespTape> = get_cmds
        .iter()
        .map(|c| RespTape::parse_pipeline(c).unwrap())
        .collect();

    let keyspace = ConcurrentKeyspace::with_capacity(BENCH_CONCURRENT_SHARDS, n_keys);
    for i in 0..n_keys {
        let key = VortexKey::from(format!("k:{i:08}").as_bytes());
        insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"val"));
    }

    c.bench_function("throughput_get_set_1m", |b| {
        b.iter(|| {
            for i in 0..batch {
                let idx = i % n_tapes;
                if i & 1 == 0 {
                    let frame = set_tapes[idx].iter().next().unwrap();
                    let r = execute_command(&keyspace, b"SET", &frame, 0);
                    black_box(r);
                } else {
                    let frame = get_tapes[idx].iter().next().unwrap();
                    let r = execute_command(&keyspace, b"GET", &frame, 0);
                    black_box(r);
                }
            }
        });
    });
}

// ── 3.10.4 — Latency Distribution ─────────────────────────────────

fn bench_latency_distribution_get(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::with_capacity(BENCH_CONCURRENT_SHARDS, 100_000);
    for i in 0..100_000 {
        let key = VortexKey::from(format!("k:{i:08}").as_bytes());
        insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"value"));
    }

    let cmd = make_resp(&[b"GET", b"k:00050000"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("latency_get_p50_p99_p999", |b| {
        b.iter(|| {
            let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
            for _ in 0..10_000 {
                let start = std::time::Instant::now();
                let frame = tape.iter().next().unwrap();
                let r = execute_command(black_box(&keyspace), b"GET", &frame, 0);
                black_box(r);
                let elapsed = start.elapsed().as_nanos() as u64;
                let _ = hist.record(elapsed);
            }
            let p50 = hist.value_at_quantile(0.50);
            let p99 = hist.value_at_quantile(0.99);
            let p999 = hist.value_at_quantile(0.999);
            black_box((p50, p99, p999));
        });
    });
}

fn bench_latency_distribution_set(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::with_capacity(BENCH_CONCURRENT_SHARDS, 100_000);
    for i in 0..100_000 {
        let key = VortexKey::from(format!("k:{i:08}").as_bytes());
        insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"old"));
    }

    let cmd = make_resp(&[b"SET", b"k:00050000", b"newvalue"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("latency_set_p50_p99_p999", |b| {
        b.iter(|| {
            let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
            for _ in 0..10_000 {
                let start = std::time::Instant::now();
                let frame = tape.iter().next().unwrap();
                let r = execute_command(black_box(&keyspace), b"SET", &frame, 0);
                black_box(r);
                let elapsed = start.elapsed().as_nanos() as u64;
                let _ = hist.record(elapsed);
            }
            let p50 = hist.value_at_quantile(0.50);
            let p99 = hist.value_at_quantile(0.99);
            let p999 = hist.value_at_quantile(0.999);
            black_box((p50, p99, p999));
        });
    });
}

// ── KEYS-014 — Read-Side Eviction Metadata Contention ──────────────

/// Baseline: hot-key GET with no eviction policy — no metadata writes.
fn bench_hotkey_get_no_eviction(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let key = VortexKey::from(b"hotkey" as &[u8]);
    insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"hotvalue"));

    let cmd = make_resp(&[b"GET", b"hotkey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("hotkey_get_no_eviction", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"GET", &frame, 0);
            black_box(r);
        });
    });
}

/// LRU read tracking: hot-key GET with allkeys-lru — entry AtomicU8 morris
/// counter write on every hit.
fn bench_hotkey_get_allkeys_lru(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let key = VortexKey::from(b"hotkey" as &[u8]);
    insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"hotvalue"));
    keyspace.configure_eviction(1 << 20, EvictionPolicy::AllKeysLru);

    let cmd = make_resp(&[b"GET", b"hotkey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("hotkey_get_allkeys_lru", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"GET", &frame, 0);
            black_box(r);
        });
    });
}

/// LFU read tracking: hot-key GET with allkeys-lfu — sampled frequency
/// sketch update + entry AtomicU8 morris counter write.
fn bench_hotkey_get_allkeys_lfu(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
    let key = VortexKey::from(b"hotkey" as &[u8]);
    insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"hotvalue"));
    keyspace.configure_eviction(1 << 20, EvictionPolicy::AllKeysLfu);

    let cmd = make_resp(&[b"GET", b"hotkey"]);
    let tape = RespTape::parse_pipeline(&cmd).unwrap();

    c.bench_function("hotkey_get_allkeys_lfu", |b| {
        b.iter(|| {
            let frame = tape.iter().next().unwrap();
            let r = execute_command(black_box(&keyspace), b"GET", &frame, 0);
            black_box(r);
        });
    });
}

/// Latency distribution comparison: hot-key GET at p50/p99/p999 across
/// no-eviction, allkeys-lru, and allkeys-lfu.
///
/// This is the core evidence benchmark for KEYS-014: it captures the
/// tail-latency impact of read-side metadata mutations on a single hot key.
fn bench_hotkey_latency_distribution(c: &mut Criterion) {
    let sample_count = 10_000usize;

    let policies: &[(&str, Option<EvictionPolicy>)] = &[
        ("none", None),
        ("lru", Some(EvictionPolicy::AllKeysLru)),
        ("lfu", Some(EvictionPolicy::AllKeysLfu)),
    ];

    for &(label, policy) in policies {
        let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
        let key = VortexKey::from(b"hotkey" as &[u8]);
        insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"hotvalue"));
        if let Some(p) = policy {
            keyspace.configure_eviction(1 << 20, p);
        }

        let cmd = make_resp(&[b"GET", b"hotkey"]);
        let tape = RespTape::parse_pipeline(&cmd).unwrap();

        c.bench_function(&format!("hotkey_latency_{label}"), |b| {
            b.iter(|| {
                let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
                for _ in 0..sample_count {
                    let start = std::time::Instant::now();
                    let frame = tape.iter().next().unwrap();
                    let r = execute_command(black_box(&keyspace), b"GET", &frame, 0);
                    black_box(r);
                    let elapsed = start.elapsed().as_nanos() as u64;
                    let _ = hist.record(elapsed);
                }
                let p50 = hist.value_at_quantile(0.50);
                let p99 = hist.value_at_quantile(0.99);
                let p999 = hist.value_at_quantile(0.999);
                black_box((p50, p99, p999));
            });
        });
    }
}

/// Eviction admission benchmark — validates KEYS-013 sweep driver performance
/// by measuring eviction-path SET commands under all policy variants.
fn bench_eviction_admission_sweep_driver(c: &mut Criterion) {
    let policies: &[(&str, EvictionPolicy)] = &[
        ("lru", EvictionPolicy::AllKeysLru),
        ("lfu", EvictionPolicy::AllKeysLfu),
        ("random", EvictionPolicy::AllKeysRandom),
    ];

    for &(label, policy) in policies {
        let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
        let keys = same_shard_keys(&keyspace, 3);
        let get_cmd = make_resp(&[b"GET", keys[0].as_slice()]);
        let get_tape = RespTape::parse_pipeline(&get_cmd).unwrap();
        let set_cmd = make_resp(&[b"SET", keys[2].as_slice(), b"evict-test"]);
        let set_tape = RespTape::parse_pipeline(&set_cmd).unwrap();

        c.bench_function(&format!("eviction_admission_{label}"), |b| {
            b.iter_batched(
                || {
                    let ks = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
                    insert_keyspace_value(
                        &ks,
                        VortexKey::from(keys[0].as_slice()),
                        VortexValue::from_bytes(b"warm"),
                    );
                    insert_keyspace_value(
                        &ks,
                        VortexKey::from(keys[1].as_slice()),
                        VortexValue::from_bytes(b"cool"),
                    );
                    ks.configure_eviction(ks.memory_used(), policy);
                    // Warm the LRU/LFU counters for key[0].
                    for _ in 0..16 {
                        let frame = get_tape.iter().next().unwrap();
                        let _ = execute_command(&ks, b"GET", &frame, 0);
                    }
                    ks
                },
                |ks| {
                    let frame = set_tape.iter().next().unwrap();
                    let r = execute_command(black_box(&ks), b"SET", &frame, 0);
                    black_box(r);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}

// ── KEYS-014 — Multi-Thread Cache-Line Contention ──────────────────

/// Multi-thread hot-key contention: N threads reading the same key with
/// AllKeysLru. Measures throughput scaling degradation caused by
/// AtomicU8 morris_cnt writes bouncing the cache line between cores.
///
/// Compared against the same workload with NoEviction (no metadata writes).
fn bench_hotkey_concurrent_contention(c: &mut Criterion) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering as AO};

    let thread_counts = [2, 4, 8];
    let ops_per_thread = 50_000usize;

    let policies: &[(&str, Option<EvictionPolicy>)] = &[
        ("none", None),
        ("lru", Some(EvictionPolicy::AllKeysLru)),
        ("lfu", Some(EvictionPolicy::AllKeysLfu)),
    ];

    for &(label, policy) in policies {
        for &n_threads in &thread_counts {
            let name = format!("concurrent_hotkey_{label}_{n_threads}t");
            c.bench_function(&name, |b| {
                b.iter_batched(
                    || {
                        let keyspace = Arc::new(ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS));
                        let key = VortexKey::from(b"hotkey" as &[u8]);
                        // SAFETY: benchmark fixture seeding.
                        unsafe {
                            keyspace.benchmark_insert_unchecked(
                                key,
                                VortexValue::from_bytes(b"hotvalue"),
                            );
                        }
                        if let Some(p) = policy {
                            keyspace.configure_eviction(1 << 20, p);
                        }
                        keyspace
                    },
                    |keyspace| {
                        let go = Arc::new(AtomicBool::new(false));
                        let handles: Vec<_> = (0..n_threads)
                            .map(|_| {
                                let ks = Arc::clone(&keyspace);
                                let go = Arc::clone(&go);
                                std::thread::spawn(move || {
                                    let cmd = make_resp(&[b"GET", b"hotkey"]);
                                    let tape = RespTape::parse_pipeline(&cmd).unwrap();
                                    while !go.load(AO::Acquire) {
                                        std::hint::spin_loop();
                                    }
                                    for _ in 0..ops_per_thread {
                                        let frame = tape.iter().next().unwrap();
                                        let r = execute_command(
                                            black_box(&*ks),
                                            b"GET",
                                            &frame,
                                            0,
                                        );
                                        black_box(r);
                                    }
                                })
                            })
                            .collect();
                        go.store(true, AO::Release);
                        for h in handles {
                            h.join().unwrap();
                        }
                    },
                    criterion::BatchSize::PerIteration,
                );
            });
        }
    }
}

// ── KEYS-015 — LFU Decay Tail Latency ─────────────────────────────

/// Measures the tail-latency spike when FrequencySketch::try_decay fires.
///
/// Under AllKeysLfu, every `record_frequency_hash` call increments the
/// sketch's `samples` counter. At every 65,536th sample, `try_decay()`
/// runs on the caller thread — iterating all 8,192 AtomicU8 counters
/// and halving each one.
///
/// This benchmark drives exactly enough SET commands to trigger multiple
/// decay cycles and captures the latency distribution to quantify the
/// spike caused by inline decay.
fn bench_lfu_decay_latency(c: &mut Criterion) {
    // We need enough samples to trigger decay. Each SET records one
    // frequency hash, so we need 65536+ SETs for at least one decay.
    // We'll drive 3 × 65536 = 196608 to capture multiple cycles.
    let total_ops = 3 * 65_536usize;

    c.bench_function("lfu_decay_latency_distribution", |b| {
        b.iter_batched(
            || {
                let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
                let key = VortexKey::from(b"decay-probe" as &[u8]);
                insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"val"));
                keyspace.configure_eviction(1 << 20, EvictionPolicy::AllKeysLfu);
                keyspace
            },
            |keyspace| {
                let cmd = make_resp(&[b"SET", b"decay-probe", b"newval"]);
                let tape = RespTape::parse_pipeline(&cmd).unwrap();
                let mut hist = hdrhistogram::Histogram::<u64>::new(3).unwrap();
                for _ in 0..total_ops {
                    let start = std::time::Instant::now();
                    let frame = tape.iter().next().unwrap();
                    let r = execute_command(black_box(&keyspace), b"SET", &frame, 0);
                    black_box(r);
                    let elapsed = start.elapsed().as_nanos() as u64;
                    let _ = hist.record(elapsed);
                }
                let p50 = hist.value_at_quantile(0.50);
                let p99 = hist.value_at_quantile(0.99);
                let p999 = hist.value_at_quantile(0.999);
                let max = hist.max();
                black_box((p50, p99, p999, max));
            },
            criterion::BatchSize::PerIteration,
        );
    });
}

/// Comparative LFU decay benchmark: same workload under NoEviction (no
/// sketch recording) vs AllKeysLfu (sketch + periodic decay). This
/// isolates the decay cost from baseline SET overhead.
fn bench_lfu_decay_vs_no_eviction(c: &mut Criterion) {
    let batch_size = 65_536usize;

    for &(label, policy) in &[
        ("noeviction", EvictionPolicy::NoEviction),
        ("allkeys_lfu", EvictionPolicy::AllKeysLfu),
    ] {
        c.bench_function(&format!("lfu_decay_batch_{label}"), |b| {
            b.iter_batched(
                || {
                    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);
                    let key = VortexKey::from(b"decay-probe" as &[u8]);
                    insert_keyspace_value(&keyspace, key, VortexValue::from_bytes(b"val"));
                    if policy != EvictionPolicy::NoEviction {
                        keyspace.configure_eviction(1 << 20, policy);
                    }
                    keyspace
                },
                |keyspace| {
                    let cmd = make_resp(&[b"SET", b"decay-probe", b"newval"]);
                    let tape = RespTape::parse_pipeline(&cmd).unwrap();
                    for _ in 0..batch_size {
                        let frame = tape.iter().next().unwrap();
                        let r = execute_command(black_box(&keyspace), b"SET", &frame, 0);
                        black_box(r);
                    }
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
}

// ── KEYS-016 — Transaction Gate Backoff ────────────────────────────

/// Baseline: uncontended command gate entry/exit cost.
/// This measures the hot-path overhead of `enter_command_gate_slot` when
/// no transaction (EXEC) is active — the per-command cost of the gate.
fn bench_gate_command_uncontended(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);

    c.bench_function("gate_command_uncontended", |b| {
        b.iter(|| {
            let _guard = black_box(keyspace.enter_command_gate_slot(0));
        });
    });
}

/// Baseline: uncontended transaction gate entry/exit cost.
/// This measures the EXEC-side cost of `enter_transaction_gate` when
/// no concurrent command dispatches are active.
fn bench_gate_transaction_uncontended(c: &mut Criterion) {
    let keyspace = ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS);

    c.bench_function("gate_transaction_uncontended", |b| {
        b.iter(|| {
            let _guard = black_box(keyspace.enter_transaction_gate());
        });
    });
}

/// Command-gate contention under active transaction: measures the
/// spin/yield backoff CPU cost when a transaction holds the gate.
///
/// One thread holds `enter_transaction_gate` for a fixed duration
/// while N reader threads try to enter the command gate. We measure
/// the total wall-clock time (including spin/yield wait).
fn bench_gate_command_under_transaction(c: &mut Criterion) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering as AO};

    let hold_durations_us = [10, 100, 1000]; // 10µs, 100µs, 1ms
    let n_readers = 4;
    let ops_per_reader = 1000usize;

    for &hold_us in &hold_durations_us {
        let name = format!("gate_cmd_under_tx_{hold_us}us_{n_readers}r");
        c.bench_function(&name, |b| {
            b.iter_batched(
                || Arc::new(ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS)),
                |keyspace| {
                    let ready = Arc::new(AtomicBool::new(false));
                    let done = Arc::new(AtomicBool::new(false));

                    // Transaction holder thread: enters exclusive gate, sleeps, exits.
                    let ks_tx = Arc::clone(&keyspace);
                    let ready_tx = Arc::clone(&ready);
                    let done_tx = Arc::clone(&done);
                    let tx_handle = std::thread::spawn(move || {
                        let _guard = ks_tx.enter_transaction_gate();
                        ready_tx.store(true, AO::Release);
                        // Hold the gate for the specified duration to simulate
                        // a multi-command EXEC batch.
                        std::thread::sleep(std::time::Duration::from_micros(hold_us));
                        drop(_guard);
                        done_tx.store(true, AO::Release);
                    });

                    // Wait for transaction gate to be acquired.
                    while !ready.load(AO::Acquire) {
                        std::hint::spin_loop();
                    }

                    // Reader threads: attempt command gate entry while tx is active.
                    let reader_handles: Vec<_> = (0..n_readers)
                        .map(|i| {
                            let ks = Arc::clone(&keyspace);
                            std::thread::spawn(move || {
                                let mut completed = 0usize;
                                for _ in 0..ops_per_reader {
                                    let _guard = ks.enter_command_gate_slot(i);
                                    completed += 1;
                                    black_box(&_guard);
                                }
                                completed
                            })
                        })
                        .collect();

                    tx_handle.join().unwrap();
                    let total: usize = reader_handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .sum();
                    black_box(total);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
}

/// EXEC simulation under concurrent GET/SET load.
///
/// Multiple reader threads execute GET commands through the command gate,
/// while one thread periodically enters the transaction gate (simulating
/// EXEC). Measures total throughput degradation from gate serialization.
fn bench_gate_exec_under_load(c: &mut Criterion) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AO};

    let n_workers = 4;
    let ops_per_worker = 50_000usize;

    let scenarios: &[(&str, bool)] = &[
        ("no_tx", false),    // Baseline: no EXEC, just command gate
        ("with_tx", true),   // With periodic EXEC interruptions
    ];

    for &(label, inject_tx) in scenarios {
        let name = format!("gate_exec_load_{label}_{n_workers}w");
        c.bench_function(&name, |b| {
            b.iter_batched(
                || {
                    let ks = Arc::new(ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS));
                    let key = VortexKey::from(b"gatekey" as &[u8]);
                    insert_keyspace_value(&ks, key, VortexValue::from_bytes(b"val"));
                    ks
                },
                |keyspace| {
                    let stop = Arc::new(AtomicBool::new(false));
                    let total_ops = Arc::new(AtomicU64::new(0));

                    // Worker threads: GET with command gate.
                    let worker_handles: Vec<_> = (0..n_workers)
                        .map(|i| {
                            let ks = Arc::clone(&keyspace);
                            let stop = Arc::clone(&stop);
                            let total = Arc::clone(&total_ops);
                            std::thread::spawn(move || {
                                let cmd = make_resp(&[b"GET", b"gatekey"]);
                                let tape = RespTape::parse_pipeline(&cmd).unwrap();
                                for n in 0..ops_per_worker {
                                    if stop.load(AO::Relaxed) {
                                        break;
                                    }
                                    let _gate = ks.enter_command_gate_slot(i);
                                    let frame = tape.iter().next().unwrap();
                                    let r = execute_command(
                                        black_box(&*ks),
                                        b"GET",
                                        &frame,
                                        0,
                                    );
                                    black_box(r);
                                    total.fetch_add(1, AO::Relaxed);
                                    let _ = n;
                                }
                            })
                        })
                        .collect();

                    // Transaction thread: periodic EXEC gate entry/exit.
                    let tx_handle = if inject_tx {
                        let ks = Arc::clone(&keyspace);
                        let stop = Arc::clone(&stop);
                        Some(std::thread::spawn(move || {
                            let mut tx_count = 0u64;
                            while !stop.load(AO::Relaxed) {
                                let _guard = ks.enter_transaction_gate();
                                // Simulate a short EXEC batch: 10µs of work.
                                std::thread::sleep(std::time::Duration::from_micros(10));
                                drop(_guard);
                                tx_count += 1;
                                // Rate-limit EXEC to avoid starvation.
                                std::thread::sleep(std::time::Duration::from_micros(100));
                                if tx_count >= 100 {
                                    break;
                                }
                            }
                        }))
                    } else {
                        None
                    };

                    for h in worker_handles {
                        h.join().unwrap();
                    }
                    stop.store(true, AO::Release);
                    if let Some(h) = tx_handle {
                        h.join().unwrap();
                    }
                    let completed = total_ops.load(AO::Relaxed);
                    black_box(completed);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
}

/// Measures spin_or_yield CPU behavior: how quickly does the command gate
/// unblock after the transaction gate releases?
///
/// One thread holds the transaction gate for exactly 100µs, then releases.
/// N threads measuring command gate entry latency — the delta between
/// actual entry time and the release time reveals the yield overhead.
fn bench_gate_yield_latency(c: &mut Criterion) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AO};

    let n_threads = 4;

    c.bench_function("gate_yield_wake_latency", |b| {
        b.iter_batched(
            || Arc::new(ConcurrentKeyspace::new(BENCH_CONCURRENT_SHARDS)),
            |keyspace| {
                let released = Arc::new(AtomicU64::new(0));
                let ready = Arc::new(AtomicBool::new(false));

                let ks_tx = Arc::clone(&keyspace);
                let released_tx = Arc::clone(&released);
                let ready_tx = Arc::clone(&ready);

                let tx_handle = std::thread::spawn(move || {
                    let _guard = ks_tx.enter_transaction_gate();
                    ready_tx.store(true, AO::Release);
                    std::thread::sleep(std::time::Duration::from_micros(100));
                    released_tx.store(
                        std::time::Instant::now().elapsed().as_nanos() as u64,
                        AO::Release,
                    );
                });

                while !ready.load(AO::Acquire) {
                    std::hint::spin_loop();
                }

                // Readers start trying to enter while gate is held.
                let reader_handles: Vec<_> = (0..n_threads)
                    .map(|i| {
                        let ks = Arc::clone(&keyspace);
                        std::thread::spawn(move || {
                            let start = std::time::Instant::now();
                            let _guard = ks.enter_command_gate_slot(i);
                            start.elapsed().as_nanos() as u64
                        })
                    })
                    .collect();

                tx_handle.join().unwrap();
                let waits: Vec<u64> = reader_handles
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .collect();
                let max_wait = waits.iter().copied().max().unwrap_or(0);
                black_box(max_wait);
            },
            criterion::BatchSize::PerIteration,
        );
    });
}

criterion_group!(
    benches,
    bench_cmd_get_inline,
    bench_cmd_get_inline_allkeys_lfu_hot,
    bench_cmd_set_inline,
    bench_cmd_set_inline_allkeys_lru_headroom,
    bench_cmd_set_inline_allkeys_lru_evict_same_shard,
    bench_cmd_set_inline_allkeys_lfu_headroom,
    bench_cmd_set_inline_allkeys_lfu_evict_same_shard,
    bench_cmd_set_inline_volatile_lru_no_ttl_oom,
    bench_cmd_get_miss,
    bench_cmd_incr,
    bench_cmd_mget_100,
    bench_cmd_mset_100,
    bench_cmd_append_inline,
    bench_cmd_dbsize_10k,
    bench_cmd_del_inline,
    bench_cmd_exists,
    bench_cmd_del_small_batches,
    bench_cmd_exists_small_batches,
    bench_cmd_expire,
    bench_cmd_ttl,
    bench_active_expiry_empty_shard,
    bench_cmd_type,
    bench_cmd_scan_10k,
    bench_cmd_keys_star_10k,
    bench_cmd_ping,
    bench_cmd_info,
    bench_cmd_mget_100_1m,
    bench_cmd_del_100_1m,
    bench_cmd_exists_100_1m,
    bench_throughput_get_set_mix,
    bench_throughput_get_set_1m,
    bench_latency_distribution_get,
    bench_latency_distribution_set,
    // KEYS-014: Read-side eviction metadata contention benchmarks
    bench_hotkey_get_no_eviction,
    bench_hotkey_get_allkeys_lru,
    bench_hotkey_get_allkeys_lfu,
    bench_hotkey_latency_distribution,
    bench_hotkey_concurrent_contention,
    // KEYS-013: Eviction admission sweep driver benchmarks
    bench_eviction_admission_sweep_driver,
    // KEYS-015: LFU decay latency benchmarks
    bench_lfu_decay_latency,
    bench_lfu_decay_vs_no_eviction,
    // KEYS-016: Transaction gate backoff benchmarks
    bench_gate_command_uncontended,
    bench_gate_transaction_uncontended,
    bench_gate_command_under_transaction,
    bench_gate_exec_under_load,
    bench_gate_yield_latency
);
criterion_main!(benches);
