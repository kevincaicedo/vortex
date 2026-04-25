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
    let shard_index = keyspace.shard_index(key.as_bytes());
    let mut guard = keyspace.write_shard_by_index(shard_index);
    guard.insert(key, value);
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

criterion_group!(
    benches,
    bench_cmd_get_inline,
    bench_cmd_set_inline,
    bench_cmd_set_inline_allkeys_lru_headroom,
    bench_cmd_set_inline_allkeys_lru_evict_same_shard,
    bench_cmd_set_inline_allkeys_lfu_headroom,
    bench_cmd_set_inline_allkeys_lfu_evict_same_shard,
    bench_cmd_get_miss,
    bench_cmd_incr,
    bench_cmd_mget_100,
    bench_cmd_mset_100,
    bench_cmd_append_inline,
    bench_cmd_dbsize_10k,
    bench_cmd_del_inline,
    bench_cmd_exists,
    bench_cmd_expire,
    bench_cmd_ttl,
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
    bench_latency_distribution_set
);
criterion_main!(benches);
