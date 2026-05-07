use super::mutation::*;
use super::*;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::EvictionPolicy;

const TEST_SHARDS: usize = 64;

fn fixed_key(label: &str, suffix: usize) -> VortexKey {
    VortexKey::from(format!("race:{label}:{suffix:03}"))
}

fn key_for_shard_with_len(
    keyspace: &ConcurrentKeyspace,
    target_shard: usize,
    total_len: usize,
    prefix: &str,
) -> VortexKey {
    for candidate in 0..200_000usize {
        let mut text = format!("{prefix}:{candidate:06}");
        if text.len() > total_len {
            continue;
        }
        while text.len() < total_len {
            text.push('x');
        }
        let key = VortexKey::from(text);
        if keyspace.shard_index(key.as_bytes()) == target_shard {
            return key;
        }
    }

    panic!("failed to find key for shard {target_shard} with length {total_len}");
}

fn value_of_len(len: usize, byte: u8) -> VortexValue {
    let bytes = vec![byte; len];
    VortexValue::from_bytes(&bytes)
}

fn insert_raw(keyspace: &ConcurrentKeyspace, key: VortexKey, value: VortexValue) {
    let key_bytes = key.as_bytes().to_vec();
    keyspace.write(&key_bytes, move |table| {
        table.insert(key.clone(), value.clone());
    });
}

fn entry_lsn(keyspace: &ConcurrentKeyspace, key_bytes: &[u8]) -> Option<u64> {
    let shard_index = keyspace.shard_index(key_bytes);
    let table_hash = keyspace.table_hash_key(key_bytes);
    keyspace
        .read_shard_by_index(shard_index)
        .get_lsn_version_prehashed(key_bytes, table_hash)
}

fn configure_noeviction_at_current_usage(keyspace: &ConcurrentKeyspace) {
    keyspace.configure_eviction(keyspace.memory_used(), EvictionPolicy::NoEviction);
}

fn run_projection_race<T, F, G>(
    label: &'static str,
    keyspace: Arc<ConcurrentKeyspace>,
    writer: F,
    interleave: G,
) -> MutationResult<T>
where
    T: Send + 'static,
    F: FnOnce(Arc<ConcurrentKeyspace>) -> MutationResult<T> + Send + 'static,
    G: FnOnce(&ConcurrentKeyspace),
{
    let _test_lock = PROJECTION_ADMISSION_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (entered_rx, release_tx) = install_projection_admission_test_hook(label);
    let writer_keyspace = Arc::clone(&keyspace);
    let handle = thread::spawn(move || writer(writer_keyspace));

    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("writer should pause after projection");
    interleave(&keyspace);
    release_tx
        .send(())
        .expect("writer release sender should stay alive");

    let result = handle.join().expect("writer thread should not panic");
    let mut slot = PROJECTION_ADMISSION_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    slot.take();
    result
}

fn assert_oom<T>(result: MutationResult<T>) {
    let error = match result {
        Ok(_) => panic!("mutation should fail with OOM after revalidation"),
        Err(error) => error,
    };
    assert_eq!(error.response, crate::commands::ERR_OOM);
}

#[test]
fn featureless_plain_set_bytes_does_not_allocate_or_stamp_lsn() {
    let keyspace = ConcurrentKeyspace::new(TEST_SHARDS);
    let key = b"ctx006:plain";

    let outcome = keyspace
        .set_value_plain_bytes(key, b"value", 0)
        .expect("featureless plain SET should succeed");

    assert_eq!(outcome.aof_lsn, None);
    assert_eq!(keyspace.current_lsn(), 0);
    assert_eq!(entry_lsn(&keyspace, key), Some(0));
}

#[test]
fn watch_after_featureless_plain_set_detects_next_active_write() {
    let keyspace = ConcurrentKeyspace::new(TEST_SHARDS);
    let key = VortexKey::from_bytes(b"ctx006:watch");

    keyspace
        .set_value_plain_bytes(key.as_bytes(), b"before", 0)
        .expect("historical featureless SET should succeed");
    assert_eq!(keyspace.current_lsn(), 0);
    assert_eq!(entry_lsn(&keyspace, key.as_bytes()), Some(0));

    let epoch = keyspace.current_watch_epoch();
    let watched = keyspace.watch_key(key.clone());
    assert!(!keyspace.watched_keys_changed(epoch, std::slice::from_ref(&watched)));

    let outcome = keyspace
        .set_value_plain_bytes(key.as_bytes(), b"after", 0)
        .expect("WATCH-active SET should succeed");

    assert_eq!(outcome.aof_lsn, None);
    assert_eq!(entry_lsn(&keyspace, key.as_bytes()), Some(1));
    assert_eq!(keyspace.current_lsn(), 2);
    assert!(keyspace.watched_keys_changed(epoch, std::slice::from_ref(&watched)));
    keyspace.unwatch_keys(std::iter::once(watched));
}

#[test]
fn aof_plain_set_bytes_allocates_and_stamps_lsn() {
    let keyspace = ConcurrentKeyspace::new(TEST_SHARDS);
    let key = b"ctx006:aof";
    keyspace.enable_aof_recording();

    let first = keyspace
        .set_value_plain_bytes(key, b"one", 0)
        .expect("AOF-active SET should succeed");
    assert_eq!(first.aof_lsn, Some(0));
    assert_eq!(entry_lsn(&keyspace, key), Some(0));

    let second = keyspace
        .set_value_plain_bytes(key, b"two", 0)
        .expect("second AOF-active SET should succeed");
    assert_eq!(second.aof_lsn, Some(1));
    assert_eq!(entry_lsn(&keyspace, key), Some(1));
    assert_eq!(keyspace.current_lsn(), 2);

    keyspace.disable_aof_recording();
}

#[test]
fn set_value_plain_revalidates_after_delete_and_fill() {
    let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
    let key = fixed_key("set", 0);
    let filler = fixed_key("set", 1);
    let old_value = value_of_len(32, b'a');
    let new_value = value_of_len(32, b'b');

    insert_raw(&keyspace, key.clone(), old_value.clone());
    configure_noeviction_at_current_usage(&keyspace);

    let writer_key = key.clone();
    let writer_value = new_value.clone();
    let stale_key = key.clone();
    let stale_filler = filler.clone();
    let stale_value = old_value.clone();
    let result = run_projection_race(
        "set_value_plain",
        Arc::clone(&keyspace),
        move |keyspace| keyspace.set_value_plain(writer_key, writer_value, 0),
        move |keyspace| {
            assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
            insert_raw(keyspace, stale_filler, stale_value);
        },
    );

    assert_oom(result);
    assert!(keyspace.get_value(&key, 0).is_none());
    assert!(keyspace.get_value(&filler, 0).is_some());
}

#[test]
fn append_value_revalidates_after_delete_and_fill() {
    let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
    let key = fixed_key("append", 0);
    let filler = fixed_key("append", 1);
    let old_value = value_of_len(32, b'c');

    insert_raw(&keyspace, key.clone(), old_value.clone());
    configure_noeviction_at_current_usage(&keyspace);

    let writer_key = key.clone();
    let stale_key = key.clone();
    let stale_filler = filler.clone();
    let stale_value = old_value.clone();
    let result = run_projection_race(
        "append_value",
        Arc::clone(&keyspace),
        move |keyspace| keyspace.append_value(writer_key, b"", 0),
        move |keyspace| {
            assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
            insert_raw(keyspace, stale_filler, stale_value);
        },
    );

    assert_oom(result);
    assert!(keyspace.get_value(&key, 0).is_none());
    assert!(keyspace.get_value(&filler, 0).is_some());
}

#[test]
fn setrange_value_revalidates_after_delete_and_fill() {
    let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
    let key = fixed_key("setrange", 0);
    let filler = fixed_key("setrange", 1);
    let old_value = value_of_len(1, b'd');

    insert_raw(&keyspace, key.clone(), old_value.clone());
    configure_noeviction_at_current_usage(&keyspace);

    let writer_key = key.clone();
    let stale_key = key.clone();
    let stale_filler = filler.clone();
    let stale_value = old_value.clone();
    let result = run_projection_race(
        "setrange_value",
        Arc::clone(&keyspace),
        move |keyspace| keyspace.setrange_value(writer_key, 0, b"z", 0),
        move |keyspace| {
            assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
            insert_raw(keyspace, stale_filler, stale_value);
        },
    );

    assert_oom(result);
    assert!(keyspace.get_value(&key, 0).is_none());
    assert!(keyspace.get_value(&filler, 0).is_some());
}

#[test]
fn increment_by_revalidates_after_delete_and_fill() {
    let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
    let key = fixed_key("incr", 0);
    let filler = fixed_key("incr", 1);
    let old_value = VortexValue::from(0_i64);

    insert_raw(&keyspace, key.clone(), old_value.clone());
    configure_noeviction_at_current_usage(&keyspace);

    let writer_key = key.clone();
    let stale_key = key.clone();
    let stale_filler = filler.clone();
    let stale_value = old_value.clone();
    let result = run_projection_race(
        "increment_by",
        Arc::clone(&keyspace),
        move |keyspace| keyspace.increment_by(writer_key, 0, 0),
        move |keyspace| {
            assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
            insert_raw(keyspace, stale_filler, stale_value);
        },
    );

    assert_oom(result);
    assert!(keyspace.get_value(&key, 0).is_none());
    assert!(keyspace.get_value(&filler, 0).is_some());
}

#[test]
fn increment_by_float_revalidates_after_delete_and_fill() {
    let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
    let key = fixed_key("incrfloat", 0);
    let filler = fixed_key("incrfloat", 1);
    let old_value = VortexValue::from("0.0");

    insert_raw(&keyspace, key.clone(), old_value.clone());
    configure_noeviction_at_current_usage(&keyspace);

    let writer_key = key.clone();
    let stale_key = key.clone();
    let stale_filler = filler.clone();
    let stale_value = old_value.clone();
    let result = run_projection_race(
        "increment_by_float",
        Arc::clone(&keyspace),
        move |keyspace| keyspace.increment_by_float(writer_key, 0.0, 0),
        move |keyspace| {
            assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
            insert_raw(keyspace, stale_filler, stale_value);
        },
    );

    assert_oom(result);
    assert!(keyspace.get_value(&key, 0).is_none());
    assert!(keyspace.get_value(&filler, 0).is_some());
}

#[test]
fn mset_values_revalidates_after_delete_and_fill() {
    let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
    let key = fixed_key("mset", 0);
    let filler = fixed_key("mset", 1);
    let old_value = value_of_len(32, b'm');
    let new_value = value_of_len(32, b'n');

    insert_raw(&keyspace, key.clone(), old_value.clone());
    configure_noeviction_at_current_usage(&keyspace);

    let writer_key = key.clone();
    let writer_value = new_value.clone();
    let stale_key = key.clone();
    let stale_filler = filler.clone();
    let stale_value = old_value.clone();
    let result = run_projection_race(
        "mset_values",
        Arc::clone(&keyspace),
        move |keyspace| keyspace.mset_values(vec![(writer_key, writer_value)], 0),
        move |keyspace| {
            assert!(keyspace.remove_value(&stale_key, 0).value.is_some());
            insert_raw(keyspace, stale_filler, stale_value);
        },
    );

    assert_oom(result);
    assert!(keyspace.get_value(&key, 0).is_none());
    assert!(keyspace.get_value(&filler, 0).is_some());
}

#[test]
fn mset_values_deduplicates_before_memory_admission() {
    let keyspace = ConcurrentKeyspace::new(TEST_SHARDS);
    let key = fixed_key("mset-duplicate", 0);
    let first_value = value_of_len(32, b'1');
    let second_value = value_of_len(32, b'2');
    let projected_delta = {
        let shard_index = keyspace.shard_index(key.as_bytes());
        let guard = keyspace.read_shard_by_index(shard_index);
        positive_delta(guard.projected_insert_delta(&key, &second_value)).bytes()
    };
    keyspace.configure_eviction(
        keyspace.memory_used() + projected_delta,
        EvictionPolicy::NoEviction,
    );

    let outcome = keyspace
        .mset_values(
            vec![
                (key.clone(), first_value),
                (key.clone(), second_value.clone()),
            ],
            0,
        )
        .expect("deduplicated MSET should fit maxmemory");

    assert_eq!(outcome.value, ());
    assert_eq!(keyspace.get_value(&key, 0), Some(second_value));
}

#[test]
fn mset_values_deduplicates_cross_shard_batch_before_memory_admission() {
    let keyspace = ConcurrentKeyspace::new(TEST_SHARDS);
    let duplicate_key = key_for_shard_with_len(&keyspace, 0, 24, "mset-dup-cross");
    let other_key = key_for_shard_with_len(&keyspace, TEST_SHARDS - 1, 24, "mset-other");
    let first_value = value_of_len(64, b'1');
    let final_duplicate_value = value_of_len(64, b'2');
    let other_value = value_of_len(64, b'3');
    let projected_delta = {
        let duplicate_shard = keyspace.shard_index(duplicate_key.as_bytes());
        let other_shard = keyspace.shard_index(other_key.as_bytes());
        let duplicate_guard = keyspace.read_shard_by_index(duplicate_shard);
        let duplicate_delta = positive_delta(
            duplicate_guard.projected_insert_delta(&duplicate_key, &final_duplicate_value),
        )
        .bytes();
        drop(duplicate_guard);

        let other_guard = keyspace.read_shard_by_index(other_shard);
        duplicate_delta
            + positive_delta(other_guard.projected_insert_delta(&other_key, &other_value)).bytes()
    };
    keyspace.configure_eviction(
        keyspace.memory_used() + projected_delta,
        EvictionPolicy::NoEviction,
    );

    let outcome = keyspace
        .mset_values(
            vec![
                (duplicate_key.clone(), first_value),
                (other_key.clone(), other_value.clone()),
                (duplicate_key.clone(), final_duplicate_value.clone()),
            ],
            0,
        )
        .expect("deduplicated cross-shard MSET should fit maxmemory");

    assert_eq!(outcome.value, ());
    assert_eq!(
        keyspace.get_value(&duplicate_key, 0),
        Some(final_duplicate_value)
    );
    assert_eq!(keyspace.get_value(&other_key, 0), Some(other_value));
}

#[test]
fn rename_key_revalidates_after_destination_delete_and_fill() {
    let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
    let old_key = key_for_shard_with_len(&keyspace, 0, 20, "rename-old");
    let new_key = key_for_shard_with_len(&keyspace, TEST_SHARDS - 1, 28, "rename-new");
    let filler_key = key_for_shard_with_len(&keyspace, 1, 28, "rename-fill");
    let value = value_of_len(16, b'r');

    insert_raw(&keyspace, old_key.clone(), value.clone());
    insert_raw(&keyspace, new_key.clone(), value.clone());
    configure_noeviction_at_current_usage(&keyspace);

    let writer_old = old_key.clone();
    let writer_new = new_key.clone();
    let stale_new = new_key.clone();
    let stale_filler = filler_key.clone();
    let stale_value = value.clone();
    let result = run_projection_race(
        "rename_key",
        Arc::clone(&keyspace),
        move |keyspace| keyspace.rename_key(&writer_old, writer_new, 0, false),
        move |keyspace| {
            assert!(keyspace.remove_value(&stale_new, 0).value.is_some());
            insert_raw(keyspace, stale_filler, stale_value);
        },
    );

    assert_oom(result);
    assert!(keyspace.get_value(&old_key, 0).is_some());
    assert!(keyspace.get_value(&new_key, 0).is_none());
    assert!(keyspace.get_value(&filler_key, 0).is_some());
}

#[test]
fn rename_key_same_shard_respects_memory_admission() {
    let keyspace = ConcurrentKeyspace::new(TEST_SHARDS);
    let old_key = key_for_shard_with_len(&keyspace, 0, 32, "rename-same-old");
    let new_key = key_for_shard_with_len(&keyspace, 0, 56, "rename-same-new");
    let value = value_of_len(16, b's');

    insert_raw(&keyspace, old_key.clone(), value.clone());
    configure_noeviction_at_current_usage(&keyspace);

    let result = keyspace.rename_key(&old_key, new_key.clone(), 0, false);

    assert_oom(result);
    assert_eq!(keyspace.get_value(&old_key, 0), Some(value));
    assert!(keyspace.get_value(&new_key, 0).is_none());
}

#[test]
fn copy_key_revalidates_after_destination_delete_and_fill() {
    let keyspace = Arc::new(ConcurrentKeyspace::new(TEST_SHARDS));
    let src = key_for_shard_with_len(&keyspace, 0, 20, "copy-src");
    let dst = key_for_shard_with_len(&keyspace, TEST_SHARDS - 1, 20, "copy-dst");
    let filler = key_for_shard_with_len(&keyspace, 1, 20, "copy-fill");
    let value = value_of_len(16, b'c');

    insert_raw(&keyspace, src.clone(), value.clone());
    insert_raw(&keyspace, dst.clone(), value.clone());
    configure_noeviction_at_current_usage(&keyspace);

    let writer_src = src.clone();
    let writer_dst = dst.clone();
    let stale_dst = dst.clone();
    let stale_filler = filler.clone();
    let stale_value = value.clone();
    let result = run_projection_race(
        "copy_key",
        Arc::clone(&keyspace),
        move |keyspace| keyspace.copy_key(&writer_src, writer_dst, true, 0),
        move |keyspace| {
            assert!(keyspace.remove_value(&stale_dst, 0).value.is_some());
            insert_raw(keyspace, stale_filler, stale_value);
        },
    );

    assert_oom(result);
    assert!(keyspace.get_value(&src, 0).is_some());
    assert!(keyspace.get_value(&dst, 0).is_none());
    assert!(keyspace.get_value(&filler, 0).is_some());
}
