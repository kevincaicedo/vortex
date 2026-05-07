use super::mutation::*;
use super::*;

#[derive(Clone, Copy)]
pub(super) struct MultiWriteLookup {
    pub(super) pair_index: usize,
    pub(super) shard_idx: usize,
    pub(super) table_hash: TableHash,
}

pub(super) fn string_value_memory_usage(len: usize) -> usize {
    if len <= vortex_common::MAX_INLINE_VALUE_LEN {
        size_of::<InlineBytes>() + len
    } else {
        size_of::<Bytes>() + len
    }
}

#[inline]
pub(super) fn integer_value_memory_usage() -> usize {
    size_of::<i64>()
}

pub(super) fn projected_increment_delta(
    table: &SwissTable,
    key: &VortexKey,
    delta: i64,
    now_nanos: u64,
) -> Result<PositiveDelta, &'static [u8]> {
    match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
            let current = match value {
                VortexValue::Integer(number) => *number,
                VortexValue::InlineString(inline) => {
                    crate::commands::parse_i64(inline.as_bytes()).ok_or(ERR_NOT_INTEGER)?
                }
                VortexValue::String(bytes) => {
                    crate::commands::parse_i64(bytes.as_ref()).ok_or(ERR_NOT_INTEGER)?
                }
                _ => return Err(ERR_WRONG_TYPE),
            };
            let _ = current.checked_add(delta).ok_or(ERR_OVERFLOW)?;
            Ok(projected_rewrite_delta(
                key,
                Some(value),
                integer_value_memory_usage(),
            ))
        }
        Some(_) | None => Ok(PositiveDelta::from_bytes(entry_memory_usage(
            key,
            integer_value_memory_usage(),
        ))),
    }
}

pub(super) fn projected_increment_by_float_delta(
    table: &SwissTable,
    key: &VortexKey,
    increment: f64,
    now_nanos: u64,
) -> Result<PositiveDelta, &'static [u8]> {
    let current = match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => match value {
            VortexValue::Integer(number) => *number as f64,
            VortexValue::InlineString(inline) => std::str::from_utf8(inline.as_bytes())
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or(ERR_NOT_FLOAT)?,
            VortexValue::String(bytes) => std::str::from_utf8(bytes.as_ref())
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or(ERR_NOT_FLOAT)?,
            _ => return Err(ERR_WRONG_TYPE),
        },
        Some(_) | None => 0.0,
    };

    let result = current + increment;
    if result.is_nan() || result.is_infinite() {
        return Err(ERR_NOT_FLOAT);
    }

    let mut buffer = ryu::Buffer::new();
    let text = buffer.format(result);
    let existing = match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
        _ => None,
    };

    Ok(projected_rewrite_delta(
        key,
        existing,
        string_value_memory_usage(text.len()),
    ))
}

pub(super) fn projected_append_delta(
    table: &SwissTable,
    key: &VortexKey,
    append_bytes: &[u8],
    now_nanos: u64,
) -> Result<PositiveDelta, &'static [u8]> {
    match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
            let current_len = match value {
                VortexValue::InlineString(_) | VortexValue::String(_) | VortexValue::Integer(_) => {
                    value.strlen()
                }
                _ => return Err(ERR_WRONG_TYPE),
            };
            Ok(projected_rewrite_delta(
                key,
                Some(value),
                string_value_memory_usage(current_len + append_bytes.len()),
            ))
        }
        Some(_) | None => Ok(PositiveDelta::from_bytes(entry_memory_usage(
            key,
            string_value_memory_usage(append_bytes.len()),
        ))),
    }
}

pub(super) fn projected_setrange_delta(
    table: &SwissTable,
    key: &VortexKey,
    offset: usize,
    new_bytes: &[u8],
    now_nanos: u64,
) -> Result<PositiveDelta, &'static [u8]> {
    let required_len = offset + new_bytes.len();
    match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
            let current_len = match value {
                VortexValue::InlineString(_) | VortexValue::String(_) | VortexValue::Integer(_) => {
                    value.strlen()
                }
                _ => return Err(ERR_WRONG_TYPE),
            };
            Ok(projected_rewrite_delta(
                key,
                Some(value),
                string_value_memory_usage(current_len.max(required_len)),
            ))
        }
        Some(_) | None => Ok(PositiveDelta::from_bytes(entry_memory_usage(
            key,
            string_value_memory_usage(required_len),
        ))),
    }
}

pub(super) fn projected_set_write_delta(
    table: &SwissTable,
    key: &VortexKey,
    value: &VortexValue,
    options: SetOptions,
    now_nanos: u64,
) -> PositiveDelta {
    let exists = matches!(
        table.get_with_ttl(key),
        Some((_, ttl)) if ttl == 0 || ttl > now_nanos
    );

    if options.nx && exists {
        return PositiveDelta::zero();
    }
    if options.xx && !exists {
        return PositiveDelta::zero();
    }

    positive_delta(table.projected_insert_delta(key, value))
}

pub(super) fn deduplicate_last_write_pairs(
    pairs: Vec<(VortexKey, VortexValue)>,
) -> Vec<(VortexKey, VortexValue)> {
    let mut unique: Vec<(VortexKey, VortexValue)> = Vec::with_capacity(pairs.len());
    let mut positions: HashMap<VortexKey, usize> = HashMap::with_capacity(pairs.len());

    for (key, value) in pairs {
        if let Some(&position) = positions.get(&key) {
            unique[position].1 = value;
            continue;
        }

        positions.insert(key.clone(), unique.len());
        unique.push((key, value));
    }

    unique
}

pub(super) fn build_multi_write_lookups<'a>(
    keyspace: &ConcurrentKeyspace,
    pairs: &'a [(VortexKey, VortexValue)],
) -> (SmallVec<[&'a [u8]; 16]>, SmallVec<[MultiWriteLookup; 16]>) {
    let mut key_refs: SmallVec<[&[u8]; 16]> = SmallVec::with_capacity(pairs.len());
    let mut lookups: SmallVec<[MultiWriteLookup; 16]> = SmallVec::with_capacity(pairs.len());

    for (pair_index, (key, _)) in pairs.iter().enumerate() {
        let key_bytes = key.as_bytes();
        key_refs.push(key_bytes);
        lookups.push(MultiWriteLookup {
            pair_index,
            shard_idx: keyspace.shard_index(key_bytes),
            table_hash: keyspace.table_hash_key(key_bytes),
        });
    }

    lookups.sort_unstable_by_key(|lookup| (lookup.shard_idx, lookup.pair_index));

    (key_refs, lookups)
}

pub(super) fn msetnx_all_absent_locked(
    keyspace: &ConcurrentKeyspace,
    guards: &mut ShardWriteGuards<'_>,
    plan: &ShardPlan,
    pairs: &[(VortexKey, VortexValue)],
    lookups: &[MultiWriteLookup],
    now_nanos: u64,
) -> bool {
    for lookup in lookups {
        let guard_pos = plan.guard_index_for_key(lookup.pair_index).get();
        let table = &mut *guards[guard_pos].1;
        let key_bytes = pairs[lookup.pair_index].0.as_bytes();
        let _ = keyspace.cleanup_expired_prehashed(
            lookup.shard_idx,
            table,
            key_bytes,
            lookup.table_hash,
            now_nanos,
        );
        if table.contains_key_prehashed(key_bytes, lookup.table_hash) {
            return false;
        }
    }
    true
}

pub(super) fn set_with_options_on_table(
    table: &mut SwissTable,
    key: VortexKey,
    value: VortexValue,
    options: SetOptions,
    now_nanos: u64,
) -> (SetResult, ExpiryTransition) {
    let had_ttl = ttl_present(table.get_entry_ttl(&key));
    let _ = remove_if_expired(table, &key, now_nanos);

    // Plain SET is the hot path in the benchmark workload. It does not need
    // an existence probe, previous value clone, or TTL carry-forward.
    if !options.nx && !options.xx && !options.get && !options.keepttl {
        if options.ttl_deadline != 0 {
            table.insert_with(key, value, options.ttl_deadline, None);
        } else {
            table.insert(key, value);
        }
        return (
            SetResult::Ok,
            ExpiryTransition::new(had_ttl, options.ttl_deadline != 0),
        );
    }

    let mut existing_for_notset_get = None;
    let mut existing_ttl = 0u64;
    let exists = match table.get_with_ttl(&key) {
        Some((current, ttl)) if ttl == 0 || ttl > now_nanos => {
            existing_ttl = ttl;
            if options.get && (options.nx || options.xx) {
                existing_for_notset_get = Some(current.clone());
            }
            true
        }
        _ => false,
    };

    if options.nx && exists {
        return if options.get {
            (
                SetResult::NotSetGet(existing_for_notset_get),
                ExpiryTransition::new(had_ttl, existing_ttl != 0),
            )
        } else {
            (
                SetResult::NotSet,
                ExpiryTransition::new(had_ttl, existing_ttl != 0),
            )
        };
    }

    if options.xx && !exists {
        return if options.get {
            (
                SetResult::NotSetGet(None),
                ExpiryTransition::remove(had_ttl),
            )
        } else {
            (SetResult::NotSet, ExpiryTransition::remove(had_ttl))
        };
    }

    let effective_ttl = if options.keepttl {
        existing_ttl
    } else {
        options.ttl_deadline
    };

    let previous = if effective_ttl != 0 {
        table.insert_with(key, value, effective_ttl, None)
    } else {
        table.insert(key, value)
    };

    if options.get {
        (
            SetResult::OkGet(previous),
            ExpiryTransition::new(had_ttl, effective_ttl != 0),
        )
    } else {
        (
            SetResult::Ok,
            ExpiryTransition::new(had_ttl, effective_ttl != 0),
        )
    }
}

pub(super) fn increment_table_by(
    table: &mut SwissTable,
    key: VortexKey,
    delta: i64,
    lsn: Option<u64>,
    now_nanos: u64,
) -> Result<(i64, ExpiryTransition), &'static [u8]> {
    match table.get_with_ttl(&key) {
        Some((value, ttl)) => {
            let had_ttl = ttl != 0;
            if had_ttl && ttl <= now_nanos {
                table.remove(&key);
                let hash = table.table_hash_key_bytes(key.as_bytes());
                let _ = table.mutate_prehashed(
                    key,
                    VortexValue::Integer(delta),
                    hash,
                    MutationPolicy::clear(lsn),
                );
                return Ok((delta, ExpiryTransition::ttl_removed()));
            }

            let current = match value {
                VortexValue::Integer(number) => *number,
                VortexValue::InlineString(inline) => {
                    crate::commands::parse_i64(inline.as_bytes()).ok_or(ERR_NOT_INTEGER)?
                }
                VortexValue::String(bytes) => {
                    crate::commands::parse_i64(bytes.as_ref()).ok_or(ERR_NOT_INTEGER)?
                }
                _ => return Err(ERR_WRONG_TYPE),
            };

            let result = current.checked_add(delta).ok_or(ERR_OVERFLOW)?;
            let hash = table.table_hash_key_bytes(key.as_bytes());
            table
                .replace_prehashed(
                    key.as_bytes(),
                    VortexValue::Integer(result),
                    hash,
                    MutationPolicy::preserve_ttl(lsn),
                )
                .expect("live key must exist while replacing increment result");
            Ok((result, ExpiryTransition::new(had_ttl, had_ttl)))
        }
        None => {
            let hash = table.table_hash_key_bytes(key.as_bytes());
            let _ = table.mutate_prehashed(
                key,
                VortexValue::Integer(delta),
                hash,
                MutationPolicy::clear(lsn),
            );
            Ok((delta, ExpiryTransition::default()))
        }
    }
}

pub(super) fn increment_table_by_float(
    table: &mut SwissTable,
    key: VortexKey,
    increment: f64,
    lsn: Option<u64>,
    now_nanos: u64,
) -> Result<Bytes, &'static [u8]> {
    let current = match table.get_with_ttl(&key) {
        Some((value, ttl)) => {
            if ttl != 0 && ttl <= now_nanos {
                table.remove(&key);
                0.0
            } else {
                match value {
                    VortexValue::Integer(number) => *number as f64,
                    VortexValue::InlineString(inline) => std::str::from_utf8(inline.as_bytes())
                        .ok()
                        .and_then(|text| text.parse().ok())
                        .ok_or(ERR_NOT_FLOAT)?,
                    VortexValue::String(bytes) => std::str::from_utf8(bytes.as_ref())
                        .ok()
                        .and_then(|text| text.parse().ok())
                        .ok_or(ERR_NOT_FLOAT)?,
                    _ => return Err(ERR_WRONG_TYPE),
                }
            }
        }
        None => 0.0,
    };

    let result = current + increment;
    if result.is_nan() || result.is_infinite() {
        return Err(ERR_NOT_FLOAT);
    }

    let mut buffer = ryu::Buffer::new();
    let text = buffer.format(result);
    let bytes = Bytes::copy_from_slice(text.as_bytes());
    let _ = table.insert_with_lsn(key, VortexValue::from_bytes(bytes.as_ref()), lsn);
    Ok(bytes)
}

pub(super) fn append_to_table(
    table: &mut SwissTable,
    key: VortexKey,
    append_bytes: &[u8],
    lsn: Option<u64>,
    now_nanos: u64,
) -> Result<usize, &'static [u8]> {
    let _ = remove_if_expired(table, &key, now_nanos);

    let hash = table.table_hash_key_bytes(key.as_bytes());
    if !table.contains_key_prehashed(key.as_bytes(), hash) {
        table.insert_new_prehashed(key, VortexValue::from_bytes(append_bytes), hash, lsn);
        return Ok(append_bytes.len());
    }

    let (length, new_value) = {
        let existing = table
            .get(&key)
            .expect("key must exist after contains_key_prehashed");
        match existing {
            VortexValue::InlineString(inline) => {
                let new_len = inline.len() + append_bytes.len();
                let mut next_inline = inline.clone();
                if next_inline.try_extend(append_bytes) {
                    (new_len, VortexValue::InlineString(next_inline))
                } else {
                    let mut combined = Vec::with_capacity(new_len);
                    combined.extend_from_slice(inline.as_bytes());
                    combined.extend_from_slice(append_bytes);
                    (new_len, VortexValue::String(Bytes::from(combined)))
                }
            }
            VortexValue::String(bytes) => {
                let new_len = bytes.len() + append_bytes.len();
                let mut combined = Vec::with_capacity(new_len);
                combined.extend_from_slice(bytes.as_ref());
                combined.extend_from_slice(append_bytes);
                let new_value = if new_len <= vortex_common::MAX_INLINE_VALUE_LEN {
                    VortexValue::InlineString(InlineBytes::from_slice(&combined))
                } else {
                    VortexValue::String(Bytes::from(combined))
                };
                let length = match &new_value {
                    VortexValue::InlineString(inline) => inline.len(),
                    VortexValue::String(bytes) => bytes.len(),
                    _ => 0,
                };
                (length, new_value)
            }
            VortexValue::Integer(number) => {
                let mut buffer = itoa::Buffer::new();
                let text = buffer.format(*number);
                let mut combined = Vec::with_capacity(text.len() + append_bytes.len());
                combined.extend_from_slice(text.as_bytes());
                combined.extend_from_slice(append_bytes);
                let new_value = VortexValue::from_bytes(&combined);
                let length = new_value.strlen();
                (length, new_value)
            }
            _ => return Err(ERR_WRONG_TYPE),
        }
    };
    table
        .replace_prehashed(
            key.as_bytes(),
            new_value,
            hash,
            MutationPolicy::preserve_ttl(lsn),
        )
        .expect("live key must exist while replacing append result");
    Ok(length)
}

/// Compute GETRANGE on a borrowed value. Extracted so both the legacy table
/// path and the double-checked-locking concurrent path can share the logic.
pub(super) fn getrange_from_value(
    value: &VortexValue,
    start: i64,
    end: i64,
) -> Result<Option<Bytes>, &'static [u8]> {
    fn range_bounds(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
        if len == 0 {
            return None;
        }

        let len_i64 = len as i64;
        let start_index = if start < 0 {
            (len_i64 + start).max(0) as usize
        } else {
            start.min(len_i64) as usize
        };
        let end_index = if end < 0 {
            (len_i64 + end).max(0) as usize
        } else {
            end.min(len_i64 - 1).max(0) as usize
        };

        if start_index > end_index || start_index >= len {
            None
        } else {
            Some((start_index, end_index.min(len - 1)))
        }
    }

    match value {
        VortexValue::InlineString(inline) => Ok(match range_bounds(inline.len(), start, end) {
            Some((start_index, end_index)) => Some(Bytes::copy_from_slice(
                &inline.as_bytes()[start_index..=end_index],
            )),
            None => Some(Bytes::new()),
        }),
        VortexValue::String(bytes) => Ok(match range_bounds(bytes.len(), start, end) {
            Some((start_index, end_index)) => Some(bytes.slice(start_index..=end_index)),
            None => Some(Bytes::new()),
        }),
        VortexValue::Integer(number) => {
            let mut buffer = itoa::Buffer::new();
            let text = buffer.format(*number);
            Ok(match range_bounds(text.len(), start, end) {
                Some((start_index, end_index)) => Some(Bytes::copy_from_slice(
                    &text.as_bytes()[start_index..=end_index],
                )),
                None => Some(Bytes::new()),
            })
        }
        _ => Err(ERR_WRONG_TYPE),
    }
}

pub(super) fn setrange_in_table(
    table: &mut SwissTable,
    key: VortexKey,
    offset: usize,
    new_bytes: &[u8],
    lsn: Option<u64>,
    now_nanos: u64,
) -> Result<usize, &'static [u8]> {
    let _ = remove_if_expired(table, &key, now_nanos);

    let mut data = match table.get(&key) {
        Some(value) => match value {
            VortexValue::InlineString(inline) => inline.as_bytes().to_vec(),
            VortexValue::String(bytes) => bytes.to_vec(),
            VortexValue::Integer(number) => {
                let mut buffer = itoa::Buffer::new();
                buffer.format(*number).as_bytes().to_vec()
            }
            _ => return Err(ERR_WRONG_TYPE),
        },
        None => Vec::new(),
    };

    let required_len = offset + new_bytes.len();
    if data.len() < required_len {
        data.resize(required_len, 0);
    }
    data[offset..offset + new_bytes.len()].copy_from_slice(new_bytes);

    let length = data.len();
    let _ = table.insert_with_lsn(key, VortexValue::from_bytes(&data), lsn);
    Ok(length)
}
