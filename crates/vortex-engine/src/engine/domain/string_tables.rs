use super::mutation::*;
use super::*;

pub(super) type OwnedKeyValueBatch = SmallVec<[(VortexKey, VortexValue); 16]>;

#[derive(Clone, Copy)]
pub(super) struct PlannedWriteLookup {
    pub(super) pair_index: usize,
    pub(super) shard_idx: usize,
    pub(super) table_hash: TableHash,
    pub(super) guard_pos: usize,
}

pub(super) fn planned_write_lookups(
    plan: &PrehashedShardPlan<'_>,
) -> SmallVec<[PlannedWriteLookup; 16]> {
    plan.entries()
        .iter()
        .map(|entry| PlannedWriteLookup {
            pair_index: entry.key_index(),
            shard_idx: entry.shard_index(),
            table_hash: entry.table_hash(),
            guard_pos: entry.guard_index().get(),
        })
        .collect()
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

pub(super) const OPTIMISTIC_VALUE_MUTATION_RETRIES: usize = 2;

#[derive(Clone)]
enum ValueMutationSnapshotState {
    Missing,
    Live {
        ttl_deadline: u64,
        lsn_version: u64,
        value: VortexValue,
    },
}

#[derive(Clone)]
pub(super) struct ValueMutationSnapshot {
    state: ValueMutationSnapshotState,
}

impl ValueMutationSnapshot {
    #[inline]
    fn capture(
        table: &SwissTable,
        key_bytes: &[u8],
        hash: TableHash,
        now_nanos: u64,
    ) -> Option<Self> {
        match table.get_value_ttl_lsn_prehashed(key_bytes, hash) {
            Some((_, ttl_deadline, _)) if ttl_deadline != 0 && ttl_deadline <= now_nanos => None,
            Some((value, ttl_deadline, lsn_version)) => Some(Self {
                state: ValueMutationSnapshotState::Live {
                    ttl_deadline,
                    lsn_version,
                    value: value.clone(),
                },
            }),
            None => Some(Self {
                state: ValueMutationSnapshotState::Missing,
            }),
        }
    }

    #[inline]
    pub(super) fn revalidates(
        &self,
        table: &SwissTable,
        key_bytes: &[u8],
        hash: TableHash,
        now_nanos: u64,
    ) -> bool {
        match (
            &self.state,
            table.get_value_ttl_lsn_prehashed(key_bytes, hash),
        ) {
            (ValueMutationSnapshotState::Missing, None) => true,
            (
                ValueMutationSnapshotState::Live {
                    ttl_deadline,
                    lsn_version,
                    value,
                },
                Some((current, current_ttl, current_lsn)),
            ) => {
                current_ttl == *ttl_deadline
                    && current_lsn == *lsn_version
                    && (current_ttl == 0 || current_ttl > now_nanos)
                    && current == value
            }
            _ => false,
        }
    }

    #[inline]
    fn live_value(&self) -> Option<&VortexValue> {
        match &self.state {
            ValueMutationSnapshotState::Live { value, .. } => Some(value),
            ValueMutationSnapshotState::Missing => None,
        }
    }

    #[inline]
    pub(super) fn had_live_ttl(&self) -> bool {
        match &self.state {
            ValueMutationSnapshotState::Live { ttl_deadline, .. } => *ttl_deadline != 0,
            ValueMutationSnapshotState::Missing => false,
        }
    }

    #[inline]
    pub(super) fn ttl_deadline(&self) -> Option<u64> {
        match &self.state {
            ValueMutationSnapshotState::Live { ttl_deadline, .. } => Some(*ttl_deadline),
            ValueMutationSnapshotState::Missing => None,
        }
    }

    #[inline]
    pub(super) fn is_missing(&self) -> bool {
        matches!(&self.state, ValueMutationSnapshotState::Missing)
    }
}

pub(super) struct PreparedValueMutation {
    snapshot: ValueMutationSnapshot,
    table_hash: TableHash,
    projected_delta: PositiveDelta,
    value: VortexValue,
}

impl PreparedValueMutation {
    #[inline]
    fn new(
        key: &VortexKey,
        snapshot: ValueMutationSnapshot,
        table_hash: TableHash,
        value: VortexValue,
    ) -> Self {
        let projected_delta = projected_rewrite_delta(
            key,
            snapshot.live_value(),
            string_value_memory_usage(value.strlen()),
        );
        Self {
            snapshot,
            table_hash,
            projected_delta,
            value,
        }
    }

    #[inline]
    pub(super) fn snapshot(&self) -> &ValueMutationSnapshot {
        &self.snapshot
    }

    #[inline]
    pub(super) fn table_hash(&self) -> TableHash {
        self.table_hash
    }

    #[inline]
    pub(super) fn projected_delta(&self) -> PositiveDelta {
        self.projected_delta
    }

    #[inline]
    pub(super) fn into_value(self) -> VortexValue {
        self.value
    }
}

pub(super) struct PreparedSetRange {
    mutation: PreparedValueMutation,
    length: usize,
}

impl PreparedSetRange {
    #[inline]
    pub(super) fn length(&self) -> usize {
        self.length
    }

    #[inline]
    pub(super) fn into_mutation(self) -> PreparedValueMutation {
        self.mutation
    }
}

pub(super) struct PreparedIncrFloat {
    mutation: PreparedValueMutation,
    bytes: Bytes,
}

impl PreparedIncrFloat {
    #[inline]
    pub(super) fn mutation(&self) -> &PreparedValueMutation {
        &self.mutation
    }

    #[inline]
    pub(super) fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    #[inline]
    pub(super) fn into_mutation(self) -> PreparedValueMutation {
        self.mutation
    }
}

pub(super) fn prepare_setrange_value(
    table: &SwissTable,
    key: &VortexKey,
    table_hash: TableHash,
    offset: usize,
    new_bytes: &[u8],
    now_nanos: u64,
) -> Result<Option<PreparedSetRange>, MutationErrorKind> {
    let Some(snapshot) =
        ValueMutationSnapshot::capture(table, key.as_bytes(), table_hash, now_nanos)
    else {
        return Ok(None);
    };

    let required_len = offset + new_bytes.len();
    let mut data = match snapshot.live_value() {
        Some(VortexValue::InlineString(inline)) => inline.as_bytes().to_vec(),
        Some(VortexValue::String(bytes)) => bytes.to_vec(),
        Some(VortexValue::Integer(number)) => {
            let mut buffer = itoa::Buffer::new();
            buffer.format(*number).as_bytes().to_vec()
        }
        Some(_) => return Err(MutationErrorKind::WrongType),
        None => Vec::new(),
    };

    if data.len() < required_len {
        data.resize(required_len, 0);
    }
    data[offset..offset + new_bytes.len()].copy_from_slice(new_bytes);

    let length = data.len();
    Ok(Some(PreparedSetRange {
        mutation: PreparedValueMutation::new(key, snapshot, table_hash, owned_string_value(data)),
        length,
    }))
}

pub(super) fn prepare_increment_by_float(
    table: &SwissTable,
    key: &VortexKey,
    table_hash: TableHash,
    increment: f64,
    now_nanos: u64,
) -> Result<Option<PreparedIncrFloat>, MutationErrorKind> {
    let Some(snapshot) =
        ValueMutationSnapshot::capture(table, key.as_bytes(), table_hash, now_nanos)
    else {
        return Ok(None);
    };

    let current = match snapshot.live_value() {
        Some(VortexValue::Integer(number)) => *number as f64,
        Some(VortexValue::InlineString(inline)) => std::str::from_utf8(inline.as_bytes())
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or(MutationErrorKind::NotFloat)?,
        Some(VortexValue::String(bytes)) => std::str::from_utf8(bytes.as_ref())
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or(MutationErrorKind::NotFloat)?,
        Some(_) => return Err(MutationErrorKind::WrongType),
        None => 0.0,
    };

    let result = current + increment;
    if result.is_nan() || result.is_infinite() {
        return Err(MutationErrorKind::NotFloat);
    }

    let mut buffer = ryu::Buffer::new();
    let text = buffer.format(result);
    let bytes = Bytes::copy_from_slice(text.as_bytes());
    let value = VortexValue::from_bytes(bytes.as_ref());

    Ok(Some(PreparedIncrFloat {
        mutation: PreparedValueMutation::new(key, snapshot, table_hash, value),
        bytes,
    }))
}

fn owned_string_value(data: Vec<u8>) -> VortexValue {
    if data.len() <= vortex_common::MAX_INLINE_VALUE_LEN {
        VortexValue::InlineString(InlineBytes::from_slice(&data))
    } else {
        VortexValue::String(Bytes::from(data))
    }
}

pub(super) fn projected_increment_delta(
    table: &SwissTable,
    key: &VortexKey,
    delta: i64,
    now_nanos: u64,
) -> Result<PositiveDelta, MutationErrorKind> {
    match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
            let current = match value {
                VortexValue::Integer(number) => *number,
                VortexValue::InlineString(inline) => crate::commands::parse_i64(inline.as_bytes())
                    .ok_or(MutationErrorKind::NotInteger)?,
                VortexValue::String(bytes) => crate::commands::parse_i64(bytes.as_ref())
                    .ok_or(MutationErrorKind::NotInteger)?,
                _ => return Err(MutationErrorKind::WrongType),
            };
            let _ = current
                .checked_add(delta)
                .ok_or(MutationErrorKind::Overflow)?;
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
) -> Result<PositiveDelta, MutationErrorKind> {
    let current = match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => match value {
            VortexValue::Integer(number) => *number as f64,
            VortexValue::InlineString(inline) => std::str::from_utf8(inline.as_bytes())
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or(MutationErrorKind::NotFloat)?,
            VortexValue::String(bytes) => std::str::from_utf8(bytes.as_ref())
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or(MutationErrorKind::NotFloat)?,
            _ => return Err(MutationErrorKind::WrongType),
        },
        Some(_) | None => 0.0,
    };

    let result = current + increment;
    if result.is_nan() || result.is_infinite() {
        return Err(MutationErrorKind::NotFloat);
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
) -> Result<PositiveDelta, MutationErrorKind> {
    match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
            let current_len = match value {
                VortexValue::InlineString(_) | VortexValue::String(_) | VortexValue::Integer(_) => {
                    value.strlen()
                }
                _ => return Err(MutationErrorKind::WrongType),
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
) -> Result<PositiveDelta, MutationErrorKind> {
    let required_len = offset + new_bytes.len();
    match table.get_with_ttl(key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
            let current_len = match value {
                VortexValue::InlineString(_) | VortexValue::String(_) | VortexValue::Integer(_) => {
                    value.strlen()
                }
                _ => return Err(MutationErrorKind::WrongType),
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

pub(super) fn deduplicate_last_write_pairs(pairs: OwnedKeyValueBatch) -> OwnedKeyValueBatch {
    if pairs.len() <= 16 {
        let mut unique = OwnedKeyValueBatch::new();
        for (key, value) in pairs {
            if let Some((_, existing_value)) = unique
                .iter_mut()
                .find(|(existing_key, _)| existing_key == &key)
            {
                *existing_value = value;
            } else {
                unique.push((key, value));
            }
        }
        return unique;
    }

    let mut unique = OwnedKeyValueBatch::with_capacity(pairs.len());
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

pub(super) fn msetnx_all_absent_for_projection(
    guards: &mut ShardWriteGuards<'_>,
    plan: &PrehashedShardPlan<'_>,
    pairs: &[(VortexKey, VortexValue)],
    now_nanos: u64,
) -> bool {
    for lookup in plan.entries() {
        let guard_pos = lookup.guard_index().get();
        let table = &*guards[guard_pos].1;
        let key_bytes = pairs[lookup.key_index()].0.as_bytes();
        if matches!(
            table.get_with_ttl_prehashed(key_bytes, lookup.table_hash()),
            Some((_, ttl)) if ttl == 0 || ttl > now_nanos
        ) {
            return false;
        }
    }
    true
}

pub(super) fn msetnx_all_absent_read_plan(
    keyspace: &ConcurrentKeyspace,
    plan: &PrehashedShardPlan<'_>,
    now_nanos: u64,
) -> bool {
    let entries = plan.entries();
    let mut cursor = 0;
    while cursor < entries.len() {
        let shard_idx = entries[cursor].shard_index();
        let group_start = cursor;
        while cursor < entries.len() && entries[cursor].shard_index() == shard_idx {
            cursor += 1;
        }

        let guard = keyspace.read_shard_by_index(shard_idx);
        for lookup in &entries[group_start..cursor] {
            if matches!(
                guard.get_with_ttl_prehashed(lookup.key_bytes(), lookup.table_hash()),
                Some((_, ttl)) if ttl == 0 || ttl > now_nanos
            ) {
                return false;
            }
        }
    }
    true
}

pub(super) fn msetnx_cleanup_and_all_absent_locked(
    keyspace: &ConcurrentKeyspace,
    guards: &mut ShardWriteGuards<'_>,
    plan: &PrehashedShardPlan<'_>,
    pairs: &[(VortexKey, VortexValue)],
    now_nanos: u64,
    effects: &mut SmallVec<[OwnedDeferredEffects; 16]>,
) -> bool {
    for lookup in plan.entries() {
        let guard_pos = lookup.guard_index().get();
        let table = &mut *guards[guard_pos].1;
        let key_bytes = pairs[lookup.key_index()].0.as_bytes();
        if let Some(effect) = keyspace.cleanup_expired_key_bytes_owned(
            lookup.shard_index(),
            table,
            key_bytes,
            lookup.table_hash(),
            now_nanos,
        ) {
            effects.push(effect);
        }
        if table.contains_key_prehashed(key_bytes, lookup.table_hash()) {
            return false;
        }
    }
    true
}

pub(super) fn set_with_options_on_table<F>(
    table: &mut SwissTable,
    key: VortexKey,
    value: VortexValue,
    options: SetOptions,
    now_nanos: u64,
    mut allocate_lsn: F,
) -> Result<(SetResult, ExpiryTransition, Option<AofLsn>), LsnOverflow>
where
    F: FnMut() -> Result<(Option<u64>, Option<AofLsn>), LsnOverflow>,
{
    let table_hash = table.table_hash_key_bytes(key.as_bytes());
    match table.slot_cursor_prehashed(key.as_bytes(), table_hash, now_nanos) {
        SlotCursor::Live(live) => {
            if options.nx {
                let had_ttl = live.had_ttl();
                let existing_ttl = live.ttl_deadline();
                let previous = options.get.then(|| live.cloned_value());
                let result = if options.get {
                    SetResult::NotSetGet(previous)
                } else {
                    SetResult::NotSet
                };
                return Ok((
                    result,
                    ExpiryTransition::new(had_ttl, existing_ttl != 0),
                    None,
                ));
            }
            let (entry_lsn, aof_lsn) = allocate_lsn()?;
            Ok(set_live_with_options(
                live, value, options, entry_lsn, aof_lsn,
            ))
        }
        SlotCursor::Expired(expired) => {
            if !options.xx {
                let (entry_lsn, aof_lsn) = allocate_lsn()?;
                let had_ttl = expired
                    .remove()
                    .map(|removed| removed.old_had_ttl())
                    .unwrap_or(false);
                return Ok(set_absent_after_cursor_probe(
                    table, key, value, table_hash, options, had_ttl, entry_lsn, aof_lsn,
                ));
            }
            let had_ttl = expired
                .remove()
                .map(|removed| removed.old_had_ttl())
                .unwrap_or(false);
            Ok(set_absent_after_cursor_probe_noop(options, had_ttl))
        }
        SlotCursor::Vacant(vacant) => {
            if options.xx {
                return Ok(set_vacant_noop(options, false));
            }
            let (entry_lsn, aof_lsn) = allocate_lsn()?;
            Ok(set_vacant_with_options(
                vacant, key, value, options, false, entry_lsn, aof_lsn,
            ))
        }
    }
}

fn set_live_with_options(
    live: crate::table::LiveSlotCursor<'_>,
    value: VortexValue,
    options: SetOptions,
    entry_lsn: Option<u64>,
    aof_lsn: Option<AofLsn>,
) -> (SetResult, ExpiryTransition, Option<AofLsn>) {
    let existing_ttl = live.ttl_deadline();

    let effective_ttl = if options.keepttl {
        existing_ttl
    } else {
        options.ttl_deadline
    };
    let policy = if effective_ttl != 0 {
        MutationPolicy::set(effective_ttl, entry_lsn)
    } else {
        MutationPolicy::clear(entry_lsn)
    };
    let mut report = live.replace_value(value, policy);
    let transition = ExpiryTransition::new(report.old_had_ttl(), report.new_has_ttl());
    let result = if options.get {
        SetResult::OkGet(report.take_previous())
    } else {
        SetResult::Ok
    };
    (result, transition, aof_lsn)
}

fn set_vacant_with_options(
    vacant: crate::table::VacantSlot<'_>,
    key: VortexKey,
    value: VortexValue,
    options: SetOptions,
    had_ttl: bool,
    entry_lsn: Option<u64>,
    aof_lsn: Option<AofLsn>,
) -> (SetResult, ExpiryTransition, Option<AofLsn>) {
    let effective_ttl = if options.keepttl {
        0
    } else {
        options.ttl_deadline
    };
    let policy = if effective_ttl != 0 {
        MutationPolicy::set(effective_ttl, entry_lsn)
    } else {
        MutationPolicy::clear(entry_lsn)
    };
    let report = vacant.insert(key, value, policy);
    let has_ttl_after = report.new_has_ttl();
    let result = if options.get {
        SetResult::OkGet(None)
    } else {
        SetResult::Ok
    };
    (
        result,
        ExpiryTransition::new(had_ttl, has_ttl_after),
        aof_lsn,
    )
}

fn set_vacant_noop(
    options: SetOptions,
    had_ttl: bool,
) -> (SetResult, ExpiryTransition, Option<AofLsn>) {
    let result = if options.get {
        SetResult::NotSetGet(None)
    } else {
        SetResult::NotSet
    };
    (result, ExpiryTransition::remove(had_ttl), None)
}

fn set_absent_after_cursor_probe(
    table: &mut SwissTable,
    key: VortexKey,
    value: VortexValue,
    table_hash: TableHash,
    options: SetOptions,
    had_ttl: bool,
    entry_lsn: Option<u64>,
    aof_lsn: Option<AofLsn>,
) -> (SetResult, ExpiryTransition, Option<AofLsn>) {
    let effective_ttl = if options.keepttl {
        0
    } else {
        options.ttl_deadline
    };
    let policy = if effective_ttl != 0 {
        MutationPolicy::set(effective_ttl, entry_lsn)
    } else {
        MutationPolicy::clear(entry_lsn)
    };
    let result = if options.get {
        SetResult::OkGet(None)
    } else {
        SetResult::Ok
    };
    let _ = table.mutate_prehashed(key, value, table_hash, policy);
    (
        result,
        ExpiryTransition::new(had_ttl, effective_ttl != 0),
        aof_lsn,
    )
}

fn set_absent_after_cursor_probe_noop(
    options: SetOptions,
    had_ttl: bool,
) -> (SetResult, ExpiryTransition, Option<AofLsn>) {
    set_vacant_noop(options, had_ttl)
}

pub(super) fn increment_table_by(
    table: &mut SwissTable,
    key: VortexKey,
    delta: i64,
    lsn: Option<u64>,
    now_nanos: u64,
) -> Result<(i64, ExpiryTransition), MutationErrorKind> {
    let hash = table.table_hash_key_bytes(key.as_bytes());
    match table.slot_cursor_prehashed(key.as_bytes(), hash, now_nanos) {
        SlotCursor::Live(live) => {
            let current = match live.value() {
                VortexValue::Integer(number) => *number,
                VortexValue::InlineString(inline) => crate::commands::parse_i64(inline.as_bytes())
                    .ok_or(MutationErrorKind::NotInteger)?,
                VortexValue::String(bytes) => crate::commands::parse_i64(bytes.as_ref())
                    .ok_or(MutationErrorKind::NotInteger)?,
                _ => return Err(MutationErrorKind::WrongType),
            };

            let result = current
                .checked_add(delta)
                .ok_or(MutationErrorKind::Overflow)?;
            let report = live.replace_value(
                VortexValue::Integer(result),
                MutationPolicy::preserve_ttl(lsn),
            );
            Ok((
                result,
                ExpiryTransition::new(report.old_had_ttl(), report.new_has_ttl()),
            ))
        }
        SlotCursor::Expired(expired) => {
            let had_ttl = expired
                .remove()
                .map(|removed| removed.old_had_ttl())
                .unwrap_or(false);
            let _ = table.mutate_prehashed(
                key,
                VortexValue::Integer(delta),
                hash,
                MutationPolicy::clear(lsn),
            );
            Ok((delta, ExpiryTransition::remove(had_ttl)))
        }
        SlotCursor::Vacant(vacant) => {
            let _ = vacant.insert(key, VortexValue::Integer(delta), MutationPolicy::clear(lsn));
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
) -> Result<Bytes, MutationErrorKind> {
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
                        .ok_or(MutationErrorKind::NotFloat)?,
                    VortexValue::String(bytes) => std::str::from_utf8(bytes.as_ref())
                        .ok()
                        .and_then(|text| text.parse().ok())
                        .ok_or(MutationErrorKind::NotFloat)?,
                    _ => return Err(MutationErrorKind::WrongType),
                }
            }
        }
        None => 0.0,
    };

    let result = current + increment;
    if result.is_nan() || result.is_infinite() {
        return Err(MutationErrorKind::NotFloat);
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
) -> Result<(usize, ExpiryTransition), MutationErrorKind> {
    let hash = table.table_hash_key_bytes(key.as_bytes());
    match table.slot_cursor_prehashed(key.as_bytes(), hash, now_nanos) {
        SlotCursor::Live(live) => {
            let (length, new_value) = appended_value(live.value(), append_bytes)?;
            let report = live.replace_value(new_value, MutationPolicy::preserve_ttl(lsn));
            Ok((
                length,
                ExpiryTransition::new(report.old_had_ttl(), report.new_has_ttl()),
            ))
        }
        SlotCursor::Expired(expired) => {
            let had_ttl = expired
                .remove()
                .map(|removed| removed.old_had_ttl())
                .unwrap_or(false);
            let _ = table.mutate_prehashed(
                key,
                VortexValue::from_bytes(append_bytes),
                hash,
                MutationPolicy::clear(lsn),
            );
            Ok((append_bytes.len(), ExpiryTransition::remove(had_ttl)))
        }
        SlotCursor::Vacant(vacant) => {
            let _ = vacant.insert(
                key,
                VortexValue::from_bytes(append_bytes),
                MutationPolicy::clear(lsn),
            );
            Ok((append_bytes.len(), ExpiryTransition::default()))
        }
    }
}

fn appended_value(
    existing: &VortexValue,
    append_bytes: &[u8],
) -> Result<(usize, VortexValue), MutationErrorKind> {
    match existing {
        VortexValue::InlineString(inline) => {
            let new_len = inline.len() + append_bytes.len();
            let mut next_inline = inline.clone();
            if next_inline.try_extend(append_bytes) {
                Ok((new_len, VortexValue::InlineString(next_inline)))
            } else {
                let mut combined = Vec::with_capacity(new_len);
                combined.extend_from_slice(inline.as_bytes());
                combined.extend_from_slice(append_bytes);
                Ok((new_len, VortexValue::String(Bytes::from(combined))))
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
            Ok((new_value.strlen(), new_value))
        }
        VortexValue::Integer(number) => {
            let mut buffer = itoa::Buffer::new();
            let text = buffer.format(*number);
            let mut combined = Vec::with_capacity(text.len() + append_bytes.len());
            combined.extend_from_slice(text.as_bytes());
            combined.extend_from_slice(append_bytes);
            let new_value = VortexValue::from_bytes(&combined);
            Ok((new_value.strlen(), new_value))
        }
        _ => Err(MutationErrorKind::WrongType),
    }
}

/// Compute GETRANGE on a borrowed value. Extracted so both the legacy table
/// path and the double-checked-locking concurrent path can share the logic.
pub(super) fn getrange_from_value(
    value: &VortexValue,
    start: i64,
    end: i64,
) -> Result<Option<Bytes>, MutationErrorKind> {
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
        _ => Err(MutationErrorKind::WrongType),
    }
}

pub(super) fn setrange_in_table(
    table: &mut SwissTable,
    key: VortexKey,
    offset: usize,
    new_bytes: &[u8],
    lsn: Option<u64>,
    now_nanos: u64,
) -> Result<(usize, ExpiryTransition), MutationErrorKind> {
    let hash = table.table_hash_key_bytes(key.as_bytes());
    let (mut data, transition_seed, cursor) =
        match table.slot_cursor_prehashed(key.as_bytes(), hash, now_nanos) {
            SlotCursor::Live(live) => (setrange_data_from_value(live.value())?, None, Some(live)),
            SlotCursor::Expired(expired) => {
                let had_ttl = expired
                    .remove()
                    .map(|removed| removed.old_had_ttl())
                    .unwrap_or(false);
                (Vec::new(), Some(ExpiryTransition::remove(had_ttl)), None)
            }
            SlotCursor::Vacant(vacant) => {
                let required_len = offset + new_bytes.len();
                let mut data = vec![0; required_len];
                data[offset..offset + new_bytes.len()].copy_from_slice(new_bytes);
                let length = data.len();
                let _ = vacant.insert(
                    key,
                    VortexValue::from_bytes(&data),
                    MutationPolicy::clear(lsn),
                );
                return Ok((length, ExpiryTransition::default()));
            }
        };

    let required_len = offset + new_bytes.len();
    if data.len() < required_len {
        data.resize(required_len, 0);
    }
    data[offset..offset + new_bytes.len()].copy_from_slice(new_bytes);

    let length = data.len();
    let new_value = VortexValue::from_bytes(&data);
    if let Some(live) = cursor {
        let report = live.replace_value(new_value, MutationPolicy::preserve_ttl(lsn));
        return Ok((
            length,
            ExpiryTransition::new(report.old_had_ttl(), report.new_has_ttl()),
        ));
    }

    let _ = table.mutate_prehashed(key, new_value, hash, MutationPolicy::clear(lsn));
    Ok((length, transition_seed.unwrap_or_default()))
}

fn setrange_data_from_value(value: &VortexValue) -> Result<Vec<u8>, MutationErrorKind> {
    match value {
        VortexValue::InlineString(inline) => Ok(inline.as_bytes().to_vec()),
        VortexValue::String(bytes) => Ok(bytes.to_vec()),
        VortexValue::Integer(number) => {
            let mut buffer = itoa::Buffer::new();
            Ok(buffer.format(*number).as_bytes().to_vec())
        }
        _ => Err(MutationErrorKind::WrongType),
    }
}
