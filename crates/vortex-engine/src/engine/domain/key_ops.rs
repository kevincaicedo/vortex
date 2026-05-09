use super::mutation::*;
use super::*;

fn projected_copy_delta(
    source_value: Option<&VortexValue>,
    dst_table: &SwissTable,
    dst: &VortexKey,
    replace: bool,
    now_nanos: u64,
) -> PositiveDelta {
    let Some(source_value) = source_value else {
        return PositiveDelta::zero();
    };

    let existing = match dst_table.get_with_ttl(dst) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
        _ => None,
    };
    if !replace && existing.is_some() {
        return PositiveDelta::zero();
    }

    projected_rewrite_delta(dst, existing, source_value.memory_usage())
}

fn projected_rename_delta(
    src_table: &SwissTable,
    old_key: &VortexKey,
    dst_table: &SwissTable,
    new_key: &VortexKey,
    now_nanos: u64,
    nx: bool,
) -> Result<PositiveDelta, MutationErrorKind> {
    if old_key == new_key {
        return Ok(PositiveDelta::zero());
    }

    let Some((source_value, source_ttl)) = src_table.get_with_ttl(old_key) else {
        return Err(MutationErrorKind::NoSuchKey);
    };
    if source_ttl != 0 && source_ttl <= now_nanos {
        return Err(MutationErrorKind::NoSuchKey);
    }

    let existing_dst = match dst_table.get_with_ttl(new_key) {
        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
        _ => None,
    };
    if nx && existing_dst.is_some() {
        return Ok(PositiveDelta::zero());
    }

    let moved_usage = entry_memory_usage(new_key, source_value.memory_usage());
    let removed_source_usage = entry_memory_usage(old_key, source_value.memory_usage());
    let removed_dst_usage = existing_dst
        .map(|value| entry_memory_usage(new_key, value.memory_usage()))
        .unwrap_or(0);

    Ok(positive_delta(
        moved_usage as isize - removed_source_usage as isize - removed_dst_usage as isize,
    ))
}

fn distinct_guard_tables_mut<'g>(
    guards: &'g mut ShardWriteGuards<'_>,
    first_position: usize,
    second_position: usize,
) -> (&'g mut SwissTable, &'g mut SwissTable) {
    debug_assert_ne!(
        first_position, second_position,
        "same-shard operations must use the single-shard path"
    );

    if first_position < second_position {
        let (left, right) = guards.split_at_mut(second_position);
        (&mut *left[first_position].1, &mut *right[0].1)
    } else {
        let (left, right) = guards.split_at_mut(first_position);
        (&mut *right[0].1, &mut *left[second_position].1)
    }
}

fn take_live_value(
    table: &mut SwissTable,
    key: &VortexKey,
    now_nanos: u64,
) -> (Option<VortexValue>, ExpiryTransition) {
    let hash = table.table_hash_key_bytes(key.as_bytes());
    match table.slot_cursor_prehashed(key.as_bytes(), hash, now_nanos) {
        SlotCursor::Live(live) => match live.remove() {
            Some(removed) => {
                let transition = ExpiryTransition::remove(removed.old_had_ttl());
                (Some(removed.into_value()), transition)
            }
            None => (None, ExpiryTransition::default()),
        },
        SlotCursor::Expired(expired) => match expired.remove() {
            Some(removed) => (None, ExpiryTransition::remove(removed.old_had_ttl())),
            None => (None, ExpiryTransition::default()),
        },
        SlotCursor::Vacant(_) => (None, ExpiryTransition::default()),
    }
}

fn delete_live_key_bytes(
    table: &mut SwissTable,
    key_bytes: &[u8],
    hash: TableHash,
    now_nanos: u64,
) -> (bool, ExpiryTransition) {
    match table.slot_cursor_prehashed(key_bytes, hash, now_nanos) {
        SlotCursor::Live(live) => match live.remove() {
            Some(removed) => (true, ExpiryTransition::remove(removed.old_had_ttl())),
            None => (false, ExpiryTransition::default()),
        },
        SlotCursor::Expired(expired) => match expired.remove() {
            Some(removed) => (false, ExpiryTransition::remove(removed.old_had_ttl())),
            None => (false, ExpiryTransition::default()),
        },
        SlotCursor::Vacant(_) => (false, ExpiryTransition::default()),
    }
}

fn rename_within_table(
    table: &mut SwissTable,
    old_key: &VortexKey,
    new_key: VortexKey,
    now_nanos: u64,
    nx: bool,
) -> Result<bool, MutationErrorKind> {
    let _ = remove_if_expired(table, old_key, now_nanos);
    if !table.contains_key(old_key) {
        return Err(MutationErrorKind::NoSuchKey);
    }

    if nx && old_key == &new_key {
        return Ok(false);
    }
    if old_key == &new_key {
        return Ok(true);
    }

    let _ = remove_if_expired(table, &new_key, now_nanos);
    if nx && table.contains_key(&new_key) {
        return Ok(false);
    }

    let (value, ttl) = table
        .remove_with_ttl(old_key)
        .expect("source key must exist after contains_key check");
    table.remove(&new_key);
    if ttl != 0 && ttl > now_nanos {
        table.insert_with(new_key, value, ttl, None);
    } else {
        table.insert(new_key, value);
    }
    Ok(true)
}

fn copy_within_table(
    table: &mut SwissTable,
    src: &VortexKey,
    dst: VortexKey,
    replace: bool,
    now_nanos: u64,
) -> bool {
    let _ = remove_if_expired(table, src, now_nanos);
    let (value_clone, ttl) = {
        let Some((value, ttl)) = table.get_with_ttl(src) else {
            return false;
        };
        if ttl != 0 && ttl <= now_nanos {
            return false;
        }
        (value.clone(), ttl)
    };

    let _ = remove_if_expired(table, &dst, now_nanos);
    if !replace && table.contains_key(&dst) {
        return false;
    }

    if ttl != 0 && ttl > now_nanos {
        table.insert_with(dst, value_clone, ttl, None);
    } else {
        table.insert(dst, value_clone);
    }
    true
}

impl ConcurrentKeyspace {
    pub(crate) fn remove_value(
        &self,
        key: &VortexKey,
        now_nanos: u64,
    ) -> MutationOutcome<Option<VortexValue>> {
        let shard_index = self.shard_index(key.as_bytes());
        let mut guard = self.write_shard_by_index(shard_index);
        let (removed, transition) = take_live_value(&mut guard, key, now_nanos);
        let changed = removed.is_some();
        let publish_features = self.mutation_features();
        let aof_lsn = changed.then(|| self.next_aof_lsn()).flatten();
        let effects = MutationEffects::none()
            .with_ttl(shard_index, transition)
            .with_watch_key_if(changed && publish_features.watch(), key)
            .with_aof_lsn(aof_lsn)
            .defer();
        drop(guard);
        let aof_lsn = self.publish_deferred_effects(effects).into_lsn();
        MutationOutcome::new(removed, aof_lsn)
    }

    pub(crate) fn delete_key_bytes(
        &self,
        key_bytes: &[u8],
        now_nanos: u64,
    ) -> MutationOutcome<bool> {
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let mut guard = self.write_shard_by_index(shard_index);
        let (deleted, transition) =
            delete_live_key_bytes(&mut guard, key_bytes, table_hash, now_nanos);
        let publish_features = self.mutation_features();
        let aof_lsn = deleted.then(|| self.next_aof_lsn()).flatten();
        let mut effects = MutationEffects::none()
            .with_ttl(shard_index, transition)
            .with_aof_lsn(aof_lsn);
        if deleted && publish_features.watch() {
            effects = effects.with_watch_key_bytes(key_bytes);
        }
        let effects = effects.defer();
        drop(guard);
        let aof_lsn = self.publish_deferred_effects(effects).into_lsn();
        MutationOutcome::new(deleted, aof_lsn)
    }

    pub(crate) fn delete_key_bytes_batch(
        &self,
        keys: &[&[u8]],
        now_nanos: u64,
    ) -> MutationOutcome<i64> {
        if keys.is_empty() {
            return MutationOutcome::new(0, None);
        }
        if keys.len() == 1 {
            let outcome = self.delete_key_bytes(keys[0], now_nanos);
            let deleted = outcome.value;
            return outcome.map_value(i64::from(deleted));
        }

        let plan = self.prehashed_plan(keys.iter().copied().enumerate());
        let mut guards = self.multi_write_prehashed(&plan);
        let mut deleted = 0i64;
        let publish_features = self.mutation_features();
        let mut effects: SmallVec<[DeferredEffects<'_>; 16]> = SmallVec::with_capacity(plan.len());

        for lookup in plan.entries() {
            let table = &mut *guards[lookup.guard_index().get()].1;
            let (removed, transition) =
                delete_live_key_bytes(table, lookup.key_bytes(), lookup.table_hash(), now_nanos);
            if removed {
                deleted += 1;
            }
            let mut effect = MutationEffects::none().with_ttl(lookup.shard_index(), transition);
            if removed && publish_features.watch() {
                effect = effect.with_watch_key_bytes(lookup.key_bytes());
            }
            effects.push(effect.defer());
        }

        drop(guards);
        for effect in effects {
            self.publish_deferred_effects(effect);
        }
        MutationOutcome::new(
            deleted,
            (deleted != 0).then(|| self.next_aof_lsn()).flatten(),
        )
    }

    pub(crate) fn count_existing_key_bytes(&self, keys: &[&[u8]], now_nanos: u64) -> i64 {
        if keys.is_empty() {
            return 0;
        }

        if keys.len() == 1 {
            let key_bytes = keys[0];
            let shard_index = self.shard_index(key_bytes);
            let table_hash = self.table_hash_key(key_bytes);
            let guard = self.read_shard_by_index(shard_index);
            return match guard.get_with_ttl_prehashed(key_bytes, table_hash) {
                Some((_, ttl)) if ttl == 0 || ttl > now_nanos => {
                    self.record_access_prehashed(&guard, key_bytes, table_hash);
                    1
                }
                Some(_) => {
                    drop(guard);
                    let mut wguard = self.write_shard_by_index(shard_index);
                    let effects = self.cleanup_expired_prehashed(
                        shard_index,
                        &mut wguard,
                        key_bytes,
                        table_hash,
                        now_nanos,
                    );
                    drop(wguard);
                    self.publish_optional_deferred_effects(effects);
                    0
                }
                None => 0,
            };
        }

        let plan = self.prehashed_plan(keys.iter().copied().enumerate());
        let guards = self.multi_read_prehashed(&plan);
        let mut count = 0i64;
        let mut expired: SmallVec<[PrehashedKeyPlan<'_>; 8]> = SmallVec::new();

        for lookup in plan.entries() {
            let guard = &guards[lookup.guard_index().get()].1;
            match guard.get_with_ttl_prehashed(lookup.key_bytes(), lookup.table_hash()) {
                Some((_, ttl)) if ttl == 0 || ttl > now_nanos => {
                    self.record_access_prehashed(guard, lookup.key_bytes(), lookup.table_hash());
                    count += 1;
                }
                Some(_) => expired.push(*lookup),
                None => {}
            }
        }

        drop(guards);

        if !expired.is_empty() {
            let mut cleanup_effects: SmallVec<[DeferredEffects<'_>; 8]> =
                SmallVec::with_capacity(expired.len());
            for lookup in expired {
                let mut wguard = self.write_shard_by_index(lookup.shard_index());
                if let Some(effect) = self.cleanup_expired_prehashed(
                    lookup.shard_index(),
                    &mut wguard,
                    lookup.key_bytes(),
                    lookup.table_hash(),
                    now_nanos,
                ) {
                    cleanup_effects.push(effect);
                }
                drop(wguard);
            }
            for effect in cleanup_effects {
                self.publish_deferred_effects(effect);
            }
        }

        count
    }

    pub(crate) fn expire_key_with_options(
        &self,
        key: &VortexKey,
        deadline_nanos: u64,
        now_nanos: u64,
        options: ExpireOptions,
    ) -> MutationOutcome<bool> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let mut guard = self.write_shard_by_index(shard_index);

        let mut live = match guard.slot_cursor_prehashed(key_bytes, table_hash, now_nanos) {
            SlotCursor::Live(live) => live,
            SlotCursor::Expired(expired) => {
                let removed = expired.remove();
                let effects = removed.map(|removal| {
                    MutationEffects::none()
                        .with_ttl(shard_index, ExpiryTransition::remove(removal.old_had_ttl()))
                        .with_watch_key(key)
                        .defer()
                });
                drop(guard);
                self.publish_optional_deferred_effects(effects);
                return MutationOutcome::new(false, None);
            }
            SlotCursor::Vacant(_) => return MutationOutcome::new(false, None),
        };

        let current_ttl = live.ttl_deadline();
        if !options.permits(current_ttl, deadline_nanos) {
            return MutationOutcome::new(false, None);
        }

        if deadline_nanos <= now_nanos {
            let removed = live.remove();
            let changed = removed.is_some();
            let publish_features = self.mutation_features();
            let aof_lsn = changed.then(|| self.next_aof_lsn()).flatten();
            let effects = MutationEffects::none()
                .with_ttl(
                    shard_index,
                    removed
                        .as_ref()
                        .map_or(ExpiryTransition::default(), |removed| {
                            ExpiryTransition::remove(removed.old_had_ttl())
                        }),
                )
                .with_watch_key_if(changed && publish_features.watch(), key)
                .with_aof_lsn(aof_lsn)
                .defer();
            drop(guard);
            let aof_lsn = self.publish_deferred_effects(effects).into_lsn();
            return MutationOutcome::new(changed, aof_lsn);
        }

        let publish_features = self.mutation_features();
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(publish_features);
        let report = live.set_ttl(deadline_nanos, entry_lsn);
        let effects = MutationEffects::none()
            .with_ttl(
                shard_index,
                ExpiryTransition::new(report.old_had_ttl(), report.new_has_ttl()),
            )
            .with_watch_key_if(publish_features.watch(), key)
            .with_aof_lsn(aof_lsn)
            .defer();
        drop(guard);
        let aof_lsn = self.publish_deferred_effects(effects).into_lsn();
        MutationOutcome::new(true, aof_lsn)
    }

    pub(crate) fn persist_key(&self, key: &VortexKey, now_nanos: u64) -> MutationOutcome<bool> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let mut guard = self.write_shard_by_index(shard_index);

        let mut live = match guard.slot_cursor_prehashed(key_bytes, table_hash, now_nanos) {
            SlotCursor::Live(live) => live,
            SlotCursor::Expired(expired) => {
                let removed = expired.remove();
                debug_assert!(removed.is_some(), "expired PERSIST key must be removable");
                let publish_features = self.mutation_features();
                let effects = MutationEffects::none()
                    .with_ttl(
                        shard_index,
                        removed
                            .as_ref()
                            .map_or(ExpiryTransition::default(), |removed| {
                                ExpiryTransition::remove(removed.old_had_ttl())
                            }),
                    )
                    .with_watch_key_if(publish_features.watch(), key)
                    .defer();
                drop(guard);
                self.publish_deferred_effects(effects);
                return MutationOutcome::new(false, None);
            }
            SlotCursor::Vacant(_) => return MutationOutcome::new(false, None),
        };

        if !live.had_ttl() {
            return MutationOutcome::new(false, None);
        }

        let publish_features = self.mutation_features();
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(publish_features);
        let report = live.clear_ttl(entry_lsn);
        let effects = MutationEffects::none()
            .with_ttl(
                shard_index,
                ExpiryTransition::new(report.old_had_ttl(), report.new_has_ttl()),
            )
            .with_watch_key_if(publish_features.watch(), key)
            .with_aof_lsn(aof_lsn)
            .defer();
        drop(guard);
        let aof_lsn = self.publish_deferred_effects(effects).into_lsn();
        MutationOutcome::new(true, aof_lsn)
    }

    pub(crate) fn ttl_state_bytes(&self, key_bytes: &[u8], now_nanos: u64) -> TtlState {
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl_prehashed(key_bytes, table_hash) {
            None => TtlState::Missing,
            Some((_, 0)) => {
                self.record_access_prehashed(&guard, key_bytes, table_hash);
                TtlState::Persistent
            }
            Some((_, deadline)) if deadline <= now_nanos => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                let effects = self.cleanup_expired_prehashed(
                    shard_index,
                    &mut wguard,
                    key_bytes,
                    table_hash,
                    now_nanos,
                );
                drop(wguard);
                self.publish_optional_deferred_effects(effects);
                TtlState::Missing
            }
            Some((_, deadline)) => {
                self.record_access_prehashed(&guard, key_bytes, table_hash);
                TtlState::Deadline(deadline)
            }
        }
    }

    pub(crate) fn type_of_key(&self, key: &VortexKey, now_nanos: u64) -> Option<&'static str> {
        let shard_index = self.shard_index(key.as_bytes());
        let table_hash = self.table_hash_key(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                self.record_access_prehashed(&guard, key.as_bytes(), table_hash);
                Some(value.type_name())
            }
            Some(_) => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                let effects = self.cleanup_expired_key(shard_index, &mut wguard, key, now_nanos);
                drop(wguard);
                self.publish_optional_deferred_effects(effects);
                None
            }
            None => None,
        }
    }

    pub(crate) fn rename_key(
        &self,
        old_key: &VortexKey,
        new_key: VortexKey,
        now_nanos: u64,
        nx: bool,
    ) -> MutationResult<bool> {
        let source_shard = self.shard_index(old_key.as_bytes());
        let destination_shard = self.shard_index(new_key.as_bytes());
        let new_hash = self.table_hash_key(new_key.as_bytes());

        if source_shard == destination_shard {
            let destination = new_key.clone();
            let eviction = self.eviction_config();
            let read_guard = self.read_shard_by_index(source_shard);
            let projected_delta = projected_rename_delta(
                &read_guard,
                old_key,
                &read_guard,
                &destination,
                now_nanos,
                nx,
            )?;
            drop(read_guard);
            let coordinator = ReservationCoordinator::new(self, now_nanos);
            let state = coordinator.reserve(destination_shard, projected_delta, eviction)?;
            let (mut guard, state) = coordinator.acquire_single_shard(
                source_shard,
                state,
                "rename_key",
                coordinator.admission_revalidation_active(eviction),
                |table| projected_rename_delta(table, old_key, table, &destination, now_nanos, nx),
            )?;
            let ReservationState {
                reservation,
                evicted,
                ..
            } = state;
            let mut effects: SmallVec<[DeferredEffects<'_>; 6]> = SmallVec::new();
            if let Some(effect) =
                self.cleanup_expired_key(source_shard, &mut guard, old_key, now_nanos)
            {
                effects.push(effect);
            }
            if old_key != &destination {
                if let Some(effect) =
                    self.cleanup_expired_key(source_shard, &mut guard, &destination, now_nanos)
                {
                    effects.push(effect);
                }
            }
            let old_had_ttl = ttl_present(guard.get_entry_ttl(old_key));
            let new_had_ttl = ttl_present(guard.get_entry_ttl(&destination));
            let same_key = old_key == &destination;
            let renamed = rename_within_table(&mut guard, old_key, new_key, now_nanos, nx);
            let old_has_ttl = ttl_present(guard.get_entry_ttl(old_key));
            let new_has_ttl = ttl_present(guard.get_entry_ttl(&destination));
            effects.push(
                MutationEffects::none()
                    .with_ttl(
                        source_shard,
                        ExpiryTransition::new(old_had_ttl, old_has_ttl),
                    )
                    .defer(),
            );
            if !same_key {
                effects.push(
                    MutationEffects::none()
                        .with_ttl(
                            source_shard,
                            ExpiryTransition::new(new_had_ttl, new_has_ttl),
                        )
                        .defer(),
                );
            }
            let renamed = match renamed {
                Ok(renamed) => renamed,
                Err(kind) => {
                    drop(guard);
                    for effect in effects {
                        self.publish_deferred_effects(effect);
                    }
                    return Err(MutationError::with_evictions(kind, evicted));
                }
            };
            let changed = renamed && !same_key;
            let aof_lsn = if changed {
                let (entry_lsn, aof_lsn) =
                    self.allocate_observed_mutation_lsn_with_features(self.mutation_features());
                stamp_entry_lsn_if(&mut guard, destination.as_bytes(), new_hash, entry_lsn);
                aof_lsn
            } else {
                None
            };
            if changed {
                effects.push(
                    MutationEffects::none()
                        .with_frequency(new_hash)
                        .with_watch_key(old_key)
                        .with_aof_lsn(aof_lsn)
                        .defer(),
                );
                effects.push(MutationEffects::none().with_watch_key(&destination).defer());
            }
            drop(guard);
            for effect in effects {
                self.publish_deferred_effects(effect);
            }
            reservation.settle();
            return Ok(mutation_outcome_with_evictions(renamed, aof_lsn, evicted));
        }

        let key_refs = [old_key.as_bytes(), new_key.as_bytes()];
        let eviction = self.eviction_config();
        let (read_guards, read_plan) = self.multi_read(&key_refs);
        let read_src_position = read_plan.guard_index_for_key(0).get();
        let read_dst_position = read_plan.guard_index_for_key(1).get();
        let projected_delta = projected_rename_delta(
            &read_guards[read_src_position].1,
            old_key,
            &read_guards[read_dst_position].1,
            &new_key,
            now_nanos,
            nx,
        )?;
        drop(read_guards);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(destination_shard, projected_delta, eviction)?;
        let (mut guards, plan, state) = coordinator.acquire_multi_write(
            &key_refs,
            destination_shard,
            state,
            "rename_key",
            coordinator.admission_revalidation_active(eviction),
            |guards, plan| {
                let src_position = plan.guard_index_for_key(0).get();
                let dst_position = plan.guard_index_for_key(1).get();
                let (src_table, dst_table) =
                    distinct_guard_tables_mut(guards, src_position, dst_position);

                projected_rename_delta(src_table, old_key, dst_table, &new_key, now_nanos, nx)
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let src_position = plan.guard_index_for_key(0).get();
        let dst_position = plan.guard_index_for_key(1).get();

        let (src_table, dst_table) =
            distinct_guard_tables_mut(&mut guards, src_position, dst_position);

        let destination_key = new_key.clone();
        let mut effects: SmallVec<[DeferredEffects<'_>; 6]> = SmallVec::new();
        if let Some(effect) = self.cleanup_expired_key(source_shard, src_table, old_key, now_nanos)
        {
            effects.push(effect);
        }
        if let Some(effect) =
            self.cleanup_expired_key(destination_shard, dst_table, &destination_key, now_nanos)
        {
            effects.push(effect);
        }
        let old_had_ttl = ttl_present(src_table.get_entry_ttl(old_key));
        let new_had_ttl = ttl_present(dst_table.get_entry_ttl(&destination_key));
        if !src_table.contains_key(old_key) {
            drop(guards);
            for effect in effects {
                self.publish_deferred_effects(effect);
            }
            return Err(MutationError::with_evictions(
                MutationErrorKind::NoSuchKey,
                evicted,
            ));
        }
        if nx && dst_table.contains_key(&new_key) {
            drop(guards);
            for effect in effects {
                self.publish_deferred_effects(effect);
            }
            reservation.settle();
            return Ok(mutation_outcome_with_evictions(false, None, evicted));
        }

        let (value, ttl) = src_table
            .remove_with_ttl(old_key)
            .expect("source key must exist after contains_key check");
        dst_table.remove(&new_key);
        if ttl != 0 && ttl > now_nanos {
            dst_table.insert_with(new_key, value, ttl, None);
        } else {
            dst_table.insert(new_key, value);
        }
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(self.mutation_features());
        stamp_entry_lsn_if(dst_table, destination_key.as_bytes(), new_hash, entry_lsn);
        effects.push(
            MutationEffects::none()
                .with_ttl(source_shard, ExpiryTransition::remove(old_had_ttl))
                .defer(),
        );
        effects.push(
            MutationEffects::none()
                .with_ttl(
                    destination_shard,
                    ExpiryTransition::new(new_had_ttl, ttl != 0 && ttl > now_nanos),
                )
                .defer(),
        );
        effects.push(
            MutationEffects::none()
                .with_frequency(new_hash)
                .with_watch_key(old_key)
                .with_aof_lsn(aof_lsn)
                .defer(),
        );
        effects.push(
            MutationEffects::none()
                .with_watch_key(&destination_key)
                .defer(),
        );
        drop(guards);
        for effect in effects {
            self.publish_deferred_effects(effect);
        }
        reservation.settle();
        Ok(mutation_outcome_with_evictions(true, aof_lsn, evicted))
    }

    pub(crate) fn copy_key(
        &self,
        src: &VortexKey,
        dst: VortexKey,
        replace: bool,
        now_nanos: u64,
    ) -> MutationResult<bool> {
        let source_shard = self.shard_index(src.as_bytes());
        let destination_shard = self.shard_index(dst.as_bytes());
        let dst_hash = self.table_hash_key(dst.as_bytes());

        if source_shard == destination_shard {
            let read_guard = self.read_shard_by_index(source_shard);
            let eviction = self.eviction_config();
            let source_value = match read_guard.get_with_ttl(src) {
                Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
                _ => None,
            };
            let projected_delta =
                projected_copy_delta(source_value, &read_guard, &dst, replace, now_nanos);
            drop(read_guard);
            let destination = dst.clone();
            let coordinator = ReservationCoordinator::new(self, now_nanos);
            let state = coordinator.reserve(destination_shard, projected_delta, eviction)?;
            let (mut guard, state) = coordinator.acquire_single_shard(
                source_shard,
                state,
                "copy_key",
                coordinator.admission_revalidation_active(eviction),
                |table| {
                    let source_value = match table.get_with_ttl(src) {
                        Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
                        _ => None,
                    };
                    Ok(projected_copy_delta(
                        source_value,
                        table,
                        &dst,
                        replace,
                        now_nanos,
                    ))
                },
            )?;
            let ReservationState {
                reservation,
                evicted,
                ..
            } = state;
            let mut effects: SmallVec<[DeferredEffects<'_>; 5]> = SmallVec::new();
            if let Some(effect) = self.cleanup_expired_key(source_shard, &mut guard, src, now_nanos)
            {
                effects.push(effect);
            }
            if src != &destination {
                if let Some(effect) =
                    self.cleanup_expired_key(source_shard, &mut guard, &destination, now_nanos)
                {
                    effects.push(effect);
                }
            }
            let dst_had_ttl = ttl_present(guard.get_entry_ttl(&destination));
            let copied = copy_within_table(&mut guard, src, dst, replace, now_nanos);
            let dst_has_ttl = ttl_present(guard.get_entry_ttl(&destination));
            effects.push(
                MutationEffects::none()
                    .with_ttl(
                        source_shard,
                        ExpiryTransition::new(dst_had_ttl, dst_has_ttl),
                    )
                    .defer(),
            );
            let aof_lsn = if copied {
                let (entry_lsn, aof_lsn) =
                    self.allocate_observed_mutation_lsn_with_features(self.mutation_features());
                stamp_entry_lsn_if(&mut guard, destination.as_bytes(), dst_hash, entry_lsn);
                aof_lsn
            } else {
                None
            };
            if copied {
                effects.push(
                    MutationEffects::none()
                        .with_frequency(dst_hash)
                        .with_watch_key(&destination)
                        .with_aof_lsn(aof_lsn)
                        .defer(),
                );
            }
            drop(guard);
            for effect in effects {
                self.publish_deferred_effects(effect);
            }
            reservation.settle();
            return Ok(mutation_outcome_with_evictions(copied, aof_lsn, evicted));
        }

        let key_refs = [src.as_bytes(), dst.as_bytes()];
        let eviction = self.eviction_config();
        let (read_guards, read_plan) = self.multi_read(&key_refs);
        let read_src_position = read_plan.guard_index_for_key(0).get();
        let read_dst_position = read_plan.guard_index_for_key(1).get();
        let source_value = match read_guards[read_src_position].1.get_with_ttl(src) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
            _ => None,
        };
        let projected_delta = projected_copy_delta(
            source_value,
            &read_guards[read_dst_position].1,
            &dst,
            replace,
            now_nanos,
        );
        drop(read_guards);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(destination_shard, projected_delta, eviction)?;
        let (mut guards, plan, state) = coordinator.acquire_multi_write(
            &key_refs,
            destination_shard,
            state,
            "copy_key",
            coordinator.admission_revalidation_active(eviction),
            |guards, plan| {
                let src_position = plan.guard_index_for_key(0).get();
                let dst_position = plan.guard_index_for_key(1).get();
                let (src_table, dst_table) =
                    distinct_guard_tables_mut(guards, src_position, dst_position);

                let source_value = match src_table.get_with_ttl(src) {
                    Some((value, ttl)) if ttl == 0 || ttl > now_nanos => Some(value),
                    _ => None,
                };
                Ok(projected_copy_delta(
                    source_value,
                    dst_table,
                    &dst,
                    replace,
                    now_nanos,
                ))
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let src_position = plan.guard_index_for_key(0).get();
        let dst_position = plan.guard_index_for_key(1).get();

        let (src_table, dst_table) =
            distinct_guard_tables_mut(&mut guards, src_position, dst_position);

        let destination = dst.clone();
        let mut effects: SmallVec<[DeferredEffects<'_>; 5]> = SmallVec::new();
        if let Some(effect) = self.cleanup_expired_key(source_shard, src_table, src, now_nanos) {
            effects.push(effect);
        }
        if let Some(effect) =
            self.cleanup_expired_key(destination_shard, dst_table, &destination, now_nanos)
        {
            effects.push(effect);
        }
        let dst_had_ttl = ttl_present(dst_table.get_entry_ttl(&destination));
        let (value_clone, ttl) = {
            let Some((value, ttl)) = src_table.get_with_ttl(src) else {
                drop(guards);
                for effect in effects {
                    self.publish_deferred_effects(effect);
                }
                return Ok(mutation_outcome_with_evictions(false, None, evicted));
            };
            if ttl != 0 && ttl <= now_nanos {
                drop(guards);
                for effect in effects {
                    self.publish_deferred_effects(effect);
                }
                return Ok(mutation_outcome_with_evictions(false, None, evicted));
            }
            (value.clone(), ttl)
        };

        if !replace && dst_table.contains_key(&destination) {
            drop(guards);
            for effect in effects {
                self.publish_deferred_effects(effect);
            }
            return Ok(mutation_outcome_with_evictions(false, None, evicted));
        }

        if ttl != 0 && ttl > now_nanos {
            dst_table.insert_with(dst, value_clone, ttl, None);
        } else {
            dst_table.insert(dst, value_clone);
        }
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(self.mutation_features());
        stamp_entry_lsn_if(dst_table, destination.as_bytes(), dst_hash, entry_lsn);
        effects.push(
            MutationEffects::none()
                .with_ttl(
                    destination_shard,
                    ExpiryTransition::new(dst_had_ttl, ttl != 0 && ttl > now_nanos),
                )
                .with_frequency(dst_hash)
                .with_watch_key(&destination)
                .with_aof_lsn(aof_lsn)
                .defer(),
        );
        drop(guards);
        for effect in effects {
            self.publish_deferred_effects(effect);
        }
        reservation.settle();
        Ok(mutation_outcome_with_evictions(true, aof_lsn, evicted))
    }
}
