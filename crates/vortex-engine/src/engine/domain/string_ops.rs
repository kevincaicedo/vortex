use super::mutation::*;
use super::string_tables::*;
use super::*;

impl ConcurrentKeyspace {
    ///
    /// The caller-provided encoder runs while the read guard is held so command
    /// code can build a borrowed response without cloning `VortexValue`.
    #[inline]
    pub(crate) fn read_value_with<R, F>(&self, key_bytes: &[u8], now_nanos: u64, encode: F) -> R
    where
        F: FnOnce(Option<&VortexValue>) -> R,
    {
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl_prehashed(key_bytes, table_hash) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                self.record_access_prehashed(&guard, key_bytes, table_hash);
                encode(Some(value))
            }
            Some(_) => {
                drop(guard);
                let key = VortexKey::from_bytes(key_bytes);
                let mut wguard = self.write_shard_by_index(shard_index);
                let had_ttl = matches!(wguard.get_entry_ttl(&key), Some(ttl) if ttl != 0);
                if remove_if_expired(&mut wguard, &key, now_nanos) {
                    self.commit_effects(
                        MutationEffects::none()
                            .with_ttl(shard_index, ExpiryTransition::remove(had_ttl))
                            .with_watch_key(&key),
                    );
                }
                encode(None)
            }
            None => encode(None),
        }
    }

    pub(crate) fn get_value(&self, key: &VortexKey, now_nanos: u64) -> Option<VortexValue> {
        let shard_index = self.shard_index(key.as_bytes());
        let table_hash = self.table_hash_key(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                self.record_access_prehashed(&guard, key.as_bytes(), table_hash);
                Some(value.clone())
            }
            Some(_) => {
                // Expired — drop read lock, escalate to write lock, double-check.
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                self.cleanup_expired_key(shard_index, &mut wguard, key, now_nanos);
                None
            }
            None => None,
        }
    }

    pub(crate) fn get_value_with_expiry_option(
        &self,
        key: &VortexKey,
        option: GetExOption,
        now_nanos: u64,
    ) -> MutationOutcome<Option<VortexValue>> {
        if option == GetExOption::None {
            return MutationOutcome::new(self.get_value(key, now_nanos), None);
        }

        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let mut guard = self.write_shard_by_index(shard_index);
        let Some((value, ttl_deadline)) = guard.get_with_ttl(key) else {
            return MutationOutcome::new(None, None);
        };
        let had_ttl = ttl_deadline != 0;
        if had_ttl && ttl_deadline <= now_nanos {
            let removed = remove_if_expired(&mut guard, key, now_nanos);
            debug_assert!(removed, "expired GETEX key must be removable");
            self.commit_effects(
                MutationEffects::none()
                    .with_ttl(shard_index, ExpiryTransition::ttl_removed())
                    .with_watch_key(key),
            );
            return MutationOutcome::new(None, None);
        }

        let value = value.clone();
        self.record_access_prehashed(&guard, key_bytes, table_hash);

        let (changed, transition) = match option {
            GetExOption::None => unreachable!("GETEX none exits through get_value"),
            GetExOption::ExpireAt(deadline) if deadline <= now_nanos => {
                let removed = guard.remove(key).is_some();
                debug_assert!(removed, "live GETEX key must be removable");
                (removed, ExpiryTransition::remove(had_ttl))
            }
            GetExOption::ExpireAt(deadline) => {
                let updated = guard.set_entry_ttl(key, deadline);
                (updated, ExpiryTransition::new(had_ttl, updated))
            }
            GetExOption::Persist => {
                let updated = guard.clear_entry_ttl(key);
                (updated, ExpiryTransition::remove(had_ttl))
            }
        };

        let aof_lsn = if changed {
            match option {
                GetExOption::ExpireAt(deadline) if deadline <= now_nanos => self.next_aof_lsn(),
                _ => {
                    let (entry_lsn, aof_lsn) =
                        self.allocate_observed_mutation_lsn_with_features(self.mutation_features());
                    stamp_entry_lsn_if(&mut guard, key_bytes, table_hash, entry_lsn);
                    aof_lsn
                }
            }
        } else {
            None
        };
        let aof_lsn = self
            .commit_effects(
                MutationEffects::none()
                    .with_ttl(shard_index, transition)
                    .with_watch_key_if(changed, key)
                    .with_aof_lsn(aof_lsn),
            )
            .into_lsn();
        MutationOutcome::new(Some(value), aof_lsn)
    }

    pub(crate) fn set_value_with_ttl(
        &self,
        key: VortexKey,
        value: VortexValue,
        ttl_deadline_nanos: u64,
        now_nanos: u64,
    ) -> MutationResult<Option<VortexValue>> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let eviction = self.eviction_config();
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let admission_active = coordinator.admission_revalidation_active(eviction);
        let state = if admission_active {
            let projected_delta = self
                .read_shard_by_index(shard_index)
                .projected_insert_delta(&key, &value);
            coordinator.reserve(shard_index, positive_delta(projected_delta), eviction)?
        } else {
            ReservationState::empty(self, eviction)
        };
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "set_value_with_ttl",
            admission_active,
            |table| Ok(positive_delta(table.projected_insert_delta(&key, &value))),
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let had_ttl = ttl_present(guard.get_entry_ttl(&key));
        let publish_features = self.mutation_features();
        let watched_key = publish_features.watch().then(|| key.clone());
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(publish_features);
        let previous = guard.insert_with(key, value, ttl_deadline_nanos, entry_lsn);
        self.commit_effects(
            MutationEffects::none()
                .with_ttl(shard_index, ExpiryTransition::new(had_ttl, true))
                .with_frequency(table_hash)
                .with_optional_watch_key(watched_key.as_ref()),
        );
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(previous, aof_lsn, evicted))
    }

    /// Fast path for plain `SET key value` (no options, no TTL).
    ///
    /// Eliminates three redundant table probes:
    /// - No `remove_if_expired` probe (insert overwrites unconditionally)
    /// - No `get_entry_ttl` pre-probe (fused into `insert_no_ttl`)
    /// - Single probe in `insert_no_ttl` returns old TTL status
    ///
    /// # Why skipping `remove_if_expired` is correct
    ///
    /// Plain SET unconditionally overwrites the key with TTL=0, so any
    /// expired value is replaced regardless. The expiry count is still
    /// correctly maintained via the `old_had_ttl` return value.
    #[inline]
    pub(crate) fn set_value_plain_bytes(
        &self,
        key_bytes: &[u8],
        value_bytes: &[u8],
        now_nanos: u64,
    ) -> MutationResult<()> {
        let features = self.mutation_features();
        if !features.is_empty() {
            return self.set_value_plain(
                VortexKey::from(key_bytes),
                VortexValue::from_bytes(value_bytes),
                now_nanos,
            );
        }

        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let mut guard = self.write_shard_by_index(shard_index);
        if !self.mutation_features().is_empty() {
            drop(guard);
            return self.set_value_plain(
                VortexKey::from(key_bytes),
                VortexValue::from_bytes(value_bytes),
                now_nanos,
            );
        }
        let old_had_ttl = if value_bytes.len() <= vortex_common::MAX_INLINE_VALUE_LEN {
            let value = VortexValue::from_bytes(value_bytes);
            guard
                .mutate_prehashed(
                    BorrowedKey(key_bytes),
                    value,
                    table_hash,
                    MutationPolicy::clear(None),
                )
                .had_ttl()
        } else {
            guard
                .mutate_prehashed(
                    BorrowedKey(key_bytes),
                    RawValueBytes(value_bytes),
                    table_hash,
                    MutationPolicy::clear(None),
                )
                .had_ttl()
        };
        self.commit_effects(
            MutationEffects::none().with_ttl(shard_index, ExpiryTransition::remove(old_had_ttl)),
        );
        Ok(MutationOutcome::new((), None))
    }

    #[inline]
    pub(crate) fn set_value_plain(
        &self,
        key: VortexKey,
        value: VortexValue,
        now_nanos: u64,
    ) -> MutationResult<()> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        // Pre-hash for the table BEFORE acquiring the write lock.
        let table_hash = self.table_hash_key(key_bytes);
        let features = self.mutation_features();
        let eviction = self.eviction_config();
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = if features.maxmemory() {
            let projected_delta = self
                .read_shard_by_index(shard_index)
                .projected_insert_delta_prehashed(&key, &value, table_hash);
            coordinator.reserve(shard_index, positive_delta(projected_delta), eviction)?
        } else {
            ReservationState {
                snapshot: eviction,
                reservation: MemoryReservation::new(self, 0),
                evicted: None,
            }
        };
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "set_value_plain",
            features.maxmemory() && coordinator.admission_revalidation_active(eviction),
            |table| {
                Ok(positive_delta(table.projected_insert_delta_prehashed(
                    &key, &value, table_hash,
                )))
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let publish_features = self.mutation_features();
        let watched_key = publish_features.watch().then(|| key.clone());
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(publish_features);
        let old_had_ttl = guard
            .mutate_prehashed(key, value, table_hash, MutationPolicy::clear(entry_lsn))
            .had_ttl();
        let mut effects = MutationEffects::none()
            .with_ttl(shard_index, ExpiryTransition::remove(old_had_ttl))
            .with_aof_lsn(aof_lsn)
            .with_optional_watch_key(watched_key.as_ref());
        if features.maxmemory() {
            effects = effects.with_frequency(table_hash);
        }
        let aof_lsn = self.commit_effects(effects).into_lsn();
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions((), aof_lsn, evicted))
    }
    pub(crate) fn set_value_with_options(
        &self,
        key: VortexKey,
        value: VortexValue,
        options: SetOptions,
        now_nanos: u64,
    ) -> MutationResult<SetResult> {
        let shard_index = self.shard_index(key.as_bytes());
        let eviction = self.eviction_config();
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let admission_active = coordinator.admission_revalidation_active(eviction);
        let state = if admission_active {
            let projected_delta = projected_set_write_delta(
                &self.read_shard_by_index(shard_index),
                &key,
                &value,
                options,
                now_nanos,
            );
            coordinator.reserve(shard_index, projected_delta, eviction)?
        } else {
            ReservationState::empty(self, eviction)
        };
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "set_value_with_options",
            admission_active,
            |table| {
                Ok(projected_set_write_delta(
                    table, &key, &value, options, now_nanos,
                ))
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let (result, transition) =
            set_with_options_on_table(&mut guard, key.clone(), value, options, now_nanos);
        let changed = matches!(&result, SetResult::Ok | SetResult::OkGet(_));
        let table_hash = self.table_hash_key(key.as_bytes());
        let aof_lsn = if changed {
            let key_bytes = key.as_bytes();
            let (entry_lsn, aof_lsn) =
                self.allocate_observed_mutation_lsn_with_features(self.mutation_features());
            stamp_entry_lsn_if(&mut guard, key_bytes, table_hash, entry_lsn);
            aof_lsn
        } else {
            None
        };
        let mut effects = MutationEffects::none()
            .with_ttl(shard_index, transition)
            .with_aof_lsn(aof_lsn);
        if changed {
            effects = effects.with_frequency(table_hash).with_watch_key(&key);
        }
        let aof_lsn = self.commit_effects(effects).into_lsn();
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(result, aof_lsn, evicted))
    }

    pub(crate) fn mget_values_with<R, F, N>(
        &self,
        keys: &[&[u8]],
        now_nanos: u64,
        mut encode: F,
        mut nil: N,
    ) -> Vec<R>
    where
        F: FnMut(&VortexValue) -> R,
        N: FnMut() -> R,
    {
        if keys.is_empty() {
            return Vec::new();
        }

        #[derive(Clone, Copy)]
        struct KeyLookup {
            output_idx: usize,
            shard_idx: usize,
            hash: TableHash,
        }

        let mut lookups: SmallVec<[KeyLookup; 16]> = SmallVec::with_capacity(keys.len());
        for (output_idx, &key_bytes) in keys.iter().enumerate() {
            lookups.push(KeyLookup {
                output_idx,
                shard_idx: self.shard_index(key_bytes),
                hash: self.table_hash_key(key_bytes),
            });
        }
        lookups.sort_unstable_by_key(|lookup| lookup.shard_idx);

        let mut values = Vec::with_capacity(keys.len());
        values.resize_with(keys.len(), || None);
        let mut expired: SmallVec<[KeyLookup; 8]> = SmallVec::new();

        // Process one shard at a time so the hot path avoids the extra vectors,
        // binary searches, and guard fan-out of the generic multi-read helper.
        let mut cursor = 0;
        while cursor < lookups.len() {
            let shard_idx = lookups[cursor].shard_idx;
            let group_start = cursor;
            while cursor < lookups.len() && lookups[cursor].shard_idx == shard_idx {
                cursor += 1;
            }

            let guard = self.read_shard_by_index(shard_idx);
            for lookup in &lookups[group_start..cursor] {
                guard.prefetch_group(lookup.hash.get());
            }

            for lookup in &lookups[group_start..cursor] {
                let key_bytes = keys[lookup.output_idx];
                match guard.get_with_ttl_prehashed(key_bytes, lookup.hash) {
                    Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                        self.record_access_prehashed(&guard, key_bytes, lookup.hash);
                        values[lookup.output_idx] = Some(encode(value));
                    }
                    Some(_) => {
                        expired.push(*lookup);
                    }
                    None => {}
                }
            }
        }

        // `expired` is emitted in the same shard-group order as the read pass,
        // so we can batch lazy expiry cleanup with one write lock per shard.
        let mut expired_cursor = 0;
        while expired_cursor < expired.len() {
            let shard_idx = expired[expired_cursor].shard_idx;
            let mut wguard = self.write_shard_by_index(shard_idx);
            while expired_cursor < expired.len() && expired[expired_cursor].shard_idx == shard_idx {
                let lookup = expired[expired_cursor];
                let _ = self.cleanup_expired_prehashed(
                    shard_idx,
                    &mut wguard,
                    keys[lookup.output_idx],
                    lookup.hash,
                    now_nanos,
                );
                expired_cursor += 1;
            }
        }

        values
            .into_iter()
            .map(|value| match value {
                Some(value) => value,
                None => nil(),
            })
            .collect()
    }

    pub(crate) fn mset_values(
        &self,
        pairs: Vec<(VortexKey, VortexValue)>,
        now_nanos: u64,
    ) -> MutationResult<()> {
        if pairs.is_empty() {
            return Ok(MutationOutcome::new((), None));
        }

        let eviction = self.eviction_config();
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let admission_active = coordinator.admission_revalidation_active(eviction);
        let pairs = if admission_active {
            deduplicate_last_write_pairs(pairs)
        } else {
            pairs
        };
        let preferred_shard = self.shard_index(pairs[0].0.as_bytes());
        let state = if admission_active {
            let projected_delta = PositiveDelta::sum(pairs.iter().map(|(key, value)| {
                let shard_index = self.shard_index(key.as_bytes());
                let guard = self.read_shard_by_index(shard_index);
                positive_delta(guard.projected_insert_delta(key, value))
            }));
            coordinator.reserve(preferred_shard, projected_delta, eviction)?
        } else {
            ReservationState::empty(self, eviction)
        };

        let (key_refs, lookups) = build_multi_write_lookups(self, &pairs);
        let (mut guards, plan, state) = coordinator.acquire_multi_write(
            &key_refs,
            preferred_shard,
            state,
            "mset_values",
            admission_active,
            |guards, plan| {
                Ok(PositiveDelta::sum(lookups.iter().map(|lookup| {
                    let guard_pos = plan.guard_index_for_key(lookup.pair_index).get();
                    let table = &*guards[guard_pos].1;
                    let (key, value) = &pairs[lookup.pair_index];
                    positive_delta(table.projected_insert_delta_prehashed(
                        key,
                        value,
                        lookup.table_hash,
                    ))
                })))
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        drop(key_refs);
        let mut pairs = pairs.into_iter().map(Some).collect::<Vec<_>>();
        let publish_features = self.mutation_features();
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(publish_features);

        for lookup in lookups {
            let guard_pos = plan.guard_index_for_key(lookup.pair_index).get();
            let table = &mut *guards[guard_pos].1;
            let (key, value) = pairs[lookup.pair_index]
                .take()
                .expect("mset pair must be available exactly once");
            let watched_key = publish_features.watch().then(|| key.clone());
            let old_had_ttl = table
                .mutate_prehashed(
                    key,
                    value,
                    lookup.table_hash,
                    MutationPolicy::clear(entry_lsn),
                )
                .had_ttl();
            self.commit_effects(
                MutationEffects::none()
                    .with_ttl(lookup.shard_idx, ExpiryTransition::remove(old_had_ttl))
                    .with_frequency(lookup.table_hash)
                    .with_optional_watch_key(watched_key.as_ref()),
            );
        }
        drop(guards);
        reservation.settle();
        Ok(mutation_outcome_with_evictions((), aof_lsn, evicted))
    }

    pub(crate) fn msetnx_values(
        &self,
        pairs: Vec<(VortexKey, VortexValue)>,
        now_nanos: u64,
    ) -> MutationResult<bool> {
        let pairs = deduplicate_last_write_pairs(pairs);
        if pairs.is_empty() {
            return Ok(MutationOutcome::new(true, None));
        }

        let all_absent = pairs.iter().all(|(key, _)| {
            !matches!(
                self.read_shard_by_index(self.shard_index(key.as_bytes()))
                    .get_with_ttl(key),
                Some((_, ttl)) if ttl == 0 || ttl > now_nanos
            )
        });
        if !all_absent {
            return Ok(MutationOutcome::new(false, None));
        }

        let preferred_shard = self.shard_index(pairs[0].0.as_bytes());
        let eviction = self.eviction_config();
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let admission_active = coordinator.admission_revalidation_active(eviction);
        let state = if admission_active {
            let projected_delta = PositiveDelta::sum(pairs.iter().map(|(key, value)| {
                let shard_index = self.shard_index(key.as_bytes());
                let guard = self.read_shard_by_index(shard_index);
                positive_delta(guard.projected_insert_delta(key, value))
            }));
            coordinator.reserve(preferred_shard, projected_delta, eviction)?
        } else {
            ReservationState::empty(self, eviction)
        };

        let (key_refs, lookups) = build_multi_write_lookups(self, &pairs);
        let (mut guards, plan, state) = coordinator.acquire_multi_write(
            &key_refs,
            preferred_shard,
            state,
            "msetnx_values",
            admission_active,
            |guards, plan| {
                if !msetnx_all_absent_locked(self, guards, plan, &pairs, &lookups, now_nanos) {
                    return Ok(PositiveDelta::zero());
                }
                Ok(PositiveDelta::sum(lookups.iter().map(|lookup| {
                    let guard_pos = plan.guard_index_for_key(lookup.pair_index).get();
                    let table = &*guards[guard_pos].1;
                    let (key, value) = &pairs[lookup.pair_index];
                    positive_delta(table.projected_insert_delta_prehashed(
                        key,
                        value,
                        lookup.table_hash,
                    ))
                })))
            },
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        if !msetnx_all_absent_locked(self, &mut guards, &plan, &pairs, &lookups, now_nanos) {
            drop(guards);
            reservation.settle();
            return Ok(mutation_outcome_with_evictions(false, None, evicted));
        }
        drop(key_refs);

        let mut pairs = pairs.into_iter().map(Some).collect::<Vec<_>>();
        let publish_features = self.mutation_features();
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(publish_features);
        for lookup in lookups {
            let guard_pos = plan.guard_index_for_key(lookup.pair_index).get();
            let table = &mut *guards[guard_pos].1;
            let (key, value) = pairs[lookup.pair_index]
                .take()
                .expect("msetnx pair must be available exactly once");
            let watched_key = publish_features.watch().then(|| key.clone());
            table.insert_new_prehashed(key, value, lookup.table_hash, entry_lsn);
            self.commit_effects(
                MutationEffects::none()
                    .with_frequency(lookup.table_hash)
                    .with_optional_watch_key(watched_key.as_ref()),
            );
        }
        drop(guards);
        reservation.settle();

        Ok(mutation_outcome_with_evictions(true, aof_lsn, evicted))
    }

    pub(crate) fn increment_by(
        &self,
        key: VortexKey,
        delta: i64,
        now_nanos: u64,
    ) -> MutationResult<i64> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let eviction = self.eviction_config();
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta = projected_increment_delta(&read_guard, &key, delta, now_nanos)?;
        drop(read_guard);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(shard_index, projected_delta, eviction)?;
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "increment_by",
            coordinator.admission_revalidation_active(eviction),
            |table| projected_increment_delta(table, &key, delta, now_nanos),
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let publish_features = self.mutation_features();
        let watched_key = publish_features.watch().then(|| key.clone());
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(publish_features);
        let (result, transition) =
            match increment_table_by(&mut guard, key, delta, entry_lsn, now_nanos) {
                Ok(result) => result,
                Err(err) => return Err(MutationError::with_evictions(err, evicted)),
            };
        self.commit_effects(
            MutationEffects::none()
                .with_ttl(shard_index, transition)
                .with_frequency(table_hash)
                .with_optional_watch_key(watched_key.as_ref()),
        );
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(result, aof_lsn, evicted))
    }

    pub(crate) fn increment_by_float(
        &self,
        key: VortexKey,
        increment: f64,
        now_nanos: u64,
    ) -> MutationResult<FloatIncrementResult> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let eviction = self.eviction_config();
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta =
            projected_increment_by_float_delta(&read_guard, &key, increment, now_nanos)?;
        drop(read_guard);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(shard_index, projected_delta, eviction)?;
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "increment_by_float",
            coordinator.admission_revalidation_active(eviction),
            |table| projected_increment_by_float_delta(table, &key, increment, now_nanos),
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let ttl_deadline = guard.get_entry_ttl(&key);
        let publish_features = self.mutation_features();
        let watched_key = publish_features.watch().then(|| key.clone());
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(publish_features);
        let result =
            match increment_table_by_float(&mut guard, key, increment, entry_lsn, now_nanos) {
                Ok(result) => result,
                Err(err) => return Err(MutationError::with_evictions(err, evicted)),
            };
        let ttl_after = match ttl_deadline {
            Some(deadline) if deadline > now_nanos => TtlState::Deadline(deadline),
            _ => TtlState::Persistent,
        };
        let mut effects = MutationEffects::none()
            .with_ttl(
                shard_index,
                ExpiryTransition::new(
                    ttl_present(ttl_deadline),
                    matches!(ttl_after, TtlState::Deadline(_)),
                ),
            )
            .with_frequency(table_hash);
        if let Some(key) = watched_key.as_ref() {
            effects = effects.with_watch_key(key);
        }
        self.commit_effects(effects);
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(
            FloatIncrementResult {
                value: result,
                ttl_after,
            },
            aof_lsn,
            evicted,
        ))
    }

    pub(crate) fn append_value(
        &self,
        key: VortexKey,
        append_bytes: &[u8],
        now_nanos: u64,
    ) -> MutationResult<usize> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let eviction = self.eviction_config();
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta = projected_append_delta(&read_guard, &key, append_bytes, now_nanos)?;
        drop(read_guard);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(shard_index, projected_delta, eviction)?;
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "append_value",
            coordinator.admission_revalidation_active(eviction),
            |table| projected_append_delta(table, &key, append_bytes, now_nanos),
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let ttl_deadline = guard.get_entry_ttl(&key);
        let publish_features = self.mutation_features();
        let watched_key = publish_features.watch().then(|| key.clone());
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(publish_features);
        let length = match append_to_table(&mut guard, key, append_bytes, entry_lsn, now_nanos) {
            Ok(length) => length,
            Err(err) => return Err(MutationError::with_evictions(err, evicted)),
        };
        self.commit_effects(
            MutationEffects::none()
                .with_ttl(
                    shard_index,
                    ExpiryTransition::new(
                        ttl_present(ttl_deadline),
                        matches!(ttl_deadline, Some(deadline) if deadline > now_nanos),
                    ),
                )
                .with_frequency(table_hash)
                .with_optional_watch_key(watched_key.as_ref()),
        );
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(length, aof_lsn, evicted))
    }

    pub(crate) fn strlen_value(
        &self,
        key: &VortexKey,
        now_nanos: u64,
    ) -> Result<Option<usize>, &'static [u8]> {
        let shard_index = self.shard_index(key.as_bytes());
        let table_hash = self.table_hash_key(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                self.record_access_prehashed(&guard, key.as_bytes(), table_hash);
                if !value.is_string() {
                    Err(ERR_WRONG_TYPE)
                } else {
                    Ok(Some(value.strlen()))
                }
            }
            Some(_) => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                self.cleanup_expired_key(shard_index, &mut wguard, key, now_nanos);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    pub(crate) fn getrange_value(
        &self,
        key: &VortexKey,
        start: i64,
        end: i64,
        now_nanos: u64,
    ) -> Result<Option<Bytes>, &'static [u8]> {
        let shard_index = self.shard_index(key.as_bytes());
        let table_hash = self.table_hash_key(key.as_bytes());
        let guard = self.read_shard_by_index(shard_index);
        match guard.get_with_ttl(key) {
            Some((value, ttl)) if ttl == 0 || ttl > now_nanos => {
                self.record_access_prehashed(&guard, key.as_bytes(), table_hash);
                // Live key — compute range while read guard is held.
                getrange_from_value(value, start, end)
            }
            Some(_) => {
                drop(guard);
                let mut wguard = self.write_shard_by_index(shard_index);
                self.cleanup_expired_key(shard_index, &mut wguard, key, now_nanos);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    pub(crate) fn setrange_value(
        &self,
        key: VortexKey,
        offset: usize,
        new_bytes: &[u8],
        now_nanos: u64,
    ) -> MutationResult<usize> {
        let key_bytes = key.as_bytes();
        let shard_index = self.shard_index(key_bytes);
        let table_hash = self.table_hash_key(key_bytes);
        let eviction = self.eviction_config();
        let read_guard = self.read_shard_by_index(shard_index);
        let projected_delta =
            projected_setrange_delta(&read_guard, &key, offset, new_bytes, now_nanos)?;
        drop(read_guard);
        let coordinator = ReservationCoordinator::new(self, now_nanos);
        let state = coordinator.reserve(shard_index, projected_delta, eviction)?;
        let (mut guard, state) = coordinator.acquire_single_shard(
            shard_index,
            state,
            "setrange_value",
            coordinator.admission_revalidation_active(eviction),
            |table| projected_setrange_delta(table, &key, offset, new_bytes, now_nanos),
        )?;
        let ReservationState {
            reservation,
            evicted,
            ..
        } = state;
        let had_ttl = ttl_present(guard.get_entry_ttl(&key));
        let ttl_probe_key = key.clone();
        let publish_features = self.mutation_features();
        let watched_key = publish_features.watch().then(|| key.clone());
        let (entry_lsn, aof_lsn) =
            self.allocate_observed_mutation_lsn_with_features(publish_features);
        let length =
            match setrange_in_table(&mut guard, key, offset, new_bytes, entry_lsn, now_nanos) {
                Ok(length) => length,
                Err(err) => return Err(MutationError::with_evictions(err, evicted)),
            };
        let has_ttl_after = ttl_present(guard.get_entry_ttl(&ttl_probe_key));
        self.commit_effects(
            MutationEffects::none()
                .with_ttl(shard_index, ExpiryTransition::new(had_ttl, has_ttl_after))
                .with_frequency(table_hash)
                .with_optional_watch_key(watched_key.as_ref()),
        );
        drop(guard);
        reservation.settle();
        Ok(mutation_outcome_with_evictions(length, aof_lsn, evicted))
    }
}
