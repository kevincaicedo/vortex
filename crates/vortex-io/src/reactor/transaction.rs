use super::*;

pub(super) struct TransactionExecutionPlan {
    pub(super) gate_scope: TransactionGateScope,
    pub(super) has_write: bool,
}

pub(super) enum TransactionGateScope {
    None,
    Shards(TransactionGatePlan),
    Full,
}

impl Reactor {
    /// UNWATCH
    ///
    /// Clears every key watched by the connection.
    ///
    /// Big-O: `O(W)`, where `W` is watched keys for this connection.
    ///
    /// Compatibility: Returns `OK` even when no keys are watched. `EXEC`,
    /// `DISCARD`, and connection close also call this path.
    ///
    /// Notes: Watch removals touch only the watch side table and do not take
    /// keyspace shard write locks.
    pub(super) fn clear_watches(&mut self, conn_id: usize) {
        if conn_id >= self.transaction_states.len() {
            return;
        }
        let watched = std::mem::take(&mut self.transaction_states[conn_id].watched);
        self.transaction_states[conn_id].watch_epoch = 0;
        if !watched.is_empty() {
            self.keyspace.unwatch_keys(watched);
        }
    }

    #[inline]
    pub(super) fn clear_transaction_state(&mut self, conn_id: usize) {
        if conn_id >= self.transaction_states.len() {
            return;
        }
        self.clear_watches(conn_id);
        self.transaction_states[conn_id].reset_all();
    }

    /// MULTI
    ///
    /// Starts transaction queueing for one connection.
    ///
    /// Big-O: `O(Q)` only when stale queued commands must be cleared from a
    /// previous dirty state; the normal path is `O(1)`.
    ///
    /// Compatibility: Returns `OK` and queues later commands until `EXEC` or
    /// `DISCARD`. Transaction-control command errors such as nested `MULTI` are
    /// returned immediately and do not dirty the queue; ordinary queued command
    /// errors still make `EXEC` fail with `EXECABORT`.
    ///
    /// Notes: The state is reactor-local to avoid adding transaction branches
    /// or connection lookups to the keyspace hot path.
    #[inline]
    pub(super) fn begin_transaction(&mut self, conn_id: usize) -> (CommandResponse, bool) {
        if conn_id < self.transaction_states.len() {
            let tx = &mut self.transaction_states[conn_id];
            tx.queueing = true;
            tx.dirty = false;
            tx.clear_queued();
        }
        (
            CommandResponse::Static(vortex_engine::commands::RESP_OK),
            false,
        )
    }

    /// DISCARD
    ///
    /// Drops queued commands, exits transaction mode, and clears WATCH state.
    ///
    /// Big-O: `O(Q + W)`, where `Q` is queued commands and `W` is watched keys.
    ///
    /// Compatibility: Returns `OK` inside MULTI and `ERR DISCARD without MULTI`
    /// outside MULTI.
    ///
    /// Notes: Because queued commands were not executed, DISCARD never appends
    /// their payloads to AOF.
    #[inline]
    pub(super) fn discard_transaction(&mut self, conn_id: usize) -> (CommandResponse, bool) {
        self.clear_transaction_state(conn_id);
        (
            CommandResponse::Static(vortex_engine::commands::RESP_OK),
            false,
        )
    }

    pub(super) fn normalized_command_name(frame: &FrameRef<'_>) -> Option<([u8; 32], usize)> {
        let cmd_name = frame.command_name()?;
        let len = cmd_name.len();
        if len == 0 || len > 32 {
            return None;
        }
        let mut upper = [0u8; 32];
        upper[..len].copy_from_slice(cmd_name);
        uppercase_inplace(&mut upper[..len]);
        Some((upper, len))
    }

    #[inline]
    pub(super) fn aof_failed_for_write(&self) -> bool {
        self.aof_writer.is_some() && self.aof_coordinator.is_failed()
    }

    #[inline]
    pub(super) fn validate_aof_writer_epoch(&self) -> io::Result<AofEpoch> {
        let epoch = self
            .aof_writer
            .as_ref()
            .map(|slot| slot.epoch)
            .ok_or_else(|| io::Error::other("AOF LSN allocated without a reactor writer"))?;
        self.aof_coordinator.validate_writer_epoch(epoch)?;
        Ok(epoch)
    }

    #[cfg(test)]
    pub(super) fn maybe_inject_aof_append_failure(&mut self) -> io::Result<()> {
        let Some(remaining) = self.aof_append_fail_after.as_mut() else {
            return Ok(());
        };

        if *remaining == 0 {
            self.aof_append_fail_after = None;
            Err(io::Error::other("injected AOF append failure"))
        } else {
            *remaining -= 1;
            Ok(())
        }
    }

    #[cfg(test)]
    pub(super) fn maybe_inject_aof_fsync_failure(&mut self) -> io::Result<()> {
        let Some(remaining) = self.aof_fsync_fail_after.as_mut() else {
            return Ok(());
        };

        if *remaining == 0 {
            self.aof_fsync_fail_after = None;
            Err(io::Error::other("injected AOF fsync failure"))
        } else {
            *remaining -= 1;
            Ok(())
        }
    }

    #[cfg(not(test))]
    #[inline]
    pub(super) fn maybe_inject_aof_append_failure(&mut self) -> io::Result<()> {
        Ok(())
    }

    #[cfg(not(test))]
    #[inline]
    pub(super) fn maybe_inject_aof_fsync_failure(&mut self) -> io::Result<()> {
        Ok(())
    }

    pub(super) fn frame_to_owned_resp(&mut self, frame: &FrameRef<'_>) -> io::Result<Box<[u8]>> {
        let written = loop {
            if let Some(written) = frame.write_resp_to(&mut self.aof_scratch) {
                break written;
            }

            let next_len = self
                .aof_scratch
                .len()
                .max(1)
                .checked_mul(2)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "RESP frame exceeded scratch buffer growth limit",
                    )
                })?;
            self.aof_scratch.resize(next_len, 0);
        };
        Ok(self.aof_scratch[..written].to_vec().into_boxed_slice())
    }

    pub(super) fn queue_transaction_command(
        &mut self,
        conn_id: usize,
        frame: &FrameRef<'_>,
    ) -> CommandResponse {
        if self.transaction_states[conn_id].queued.len()
            >= self.config.connection_caps.max_multi_queue_commands
        {
            self.transaction_states[conn_id].dirty = true;
            self.keyspace
                .record_reactor_multi_queue_command_cap_exceeded(self.id);
            return CommandResponse::Static(RESP_ERR_TX_QUEUE_FULL);
        }

        match self.command_router.dispatch(frame) {
            DispatchResult::Dispatch { meta, .. } => {
                if self.aof_failed_for_write() && meta.flags.contains(CommandFlags::WRITE) {
                    self.transaction_states[conn_id].dirty = true;
                    return CommandResponse::Static(RESP_ERR_AOF_MISCONF);
                }
            }
            DispatchResult::WrongArity { .. } => {
                self.transaction_states[conn_id].dirty = true;
                return CommandResponse::Static(RESP_ERR_WRONG_ARGC);
            }
            DispatchResult::UnknownCommand => {
                self.transaction_states[conn_id].dirty = true;
                return CommandResponse::Static(RESP_ERR_UNKNOWN);
            }
        }

        match self.frame_to_owned_resp(frame) {
            Ok(payload) => {
                let queued_bytes = self.transaction_states[conn_id].queued_bytes;
                let Some(next_bytes) = queued_bytes.checked_add(payload.len()) else {
                    self.transaction_states[conn_id].dirty = true;
                    self.keyspace
                        .record_reactor_multi_queue_bytes_cap_exceeded(self.id);
                    return CommandResponse::Static(RESP_ERR_TX_QUEUE_BYTES);
                };
                if next_bytes > self.config.connection_caps.max_multi_queue_bytes {
                    self.transaction_states[conn_id].dirty = true;
                    self.keyspace
                        .record_reactor_multi_queue_bytes_cap_exceeded(self.id);
                    return CommandResponse::Static(RESP_ERR_TX_QUEUE_BYTES);
                }
                self.transaction_states[conn_id].queued.push(payload);
                self.transaction_states[conn_id].queued_bytes = next_bytes;
                CommandResponse::Static(RESP_QUEUED)
            }
            Err(_) => {
                self.transaction_states[conn_id].dirty = true;
                CommandResponse::Static(RESP_ERR_PROTOCOL)
            }
        }
    }

    /// WATCH key [key ...]
    ///
    /// Registers optimistic CAS watches for the current connection. `EXEC`
    /// aborts with a nil array if any watched key changes before execution.
    ///
    /// Big-O: `O(K + D)`, where `K` is the number of requested keys and `D` is
    /// the current watched-key count for duplicate suppression.
    ///
    /// Compatibility: Requires at least one key; `WATCH` inside MULTI is
    /// rejected in `dispatch_command` without dirtying the transaction queue.
    ///
    /// Notes: The first WATCH snapshots the global watch epoch. Writes stay on
    /// the fast path when no watches exist because keyspace mutation only
    /// consults the watch side table while the watch feature bit is active.
    pub(super) fn handle_watch(&mut self, conn_id: usize, frame: &FrameRef<'_>) -> CommandResponse {
        let argc = match frame.element_count() {
            Some(argc) if argc >= 2 => argc as usize,
            _ => return CommandResponse::Static(RESP_ERR_WRONG_ARGC),
        };

        let mut keys = Vec::with_capacity(argc - 1);
        let Some(mut children) = frame.children() else {
            return CommandResponse::Static(RESP_ERR_WRONG_ARGC);
        };
        let _ = children.next();
        for child in children {
            let Some(key_bytes) = child.as_bytes() else {
                return CommandResponse::Static(RESP_ERR_WRONG_ARGC);
            };
            keys.push(key_from_bytes(key_bytes));
        }

        let mut new_keys = Vec::new();
        for key in keys {
            if self.transaction_states[conn_id]
                .watched
                .iter()
                .any(|watched| watched.key() == &key)
                || new_keys.iter().any(|new_key| new_key == &key)
            {
                continue;
            }
            new_keys.push(key);
        }

        let watched_len = self.transaction_states[conn_id].watched.len();
        let Some(next_watched_len) = watched_len.checked_add(new_keys.len()) else {
            self.keyspace.record_reactor_watch_cap_exceeded(self.id);
            return CommandResponse::Static(RESP_ERR_WATCH_LIMIT);
        };
        if next_watched_len > self.config.connection_caps.max_watch_registrations {
            self.keyspace.record_reactor_watch_cap_exceeded(self.id);
            return CommandResponse::Static(RESP_ERR_WATCH_LIMIT);
        }

        if self.transaction_states[conn_id].watched.is_empty() {
            self.transaction_states[conn_id].watch_epoch = self.keyspace.current_watch_epoch();
        }

        for key in new_keys {
            let watched = self.keyspace.watch_key(key);
            self.transaction_states[conn_id].watched.push(watched);
        }

        CommandResponse::Static(vortex_engine::commands::RESP_OK)
    }

    pub(super) fn queued_transaction_plan(
        &self,
        queued: &[Box<[u8]>],
        watched: &[WatchRegistration],
    ) -> io::Result<TransactionExecutionPlan> {
        let mut watched_keys: smallvec::SmallVec<[&[u8]; 16]> = smallvec::SmallVec::new();
        for watched in watched {
            watched_keys.push(watched.key().as_bytes());
        }
        let mut gate_plan = self
            .keyspace
            .transaction_gate_plan_for_keys(watched_keys.as_slice());

        let mut has_write = false;
        let mut full_keyspace = false;
        for payload in queued {
            let tape = BorrowedRespTape::parse_pipeline(payload.as_ref()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "queued RESP parse failed")
            })?;
            let frame = tape.iter().next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "empty queued command")
            })?;

            let Some((upper, len)) = Self::normalized_command_name(&frame) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "queued command failed name normalization",
                ));
            };
            let DispatchResult::Dispatch { meta, .. } =
                CommandRouter::dispatch_normalized(&frame, &upper[..len])
            else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "queued command failed dispatch validation",
                ));
            };

            has_write |= meta.flags.contains(CommandFlags::WRITE);
            let scope = command_keyspace_gate_scope(meta, &frame);
            if scope.full {
                full_keyspace = true;
            } else if !scope.keys.is_empty() {
                gate_plan.merge(
                    self.keyspace
                        .transaction_gate_plan_for_keys(scope.keys.as_slice()),
                );
            }
        }

        let gate_scope = if full_keyspace {
            TransactionGateScope::Full
        } else if gate_plan.is_empty() {
            TransactionGateScope::None
        } else {
            TransactionGateScope::Shards(gate_plan)
        };

        Ok(TransactionExecutionPlan {
            gate_scope,
            has_write,
        })
    }

    #[cfg(test)]
    pub(super) fn maybe_inject_transaction_aof_precommit_failure(&mut self) -> io::Result<()> {
        let Some(slot) = self.aof_writer.as_ref() else {
            return Ok(());
        };

        // Immediate injected EXEC failures are treated as precommit faults so
        // the failure harness can prove no transaction becomes visible or
        // replayable as complete after an injected append/fsync failure.
        if self.aof_append_fail_after == Some(0) {
            return self.maybe_inject_aof_append_failure();
        }

        let requires_fsync =
            slot.writer.durability_requirement().release_after() == AofCommitPoint::FsyncDurable;
        if requires_fsync && self.aof_fsync_fail_after == Some(0) {
            return self.maybe_inject_aof_fsync_failure();
        }

        Ok(())
    }

    #[cfg(not(test))]
    #[inline]
    pub(super) fn maybe_inject_transaction_aof_precommit_failure(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// EXEC
    ///
    /// Atomically validates WATCH state and executes queued commands for one
    /// connection, returning one RESP array with each command reply.
    ///
    /// Big-O: `O(W + Q * C)`, where `W` is watched keys, `Q` is queued commands,
    /// and `C` is the cost of each command. Response construction is linear in
    /// the serialized replies.
    ///
    /// Compatibility: Queue-time errors cause `EXECABORT`; runtime command
    /// errors are returned inside the EXEC array and do not stop later queued
    /// commands. A watched-key conflict returns a nil array.
    ///
    /// Notes: EXEC plans the watched and queued command keys first, then enters
    /// the sorted shard transaction gates for only that shard set. Full-keyspace
    /// commands take the rare all-shard path. AOF writes and response
    /// serialization happen after the gates are released.
    pub(super) fn execute_transaction(&mut self, conn_id: usize) -> (CommandResponse, bool) {
        if conn_id >= self.transaction_states.len() || !self.transaction_states[conn_id].queueing {
            return (CommandResponse::Static(RESP_ERR_EXEC_WITHOUT_MULTI), false);
        }

        if self.transaction_states[conn_id].dirty {
            self.clear_transaction_state(conn_id);
            return (CommandResponse::Static(RESP_ERR_EXECABORT), false);
        }

        let queued = std::mem::take(&mut self.transaction_states[conn_id].queued);
        self.transaction_states[conn_id].queueing = false;
        self.transaction_states[conn_id].dirty = false;
        self.transaction_states[conn_id].queued_bytes = 0;

        let plan = {
            let tx = &self.transaction_states[conn_id];
            match self.queued_transaction_plan(&queued, tx.watched.as_slice()) {
                Ok(plan) => plan,
                Err(_) => {
                    self.clear_watches(conn_id);
                    return (CommandResponse::Static(RESP_ERR_PROTOCOL), false);
                }
            }
        };
        let queued_has_write = plan.has_write;

        if self.aof_failed_for_write() && queued_has_write {
            self.clear_watches(conn_id);
            return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
        }

        if queued_has_write
            && let Err(error) = self.maybe_inject_transaction_aof_precommit_failure()
        {
            self.clear_watches(conn_id);
            self.aof_coordinator
                .mark_failed(self.id, "append_transaction_precommit", &error);
            return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
        }

        let keyspace = Arc::clone(&self.keyspace);
        let mut aof_batch = TransactionAofBatch::new();
        let mut command_results = Vec::with_capacity(queued.len());

        let transaction_result = {
            #[cfg(feature = "lock-profile")]
            let _lock_profile = keyspace
                .enter_lock_profile_scope(vortex_engine::keyspace::LockProfileClass::Transaction);
            let _transaction_gate = match &plan.gate_scope {
                TransactionGateScope::None => keyspace.enter_transaction_gate_for_keys(&[]),
                TransactionGateScope::Shards(plan) => {
                    keyspace.enter_transaction_gate_for_plan(plan)
                }
                TransactionGateScope::Full => keyspace.enter_all_shard_transaction_gate(),
            };
            drop(plan);

            let watched_changed = {
                let tx = &self.transaction_states[conn_id];
                !tx.watched.is_empty()
                    && keyspace.watched_keys_changed(tx.watch_epoch, tx.watched.as_slice())
            };
            if watched_changed {
                Err(CommandResponse::Static(RESP_NULL_ARRAY))
            } else {
                self.clear_watches(conn_id);
                for payload in &queued {
                    match self.execute_queued_payload(payload.as_ref(), &mut aof_batch) {
                        Ok(result) => command_results.push(result),
                        Err(error) => {
                            self.aof_coordinator
                                .mark_failed(self.id, "append_transaction", &error);
                            return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
                        }
                    }
                }
                Ok(())
            }
        };

        if let Err(response) = transaction_result {
            self.clear_watches(conn_id);
            return (response, false);
        }

        let mut response = Vec::with_capacity(32 + queued.len() * 8);
        push_resp_array_len(&mut response, queued.len());
        for result in command_results {
            match result {
                QueuedCommandResult::Static(bytes) => response.extend_from_slice(bytes),
                QueuedCommandResult::Engine(command_response) => {
                    append_cmd_result(&mut response, command_response);
                }
            }
        }

        if !aof_batch.is_empty()
            && let Err(error) = self.append_transaction_aof_batch(&aof_batch)
        {
            self.aof_coordinator
                .mark_failed(self.id, "append_transaction_batch", &error);
            return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
        }

        (CommandResponse::Owned(response.into_boxed_slice()), false)
    }
}
