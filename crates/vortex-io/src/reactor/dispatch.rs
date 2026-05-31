use super::*;

impl Reactor {
    pub(super) fn execute_queued_payload(
        &mut self,
        payload: &[u8],
        aof_batch: &mut TransactionAofBatch,
    ) -> io::Result<QueuedCommandResult> {
        let tape = BorrowedRespTape::parse_pipeline(payload)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "queued RESP parse failed"))?;
        let frame = tape
            .iter()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty queued command"))?;

        let dispatch = self.command_router.dispatch(&frame);
        match dispatch {
            DispatchResult::Dispatch { name, .. } => {
                let clock = CommandClock::new(self.cached_nanos, self.cached_unix_nanos);
                match self.command_executor.execute(name, &frame, clock) {
                    Some(executed) => {
                        let vortex_engine::commands::ExecutedCommand {
                            response: command_response,
                            aof_records,
                            aof_commit,
                            aof_payload,
                        } = executed;
                        if let Some(records) = aof_records {
                            for record in records {
                                let mut eviction_payload = Vec::with_capacity(
                                    32usize.saturating_add(record.key.as_bytes().len()),
                                );
                                push_resp_array_len(&mut eviction_payload, 2);
                                push_resp_bulk_string(&mut eviction_payload, b"DEL");
                                push_resp_bulk_string(&mut eviction_payload, record.key.as_bytes());
                                aof_batch
                                    .push_owned(record.lsn, eviction_payload.into_boxed_slice());
                            }
                        }
                        if let Some(aof_commit) = aof_commit {
                            let payload = aof_payload.as_deref().unwrap_or(payload);
                            aof_batch.push_copy(aof_commit.lsn(), payload);
                        }
                        Ok(QueuedCommandResult::Engine(command_response))
                    }
                    None => Ok(QueuedCommandResult::Static(
                        b"-ERR command not yet implemented\r\n",
                    )),
                }
            }
            DispatchResult::WrongArity { .. } => {
                Ok(QueuedCommandResult::Static(RESP_ERR_WRONG_ARGC))
            }
            DispatchResult::UnknownCommand => Ok(QueuedCommandResult::Static(RESP_ERR_UNKNOWN)),
        }
    }

    /// Dispatch a parsed tape frame through the engine.
    ///
    /// Uses O(1) perfect-hash lookup with SWAR uppercase normalization,
    /// then routes through the concrete shared-keyspace executor.
    /// Returns `(CommandResponse, should_close)` — the bool signals QUIT.
    pub(super) fn dispatch_command(
        &mut self,
        conn_id: usize,
        frame: &FrameRef<'_>,
    ) -> (CommandResponse, bool) {
        let Some((upper, len)) = Self::normalized_command_name(frame) else {
            return (CommandResponse::Static(RESP_ERR_UNKNOWN), false);
        };
        let command_name = &upper[..len];

        if conn_id < self.transaction_states.len() && self.transaction_states[conn_id].queueing {
            match command_name {
                b"MULTI" => {
                    if !Self::has_exact_arity(frame, 1) {
                        return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                    }
                    return (CommandResponse::Static(RESP_ERR_NESTED_MULTI), false);
                }
                b"EXEC" => {
                    if !Self::has_exact_arity(frame, 1) {
                        return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                    }
                    return self.execute_transaction(conn_id);
                }
                b"DISCARD" => {
                    if !Self::has_exact_arity(frame, 1) {
                        return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                    }
                    return self.discard_transaction(conn_id);
                }
                b"WATCH" => {
                    if !Self::has_min_arity(frame, 2) {
                        return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                    }
                    return (CommandResponse::Static(RESP_ERR_WATCH_INSIDE_MULTI), false);
                }
                _ => return (self.queue_transaction_command(conn_id, frame), false),
            }
        }

        match command_name {
            b"MULTI" => {
                if !Self::has_exact_arity(frame, 1) {
                    return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                }
                return self.begin_transaction(conn_id);
            }
            b"EXEC" => {
                if !Self::has_exact_arity(frame, 1) {
                    return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                }
                return (CommandResponse::Static(RESP_ERR_EXEC_WITHOUT_MULTI), false);
            }
            b"DISCARD" => {
                if !Self::has_exact_arity(frame, 1) {
                    return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                }
                return (
                    CommandResponse::Static(RESP_ERR_DISCARD_WITHOUT_MULTI),
                    false,
                );
            }
            b"WATCH" => {
                if !Self::has_min_arity(frame, 2) {
                    return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                }
                return (self.handle_watch(conn_id, frame), false);
            }
            b"UNWATCH" => {
                if !Self::has_exact_arity(frame, 1) {
                    return (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false);
                }
                self.clear_watches(conn_id);
                return (
                    CommandResponse::Static(vortex_engine::commands::RESP_OK),
                    false,
                );
            }
            b"BGREWRITEAOF" => return self.handle_bgrewriteaof(),
            b"CONFIG" => {
                if let Some(resp) = self.handle_config(frame) {
                    return resp;
                }
            }
            _ => {}
        }

        match CommandRouter::dispatch_normalized(frame, command_name) {
            DispatchResult::Dispatch { meta, name, .. } => {
                if self.aof_failed_for_write() && meta.flags.contains(CommandFlags::WRITE) {
                    return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
                }

                let clock = CommandClock::new(self.cached_nanos, self.cached_unix_nanos);
                let scope = command_keyspace_gate_scope(meta, frame);
                let execution_scope = if scope.full {
                    if scope.exclusive {
                        CommandExecutionScope::FullExclusive
                    } else {
                        CommandExecutionScope::Full
                    }
                } else if scope.keys.is_empty() {
                    CommandExecutionScope::None
                } else {
                    CommandExecutionScope::Keys(scope.keys.as_slice())
                };
                let executed = self.command_executor.execute_scoped(
                    execution_scope,
                    self.id,
                    name,
                    frame,
                    clock,
                );
                match executed {
                    Some(executed) => {
                        let vortex_engine::commands::ExecutedCommand {
                            response,
                            aof_records,
                            aof_commit,
                            aof_payload,
                        } = executed;
                        if let Some(records) = aof_records {
                            for record in records {
                                if let Err(error) = self
                                    .append_eviction_aof_record(record.lsn, record.key.as_bytes())
                                {
                                    self.aof_coordinator.mark_failed(
                                        self.id,
                                        "append_eviction_record",
                                        &error,
                                    );
                                    return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
                                }
                            }
                        }
                        if let Some(aof_commit) = aof_commit {
                            if let Err(error) =
                                self.append_to_aof(aof_commit, frame, aof_payload.as_deref())
                            {
                                self.aof_coordinator.mark_failed(
                                    self.id,
                                    "append_with_lsn",
                                    &error,
                                );
                                return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
                            }
                        }
                        match response {
                            CmdResult::Static(buf) => {
                                let close = meta.name == "QUIT";
                                (CommandResponse::Static(buf), close)
                            }
                            CmdResult::Inline(inline) => (CommandResponse::Inline(inline), false),
                            CmdResult::Owned(bytes) => (CommandResponse::Owned(bytes), false),
                            CmdResult::Resp(f) => (CommandResponse::Frame(f), false),
                        }
                    }
                    None => {
                        // Engine doesn't handle this command — shouldn't happen
                        // since all PHF commands are wired, but handle gracefully.
                        (
                            CommandResponse::Static(b"-ERR command not yet implemented\r\n"),
                            false,
                        )
                    }
                }
            }
            DispatchResult::WrongArity { .. } => {
                (CommandResponse::Static(RESP_ERR_WRONG_ARGC), false)
            }
            DispatchResult::UnknownCommand => (CommandResponse::Static(RESP_ERR_UNKNOWN), false),
        }
    }
}
