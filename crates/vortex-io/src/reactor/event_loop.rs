use super::*;

impl Reactor {
    /// Runs the reactor event loop. Blocks until shutdown.
    pub fn run(&mut self) {
        self.running = true;
        self.start_nanos = Timestamp::now().as_nanos();
        tracing::info!(reactor_id = self.id, "reactor starting");

        // Submit initial accept.
        if let Err(e) = self.submit_accept_rearm() {
            tracing::error!(error = %e, "failed to submit initial accept");
            return;
        }

        loop {
            // Cache monotonic time once per iteration and derive the seconds view from it.
            self.cached_nanos = Timestamp::now().as_nanos();
            self.cached_unix_nanos = current_unix_time_nanos();
            self.now_secs = ((self.cached_nanos - self.start_nanos) / 1_000_000_000) as u32;
            self.local_metrics.record_loop_iteration();

            // 1. Flush pending submissions.
            self.publish_backend_queue_pressure_hot();
            let flush_counts_submit = self.flush_counts_submit_syscall();
            if let Err(e) = self.backend.flush() {
                if flush_counts_submit {
                    self.keyspace.record_reactor_backend_submit_syscall(self.id);
                }
                self.keyspace.record_reactor_submit_failure(self.id);
                tracing::error!(error = %e, "backend flush failed");
                break;
            }
            if flush_counts_submit {
                self.keyspace.record_reactor_backend_submit_syscall(self.id);
            }

            // 2. Drain completions.
            let completion_phase_start = self.profile_metric_start();
            let mut saw_completion_work = false;
            let mut completion_budget = SliceBudget::new(self.config.budgets.completion.get());
            let mut completion_budget_exhausted = false;

            if !self.pending_completions.is_empty() {
                saw_completion_work |=
                    self.process_pending_completion_queue(&mut completion_budget);
                completion_budget_exhausted |= !self.pending_completions.is_empty();
            }

            if !completion_budget.is_empty() {
                self.cqe_buf.clear();
                let completions_count_submit = self.completions_count_submit_syscall();
                match self.backend.completions(&mut self.cqe_buf) {
                    Ok(_) => {
                        if completions_count_submit {
                            self.keyspace.record_reactor_backend_submit_syscall(self.id);
                        }
                    }
                    Err(e) => {
                        if completions_count_submit {
                            self.keyspace.record_reactor_backend_submit_syscall(self.id);
                        }
                        self.keyspace.record_reactor_submit_failure(self.id);
                        tracing::error!(error = %e, "backend completions failed");
                        break;
                    }
                }
                if !self.cqe_buf.is_empty() {
                    saw_completion_work = true;
                    self.local_metrics
                        .record_completion_batch(self.cqe_buf.len());
                    let mut completions = std::mem::take(&mut self.cqe_buf);
                    completion_budget_exhausted |=
                        self.process_completion_vec(&mut completions, &mut completion_budget);
                    completions.clear();
                    self.cqe_buf = completions;
                }
            }

            // 3b. Fast-path: process newly submitted ops (e.g., writes after
            //     reads) immediately while staying inside the same completion
            //     budget. Uses drain_cq() for a non-blocking CQ peek.
            if !completion_budget.is_empty() && !completion_budget_exhausted {
                self.publish_backend_queue_pressure_hot();
                self.cqe_buf.clear();
                let drain_ok = match self.backend.drain_cq(&mut self.cqe_buf) {
                    Ok(count) => {
                        if self.drain_cq_counts_submit_syscall(count) {
                            self.keyspace.record_reactor_backend_submit_syscall(self.id);
                        }
                        true
                    }
                    Err(error) => {
                        self.keyspace.record_reactor_submit_failure(self.id);
                        tracing::debug!(error = %error, "backend non-blocking completion drain failed");
                        false
                    }
                };
                if drain_ok && !self.cqe_buf.is_empty() {
                    saw_completion_work = true;
                    self.local_metrics
                        .record_completion_batch(self.cqe_buf.len());
                    let mut fast_completions = std::mem::take(&mut self.cqe_buf);
                    completion_budget_exhausted |=
                        self.process_completion_vec(&mut fast_completions, &mut completion_budget);
                    fast_completions.clear();
                    self.cqe_buf = fast_completions;
                }
            }
            if completion_budget_exhausted {
                self.keyspace
                    .record_reactor_completion_budget_exhaustion(self.id);
            }
            if saw_completion_work {
                let elapsed = self.elapsed_profile_metric_nanos(completion_phase_start);
                self.keyspace
                    .record_reactor_completion_nanos(self.id, elapsed);
            }

            // 4. Run bounded maintenance classes. Each class owns its own
            // resume cursor and consumes at most one maintenance unit per
            // scheduler dispatch.
            self.run_shared_nothing_scheduler();
            self.run_maintenance_scheduler();
            self.run_admission_resume_slice();

            // 5. Check for force-kill (immediate exit, no flushing).
            if self.coordinator.is_force_kill() {
                tracing::warn!(
                    reactor_id = self.id,
                    "force-kill received, exiting immediately"
                );
                break;
            }

            // 6. Check for graceful shutdown.
            if self.coordinator.is_draining() && !self.draining {
                self.enter_drain_mode();
            }

            if self.draining && self.connections.is_empty() {
                tracing::info!(reactor_id = self.id, "drain complete, exiting");
                break;
            }

            // 7. Reset the per-iteration arena allocator.
            self.arena.reset();
        }

        // Flush and sync AOF before tearing down I/O.
        if let Some(ref mut slot) = self.aof_writer {
            if let Err(e) = slot.writer.flush_and_sync() {
                tracing::error!(reactor_id = self.id, error = %e, "AOF final flush failed");
            } else {
                tracing::info!(reactor_id = self.id, "AOF flushed and synced on shutdown");
            }
        }

        // Cancel all in-flight I/O and drain completions before dropping the
        // BufferPool. This prevents use-after-unmap when MmapRegion drops.
        self.drain_inflight_io();

        self.running = false;

        // Signal the coordinator that this reactor is done.
        self.coordinator.reactor_finished(self.id);

        self.close_listener_fd();
        tracing::info!(reactor_id = self.id, "reactor stopped");
    }

    #[inline]
    fn publish_backend_queue_pressure_hot(&mut self) {
        if self.config.telemetry_mode.profile_timers_enabled() {
            self.publish_backend_queue_pressure();
        }
    }

    #[inline]
    pub(super) fn publish_backend_queue_pressure(&mut self) {
        let status = self.backend.queue_status();
        if status.has_runtime_signal() {
            self.publish_backend_queue_status(status);
        }
    }

    #[inline]
    fn publish_backend_queue_status(&self, status: BackendQueueStatus) {
        self.keyspace.record_reactor_backend_queue_status(
            self.id,
            status.sq_occupancy,
            status.sq_capacity,
            status.cq_occupancy,
            status.cq_capacity,
            status.cq_overflow_delta,
        );
    }

    pub(super) fn close_listener_fd(&mut self) {
        let fd = std::mem::replace(&mut self.listener_fd, -1);
        if fd >= 0 {
            // SAFETY: `fd` is the listener descriptor owned by this reactor,
            // and replacing it with -1 prevents a second close through `Drop`.
            unsafe {
                libc::close(fd);
            }
        }
    }

    /// Signals the reactor to stop (initiates graceful shutdown).
    pub fn stop(&self) {
        self.coordinator.initiate();
    }

    /// Returns whether the reactor is currently running.
    pub fn is_running(&self) -> bool {
        self.running
    }

    pub(super) fn run_shared_nothing_scheduler(&mut self) {
        if self.shared_nothing.is_none() {
            return;
        }

        let budget = self.config.budgets.command.get().min(256);
        if let Some(runtime) = self.shared_nothing.as_mut() {
            runtime.drain_owner_commands(budget);
            runtime.drain_replies(budget);
        }
        self.flush_shared_nothing_replies(budget);
    }

    fn flush_shared_nothing_replies(&mut self, budget: usize) {
        if budget == 0 {
            return;
        }
        let mut connection_ids = std::mem::take(&mut self.shared_nothing_flush_ids);
        connection_ids.clear();
        connection_ids.extend(self.connections.ids().take(budget));
        for conn_id in connection_ids.iter().copied() {
            if self.connections.is_closing(conn_id) {
                continue;
            }
            if self
                .inflight_ops
                .get(conn_id)
                .is_some_and(|inflight| inflight.has(OpType::Write) || inflight.has(OpType::Writev))
            {
                continue;
            }
            if !self.publish_shared_nothing_ready(conn_id) {
                self.close_connection(conn_id);
                continue;
            }
            if self.writev_states[conn_id].queued_len() == 0 {
                continue;
            }
            let Some(fd) = self.connections.get(conn_id).map(|conn| conn.fd) else {
                continue;
            };
            self.finish_command_processing(conn_id, fd, false, false);
        }
        connection_ids.clear();
        self.shared_nothing_flush_ids = connection_ids;
    }

    pub(super) fn process_pending_completion_queue(&mut self, budget: &mut SliceBudget) -> bool {
        let mut saw_work = false;
        while !budget.is_empty() {
            let Some(cqe) = self.pending_completions.pop_front() else {
                return saw_work;
            };
            budget.consume_one();
            saw_work = true;
            self.handle_completion(&cqe);
        }
        saw_work
    }

    pub(super) fn process_completion_vec(
        &mut self,
        completions: &mut Vec<Completion>,
        budget: &mut SliceBudget,
    ) -> bool {
        let mut index = 0usize;
        while index < completions.len() && !budget.is_empty() {
            budget.consume_one();
            self.handle_completion(&completions[index]);
            index += 1;
        }

        if index < completions.len() {
            self.pending_completions.extend(completions.drain(index..));
            return true;
        }
        false
    }

    pub(super) fn handle_completion(&mut self, cqe: &Completion) {
        match cqe.token.decode() {
            Ok(DecodedCompletionToken::Accept) => self.handle_accept(cqe),
            Ok(DecodedCompletionToken::Wake) => {
                if let Err(error) = self.backend.rearm_wakeup() {
                    self.keyspace.record_reactor_submit_failure(self.id);
                    tracing::warn!(
                        reactor_id = self.id,
                        error = %error,
                        "failed to rearm backend wakeup"
                    );
                }
                self.run_shared_nothing_scheduler();
            }
            Ok(DecodedCompletionToken::Cancel { target }) => {
                self.handle_cancel_completion(target, cqe);
            }
            Ok(DecodedCompletionToken::Conn { id, generation, op }) => {
                // Stale CQEs from closed/reused slots are silently discarded.
                let valid = self
                    .connections
                    .get(id)
                    .is_some_and(|_| self.generations.get(id).copied().unwrap_or(0) == generation);
                if !valid {
                    return;
                }

                let expected = self
                    .inflight_ops
                    .get(id)
                    .is_some_and(|inflight| inflight.has(op));
                if !expected {
                    self.unexpected_completion_tokens =
                        self.unexpected_completion_tokens.saturating_add(1);
                    tracing::warn!(
                        conn_id = id,
                        generation,
                        ?op,
                        "dropping completion for operation not tracked as in-flight"
                    );
                    return;
                }

                match op {
                    OpType::Read => self.handle_read(id, cqe),
                    OpType::Write | OpType::Writev => self.handle_write(id, op, cqe),
                    OpType::Close => self.handle_close(id),
                }
            }
            Err(error) => {
                self.invalid_completion_tokens = self.invalid_completion_tokens.saturating_add(1);
                tracing::warn!(
                    token = cqe.token.raw(),
                    ?error,
                    "dropping malformed completion token"
                );
            }
        }
    }

    pub(super) fn handle_cancel_completion(&mut self, target: CompletionToken, cqe: &Completion) {
        let result = self.classify_cancel_completion(target, cqe.result);
        let cancel_conn_id = self.mark_cancel_completed_for_target(target);
        let mut terminal_conn_id = None;

        match result {
            CancelResult::Canceled | CancelResult::AlreadyTerminal => {}
            CancelResult::NotFound => {
                tracing::warn!(
                    target = target.raw(),
                    result = cqe.result,
                    "cancel target not found; treating tracked target as terminal"
                );
                if let Some((conn_id, op)) = self.live_conn_token(target) {
                    self.mark_inflight_completed(conn_id, op);
                    terminal_conn_id = Some(conn_id);
                }
            }
            CancelResult::BackendError { errno } => {
                tracing::warn!(
                    target = target.raw(),
                    errno,
                    "backend cancel request failed"
                );
            }
        }

        if let Some(conn_id) = cancel_conn_id.or(terminal_conn_id) {
            self.maybe_finalize_close(conn_id);
        }
    }

    #[inline]
    pub(super) fn classify_cancel_completion(
        &self,
        target: CompletionToken,
        result: i32,
    ) -> CancelResult {
        CancelResult::from_completion_result(result, self.cancel_target_inflight(target))
    }

    pub(super) fn cancel_target_inflight(&self, target: CompletionToken) -> bool {
        self.live_conn_token(target).is_some_and(|(id, op)| {
            self.inflight_ops
                .get(id)
                .is_some_and(|inflight| inflight.has(op))
        })
    }
}
