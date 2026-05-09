use super::*;

impl Reactor {
    pub(super) fn handle_close(&mut self, conn_id: usize) {
        self.mark_inflight_completed(conn_id, OpType::Close);
        self.maybe_finalize_close(conn_id);
    }

    // ── Helpers ────────────────────────────────────────────────────

    pub(super) fn submit_accept_rearm(&mut self) -> Result<(), SubmitError> {
        let token = CompletionToken::accept();
        let listener_fd = self.listener_fd;
        self.submit_backend_op("accept", |backend| {
            backend.submit_accept(ListenerFd::new(listener_fd), token)
        })
    }

    #[inline]
    pub(super) fn conn_token(
        conn_id: usize,
        generation: u32,
        op: OpType,
    ) -> Option<CompletionToken> {
        match encode_token(conn_id, generation, op) {
            Ok(token) => Some(token),
            Err(error) => {
                tracing::error!(
                    conn_id,
                    generation,
                    ?op,
                    ?error,
                    "failed to encode completion token"
                );
                None
            }
        }
    }

    #[inline]
    pub(super) fn live_conn_token(&self, token: CompletionToken) -> Option<(usize, OpType)> {
        let Ok(DecodedCompletionToken::Conn { id, generation, op }) = token.decode() else {
            return None;
        };
        let generation_valid = self
            .connections
            .get(id)
            .is_some_and(|_| self.generations.get(id).copied().unwrap_or(0) == generation);
        generation_valid.then_some((id, op))
    }

    pub(super) fn submit_cancel_for(&mut self, target: CompletionToken) {
        let cancel = match CompletionToken::cancel(target) {
            Ok(cancel) => cancel,
            Err(error) => {
                tracing::warn!(
                    target = target.raw(),
                    ?error,
                    "failed to encode cancel token"
                );
                return;
            }
        };
        if self
            .submit_backend_op("cancel", |backend| backend.submit_cancel(target, cancel))
            .is_ok()
        {
            self.mark_cancel_submitted_for_target(target);
        }
    }

    pub(super) fn submit_backend_op<F>(
        &mut self,
        op_name: &'static str,
        mut submit: F,
    ) -> Result<(), SubmitError>
    where
        F: FnMut(&mut Backend) -> Result<(), SubmitError>,
    {
        match submit(&mut self.backend) {
            Ok(()) => Ok(()),
            Err(SubmitError::QueueFull) => {
                self.keyspace.record_reactor_submit_sq_full_retry(self.id);
                let flush_counts_submit = self.flush_counts_submit_syscall();
                if let Err(flush_error) = self.backend.flush() {
                    if flush_counts_submit {
                        self.keyspace.record_reactor_backend_submit_syscall(self.id);
                    }
                    self.keyspace.record_reactor_submit_failure(self.id);
                    tracing::warn!(op = op_name, error = %flush_error, "submit retry flush failed");
                    return Err(flush_error);
                }
                if flush_counts_submit {
                    self.keyspace.record_reactor_backend_submit_syscall(self.id);
                }

                match submit(&mut self.backend) {
                    Ok(()) => Ok(()),
                    Err(retry_error) => {
                        self.keyspace.record_reactor_submit_failure(self.id);
                        tracing::warn!(op = op_name, error = %retry_error, "submit retry failed");
                        Err(retry_error)
                    }
                }
            }
            Err(error) => {
                self.keyspace.record_reactor_submit_failure(self.id);
                tracing::warn!(op = op_name, error = %error, "submit failed");
                Err(error)
            }
        }
    }

    #[inline]
    pub(super) fn fixed_buffer_id(
        &self,
        index: usize,
    ) -> Result<Option<FixedBufferId>, SubmitError> {
        if !self.fixed_buffers_enabled {
            return Ok(None);
        }

        FixedBufferId::new(index).map(Some)
    }

    /// Submit a read SQE for the given connection using the fixed buffer pool.
    pub(super) fn submit_read_for(&mut self, conn_id: usize, fd: RawFd) {
        if self
            .inflight_ops
            .get(conn_id)
            .is_some_and(|inflight| inflight.has(OpType::Read))
        {
            tracing::warn!(conn_id, "read submission skipped; read already in flight");
            return;
        }

        let (read_idx, cursor) = match self.connections.get(conn_id) {
            Some(c) => (c.read_buf_offset as usize, c.read_buf_len as usize),
            None => return,
        };

        let buf_size = self.buffer_pool.buffer_size();
        let remaining = buf_size - cursor;

        if remaining == 0 {
            // Buffer full — apply TCP backpressure. Don't re-arm the read SQE
            // until the parser has consumed bytes. If we reach this point after
            // parsing (i.e., the command itself is larger than the buffer), the
            // connection is unsalvageable — close it.
            tracing::warn!(
                conn_id,
                buf_size,
                "read buffer full, closing connection (backpressure)"
            );
            self.close_connection(conn_id);
            return;
        }

        if self.read_backpressure_active() {
            self.disable_read_for_backpressure(conn_id);
            return;
        }

        // SAFETY: read_idx is a valid pool index leased for this connection.
        // cursor bytes have been written; we read into the remainder.
        let buf_ptr = unsafe { self.buffer_pool.ptr(read_idx).add(cursor) };
        let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
        let Some(token) = Self::conn_token(conn_id, cgen, OpType::Read) else {
            self.close_connection(conn_id);
            return;
        };
        let fixed = match self.fixed_buffer_id(read_idx) {
            Ok(fixed) => fixed,
            Err(error) => {
                tracing::warn!(conn_id, read_idx, error = %error, "invalid read buffer id");
                self.close_connection(conn_id);
                return;
            }
        };
        let submit_name = if fixed.is_some() {
            "read_fixed"
        } else {
            "read"
        };
        // SAFETY: `buf_ptr..buf_ptr+remaining` is the unfilled tail of the
        // connection's leased read buffer and remains live until the read CQE
        // is handled.
        let lease = match unsafe { ReadLease::new(ConnFd::new(fd), buf_ptr, remaining, fixed) } {
            Ok(lease) => lease,
            Err(error) => {
                tracing::warn!(conn_id, error = %error, "failed to create read lease");
                self.close_connection(conn_id);
                return;
            }
        };
        if self
            .submit_backend_op(submit_name, |backend| backend.submit_read(lease, token))
            .is_ok()
        {
            self.mark_inflight_submitted(conn_id, OpType::Read);
        } else {
            self.close_connection(conn_id);
        }
    }

    /// Initiate a connection close.
    pub(super) fn close_connection(&mut self, conn_id: usize) {
        let (fd, timer_slot) = match self.connections.get(conn_id) {
            Some(c) if !self.connections.is_closing(conn_id) => (c.fd, c.timer_slot),
            _ => return, // Already closing or not found — prevent double-close.
        };

        // Cancel pending idle timer.
        self.timer_wheel.cancel(timer_slot);
        self.clear_command_accumulator(conn_id);

        if let Err(error) = self.connections.transition_to_closing(conn_id) {
            tracing::warn!(
                conn_id,
                ?error,
                "failed to transition connection to closing"
            );
            return;
        }
        if conn_id < self.close_started_nanos.len() && self.close_started_nanos[conn_id] == 0 {
            self.close_started_nanos[conn_id] = self.profile_metric_start().unwrap_or(0);
        }

        let inflight = self.inflight_ops.get(conn_id).copied().unwrap_or_default();

        let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
        if inflight.read {
            if let Some(token) = Self::conn_token(conn_id, cgen, OpType::Read) {
                self.submit_cancel_for(token);
            }
        }
        if inflight.write {
            if let Some(token) = Self::conn_token(conn_id, cgen, OpType::Write) {
                self.submit_cancel_for(token);
            }
        }
        if inflight.writev {
            if let Some(token) = Self::conn_token(conn_id, cgen, OpType::Writev) {
                self.submit_cancel_for(token);
            }
        }

        let Some(token) = Self::conn_token(conn_id, cgen, OpType::Close) else {
            tracing::warn!(
                conn_id,
                "failed to encode close token; fd will be closed after in-flight operations drain"
            );
            self.maybe_finalize_close(conn_id);
            return;
        };
        if self
            .submit_backend_op("close", |backend| {
                backend.submit_close(ConnFd::new(fd), token)
            })
            .is_ok()
        {
            self.connections.mark_close_submitted(conn_id);
            self.mark_inflight_submitted(conn_id, OpType::Close);
        } else {
            tracing::warn!(
                conn_id,
                "submit_close failed; fd will be closed after in-flight operations drain"
            );
            self.maybe_finalize_close(conn_id);
        }
    }

    #[inline]
    pub(super) fn mark_inflight_submitted(&mut self, conn_id: usize, op: OpType) {
        if let Some(inflight) = self.inflight_ops.get_mut(conn_id) {
            inflight.mark_submitted(op);
        }
    }

    #[inline]
    pub(super) fn mark_inflight_completed(&mut self, conn_id: usize, op: OpType) {
        if let Some(inflight) = self.inflight_ops.get_mut(conn_id) {
            inflight.mark_completed(op);
        }
    }

    #[inline]
    pub(super) fn mark_cancel_submitted_for_target(&mut self, target: CompletionToken) {
        let Some((conn_id, op)) = self.live_conn_token(target) else {
            return;
        };
        if let Some(inflight) = self.inflight_ops.get_mut(conn_id) {
            inflight.mark_cancel_submitted(op);
        }
    }

    #[inline]
    pub(super) fn mark_cancel_completed_for_target(
        &mut self,
        target: CompletionToken,
    ) -> Option<usize> {
        let (conn_id, op) = self.live_conn_token(target)?;
        if let Some(inflight) = self.inflight_ops.get_mut(conn_id) {
            inflight.mark_cancel_completed(op);
        }
        Some(conn_id)
    }

    pub(super) fn maybe_finalize_close(&mut self, conn_id: usize) {
        if !self.connection_ready_to_finalize(conn_id) {
            return;
        }

        if self
            .close_finalization_pending
            .get(conn_id)
            .copied()
            .unwrap_or(false)
        {
            return;
        }

        if let Some(pending) = self.close_finalization_pending.get_mut(conn_id) {
            *pending = true;
        }
        self.pending_close_finalization.push_back(conn_id);
    }

    #[inline]
    pub(super) fn connection_ready_to_finalize(&self, conn_id: usize) -> bool {
        if self.connections.get(conn_id).is_none() {
            return false;
        }
        if !self.connections.is_closing(conn_id) {
            return false;
        }
        !self
            .inflight_ops
            .get(conn_id)
            .is_some_and(InflightSet::has_any)
    }

    pub(super) fn finalize_close_now(&mut self, conn_id: usize) {
        if !self.connection_ready_to_finalize(conn_id) {
            return;
        }
        let Some(drained) = self.connections.drain_closing(conn_id) else {
            return;
        };
        let needs_direct_close = drained.needs_direct_close();
        let connection = drained.into_meta();
        let read_idx = connection.read_buf_offset as usize;
        let write_idx = connection.write_buf_offset as usize;
        self.overload
            .adjust_pending_response_bytes(connection.write_buf_len as usize, 0);
        self.buffer_pool.release_index(read_idx);
        if write_idx != read_idx {
            self.buffer_pool.release_index(write_idx);
        }
        if needs_direct_close {
            // SAFETY: no backend close CQE is outstanding and all read/write
            // operations have reached terminal state, so the drained slot still
            // owns this fd.
            unsafe {
                libc::close(connection.fd);
            }
        }
        if conn_id < self.writev_states.len() {
            self.writev_states[conn_id].clear();
        }
        if conn_id < self.inflight_ops.len() {
            self.inflight_ops[conn_id] = InflightSet::default();
        }
        if conn_id < self.close_started_nanos.len() {
            let started = self.close_started_nanos[conn_id];
            if started != 0 {
                let elapsed = self.elapsed_profile_metric_nanos(Some(started));
                self.keyspace
                    .record_reactor_close_drain_nanos(self.id, elapsed);
                self.close_started_nanos[conn_id] = 0;
            }
        }
        if let Some(pending) = self.close_finalization_pending.get_mut(conn_id) {
            *pending = false;
        }
        self.clear_transaction_state(conn_id);
        self.clear_command_accumulator(conn_id);
        self.clear_overload_connection_state(conn_id);

        tracing::debug!(reactor_id = self.id, conn_id, "connection closed");
    }

    pub(super) fn drain_close_finalization_until_idle(&mut self) {
        while !self.pending_close_finalization.is_empty() {
            let _ = self.run_close_drain_slice();
        }
    }

    // ── Timer management ───────────────────────────────────────────

    /// Handle a single expired timer entry.
    pub(super) fn handle_expired_timer(&mut self, conn_id: usize, timer_gen: u32) {
        // Validate generation — skip stale entries where the slab slot was
        // reused by a different connection.
        let current_gen = self.generations.get(conn_id).copied().unwrap_or(0);
        if current_gen != timer_gen {
            return;
        }

        let (last_active, is_closing) = match self.connections.get(conn_id) {
            Some(c) => (c.last_active, self.connections.is_closing(conn_id)),
            None => return,
        };

        if is_closing {
            return;
        }

        let idle = self.now_secs.saturating_sub(last_active);
        if idle >= self.connection_timeout {
            tracing::debug!(conn_id, idle, "closing idle connection");
            self.close_connection(conn_id);
        } else {
            // Connection was active since the timer was scheduled — reschedule
            // from last_active rather than now to avoid timer drift.
            let new_deadline = last_active + self.connection_timeout;
            let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
            let entry = self.timer_wheel.schedule(conn_id, cgen, new_deadline);
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.timer_slot = entry;
            }
        }
    }

    /// Enter drain mode: stop accepting new connections and gracefully close
    /// existing ones (flush pending writes first).
    pub(super) fn enter_drain_mode(&mut self) {
        self.draining = true;
        tracing::info!(reactor_id = self.id, "entering drain mode");
        self.submit_cancel_for(CompletionToken::accept());

        // Graceful drain: flush connections with pending writes, close the rest.
        let conn_ids: Vec<usize> = self.connections.ids().collect();
        for conn_id in conn_ids {
            let (fd, write_len, write_idx, is_closing) = match self.connections.get(conn_id) {
                Some(c) => (
                    c.fd,
                    c.write_buf_len as usize,
                    c.write_buf_offset as usize,
                    self.connections.is_closing(conn_id),
                ),
                None => continue,
            };

            if is_closing {
                continue;
            }

            if write_len > 0 {
                let _ = (fd, write_idx);
                if let Some(c) = self.connections.get_mut(conn_id) {
                    c.flags |= ConnectionFlags::CLOSE_AFTER_WRITE;
                }
            } else {
                // No pending writes — close immediately.
                self.close_connection(conn_id);
            }
        }
    }

    /// Returns the number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    // ── Graceful I/O drain ─────────────────────────────────────────

    /// Cancel all in-flight I/O operations and drain the completion queue.
    ///
    /// Must be called before the `BufferPool` is dropped to prevent the
    /// kernel from DMA-ing into unmapped memory (`use-after-unmap`).
    pub(super) fn drain_inflight_io(&mut self) {
        self.draining = true;
        let conn_ids: Vec<usize> = self.connections.ids().collect();
        for conn_id in conn_ids {
            if self.connections.is_closing(conn_id) {
                continue;
            }
            self.close_connection(conn_id);
        }

        // Also cancel the accept token.
        self.submit_cancel_for(CompletionToken::accept());

        while !self.connections.is_empty() {
            self.drain_close_finalization_until_idle();
            if self.connections.is_empty() {
                break;
            }

            let flush_counts_submit = self.flush_counts_submit_syscall();
            let flush_result = self.backend.flush();
            if flush_counts_submit {
                self.keyspace.record_reactor_backend_submit_syscall(self.id);
            }
            if let Err(error) = flush_result {
                self.keyspace.record_reactor_submit_failure(self.id);
                tracing::warn!(
                    reactor_id = self.id,
                    error = %error,
                    "backend flush failed during shutdown drain"
                );
            }

            self.cqe_buf.clear();
            let completions_count_submit = self.completions_count_submit_syscall();
            match self.backend.completions(&mut self.cqe_buf) {
                Ok(0) => {
                    if completions_count_submit {
                        self.keyspace.record_reactor_backend_submit_syscall(self.id);
                    }
                    std::thread::yield_now();
                    continue;
                }
                Ok(_) => {
                    if completions_count_submit {
                        self.keyspace.record_reactor_backend_submit_syscall(self.id);
                    }
                }
                Err(error) => {
                    if completions_count_submit {
                        self.keyspace.record_reactor_backend_submit_syscall(self.id);
                    }
                    self.keyspace.record_reactor_submit_failure(self.id);
                    tracing::warn!(
                        reactor_id = self.id,
                        error = %error,
                        "completion drain failed during shutdown"
                    );
                    std::thread::yield_now();
                    continue;
                }
            }

            let completions = std::mem::take(&mut self.cqe_buf);
            for cqe in &completions {
                self.handle_completion(cqe);
            }
            self.cqe_buf = completions;
            self.drain_close_finalization_until_idle();
        }
    }
}
