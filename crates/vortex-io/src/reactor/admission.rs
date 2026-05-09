use super::*;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ReactorOverloadSnapshot {
    pub(super) pending_response_bytes: usize,
    pub(super) pending_response_bytes_peak: usize,
    pub(super) parser_accumulator_bytes: usize,
    pub(super) parser_accumulator_bytes_peak: usize,
    pub(super) writev_backlog_bytes: usize,
    pub(super) writev_backlog_bytes_peak: usize,
    pub(super) aof_pending_bytes: u64,
    pub(super) maintenance_debt: usize,
    pub(super) maintenance_debt_peak: usize,
    pub(super) read_disabled_connections: usize,
    pub(super) read_disabled_connections_peak: usize,
    pub(super) deferred_commands: usize,
    pub(super) deferred_commands_peak: usize,
}

#[derive(Debug)]
pub(super) struct ReactorOverloadState {
    pending_response_bytes: usize,
    pending_response_bytes_peak: usize,
    parser_accumulator_bytes: usize,
    parser_accumulator_bytes_peak: usize,
    writev_backlog_bytes: usize,
    writev_backlog_bytes_peak: usize,
    maintenance_debt_peak: usize,
    read_disabled: VecDeque<usize>,
    read_disabled_pending: Vec<bool>,
    read_disabled_count: usize,
    read_disabled_peak: usize,
    deferred_commands: VecDeque<usize>,
    deferred_command_pending: Vec<bool>,
    deferred_command_count: usize,
    deferred_command_peak: usize,
    accept_rearm_deferred: bool,
}

impl ReactorOverloadState {
    pub(super) fn with_capacity(max_connections: usize) -> Self {
        Self {
            pending_response_bytes: 0,
            pending_response_bytes_peak: 0,
            parser_accumulator_bytes: 0,
            parser_accumulator_bytes_peak: 0,
            writev_backlog_bytes: 0,
            writev_backlog_bytes_peak: 0,
            maintenance_debt_peak: 0,
            read_disabled: VecDeque::new(),
            read_disabled_pending: vec![false; max_connections],
            read_disabled_count: 0,
            read_disabled_peak: 0,
            deferred_commands: VecDeque::new(),
            deferred_command_pending: vec![false; max_connections],
            deferred_command_count: 0,
            deferred_command_peak: 0,
            accept_rearm_deferred: false,
        }
    }

    #[inline]
    pub(super) fn adjust_pending_response_bytes(&mut self, old: usize, new: usize) {
        self.pending_response_bytes = self
            .pending_response_bytes
            .saturating_sub(old)
            .saturating_add(new);
        self.writev_backlog_bytes = self.pending_response_bytes;
        self.pending_response_bytes_peak = self
            .pending_response_bytes_peak
            .max(self.pending_response_bytes);
        self.writev_backlog_bytes_peak = self
            .writev_backlog_bytes_peak
            .max(self.writev_backlog_bytes);
    }

    #[inline]
    pub(super) fn add_parser_accumulator_bytes(&mut self, bytes: usize) {
        self.parser_accumulator_bytes = self.parser_accumulator_bytes.saturating_add(bytes);
        self.parser_accumulator_bytes_peak = self
            .parser_accumulator_bytes_peak
            .max(self.parser_accumulator_bytes);
    }

    #[inline]
    pub(super) fn subtract_parser_accumulator_bytes(&mut self, bytes: usize) {
        self.parser_accumulator_bytes = self.parser_accumulator_bytes.saturating_sub(bytes);
    }

    #[inline]
    pub(super) fn queue_read_disabled(&mut self, conn_id: usize) -> bool {
        let Some(pending) = self.read_disabled_pending.get_mut(conn_id) else {
            return false;
        };
        if *pending {
            return false;
        }
        *pending = true;
        self.read_disabled.push_back(conn_id);
        self.read_disabled_count = self.read_disabled_count.saturating_add(1);
        self.read_disabled_peak = self.read_disabled_peak.max(self.read_disabled_count);
        true
    }

    #[inline]
    pub(super) fn pop_read_disabled(&mut self) -> Option<usize> {
        while let Some(conn_id) = self.read_disabled.pop_front() {
            if self
                .read_disabled_pending
                .get(conn_id)
                .copied()
                .unwrap_or(false)
            {
                return Some(conn_id);
            }
        }
        None
    }

    #[inline]
    pub(super) fn clear_read_disabled(&mut self, conn_id: usize) {
        if let Some(pending) = self.read_disabled_pending.get_mut(conn_id)
            && *pending
        {
            *pending = false;
            self.read_disabled_count = self.read_disabled_count.saturating_sub(1);
        }
    }

    #[inline]
    pub(super) fn queue_deferred_command(&mut self, conn_id: usize) -> bool {
        let Some(pending) = self.deferred_command_pending.get_mut(conn_id) else {
            return false;
        };
        if *pending {
            return false;
        }
        *pending = true;
        self.deferred_commands.push_back(conn_id);
        self.deferred_command_count = self.deferred_command_count.saturating_add(1);
        self.deferred_command_peak = self.deferred_command_peak.max(self.deferred_command_count);
        true
    }

    #[inline]
    pub(super) fn pop_deferred_command(&mut self) -> Option<usize> {
        while let Some(conn_id) = self.deferred_commands.pop_front() {
            if self
                .deferred_command_pending
                .get(conn_id)
                .copied()
                .unwrap_or(false)
            {
                return Some(conn_id);
            }
        }
        None
    }

    #[inline]
    pub(super) fn clear_deferred_command(&mut self, conn_id: usize) {
        if let Some(pending) = self.deferred_command_pending.get_mut(conn_id)
            && *pending
        {
            *pending = false;
            self.deferred_command_count = self.deferred_command_count.saturating_sub(1);
        }
    }

    #[inline]
    pub(super) fn mark_accept_rearm_deferred(&mut self) {
        self.accept_rearm_deferred = true;
    }

    #[inline]
    pub(super) fn take_accept_rearm_deferred(&mut self) -> bool {
        let deferred = self.accept_rearm_deferred;
        self.accept_rearm_deferred = false;
        deferred
    }

    #[inline]
    pub(super) fn accept_rearm_deferred(&self) -> bool {
        self.accept_rearm_deferred
    }

    #[inline]
    pub(super) fn note_maintenance_debt(&mut self, debt: usize) {
        self.maintenance_debt_peak = self.maintenance_debt_peak.max(debt);
    }

    #[inline]
    pub(super) fn snapshot(
        &self,
        aof_pending_bytes: u64,
        maintenance_debt: usize,
    ) -> ReactorOverloadSnapshot {
        ReactorOverloadSnapshot {
            pending_response_bytes: self.pending_response_bytes,
            pending_response_bytes_peak: self.pending_response_bytes_peak,
            parser_accumulator_bytes: self.parser_accumulator_bytes,
            parser_accumulator_bytes_peak: self.parser_accumulator_bytes_peak,
            writev_backlog_bytes: self.writev_backlog_bytes,
            writev_backlog_bytes_peak: self.writev_backlog_bytes_peak,
            aof_pending_bytes,
            maintenance_debt,
            maintenance_debt_peak: self.maintenance_debt_peak.max(maintenance_debt),
            read_disabled_connections: self.read_disabled_count,
            read_disabled_connections_peak: self.read_disabled_peak,
            deferred_commands: self.deferred_command_count,
            deferred_commands_peak: self.deferred_command_peak,
        }
    }
}

impl Reactor {
    #[inline]
    pub(super) fn accept_connection_limit(&self) -> usize {
        let percent = self
            .config
            .overload_policy
            .accept_throttle_connection_percent
            .clamp(1, 100) as usize;
        self.config
            .max_connections
            .saturating_mul(percent)
            .div_ceil(100)
    }

    #[inline]
    pub(super) fn aof_pending_bytes(&self) -> u64 {
        self.aof_writer
            .as_ref()
            .map_or(0, |slot| slot.writer.pending_bytes())
    }

    #[inline]
    pub(super) fn maintenance_debt(&self) -> usize {
        let timer_due = usize::from(
            self.connection_timeout != 0
                && self.timer_wheel.current_tick() <= self.now_secs
                && !self.connections.is_empty(),
        );
        let active_expiry_due = usize::from(
            self.cached_nanos >= self.next_active_expiry_nanos && self.keyspace.has_expiring_keys(),
        );
        let eviction_due = usize::from(self.keyspace.eviction_pressure_active());
        let aof_due = usize::from(
            self.aof_writer
                .as_ref()
                .is_some_and(|slot| slot.writer.pending_writes() != 0),
        );
        self.pending_close_finalization
            .len()
            .saturating_add(self.pending_expired_timers.len())
            .saturating_add(timer_due)
            .saturating_add(active_expiry_due)
            .saturating_add(eviction_due)
            .saturating_add(aof_due)
    }

    #[inline]
    pub(super) fn note_maintenance_debt(&mut self) -> usize {
        let debt = self.maintenance_debt();
        self.overload.note_maintenance_debt(debt);
        debt
    }

    #[inline]
    pub(super) fn read_backpressure_active(&mut self) -> bool {
        let policy = self.config.overload_policy;
        let maintenance_debt = self.note_maintenance_debt();
        self.overload.pending_response_bytes >= policy.read_disable_pending_response_bytes
            || self.overload.parser_accumulator_bytes
                >= policy.read_disable_parser_accumulator_bytes
            || self.overload.writev_backlog_bytes >= policy.writev_backlog_bytes
            || maintenance_debt >= policy.maintenance_debt
    }

    #[inline]
    pub(super) fn accept_backpressure_active(&mut self) -> bool {
        self.connection_count() >= self.accept_connection_limit()
            || self.read_backpressure_active()
            || self.aof_pending_bytes() >= self.config.overload_policy.aof_pending_bytes
    }

    #[inline]
    pub(super) fn aof_write_backpressure_active(&self) -> bool {
        self.aof_writer.is_some()
            && self.aof_pending_bytes() >= self.config.overload_policy.aof_pending_bytes
    }

    pub(super) fn frame_writes_to_aof(&self, conn_id: usize, frame: &FrameRef<'_>) -> bool {
        let Some((upper, len)) = Self::normalized_command_name(frame) else {
            return false;
        };
        let command_name = &upper[..len];
        if command_name == b"EXEC"
            && self
                .transaction_states
                .get(conn_id)
                .is_some_and(|tx| tx.queueing && !tx.queued.is_empty())
        {
            return true;
        }
        let DispatchResult::Dispatch { meta, .. } =
            CommandRouter::dispatch_normalized(frame, command_name)
        else {
            return false;
        };
        meta.flags.contains(CommandFlags::WRITE)
    }

    #[inline]
    pub(super) fn should_defer_command_frame(
        &mut self,
        conn_id: usize,
        frame: &FrameRef<'_>,
    ) -> bool {
        let maintenance_debt = self.note_maintenance_debt();
        if maintenance_debt >= self.config.overload_policy.maintenance_debt {
            return true;
        }
        self.aof_write_backpressure_active() && self.frame_writes_to_aof(conn_id, frame)
    }

    #[inline]
    pub(super) fn defer_command_processing(&mut self, conn_id: usize) {
        if self.overload.queue_deferred_command(conn_id) {
            self.keyspace
                .record_reactor_overload_command_deferred(self.id);
        }
    }

    #[inline]
    pub(super) fn disable_read_for_backpressure(&mut self, conn_id: usize) {
        if self.overload.queue_read_disabled(conn_id) {
            self.keyspace.record_reactor_overload_read_disabled(self.id);
        }
    }

    #[inline]
    pub(super) fn clear_overload_connection_state(&mut self, conn_id: usize) {
        self.overload.clear_read_disabled(conn_id);
        self.overload.clear_deferred_command(conn_id);
    }

    #[inline]
    pub(super) fn publish_overload_telemetry(&self) {
        let snapshot = self
            .overload
            .snapshot(self.aof_pending_bytes(), self.maintenance_debt());
        self.keyspace.publish_reactor_overload_telemetry(
            self.id,
            RuntimeOverloadTelemetry {
                pending_response_bytes: snapshot.pending_response_bytes as u64,
                pending_response_bytes_peak: snapshot.pending_response_bytes_peak as u64,
                parser_accumulator_bytes: snapshot.parser_accumulator_bytes as u64,
                parser_accumulator_bytes_peak: snapshot.parser_accumulator_bytes_peak as u64,
                writev_backlog_bytes: snapshot.writev_backlog_bytes as u64,
                writev_backlog_bytes_peak: snapshot.writev_backlog_bytes_peak as u64,
                aof_pending_bytes: snapshot.aof_pending_bytes,
                maintenance_debt: snapshot.maintenance_debt as u64,
                maintenance_debt_peak: snapshot.maintenance_debt_peak as u64,
                read_disabled_connections: snapshot.read_disabled_connections as u64,
                read_disabled_connections_peak: snapshot.read_disabled_connections_peak as u64,
                deferred_commands: snapshot.deferred_commands as u64,
                deferred_commands_peak: snapshot.deferred_commands_peak as u64,
            },
        );
    }

    pub(super) fn run_admission_resume_slice(&mut self) {
        self.resume_deferred_commands();
        self.resume_disabled_reads();
        self.maybe_rearm_deferred_accept();
    }

    pub(super) fn resume_deferred_commands(&mut self) {
        if self.note_maintenance_debt() >= self.config.overload_policy.maintenance_debt
            || self.aof_write_backpressure_active()
        {
            return;
        }

        let mut budget = self.config.budgets.accept.get();
        while budget != 0 {
            let Some(conn_id) = self.overload.pop_deferred_command() else {
                return;
            };
            self.overload.clear_deferred_command(conn_id);
            if self.connections.is_closing(conn_id) {
                continue;
            }
            let Some(fd) = self.connections.get(conn_id).map(|conn| conn.fd) else {
                continue;
            };
            if self
                .connections
                .get(conn_id)
                .is_some_and(|conn| conn.read_buf_len == 0)
                && !self.has_pending_command_bytes(conn_id)
            {
                continue;
            }
            self.keyspace
                .record_reactor_overload_command_resumed(self.id);
            self.process_commands(conn_id, fd);
            budget -= 1;
        }
    }

    pub(super) fn resume_disabled_reads(&mut self) {
        if self.read_backpressure_active() {
            return;
        }

        let mut budget = self.config.budgets.accept.get();
        while budget != 0 {
            let Some(conn_id) = self.overload.pop_read_disabled() else {
                return;
            };
            self.overload.clear_read_disabled(conn_id);
            if self.connections.is_closing(conn_id) {
                continue;
            }
            let Some(fd) = self.connections.get(conn_id).map(|conn| conn.fd) else {
                continue;
            };
            self.keyspace.record_reactor_overload_read_resumed(self.id);
            self.submit_read_for(conn_id, fd);
            budget -= 1;
            if self.read_backpressure_active() {
                return;
            }
        }
    }

    pub(super) fn maybe_rearm_deferred_accept(&mut self) {
        if !self.overload.accept_rearm_deferred() || self.draining {
            return;
        }
        if self.accept_backpressure_active() {
            return;
        }
        if !self.overload.take_accept_rearm_deferred() {
            return;
        }
        if self.submit_accept_rearm().is_ok() {
            self.keyspace
                .record_reactor_overload_accept_resumed(self.id);
        }
    }
}
