use super::*;

impl Reactor {
    pub(super) fn run_maintenance_scheduler(&mut self) {
        let mut budget = SliceBudget::new(self.config.budgets.maintenance.get());
        let mut idle_classes = 0usize;
        let mut total_nanos = 0u64;

        while !budget.is_empty() && idle_classes < MaintenanceScheduler::class_count() {
            let class = self.maintenance_scheduler.next_class();
            let run = self.run_maintenance_class(class);
            if !run.did_work {
                idle_classes += 1;
                continue;
            }

            idle_classes = 0;
            let last_unit = budget.remaining() == 1;
            budget.consume_one();
            total_nanos = total_nanos.saturating_add(run.elapsed_nanos);
            if run.pending_more && last_unit {
                self.keyspace
                    .record_reactor_maintenance_budget_exhaustion(self.id);
            }
        }

        if total_nanos != 0 {
            self.keyspace
                .record_reactor_maintenance_nanos(self.id, total_nanos);
        }
    }

    #[inline]
    pub(super) fn run_maintenance_class(&mut self, class: MaintenanceClass) -> MaintenanceRun {
        match class {
            MaintenanceClass::CloseDrain => self.run_close_drain_slice(),
            MaintenanceClass::Timer => self.run_timer_slice(),
            MaintenanceClass::ActiveExpiry => self.run_active_expiry_slice(),
            MaintenanceClass::EvictionPressure => self.run_eviction_pressure_slice(),
            MaintenanceClass::AofFsync => self.run_aof_fsync_slice(),
            MaintenanceClass::MetricsFlush => self.run_metrics_flush_slice(),
        }
    }

    pub(super) fn run_close_drain_slice(&mut self) -> MaintenanceRun {
        if self.pending_close_finalization.is_empty() {
            return MaintenanceRun::idle();
        }

        let start = self.profile_metric_start();
        while let Some(conn_id) = self.pending_close_finalization.pop_front() {
            if let Some(pending) = self.close_finalization_pending.get_mut(conn_id) {
                *pending = false;
            }

            if self.connection_ready_to_finalize(conn_id) {
                self.finalize_close_now(conn_id);
                return MaintenanceRun::ran(
                    self.elapsed_profile_metric_nanos(start),
                    !self.pending_close_finalization.is_empty(),
                );
            }
        }

        MaintenanceRun::ran(self.elapsed_profile_metric_nanos(start), false)
    }

    pub(super) fn run_timer_slice(&mut self) -> MaintenanceRun {
        if self.connection_timeout == 0 {
            return MaintenanceRun::idle();
        }
        if self.pending_expired_timers.is_empty() && self.timer_wheel.current_tick() > self.now_secs
        {
            return MaintenanceRun::idle();
        }

        let start = self.profile_metric_start();
        if let Some(expired) = self.pending_expired_timers.pop_front() {
            self.handle_expired_timer(expired.conn_id, expired.generation);
            let pending_more = !self.pending_expired_timers.is_empty()
                || self.timer_wheel.current_tick() <= self.now_secs;
            return MaintenanceRun::ran(self.elapsed_profile_metric_nanos(start), pending_more);
        }

        self.expired_buf.clear();
        let more_in_tick = self.timer_wheel.tick_limited(&mut self.expired_buf, 1);
        if let Some(expired) = self.expired_buf.pop() {
            self.handle_expired_timer(expired.conn_id, expired.generation);
        }
        self.expired_buf.clear();
        let pending_more = more_in_tick || self.timer_wheel.current_tick() <= self.now_secs;
        MaintenanceRun::ran(self.elapsed_profile_metric_nanos(start), pending_more)
    }

    pub(super) fn run_active_expiry_slice(&mut self) -> MaintenanceRun {
        let now = self.cached_nanos;
        if now < self.next_active_expiry_nanos {
            return MaintenanceRun::idle();
        }
        self.next_active_expiry_nanos = now.saturating_add(ACTIVE_EXPIRY_INTERVAL_NANOS);
        if !self.keyspace.has_expiring_keys() {
            return MaintenanceRun::idle();
        }

        let start = self.profile_metric_start();
        let num_shards = self.keyspace.num_shards();
        let shard_idx = self.expiry_shard_cursor % num_shards;
        let (expired, sampled) = self.keyspace.run_active_expiry_on_shard(
            shard_idx,
            self.expiry_slot_cursor,
            ACTIVE_EXPIRY_MAX_EFFORT,
            now,
        );
        self.keyspace
            .record_reactor_active_expiry(self.id, sampled, expired);
        self.expiry_slot_cursor = self
            .expiry_slot_cursor
            .wrapping_add(ACTIVE_EXPIRY_MAX_EFFORT);
        self.expiry_shard_cursor = self.expiry_shard_cursor.wrapping_add(1);

        let pending_more = sampled != 0 && expired.saturating_mul(4) > sampled;
        if pending_more {
            self.next_active_expiry_nanos = now;
        }
        let elapsed = self.elapsed_profile_metric_nanos(start);
        self.keyspace
            .record_reactor_active_expiry_nanos(self.id, elapsed);
        MaintenanceRun::ran(elapsed, pending_more)
    }

    pub(super) fn run_eviction_pressure_slice(&mut self) -> MaintenanceRun {
        if !self.keyspace.eviction_pressure_active() {
            return MaintenanceRun::idle();
        }

        let start = self.profile_metric_start();
        let outcome = self
            .keyspace
            .run_eviction_maintenance_on_shard(self.eviction_shard_cursor, self.cached_nanos);
        self.eviction_shard_cursor = self.eviction_shard_cursor.wrapping_add(1);

        if let Some(records) = outcome.aof_records {
            for record in records {
                if let Err(error) =
                    self.append_eviction_aof_record(record.lsn, record.key.as_bytes())
                {
                    self.aof_coordinator.mark_failed(
                        self.id,
                        "maintenance_eviction_record",
                        &error,
                    );
                    break;
                }
            }
        }

        let elapsed = self.elapsed_profile_metric_nanos(start);
        let pending_more = outcome.oom_after_scan && self.keyspace.eviction_pressure_active();
        MaintenanceRun::ran(elapsed, pending_more)
    }

    pub(super) fn run_aof_fsync_slice(&mut self) -> MaintenanceRun {
        let Some(slot) = self.aof_writer.as_ref() else {
            return MaintenanceRun::idle();
        };
        if slot.writer.pending_writes() == 0 {
            return MaintenanceRun::idle();
        };

        let start = self.profile_metric_start();
        let (fsync_result, telemetry) = {
            let slot = self.aof_writer.as_mut().expect("AOF writer checked above");
            let fsync_result = slot.writer.maybe_fsync();
            let telemetry = slot.writer.telemetry();
            (fsync_result, telemetry)
        };
        match fsync_result {
            Ok(outcome) => {
                if outcome.did_work() {
                    self.publish_aof_telemetry_snapshot(telemetry);
                }
            }
            Err(error) => {
                self.publish_aof_telemetry_snapshot(telemetry);
                self.aof_coordinator
                    .mark_failed(self.id, "maybe_fsync", &error);
            }
        }
        let elapsed = self.elapsed_profile_metric_nanos(start);
        self.keyspace
            .record_reactor_aof_fsync_nanos(self.id, elapsed);
        MaintenanceRun::ran(elapsed, false)
    }

    pub(super) fn run_metrics_flush_slice(&mut self) -> MaintenanceRun {
        let now = self.cached_nanos;
        if now < self.next_metrics_flush_nanos {
            return MaintenanceRun::idle();
        }

        let start = self.profile_metric_start();
        self.next_metrics_flush_nanos = now.saturating_add(METRICS_FLUSH_INTERVAL_NANOS);
        self.flush_local_runtime_metrics();
        self.publish_backend_queue_pressure();
        self.publish_aof_telemetry();
        self.publish_client_retained_memory();
        self.publish_overload_telemetry();
        self.keyspace
            .publish_runtime_backend(Self::runtime_backend_snapshot(
                &self.config,
                backend_plan_for(&self.config, &self.backend),
                self.fixed_buffers_enabled,
            ));
        let elapsed = self.elapsed_profile_metric_nanos(start);
        self.keyspace
            .record_reactor_metrics_flush_nanos(self.id, elapsed);
        MaintenanceRun::ran(elapsed, false)
    }

    // ── Accept handler ─────────────────────────────────────────────
}
