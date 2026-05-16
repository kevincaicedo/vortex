use super::*;

impl Reactor {
    /// Estimated reactor-reserved bytes for per-connection state arrays.
    ///
    /// This intentionally excludes fixed I/O buffers, which are reported as
    /// their own attribution bucket by the reactor pool.
    pub(crate) fn per_connection_state_bytes(max_connections: usize) -> usize {
        max_connections.saturating_mul(
            std::mem::size_of::<ConnectionMeta>()
                + std::mem::size_of::<PendingWritev>()
                + std::mem::size_of::<InflightSet>()
                + std::mem::size_of::<TransactionState>()
                + std::mem::size_of::<CommandAccumulator>()
                + std::mem::size_of::<u32>()
                + std::mem::size_of::<bool>()
                + std::mem::size_of::<bool>()
                + std::mem::size_of::<bool>()
                + std::mem::size_of::<u64>(),
        )
    }

    #[inline]
    pub(super) fn has_exact_arity(frame: &FrameRef<'_>, expected: usize) -> bool {
        frame
            .element_count()
            .is_some_and(|argc| argc as usize == expected)
    }

    #[inline]
    pub(super) fn has_min_arity(frame: &FrameRef<'_>, minimum: usize) -> bool {
        frame
            .element_count()
            .is_some_and(|argc| argc as usize >= minimum)
    }

    #[inline]
    pub(super) fn profile_metric_start(&self) -> Option<u64> {
        #[cfg(feature = "profile-telemetry")]
        if self.config.telemetry_mode.profile_timers_enabled() {
            return Some(Timestamp::now().as_nanos());
        }
        None
    }

    #[inline]
    pub(super) fn elapsed_profile_metric_nanos(&self, start: Option<u64>) -> u64 {
        #[cfg(feature = "profile-telemetry")]
        {
            return start
                .map(|started| Timestamp::now().as_nanos().saturating_sub(started).max(1))
                .unwrap_or(0);
        }
        #[cfg(not(feature = "profile-telemetry"))]
        {
            let _ = start;
            0
        }
    }

    #[inline]
    pub(super) fn flush_local_runtime_metrics(&mut self) {
        let metrics = self.local_metrics.take();
        self.keyspace.flush_reactor_local_metrics(self.id, metrics);
    }

    #[inline]
    pub(super) fn flush_counts_submit_syscall(&self) -> bool {
        self.backend_flush_counts_submit_syscall
    }

    #[inline]
    pub(super) fn completions_count_submit_syscall(&self) -> bool {
        self.backend_completions_count_submit_syscall
    }

    #[inline]
    pub(super) fn drain_cq_counts_submit_syscall(&self, completions: usize) -> bool {
        completions != 0 && self.backend_drain_cq_counts_submit_syscall
    }

    pub(crate) fn min_buffer_count_for_connections(max_connections: usize) -> io::Result<usize> {
        Ok(max_connections)
    }

    pub(super) fn validate_buffer_configuration(
        max_connections: usize,
        buffer_count: usize,
    ) -> io::Result<()> {
        if buffer_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer_count must be greater than zero",
            ));
        }

        let min_buffer_count = Self::min_buffer_count_for_connections(max_connections)?;
        if buffer_count < min_buffer_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "buffer_count ({buffer_count}) must be at least max_connections ({min_buffer_count})"
                ),
            ));
        }

        Ok(())
    }

    pub(super) fn validate_request_configuration(
        buffer_size: usize,
        max_request_bytes: usize,
        caps: ConnectionMemoryCaps,
        overload_policy: ReactorOverloadPolicy,
    ) -> io::Result<()> {
        if max_request_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_request_bytes must be greater than zero",
            ));
        }
        if max_request_bytes < buffer_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "max_request_bytes ({max_request_bytes}) must be at least buffer_size ({buffer_size})"
                ),
            ));
        }
        if caps.max_parser_accumulator_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_parser_accumulator_bytes must be greater than zero",
            ));
        }
        if caps.max_pending_response_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_pending_response_bytes must be greater than zero",
            ));
        }
        if caps.max_multi_queue_commands == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_multi_queue_commands must be greater than zero",
            ));
        }
        if caps.max_multi_queue_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_multi_queue_bytes must be greater than zero",
            ));
        }
        if caps.max_watch_registrations == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_watch_registrations must be greater than zero",
            ));
        }
        if caps.max_writev_chunks == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_writev_chunks must be greater than zero",
            ));
        }
        if !(1..=100).contains(&overload_policy.accept_throttle_connection_percent) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "accept_throttle_connection_percent must be in 1..=100",
            ));
        }
        if overload_policy.read_disable_pending_response_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read_disable_pending_response_bytes must be greater than zero",
            ));
        }
        if overload_policy.read_disable_parser_accumulator_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read_disable_parser_accumulator_bytes must be greater than zero",
            ));
        }
        if overload_policy.aof_pending_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overload aof_pending_bytes must be greater than zero",
            ));
        }
        if overload_policy.writev_backlog_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "writev_backlog_bytes must be greater than zero",
            ));
        }
        if overload_policy.maintenance_debt == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "maintenance_debt must be greater than zero",
            ));
        }
        Ok(())
    }

    pub(super) fn aof_writer_id(id: usize) -> io::Result<AofReactorId> {
        AofReactorId::try_from_usize(id).map_err(io::Error::from)
    }

    #[inline]
    pub(super) fn require_aof_outcome(
        outcome: AofAppendOutcome,
        requirement: AofDurabilityRequirement,
    ) -> io::Result<AofAppendOutcome> {
        if outcome.satisfies(requirement) {
            return Ok(outcome);
        }

        Err(io::Error::other(
            "AOF append outcome did not satisfy the configured durability requirement",
        ))
    }

    #[inline]
    pub(super) fn publish_aof_telemetry_snapshot(&self, snapshot: AofTelemetrySnapshot) {
        self.keyspace.publish_reactor_aof_telemetry(
            self.id,
            RuntimeAofTelemetry {
                pending_bytes: snapshot.pending_bytes,
                pending_writes: snapshot.pending_writes,
                fsync_requested: snapshot.fsync_requested,
                fsync_completed: snapshot.fsync_completed,
                fsync_failed: snapshot.fsync_failed,
                fsync_worker_saturation: snapshot.fsync_worker_saturation,
                backpressure_events: snapshot.backpressure_events,
                backpressure_nanos_total: snapshot.backpressure_nanos_total,
                backpressure_nanos_max: snapshot.backpressure_nanos_max,
                last_appended_lsn: snapshot.last_appended_lsn_raw(),
                last_durable_lsn: snapshot.last_durable_lsn_raw(),
                fsync_latency_nanos_total: snapshot.fsync_latency_nanos_total,
                fsync_latency_nanos_max: snapshot.fsync_latency_nanos_max,
                fsync_latency_buckets: snapshot.fsync_latency_buckets,
            },
        );
    }

    #[inline]
    pub(super) fn publish_aof_telemetry(&self) {
        if let Some(slot) = self.aof_writer.as_ref() {
            self.publish_aof_telemetry_snapshot(slot.writer.telemetry());
        }
    }

    #[inline]
    pub(super) fn retained_client_memory_bytes(&self, conn_id: usize) -> usize {
        if conn_id >= self.command_accumulators.len() {
            return 0;
        }

        let accumulator = self.command_accumulators[conn_id].bytes.capacity();
        let response = self.writev_states[conn_id]
            .remaining_len()
            .max(self.writev_states[conn_id].queued_len());
        let tx = &self.transaction_states[conn_id];
        let watch_bytes = tx.watched.iter().fold(0usize, |sum, watched| {
            sum.saturating_add(std::mem::size_of::<WatchRegistration>())
                .saturating_add(watched.key().as_bytes().len())
        });

        accumulator
            .saturating_add(response)
            .saturating_add(tx.queued_bytes)
            .saturating_add(watch_bytes)
    }

    pub(super) fn publish_client_retained_memory(&self) {
        let mut total = 0usize;
        let mut max_connection = 0usize;
        for conn_id in self.connections.ids() {
            let retained = self.retained_client_memory_bytes(conn_id);
            total = total.saturating_add(retained);
            max_connection = max_connection.max(retained);
        }
        self.keyspace
            .publish_reactor_client_retained_bytes(self.id, total, max_connection);
    }

    pub(super) fn fixed_buffer_policy(
        registration: FixedBufferRegistrationMode,
        requested: IoBackendMode,
        effective: BackendKind,
        capabilities: BackendCapabilities,
        buffer_count: usize,
    ) -> io::Result<FixedBufferPolicy> {
        if registration == FixedBufferRegistrationMode::Off {
            return Ok(FixedBufferPolicy::Disabled);
        }

        if !capabilities.fixed_buffers {
            if registration == FixedBufferRegistrationMode::On {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "fixed-buffer registration was requested but effective {:?} backend does not support it",
                        effective
                    ),
                ));
            }
            return Ok(FixedBufferPolicy::Disabled);
        }

        if FixedBufferId::validate_pool_len(buffer_count).is_ok() {
            return Ok(FixedBufferPolicy::Register);
        }

        if registration == FixedBufferRegistrationMode::On || requested == IoBackendMode::Uring {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "buffer_count ({buffer_count}) exceeds fixed-buffer buf_index range ({}) for requested {:?} backend with {:?} fixed-buffer registration",
                    FixedBufferId::MAX_BUFFER_COUNT,
                    effective,
                    registration
                ),
            ));
        }

        tracing::warn!(
            requested = ?requested,
            effective = ?effective,
            buffer_count,
            max_fixed_buffer_count = FixedBufferId::MAX_BUFFER_COUNT,
            "fixed-buffer registration disabled because buffer_count exceeds io_uring buf_index range"
        );
        Ok(FixedBufferPolicy::Disabled)
    }

    pub(super) fn runtime_backend_snapshot(
        config: &ReactorConfig,
        plan: BackendPlan,
        fixed_buffers_registered: bool,
    ) -> RuntimeBackendSnapshot {
        let effective = runtime_effective_backend(plan.effective);
        RuntimeBackendSnapshot {
            requested: runtime_requested_backend(plan.requested),
            effective,
            mixed: false,
            fixed_buffers_capable: plan.capabilities.fixed_buffers,
            fixed_buffers_registered,
            sqpoll: plan.capabilities.sqpoll,
            multishot_accept: plan.capabilities.multishot_accept,
            accept4: plan.capabilities.accept4,
            close_opcode: plan.capabilities.close_opcode,
            cancel_support: plan.capabilities.async_cancel,
            nonblocking_drain: plan.capabilities.nonblocking_drain,
            requested_ring_size: config.ring_size as u64,
            effective_ring_size: if effective == RuntimeBackendMode::IoUring {
                config.ring_size as u64
            } else {
                0
            },
        }
    }

    /// Creates a new reactor with the polling backend and a fresh keyspace.
    ///
    /// This constructor creates its own `ConcurrentKeyspace`. For production
    /// use, prefer [`with_shared_keyspace`](Self::with_shared_keyspace)
    /// which accepts a pool-owned `Arc<ConcurrentKeyspace>`.
    pub fn new(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
    ) -> std::io::Result<Self> {
        let (backend_plan, backend) = make_backend(&config)?;
        tracing::info!(
            backend_requested = runtime_requested_backend(backend_plan.requested).as_str(),
            backend_effective = runtime_effective_backend(backend_plan.effective).as_str(),
            fixed_buffers_capable = backend_plan.capabilities.fixed_buffers,
            sqpoll = backend_plan.capabilities.sqpoll,
            multishot_accept = backend_plan.capabilities.multishot_accept,
            accept4 = backend_plan.capabilities.accept4,
            close_opcode = backend_plan.capabilities.close_opcode,
            cancel_support = backend_plan.capabilities.async_cancel,
            requested = ?backend_plan.requested,
            effective = ?backend_plan.effective,
            capabilities = ?backend_plan.capabilities,
            "reactor backend selected"
        );
        let keyspace = Arc::new(ConcurrentKeyspace::new(DEFAULT_SHARD_COUNT));
        let aof_coordinator = Arc::new(AofCoordinator::new(1));
        let startup_aof_epoch = if config.aof_config.is_some() {
            Some(aof_coordinator.begin_startup_enable()?)
        } else {
            None
        };
        let result = Self::with_keyspace_and_backend(
            id,
            config,
            coordinator,
            Arc::clone(&keyspace),
            backend,
            AofRuntime::new(Arc::clone(&aof_coordinator), startup_aof_epoch),
            1,
        );

        match result {
            Ok(reactor) => {
                if let Some(epoch) = startup_aof_epoch {
                    reactor
                        .aof_coordinator
                        .commit_enable(epoch, &reactor.keyspace)?;
                }
                Ok(reactor)
            }
            Err(error) => {
                if let Some(epoch) = startup_aof_epoch {
                    aof_coordinator.abort_enable(epoch);
                }
                Err(error)
            }
        }
    }

    /// Creates a new reactor with a shared keyspace (used by `ReactorPool`).
    ///
    /// The keyspace is already populated (e.g. from AOF replay) before
    /// the reactor starts accepting connections.
    /// Creates a new reactor with a shared keyspace (used by `ReactorPool`).
    ///
    /// The keyspace is already populated (e.g. from AOF replay) before
    /// the reactor starts accepting connections.
    ///
    /// `num_reactors` controls active-expiry cursor staggering so each
    /// reactor sweeps a distinct shard region, avoiding duplicate work.
    pub fn with_shared_keyspace(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
        keyspace: Arc<ConcurrentKeyspace>,
        num_reactors: usize,
    ) -> std::io::Result<Self> {
        if config.aof_config.is_some() && num_reactors != 1 {
            return Err(runtime_reconfigure_unsupported());
        }

        let aof_coordinator = Arc::new(AofCoordinator::new(1));
        let startup_aof_epoch = if config.aof_config.is_some() {
            Some(aof_coordinator.begin_startup_enable()?)
        } else {
            None
        };
        let result = Self::with_shared_keyspace_and_aof_state(
            id,
            config,
            coordinator,
            Arc::clone(&keyspace),
            AofRuntime::new(Arc::clone(&aof_coordinator), startup_aof_epoch),
            num_reactors,
        );

        match result {
            Ok(reactor) => {
                if let Some(epoch) = startup_aof_epoch {
                    reactor
                        .aof_coordinator
                        .commit_enable(epoch, &reactor.keyspace)?;
                }
                Ok(reactor)
            }
            Err(error) => {
                if let Some(epoch) = startup_aof_epoch {
                    aof_coordinator.abort_enable(epoch);
                }
                Err(error)
            }
        }
    }

    pub(crate) fn with_shared_keyspace_and_aof_state(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
        keyspace: Arc<ConcurrentKeyspace>,
        aof_runtime: AofRuntime,
        num_reactors: usize,
    ) -> std::io::Result<Self> {
        let (backend_plan, backend) = make_backend(&config)?;
        tracing::info!(
            backend_requested = runtime_requested_backend(backend_plan.requested).as_str(),
            backend_effective = runtime_effective_backend(backend_plan.effective).as_str(),
            fixed_buffers_capable = backend_plan.capabilities.fixed_buffers,
            sqpoll = backend_plan.capabilities.sqpoll,
            multishot_accept = backend_plan.capabilities.multishot_accept,
            accept4 = backend_plan.capabilities.accept4,
            close_opcode = backend_plan.capabilities.close_opcode,
            cancel_support = backend_plan.capabilities.async_cancel,
            requested = ?backend_plan.requested,
            effective = ?backend_plan.effective,
            capabilities = ?backend_plan.capabilities,
            "reactor backend selected"
        );
        Self::with_keyspace_and_backend(
            id,
            config,
            coordinator,
            keyspace,
            backend,
            aof_runtime,
            num_reactors,
        )
    }

    /// Internal constructor — all public constructors delegate here.
    pub(crate) fn with_keyspace_and_backend(
        id: usize,
        config: ReactorConfig,
        coordinator: Arc<ShutdownCoordinator>,
        keyspace: Arc<ConcurrentKeyspace>,
        mut backend: Backend,
        aof_runtime: AofRuntime,
        num_reactors: usize,
    ) -> std::io::Result<Self> {
        Self::validate_buffer_configuration(config.max_connections, config.buffer_count)?;
        Self::validate_request_configuration(
            config.buffer_size,
            config.max_request_bytes,
            config.connection_caps,
            config.overload_policy,
        )?;
        let backend_plan = backend_plan_for(&config, &backend);
        let fixed_buffer_policy = Self::fixed_buffer_policy(
            config.fixed_buffer_registration,
            backend_plan.requested,
            backend_plan.effective,
            backend_plan.capabilities,
            config.buffer_count,
        )?;

        let max_conn = config.max_connections;
        let connection_timeout = config.connection_timeout;

        // `buffer_count` is validated up-front and remains authoritative here.
        let buffer_pool = BufferPool::new(config.buffer_count, config.buffer_size);

        let fixed_buffers_enabled = match fixed_buffer_policy {
            FixedBufferPolicy::Disabled => false,
            FixedBufferPolicy::Register => {
                let iovecs = buffer_pool.as_iovecs();
                match backend.register_buffers(&iovecs) {
                    Ok(()) => {
                        tracing::info!(
                            reactor_id = id,
                            buffer_count = config.buffer_count,
                            "fixed buffers registered"
                        );
                        true
                    }
                    Err(error) if config.io_backend == IoBackendMode::Uring => {
                        return Err(io::Error::other(format!(
                            "failed to register fixed buffers for requested io_uring backend: {error}"
                        )));
                    }
                    Err(error) => {
                        tracing::warn!(
                            reactor_id = id,
                            error = %error,
                            "fixed-buffer registration failed; continuing with non-fixed I/O in auto mode"
                        );
                        false
                    }
                }
            }
        };

        // Create the listener socket with SO_REUSEPORT + SO_REUSEADDR + SO_INCOMING_CPU
        // after fixed-buffer validation/registration so strict startup errors
        // cannot leak a raw listener fd.
        let listener_fd = crate::accept::create_listener(config.bind_addr, Some(id))?;

        // Initialize AOF writer if persistence is enabled.
        // NOTE: AOF replay is now handled at the pool level (before reactor creation).
        // The reactor only opens the writer for appending new mutations.
        let aof_writer = if let Some(ref aof_cfg) = config.aof_config {
            let epoch = aof_runtime.startup_epoch.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "AOF config requires a coordinator enable epoch",
                )
            })?;
            let aof_path = reactor_aof_writer_path(&aof_cfg.path, id)?;
            let writer_id = Self::aof_writer_id(id)?;
            let writer = open_aof_writer(&aof_path, writer_id, aof_cfg, config.telemetry_mode)
                .map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("failed to open AOF file {}: {error}", aof_path.display()),
                    )
                })?;
            tracing::info!(
                reactor_id = id,
                epoch = epoch.get(),
                path = %aof_path.display(),
                policy = ?aof_cfg.fsync_policy,
                "AOF writer initialized"
            );
            Some(AofWriterSlot::new(epoch, writer))
        } else {
            None
        };

        // Stagger the starting shard cursor by reactor ID so each reactor
        // sweeps a distinct region, distributing expiry work evenly.
        // With K shards and N reactors, spacing = K/N.
        let expiry_shard_cursor = id * (keyspace.num_shards() / num_reactors.max(1)).max(1);

        let cached_nanos = Timestamp::now().as_nanos();
        let cached_unix_nanos = current_unix_time_nanos();
        let backend_flush_counts_submit_syscall = backend_flush_counts_submit_syscall(backend_plan);
        let backend_completions_count_submit_syscall =
            backend_completions_count_submit_syscall(backend_plan);
        let backend_drain_cq_counts_submit_syscall =
            backend_drain_cq_counts_submit_syscall(backend_plan);
        keyspace.set_runtime_telemetry_mode(config.telemetry_mode);
        keyspace.publish_runtime_backend(Self::runtime_backend_snapshot(
            &config,
            backend_plan,
            fixed_buffers_enabled,
        ));
        let backend_queue = backend.queue_status();
        if backend_queue.has_capacity() {
            keyspace.record_reactor_backend_queue_status(
                id,
                backend_queue.sq_occupancy,
                backend_queue.sq_capacity,
                backend_queue.cq_occupancy,
                backend_queue.cq_capacity,
                backend_queue.cq_overflow_delta,
            );
        }

        Ok(Self {
            id,
            backend,
            backend_flush_counts_submit_syscall,
            backend_completions_count_submit_syscall,
            backend_drain_cq_counts_submit_syscall,
            connections: ConnectionSlab::with_capacity(max_conn),
            listener_fd,
            buffer_pool,
            fixed_buffers_enabled,
            generations: vec![0u32; max_conn],
            cqe_buf: Vec::with_capacity(config.budgets.completion.get()),
            pending_completions: VecDeque::new(),
            local_metrics: ReactorLocalMetrics::default(),
            invalid_completion_tokens: 0,
            unexpected_completion_tokens: 0,
            timer_wheel: TimerWheel::new(max_conn),
            start_nanos: cached_nanos,
            now_secs: 0,
            expired_buf: Vec::with_capacity(64),
            pending_expired_timers: VecDeque::new(),
            pending_close_finalization: VecDeque::new(),
            connection_timeout,
            running: false,
            coordinator,
            draining: false,
            config,
            arena: ArenaAllocator::new(vortex_memory::arena::DEFAULT_ARENA_CAPACITY),
            writev_states: std::iter::repeat_with(PendingWritev::new)
                .take(max_conn)
                .collect(),
            inflight_ops: vec![InflightSet::default(); max_conn],
            close_started_nanos: vec![0; max_conn],
            close_finalization_pending: vec![false; max_conn],
            transaction_states: std::iter::repeat_with(TransactionState::default)
                .take(max_conn)
                .collect(),
            command_accumulators: std::iter::repeat_with(CommandAccumulator::default)
                .take(max_conn)
                .collect(),
            command_router: CommandRouter::new(),
            command_executor: SharedKeyspaceExecutor::new(Arc::clone(&keyspace)),
            shared_nothing: None,
            shared_nothing_flush_ids: Vec::with_capacity(256),
            keyspace,
            cached_nanos,
            cached_unix_nanos,
            next_active_expiry_nanos: cached_nanos,
            next_metrics_flush_nanos: cached_nanos.saturating_add(METRICS_FLUSH_INTERVAL_NANOS),
            aof_writer,
            aof_scratch: vec![0u8; 4096],
            aof_coordinator: aof_runtime.coordinator,
            #[cfg(test)]
            aof_append_fail_after: None,
            #[cfg(test)]
            aof_fsync_fail_after: None,
            expiry_shard_cursor,
            expiry_slot_cursor: 0,
            eviction_shard_cursor: expiry_shard_cursor,
            maintenance_scheduler: MaintenanceScheduler::new(),
            overload: ReactorOverloadState::with_capacity(max_conn),
            parse_entries: Vec::with_capacity(64),
        })
    }

    pub(crate) fn enable_shared_nothing(
        &mut self,
        fabric: Arc<SharedNothingServerFabric<SHARED_NOTHING_MAILBOX_RING_SLOTS>>,
        capacity_per_owner: usize,
    ) -> std::io::Result<()> {
        let runtime = SharedNothingServerRuntime::new(
            fabric,
            self.id,
            self.config.max_connections,
            capacity_per_owner,
        )
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "invalid shared-nothing topology for reactor {}: {error}",
                    self.id
                ),
            )
        })?;
        tracing::info!(
            reactor_id = self.id,
            owner = runtime.local_owner().get(),
            "shared-nothing owner runtime enabled"
        );
        if let Some(waker) = self.backend.waker() {
            runtime.register_waker(waker);
        }
        self.shared_nothing = Some(runtime);
        Ok(())
    }
}
