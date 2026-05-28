use super::*;

#[inline]
fn validated_aof_writer_slot(
    slot: &Option<AofWriterSlot>,
    epoch: AofEpoch,
) -> io::Result<&AofWriterSlot> {
    match slot.as_ref() {
        Some(slot) if slot.epoch == epoch => Ok(slot),
        Some(_) => Err(io::Error::other(
            "AOF writer epoch changed after validation",
        )),
        None => Err(io::Error::other(
            "AOF LSN allocated without a reactor writer",
        )),
    }
}

#[inline]
fn validated_aof_writer_slot_mut(
    slot: &mut Option<AofWriterSlot>,
    epoch: AofEpoch,
) -> io::Result<&mut AofWriterSlot> {
    match slot.as_mut() {
        Some(slot) if slot.epoch == epoch => Ok(slot),
        Some(_) => Err(io::Error::other(
            "AOF writer epoch changed after validation",
        )),
        None => Err(io::Error::other(
            "AOF LSN allocated without a reactor writer",
        )),
    }
}

impl Reactor {
    /// Append a mutation command to the AOF file with its global LSN.
    ///
    /// Serializes the frame's RESP encoding into the scratch buffer, then
    /// writes it with the LSN prefix to the AOF. This is the only AOF
    /// hot-path code.
    pub(super) fn append_eviction_aof_record(
        &mut self,
        lsn: AofLsn,
        key: &[u8],
    ) -> io::Result<AofAppendOutcome> {
        let epoch = self.validate_aof_writer_epoch()?;
        self.maybe_inject_aof_append_failure()?;
        let requirement = validated_aof_writer_slot(&self.aof_writer, epoch)?
            .writer
            .durability_requirement();

        let append_start = self.profile_metric_start();
        self.aof_scratch.clear();
        push_resp_array_len(&mut self.aof_scratch, 2);
        push_resp_bulk_string(&mut self.aof_scratch, b"DEL");
        push_resp_bulk_string(&mut self.aof_scratch, key);
        let record = AofRecordBytes::try_from_resp(&self.aof_scratch)?;
        let outcome = validated_aof_writer_slot_mut(&mut self.aof_writer, epoch)?
            .writer
            .append_with_lsn(lsn, record)?;
        if outcome.durable_lsn().is_some() {
            self.maybe_inject_aof_fsync_failure()?;
        }
        let outcome = Self::require_aof_outcome(outcome, requirement)?;
        self.publish_aof_telemetry();
        if self.aof_scratch.len() < 4096 {
            self.aof_scratch.resize(4096, 0);
        }
        let append_elapsed = self.elapsed_profile_metric_nanos(append_start);
        self.keyspace
            .record_reactor_aof_append_nanos(self.id, append_elapsed);
        Ok(outcome)
    }

    #[inline]
    pub(super) fn append_aof_payload(
        &mut self,
        effect: AofCommitEffect,
        payload: &[u8],
    ) -> io::Result<AofAppendOutcome> {
        let epoch = self.validate_aof_writer_epoch()?;
        self.maybe_inject_aof_append_failure()?;
        let requirement = validated_aof_writer_slot(&self.aof_writer, epoch)?
            .writer
            .durability_requirement();

        let append_start = self.profile_metric_start();
        let record = AofRecordBytes::try_from_resp(payload)?;
        let outcome = validated_aof_writer_slot_mut(&mut self.aof_writer, epoch)?
            .writer
            .append_with_lsn(effect.lsn(), record)?;
        if outcome.durable_lsn().is_some() {
            self.maybe_inject_aof_fsync_failure()?;
        }
        let outcome = Self::require_aof_outcome(outcome, requirement)?;
        self.publish_aof_telemetry();
        let append_elapsed = self.elapsed_profile_metric_nanos(append_start);
        self.keyspace
            .record_reactor_aof_append_nanos(self.id, append_elapsed);
        Ok(outcome)
    }

    #[inline]
    pub(super) fn append_transaction_aof_batch(
        &mut self,
        batch: &TransactionAofBatch,
    ) -> io::Result<AofAppendOutcome> {
        let max_lsn = batch
            .max_lsn()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty AOF batch"))?;

        let payload_len = batch.records.iter().fold(0usize, |acc, (_, payload)| {
            acc.saturating_add(payload.len())
        });
        let mut pipeline = Vec::with_capacity(payload_len);
        for (_, payload) in &batch.records {
            pipeline.extend_from_slice(payload);
        }

        self.aof_scratch.clear();
        push_resp_array_len(&mut self.aof_scratch, 2);
        push_resp_bulk_string(&mut self.aof_scratch, AOF_TRANSACTION_BATCH_COMMAND);
        push_resp_bulk_string(&mut self.aof_scratch, &pipeline);

        let payload = std::mem::take(&mut self.aof_scratch);
        let outcome = self.append_aof_payload(AofCommitEffect::new(max_lsn), &payload);
        self.aof_scratch = payload;
        if self.aof_scratch.len() < 4096 {
            self.aof_scratch.resize(4096, 0);
        }
        outcome
    }

    #[inline]
    pub(super) fn append_to_aof(
        &mut self,
        effect: AofCommitEffect,
        frame: &FrameRef<'_>,
        aof_payload: Option<&[u8]>,
    ) -> io::Result<AofAppendOutcome> {
        let epoch = self.validate_aof_writer_epoch()?;
        self.maybe_inject_aof_append_failure()?;
        let requirement = validated_aof_writer_slot(&self.aof_writer, epoch)?
            .writer
            .durability_requirement();

        let append_start = self.profile_metric_start();
        let payload = if let Some(payload) = aof_payload {
            payload
        } else {
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
                            "AOF frame exceeded scratch buffer growth limit",
                        )
                    })?;
                self.aof_scratch.resize(next_len, 0);
            };
            &self.aof_scratch[..written]
        };

        let record = AofRecordBytes::try_from_resp(payload)?;
        let outcome = validated_aof_writer_slot_mut(&mut self.aof_writer, epoch)?
            .writer
            .append_with_lsn(effect.lsn(), record)?;
        if outcome.durable_lsn().is_some() {
            self.maybe_inject_aof_fsync_failure()?;
        }
        let outcome = Self::require_aof_outcome(outcome, requirement)?;
        self.publish_aof_telemetry();
        let append_elapsed = self.elapsed_profile_metric_nanos(append_start);
        self.keyspace
            .record_reactor_aof_append_nanos(self.id, append_elapsed);

        Ok(outcome)
    }

    pub(super) fn handle_bgrewriteaof(&mut self) -> (CommandResponse, bool) {
        (
            CommandResponse::Static(RESP_ERR_BGREWRITEAOF_DISABLED),
            false,
        )
    }

    pub(super) fn enable_aof_runtime(&mut self) -> (CommandResponse, bool) {
        if self.aof_writer.is_some() {
            return (CommandResponse::Static(b"+OK\r\n"), false);
        }
        if self.aof_coordinator.reactor_count() != 1 {
            return (
                CommandResponse::Static(RESP_ERR_CONFIG_SET_APPENDONLY_MULTI),
                false,
            );
        }

        let aof_cfg = match &self.config.aof_config {
            Some(cfg) => cfg.clone(),
            None => AofConfig {
                path: std::path::PathBuf::from("vortex.aof"),
                fsync_policy: vortex_persist::aof::AofFsyncPolicy::Everysec,
                max_pending_fsync_bytes: DEFAULT_EVERYSEC_MAX_PENDING_BYTES,
            },
        };

        let epoch = match self.aof_coordinator.begin_runtime_enable() {
            Ok(epoch) => epoch,
            Err(error) if error.kind() == io::ErrorKind::Unsupported => {
                return (
                    CommandResponse::Static(RESP_ERR_CONFIG_SET_APPENDONLY_MULTI),
                    false,
                );
            }
            Err(error) => {
                return (
                    CommandResponse::Frame(RespFrame::Error(
                        format!("ERR failed to enable AOF: {error}").into(),
                    )),
                    false,
                );
            }
        };

        let aof_path = match reactor_aof_writer_path(&aof_cfg.path, self.id) {
            Ok(path) => path,
            Err(error) => {
                self.aof_coordinator.abort_enable(epoch);
                return (
                    CommandResponse::Owned(
                        format!("-ERR failed to resolve AOF path: {error}\r\n")
                            .into_bytes()
                            .into(),
                    ),
                    false,
                );
            }
        };
        let writer_id = match Self::aof_writer_id(self.id) {
            Ok(writer_id) => writer_id,
            Err(error) => {
                self.aof_coordinator.abort_enable(epoch);
                return (
                    CommandResponse::Frame(RespFrame::Error(
                        format!("ERR failed to enable AOF: {error}").into(),
                    )),
                    false,
                );
            }
        };

        match open_aof_writer(&aof_path, writer_id, &aof_cfg, self.config.telemetry_mode) {
            Ok(writer) => {
                self.aof_writer = Some(AofWriterSlot::new(epoch, writer));
                self.config.aof_config = Some(aof_cfg);
                if let Err(error) = self.aof_coordinator.commit_enable(epoch, &self.keyspace) {
                    self.aof_writer = None;
                    self.aof_coordinator.abort_enable(epoch);
                    return (
                        CommandResponse::Frame(RespFrame::Error(
                            format!("ERR failed to enable AOF: {error}").into(),
                        )),
                        false,
                    );
                }
                tracing::info!(
                    reactor_id = self.id,
                    epoch = epoch.get(),
                    "AOF enabled via CONFIG SET"
                );
                (CommandResponse::Static(b"+OK\r\n"), false)
            }
            Err(error) => {
                self.aof_coordinator.abort_enable(epoch);
                (
                    CommandResponse::Frame(RespFrame::Error(
                        format!("ERR failed to enable AOF: {error}").into(),
                    )),
                    false,
                )
            }
        }
    }

    pub(super) fn disable_aof_runtime(&mut self) -> (CommandResponse, bool) {
        if self.aof_coordinator.reactor_count() != 1 {
            return (
                CommandResponse::Static(RESP_ERR_CONFIG_SET_APPENDONLY_MULTI),
                false,
            );
        }

        let Some(epoch) = self.aof_writer.as_ref().map(|slot| slot.epoch) else {
            return (CommandResponse::Static(b"+OK\r\n"), false);
        };

        if let Err(error) = self.aof_coordinator.begin_runtime_disable(epoch) {
            return (
                CommandResponse::Frame(RespFrame::Error(
                    format!("ERR failed to disable AOF: {error}").into(),
                )),
                false,
            );
        }

        if let Some(slot) = self.aof_writer.as_mut() {
            if let Err(error) = slot.writer.flush_and_sync() {
                self.aof_coordinator.abort_disable(epoch);
                self.aof_coordinator
                    .mark_failed(self.id, "disable_flush_and_sync", &error);
                return (CommandResponse::Static(RESP_ERR_AOF_MISCONF), false);
            }
        }

        if let Err(error) = self.aof_coordinator.commit_disable(epoch, &self.keyspace) {
            self.aof_coordinator.abort_disable(epoch);
            return (
                CommandResponse::Frame(RespFrame::Error(
                    format!("ERR failed to disable AOF: {error}").into(),
                )),
                false,
            );
        }

        self.aof_writer = None;
        tracing::info!(
            reactor_id = self.id,
            epoch = epoch.get(),
            "AOF disabled via CONFIG SET"
        );
        (CommandResponse::Static(b"+OK\r\n"), false)
    }

    /// Handle CONFIG subcommands relevant to AOF.
    ///
    /// Returns `Some(response)` for handled subcommands, `None` to fall through
    /// to the engine for other CONFIG operations.
    pub(super) fn handle_config(
        &mut self,
        frame: &FrameRef<'_>,
    ) -> Option<(CommandResponse, bool)> {
        // CONFIG GET/SET require at least 3 args: CONFIG <subcmd> <param>
        let argc = arg_count(frame);
        if argc < 3 {
            return None;
        }

        let subcmd = arg_bytes(frame, 1)?;
        let param = arg_bytes(frame, 2)?;

        // Normalize subcmd to uppercase for comparison.
        let subcmd_upper: Vec<u8> = subcmd.iter().map(|b| b.to_ascii_uppercase()).collect();

        match subcmd_upper.as_slice() {
            b"GET" => {
                let param_lower: Vec<u8> = param.iter().map(|b| b.to_ascii_lowercase()).collect();
                match param_lower.as_slice() {
                    b"appendonly" => Some(config_pair_response(
                        b"appendonly",
                        if self.aof_writer.is_some() {
                            b"yes".to_vec()
                        } else {
                            b"no".to_vec()
                        },
                    )),
                    b"appendfsync" => Some(config_pair_response(
                        b"appendfsync",
                        match &self.config.aof_config {
                            Some(cfg) => match cfg.fsync_policy {
                                vortex_persist::aof::AofFsyncPolicy::Always => b"always".to_vec(),
                                vortex_persist::aof::AofFsyncPolicy::Everysec => {
                                    b"everysec".to_vec()
                                }
                                vortex_persist::aof::AofFsyncPolicy::No => b"no".to_vec(),
                            },
                            None => b"everysec".to_vec(),
                        },
                    )),
                    b"maxmemory" => Some(config_pair_response(
                        b"maxmemory",
                        self.keyspace.max_memory().to_string().into_bytes(),
                    )),
                    b"maxmemory-policy" => Some(config_pair_response(
                        b"maxmemory-policy",
                        self.keyspace.eviction_policy().as_str().as_bytes().to_vec(),
                    )),
                    _ => None, // Fall through to engine.
                }
            }
            b"SET" => {
                if argc < 4 {
                    return Some((
                        CommandResponse::Static(
                            b"-ERR wrong number of arguments for CONFIG SET\r\n",
                        ),
                        false,
                    ));
                }
                let param_lower: Vec<u8> = param.iter().map(|b| b.to_ascii_lowercase()).collect();
                let value = arg_bytes(frame, 3)?;
                match param_lower.as_slice() {
                    b"appendonly" => {
                        let val_lower: Vec<u8> =
                            value.iter().map(|b| b.to_ascii_lowercase()).collect();
                        match val_lower.as_slice() {
                            b"yes" => Some(self.enable_aof_runtime()),
                            b"no" => Some(self.disable_aof_runtime()),
                            _ => Some((
                                CommandResponse::Static(
                                    b"-ERR invalid argument for CONFIG SET appendonly\r\n",
                                ),
                                false,
                            )),
                        }
                    }
                    b"maxmemory" => {
                        let Ok(raw_value) = std::str::from_utf8(value) else {
                            return Some((
                                CommandResponse::Static(RESP_ERR_CONFIG_SET_MAXMEMORY),
                                false,
                            ));
                        };
                        let Ok(max_memory) = raw_value.parse::<usize>() else {
                            return Some((
                                CommandResponse::Static(RESP_ERR_CONFIG_SET_MAXMEMORY),
                                false,
                            ));
                        };
                        self.keyspace.set_max_memory(max_memory);
                        Some((CommandResponse::Static(b"+OK\r\n"), false))
                    }
                    b"maxmemory-policy" => {
                        let Some(policy) = EvictionPolicy::parse_bytes(value) else {
                            return Some((
                                CommandResponse::Static(RESP_ERR_CONFIG_SET_POLICY),
                                false,
                            ));
                        };
                        self.keyspace.set_eviction_policy(policy);
                        Some((CommandResponse::Static(b"+OK\r\n"), false))
                    }
                    _ => None, // Fall through to engine.
                }
            }
            _ => None, // RESETSTAT, REWRITE, etc. — fall through.
        }
    }

    // ── Write handler ──────────────────────────────────────────────
}
