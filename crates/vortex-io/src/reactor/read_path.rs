use super::*;

impl Reactor {
    pub(super) fn handle_read(&mut self, conn_id: usize, cqe: &Completion) {
        self.mark_inflight_completed(conn_id, OpType::Read);

        // Ignore late completions for connections being torn down.
        if self.connections.is_closing(conn_id) {
            self.maybe_finalize_close(conn_id);
            return;
        }

        if cqe.result <= 0 {
            // EOF or error — close connection.
            if cqe.result == 0 {
                tracing::debug!(conn_id, "client disconnected (EOF)");
            } else {
                tracing::debug!(conn_id, errno = -cqe.result, "read error");
            }
            self.close_connection(conn_id);
            return;
        }

        let bytes_read = cqe.result as usize;
        let buffer_size = self.buffer_pool.buffer_size();
        let (fd, cursor) = match self.connections.get(conn_id) {
            Some(c) => (c.fd, c.read_buf_len as usize),
            None => return, // Connection already removed.
        };
        let Some(next_cursor) = cursor.checked_add(bytes_read) else {
            tracing::warn!(
                conn_id,
                cursor,
                bytes_read,
                "read completion length overflow, closing connection"
            );
            self.close_connection(conn_id);
            return;
        };
        if next_cursor > buffer_size || next_cursor > u32::MAX as usize {
            tracing::warn!(
                conn_id,
                cursor,
                bytes_read,
                buffer_size,
                "read completion exceeded leased buffer, closing connection"
            );
            self.close_connection(conn_id);
            return;
        }

        // Advance read cursor in ConnectionMeta.
        if let Some(c) = self.connections.get_mut(conn_id) {
            c.read_buf_len = next_cursor as u32;
            c.last_active = self.now_secs;
        }

        // Attempt to parse and process commands from the read buffer.
        self.process_commands(conn_id, fd);
    }

    #[inline]
    pub(super) fn accumulator_retain_capacity(&self) -> usize {
        self.config.buffer_size.min(64 * 1024)
    }

    #[inline]
    pub(super) fn parser_accumulator_cap(&self) -> usize {
        self.config
            .max_request_bytes
            .min(self.config.connection_caps.max_parser_accumulator_bytes)
    }

    #[inline]
    pub(super) fn has_pending_command_bytes(&self, conn_id: usize) -> bool {
        self.command_accumulators
            .get(conn_id)
            .is_some_and(|acc| !acc.is_empty())
    }

    #[inline]
    pub(super) fn clear_command_accumulator(&mut self, conn_id: usize) {
        if let Some(acc) = self.command_accumulators.get_mut(conn_id) {
            self.overload
                .subtract_parser_accumulator_bytes(acc.bytes.len());
            acc.clear();
        }
    }

    #[inline]
    pub(super) fn take_command_accumulator(&mut self, conn_id: usize) -> Vec<u8> {
        let bytes = self.command_accumulators[conn_id].take();
        self.overload.subtract_parser_accumulator_bytes(bytes.len());
        bytes
    }

    #[inline]
    pub(super) fn put_command_accumulator(
        &mut self,
        conn_id: usize,
        bytes: Vec<u8>,
        retain_capacity: usize,
    ) {
        self.overload.add_parser_accumulator_bytes(bytes.len());
        self.command_accumulators[conn_id].put(bytes, retain_capacity);
    }

    #[inline]
    pub(super) fn set_connection_write_len(&mut self, conn_id: usize, len: usize) {
        let Some(conn) = self.connections.get_mut(conn_id) else {
            return;
        };
        let old = conn.write_buf_len as usize;
        conn.write_buf_len = len as u32;
        self.overload.adjust_pending_response_bytes(old, len);
    }

    pub(super) fn append_to_accumulator(
        max_accumulator_bytes: usize,
        accumulator: &mut Vec<u8>,
        incoming: &[u8],
    ) -> bool {
        let Some(new_len) = accumulator.len().checked_add(incoming.len()) else {
            return false;
        };
        if new_len > max_accumulator_bytes {
            return false;
        }
        accumulator.extend_from_slice(incoming);
        true
    }

    pub(super) fn shift_read_buffer(
        &mut self,
        conn_id: usize,
        read_idx: usize,
        consumed: usize,
        cursor: usize,
    ) {
        if consumed == 0 {
            return;
        }
        let remaining = cursor.saturating_sub(consumed);
        let read_ptr = self.buffer_pool.ptr(read_idx);
        // SAFETY: Source and dest are within the same mmap buffer. The regions
        // may overlap, so we use `copy` (memmove) not `copy_nonoverlapping`.
        unsafe {
            std::ptr::copy(read_ptr.add(consumed), read_ptr, remaining);
        }
        if let Some(c) = self.connections.get_mut(conn_id) {
            c.read_buf_len = remaining as u32;
        }
    }

    #[inline]
    pub(super) fn response_wire_len(response: &CommandResponse) -> usize {
        match response {
            CommandResponse::Static(buf) => buf.len(),
            CommandResponse::Inline(inline) => inline.as_bytes().len(),
            CommandResponse::Owned(bytes) => bytes.len(),
            CommandResponse::Frame(resp_frame) => RespSerializer::serialized_len(resp_frame),
        }
    }

    #[inline]
    pub(super) fn push_command_response_unchecked(
        &mut self,
        conn_id: usize,
        response: CommandResponse,
    ) {
        match response {
            CommandResponse::Static(buf) => self.writev_states[conn_id].push_static(buf),
            CommandResponse::Inline(inline) => {
                self.writev_states[conn_id].push_inline(inline.as_bytes());
            }
            CommandResponse::Owned(bytes) => {
                self.writev_states[conn_id].push_owned(bytes);
            }
            CommandResponse::Frame(resp_frame) => {
                self.writev_states[conn_id].push_frame(resp_frame);
            }
        }
    }

    #[inline]
    pub(super) fn try_push_command_response(
        &mut self,
        conn_id: usize,
        response: CommandResponse,
    ) -> bool {
        let response_len = Self::response_wire_len(&response);
        let current = self.writev_states[conn_id].queued_len();
        let Some(next) = current.checked_add(response_len) else {
            return false;
        };
        if next > self.config.connection_caps.max_pending_response_bytes {
            return false;
        }
        self.push_command_response_unchecked(conn_id, response);
        true
    }

    #[inline]
    pub(super) fn push_static_if_response_cap_allows(
        &mut self,
        conn_id: usize,
        response: &'static [u8],
    ) {
        let current = self.writev_states[conn_id].queued_len();
        let Some(next) = current.checked_add(response.len()) else {
            return;
        };
        if next <= self.config.connection_caps.max_pending_response_bytes {
            self.writev_states[conn_id].push_static(response);
        }
    }

    #[inline]
    pub(super) fn reject_response_cap(&mut self, conn_id: usize) {
        self.keyspace.record_reactor_response_cap_exceeded(self.id);
        self.push_static_if_response_cap_allows(conn_id, RESP_ERR_RESPONSE_TOO_LARGE);
    }

    pub(super) fn process_command_slice(
        &mut self,
        conn_id: usize,
        input: &[u8],
        parse_entries: &mut Vec<TapeEntry>,
        command_budget: &mut CommandSliceBudget,
    ) -> CommandSliceOutcome {
        let mut offset = 0usize;
        let mut close_after_write = false;
        let mut yielded = false;
        let mut need_more_data = false;

        while offset < input.len() && command_budget.remaining() != 0 {
            match BorrowedRespTape::parse_pipeline_limited_into(
                &input[offset..],
                parse_entries,
                if self.aof_write_backpressure_active() {
                    1
                } else {
                    command_budget.remaining()
                },
            ) {
                Ok(tape) => {
                    let batch_end = offset + tape.consumed();
                    let mut batch_width = 0usize;
                    for frame in tape.iter() {
                        if self.should_defer_command_frame(conn_id, &frame) {
                            self.defer_command_processing(conn_id);
                            yielded = true;
                            break;
                        }
                        batch_width += 1;
                        let (response, should_close) = self.dispatch_command(conn_id, &frame);
                        if should_close {
                            close_after_write = true;
                        }
                        if !self.try_push_command_response(conn_id, response) {
                            self.reject_response_cap(conn_id);
                            close_after_write = true;
                            break;
                        }
                    }
                    if batch_width != 0 {
                        self.local_metrics.record_command_batch(batch_width);
                        command_budget.consume_commands(batch_width);
                    }

                    if yielded && batch_width == 0 {
                        break;
                    }
                    offset = batch_end;
                    if close_after_write {
                        break;
                    }

                    if command_budget.should_yield_after_batch() {
                        yielded = offset < input.len();
                        break;
                    }
                }
                Err(ParseError::NeedMoreData) => {
                    self.keyspace.record_reactor_parser_resume(self.id);
                    need_more_data = true;
                    break;
                }
                Err(
                    ParseError::FrameTooLarge
                    | ParseError::NestingTooDeep
                    | ParseError::InvalidFrame,
                ) => {
                    self.push_static_if_response_cap_allows(conn_id, RESP_ERR_PROTOCOL);
                    close_after_write = true;
                    offset = input.len();
                    break;
                }
            }
        }
        if !close_after_write && command_budget.remaining() == 0 && offset < input.len() {
            yielded = true;
        }

        CommandSliceOutcome {
            consumed: offset,
            close_after_write,
            yielded,
            need_more_data,
        }
    }

    pub(super) fn finish_command_processing(
        &mut self,
        conn_id: usize,
        fd: RawFd,
        close_after_write: bool,
        yielded: bool,
    ) {
        if close_after_write {
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.flags |= ConnectionFlags::CLOSE_AFTER_WRITE;
            }
            self.clear_command_accumulator(conn_id);
        }

        if yielded {
            self.keyspace
                .record_reactor_command_budget_exhaustion(self.id);
            self.keyspace.record_reactor_yielded_connection(self.id);
        }

        self.writev_states[conn_id].finalize();
        if self.writev_states[conn_id].remaining_len() != 0
            && self.writev_states[conn_id].remaining_writev_chunks(self.config.budgets.writev)
                > self.config.connection_caps.max_writev_chunks
        {
            self.keyspace
                .record_reactor_writev_chunk_cap_exceeded(self.id);
            self.writev_states[conn_id].compact_to_owned();
        }
        let total = self.writev_states[conn_id].remaining_len();
        if total > 0 {
            self.local_metrics.record_queued_response_bytes(total);
            self.set_connection_write_len(conn_id, total);
            let (iov_ptr, iov_count) = {
                let remaining_iovecs =
                    self.writev_states[conn_id].remaining_iovec_batch(self.config.budgets.writev);
                (remaining_iovecs.as_ptr(), remaining_iovecs.len())
            };
            let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
            let Some(token) = Self::conn_token(conn_id, cgen, OpType::Writev) else {
                self.writev_states[conn_id].clear();
                self.close_connection(conn_id);
                return;
            };
            // SAFETY: `iov_ptr` points into `writev_states[conn_id]`, and the
            // state is not cleared or advanced again until the writev
            // completion is handled.
            let batch = match unsafe { IovecBatch::new(ConnFd::new(fd), iov_ptr, iov_count) } {
                Ok(batch) => batch,
                Err(error) => {
                    tracing::warn!(conn_id, error = %error, "failed to create writev batch");
                    self.writev_states[conn_id].clear();
                    self.close_connection(conn_id);
                    return;
                }
            };
            if self
                .submit_backend_op("writev", |backend| backend.submit_writev(batch, token))
                .is_err()
            {
                self.writev_states[conn_id].clear();
                self.close_connection(conn_id);
            } else {
                if self.writev_states[conn_id].remaining_iovecs().len() > iov_count {
                    self.keyspace
                        .record_reactor_writev_budget_exhaustion(self.id);
                }
                self.local_metrics.record_writev_chunk(iov_count);
                self.mark_inflight_submitted(conn_id, OpType::Writev);
            }
        } else if close_after_write {
            self.close_connection(conn_id);
        } else if yielded {
            self.writev_states[conn_id].clear();
        } else if self.draining {
            // Drain mode: no complete command and nothing to write — close.
            self.close_connection(conn_id);
        } else {
            // No complete command yet — re-arm read.
            self.writev_states[conn_id].clear();
            self.submit_read_for(conn_id, fd);
        }
    }

    /// Parse RESP frames from the read buffer and generate responses.
    ///
    /// Responses are accumulated into [`PendingWritev`] and submitted via
    /// `writev` / `IORING_OP_WRITEV`. The fixed read buffer is transport
    /// staging only; oversized partial requests are promoted into a bounded
    /// per-connection accumulator.
    pub(super) fn process_commands(&mut self, conn_id: usize, fd: RawFd) {
        if self.has_pending_command_bytes(conn_id) {
            self.process_accumulated_commands(conn_id, fd);
            return;
        }

        let (read_idx, cursor) = match self.connections.get(conn_id) {
            Some(c) => (c.read_buf_offset as usize, c.read_buf_len as usize),
            None => return,
        };

        if cursor == 0 {
            self.submit_read_for(conn_id, fd);
            return;
        }

        self.writev_states[conn_id].clear();

        let read_ptr = self.buffer_pool.ptr(read_idx);

        // SAFETY: read_ptr points to a reactor-owned mmap-backed region that is
        // valid for `cursor` bytes. Access is single-threaded on the reactor.
        let read_slice = unsafe { std::slice::from_raw_parts(read_ptr, cursor) };

        let mut parse_entries = std::mem::take(&mut self.parse_entries);
        let mut command_budget = CommandSliceBudget::new(
            self.config.budgets.command,
            self.config.budgets.time,
            self.cached_nanos,
        );
        let mut outcome = self.process_command_slice(
            conn_id,
            read_slice,
            &mut parse_entries,
            &mut command_budget,
        );

        if outcome.need_more_data
            && outcome.consumed == 0
            && cursor == self.buffer_pool.buffer_size()
        {
            let mut accumulator = self.take_command_accumulator(conn_id);
            if Self::append_to_accumulator(
                self.parser_accumulator_cap(),
                &mut accumulator,
                read_slice,
            ) {
                outcome.consumed = cursor;
            } else {
                accumulator.clear();
                self.keyspace.record_reactor_request_cap_exceeded(self.id);
                self.push_static_if_response_cap_allows(conn_id, RESP_ERR_REQUEST_TOO_LARGE);
                outcome.close_after_write = true;
                outcome.consumed = cursor;
            }
            let retain = self.accumulator_retain_capacity();
            self.put_command_accumulator(conn_id, accumulator, retain);
        }

        if outcome.consumed > 0 {
            self.shift_read_buffer(conn_id, read_idx, outcome.consumed, cursor);
        }

        parse_entries.clear();
        self.parse_entries = parse_entries;

        self.finish_command_processing(conn_id, fd, outcome.close_after_write, outcome.yielded);
    }

    pub(super) fn process_accumulated_commands(&mut self, conn_id: usize, fd: RawFd) {
        let (read_idx, cursor) = match self.connections.get(conn_id) {
            Some(c) => (c.read_buf_offset as usize, c.read_buf_len as usize),
            None => return,
        };

        let mut accumulated = self.take_command_accumulator(conn_id);
        if cursor != 0 {
            let read_ptr = self.buffer_pool.ptr(read_idx);
            // SAFETY: read_ptr points to a reactor-owned mmap-backed region
            // that is valid for `cursor` bytes. Access is single-threaded on
            // the reactor.
            let read_slice = unsafe { std::slice::from_raw_parts(read_ptr, cursor) };
            if !Self::append_to_accumulator(
                self.parser_accumulator_cap(),
                &mut accumulated,
                read_slice,
            ) {
                accumulated.clear();
                let retain = self.accumulator_retain_capacity();
                self.put_command_accumulator(conn_id, accumulated, retain);
                if let Some(c) = self.connections.get_mut(conn_id) {
                    c.read_buf_len = 0;
                }
                self.writev_states[conn_id].clear();
                self.keyspace.record_reactor_request_cap_exceeded(self.id);
                self.push_static_if_response_cap_allows(conn_id, RESP_ERR_REQUEST_TOO_LARGE);
                self.finish_command_processing(conn_id, fd, true, false);
                return;
            }
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.read_buf_len = 0;
            }
        }

        if accumulated.is_empty() {
            let retain = self.accumulator_retain_capacity();
            self.put_command_accumulator(conn_id, accumulated, retain);
            self.submit_read_for(conn_id, fd);
            return;
        }

        self.writev_states[conn_id].clear();

        let mut parse_entries = std::mem::take(&mut self.parse_entries);
        let mut command_budget = CommandSliceBudget::new(
            self.config.budgets.command,
            self.config.budgets.time,
            self.cached_nanos,
        );
        let mut outcome = self.process_command_slice(
            conn_id,
            accumulated.as_slice(),
            &mut parse_entries,
            &mut command_budget,
        );

        if outcome.need_more_data && accumulated.len() >= self.parser_accumulator_cap() {
            accumulated.clear();
            self.keyspace.record_reactor_request_cap_exceeded(self.id);
            self.push_static_if_response_cap_allows(conn_id, RESP_ERR_REQUEST_TOO_LARGE);
            outcome.close_after_write = true;
            outcome.consumed = 0;
        } else if outcome.close_after_write {
            accumulated.clear();
        } else if outcome.consumed != 0 {
            let remaining = accumulated.len() - outcome.consumed;
            accumulated.copy_within(outcome.consumed.., 0);
            accumulated.truncate(remaining);
        }

        parse_entries.clear();
        self.parse_entries = parse_entries;

        let retain = self.accumulator_retain_capacity();
        self.put_command_accumulator(conn_id, accumulated, retain);

        self.finish_command_processing(conn_id, fd, outcome.close_after_write, outcome.yielded);
    }
}
