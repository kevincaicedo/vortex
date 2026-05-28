use super::*;

pub(super) struct PendingWritev {
    writer: IovecWriter,
    frames: Vec<RespFrame>,
    owned: Vec<Box<[u8]>>,
    iovecs: Vec<libc::iovec>,
    iov_start: usize,
    total_len: usize,
}

impl PendingWritev {
    pub(super) fn new() -> Self {
        Self {
            writer: IovecWriter::new(),
            frames: Vec::new(),
            owned: Vec::new(),
            iovecs: Vec::new(),
            iov_start: 0,
            total_len: 0,
        }
    }

    pub(super) fn clear(&mut self) {
        self.writer.clear();
        self.frames.clear();
        self.owned.clear();
        self.iovecs.clear();
        self.iov_start = 0;
        self.total_len = 0;
    }

    pub(super) fn push_static(&mut self, buf: &'static [u8]) {
        self.writer.push_static(buf);
    }

    pub(super) fn push_inline(&mut self, buf: &[u8]) {
        self.writer.push_scratch(buf);
    }

    pub(super) fn push_owned(&mut self, buf: Box<[u8]>) {
        if buf.is_empty() {
            return;
        }
        self.owned.push(buf);
        let idx = self.owned.len() - 1;
        self.writer.push_bytes(self.owned[idx].as_ref());
    }

    pub(super) fn push_frame(&mut self, frame: RespFrame) {
        self.frames.push(frame);
        let idx = self.frames.len() - 1;
        RespSerializer::serialize_to_iovecs(&self.frames[idx], &mut self.writer);
    }

    #[inline]
    pub(super) fn queued_len(&self) -> usize {
        self.writer.total_len()
    }

    pub(super) fn finalize(&mut self) {
        self.total_len = self.writer.total_len();
        self.writer.write_raw_iovecs(&mut self.iovecs);
        self.iov_start = 0;
    }

    pub(super) fn remaining_len(&self) -> usize {
        self.total_len
    }

    #[cfg(test)]
    pub(super) fn raw_iovec_capacity(&self) -> usize {
        self.iovecs.capacity()
    }

    pub(super) fn remaining_iovecs(&self) -> &[libc::iovec] {
        &self.iovecs[self.iov_start..]
    }

    pub(super) fn remaining_iovec_batch(&self, budget: WritevBudget) -> &[libc::iovec] {
        let remaining = self.remaining_iovecs();
        let count = remaining
            .len()
            .min(budget.get())
            .min(IovecBatch::MAX_SEGMENTS);
        &remaining[..count]
    }

    #[inline]
    pub(super) fn remaining_writev_chunks(&self, budget: WritevBudget) -> usize {
        let chunk_width = budget.get().min(IovecBatch::MAX_SEGMENTS);
        self.remaining_iovecs().len().div_ceil(chunk_width)
    }

    pub(super) fn compact_to_owned(&mut self) {
        let bytes = self.writer.flatten().into_boxed_slice();
        self.clear();
        self.push_owned(bytes);
        self.finalize();
    }

    pub(super) fn advance(&mut self, mut bytes_written: usize) {
        while bytes_written > 0 && self.iov_start < self.iovecs.len() {
            let iov = &mut self.iovecs[self.iov_start];
            if bytes_written < iov.iov_len {
                // SAFETY: advancing within the currently outstanding iovec segment.
                iov.iov_base =
                    unsafe { (iov.iov_base as *mut u8).add(bytes_written) }.cast::<libc::c_void>();
                iov.iov_len -= bytes_written;
                self.total_len -= bytes_written;
                return;
            }

            let consumed = iov.iov_len;
            bytes_written -= consumed;
            self.total_len -= consumed;
            self.iov_start += 1;
        }
    }
}

impl Reactor {
    pub(super) fn handle_write(&mut self, conn_id: usize, op: OpType, cqe: &Completion) {
        self.mark_inflight_completed(conn_id, op);

        // Ignore late completions for connections being torn down.
        if self.connections.is_closing(conn_id) {
            self.set_connection_write_len(conn_id, 0);
            if matches!(op, OpType::Writev) {
                self.writev_states[conn_id].clear();
            }
            self.maybe_finalize_close(conn_id);
            return;
        }

        if cqe.result < 0 {
            if cqe.result == -libc::EAGAIN || cqe.result == -libc::EWOULDBLOCK {
                self.resubmit_would_block_write(conn_id, op);
                return;
            }
            tracing::debug!(conn_id, errno = -cqe.result, "write error");
            self.close_connection(conn_id);
            return;
        }

        let (fd, write_idx, total) = match self.connections.get(conn_id) {
            Some(c) => (c.fd, c.write_buf_offset as usize, c.write_buf_len as usize),
            None => return,
        };

        let bytes_written = cqe.result as usize;
        if bytes_written > total {
            tracing::warn!(
                conn_id,
                bytes_written,
                total,
                "write completion exceeded pending write length, closing connection"
            );
            self.close_connection(conn_id);
            return;
        }

        if bytes_written < total {
            let remaining = total - bytes_written;
            match op {
                OpType::Write => {
                    // Partial write — shift unwritten bytes to front and resubmit.
                    let write_ptr = self.buffer_pool.ptr(write_idx);
                    // SAFETY: Both source and dest are within the same mmap buffer.
                    unsafe {
                        std::ptr::copy(write_ptr.add(bytes_written), write_ptr, remaining);
                    }
                    self.set_connection_write_len(conn_id, remaining);
                    let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
                    let Some(token) = Self::conn_token(conn_id, cgen, OpType::Write) else {
                        self.close_connection(conn_id);
                        return;
                    };
                    let fixed = match self.fixed_buffer_id(write_idx) {
                        Ok(fixed) => fixed,
                        Err(error) => {
                            tracing::warn!(conn_id, write_idx, error = %error, "invalid write buffer id");
                            self.close_connection(conn_id);
                            return;
                        }
                    };
                    let submit_name = if fixed.is_some() {
                        "write_fixed_partial"
                    } else {
                        "write_partial"
                    };
                    // SAFETY: `write_ptr..write_ptr+remaining` is the
                    // connection's leased write buffer and remains live until
                    // the write completion is processed.
                    let lease = match unsafe {
                        WriteLease::new(ConnFd::new(fd), write_ptr as *const u8, remaining, fixed)
                    } {
                        Ok(lease) => lease,
                        Err(error) => {
                            tracing::warn!(conn_id, error = %error, "failed to create write lease");
                            self.close_connection(conn_id);
                            return;
                        }
                    };
                    if self
                        .submit_backend_op(submit_name, |backend| {
                            backend.submit_write(lease, token)
                        })
                        .is_ok()
                    {
                        self.mark_inflight_submitted(conn_id, OpType::Write);
                    } else {
                        self.close_connection(conn_id);
                    }
                }
                OpType::Writev => {
                    self.writev_states[conn_id].advance(bytes_written);
                    self.set_connection_write_len(conn_id, remaining);
                    let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
                    let Some(token) = Self::conn_token(conn_id, cgen, OpType::Writev) else {
                        self.writev_states[conn_id].clear();
                        self.close_connection(conn_id);
                        return;
                    };
                    let (iov_ptr, iov_count) = {
                        let remaining_iovecs = self.writev_states[conn_id]
                            .remaining_iovec_batch(self.config.budgets.writev);
                        (remaining_iovecs.as_ptr(), remaining_iovecs.len())
                    };
                    // SAFETY: `iov_ptr` points into `writev_states[conn_id]`,
                    // and the state is stable until the writev completion is
                    // processed.
                    let batch =
                        match unsafe { IovecBatch::new(ConnFd::new(fd), iov_ptr, iov_count) } {
                            Ok(batch) => batch,
                            Err(error) => {
                                tracing::warn!(
                                    conn_id,
                                    error = %error,
                                    "failed to create partial writev batch"
                                );
                                self.writev_states[conn_id].clear();
                                self.close_connection(conn_id);
                                return;
                            }
                        };
                    if self
                        .submit_backend_op("writev_partial", |backend| {
                            backend.submit_writev(batch, token)
                        })
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
                }
                _ => unreachable!(),
            }
        } else if self.draining
            || self
                .connections
                .get(conn_id)
                .is_some_and(|c| (c.flags & ConnectionFlags::CLOSE_AFTER_WRITE) != 0)
        {
            // Drain mode: writes flushed — close connection, don't re-arm read.
            if matches!(op, OpType::Writev) {
                self.writev_states[conn_id].clear();
            }
            self.close_connection(conn_id);
        } else {
            // Write complete — clear write length and re-arm read.
            self.set_connection_write_len(conn_id, 0);
            if matches!(op, OpType::Writev) {
                self.writev_states[conn_id].clear();
            }
            if self
                .connections
                .get(conn_id)
                .is_some_and(|c| c.read_buf_len != 0)
                || self.has_pending_command_bytes(conn_id)
            {
                self.process_commands(conn_id, fd);
            } else {
                self.submit_read_for(conn_id, fd);
            }
        }
    }

    fn resubmit_would_block_write(&mut self, conn_id: usize, op: OpType) {
        if self.connections.is_closing(conn_id) {
            self.set_connection_write_len(conn_id, 0);
            if matches!(op, OpType::Writev) {
                self.writev_states[conn_id].clear();
            }
            self.maybe_finalize_close(conn_id);
            return;
        }

        let (fd, write_idx, len) = match self.connections.get(conn_id) {
            Some(c) => (c.fd, c.write_buf_offset as usize, c.write_buf_len as usize),
            None => return,
        };
        if len == 0 {
            return;
        }
        let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
        let Some(token) = Self::conn_token(conn_id, cgen, op) else {
            if matches!(op, OpType::Writev) {
                self.writev_states[conn_id].clear();
            }
            self.close_connection(conn_id);
            return;
        };

        match op {
            OpType::Write => {
                let write_ptr = self.buffer_pool.ptr(write_idx);
                let fixed = match self.fixed_buffer_id(write_idx) {
                    Ok(fixed) => fixed,
                    Err(error) => {
                        tracing::warn!(
                            conn_id,
                            write_idx,
                            error = %error,
                            "invalid write buffer id after write EAGAIN"
                        );
                        self.close_connection(conn_id);
                        return;
                    }
                };
                let submit_name = if fixed.is_some() {
                    "write_fixed_eagain"
                } else {
                    "write_eagain"
                };
                // SAFETY: `write_ptr..write_ptr+len` is the connection's
                // leased write buffer and remains live until a terminal write
                // completion is handled.
                let lease = match unsafe {
                    WriteLease::new(ConnFd::new(fd), write_ptr as *const u8, len, fixed)
                } {
                    Ok(lease) => lease,
                    Err(error) => {
                        tracing::warn!(
                            conn_id,
                            error = %error,
                            "failed to recreate write lease after EAGAIN"
                        );
                        self.close_connection(conn_id);
                        return;
                    }
                };
                if self
                    .submit_backend_op(submit_name, |backend| backend.submit_write(lease, token))
                    .is_ok()
                {
                    self.mark_inflight_submitted(conn_id, OpType::Write);
                } else {
                    self.close_connection(conn_id);
                }
            }
            OpType::Writev => {
                let (iov_ptr, iov_count) = {
                    let remaining_iovecs = self.writev_states[conn_id]
                        .remaining_iovec_batch(self.config.budgets.writev);
                    (remaining_iovecs.as_ptr(), remaining_iovecs.len())
                };
                // SAFETY: `iov_ptr` points into `writev_states[conn_id]`.
                // The state is stable until the next writev completion.
                let batch = match unsafe { IovecBatch::new(ConnFd::new(fd), iov_ptr, iov_count) } {
                    Ok(batch) => batch,
                    Err(error) => {
                        tracing::warn!(
                            conn_id,
                            error = %error,
                            "failed to recreate writev batch after EAGAIN"
                        );
                        self.writev_states[conn_id].clear();
                        self.close_connection(conn_id);
                        return;
                    }
                };
                if self
                    .submit_backend_op("writev_eagain", |backend| {
                        backend.submit_writev(batch, token)
                    })
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
            }
            _ => {
                self.close_connection(conn_id);
            }
        }
    }

    // ── Close handler ──────────────────────────────────────────────
}
