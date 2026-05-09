use super::*;

impl Reactor {
    pub(super) fn handle_accept(&mut self, cqe: &Completion) {
        if cqe.result < 0 {
            let errno = -cqe.result;
            // EAGAIN/EWOULDBLOCK is normal for non-blocking accept — retry.
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                self.keyspace.record_reactor_accept_eagain_rearm(self.id);
                if !self.draining && !self.accept_backpressure_active() {
                    let _ = self.submit_accept_rearm();
                } else if !self.draining {
                    self.keyspace
                        .record_reactor_overload_accept_throttled(self.id);
                    self.overload.mark_accept_rearm_deferred();
                }
                return;
            }
            tracing::warn!(errno, "accept failed");
            if !self.draining && !self.accept_backpressure_active() {
                let _ = self.submit_accept_rearm();
            } else if !self.draining {
                self.keyspace
                    .record_reactor_overload_accept_throttled(self.id);
                self.overload.mark_accept_rearm_deferred();
            }
            return;
        }

        if self.draining {
            // SAFETY: fd came from a successful accept completion and is not
            // tracked by any reactor slot in drain mode.
            unsafe {
                libc::close(cqe.result);
            }
            return;
        }

        if self.accept_backpressure_active() {
            self.keyspace
                .record_reactor_overload_accept_throttled(self.id);
            self.keyspace
                .record_reactor_overload_connection_dropped(self.id);
            self.overload.mark_accept_rearm_deferred();
            // SAFETY: fd came from a successful accept completion and is not
            // tracked by any reactor slot when admission rejects it.
            unsafe {
                libc::close(cqe.result);
            }
            return;
        }

        self.handle_accepted_fd(cqe.result);
        let (drained, accept_budget_exhausted) = self.drain_ready_accepts();
        self.local_metrics.record_accept_drain(drained);
        if accept_budget_exhausted {
            self.keyspace
                .record_reactor_accept_budget_exhaustion(self.id);
        }

        // Re-arm accept (unless draining).
        if !self.draining && !self.accept_backpressure_active() {
            let _ = self.submit_accept_rearm();
        } else if !self.draining {
            self.keyspace
                .record_reactor_overload_accept_throttled(self.id);
            self.overload.mark_accept_rearm_deferred();
        }
    }

    pub(super) fn drain_ready_accepts(&mut self) -> (usize, bool) {
        if self.draining {
            return (0, false);
        }

        let mut accepted = 0usize;
        let budget = self.config.budgets.accept.get();
        for _ in 0..budget {
            if self.accept_backpressure_active() {
                self.keyspace
                    .record_reactor_overload_accept_throttled(self.id);
                self.overload.mark_accept_rearm_deferred();
                return (accepted, false);
            }
            let result = unsafe {
                let mut addr: libc::sockaddr_storage = std::mem::zeroed();
                let mut addr_len: libc::socklen_t =
                    std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                libc::accept(
                    self.listener_fd,
                    &mut addr as *mut libc::sockaddr_storage as *mut libc::sockaddr,
                    &mut addr_len,
                )
            };

            if result >= 0 {
                self.handle_accepted_fd(result);
                accepted += 1;
                continue;
            }

            let err = std::io::Error::last_os_error();
            if err.kind() != std::io::ErrorKind::WouldBlock {
                tracing::warn!(errno = err.raw_os_error().unwrap_or(1), "accept failed");
            }
            return (accepted, false);
        }
        (accepted, accepted == budget)
    }

    pub(super) fn handle_accepted_fd(&mut self, new_fd: RawFd) {
        if self.draining {
            // SAFETY: `new_fd` is not inserted into the connection slab while
            // draining, so this path owns the fd and must close it.
            unsafe {
                libc::close(new_fd);
            }
            return;
        }

        if self.accept_backpressure_active() {
            self.keyspace
                .record_reactor_overload_accept_throttled(self.id);
            self.keyspace
                .record_reactor_overload_connection_dropped(self.id);
            self.overload.mark_accept_rearm_deferred();
            // SAFETY: `new_fd` is not inserted into the connection slab after
            // admission rejects it, so this path owns and closes the fd.
            unsafe {
                libc::close(new_fd);
            }
            return;
        }

        unsafe {
            let nodelay: libc::c_int = 1;
            libc::setsockopt(
                new_fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &nodelay as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            let flags = libc::fcntl(new_fd, libc::F_GETFL);
            libc::fcntl(new_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        if self.connections.len() >= self.config.max_connections {
            tracing::warn!("max connections reached, rejecting");
            self.keyspace
                .record_reactor_overload_connection_dropped(self.id);
            unsafe {
                libc::close(new_fd);
            }
            return;
        }

        let read_idx = match self.buffer_pool.lease_index() {
            Some(idx) => idx,
            None => {
                tracing::warn!("buffer pool exhausted (read), rejecting connection");
                self.keyspace
                    .record_reactor_overload_connection_dropped(self.id);
                unsafe {
                    libc::close(new_fd);
                }
                return;
            }
        };

        let mut meta = ConnectionMeta::new(new_fd, read_idx as u32);
        meta.last_active = self.now_secs;
        meta.read_buf_offset = read_idx as u32;
        meta.read_buf_len = 0;
        meta.write_buf_len = 0;
        let conn_id = self.connections.insert(meta);

        if conn_id < self.generations.len() {
            self.generations[conn_id] = self.generations[conn_id].wrapping_add(1) & 0xFF_FFFF;
        }
        let cgen = self.generations.get(conn_id).copied().unwrap_or(0);
        self.writev_states[conn_id].clear();
        self.inflight_ops[conn_id] = InflightSet::default();
        self.close_started_nanos[conn_id] = 0;
        self.close_finalization_pending[conn_id] = false;
        self.transaction_states[conn_id].reset_all();
        self.clear_command_accumulator(conn_id);

        tracing::debug!(
            reactor_id = self.id,
            conn_id,
            fd = new_fd,
            cgen,
            read_idx,
            "connection accepted"
        );

        if self.connection_timeout > 0 {
            let deadline = self.now_secs + self.connection_timeout;
            let entry = self.timer_wheel.schedule(conn_id, cgen, deadline);
            if let Some(c) = self.connections.get_mut(conn_id) {
                c.timer_slot = entry;
            }
        }

        self.submit_read_for(conn_id, new_fd);
    }

    // ── Read handler ───────────────────────────────────────────────
}
