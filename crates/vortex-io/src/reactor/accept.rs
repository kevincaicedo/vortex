use super::*;

impl Reactor {
    pub(super) fn handle_accept(&mut self, cqe: &Completion) {
        if cqe.result < 0 {
            let errno = -cqe.result;
            // EAGAIN/EWOULDBLOCK is normal for non-blocking accept — retry.
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                self.local_metrics.record_accept_eagain_rearm();
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
            self.local_metrics.record_accept_budget_exhaustion();
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
            match accept_ready_fd(self.listener_fd) {
                Ok(fd) => {
                    self.handle_accepted_fd(fd);
                    accepted += 1;
                    continue;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    return (accepted, false);
                }
                Err(err) => {
                    tracing::warn!(errno = err.raw_os_error().unwrap_or(1), "accept failed");
                    return (accepted, false);
                }
            }
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

        if self.connections.len() >= self.config.max_connections {
            tracing::warn!("max connections reached, rejecting");
            self.keyspace
                .record_reactor_overload_connection_dropped(self.id);
            unsafe {
                libc::close(new_fd);
            }
            return;
        }

        if let Err(error) = configure_accepted_fd(new_fd) {
            tracing::warn!(
                reactor_id = self.id,
                error = %error,
                "accepted fd configuration failed, rejecting connection"
            );
            self.keyspace
                .record_reactor_overload_connection_dropped(self.id);
            // SAFETY: `new_fd` has not been inserted into the connection slab,
            // so this path still owns the descriptor.
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

fn configure_accepted_fd(fd: RawFd) -> io::Result<()> {
    // Best-effort TCP latency hint. This can fail for non-TCP test sockets; the
    // reactor requires only nonblocking and close-on-exec as hard invariants.
    let nodelay: libc::c_int = 1;
    // SAFETY: `fd` is an accepted descriptor not yet inserted into reactor state.
    unsafe {
        let _ = libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &nodelay as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }

    // SAFETY: `fcntl` operates on the accepted descriptor only; errors are
    // returned to the caller before the descriptor enters connection state.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `flags` came from `F_GETFL` for this descriptor.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: same descriptor; `F_GETFD`/`F_SETFD` do not touch memory.
    let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if fd_flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd_flags` came from `F_GETFD` for this descriptor.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

fn accept_ready_fd(listener_fd: RawFd) -> io::Result<RawFd> {
    let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut addr_len: libc::socklen_t =
        std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: listener_fd is a valid nonblocking listener owned by the reactor,
    // and addr/addr_len point to stack storage valid for the syscall.
    let fd = unsafe {
        accept_ready_syscall(
            listener_fd,
            &mut addr as *mut libc::sockaddr_storage as *mut libc::sockaddr,
            &mut addr_len,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[inline]
unsafe fn accept_ready_syscall(
    listener_fd: RawFd,
    addr: *mut libc::sockaddr,
    addr_len: *mut libc::socklen_t,
) -> RawFd {
    // SAFETY: caller provides a valid listener fd and sockaddr storage.
    unsafe {
        libc::accept4(
            listener_fd,
            addr,
            addr_len,
            libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
        )
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
#[inline]
unsafe fn accept_ready_syscall(
    listener_fd: RawFd,
    addr: *mut libc::sockaddr,
    addr_len: *mut libc::socklen_t,
) -> RawFd {
    // SAFETY: caller provides a valid listener fd and sockaddr storage. The
    // accepted fd is configured by handle_accepted_fd before slab insertion.
    unsafe { libc::accept(listener_fd, addr, addr_len) }
}
