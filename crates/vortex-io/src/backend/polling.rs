//! Cross-platform polling backend using the `polling` crate.
//!
//! Wraps epoll (Linux), kqueue (macOS/BSD). Emulates
//! io_uring's completion-based model by performing I/O inline on poll events
//! and generating synthetic [`Completion`] structs.
//!
//! ## Optimisation
//!
//! * **Edge-triggered persistent interest** – fds are registered with
//!   `PollMode::Edge` (kqueue `EV_CLEAR`, epoll `EPOLLET`) so that read
//!   interest persists across events. On read `EAGAIN`, re-registration is
//!   skipped when interest is already active, eliminating a `kevent`/`epoll_ctl`
//!   syscall per read cycle (~20% CPU reduction measured via profiling).
//! * **Write-readiness registration** – on `EAGAIN` from `write`/`writev`,
//!   the fd is registered for write-readiness with the poller instead of
//!   blindly re-trying every iteration (busy-poll elimination).
//! * **Immediate retry after poll** – when `poller.wait()` returns readiness
//!   events, all pending ops are retried in the *same* `completions()` call
//!   instead of deferring to the next reactor iteration.
//! * **Bounded fd tracking** – readiness interest is stored in a dense registry
//!   keyed by fd, so one high-numbered fd cannot resize backend state
//!   proportional to the raw descriptor value.
//! * **1 ms poll timeout** – matches io_uring's wait strategy and keeps tail
//!   latency low during idle periods.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::num::NonZeroUsize;
use std::os::fd::{BorrowedFd, RawFd};

use polling::{Event, Events, PollMode, Poller};

use super::{
    BackendDriver, Completion, CompletionToken, ConnFd, DecodedCompletionToken, IovecBatch,
    ListenerFd, OpType, ReadLease, SubmitError, WriteLease,
};

/// Interest flag: fd is registered for read-readiness.
const INTEREST_READABLE: u8 = 1;
/// Interest flag: fd is registered for write-readiness.
const INTEREST_WRITABLE: u8 = 2;
/// Maximum synthetic completions released from the backend ready queue per call.
const READY_DRAIN_BUDGET: usize = 256;
/// Maximum pending operations attempted by one polling backend pass.
const PENDING_OP_DRAIN_BUDGET: usize = 256;
/// Maximum OS readiness events returned by one `poller.wait()` call.
const READINESS_EVENT_BUDGET: usize = 256;

/// Pending operation queued for execution on the next `flush()`/`completions()`.
enum PendingOp {
    Accept {
        listener_fd: ListenerFd,
        token: CompletionToken,
    },
    Read {
        lease: ReadLease,
        token: CompletionToken,
    },
    Write {
        lease: WriteLease,
        token: CompletionToken,
    },
    Writev {
        batch: IovecBatch,
        token: CompletionToken,
    },
    Close {
        fd: ConnFd,
        token: CompletionToken,
    },
}

// SAFETY: PendingOp only stores typed leases created by the reactor for memory
// that remains valid until the matching completion is handled. PollingBackend
// is single-threaded (thread-per-core).
unsafe impl Send for PendingOp {}

struct ArmedRead {
    lease: ReadLease,
    token: CompletionToken,
}

// SAFETY: ArmedRead stores a reactor-owned typed lease and is only used by the
// single-threaded polling backend.
unsafe impl Send for ArmedRead {}

enum ArmedWrite {
    Write {
        lease: WriteLease,
        token: CompletionToken,
    },
    Writev {
        batch: IovecBatch,
        token: CompletionToken,
    },
}

// SAFETY: ArmedWrite stores reactor-owned typed leases and is only used by the
// single-threaded polling backend.
unsafe impl Send for ArmedWrite {}

#[derive(Clone, Copy)]
struct RegisteredInterest {
    fd: RawFd,
    flags: u8,
    read_token: Option<CompletionToken>,
    write_token: Option<CompletionToken>,
}

#[derive(Default)]
struct ConnOpHandles {
    pending_read: Option<CompletionToken>,
    pending_write: Option<CompletionToken>,
    pending_writev: Option<CompletionToken>,
    armed_read: Option<CompletionToken>,
    armed_write: Option<CompletionToken>,
}

/// Cross-platform I/O backend using the `polling` crate.
pub struct PollingBackend {
    poller: Poller,
    events: Events,
    pending: VecDeque<PendingOp>,
    pending_accept: Option<CompletionToken>,
    armed_accept: Option<(ListenerFd, CompletionToken)>,
    armed_reads: Vec<Option<ArmedRead>>,
    armed_writes: Vec<Option<ArmedWrite>>,
    conn_ops: Vec<ConnOpHandles>,
    canceled_pending: HashSet<CompletionToken>,
    ready: VecDeque<Completion>,
    ready_accept_tokens: Vec<CompletionToken>,
    ready_read_tokens: Vec<CompletionToken>,
    ready_write_tokens: Vec<CompletionToken>,
    /// Dense readiness-interest registry. The map gives O(1) lookup by raw fd
    /// without allocating state proportional to the numeric fd value.
    registered: Vec<RegisteredInterest>,
    registered_lookup: HashMap<RawFd, usize>,
}

impl PollingBackend {
    /// Creates a new polling backend.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            poller: Poller::new()?,
            events: Events::with_capacity(NonZeroUsize::new(READINESS_EVENT_BUDGET).unwrap()),
            pending: VecDeque::with_capacity(256),
            pending_accept: None,
            armed_accept: None,
            armed_reads: Vec::new(),
            armed_writes: Vec::new(),
            conn_ops: Vec::new(),
            canceled_pending: HashSet::new(),
            ready: VecDeque::with_capacity(64),
            ready_accept_tokens: Vec::with_capacity(4),
            ready_read_tokens: Vec::with_capacity(64),
            ready_write_tokens: Vec::with_capacity(64),
            registered: Vec::with_capacity(64),
            registered_lookup: HashMap::with_capacity(64),
        })
    }

    #[inline]
    fn ensure_read_capacity(&mut self, conn_id: usize) {
        if conn_id >= self.armed_reads.len() {
            self.armed_reads.resize_with(conn_id + 1, || None);
        }
    }

    #[inline]
    fn ensure_write_capacity(&mut self, conn_id: usize) {
        if conn_id >= self.armed_writes.len() {
            self.armed_writes.resize_with(conn_id + 1, || None);
        }
    }

    #[inline]
    fn ensure_conn_op_capacity(&mut self, conn_id: usize) {
        if conn_id >= self.conn_ops.len() {
            self.conn_ops
                .resize_with(conn_id + 1, ConnOpHandles::default);
        }
    }

    #[inline]
    fn registered_flags(&self, fd: RawFd) -> u8 {
        self.registered_lookup
            .get(&fd)
            .and_then(|&slot| self.registered.get(slot))
            .map_or(0, |entry| entry.flags)
    }

    #[inline]
    fn clear_registered(&mut self, fd: RawFd) -> bool {
        let Some(slot) = self.registered_lookup.remove(&fd) else {
            return false;
        };
        self.registered.swap_remove(slot);
        if let Some(moved) = self.registered.get(slot) {
            self.registered_lookup.insert(moved.fd, slot);
        }
        true
    }

    #[inline]
    fn readiness_event(fd: RawFd, flags: u8) -> Event {
        debug_assert!(fd >= 0);
        Event::new(
            fd as usize,
            (flags & INTEREST_READABLE) != 0,
            (flags & INTEREST_WRITABLE) != 0,
        )
    }

    /// Register (or re-arm) interest for `fd` with the poller using
    /// edge-triggered mode (`EV_CLEAR` on kqueue, `EPOLLET` on epoll).
    ///
    /// The poll event is keyed by fd while the registry stores the active read
    /// and write tokens separately. This lets one fd keep both readiness
    /// interests armed without replacing a read token with a write token.
    fn register_interest(&mut self, fd: RawFd, flags: u8, token: CompletionToken) {
        let previous = self.registered_lookup.get(&fd).copied();
        let mut next_flags = flags;
        let mut read_token = None;
        let mut write_token = None;

        if let Some(slot) = previous {
            if let Some(entry) = self.registered.get(slot) {
                next_flags |= entry.flags;
                read_token = entry.read_token;
                write_token = entry.write_token;
            }
        }
        if (flags & INTEREST_READABLE) != 0 {
            read_token = Some(token);
        }
        if (flags & INTEREST_WRITABLE) != 0 {
            write_token = Some(token);
        }

        let event = Self::readiness_event(fd, next_flags);
        // SAFETY: fd is a valid, open file descriptor owned by the reactor.
        let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
        if previous.is_some() {
            let _ = self
                .poller
                .modify_with_mode(borrowed, event, PollMode::Edge);
        } else {
            unsafe {
                self.poller
                    .add_with_mode(&borrowed, event, PollMode::Edge)
                    .unwrap_or_else(|_| {
                        let _ = self
                            .poller
                            .modify_with_mode(borrowed, event, PollMode::Edge);
                    });
            }
        }
        self.set_registered_interest(fd, next_flags, read_token, write_token);
    }

    #[inline]
    fn set_registered_interest(
        &mut self,
        fd: RawFd,
        flags: u8,
        read_token: Option<CompletionToken>,
        write_token: Option<CompletionToken>,
    ) {
        if let Some(&slot) = self.registered_lookup.get(&fd) {
            if let Some(entry) = self.registered.get_mut(slot) {
                entry.flags = flags;
                entry.read_token = read_token;
                entry.write_token = write_token;
            }
            return;
        }
        let slot = self.registered.len();
        self.registered.push(RegisteredInterest {
            fd,
            flags,
            read_token,
            write_token,
        });
        self.registered_lookup.insert(fd, slot);
    }

    #[inline]
    fn mark_pending_conn_op(&mut self, token: CompletionToken) {
        let Some((conn_id, op)) = decode_conn_token(token) else {
            return;
        };
        self.ensure_conn_op_capacity(conn_id);
        let handles = &mut self.conn_ops[conn_id];
        match op {
            OpType::Read => handles.pending_read = Some(token),
            OpType::Write => handles.pending_write = Some(token),
            OpType::Writev => handles.pending_writev = Some(token),
            OpType::Close => {}
        }
    }

    #[inline]
    fn clear_pending_conn_op(&mut self, token: CompletionToken) {
        let Some((conn_id, op)) = decode_conn_token(token) else {
            if token == CompletionToken::accept() && self.pending_accept == Some(token) {
                self.pending_accept = None;
            }
            return;
        };
        if conn_id >= self.conn_ops.len() {
            return;
        }
        let handles = &mut self.conn_ops[conn_id];
        match op {
            OpType::Read if handles.pending_read == Some(token) => handles.pending_read = None,
            OpType::Write if handles.pending_write == Some(token) => handles.pending_write = None,
            OpType::Writev if handles.pending_writev == Some(token) => {
                handles.pending_writev = None
            }
            OpType::Read | OpType::Write | OpType::Writev | OpType::Close => {}
        }
    }

    #[inline]
    fn mark_armed_read(&mut self, token: CompletionToken) {
        let Some((conn_id, OpType::Read)) = decode_conn_token(token) else {
            return;
        };
        self.ensure_conn_op_capacity(conn_id);
        self.conn_ops[conn_id].armed_read = Some(token);
    }

    #[inline]
    fn clear_armed_read(&mut self, conn_id: usize, token: CompletionToken) {
        if conn_id < self.conn_ops.len() && self.conn_ops[conn_id].armed_read == Some(token) {
            self.conn_ops[conn_id].armed_read = None;
        }
    }

    #[inline]
    fn mark_armed_write(&mut self, token: CompletionToken) {
        let Some((conn_id, op)) = decode_conn_token(token) else {
            return;
        };
        if !matches!(op, OpType::Write | OpType::Writev) {
            return;
        }
        self.ensure_conn_op_capacity(conn_id);
        self.conn_ops[conn_id].armed_write = Some(token);
    }

    #[inline]
    fn clear_armed_write(&mut self, conn_id: usize, token: CompletionToken) {
        if conn_id < self.conn_ops.len() && self.conn_ops[conn_id].armed_write == Some(token) {
            self.conn_ops[conn_id].armed_write = None;
        }
    }

    /// Process all currently-pending ops, appending completions to `out`.
    fn drain_pending(&mut self, out: &mut Vec<Completion>) {
        let n_pending = self.pending.len().min(PENDING_OP_DRAIN_BUDGET);
        for _ in 0..n_pending {
            if let Some(op) = self.pending.pop_front() {
                let token = pending_op_token(&op);
                if self.canceled_pending.remove(&token) {
                    continue;
                }
                self.clear_pending_conn_op(token);
                match op {
                    PendingOp::Accept { listener_fd, token } => {
                        self.do_accept(listener_fd, token, out);
                    }
                    PendingOp::Read { lease, token } => {
                        self.do_read(lease, token, out);
                    }
                    PendingOp::Write { lease, token } => {
                        self.do_write(lease, token, out);
                    }
                    PendingOp::Writev { batch, token } => {
                        self.do_writev(batch, token, out);
                    }
                    PendingOp::Close { fd, token } => {
                        self.do_close(fd, token, out);
                    }
                }
            }
        }
    }

    fn rearm_ready_ops(&mut self) {
        self.ready_accept_tokens.clear();
        self.ready_read_tokens.clear();
        self.ready_write_tokens.clear();
        for event in self.events.iter() {
            let fd = event.key as RawFd;
            let Some(entry) = self
                .registered_lookup
                .get(&fd)
                .and_then(|&slot| self.registered.get(slot))
            else {
                continue;
            };

            if event.readable {
                if let Some(token) = entry.read_token {
                    match token.decode() {
                        Ok(DecodedCompletionToken::Accept) => {
                            self.ready_accept_tokens.push(token);
                        }
                        Ok(DecodedCompletionToken::Conn {
                            id,
                            op: OpType::Read,
                            ..
                        }) if id < self.armed_reads.len() => {
                            self.ready_read_tokens.push(token);
                        }
                        Ok(DecodedCompletionToken::Cancel { .. })
                        | Ok(DecodedCompletionToken::Conn { .. })
                        | Err(_) => {}
                    }
                }
            }

            if event.writable {
                if let Some(token) = entry.write_token {
                    match token.decode() {
                        Ok(DecodedCompletionToken::Conn { id, op, .. })
                            if matches!(op, OpType::Write | OpType::Writev)
                                && id < self.armed_writes.len() =>
                        {
                            self.ready_write_tokens.push(token);
                        }
                        Ok(DecodedCompletionToken::Accept)
                        | Ok(DecodedCompletionToken::Cancel { .. })
                        | Ok(DecodedCompletionToken::Conn { .. })
                        | Err(_) => {}
                    }
                }
            }
        }

        let mut ready_accept_tokens = std::mem::take(&mut self.ready_accept_tokens);
        for token in ready_accept_tokens.drain(..) {
            let Some((listener_fd, accept_token)) = self.armed_accept.take() else {
                continue;
            };
            if accept_token == token {
                self.pending_accept = Some(accept_token);
                self.pending.push_back(PendingOp::Accept {
                    listener_fd,
                    token: accept_token,
                });
            } else {
                self.armed_accept = Some((listener_fd, accept_token));
            }
        }
        self.ready_accept_tokens = ready_accept_tokens;

        let mut ready_read_tokens = std::mem::take(&mut self.ready_read_tokens);
        for token in ready_read_tokens.drain(..) {
            let Some((conn_id, _)) = decode_conn_token(token) else {
                continue;
            };
            let Some(read) = self.armed_reads.get_mut(conn_id).and_then(Option::take) else {
                continue;
            };
            if read.token != token {
                self.armed_reads[conn_id] = Some(read);
                continue;
            }
            self.clear_armed_read(conn_id, read.token);
            self.mark_pending_conn_op(read.token);
            self.pending.push_back(PendingOp::Read {
                lease: read.lease,
                token: read.token,
            });
        }
        self.ready_read_tokens = ready_read_tokens;

        let mut ready_write_tokens = std::mem::take(&mut self.ready_write_tokens);
        for token in ready_write_tokens.drain(..) {
            let Some((conn_id, _)) = decode_conn_token(token) else {
                continue;
            };
            let Some(write) = self.armed_writes.get_mut(conn_id).and_then(Option::take) else {
                continue;
            };
            match write {
                ArmedWrite::Write {
                    lease,
                    token: write_token,
                } if write_token == token => {
                    self.clear_armed_write(conn_id, write_token);
                    self.mark_pending_conn_op(write_token);
                    self.pending.push_back(PendingOp::Write {
                        lease,
                        token: write_token,
                    });
                }
                ArmedWrite::Writev {
                    batch,
                    token: write_token,
                } if write_token == token => {
                    self.clear_armed_write(conn_id, write_token);
                    self.mark_pending_conn_op(write_token);
                    self.pending.push_back(PendingOp::Writev {
                        batch,
                        token: write_token,
                    });
                }
                stale => {
                    self.armed_writes[conn_id] = Some(stale);
                }
            }
        }
        self.ready_write_tokens = ready_write_tokens;
    }

    #[inline]
    fn push_canceled_completion(&mut self, token: CompletionToken) {
        self.ready.push_back(Completion {
            token,
            result: -libc::ECANCELED,
            flags: 0,
        });
    }

    fn drain_ready(&mut self, out: &mut Vec<Completion>) -> usize {
        let start = out.len();
        for _ in 0..READY_DRAIN_BUDGET {
            let Some(completion) = self.ready.pop_front() else {
                break;
            };
            out.push(completion);
        }
        out.len() - start
    }

    fn cancel_pending_token(&mut self, token: CompletionToken) -> bool {
        let mut canceled = false;

        if token == CompletionToken::accept() {
            if self.pending_accept == Some(token) {
                self.pending_accept = None;
                self.canceled_pending.insert(token);
                canceled = true;
                self.push_canceled_completion(token);
            }
            if self
                .armed_accept
                .as_ref()
                .is_some_and(|(_, accept_token)| *accept_token == token)
            {
                self.armed_accept = None;
                canceled = true;
                self.push_canceled_completion(token);
            }
            return canceled;
        }

        let Some((conn_id, op)) = decode_conn_token(token) else {
            return canceled;
        };
        if conn_id >= self.conn_ops.len() {
            return canceled;
        }

        let handles = &mut self.conn_ops[conn_id];
        match op {
            OpType::Read if handles.pending_read == Some(token) => {
                handles.pending_read = None;
                self.canceled_pending.insert(token);
                canceled = true;
                self.push_canceled_completion(token);
            }
            OpType::Write if handles.pending_write == Some(token) => {
                handles.pending_write = None;
                self.canceled_pending.insert(token);
                canceled = true;
                self.push_canceled_completion(token);
            }
            OpType::Writev if handles.pending_writev == Some(token) => {
                handles.pending_writev = None;
                self.canceled_pending.insert(token);
                canceled = true;
                self.push_canceled_completion(token);
            }
            OpType::Read | OpType::Write | OpType::Writev | OpType::Close => {}
        }

        if matches!(op, OpType::Read)
            && self.conn_ops[conn_id].armed_read == Some(token)
            && conn_id < self.armed_reads.len()
            && self.armed_reads[conn_id]
                .as_ref()
                .is_some_and(|read| read.token == token)
        {
            self.armed_reads[conn_id] = None;
            self.conn_ops[conn_id].armed_read = None;
            canceled = true;
            self.push_canceled_completion(token);
        }

        if matches!(op, OpType::Write | OpType::Writev)
            && self.conn_ops[conn_id].armed_write == Some(token)
            && conn_id < self.armed_writes.len()
            && self.armed_writes[conn_id]
                .as_ref()
                .is_some_and(|write| armed_write_token(write) == token)
        {
            self.armed_writes[conn_id] = None;
            self.conn_ops[conn_id].armed_write = None;
            canceled = true;
            self.push_canceled_completion(token);
        }

        canceled
    }

    fn cancel_conn_ops(&mut self, conn_id: usize) {
        if conn_id >= self.conn_ops.len() {
            return;
        }

        let handles = std::mem::take(&mut self.conn_ops[conn_id]);
        for token in [
            handles.pending_read,
            handles.pending_write,
            handles.pending_writev,
        ]
        .into_iter()
        .flatten()
        {
            self.canceled_pending.insert(token);
            self.push_canceled_completion(token);
        }

        if let Some(token) = handles.armed_read {
            if conn_id < self.armed_reads.len()
                && self.armed_reads[conn_id]
                    .as_ref()
                    .is_some_and(|read| read.token == token)
            {
                self.armed_reads[conn_id] = None;
                self.push_canceled_completion(token);
            }
        }

        if let Some(token) = handles.armed_write {
            if conn_id < self.armed_writes.len()
                && self.armed_writes[conn_id]
                    .as_ref()
                    .is_some_and(|write| armed_write_token(write) == token)
            {
                self.armed_writes[conn_id] = None;
                self.push_canceled_completion(token);
            }
        }
    }
}

impl BackendDriver for PollingBackend {
    fn submit_accept(
        &mut self,
        listener_fd: ListenerFd,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        self.pending_accept = Some(token);
        self.pending
            .push_back(PendingOp::Accept { listener_fd, token });
        Ok(())
    }

    fn submit_read(&mut self, lease: ReadLease, token: CompletionToken) -> Result<(), SubmitError> {
        self.mark_pending_conn_op(token);
        self.pending.push_back(PendingOp::Read { lease, token });
        Ok(())
    }

    fn submit_write(
        &mut self,
        lease: WriteLease,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        self.mark_pending_conn_op(token);
        self.pending.push_back(PendingOp::Write { lease, token });
        Ok(())
    }

    fn submit_close(&mut self, fd: ConnFd, token: CompletionToken) -> Result<(), SubmitError> {
        self.pending.push_back(PendingOp::Close { fd, token });
        Ok(())
    }

    fn submit_writev(
        &mut self,
        batch: IovecBatch,
        token: CompletionToken,
    ) -> Result<(), SubmitError> {
        self.mark_pending_conn_op(token);
        self.pending.push_back(PendingOp::Writev { batch, token });
        Ok(())
    }

    fn submit_cancel(
        &mut self,
        target: CompletionToken,
        cancel: CompletionToken,
    ) -> Result<(), SubmitError> {
        let canceled = self.cancel_pending_token(target);
        self.ready.push_back(Completion {
            token: cancel,
            result: if canceled { 0 } else { -libc::ENOENT },
            flags: 0,
        });
        Ok(())
    }

    fn flush(&mut self) -> Result<usize, SubmitError> {
        // The polling backend doesn't batch — ops are processed in completions().
        Ok(0)
    }

    fn completions(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        let start = out.len();

        if self.drain_ready(out) > 0 {
            return Ok(out.len() - start);
        }

        // First pass: attempt all pending operations.
        self.drain_pending(out);
        self.drain_ready(out);

        // Hot path: if we produced completions, return immediately.
        if out.len() > start {
            return Ok(out.len() - start);
        }
        if !self.pending.is_empty() {
            return Ok(0);
        }

        // No completions — poll for readiness with a short timeout so the
        // reactor can tick timers and check shutdown.
        self.events.clear();
        self.poller
            .wait(&mut self.events, Some(std::time::Duration::from_millis(1)))?;

        // Immediately retry pending ops now that readiness has been signalled.
        // This avoids deferring to the next reactor iteration.
        if !self.events.is_empty() {
            self.rearm_ready_ops();
            self.drain_pending(out);
            self.drain_ready(out);
        }

        Ok(out.len() - start)
    }

    fn drain_cq(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        let start = out.len();

        self.drain_ready(out);
        if out.len() == start {
            // Non-blocking progress pass for operations submitted while the
            // reactor was processing completions. This makes polling mode
            // match the reactor fast-path contract without entering poll().
            self.drain_pending(out);
            self.drain_ready(out);
        }

        Ok(out.len() - start)
    }
}

impl PollingBackend {
    fn do_accept(
        &mut self,
        listener_fd: ListenerFd,
        token: CompletionToken,
        out: &mut Vec<Completion>,
    ) {
        let listener = listener_fd;
        let listener_fd = listener.raw();

        match accept_nonblocking_cloexec(listener_fd) {
            Ok(result) => {
                out.push(Completion {
                    token,
                    result,
                    flags: 0,
                });
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                // Not ready — register listener for read-readiness and re-queue.
                // Edge-triggered: skip if already registered.
                if (self.registered_flags(listener_fd) & INTEREST_READABLE) == 0 {
                    self.register_interest(listener_fd, INTEREST_READABLE, token);
                }
                self.armed_accept = Some((listener, token));
            }
            Err(err) => {
                let errno = err.raw_os_error().unwrap_or(1);
                out.push(Completion {
                    token,
                    result: -errno,
                    flags: 0,
                });
            }
        }
    }

    fn do_read(&mut self, lease: ReadLease, token: CompletionToken, out: &mut Vec<Completion>) {
        let fd = lease.fd().raw();
        let buf_ptr = lease.ptr();
        let buf_len = lease.len();
        // Read-loop: drain available data without forcing a final EAGAIN on
        // short reads. A short nonblocking read means the socket receive
        // buffer was drained for this pass; the reactor will submit the next
        // read immediately if more command data is needed.
        let mut total: usize = 0;
        let mut ptr = buf_ptr;
        let mut remaining = buf_len;

        loop {
            // SAFETY: ptr is a valid buffer owned by the reactor (same thread),
            // fd is a valid socket fd.
            let n = unsafe { libc::read(fd, ptr.cast::<libc::c_void>(), remaining) };

            if n > 0 {
                let bytes = n as usize;
                let short_read = bytes < remaining;
                total += bytes;
                // SAFETY: advancing within the same buffer allocation.
                ptr = unsafe { ptr.add(bytes) };
                remaining -= bytes;
                if short_read || remaining == 0 {
                    break;
                }
                continue; // More buffer space — try to read more.
            } else if n == 0 {
                // EOF — report any accumulated data first; the next read
                // will observe the EOF again.
                if total > 0 {
                    break;
                }
                out.push(Completion {
                    token,
                    result: 0,
                    flags: 0,
                });
                return;
            } else {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    break; // Kernel buffer drained.
                }
                // Real error — report accumulated data first if any.
                if total > 0 {
                    break;
                }
                let errno = err.raw_os_error().unwrap_or(1);
                out.push(Completion {
                    token,
                    result: -errno,
                    flags: 0,
                });
                return;
            }
        }

        if total > 0 {
            out.push(Completion {
                token,
                result: total as i32,
                flags: 0,
            });
        } else {
            // EAGAIN with no data — register for readiness and re-queue.
            // Edge-triggered: only register if not already registered for
            // readable. With PollMode::Edge (EV_CLEAR), the interest
            // persists across events, so re-registration is unnecessary
            // and would waste a kevent/epoll_ctl syscall.
            if (self.registered_flags(fd) & INTEREST_READABLE) == 0 {
                self.register_interest(fd, INTEREST_READABLE, token);
            }
            if let Some((conn_id, op)) = decode_conn_token(token) {
                debug_assert!(matches!(op, OpType::Read));
                self.ensure_read_capacity(conn_id);
                self.armed_reads[conn_id] = Some(ArmedRead { lease, token });
                self.mark_armed_read(token);
            }
        }
    }

    fn do_write(&mut self, lease: WriteLease, token: CompletionToken, out: &mut Vec<Completion>) {
        let fd = lease.fd().raw();
        let buf_ptr = lease.ptr();
        let buf_len = lease.len();
        // SAFETY: buf_ptr is a valid buffer owned by the reactor (same thread),
        // fd is a valid socket fd.
        let n = unsafe { libc::write(fd, buf_ptr.cast::<libc::c_void>(), buf_len) };

        if n >= 0 {
            out.push(Completion {
                token,
                result: n as i32,
                flags: 0,
            });
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                // Register for write-readiness instead of busy-polling while
                // preserving any already-armed read readiness on this fd.
                self.register_interest(fd, INTEREST_WRITABLE, token);
                if let Some((conn_id, _)) = decode_conn_token(token) {
                    self.ensure_write_capacity(conn_id);
                    self.armed_writes[conn_id] = Some(ArmedWrite::Write { lease, token });
                    self.mark_armed_write(token);
                }
                return;
            }
            let errno = err.raw_os_error().unwrap_or(1);
            out.push(Completion {
                token,
                result: -errno,
                flags: 0,
            });
        }
    }

    fn do_writev(&mut self, batch: IovecBatch, token: CompletionToken, out: &mut Vec<Completion>) {
        let fd = batch.fd().raw();
        let iovecs = batch.ptr();
        let iov_count = batch.count();
        // SAFETY: iovecs points to a valid array of iov_count iovec structs
        // on the reactor thread. fd is a valid socket fd.
        let n = unsafe { libc::writev(fd, iovecs, iov_count as libc::c_int) };

        if n >= 0 {
            out.push(Completion {
                token,
                result: n as i32,
                flags: 0,
            });
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                // Register for write-readiness instead of busy-polling while
                // preserving any already-armed read readiness on this fd.
                self.register_interest(fd, INTEREST_WRITABLE, token);
                if let Some((conn_id, _)) = decode_conn_token(token) {
                    self.ensure_write_capacity(conn_id);
                    self.armed_writes[conn_id] = Some(ArmedWrite::Writev { batch, token });
                    self.mark_armed_write(token);
                }
                return;
            }
            let errno = err.raw_os_error().unwrap_or(1);
            out.push(Completion {
                token,
                result: -errno,
                flags: 0,
            });
        }
    }

    fn do_close(&mut self, fd: ConnFd, token: CompletionToken, out: &mut Vec<Completion>) {
        let fd = fd.raw();
        if let Some((conn_id, _)) = decode_conn_token(token) {
            self.cancel_conn_ops(conn_id);
        }

        // Remove from poller if registered.
        if self.clear_registered(fd) {
            // SAFETY: fd was previously registered with the poller.
            let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
            let _ = self.poller.delete(borrowed);
        }

        // SAFETY: fd is a valid socket fd owned by the reactor.
        let result = unsafe { libc::close(fd) };

        out.push(Completion {
            token,
            result,
            flags: 0,
        });
    }
}

#[inline]
fn accept_nonblocking_cloexec(listener_fd: RawFd) -> io::Result<RawFd> {
    let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut addr_len: libc::socklen_t =
        std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: listener_fd is a valid listener owned by the reactor. The address
    // storage is valid for the duration of the syscall.
    let fd = unsafe {
        accept4_or_accept(
            listener_fd,
            &mut addr as *mut libc::sockaddr_storage as *mut libc::sockaddr,
            &mut addr_len,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    if let Err(error) = set_nonblocking_cloexec(fd) {
        // SAFETY: fd was just returned by accept() and has not been handed to
        // the reactor, so this helper still owns it on setup failure.
        unsafe {
            libc::close(fd);
        }
        return Err(error);
    }

    Ok(fd)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[inline]
unsafe fn accept4_or_accept(
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
unsafe fn accept4_or_accept(
    listener_fd: RawFd,
    addr: *mut libc::sockaddr,
    addr_len: *mut libc::socklen_t,
) -> RawFd {
    // SAFETY: caller provides a valid listener fd and sockaddr storage.
    unsafe { libc::accept(listener_fd, addr, addr_len) }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn set_nonblocking_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: fd is a valid descriptor just returned by accept().
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fd is valid and F_SETFL updates descriptor status flags.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: fd is valid and F_GETFD reads close-on-exec flags.
    let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if fd_flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fd is valid and F_SETFD updates close-on-exec flags.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

#[inline]
fn pending_op_token(op: &PendingOp) -> CompletionToken {
    match op {
        PendingOp::Accept { token, .. }
        | PendingOp::Read { token, .. }
        | PendingOp::Write { token, .. }
        | PendingOp::Writev { token, .. }
        | PendingOp::Close { token, .. } => *token,
    }
}

#[inline]
fn armed_write_token(write: &ArmedWrite) -> CompletionToken {
    match write {
        ArmedWrite::Write { token, .. } | ArmedWrite::Writev { token, .. } => *token,
    }
}

#[inline]
fn decode_conn_token(token: CompletionToken) -> Option<(usize, OpType)> {
    match token.decode().ok()? {
        DecodedCompletionToken::Conn { id, op, .. } => Some((id, op)),
        DecodedCompletionToken::Accept | DecodedCompletionToken::Cancel { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::encode_token;

    fn nonblocking_pipe() -> (RawFd, RawFd) {
        let mut fds = [0; 2];
        // SAFETY: `pipe` initializes both fds on success.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(rc, 0);
        for fd in fds {
            // SAFETY: fd is open and owned by this test.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            assert!(flags >= 0);
            // SAFETY: fd is open and owned by this test.
            let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
            assert_eq!(rc, 0);
        }
        (fds[0], fds[1])
    }

    fn nonblocking_socket_pair() -> (RawFd, RawFd) {
        let mut fds = [0; 2];
        // SAFETY: `socketpair` initializes both fds on success.
        let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(rc, 0);
        for fd in fds {
            // SAFETY: fd is open and owned by this test.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            assert!(flags >= 0);
            // SAFETY: fd is open and owned by this test.
            let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
            assert_eq!(rc, 0);
        }
        (fds[0], fds[1])
    }

    fn close_fd(fd: RawFd) {
        // SAFETY: tests call this only for fds they still own.
        unsafe {
            libc::close(fd);
        }
    }

    fn duplicate_fd_at_least(fd: RawFd, min_fd: RawFd) -> io::Result<RawFd> {
        // SAFETY: fd is open and owned by the test; F_DUPFD_CLOEXEC returns a
        // new descriptor on success.
        let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, min_fd) };
        if duplicated < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(duplicated)
        }
    }

    fn duplicate_fd_at_candidates(fd: RawFd, candidates: &[RawFd]) -> Option<RawFd> {
        candidates
            .iter()
            .find_map(|&min_fd| duplicate_fd_at_least(fd, min_fd).ok())
    }

    fn fill_pipe_until_would_block(write_fd: RawFd) {
        let bytes = [0u8; 4096];
        loop {
            // SAFETY: write_fd is an open nonblocking pipe write end and
            // `bytes` is valid for the call.
            let n = unsafe {
                libc::write(write_fd, bytes.as_ptr().cast::<libc::c_void>(), bytes.len())
            };
            if n > 0 {
                continue;
            }
            assert_eq!(n, -1);
            let error = io::Error::last_os_error();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            break;
        }
    }

    #[test]
    fn cancel_pending_writev_yields_ecanceled_completion() {
        let mut backend = PollingBackend::new().unwrap();
        let token = encode_token(3, 9, OpType::Writev).unwrap();
        let cancel = CompletionToken::cancel(token).unwrap();
        let bytes = b"hello";
        let iovecs = [libc::iovec {
            iov_base: bytes.as_ptr() as *mut libc::c_void,
            iov_len: bytes.len(),
        }];

        // SAFETY: `iovecs` and `bytes` live until the test drains the synthetic
        // completion below.
        let batch =
            unsafe { IovecBatch::new(ConnFd::new(99), iovecs.as_ptr(), iovecs.len()) }.unwrap();
        backend.submit_writev(batch, token).unwrap();
        backend.submit_cancel(token, cancel).unwrap();

        let mut out = Vec::new();
        assert_eq!(backend.drain_cq(&mut out).unwrap(), 2);
        assert_eq!(out[0].token, token);
        assert_eq!(out[0].result, -libc::ECANCELED);
        assert_eq!(out[1].token, cancel);
        assert_eq!(out[1].result, 0);
        let mut skipped = Vec::new();
        assert_eq!(backend.drain_cq(&mut skipped).unwrap(), 0);
        assert!(backend.pending.is_empty());
    }

    #[test]
    fn cancel_armed_write_yields_ecanceled_completion() {
        let mut backend = PollingBackend::new().unwrap();
        let token = encode_token(3, 9, OpType::Write).unwrap();
        let cancel = CompletionToken::cancel(token).unwrap();
        let bytes = b"hello";

        backend.ensure_write_capacity(3);
        // SAFETY: `bytes` lives until the armed operation is canceled below.
        let lease =
            unsafe { WriteLease::new(ConnFd::new(99), bytes.as_ptr(), bytes.len(), None) }.unwrap();
        backend.armed_writes[3] = Some(ArmedWrite::Write { lease, token });
        backend.mark_armed_write(token);

        let mut out = Vec::new();
        assert_eq!(backend.drain_cq(&mut out).unwrap(), 0);
        assert!(backend.armed_writes[3].is_some());

        backend.submit_cancel(token, cancel).unwrap();
        assert_eq!(backend.drain_cq(&mut out).unwrap(), 2);
        assert_eq!(out[0].token, token);
        assert_eq!(out[0].result, -libc::ECANCELED);
        assert_eq!(out[1].token, cancel);
        assert_eq!(out[1].result, 0);
        assert!(backend.armed_writes[3].is_none());
    }

    #[test]
    fn cancel_missing_target_yields_cancel_enoent_only() {
        let mut backend = PollingBackend::new().unwrap();
        let token = encode_token(3, 9, OpType::Read).unwrap();
        let cancel = CompletionToken::cancel(token).unwrap();

        backend.submit_cancel(token, cancel).unwrap();

        let mut out = Vec::new();
        assert_eq!(backend.drain_cq(&mut out).unwrap(), 1);
        assert_eq!(out[0].token, cancel);
        assert_eq!(out[0].result, -libc::ENOENT);
    }

    #[test]
    fn original_completed_before_cancel_keeps_original_and_reports_enoent() {
        let mut backend = PollingBackend::new().unwrap();
        let token = encode_token(3, 9, OpType::Read).unwrap();
        let cancel = CompletionToken::cancel(token).unwrap();
        backend.ready.push_back(Completion {
            token,
            result: 7,
            flags: 0,
        });

        backend.submit_cancel(token, cancel).unwrap();

        let mut out = Vec::new();
        assert_eq!(backend.drain_cq(&mut out).unwrap(), 2);
        assert_eq!(out[0].token, token);
        assert_eq!(out[0].result, 7);
        assert_eq!(out[1].token, cancel);
        assert_eq!(out[1].result, -libc::ENOENT);
    }

    #[test]
    fn accept_eagain_arms_listener_without_busy_pending_retry() {
        use std::os::fd::IntoRawFd;

        let mut backend = PollingBackend::new().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let fd = listener.into_raw_fd();
        let token = CompletionToken::accept();

        let mut out = Vec::new();
        backend.do_accept(ListenerFd::new(fd), token, &mut out);

        assert!(out.is_empty());
        assert!(backend.pending.is_empty());
        assert_eq!(backend.armed_accept, Some((ListenerFd::new(fd), token)));
        assert_eq!(
            backend.registered_flags(fd) & INTEREST_READABLE,
            INTEREST_READABLE
        );

        if backend.clear_registered(fd) {
            // SAFETY: fd was registered by this test's backend.
            let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
            let _ = backend.poller.delete(borrowed);
        }
        // SAFETY: fd was obtained from into_raw_fd above and is still open.
        unsafe {
            libc::close(fd);
        }
    }

    #[test]
    fn accept_uses_nonblocking_cloexec_fd() {
        use std::os::fd::IntoRawFd;

        let mut backend = PollingBackend::new().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let listener_fd = listener.into_raw_fd();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let token = CompletionToken::accept();

        let mut out = Vec::new();
        backend.do_accept(ListenerFd::new(listener_fd), token, &mut out);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].token, token);
        assert!(out[0].result >= 0);
        let accepted_fd = out[0].result;

        // SAFETY: accepted_fd is live until this test closes it.
        let status_flags = unsafe { libc::fcntl(accepted_fd, libc::F_GETFL) };
        assert!(status_flags >= 0);
        assert_ne!(status_flags & libc::O_NONBLOCK, 0);
        // SAFETY: accepted_fd is live until this test closes it.
        let fd_flags = unsafe { libc::fcntl(accepted_fd, libc::F_GETFD) };
        assert!(fd_flags >= 0);
        assert_ne!(fd_flags & libc::FD_CLOEXEC, 0);

        close_fd(accepted_fd);
        close_fd(listener_fd);
        drop(client);
    }

    #[test]
    fn writev_eagain_arms_write_readiness_without_completion() {
        let mut backend = PollingBackend::new().unwrap();
        let (read_fd, write_fd) = nonblocking_pipe();
        fill_pipe_until_would_block(write_fd);

        let token = encode_token(4, 9, OpType::Writev).unwrap();
        let bytes = b"hello";
        let iovecs = [libc::iovec {
            iov_base: bytes.as_ptr() as *mut libc::c_void,
            iov_len: bytes.len(),
        }];
        // SAFETY: `iovecs` and `bytes` live until the armed writev is canceled
        // below by closing the fd through the backend.
        let batch =
            unsafe { IovecBatch::new(ConnFd::new(write_fd), iovecs.as_ptr(), iovecs.len()) }
                .unwrap();

        let mut out = Vec::new();
        backend.do_writev(batch, token, &mut out);

        assert!(out.is_empty());
        assert!(backend.pending.is_empty());
        assert!(matches!(
            backend.armed_writes.get(4).and_then(Option::as_ref),
            Some(ArmedWrite::Writev { token: armed, .. }) if *armed == token
        ));
        assert_eq!(
            backend.registered_flags(write_fd) & INTEREST_WRITABLE,
            INTEREST_WRITABLE
        );

        let close_token = encode_token(4, 9, OpType::Close).unwrap();
        backend.do_close(ConnFd::new(write_fd), close_token, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].token, close_token);

        let mut canceled = Vec::new();
        assert_eq!(backend.drain_cq(&mut canceled).unwrap(), 1);
        assert_eq!(canceled[0].token, token);
        assert_eq!(canceled[0].result, -libc::ECANCELED);

        close_fd(read_fd);
    }

    #[test]
    fn write_interest_preserves_existing_read_interest() {
        let mut backend = PollingBackend::new().unwrap();
        let (fd, peer_fd) = nonblocking_socket_pair();
        let read_token = encode_token(4, 9, OpType::Read).unwrap();
        let write_token = encode_token(4, 9, OpType::Writev).unwrap();

        backend.register_interest(fd, INTEREST_READABLE, read_token);
        backend.register_interest(fd, INTEREST_WRITABLE, write_token);

        let entry = backend
            .registered_lookup
            .get(&fd)
            .and_then(|&slot| backend.registered.get(slot))
            .expect("registered interest");
        assert_eq!(
            entry.flags & (INTEREST_READABLE | INTEREST_WRITABLE),
            INTEREST_READABLE | INTEREST_WRITABLE
        );
        assert_eq!(entry.read_token, Some(read_token));
        assert_eq!(entry.write_token, Some(write_token));

        if backend.clear_registered(fd) {
            // SAFETY: fd was registered by this backend above.
            let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
            let _ = backend.poller.delete(borrowed);
        }
        close_fd(fd);
        close_fd(peer_fd);
    }

    #[test]
    fn writev_partial_pipe_write_reports_short_completion() {
        let mut backend = PollingBackend::new().unwrap();
        let (read_fd, write_fd) = nonblocking_pipe();
        let token = encode_token(5, 9, OpType::Writev).unwrap();
        let bytes = vec![b'x'; 1024 * 1024];
        let iovecs = [libc::iovec {
            iov_base: bytes.as_ptr() as *mut libc::c_void,
            iov_len: bytes.len(),
        }];
        // SAFETY: `iovecs` and `bytes` live until `do_writev` returns the
        // immediate synthetic completion below.
        let batch =
            unsafe { IovecBatch::new(ConnFd::new(write_fd), iovecs.as_ptr(), iovecs.len()) }
                .unwrap();

        let mut out = Vec::new();
        backend.do_writev(batch, token, &mut out);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].token, token);
        assert!(out[0].result > 0);
        assert!((out[0].result as usize) < bytes.len());

        close_fd(read_fd);
        close_fd(write_fd);
    }

    #[test]
    fn close_purges_pending_fd_ops_and_armed_reads() {
        let mut backend = PollingBackend::new().unwrap();
        let mut fds = [0; 2];
        // SAFETY: `pipe` initialises both fds on success.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(rc, 0);
        let fd = fds[0];
        let peer_fd = fds[1];

        let read_token = encode_token(1, 5, OpType::Read).unwrap();
        let write_token = encode_token(1, 5, OpType::Write).unwrap();
        let writev_token = encode_token(1, 5, OpType::Writev).unwrap();
        let close_token = encode_token(1, 5, OpType::Close).unwrap();

        let write_bytes = [0u8; 8];
        let writev_bytes = [1u8; 8];
        let iovecs = [libc::iovec {
            iov_base: writev_bytes.as_ptr() as *mut libc::c_void,
            iov_len: writev_bytes.len(),
        }];
        // SAFETY: `write_bytes` is live until close purges the pending op below.
        let write_lease = unsafe {
            WriteLease::new(
                ConnFd::new(fd),
                write_bytes.as_ptr(),
                write_bytes.len(),
                None,
            )
        }
        .unwrap();
        // SAFETY: `iovecs` and `writev_bytes` are live until close purges the
        // pending op below.
        let writev_batch =
            unsafe { IovecBatch::new(ConnFd::new(fd), iovecs.as_ptr(), iovecs.len()) }.unwrap();
        let mut read_bytes = [0u8; 8];
        // SAFETY: `read_bytes` is live until close purges the armed read below.
        let read_lease = unsafe {
            ReadLease::new(
                ConnFd::new(fd),
                read_bytes.as_mut_ptr(),
                read_bytes.len(),
                None,
            )
        }
        .unwrap();

        backend.submit_write(write_lease, write_token).unwrap();
        backend.submit_writev(writev_batch, writev_token).unwrap();
        backend.ensure_read_capacity(1);
        backend.armed_reads[1] = Some(ArmedRead {
            lease: read_lease,
            token: read_token,
        });
        backend.mark_armed_read(read_token);

        let mut close_out = Vec::new();
        backend.do_close(ConnFd::new(fd), close_token, &mut close_out);
        assert_eq!(close_out.len(), 1);
        assert_eq!(close_out[0].token, close_token);

        let mut canceled = Vec::new();
        assert_eq!(backend.drain_cq(&mut canceled).unwrap(), 3);
        let canceled_tokens: Vec<CompletionToken> = canceled.into_iter().map(|c| c.token).collect();
        assert_eq!(canceled_tokens, vec![write_token, writev_token, read_token]);
        let mut skipped = Vec::new();
        assert_eq!(backend.drain_cq(&mut skipped).unwrap(), 0);
        assert!(backend.pending.is_empty());
        assert!(backend.armed_reads[1].is_none());

        // SAFETY: `peer_fd` is the other end of the pipe opened by this test.
        unsafe {
            libc::close(peer_fd);
        }
    }

    #[test]
    fn high_fd_registration_is_dense_not_raw_fd_indexed() {
        let mut backend = PollingBackend::new().unwrap();
        let (read_fd, write_fd) = nonblocking_pipe();
        let Some(high_fd) = duplicate_fd_at_candidates(read_fd, &[4096, 1024, 512, 128, 64]) else {
            close_fd(read_fd);
            close_fd(write_fd);
            return;
        };
        assert!(high_fd >= 64);

        let token = encode_token(9, 1, OpType::Read).unwrap();
        let initial_capacity = backend.registered.capacity();
        backend.register_interest(high_fd, INTEREST_READABLE, token);

        assert_eq!(backend.registered.len(), 1);
        assert_eq!(backend.registered_lookup.len(), 1);
        assert_eq!(
            backend.registered_flags(high_fd) & INTEREST_READABLE,
            INTEREST_READABLE
        );
        assert!(
            backend.registered.capacity() <= initial_capacity.max(1),
            "dense registry capacity should not grow for one high fd"
        );

        if backend.clear_registered(high_fd) {
            // SAFETY: high_fd was registered by this backend above.
            let borrowed = unsafe { BorrowedFd::borrow_raw(high_fd) };
            let _ = backend.poller.delete(borrowed);
        }
        close_fd(high_fd);
        close_fd(read_fd);
        close_fd(write_fd);
    }

    #[test]
    fn ready_completion_drain_is_budgeted() {
        let mut backend = PollingBackend::new().unwrap();
        for _ in 0..READY_DRAIN_BUDGET + 7 {
            backend.ready.push_back(Completion {
                token: CompletionToken::accept(),
                result: 0,
                flags: 0,
            });
        }

        let mut out = Vec::new();
        assert_eq!(backend.drain_ready(&mut out), READY_DRAIN_BUDGET);
        assert_eq!(out.len(), READY_DRAIN_BUDGET);
        assert_eq!(backend.ready.len(), 7);
    }

    #[test]
    fn pending_operation_drain_is_budgeted() {
        let mut backend = PollingBackend::new().unwrap();
        for conn_id in 0..PENDING_OP_DRAIN_BUDGET + 7 {
            let token = encode_token(conn_id, 1, OpType::Close).unwrap();
            backend.pending.push_back(PendingOp::Close {
                fd: ConnFd::new(-1),
                token,
            });
        }

        let mut out = Vec::new();
        backend.drain_pending(&mut out);

        assert_eq!(out.len(), PENDING_OP_DRAIN_BUDGET);
        assert_eq!(backend.pending.len(), 7);
    }
}
