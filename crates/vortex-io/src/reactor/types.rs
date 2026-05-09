use super::*;

macro_rules! reactor_budget_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr(transparent)]
        pub struct $name(usize);

        impl $name {
            /// Creates a budget when `value` is non-zero.
            #[inline]
            pub const fn new(value: usize) -> Option<Self> {
                if value == 0 { None } else { Some(Self(value)) }
            }

            #[inline]
            pub(super) const fn new_unchecked(value: usize) -> Self {
                Self(value)
            }

            /// Returns the maximum work units allowed for one activation.
            #[inline]
            pub const fn get(self) -> usize {
                self.0
            }
        }
    };
}

reactor_budget_type!(CompletionBudget);
reactor_budget_type!(CommandBudget);
reactor_budget_type!(AcceptBudget);
reactor_budget_type!(WritevBudget);
reactor_budget_type!(MaintenanceBudget);

/// Optional monotonic time budget for a command activation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct TimeBudget {
    nanos: u64,
}

impl TimeBudget {
    /// Creates a time budget when `nanos` is non-zero.
    #[inline]
    pub const fn from_nanos(nanos: u64) -> Option<Self> {
        if nanos == 0 {
            None
        } else {
            Some(Self { nanos })
        }
    }

    /// Creates a time budget from microseconds when `micros` is non-zero.
    #[inline]
    pub const fn from_micros(micros: u64) -> Option<Self> {
        Self::from_nanos(micros.saturating_mul(1_000))
    }

    #[inline]
    pub const fn as_nanos(self) -> u64 {
        self.nanos
    }
}

/// Reactor work-class budgets applied to one loop activation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReactorBudgets {
    pub completion: CompletionBudget,
    pub command: CommandBudget,
    pub accept: AcceptBudget,
    pub writev: WritevBudget,
    pub maintenance: MaintenanceBudget,
    pub time: Option<TimeBudget>,
}

impl Default for ReactorBudgets {
    fn default() -> Self {
        Self {
            completion: DEFAULT_COMPLETION_BUDGET,
            command: DEFAULT_COMMAND_BUDGET,
            accept: DEFAULT_ACCEPT_BUDGET,
            writev: DEFAULT_WRITEV_BUDGET,
            maintenance: DEFAULT_MAINTENANCE_BUDGET,
            time: None,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct SliceBudget {
    pub(super) remaining: usize,
}

impl SliceBudget {
    #[inline]
    pub(super) fn new(units: usize) -> Self {
        Self { remaining: units }
    }

    #[inline]
    pub(super) fn consume_one(&mut self) -> bool {
        if self.remaining == 0 {
            return false;
        }
        self.remaining -= 1;
        true
    }

    #[inline]
    pub(super) fn is_empty(self) -> bool {
        self.remaining == 0
    }

    #[inline]
    pub(super) fn remaining(self) -> usize {
        self.remaining
    }
}

#[derive(Debug, Default)]
pub(super) struct ReactorLocalMetrics {
    pending: RuntimeLocalFlushMetrics,
}

impl ReactorLocalMetrics {
    #[inline]
    pub(super) fn record_loop_iteration(&mut self) {
        self.pending.loop_iterations = self.pending.loop_iterations.saturating_add(1);
    }

    #[inline]
    pub(super) fn record_accept_drain(&mut self, accepted: usize) {
        self.pending.accept_drain_runs = self.pending.accept_drain_runs.saturating_add(1);
        if accepted == 0 {
            return;
        }
        let accepted = accepted as u64;
        self.pending.accept_drain_accepted =
            self.pending.accept_drain_accepted.saturating_add(accepted);
        self.pending.accept_drain_accepted_max =
            self.pending.accept_drain_accepted_max.max(accepted);
    }

    #[inline]
    pub(super) fn record_completion_batch(&mut self, width: usize) {
        if width == 0 {
            return;
        }
        let width = width as u64;
        self.pending.completion_batch_count = self.pending.completion_batch_count.saturating_add(1);
        self.pending.completion_batch_total =
            self.pending.completion_batch_total.saturating_add(width);
        self.pending.completion_batch_max = self.pending.completion_batch_max.max(width);
    }

    #[inline]
    pub(super) fn record_command_batch(&mut self, width: usize) {
        if width == 0 {
            return;
        }
        let width = width as u64;
        self.pending.command_batch_count = self.pending.command_batch_count.saturating_add(1);
        self.pending.command_batch_total = self.pending.command_batch_total.saturating_add(width);
        self.pending.command_batch_max = self.pending.command_batch_max.max(width);
    }

    #[inline]
    pub(super) fn record_writev_chunk(&mut self, iovecs: usize) {
        self.pending.writev_chunks = self.pending.writev_chunks.saturating_add(1);
        if iovecs == 0 {
            return;
        }
        let iovecs = iovecs as u64;
        self.pending.writev_iovecs_total = self.pending.writev_iovecs_total.saturating_add(iovecs);
        self.pending.writev_iovecs_max = self.pending.writev_iovecs_max.max(iovecs);
    }

    #[inline]
    pub(super) fn record_queued_response_bytes(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let bytes = bytes as u64;
        self.pending.queued_response_bytes_total = self
            .pending
            .queued_response_bytes_total
            .saturating_add(bytes);
        self.pending.queued_response_bytes_max = self.pending.queued_response_bytes_max.max(bytes);
    }

    #[inline]
    pub(super) fn take(&mut self) -> RuntimeLocalFlushMetrics {
        std::mem::take(&mut self.pending)
    }
}

pub(super) struct CommandSliceBudget {
    pub(super) remaining: usize,
    pub(super) deadline_nanos: Option<u64>,
    pub(super) commands_until_time_check: usize,
}

impl CommandSliceBudget {
    #[inline]
    pub(super) fn new(
        command_budget: CommandBudget,
        time_budget: Option<TimeBudget>,
        now_nanos: u64,
    ) -> Self {
        Self {
            remaining: command_budget.get(),
            deadline_nanos: time_budget.map(|budget| now_nanos.saturating_add(budget.as_nanos())),
            commands_until_time_check: COMMAND_TIME_CHECK_GRANULARITY,
        }
    }

    #[inline]
    pub(super) fn remaining(&self) -> usize {
        self.remaining
    }

    #[inline]
    pub(super) fn consume_commands(&mut self, count: usize) {
        self.remaining = self.remaining.saturating_sub(count);
        self.commands_until_time_check = self.commands_until_time_check.saturating_sub(count);
    }

    #[inline]
    pub(super) fn should_yield_after_batch(&mut self) -> bool {
        if self.remaining == 0 {
            return true;
        }

        let Some(deadline) = self.deadline_nanos else {
            return false;
        };
        if self.commands_until_time_check != 0 {
            return false;
        }

        self.commands_until_time_check = COMMAND_TIME_CHECK_GRANULARITY;
        Timestamp::now().as_nanos() >= deadline
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MaintenanceClass {
    CloseDrain,
    Timer,
    ActiveExpiry,
    EvictionPressure,
    AofFsync,
    MetricsFlush,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct MaintenanceRun {
    pub(super) did_work: bool,
    pub(super) pending_more: bool,
    pub(super) elapsed_nanos: u64,
}

impl MaintenanceRun {
    #[inline]
    pub(super) const fn idle() -> Self {
        Self {
            did_work: false,
            pending_more: false,
            elapsed_nanos: 0,
        }
    }

    #[inline]
    pub(super) const fn ran(elapsed_nanos: u64, pending_more: bool) -> Self {
        Self {
            did_work: true,
            pending_more,
            elapsed_nanos,
        }
    }
}

#[derive(Debug)]
pub(super) struct MaintenanceScheduler {
    pub(super) cursor: usize,
}

impl MaintenanceScheduler {
    const CLASSES: [MaintenanceClass; 6] = [
        MaintenanceClass::CloseDrain,
        MaintenanceClass::Timer,
        MaintenanceClass::ActiveExpiry,
        MaintenanceClass::EvictionPressure,
        MaintenanceClass::AofFsync,
        MaintenanceClass::MetricsFlush,
    ];

    #[inline]
    pub(super) const fn new() -> Self {
        Self { cursor: 0 }
    }

    #[inline]
    pub(super) fn next_class(&mut self) -> MaintenanceClass {
        let class = Self::CLASSES[self.cursor];
        self.cursor += 1;
        if self.cursor == Self::CLASSES.len() {
            self.cursor = 0;
        }
        class
    }

    #[inline]
    pub(super) const fn class_count() -> usize {
        Self::CLASSES.len()
    }
}

/// Command response type — determines serialization strategy.
#[allow(dead_code)] // Frame variant is infrastructure for Phase 2.5 engine commands.
#[derive(Debug)]
pub(super) enum CommandResponse {
    /// Pre-computed static response (PONG, OK, ERR).
    /// Emitted as a zero-copy `writev` segment.
    Static(&'static [u8]),
    /// Tiny dynamic response already serialized into inline RESP bytes.
    Inline(vortex_engine::commands::InlineResp),
    /// Dynamic RESP frame requiring serialization.
    /// Serialized into `writev` segments owned by [`PendingWritev`].
    Frame(RespFrame),
    /// Owned serialized RESP bytes.
    Owned(Box<[u8]>),
}

#[derive(Debug)]
pub(super) enum QueuedCommandResult {
    Static(&'static [u8]),
    Engine(CmdResult),
}

#[derive(Default)]
pub(super) struct TransactionState {
    pub(super) queueing: bool,
    pub(super) dirty: bool,
    pub(super) queued: Vec<Box<[u8]>>,
    pub(super) queued_bytes: usize,
    pub(super) watched: Vec<WatchRegistration>,
    pub(super) watch_epoch: u64,
}

#[derive(Default)]
pub(super) struct CommandAccumulator {
    pub(super) bytes: Vec<u8>,
}

impl CommandAccumulator {
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        self.bytes.clear();
    }

    #[inline]
    pub(super) fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.bytes)
    }

    #[inline]
    pub(super) fn put(&mut self, mut bytes: Vec<u8>, retain_capacity: usize) {
        if bytes.is_empty() && bytes.capacity() > retain_capacity {
            bytes = Vec::new();
        }
        self.bytes = bytes;
    }
}

#[derive(Default)]
pub(super) struct CommandSliceOutcome {
    pub(super) consumed: usize,
    pub(super) close_after_write: bool,
    pub(super) yielded: bool,
    pub(super) need_more_data: bool,
}

pub(super) struct TransactionAofBatch {
    pub(super) records: Vec<(AofLsn, Box<[u8]>)>,
    pub(super) max_lsn: Option<AofLsn>,
}

impl TransactionAofBatch {
    pub(super) fn new() -> Self {
        Self {
            records: Vec::new(),
            max_lsn: None,
        }
    }

    #[inline]
    pub(super) fn push_owned(&mut self, lsn: AofLsn, payload: Box<[u8]>) {
        self.max_lsn = Some(self.max_lsn.map_or(lsn, |max| max.max(lsn)));
        self.records.push((lsn, payload));
    }

    #[inline]
    pub(super) fn push_copy(&mut self, lsn: AofLsn, payload: &[u8]) {
        self.push_owned(lsn, payload.to_vec().into_boxed_slice());
    }

    #[inline]
    pub(super) fn max_lsn(&self) -> Option<AofLsn> {
        self.max_lsn
    }

    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl TransactionState {
    #[inline]
    pub(super) fn clear_queued(&mut self) {
        self.queued.clear();
        self.queued_bytes = 0;
    }

    #[inline]
    pub(super) fn reset_multi(&mut self) {
        self.queueing = false;
        self.dirty = false;
        self.clear_queued();
    }

    #[inline]
    pub(super) fn reset_all(&mut self) {
        self.reset_multi();
        debug_assert!(
            self.watched.is_empty(),
            "WATCH registrations must be released through clear_watches"
        );
        self.watch_epoch = 0;
    }
}

pub(super) fn config_pair_response(
    param: &'static [u8],
    value: Vec<u8>,
) -> (CommandResponse, bool) {
    (
        CommandResponse::Frame(RespFrame::Array(Some(vec![
            RespFrame::BulkString(Some(bytes::Bytes::from_static(param))),
            RespFrame::BulkString(Some(bytes::Bytes::from(value))),
        ]))),
        false,
    )
}

#[inline]
pub(super) fn push_resp_array_len(buf: &mut Vec<u8>, len: usize) {
    buf.push(b'*');
    push_decimal(buf, len);
    buf.extend_from_slice(b"\r\n");
}

#[inline]
pub(super) fn push_resp_bulk_string(buf: &mut Vec<u8>, value: &[u8]) {
    buf.push(b'$');
    push_decimal(buf, value.len());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(value);
    buf.extend_from_slice(b"\r\n");
}

#[inline]
fn push_decimal<T: itoa::Integer>(buf: &mut Vec<u8>, value: T) {
    let mut digits = itoa::Buffer::new();
    buf.extend_from_slice(digits.format(value).as_bytes());
}

pub(super) fn append_cmd_result(buf: &mut Vec<u8>, result: CmdResult) {
    match result {
        CmdResult::Static(bytes) => buf.extend_from_slice(bytes),
        CmdResult::Inline(inline) => buf.extend_from_slice(inline.as_bytes()),
        CmdResult::Resp(frame) => append_resp_frame(buf, &frame),
    }
}

pub(super) fn append_resp_frame(buf: &mut Vec<u8>, frame: &RespFrame) {
    let start = buf.len();
    let mut len = 128usize;
    loop {
        buf.resize(start + len, 0);
        if let Some(written) = RespSerializer::serialize_to_slice(frame, &mut buf[start..]) {
            buf.truncate(start + written);
            return;
        }
        len = len.saturating_mul(2);
    }
}
