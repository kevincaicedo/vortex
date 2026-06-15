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
    backend_submit_sampler: LocalMetricSampler,
    loop_sampler: LocalMetricSampler,
    accept_eagain_sampler: LocalMetricSampler,
    accept_sampler: LocalMetricSampler,
    completion_sampler: LocalMetricSampler,
    completion_budget_sampler: LocalMetricSampler,
    command_sampler: LocalMetricSampler,
    command_budget_sampler: LocalMetricSampler,
    accept_budget_sampler: LocalMetricSampler,
    writev_budget_sampler: LocalMetricSampler,
    maintenance_budget_sampler: LocalMetricSampler,
    yielded_connection_sampler: LocalMetricSampler,
    parser_resume_sampler: LocalMetricSampler,
    writev_sampler: LocalMetricSampler,
    queued_response_sampler: LocalMetricSampler,
    active_expiry_sampler: LocalMetricSampler,
}

#[derive(Debug, Default)]
struct LocalMetricSampler {
    sample_rate: u32,
    sample_countdown: u32,
}

impl LocalMetricSampler {
    #[inline]
    fn new(sample_rate: u32) -> Self {
        Self {
            sample_rate,
            sample_countdown: sample_rate,
        }
    }

    #[inline]
    fn record_weight(&mut self) -> Option<u64> {
        match self.sample_rate {
            0 => None,
            1 => Some(1),
            rate => {
                if self.sample_countdown > 1 {
                    self.sample_countdown -= 1;
                    return None;
                }
                self.sample_countdown = rate;
                Some(rate as u64)
            }
        }
    }
}

impl ReactorLocalMetrics {
    pub(super) fn new(sample_rate: u32) -> Self {
        Self {
            pending: RuntimeLocalFlushMetrics::default(),
            backend_submit_sampler: LocalMetricSampler::new(sample_rate),
            loop_sampler: LocalMetricSampler::new(sample_rate),
            accept_eagain_sampler: LocalMetricSampler::new(sample_rate),
            accept_sampler: LocalMetricSampler::new(sample_rate),
            completion_sampler: LocalMetricSampler::new(sample_rate),
            completion_budget_sampler: LocalMetricSampler::new(sample_rate),
            command_sampler: LocalMetricSampler::new(sample_rate),
            command_budget_sampler: LocalMetricSampler::new(sample_rate),
            accept_budget_sampler: LocalMetricSampler::new(sample_rate),
            writev_budget_sampler: LocalMetricSampler::new(sample_rate),
            maintenance_budget_sampler: LocalMetricSampler::new(sample_rate),
            yielded_connection_sampler: LocalMetricSampler::new(sample_rate),
            parser_resume_sampler: LocalMetricSampler::new(sample_rate),
            writev_sampler: LocalMetricSampler::new(sample_rate),
            queued_response_sampler: LocalMetricSampler::new(sample_rate),
            active_expiry_sampler: LocalMetricSampler::new(sample_rate),
        }
    }

    #[inline]
    pub(super) fn record_backend_submit_syscall(&mut self) {
        let Some(weight) = self.backend_submit_sampler.record_weight() else {
            return;
        };
        self.pending.backend_submit_syscalls =
            self.pending.backend_submit_syscalls.saturating_add(weight);
    }

    #[inline]
    pub(super) fn record_loop_iteration(&mut self) {
        let Some(weight) = self.loop_sampler.record_weight() else {
            return;
        };
        self.pending.loop_iterations = self.pending.loop_iterations.saturating_add(weight);
    }

    #[inline]
    pub(super) fn record_accept_eagain_rearm(&mut self) {
        let Some(weight) = self.accept_eagain_sampler.record_weight() else {
            return;
        };
        self.pending.accept_eagain_rearms =
            self.pending.accept_eagain_rearms.saturating_add(weight);
    }

    #[inline]
    pub(super) fn record_accept_drain(&mut self, accepted: usize) {
        let Some(weight) = self.accept_sampler.record_weight() else {
            return;
        };
        self.pending.accept_drain_runs = self.pending.accept_drain_runs.saturating_add(weight);
        if accepted == 0 {
            return;
        }
        let accepted = accepted as u64;
        self.pending.accept_drain_accepted = self
            .pending
            .accept_drain_accepted
            .saturating_add(accepted.saturating_mul(weight));
        self.pending.accept_drain_accepted_max =
            self.pending.accept_drain_accepted_max.max(accepted);
    }

    #[inline]
    pub(super) fn record_completion_budget_exhaustion(&mut self) {
        let Some(weight) = self.completion_budget_sampler.record_weight() else {
            return;
        };
        self.pending.completion_budget_exhaustions = self
            .pending
            .completion_budget_exhaustions
            .saturating_add(weight);
    }

    #[inline]
    pub(super) fn record_command_budget_exhaustion(&mut self) {
        let Some(weight) = self.command_budget_sampler.record_weight() else {
            return;
        };
        self.pending.command_budget_exhaustions = self
            .pending
            .command_budget_exhaustions
            .saturating_add(weight);
    }

    #[inline]
    pub(super) fn record_accept_budget_exhaustion(&mut self) {
        let Some(weight) = self.accept_budget_sampler.record_weight() else {
            return;
        };
        self.pending.accept_budget_exhaustions = self
            .pending
            .accept_budget_exhaustions
            .saturating_add(weight);
    }

    #[inline]
    pub(super) fn record_writev_budget_exhaustion(&mut self) {
        let Some(weight) = self.writev_budget_sampler.record_weight() else {
            return;
        };
        self.pending.writev_budget_exhaustions = self
            .pending
            .writev_budget_exhaustions
            .saturating_add(weight);
    }

    #[inline]
    pub(super) fn record_maintenance_budget_exhaustion(&mut self) {
        let Some(weight) = self.maintenance_budget_sampler.record_weight() else {
            return;
        };
        self.pending.maintenance_budget_exhaustions = self
            .pending
            .maintenance_budget_exhaustions
            .saturating_add(weight);
    }

    #[inline]
    pub(super) fn record_yielded_connection(&mut self) {
        let Some(weight) = self.yielded_connection_sampler.record_weight() else {
            return;
        };
        self.pending.yielded_connections = self.pending.yielded_connections.saturating_add(weight);
    }

    #[inline]
    pub(super) fn record_parser_resume(&mut self) {
        let Some(weight) = self.parser_resume_sampler.record_weight() else {
            return;
        };
        self.pending.parser_resumes = self.pending.parser_resumes.saturating_add(weight);
    }

    #[inline]
    pub(super) fn record_completion_batch(&mut self, width: usize) {
        if width == 0 {
            return;
        }
        let Some(weight) = self.completion_sampler.record_weight() else {
            return;
        };
        let width = width as u64;
        self.pending.completion_batch_count =
            self.pending.completion_batch_count.saturating_add(weight);
        self.pending.completion_batch_total = self
            .pending
            .completion_batch_total
            .saturating_add(width.saturating_mul(weight));
        self.pending.completion_batch_max = self.pending.completion_batch_max.max(width);
    }

    #[inline]
    pub(super) fn record_command_batch(&mut self, width: usize) {
        if width == 0 {
            return;
        }
        let Some(weight) = self.command_sampler.record_weight() else {
            return;
        };
        let width = width as u64;
        self.pending.command_batch_count = self.pending.command_batch_count.saturating_add(weight);
        self.pending.command_batch_total = self
            .pending
            .command_batch_total
            .saturating_add(width.saturating_mul(weight));
        self.pending.command_batch_max = self.pending.command_batch_max.max(width);
    }

    #[inline]
    pub(super) fn record_writev_chunk(&mut self, iovecs: usize) {
        let Some(weight) = self.writev_sampler.record_weight() else {
            return;
        };
        self.pending.writev_chunks = self.pending.writev_chunks.saturating_add(weight);
        if iovecs == 0 {
            return;
        }
        let iovecs = iovecs as u64;
        self.pending.writev_iovecs_total = self
            .pending
            .writev_iovecs_total
            .saturating_add(iovecs.saturating_mul(weight));
        self.pending.writev_iovecs_max = self.pending.writev_iovecs_max.max(iovecs);
    }

    #[inline]
    pub(super) fn record_queued_response_bytes(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let Some(weight) = self.queued_response_sampler.record_weight() else {
            return;
        };
        let bytes = bytes as u64;
        self.pending.queued_response_bytes_total = self
            .pending
            .queued_response_bytes_total
            .saturating_add(bytes.saturating_mul(weight));
        self.pending.queued_response_bytes_max = self.pending.queued_response_bytes_max.max(bytes);
    }

    #[inline]
    pub(super) fn record_active_expiry(&mut self, sampled: usize, expired: usize) {
        let Some(weight) = self.active_expiry_sampler.record_weight() else {
            return;
        };
        self.pending.active_expiry_runs = self.pending.active_expiry_runs.saturating_add(weight);
        if sampled != 0 {
            self.pending.active_expiry_sampled = self
                .pending
                .active_expiry_sampled
                .saturating_add((sampled as u64).saturating_mul(weight));
        }
        if expired != 0 {
            self.pending.active_expiry_expired = self
                .pending
                .active_expiry_expired
                .saturating_add((expired as u64).saturating_mul(weight));
        }
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
        CmdResult::Owned(bytes) => buf.extend_from_slice(&bytes),
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
