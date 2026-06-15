use super::*;

#[derive(Clone, Copy, Default)]
pub(super) struct InflightSet {
    pub(super) read: bool,
    pub(super) write: bool,
    pub(super) writev: bool,
    pub(super) close: bool,
    pub(super) cancel_read: bool,
    pub(super) cancel_write: bool,
    pub(super) cancel_writev: bool,
    pub(super) cancel_close: bool,
}

impl InflightSet {
    #[inline]
    pub(super) fn mark_submitted(&mut self, op: OpType) {
        match op {
            OpType::Read => self.read = true,
            OpType::Write => self.write = true,
            OpType::Writev => self.writev = true,
            OpType::Close => self.close = true,
        }
    }

    #[inline]
    pub(super) fn mark_completed(&mut self, op: OpType) {
        match op {
            OpType::Read => self.read = false,
            OpType::Write => self.write = false,
            OpType::Writev => self.writev = false,
            OpType::Close => self.close = false,
        }
    }

    #[inline]
    pub(super) fn mark_cancel_submitted(&mut self, op: OpType) {
        match op {
            OpType::Read => self.cancel_read = true,
            OpType::Write => self.cancel_write = true,
            OpType::Writev => self.cancel_writev = true,
            OpType::Close => self.cancel_close = true,
        }
    }

    #[inline]
    pub(super) fn mark_cancel_completed(&mut self, op: OpType) {
        match op {
            OpType::Read => self.cancel_read = false,
            OpType::Write => self.cancel_write = false,
            OpType::Writev => self.cancel_writev = false,
            OpType::Close => self.cancel_close = false,
        }
    }

    #[inline]
    pub(super) fn cancel_inflight(&self, op: OpType) -> bool {
        match op {
            OpType::Read => self.cancel_read,
            OpType::Write => self.cancel_write,
            OpType::Writev => self.cancel_writev,
            OpType::Close => self.cancel_close,
        }
    }

    #[inline]
    pub(super) fn has(&self, op: OpType) -> bool {
        match op {
            OpType::Read => self.read,
            OpType::Write => self.write,
            OpType::Writev => self.writev,
            OpType::Close => self.close,
        }
    }

    #[inline]
    pub(super) fn has_any(&self) -> bool {
        self.read
            || self.write
            || self.writev
            || self.close
            || self.cancel_read
            || self.cancel_write
            || self.cancel_writev
            || self.cancel_close
    }
}
