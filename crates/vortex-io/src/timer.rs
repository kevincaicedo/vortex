//! Hierarchical timing wheel for O(1) connection timeout management.
//!
//! Three levels provide a total range of ~12 days at 1-second resolution:
//!
//! | Level | Slots | Resolution | Range        |
//! |-------|-------|------------|--------------|
//! | 0     | 256   | 1 s        | 256 s (~4 m) |
//! | 1     | 64    | 256 s      | 16 384 s (~4.5 h) |
//! | 2     | 64    | 16 384 s   | 1 048 576 s (~12 d) |
//!
//! Timers are stored in a pre-allocated entry pool — no heap allocation
//! per insert. Cancellation is lazy: cancelled entries are skipped during
//! tick processing.

/// Sentinel value: empty slot / end-of-list / no entry.
pub const TIMER_NIL: u32 = u32::MAX;

// -- Level geometry ---------------------------------------------------------

const L0_BITS: u32 = 8;
const L1_BITS: u32 = 6;
const L2_BITS: u32 = 6;

const L0_SIZE: usize = 1 << L0_BITS; // 256
const L1_SIZE: usize = 1 << L1_BITS; // 64
const L2_SIZE: usize = 1 << L2_BITS; // 64

const L0_MASK: u32 = (L0_SIZE as u32) - 1;
const L1_MASK: u32 = (L1_SIZE as u32) - 1;
const L2_MASK: u32 = (L2_SIZE as u32) - 1;

const L1_SHIFT: u32 = L0_BITS;
const L2_SHIFT: u32 = L0_BITS + L1_BITS;

// -- Entry pool -------------------------------------------------------------

/// Internal entry stored in the pre-allocated pool.
#[derive(Clone, Copy)]
struct TimerEntry {
    /// Packed `(conn_id << 8) | generation`. Zero when cancelled/free.
    conn_and_gen: u32,
    /// Absolute deadline tick (seconds since reactor start).
    deadline: u32,
    /// Next entry index in the same wheel slot, or [`TIMER_NIL`].
    next: u32,
}

impl TimerEntry {
    const FREE: Self = Self {
        conn_and_gen: 0,
        deadline: 0,
        next: TIMER_NIL,
    };

    #[inline]
    fn pack(conn_id: usize, generation: u8) -> u32 {
        // Shift conn_id up by 1 so that (conn_id=0, gen=0) never produces 0.
        // This reserves conn_and_gen == 0 as the "cancelled/free" sentinel.
        (((conn_id as u32) + 1) << 8) | (generation as u32)
    }

    #[inline]
    fn conn_id(self) -> usize {
        ((self.conn_and_gen >> 8) - 1) as usize
    }

    #[inline]
    fn generation(self) -> u8 {
        (self.conn_and_gen & 0xFF) as u8
    }

    #[inline]
    fn is_active(self) -> bool {
        self.conn_and_gen != 0
    }
}

// -- Public types -----------------------------------------------------------

/// An expired timer returned by [`TimerWheel::tick`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpiredTimer {
    pub conn_id: usize,
    pub generation: u8,
}

// -- Timer wheel ------------------------------------------------------------

/// Hierarchical timing wheel with 3 levels and a pre-allocated entry pool.
pub struct TimerWheel {
    level0: [u32; L0_SIZE],
    level1: [u32; L1_SIZE],
    level2: [u32; L2_SIZE],
    /// Pre-allocated entry pool, indexed by entry index.
    entries: Vec<TimerEntry>,
    /// Head of the free list inside `entries`.
    free_head: u32,
    /// Current tick (seconds since reactor start).
    current_tick: u32,
    /// Number of active (non-cancelled, non-free) entries.
    pending: u32,
}

impl TimerWheel {
    /// Creates a new timer wheel with a pool of `capacity` entries.
    ///
    /// `capacity` should equal `max_connections` — each connection has at
    /// most one active timer.
    pub fn new(capacity: usize) -> Self {
        let mut entries = vec![TimerEntry::FREE; capacity];
        // Thread the free list through `next`.
        for i in 0..capacity.saturating_sub(1) {
            entries[i].next = (i + 1) as u32;
        }
        if capacity > 0 {
            entries[capacity - 1].next = TIMER_NIL;
        }

        Self {
            level0: [TIMER_NIL; L0_SIZE],
            level1: [TIMER_NIL; L1_SIZE],
            level2: [TIMER_NIL; L2_SIZE],
            entries,
            free_head: if capacity > 0 { 0 } else { TIMER_NIL },
            current_tick: 0,
            pending: 0,
        }
    }

    /// Current wheel tick.
    #[inline]
    pub fn current_tick(&self) -> u32 {
        self.current_tick
    }

    /// Number of non-cancelled timers in the wheel.
    #[inline]
    pub fn pending(&self) -> u32 {
        self.pending
    }

    /// Schedule a timer for `conn_id` at the given absolute `deadline` tick.
    ///
    /// Returns the entry-pool index (store in `ConnectionMeta.timer_slot`
    /// for O(1) cancellation).
    ///
    /// # Panics
    ///
    /// Debug-panics if the entry pool is exhausted.
    pub fn schedule(
        &mut self,
        conn_id: usize,
        generation: u8,
        deadline: u32,
    ) -> u32 {
        let entry_idx = self.alloc_entry();
        debug_assert!(entry_idx != TIMER_NIL, "timer entry pool exhausted");

        let entry = &mut self.entries[entry_idx as usize];
        entry.conn_and_gen = TimerEntry::pack(conn_id, generation);
        entry.deadline = deadline;

        // Determine level and slot.
        let delta = deadline.wrapping_sub(self.current_tick);
        let head = if delta < L0_SIZE as u32 {
            &mut self.level0[(deadline & L0_MASK) as usize]
        } else if delta < ((L0_SIZE as u32) << L1_BITS) {
            &mut self.level1[((deadline >> L1_SHIFT) & L1_MASK) as usize]
        } else {
            &mut self.level2[((deadline >> L2_SHIFT) & L2_MASK) as usize]
        };

        // Prepend to the slot's linked list.
        self.entries[entry_idx as usize].next = *head;
        *head = entry_idx;
        self.pending += 1;

        entry_idx
    }

    /// Lazily cancel a timer by its entry-pool index.
    ///
    /// The entry remains in its slot's linked list but is skipped during
    /// tick processing and freed at that point.
    #[inline]
    pub fn cancel(&mut self, entry_idx: u32) {
        if entry_idx != TIMER_NIL {
            if let Some(e) = self.entries.get_mut(entry_idx as usize) {
                if e.is_active() {
                    e.conn_and_gen = 0;
                    self.pending = self.pending.saturating_sub(1);
                }
            }
        }
    }

    /// Advance the wheel by one tick.
    ///
    /// Expired timers are appended to `expired`. The caller is responsible
    /// for checking `last_active` and deciding whether to close or
    /// reschedule each connection.
    pub fn tick(&mut self, expired: &mut Vec<ExpiredTimer>) {
        // Cascade from higher levels into level 0 BEFORE processing the
        // current level-0 slot.
        if self.current_tick > 0 && self.current_tick & L0_MASK == 0 {
            self.cascade_l1_to_l0();

            if self.current_tick & ((1u32 << L2_SHIFT) - 1) == 0 {
                self.cascade_l2_to_l1();
            }
        }

        // Drain the current level-0 slot.
        let slot = (self.current_tick & L0_MASK) as usize;
        let mut idx = self.level0[slot];
        self.level0[slot] = TIMER_NIL;

        while idx != TIMER_NIL {
            let entry = self.entries[idx as usize];
            let next_idx = entry.next;

            if entry.is_active() {
                expired.push(ExpiredTimer {
                    conn_id: entry.conn_id(),
                    generation: entry.generation(),
                });
                self.pending = self.pending.saturating_sub(1);
            }

            // Return node to the free list.
            self.free_entry(idx);
            idx = next_idx;
        }

        self.current_tick = self.current_tick.wrapping_add(1);
    }

    // -- Cascade helpers ----------------------------------------------------

    fn cascade_l1_to_l0(&mut self) {
        let l1_slot = ((self.current_tick >> L1_SHIFT) & L1_MASK) as usize;
        let mut idx = self.level1[l1_slot];
        self.level1[l1_slot] = TIMER_NIL;

        while idx != TIMER_NIL {
            let entry = self.entries[idx as usize];
            let next_idx = entry.next;

            if entry.is_active() {
                // Re-insert into the correct level-0 slot.
                let l0_slot = (entry.deadline & L0_MASK) as usize;
                self.entries[idx as usize].next = self.level0[l0_slot];
                self.level0[l0_slot] = idx;
            } else {
                self.free_entry(idx);
            }

            idx = next_idx;
        }
    }

    fn cascade_l2_to_l1(&mut self) {
        let l2_slot = ((self.current_tick >> L2_SHIFT) & L2_MASK) as usize;
        let mut idx = self.level2[l2_slot];
        self.level2[l2_slot] = TIMER_NIL;

        while idx != TIMER_NIL {
            let entry = self.entries[idx as usize];
            let next_idx = entry.next;

            if entry.is_active() {
                let l1_slot = ((entry.deadline >> L1_SHIFT) & L1_MASK) as usize;
                self.entries[idx as usize].next = self.level1[l1_slot];
                self.level1[l1_slot] = idx;
            } else {
                self.free_entry(idx);
            }

            idx = next_idx;
        }
    }

    // -- Pool helpers -------------------------------------------------------

    #[inline]
    fn alloc_entry(&mut self) -> u32 {
        let idx = self.free_head;
        if idx != TIMER_NIL {
            self.free_head = self.entries[idx as usize].next;
        }
        idx
    }

    #[inline]
    fn free_entry(&mut self, idx: u32) {
        self.entries[idx as usize] = TimerEntry::FREE;
        self.entries[idx as usize].next = self.free_head;
        self.free_head = idx;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── Basic schedule & tick ───────────────────────────────────────

    #[test]
    fn schedule_and_expire_single() {
        let mut tw = TimerWheel::new(16);
        tw.schedule(1, 0, 5); // deadline at tick 5
        assert_eq!(tw.pending(), 1);

        let mut expired = Vec::new();
        // Advance ticks 0..4 — nothing should expire.
        for _ in 0..5 {
            tw.tick(&mut expired);
        }
        assert!(expired.is_empty());

        // Tick 5 — the timer fires.
        tw.tick(&mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].conn_id, 1);
        assert_eq!(tw.pending(), 0);
    }

    #[test]
    fn schedule_multiple_same_slot() {
        let mut tw = TimerWheel::new(16);
        tw.schedule(10, 1, 3);
        tw.schedule(20, 2, 3);
        tw.schedule(30, 3, 3);
        assert_eq!(tw.pending(), 3);

        let mut expired = Vec::new();
        // Advance to tick 3.
        for _ in 0..3 {
            tw.tick(&mut expired);
        }
        assert!(expired.is_empty());

        tw.tick(&mut expired);
        assert_eq!(expired.len(), 3);
        let ids: Vec<usize> = expired.iter().map(|e| e.conn_id).collect();
        assert!(ids.contains(&10));
        assert!(ids.contains(&20));
        assert!(ids.contains(&30));
        assert_eq!(tw.pending(), 0);
    }

    #[test]
    fn schedule_at_current_tick() {
        let mut tw = TimerWheel::new(4);
        tw.schedule(1, 0, 0); // deadline == current_tick
        let mut expired = Vec::new();
        tw.tick(&mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].conn_id, 1);
    }

    // ── Cancellation ───────────────────────────────────────────────

    #[test]
    fn cancel_before_expire() {
        let mut tw = TimerWheel::new(8);
        let entry = tw.schedule(5, 0, 10);
        assert_eq!(tw.pending(), 1);

        tw.cancel(entry);
        assert_eq!(tw.pending(), 0);

        let mut expired = Vec::new();
        for _ in 0..=10 {
            tw.tick(&mut expired);
        }
        assert!(expired.is_empty());
    }

    #[test]
    fn cancel_nil_is_noop() {
        let mut tw = TimerWheel::new(4);
        tw.cancel(TIMER_NIL); // should not panic
        assert_eq!(tw.pending(), 0);
    }

    #[test]
    fn double_cancel() {
        let mut tw = TimerWheel::new(4);
        let entry = tw.schedule(1, 0, 5);
        tw.cancel(entry);
        tw.cancel(entry); // idempotent
        assert_eq!(tw.pending(), 0);
    }

    // ── Cascade Level 1 → Level 0 ─────────────────────────────────

    #[test]
    fn cascade_l1_to_l0() {
        let mut tw = TimerWheel::new(16);
        // Deadline at tick 300 (> 256) → goes to level 1.
        tw.schedule(7, 1, 300);
        assert_eq!(tw.pending(), 1);

        let mut expired = Vec::new();
        // Advance 300 ticks.
        for _ in 0..300 {
            tw.tick(&mut expired);
        }
        assert!(expired.is_empty());

        // Tick 300 — after cascade, the entry should fire.
        tw.tick(&mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].conn_id, 7);
        assert_eq!(expired[0].generation, 1);
        assert_eq!(tw.pending(), 0);
    }

    #[test]
    fn cascade_multiple_l1_entries() {
        let mut tw = TimerWheel::new(32);
        tw.schedule(1, 0, 260);
        tw.schedule(2, 0, 270);
        tw.schedule(3, 0, 280);

        let mut expired = Vec::new();

        // Advance to tick 260.
        for _ in 0..260 {
            tw.tick(&mut expired);
        }
        assert!(expired.is_empty());

        tw.tick(&mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].conn_id, 1);
        expired.clear();

        // Advance to tick 270.
        for _ in 0..9 {
            tw.tick(&mut expired);
        }
        assert!(expired.is_empty());
        tw.tick(&mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].conn_id, 2);
        expired.clear();

        // Advance to tick 280.
        for _ in 0..9 {
            tw.tick(&mut expired);
        }
        assert!(expired.is_empty());
        tw.tick(&mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].conn_id, 3);
    }

    // ── Cascade Level 2 → Level 1 → Level 0 ───────────────────────

    #[test]
    fn cascade_l2_to_l1_to_l0() {
        let mut tw = TimerWheel::new(8);
        // Deadline at tick 20_000 (> 16384) → goes to level 2.
        tw.schedule(99, 5, 20_000);
        assert_eq!(tw.pending(), 1);

        let mut expired = Vec::new();
        for _ in 0..20_000 {
            tw.tick(&mut expired);
        }
        assert!(expired.is_empty());

        tw.tick(&mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].conn_id, 99);
        assert_eq!(expired[0].generation, 5);
    }

    // ── Cancelled entries in cascaded slots ─────────────────────────

    #[test]
    fn cancel_before_cascade() {
        let mut tw = TimerWheel::new(8);
        let entry = tw.schedule(7, 0, 300);
        tw.cancel(entry);

        let mut expired = Vec::new();
        for _ in 0..=300 {
            tw.tick(&mut expired);
        }
        assert!(expired.is_empty());
    }

    // ── Freelist reuse ─────────────────────────────────────────────

    #[test]
    fn entries_reused_after_expire() {
        let mut tw = TimerWheel::new(2);
        tw.schedule(1, 0, 1);
        tw.schedule(2, 0, 2);
        // Pool is full.
        assert_eq!(tw.pending(), 2);

        let mut expired = Vec::new();
        tw.tick(&mut expired); // tick 0 — nothing
        tw.tick(&mut expired); // tick 1 — conn 1 expires

        // Now there's a free entry — we can schedule again.
        tw.schedule(3, 0, 5);
        assert_eq!(tw.pending(), 2); // conn 2 + conn 3
    }

    // ── Generation preserved ───────────────────────────────────────

    #[test]
    fn generation_roundtrip() {
        let mut tw = TimerWheel::new(4);
        tw.schedule(42, 200, 3);

        let mut expired = Vec::new();
        for _ in 0..=3 {
            tw.tick(&mut expired);
        }
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].conn_id, 42);
        assert_eq!(expired[0].generation, 200);
    }

    // ── Bulk insert & tick ─────────────────────────────────────────

    #[test]
    fn bulk_timers() {
        let count = 256;
        let mut tw = TimerWheel::new(count);
        for i in 0..count {
            tw.schedule(i, (i & 0xFF) as u8, i as u32 + 1);
        }
        assert_eq!(tw.pending() as usize, count);

        let mut expired = Vec::new();
        let mut total_expired = 0;
        for _ in 0..=count {
            expired.clear();
            tw.tick(&mut expired);
            total_expired += expired.len();
        }
        assert_eq!(total_expired, count);
        assert_eq!(tw.pending(), 0);
    }
}
