//! Dual timing-wheel TTL expiry engine.
//!
//! Two wheels target different resolution / range trade-offs:
//!
//! | Wheel   | Slots  | Resolution | Range       |
//! |---------|--------|------------|-------------|
//! | Seconds | 65 536 | 1 s        | ≈18.2 hours |
//! | Millis  |  1 024 | 1 ms       | ≈1.024 s    |
//!
//! Keys with TTL ≥ 1 s are placed in the seconds wheel.
//! Keys with sub-second TTL go into the milliseconds wheel.
//!
//! **Ghost detection:** When a key's TTL is updated (re-EXPIRE) or removed
//! (PERSIST), the old wheel entry becomes a *ghost*. During tick, each
//! candidate's `deadline_nanos` is compared against the entry's live
//! `ttl_deadline` in the Swiss Table. Mismatches are silently discarded.

// ── Constants ───────────────────────────────────────────────────────

const SECOND_WHEEL_SLOTS: usize = 65_536;
const SECOND_RESOLUTION_NS: u64 = 1_000_000_000;

const MILLIS_WHEEL_SLOTS: usize = 1_024;
const MILLIS_RESOLUTION_NS: u64 = 1_000_000;

// ── ExpiryEntry ─────────────────────────────────────────────────────

/// A 16-byte entry stored in a timing-wheel slot.
///
/// `key_hash` is the full 64-bit hash used by the Swiss Table so the
/// active sweep can probe by hash without storing key bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpiryEntry {
    pub key_hash: u64,
    pub deadline_nanos: u64,
}

// ── ExpiryWheel ─────────────────────────────────────────────────────

/// Dual timing wheel for O(1) TTL registration and bounded-work expiry ticks.
pub struct ExpiryWheel {
    second_slots: Box<[Vec<ExpiryEntry>]>,
    millis_slots: Box<[Vec<ExpiryEntry>]>,
    /// Last ticked second (absolute nanoseconds / 1e9).
    last_tick_sec: u64,
    /// Last ticked millisecond (absolute nanoseconds / 1e6).
    last_tick_ms: u64,
    /// Total registered entries (includes ghosts — lazily cleaned on tick).
    count: usize,
}

impl ExpiryWheel {
    /// Create an empty dual timing wheel.
    pub fn new() -> Self {
        Self {
            second_slots: (0..SECOND_WHEEL_SLOTS)
                .map(|_| Vec::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            millis_slots: (0..MILLIS_WHEEL_SLOTS)
                .map(|_| Vec::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            last_tick_sec: 0,
            last_tick_ms: 0,
            count: 0,
        }
    }

    /// Set the initial time reference without processing any entries.
    ///
    /// Must be called before the first `register` / `tick` to establish
    /// the wheel's time baseline. The reactor calls this at startup with
    /// its cached monotonic timestamp.
    #[inline]
    pub fn set_time(&mut self, now_nanos: u64) {
        self.last_tick_sec = now_nanos / SECOND_RESOLUTION_NS;
        self.last_tick_ms = now_nanos / MILLIS_RESOLUTION_NS;
    }

    /// Register a key in the appropriate wheel.
    ///
    /// `key_hash` is the full 64-bit hash from the Swiss Table.
    /// `deadline_nanos` is the absolute monotonic nanosecond deadline.
    #[inline]
    pub fn register(&mut self, key_hash: u64, deadline_nanos: u64) {
        let entry = ExpiryEntry {
            key_hash,
            deadline_nanos,
        };

        // Sub-second deadlines → millis wheel. ≥ 1 s → seconds wheel.
        let now_ns = self.last_tick_sec * SECOND_RESOLUTION_NS;
        let distance_ns = deadline_nanos.saturating_sub(now_ns);

        if distance_ns < SECOND_RESOLUTION_NS {
            let slot =
                ((deadline_nanos / MILLIS_RESOLUTION_NS) % MILLIS_WHEEL_SLOTS as u64) as usize;
            self.millis_slots[slot].push(entry);
        } else {
            let slot =
                ((deadline_nanos / SECOND_RESOLUTION_NS) % SECOND_WHEEL_SLOTS as u64) as usize;
            self.second_slots[slot].push(entry);
        }

        self.count += 1;
    }

    /// Tick both wheels up to `now_nanos`, collecting expired entries.
    ///
    /// Work is bounded to `max_entries` total expired entries returned.
    /// Returns the number of newly expired entries appended to `expired`.
    #[inline]
    pub fn tick(
        &mut self,
        now_nanos: u64,
        max_entries: usize,
        expired: &mut Vec<ExpiryEntry>,
    ) -> usize {
        let start = expired.len();

        self.tick_millis(now_nanos, max_entries, expired);

        let used = expired.len() - start;
        if used < max_entries {
            self.tick_seconds(now_nanos, max_entries - used, expired);
        }

        expired.len() - start
    }

    /// Total registered entries (including ghosts).
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    // ── Internal tick helpers ───────────────────────────────────────

    fn tick_seconds(
        &mut self,
        now_nanos: u64,
        max_entries: usize,
        expired: &mut Vec<ExpiryEntry>,
    ) {
        let now_sec = now_nanos / SECOND_RESOLUTION_NS;
        if now_sec <= self.last_tick_sec {
            return;
        }

        let gap = (now_sec - self.last_tick_sec) as usize;

        // Full rotation: drain all non-empty slots without per-index iteration.
        if gap >= SECOND_WHEEL_SLOTS {
            let mut budget = max_entries;
            for slot in self.second_slots.iter_mut() {
                if slot.is_empty() {
                    continue;
                }
                let removed = Self::drain_slot(slot, now_nanos, &mut budget, expired);
                self.count -= removed;
                if budget == 0 {
                    return;
                }
            }
            self.last_tick_sec = now_sec;
            return;
        }

        let mut budget = max_entries;
        let base = self.last_tick_sec;

        for i in 0..gap {
            let slot_idx = (base as usize + 1 + i) % SECOND_WHEEL_SLOTS;
            let slot = &mut self.second_slots[slot_idx];
            if slot.is_empty() {
                continue;
            }

            // O(budget) drain via swap_remove — avoids O(n) retain on large slots.
            let removed = Self::drain_slot(slot, now_nanos, &mut budget, expired);
            self.count -= removed;

            if budget == 0 {
                // Don't advance past this slot — resume here next tick.
                self.last_tick_sec = base + i as u64;
                return;
            }
        }

        self.last_tick_sec = now_sec;
    }

    fn tick_millis(
        &mut self,
        now_nanos: u64,
        max_entries: usize,
        expired: &mut Vec<ExpiryEntry>,
    ) {
        let now_ms = now_nanos / MILLIS_RESOLUTION_NS;
        if now_ms <= self.last_tick_ms {
            return;
        }

        let gap = (now_ms - self.last_tick_ms) as usize;

        // If the gap covers a full wheel rotation, drain everything and skip iteration.
        if gap >= MILLIS_WHEEL_SLOTS {
            let mut budget = max_entries;
            for slot in self.millis_slots.iter_mut() {
                if slot.is_empty() {
                    continue;
                }
                let removed = Self::drain_slot(slot, now_nanos, &mut budget, expired);
                self.count -= removed;
                if budget == 0 {
                    return;
                }
            }
            self.last_tick_ms = now_ms;
            return;
        }

        let mut budget = max_entries;
        let base = self.last_tick_ms;

        for i in 0..gap {
            let slot_idx = (base as usize + 1 + i) % MILLIS_WHEEL_SLOTS;
            let slot = &mut self.millis_slots[slot_idx];
            if slot.is_empty() {
                continue;
            }

            let removed = Self::drain_slot(slot, now_nanos, &mut budget, expired);
            self.count -= removed;

            if budget == 0 {
                self.last_tick_ms = base + i as u64;
                return;
            }
        }

        self.last_tick_ms = now_ms;
    }

    /// Drain expired entries from a single slot using O(budget) swap_remove.
    ///
    /// Returns the number of entries removed.
    #[inline]
    fn drain_slot(
        slot: &mut Vec<ExpiryEntry>,
        now_nanos: u64,
        budget: &mut usize,
        expired: &mut Vec<ExpiryEntry>,
    ) -> usize {
        let mut removed = 0;
        let mut i = 0;
        while i < slot.len() && *budget > 0 {
            if slot[i].deadline_nanos <= now_nanos {
                expired.push(slot.swap_remove(i));
                *budget -= 1;
                removed += 1;
                // Don't increment i — swap_remove moved the last element here.
            } else {
                i += 1; // Non-expired wrap-around entry — skip.
            }
        }
        removed
    }
}

impl Default for ExpiryWheel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS_PER_SEC: u64 = 1_000_000_000;
    const NS_PER_MS: u64 = 1_000_000;

    #[test]
    fn wheel_sizes() {
        let wheel = ExpiryWheel::new();
        assert_eq!(wheel.second_slots.len(), SECOND_WHEEL_SLOTS);
        assert_eq!(wheel.millis_slots.len(), MILLIS_WHEEL_SLOTS);
        assert!(wheel.is_empty());
    }

    #[test]
    fn register_and_tick_seconds() {
        let mut wheel = ExpiryWheel::new();
        wheel.set_time(100 * NS_PER_SEC);

        // Register a key expiring at t=105s
        let deadline = 105 * NS_PER_SEC;
        wheel.register(0xDEAD, deadline);
        assert_eq!(wheel.len(), 1);

        // Tick to t=104s — nothing should expire
        let mut expired = Vec::new();
        let n = wheel.tick(104 * NS_PER_SEC, 100, &mut expired);
        assert_eq!(n, 0);
        assert!(expired.is_empty());

        // Tick to t=106s — entry should expire
        let n = wheel.tick(106 * NS_PER_SEC, 100, &mut expired);
        assert_eq!(n, 1);
        assert_eq!(expired[0].key_hash, 0xDEAD);
        assert_eq!(expired[0].deadline_nanos, deadline);
        assert_eq!(wheel.len(), 0);
    }

    #[test]
    fn register_and_tick_millis() {
        let mut wheel = ExpiryWheel::new();
        wheel.set_time(100 * NS_PER_SEC);

        // Register a key expiring in 500ms (sub-second TTL → millis wheel)
        let deadline = 100 * NS_PER_SEC + 500 * NS_PER_MS;
        wheel.register(0xBEEF, deadline);
        assert_eq!(wheel.len(), 1);

        // Tick to 100.400s — not expired
        let mut expired = Vec::new();
        let n = wheel.tick(100 * NS_PER_SEC + 400 * NS_PER_MS, 100, &mut expired);
        assert_eq!(n, 0);

        // Tick to 100.600s — expired
        let n = wheel.tick(100 * NS_PER_SEC + 600 * NS_PER_MS, 100, &mut expired);
        assert_eq!(n, 1);
        assert_eq!(expired[0].key_hash, 0xBEEF);
    }

    #[test]
    fn ghost_entries_survive_until_drained() {
        let mut wheel = ExpiryWheel::new();
        wheel.set_time(100 * NS_PER_SEC);

        // Register two entries at the same deadline
        let deadline = 105 * NS_PER_SEC;
        wheel.register(0xAAAA, deadline);
        wheel.register(0xBBBB, deadline);
        assert_eq!(wheel.len(), 2);

        // Tick past deadline — both drained
        let mut expired = Vec::new();
        wheel.tick(106 * NS_PER_SEC, 100, &mut expired);
        assert_eq!(expired.len(), 2);
        assert_eq!(wheel.len(), 0);
    }

    #[test]
    fn bounded_tick_respects_max_entries() {
        let mut wheel = ExpiryWheel::new();
        wheel.set_time(100 * NS_PER_SEC);

        // Register 50 entries at t=105s
        let deadline = 105 * NS_PER_SEC;
        for i in 0..50 {
            wheel.register(i, deadline);
        }
        assert_eq!(wheel.len(), 50);

        let mut expired = Vec::new();

        // Tick with max_entries=20 — should only get 20
        let n = wheel.tick(106 * NS_PER_SEC, 20, &mut expired);
        assert_eq!(n, 20);
        assert_eq!(expired.len(), 20);

        // Tick again to get the rest
        let n = wheel.tick(106 * NS_PER_SEC, 100, &mut expired);
        assert_eq!(n, 30);
        assert_eq!(expired.len(), 50);
        assert_eq!(wheel.len(), 0);
    }

    #[test]
    fn wrap_around_entries_preserved() {
        let mut wheel = ExpiryWheel::new();
        wheel.set_time(100 * NS_PER_SEC);

        // Register a key expiring at t=100 + 65536 + 5 = wraps to same slot as t=105
        let near_deadline = 105 * NS_PER_SEC;
        let far_deadline = (100 + SECOND_WHEEL_SLOTS as u64 + 5) * NS_PER_SEC;

        // Both end up in the same slot: (deadline / 1e9) % 65536
        wheel.register(0x1111, near_deadline);
        wheel.register(0x2222, far_deadline);
        assert_eq!(wheel.len(), 2);

        // Tick to t=106 — only the near entry should expire
        let mut expired = Vec::new();
        wheel.tick(106 * NS_PER_SEC, 100, &mut expired);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].key_hash, 0x1111);
        assert_eq!(wheel.len(), 1); // far entry preserved
    }

    #[test]
    fn multiple_ticks_advance() {
        let mut wheel = ExpiryWheel::new();
        wheel.set_time(0);

        // Register entries at 1s, 2s, 3s
        wheel.register(1, NS_PER_SEC);
        wheel.register(2, 2 * NS_PER_SEC);
        wheel.register(3, 3 * NS_PER_SEC);

        let mut expired = Vec::new();

        // Tick to 1.5s — first entry expires
        wheel.tick(NS_PER_SEC + 500 * NS_PER_MS, 100, &mut expired);
        assert_eq!(expired.len(), 1);

        // Tick to 3.5s — remaining two expire
        wheel.tick(3 * NS_PER_SEC + 500 * NS_PER_MS, 100, &mut expired);
        assert_eq!(expired.len(), 3);
    }

    #[test]
    fn entry_size() {
        // ExpiryEntry should be 16 bytes (two u64s) — cache-friendly.
        assert_eq!(core::mem::size_of::<ExpiryEntry>(), 16);
    }
}
