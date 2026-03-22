use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_utils::CachePadded;

/// Per-core sharded atomic counter for contention-free metrics.
///
/// Each slot is on its own cache line (`CachePadded`) so increments from
/// different cores don't cause false sharing. Aggregation is O(cores)
/// but the hot path (increment) is contention-free.
pub struct ShardedCounter {
    slots: Box<[CachePadded<AtomicU64>]>,
}

impl ShardedCounter {
    /// Creates a new sharded counter with the given number of slots.
    /// Typically `num_cpus` or the reactor count.
    pub fn new(num_slots: usize) -> Self {
        let slots: Vec<CachePadded<AtomicU64>> = (0..num_slots)
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect();
        Self {
            slots: slots.into_boxed_slice(),
        }
    }

    /// Increments the counter for a specific slot (typically the current core ID).
    #[inline]
    pub fn increment(&self, slot: usize) {
        if let Some(counter) = self.slots.get(slot) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Adds `n` to the counter for a specific slot.
    #[inline]
    pub fn add(&self, slot: usize, n: u64) {
        if let Some(counter) = self.slots.get(slot) {
            counter.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Returns the aggregate count across all slots.
    pub fn total(&self) -> u64 {
        self.slots
            .iter()
            .map(|s| s.load(Ordering::Relaxed))
            .sum()
    }

    /// Returns the count for a specific slot.
    pub fn get(&self, slot: usize) -> u64 {
        self.slots
            .get(slot)
            .map(|s| s.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Resets all counters to zero.
    pub fn reset(&self) {
        for slot in self.slots.iter() {
            slot.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_counting() {
        let counter = ShardedCounter::new(4);
        counter.increment(0);
        counter.increment(1);
        counter.add(2, 10);
        assert_eq!(counter.total(), 12);
        assert_eq!(counter.get(0), 1);
        assert_eq!(counter.get(2), 10);
    }

    #[test]
    fn reset_clears() {
        let counter = ShardedCounter::new(4);
        counter.add(0, 100);
        counter.reset();
        assert_eq!(counter.total(), 0);
    }

    #[test]
    fn out_of_bounds_is_safe() {
        let counter = ShardedCounter::new(2);
        counter.increment(999); // Should not panic
        assert_eq!(counter.total(), 0);
    }
}
