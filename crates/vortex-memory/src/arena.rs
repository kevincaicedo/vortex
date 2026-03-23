//! Per-reactor bump allocator for short-lived, per-iteration allocations.
//!
//! Each reactor owns its own `ArenaAllocator`. Within a single event-loop
//! iteration, temporary response-building buffers are allocated via `alloc()`.
//! At the end of the iteration, `reset()` rewinds the bump pointer in O(1)
//! without any per-object deallocation.
//!
//! If the arena is exhausted, a `Vec`-backed fallback is used and a warning
//! is logged (this indicates the arena capacity should be increased).

use std::alloc::Layout;

/// Default arena capacity: 1 MiB.
pub const DEFAULT_ARENA_CAPACITY: usize = 1024 * 1024;

/// A simple bump allocator backed by a pre-allocated slab.
///
/// Not thread-safe — designed for single-reactor ownership.
pub struct ArenaAllocator {
    /// The backing slab (page-aligned via `Vec` guarantee on alloc).
    slab: Vec<u8>,
    /// Current allocation offset into `slab`.
    offset: usize,
    /// Overflow allocations (when slab is exhausted).
    overflow: Vec<Vec<u8>>,
}

impl ArenaAllocator {
    /// Creates a new arena with the given capacity in bytes.
    pub fn new(capacity: usize) -> Self {
        Self {
            slab: vec![0u8; capacity],
            offset: 0,
            overflow: Vec::new(),
        }
    }

    /// Allocates `size` bytes with the given `align`ment from the arena.
    ///
    /// Returns a pointer to the allocated region. If the arena is exhausted,
    /// falls back to a heap-allocated `Vec` and logs a warning.
    ///
    /// # Safety
    ///
    /// The returned pointer is valid until [`reset()`](Self::reset) is called.
    /// The caller must not use the pointer after reset.
    ///
    /// `align` must be a power of two and `size` must be non-zero.
    pub fn alloc(&mut self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        let align = layout.align();

        // Compute aligned address from the absolute pointer, not the offset,
        // because the Vec<u8> backing has no alignment guarantees beyond 1.
        let base = self.slab.as_mut_ptr() as usize;
        let current = base + self.offset;
        let aligned_addr = (current + align - 1) & !(align - 1);
        let aligned_offset = aligned_addr - base;
        let new_offset = aligned_offset + size;

        if new_offset <= self.slab.len() {
            self.offset = new_offset;
            // SAFETY: `aligned_offset` and `new_offset` are within `self.slab` bounds
            // (checked by the `if` above). The slab is owned exclusively by this
            // arena and not aliased (single-reactor, no concurrent access).
            unsafe { self.slab.as_mut_ptr().add(aligned_offset) }
        } else {
            // Overflow: fall back to heap allocation.
            tracing::warn!(
                requested = size,
                arena_capacity = self.slab.len(),
                "arena exhausted — falling back to heap allocation; consider increasing arena capacity"
            );
            let mut buf = vec![0u8; size];
            let ptr = buf.as_mut_ptr();
            self.overflow.push(buf);
            ptr
        }
    }

    /// Resets the arena, making all previous allocations invalid.
    ///
    /// This is O(1) for the slab and O(n) for any overflow allocations
    /// (which are dropped). After reset the full arena capacity is available.
    #[inline]
    pub fn reset(&mut self) {
        self.offset = 0;
        self.overflow.clear();
    }

    /// Returns the number of bytes currently allocated from the slab.
    #[inline]
    pub fn used(&self) -> usize {
        self.offset
    }

    /// Returns the total slab capacity in bytes.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.slab.len()
    }

    /// Returns the remaining slab capacity in bytes.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.slab.len().saturating_sub(self.offset)
    }

    /// Returns the number of overflow (heap-fallback) allocations.
    #[inline]
    pub fn overflow_count(&self) -> usize {
        self.overflow.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_alloc_and_reset() {
        let mut arena = ArenaAllocator::new(4096);

        // Use align=1 so that `used()` is deterministic regardless of the
        // slab base address (varies under Miri). Alignment-specific behaviour
        // is covered by `alignment_is_respected`.
        let layout = Layout::from_size_align(128, 1).unwrap();
        let ptr = arena.alloc(layout);
        assert!(!ptr.is_null());
        assert_eq!(arena.used(), 128);

        arena.reset();
        assert_eq!(arena.used(), 0);
        assert_eq!(arena.remaining(), 4096);
    }

    #[test]
    fn alignment_is_respected() {
        let mut arena = ArenaAllocator::new(4096);

        // Allocate 1 byte to offset the bump pointer.
        let layout1 = Layout::from_size_align(1, 1).unwrap();
        arena.alloc(layout1);

        // Next allocation with 64-byte alignment must yield an aligned pointer.
        let layout2 = Layout::from_size_align(32, 64).unwrap();
        let ptr = arena.alloc(layout2);
        assert_eq!(ptr as usize % 64, 0, "allocation must be 64-byte aligned");
    }

    #[test]
    fn overflow_falls_back_to_heap() {
        let mut arena = ArenaAllocator::new(64);

        // First allocation fits.
        let layout1 = Layout::from_size_align(32, 1).unwrap();
        arena.alloc(layout1);
        assert_eq!(arena.overflow_count(), 0);

        // This allocation exceeds remaining capacity → overflow.
        let layout2 = Layout::from_size_align(64, 1).unwrap();
        let ptr = arena.alloc(layout2);
        assert!(!ptr.is_null());
        assert_eq!(arena.overflow_count(), 1);

        // Reset clears overflow.
        arena.reset();
        assert_eq!(arena.overflow_count(), 0);
    }

    #[test]
    fn no_heap_allocation_within_capacity() {
        let mut arena = ArenaAllocator::new(4096);
        // Use align=1 so that `used()` is deterministic (see `basic_alloc_and_reset`).
        let layout = Layout::from_size_align(128, 1).unwrap();

        for _ in 0..30 {
            arena.alloc(layout);
        }

        // 30 × 128 = 3840, which fits in 4096.
        assert_eq!(arena.overflow_count(), 0);
        assert_eq!(arena.used(), 3840);
    }
}
