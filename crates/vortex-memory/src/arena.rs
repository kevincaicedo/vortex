//! Per-reactor bump allocator for short-lived, per-iteration allocations.
//!
//! Each reactor owns its own `ArenaAllocator`. Within a single event-loop
//! iteration, temporary response-building buffers are allocated via `alloc()`.
//! At the end of the iteration, `reset()` rewinds the bump pointer in O(1)
//! without any per-object deallocation.
//!
//! If the arena is exhausted, an aligned heap fallback is used and a warning is
//! logged (this indicates the arena capacity should be increased).

use std::alloc::{self, Layout};
use std::ptr::NonNull;

/// Default arena capacity: 1 MiB.
pub const DEFAULT_ARENA_CAPACITY: usize = 1024 * 1024;

/// A simple bump allocator backed by a pre-allocated slab.
///
/// Not thread-safe — designed for single-reactor ownership.
pub struct ArenaAllocator {
    /// The backing slab; allocations are aligned from its actual base pointer.
    slab: Vec<u8>,
    /// Current allocation offset into `slab`.
    offset: usize,
    /// Overflow allocations (when slab is exhausted).
    overflow: Vec<OverflowAllocation>,
}

struct OverflowAllocation {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl OverflowAllocation {
    fn new(layout: Layout) -> Self {
        // SAFETY: `layout` comes from the caller and is non-zero sized by the
        // public allocator check before this cold fallback path is used.
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        let Some(ptr) = NonNull::new(ptr) else {
            alloc::handle_alloc_error(layout);
        };
        Self { ptr, layout }
    }

    #[inline]
    fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for OverflowAllocation {
    fn drop(&mut self) {
        // SAFETY: `ptr` was allocated with this exact layout in `new`.
        unsafe {
            alloc::dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
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
    /// falls back to an aligned heap allocation and logs a warning.
    ///
    /// The returned pointer is valid until [`reset()`](Self::reset) is called.
    /// The caller must not use the pointer after reset. The pointer satisfies
    /// the size and alignment requested by `layout`.
    ///
    /// # Panics
    ///
    /// Panics if `layout.size() == 0`.
    pub fn alloc(&mut self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        let align = layout.align();
        assert!(size > 0, "arena allocations must be non-zero sized");

        // Compute aligned address from the absolute pointer, not the offset,
        // because the Vec<u8> backing has no alignment guarantees beyond 1.
        let base = self.slab.as_mut_ptr() as usize;
        let maybe_slab_allocation = base
            .checked_add(self.offset)
            .and_then(|current| current.checked_add(align - 1))
            .map(|addr| addr & !(align - 1))
            .and_then(|aligned_addr| aligned_addr.checked_sub(base))
            .and_then(|aligned_offset| {
                aligned_offset
                    .checked_add(size)
                    .map(|new_offset| (aligned_offset, new_offset))
            });

        if let Some((aligned_offset, new_offset)) = maybe_slab_allocation
            && new_offset <= self.slab.len()
        {
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
            let allocation = OverflowAllocation::new(layout);
            let ptr = allocation.as_mut_ptr();
            self.overflow.push(allocation);
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
    fn overflow_respects_requested_alignment() {
        let mut arena = ArenaAllocator::new(1);
        let layout = Layout::from_size_align(32, 64).unwrap();
        let ptr = arena.alloc(layout);
        assert_eq!(ptr as usize % 64, 0, "overflow allocation must align");
        assert_eq!(arena.overflow_count(), 1);
    }

    #[test]
    #[should_panic(expected = "arena allocations must be non-zero sized")]
    fn zero_sized_allocations_are_rejected() {
        let mut arena = ArenaAllocator::new(64);
        let layout = Layout::from_size_align(0, 1).unwrap();
        let _ = arena.alloc(layout);
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
