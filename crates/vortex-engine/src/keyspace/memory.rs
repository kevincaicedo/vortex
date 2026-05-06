use std::sync::atomic::Ordering;

use super::{ConcurrentKeyspace, EvictedKeys};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProjectedDelta(isize);

impl ProjectedDelta {
    #[inline]
    pub(crate) const fn from_bytes(bytes: isize) -> Self {
        Self(bytes)
    }

    #[inline]
    pub(crate) const fn positive(self) -> PositiveDelta {
        if self.0 > 0 {
            PositiveDelta(self.0 as usize)
        } else {
            PositiveDelta::zero()
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PositiveDelta(usize);

impl PositiveDelta {
    #[inline]
    pub(crate) const fn zero() -> Self {
        Self(0)
    }

    #[inline]
    pub(crate) const fn from_bytes(bytes: usize) -> Self {
        Self(bytes)
    }

    #[inline]
    pub(crate) const fn bytes(self) -> usize {
        self.0
    }

    #[inline]
    pub(crate) const fn is_zero(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub(crate) fn sum<I>(deltas: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        Self(deltas.into_iter().map(Self::bytes).sum::<usize>())
    }
}

/// RAII guard for a memory reservation. Automatically releases the reserved
/// bytes from the global reservation counter on drop, preventing leaks on
/// error paths. The caller must call `settle()` after mutation to adjust
/// the reservation to the actual memory delta.
pub(crate) struct MemoryReservation<'a> {
    keyspace: &'a ConcurrentKeyspace,
    reserved_bytes: usize,
}

impl std::fmt::Debug for MemoryReservation<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryReservation")
            .field("reserved_bytes", &self.reserved_bytes)
            .finish()
    }
}

impl<'a> MemoryReservation<'a> {
    #[inline]
    pub(crate) fn new(keyspace: &'a ConcurrentKeyspace, reserved_bytes: usize) -> Self {
        Self {
            keyspace,
            reserved_bytes,
        }
    }

    #[inline]
    pub(crate) const fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }

    #[inline]
    pub(crate) fn absorb(&mut self, mut other: MemoryReservation<'a>) {
        debug_assert!(std::ptr::eq(self.keyspace, other.keyspace));
        self.reserved_bytes = self
            .reserved_bytes
            .checked_add(other.reserved_bytes)
            .expect("reservation bytes should not overflow usize");
        other.reserved_bytes = 0;
    }

    /// Settle the reservation: release the reserved bytes from the
    /// reservation counter. Should be called after the mutation has
    /// committed and the actual delta is reflected in `global_memory_used`.
    #[inline]
    pub(crate) fn settle(mut self) {
        if self.reserved_bytes != 0 {
            self.keyspace
                .memory_reserved
                .fetch_sub(self.reserved_bytes, Ordering::Release);
            self.reserved_bytes = 0;
        }
    }
}

impl Drop for MemoryReservation<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.reserved_bytes != 0 {
            self.keyspace
                .memory_reserved
                .fetch_sub(self.reserved_bytes, Ordering::Release);
        }
    }
}

#[derive(Debug)]
pub(crate) struct EvictionAdmissionError {
    pub(crate) response: &'static [u8],
    pub(crate) evicted: EvictedKeys,
}

impl EvictionAdmissionError {
    #[inline]
    pub(super) fn new(response: &'static [u8], evicted: EvictedKeys) -> Self {
        Self { response, evicted }
    }
}
