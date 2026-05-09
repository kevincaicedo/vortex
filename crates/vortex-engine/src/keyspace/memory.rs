use std::sync::atomic::{AtomicUsize, Ordering};

use crate::effects::MutationErrorKind;

use super::{ConcurrentKeyspace, EvictedKeys};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct EngineMemoryAttributionSnapshot {
    pub live_keys: usize,
    pub logical_dataset_bytes: usize,
    pub table_allocated_bytes: usize,
    pub table_total_slots: usize,
    pub capacity_slack_slots: usize,
    pub tombstone_slots: usize,
    pub load_factor: f64,
    pub bytes_per_live_key: Option<f64>,
    pub shard_count: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ServerMemoryAttributionSnapshot {
    pub io_fixed_buffer_reserved_bytes: usize,
    pub io_fixed_buffer_committed_bytes: usize,
    pub io_fixed_buffer_active_bytes: usize,
    pub per_connection_state_bytes: usize,
    pub connection_capacity: usize,
    pub fixed_buffer_count: usize,
    pub fixed_buffer_size: usize,
}

#[derive(Debug, Default)]
pub(super) struct ServerMemoryAttribution {
    io_fixed_buffer_reserved_bytes: AtomicUsize,
    io_fixed_buffer_committed_bytes: AtomicUsize,
    io_fixed_buffer_active_bytes: AtomicUsize,
    per_connection_state_bytes: AtomicUsize,
    connection_capacity: AtomicUsize,
    fixed_buffer_count: AtomicUsize,
    fixed_buffer_size: AtomicUsize,
}

impl ServerMemoryAttribution {
    #[inline]
    pub(super) fn store(&self, snapshot: ServerMemoryAttributionSnapshot) {
        self.io_fixed_buffer_reserved_bytes
            .store(snapshot.io_fixed_buffer_reserved_bytes, Ordering::Relaxed);
        self.io_fixed_buffer_committed_bytes
            .store(snapshot.io_fixed_buffer_committed_bytes, Ordering::Relaxed);
        self.io_fixed_buffer_active_bytes
            .store(snapshot.io_fixed_buffer_active_bytes, Ordering::Relaxed);
        self.per_connection_state_bytes
            .store(snapshot.per_connection_state_bytes, Ordering::Relaxed);
        self.connection_capacity
            .store(snapshot.connection_capacity, Ordering::Relaxed);
        self.fixed_buffer_count
            .store(snapshot.fixed_buffer_count, Ordering::Relaxed);
        self.fixed_buffer_size
            .store(snapshot.fixed_buffer_size, Ordering::Relaxed);
    }

    #[inline]
    pub(super) fn snapshot(&self) -> ServerMemoryAttributionSnapshot {
        ServerMemoryAttributionSnapshot {
            io_fixed_buffer_reserved_bytes: self
                .io_fixed_buffer_reserved_bytes
                .load(Ordering::Relaxed),
            io_fixed_buffer_committed_bytes: self
                .io_fixed_buffer_committed_bytes
                .load(Ordering::Relaxed),
            io_fixed_buffer_active_bytes: self.io_fixed_buffer_active_bytes.load(Ordering::Relaxed),
            per_connection_state_bytes: self.per_connection_state_bytes.load(Ordering::Relaxed),
            connection_capacity: self.connection_capacity.load(Ordering::Relaxed),
            fixed_buffer_count: self.fixed_buffer_count.load(Ordering::Relaxed),
            fixed_buffer_size: self.fixed_buffer_size.load(Ordering::Relaxed),
        }
    }
}

impl ConcurrentKeyspace {
    #[inline]
    pub fn set_server_memory_attribution(&self, snapshot: ServerMemoryAttributionSnapshot) {
        self.server_memory_attribution.store(snapshot);
    }

    #[inline]
    pub fn server_memory_attribution(&self) -> ServerMemoryAttributionSnapshot {
        self.server_memory_attribution.snapshot()
    }

    pub fn engine_memory_attribution(&self) -> EngineMemoryAttributionSnapshot {
        let (table_total_slots, table_allocated_bytes, live_keys, occupied_slots) = self
            .scan_all_shards(|_, table| {
                (
                    table.total_slots(),
                    table.allocated_bytes(),
                    table.len(),
                    table.occupied_slots(),
                )
            })
            .into_iter()
            .fold(
                (0usize, 0usize, 0usize, 0usize),
                |(slots_sum, bytes_sum, live_sum, occupied_sum), (slots, bytes, live, occupied)| {
                    (
                        slots_sum.saturating_add(slots),
                        bytes_sum.saturating_add(bytes),
                        live_sum.saturating_add(live),
                        occupied_sum.saturating_add(occupied),
                    )
                },
            );
        let tombstone_slots = occupied_slots.saturating_sub(live_keys);
        let capacity_slack_slots = table_total_slots.saturating_sub(occupied_slots);
        let bytes_per_live_key = if live_keys == 0 {
            None
        } else {
            Some(table_allocated_bytes as f64 / live_keys as f64)
        };
        let load_factor = if table_total_slots == 0 {
            0.0
        } else {
            live_keys as f64 / table_total_slots as f64
        };

        EngineMemoryAttributionSnapshot {
            live_keys,
            logical_dataset_bytes: self.memory_used(),
            table_allocated_bytes,
            table_total_slots,
            capacity_slack_slots,
            tombstone_slots,
            load_factor,
            bytes_per_live_key,
            shard_count: self.num_shards(),
        }
    }
}

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
    pub(crate) kind: MutationErrorKind,
    pub(crate) evicted: EvictedKeys,
}

impl EvictionAdmissionError {
    #[inline]
    pub(super) fn new(kind: MutationErrorKind, evicted: EvictedKeys) -> Self {
        Self { kind, evicted }
    }
}
