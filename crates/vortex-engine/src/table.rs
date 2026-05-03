//! SIMD-probed Swiss Table hash map.
//!
//! Open-addressing hash table where each "group" of 16 slots has a 16-byte
//! control array fitting in one SSE2/NEON register. A lookup hashes the key,
//! loads the control bytes, broadcasts the H₂ fingerprint, and does a single
//! SIMD compare to find all matching slots in one instruction.
//!
//! Control byte encoding:
//!   EMPTY   = 0xFF  (never written)
//!   DELETED = 0x80  (tombstone)
//!   H₂      = 0x81..=0xFE  (hash fingerprint)
//!
//! Probing: triangular — `pos = (pos + step); step += 1` mod num_groups.
//! With power-of-two groups this visits every group before cycling.

use std::alloc::{self, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

use ahash::RandomState;
use vortex_common::{VortexKey, VortexValue};

use crate::entry::{CTRL_DELETED, CTRL_EMPTY, Entry};

// ── Architecture SIMD imports ───────────────────────────────────────

#[cfg(all(feature = "simd", target_arch = "x86_64"))]
use core::arch::x86_64::{
    __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
};

#[cfg(all(feature = "simd", target_arch = "aarch64"))]
use std::simd::{Simd, cmp::SimdPartialEq};

// ── Constants ───────────────────────────────────────────────────────

/// Slots per group (one SSE2/NEON register width).
const GROUP_SIZE: usize = 16;

/// Load factor = 7/8 = 87.5%. Resize when `occupied >= capacity * 7 / 8`.
const LOAD_FACTOR_N: usize = 7;
const LOAD_FACTOR_D: usize = 8;

/// Minimum allocation is 1 group (16 slots).
const MIN_GROUPS: usize = 1;

/// Per-shard flush threshold for global memory accounting.
pub(crate) const MEMORY_ACCOUNTING_FLUSH_THRESHOLD: usize = 16 * 1024;

/// Extract the 7-bit H₂ fingerprint from a 64-bit hash.
///
/// Result is in `0x81..=0xFE` — never `EMPTY` (0xFF) or `DELETED` (0x80).
#[inline(always)]
fn h2_from_hash(hash: u64) -> u8 {
    let raw = ((hash >> 57) as u8) | 0x80; // Set high bit to avoid EMPTY=0xFF
    match raw {
        CTRL_DELETED => 0x81, // Remap DELETED to 0x81 to avoid collision with H₂=0x80.
        CTRL_EMPTY => 0xFE,   // Remap EMPTY to 0xFE to avoid collision with H₂=0xFF.
        _ => raw,
    }
}

/// Extract H₁ — the low bits used for group indexing.
#[inline(always)]
const fn h1_from_hash(hash: u64) -> usize {
    hash as usize
}

/// Bitmask of matching slots within a group (one bit per slot, max 16).
#[derive(Clone, Copy)]
struct BitMask(u16);

impl BitMask {
    #[inline]
    const fn any_set(self) -> bool {
        self.0 != 0
    }

    #[inline]
    fn lowest(self) -> Option<usize> {
        if self.0 == 0 {
            None
        } else {
            Some(self.0.trailing_zeros() as usize)
        }
    }
}

impl Iterator for BitMask {
    type Item = usize;
    #[inline]
    fn next(&mut self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }
        let idx = self.0.trailing_zeros() as usize;
        self.0 &= self.0 - 1;
        Some(idx)
    }
}

/// SIMD operations on a group of 16 control bytes.
struct Group;

impl Group {
    #[inline]
    fn match_h2(ctrl: *const u8, h2: u8) -> BitMask {
        #[cfg(all(feature = "simd", target_arch = "x86_64"))]
        {
            // SAFETY: SSE2 is baseline on x86_64. ctrl valid per caller.
            unsafe {
                let group = _mm_loadu_si128(ctrl.cast::<__m128i>());
                let needle = _mm_set1_epi8(h2 as i8);
                let cmp = _mm_cmpeq_epi8(group, needle);
                BitMask(_mm_movemask_epi8(cmp) as u16)
            }
        }

        #[cfg(all(feature = "simd", target_arch = "aarch64"))]
        {
            // SAFETY: ctrl valid per caller, exactly 16 bytes.
            let slice = unsafe { core::slice::from_raw_parts(ctrl, 16) };
            let group = Simd::<u8, 16>::from_slice(slice);
            let needle = Simd::<u8, 16>::splat(h2);
            BitMask(group.simd_eq(needle).to_bitmask() as u16)
        }

        #[cfg(not(any(
            all(feature = "simd", target_arch = "x86_64"),
            all(feature = "simd", target_arch = "aarch64"),
        )))]
        {
            // SAFETY: ctrl valid per caller.
            unsafe { Self::match_byte_scalar(ctrl, h2) }
        }
    }

    #[inline]
    fn match_empty(ctrl: *const u8) -> BitMask {
        Self::match_h2(ctrl, CTRL_EMPTY)
    }

    #[inline]
    fn match_empty_or_deleted(ctrl: *const u8) -> BitMask {
        let empty = Self::match_h2(ctrl, CTRL_EMPTY);
        let deleted = Self::match_h2(ctrl, CTRL_DELETED);
        BitMask(empty.0 | deleted.0)
    }

    #[allow(dead_code)]
    #[inline]
    fn match_byte_scalar(ctrl: *const u8, byte: u8) -> BitMask {
        let mut mask: u16 = 0;
        for i in 0..GROUP_SIZE {
            // SAFETY: ctrl valid for GROUP_SIZE bytes per caller.
            if unsafe { *ctrl.add(i) } == byte {
                mask |= 1 << i;
            }
        }
        BitMask(mask)
    }
}

/// Triangular probing: pos = (pos + step); step += 1 mod num_groups.
/// With power-of-two groups this visits every group before cycling.
/// Maintains the current probe position and step size.
struct ProbeSeq {
    pos: usize,
    stride: usize,
    mask: usize,
}

impl ProbeSeq {
    #[inline]
    fn new(h1: usize, mask: usize) -> Self {
        Self {
            pos: h1 & mask,
            stride: 0,
            mask,
        }
    }

    #[inline]
    fn advance(&mut self) {
        self.stride += 1;
        self.pos = (self.pos + self.stride) & self.mask;
    }
}

/// Raw storage: contiguous control byte array + entry array.
///
/// Layout: `[ctrl: (num_groups+1)*16 bytes] [pad to 64] [entries: num_groups*16*64 bytes]`
///
/// The extra `+1` group of ctrl bytes is a sentinel mirror of group 0,
/// so SIMD loads at the boundary don't read out of bounds.
struct RawTable {
    ctrl: *mut u8,
    entries: *mut Entry,
    num_groups: usize,
    alloc_size: usize,
}

impl RawTable {
    /// Allocate a new raw table with `num_groups` groups (must be power of two).
    /// Control bytes are initialized to `CTRL_EMPTY`, and entries are initialized to empty.
    ///
    /// # Safety
    /// `num_groups` must be a power of two to ensure correct probing and sentinel mirroring.
    ///
    /// # Panics
    /// Panics if `num_groups` is not a power of two or if memory allocation fails.
    fn allocate(num_groups: usize) -> Self {
        debug_assert!(num_groups.is_power_of_two());

        let ctrl_size = (num_groups + 1) * GROUP_SIZE;
        let ctrl_padded = (ctrl_size + 63) & !63;
        let entries_size = num_groups * GROUP_SIZE * size_of::<Entry>();
        let alloc_size = ctrl_padded + entries_size;

        // SAFETY: size > 0, align = 64 (power of two). Layout is valid.
        let layout = Layout::from_size_align(alloc_size, 64).expect("SwissTable: layout overflow");
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }

        let ctrl = ptr;
        // SAFETY: `ctrl_padded <= alloc_size`, so the entry region starts within the
        // allocation and remains aligned because both bases are 64-byte aligned.
        let entries = unsafe { ptr.add(ctrl_padded).cast::<Entry>() };

        let mut raw = Self {
            ctrl,
            entries,
            num_groups,
            alloc_size,
        };
        raw.fill_ctrl(CTRL_EMPTY);

        // Initialize entries to empty.
        let num_slots = num_groups * GROUP_SIZE;
        for i in 0..num_slots {
            // SAFETY: i < num_slots, entries valid for that count.
            unsafe {
                entries.add(i).write(Entry::empty());
            }
        }

        raw
    }

    /// Fill all control bytes (including sentinel) with `byte`.
    fn fill_ctrl(&mut self, byte: u8) {
        // SAFETY: ctrl is valid for the whole control-byte region.
        unsafe {
            core::ptr::write_bytes(self.ctrl, byte, self.ctrl_bytes());
        }
    }

    #[inline]
    fn ctrl_group(&self, group_idx: usize) -> *const u8 {
        debug_assert!(group_idx <= self.num_groups);

        // SAFETY: `group_idx <= num_groups` includes the sentinel group, and each
        // group starts within the control-byte allocation.
        unsafe { self.ctrl.add(group_idx * GROUP_SIZE) }
    }

    #[inline]
    fn ctrl(&self, slot: usize) -> u8 {
        debug_assert!(slot < self.num_slots());

        // SAFETY: live slots are within the primary control-byte array.
        unsafe { *self.ctrl.add(slot) }
    }

    #[inline]
    fn entry(&self, slot: usize) -> &Entry {
        debug_assert!(slot < self.num_slots());

        // SAFETY: `slot < num_slots`, so the element lies within the entry array.
        unsafe { &*self.entries.add(slot) }
    }

    #[inline]
    fn entry_mut(&mut self, slot: usize) -> &mut Entry {
        debug_assert!(slot < self.num_slots());

        // SAFETY: `slot < num_slots` and `&mut self` guarantees exclusive access.
        unsafe { &mut *self.entries.add(slot) }
    }

    #[inline]
    fn entry_ptr(&self, slot: usize) -> *const Entry {
        debug_assert!(slot < self.num_slots());

        // SAFETY: `slot < num_slots`, so the computed pointer stays within the
        // entry array.
        unsafe { self.entries.add(slot) }
    }

    /// Set a control byte + update the sentinel mirror for group 0.
    #[inline]
    fn set_ctrl(&self, slot: usize, ctrl: u8) {
        debug_assert!(slot < self.num_slots());

        // SAFETY: `slot < num_slots`, and the mirrored sentinel slot also lies within
        // the control-byte allocation.
        unsafe {
            *self.ctrl.add(slot) = ctrl;
            // Mirror: if this slot is in the first group, also write to sentinel.
            if slot < GROUP_SIZE {
                let mirror = self.num_groups * GROUP_SIZE + slot;
                *self.ctrl.add(mirror) = ctrl;
            }
        }
    }

    #[inline]
    const fn num_slots(&self) -> usize {
        self.num_groups * GROUP_SIZE
    }

    #[inline]
    const fn ctrl_bytes(&self) -> usize {
        (self.num_groups + 1) * GROUP_SIZE
    }
}

impl Drop for RawTable {
    fn drop(&mut self) {
        // SAFETY: `allocate` created this allocation with the same size/alignment,
        // and `RawTable` is its unique owner.
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.alloc_size, 64);
            alloc::dealloc(self.ctrl, layout);
        }
    }
}

// ── SwissTable ──────────────────────────────────────────────────────

/// SIMD-probed open-addressing hash table.
///
/// Stores `VortexKey` → `VortexValue`. Keys are hashed with `ahash`.
/// Small keys (≤24 B) and values (≤16 B) are stored inline in 64-byte
/// cache-line-aligned entries. A parallel `values` array stores owned
/// `VortexValue` for borrow semantics (`get() -> Option<&VortexValue>`).
/// A parallel `keys` array stores full `VortexKey` for keys >24 bytes.
pub struct SwissTable {
    raw: RawTable,
    hasher: RandomState,
    /// Parallel key store — needed for keys >24 bytes (heap keys) and
    /// for reconstructing keys during iteration.
    keys: Vec<Option<VortexKey>>,
    /// Parallel value store — `values[slot]` holds the VortexValue for
    /// occupied slots, enabling `&VortexValue` returns.
    values: Vec<Option<VortexValue>>,
    /// Number of live entries (excludes tombstones).
    len: usize,
    /// Number of non-EMPTY slots (live + tombstones). Drives resize.
    occupied: usize,
    /// Exact shard-local memory usage for live entries.
    memory_used: usize,
    /// Pending delta not yet flushed to the global atomic.
    memory_drift: isize,
}

impl SwissTable {
    /// Creates a new empty table (minimum 16 slots).
    pub fn new() -> Self {
        Self::with_capacity_and_hasher(GROUP_SIZE, RandomState::new())
    }

    /// Creates a new empty table with an explicit shared hasher.
    pub fn with_hasher(hasher: RandomState) -> Self {
        Self::with_capacity_and_hasher(GROUP_SIZE, hasher)
    }

    /// Creates a new table pre-sized for `cap` entries (respecting load factor).
    pub fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_hasher(cap, RandomState::new())
    }

    /// Creates a new table pre-sized for `cap` entries with an explicit hasher.
    /// `cap` is the expected number of live entries; actual allocation accounts for
    /// load factor and rounds up to the next power-of-two group count.
    ///
    /// # Panics
    /// Panics if `cap` is so large that the required allocation size exceeds `usize::MAX`.
    pub fn with_capacity_and_hasher(cap: usize, hasher: RandomState) -> Self {
        let min_slots = if cap == 0 { GROUP_SIZE } else { cap };
        let required = min_slots
            .checked_mul(LOAD_FACTOR_D)
            .expect("capacity overflow")
            / LOAD_FACTOR_N;
        let num_groups = required
            .div_ceil(GROUP_SIZE)
            .next_power_of_two()
            .max(MIN_GROUPS);
        let num_slots = num_groups * GROUP_SIZE;

        Self {
            raw: RawTable::allocate(num_groups),
            hasher,
            keys: vec![None; num_slots],
            values: vec![None; num_slots],
            len: 0,
            occupied: 0,
            memory_used: 0,
            memory_drift: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    // ── Iteration ───────────────────────────────────────────────────

    /// Iterate over all live `(&VortexKey, &VortexValue)` pairs,
    /// reconstructed from entries and the value store.
    ///
    /// Note: Returns `(VortexKey, &VortexValue)` — the key is read from the
    /// slot-owned key store.
    ///
    /// # Examples
    /// ```rust
    /// use vortex_common::{VortexKey, VortexValue};
    ///
    /// let mut table = vortex_engine::SwissTable::new();
    /// table.insert(VortexKey::from("key1"), VortexValue::from("value1"));
    /// table.insert(VortexKey::from("key2"), VortexValue::from("value2"));
    /// let mut iterable = table.iter();
    /// assert!(iterable.next().is_some());
    /// assert!(iterable.next().is_some());
    /// assert!(iterable.next().is_none());
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = (&VortexKey, &VortexValue)> {
        let num_slots = self.raw.num_slots();
        (0..num_slots).filter_map(move |slot| {
            let ctrl = self.raw.ctrl(slot);
            if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
                return None;
            }
            let key = self.keys[slot].as_ref()?;
            let value = self.values[slot].as_ref()?;
            Some((key, value))
        })
    }

    /// Iterate over live entries (low-level access to the 64-byte `Entry`).
    /// Note: This is a lower-level API that exposes the raw `Entry` struct, which contains
    /// the inline key/value bytes and metadata. It does not reconstruct `VortexKey` or `VortexValue`
    /// from the parallel stores, so it is the caller's responsibility to interpret the entry correctly.
    pub fn iter_entries(&self) -> impl Iterator<Item = &Entry> {
        let num_slots = self.raw.num_slots();
        (0..num_slots).filter_map(move |slot| {
            let ctrl = self.raw.ctrl(slot);
            if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
                return None;
            }
            Some(self.raw.entry(slot))
        })
    }

    #[inline]
    pub fn memory_used(&self) -> usize {
        self.memory_used
    }

    #[inline]
    pub fn memory_drift(&self) -> isize {
        self.memory_drift
    }

    /// Flushes local memory drift to the shared counter if the threshold is exceeded or if `force` is true.
    /// If `force` is false, flushes only if the absolute drift exceeds `MEMORY_ACCOUNTING_FLUSH_THRESHOLD`.
    ///
    /// # Panics
    ///
    /// Panics if the global memory used counter overflows `usize::MAX` when applying a positive drift.
    #[inline]
    pub fn flush_memory_drift_with(&mut self, memory_used: &AtomicUsize, force: bool) {
        let drift = self.memory_drift;
        if drift == 0 {
            return;
        }

        if !force && drift.unsigned_abs() <= MEMORY_ACCOUNTING_FLUSH_THRESHOLD {
            return;
        }

        if drift > 0 {
            memory_used.fetch_add(drift as usize, Ordering::Relaxed);
        } else {
            let bytes = drift.unsigned_abs();
            let _ = memory_used.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            });
        }

        self.memory_drift = 0;
    }

    // ── Core operations ─────────────────────────────────────────────

    /// Insert a key-value pair. Returns the old value if the key existed.
    /// Resizes if `occupied` exceeds the load factor threshold. Probes for the key;
    ///
    /// # Panics
    /// Panics if the new allocation during resize exceeds `usize::MAX` or
    /// if the key/value memory usage causes overflow in memory accounting.
    pub fn insert(&mut self, key: VortexKey, value: VortexValue) -> Option<VortexValue> {
        self.insert_with_lsn(key, value, None)
    }

    /// Inserts a key-value pair and records `lsn` in the entry metadata.
    #[inline]
    pub fn insert_with_lsn(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        lsn: Option<u64>,
    ) -> Option<VortexValue> {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let h2 = h2_from_hash(hash);

        if let Some(slot) = self.find_slot(key_bytes, hash) {
            let old_bytes = self.slot_memory_usage(slot);
            let ttl = self.raw.entry(slot).ttl_deadline();
            let previous = self.replace_slot(slot, h2, key, value, ttl, lsn);
            let new_bytes = self.slot_memory_usage(slot);
            self.record_memory_delta(new_bytes as isize - old_bytes as isize);
            return previous;
        }

        let slot = self.find_insert_slot(hash);
        let was_empty = self.raw.ctrl(slot) == CTRL_EMPTY;

        self.write_new_slot(slot, h2, key, value, 0, lsn);

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(self.slot_memory_usage(slot) as isize);
        None
    }

    /// Get a reference to the value for a key.
    /// Probes for the key and returns `Some(&VortexValue)` if found, or `None` if not found.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use vortex_common::{VortexKey, VortexValue};
    ///
    /// let mut table = vortex_engine::SwissTable::new();
    /// table.insert(VortexKey::from("key1"), VortexValue::from("value1"));
    /// assert_eq!(table.get(&VortexKey::from("key1")), Some(&VortexValue::from("value1")));
    /// assert_eq!(table.get(&VortexKey::from("key2")), None);
    /// ```
    #[inline]
    pub fn get(&self, key: &VortexKey) -> Option<&VortexValue> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let slot = self.find_slot(key_bytes, hash)?;
        self.values[slot].as_ref()
    }

    /// Remove a key and return its value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use vortex_common::{VortexKey, VortexValue};
    ///
    /// let mut table = vortex_engine::SwissTable::new();
    /// table.insert(VortexKey::from("key1"), VortexValue::from("value1"));
    /// assert_eq!(table.remove(&VortexKey::from("key1")), Some(VortexValue::from("value1")));
    /// assert_eq!(table.remove(&VortexKey::from("key2")), None);
    /// ```
    #[inline]
    pub fn remove(&mut self, key: &VortexKey) -> Option<VortexValue> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let slot = self.find_slot(key_bytes, hash)?;
        self.delete_slot(slot)
    }

    /// Returns `true` if the key exists.
    ///
    /// # Examples
    /// ```rust
    /// use vortex_common::{VortexKey, VortexValue};
    ///
    /// let mut table = vortex_engine::SwissTable::new();
    /// table.insert(VortexKey::from("key1"), VortexValue::from("value1"));
    /// assert!(table.contains_key(&VortexKey::from("key1")));
    /// assert!(!table.contains_key(&VortexKey::from("key2")));
    /// ```
    #[inline]
    pub fn contains_key(&self, key: &VortexKey) -> bool {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        self.find_slot(key_bytes, hash).is_some()
    }

    // ── Cursor / scan helpers / helpers ──────────────────────────────────

    /// Total number of slots in the table (always a power of two).
    /// Includes empty, deleted, and occupied slots. Useful for cursor bounds and iteration.
    ///
    /// # Examples
    /// ```rust
    /// use vortex_common::{VortexKey, VortexValue};
    ///
    /// let mut table = vortex_engine::SwissTable::new();
    /// table.insert(VortexKey::from("key1"), VortexValue::from("value1"));
    /// assert_eq!(table.total_slots(), 32);
    /// ```
    #[inline]
    pub fn total_slots(&self) -> usize {
        self.raw.num_slots()
    }

    /// Returns `(key, value)` at `slot` if occupied, else `None`.
    ///
    /// Note: This is a low-level API that directly accesses the slot by index.
    /// It checks the control byte to determine if the slot is occupied, and if so,
    /// retrieves the key and value from the parallel stores.
    /// The caller must ensure that `slot` is within bounds (0 ≤ slot < total_slots()).
    #[inline]
    pub fn slot_key_value(&self, slot: usize) -> Option<(&VortexKey, &VortexValue)> {
        if slot >= self.raw.num_slots() {
            return None;
        }

        let ctrl = self.raw.ctrl(slot);
        if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
            return None;
        }

        let key = self.keys[slot].as_ref()?;
        let value = self.values[slot].as_ref()?;
        Some((key, value))
    }

    /// Returns the low-level entry at `slot` when the slot is live.
    ///
    /// Note: This is a low-level API that directly accesses the slot by index.
    /// It checks the control byte to determine if the slot is occupied, and if so,
    /// returns a reference to the raw `Entry` struct. The caller must ensure that
    /// `slot` is within bounds (0 ≤ slot < total_slots()).
    #[inline]
    pub fn slot_entry(&self, slot: usize) -> Option<&Entry> {
        if slot >= self.raw.num_slots() {
            return None;
        }

        let ctrl = self.raw.ctrl(slot);
        if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
            return None;
        }

        Some(self.raw.entry(slot))
    }

    /// Deletes a live slot by index and returns its stored value.
    #[inline]
    pub fn delete_slot(&mut self, slot: usize) -> Option<VortexValue> {
        if slot >= self.raw.num_slots() {
            return None;
        }

        let bytes = self.slot_memory_usage(slot);
        let entry = self.raw.entry_mut(slot);
        entry.mark_deleted();
        self.raw.set_ctrl(slot, CTRL_DELETED);

        self.len -= 1;
        self.record_memory_delta(-(bytes as isize));
        self.keys[slot] = None;
        self.values[slot].take()
    }

    /// Returns the memory usage of `slot`, or `0` when the slot is not live.
    ///
    /// Note: This is a low-level API that directly accesses the slot by index.
    /// It checks the control byte to determine if the slot is occupied, and if so,
    /// calculates the memory usage of the key and value stored in the slot.
    /// The caller must ensure that `slot` is within bounds (0 ≤ slot < total_slots()).
    #[inline]
    pub fn slot_memory_bytes(&self, slot: usize) -> usize {
        if slot >= self.raw.num_slots() {
            return 0;
        }

        let ctrl = self.raw.ctrl(slot);
        if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
            return 0;
        }

        self.slot_memory_usage(slot)
    }

    /// Expose the hash function for external callers (e.g. ExpiryWheel).
    #[inline]
    pub fn hash_key_bytes(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Estimates the local-memory delta of inserting or replacing `key` with `value`.
    #[inline]
    pub fn projected_insert_delta(&self, key: &VortexKey, value: &VortexValue) -> isize {
        let hash = self.hash_key(key.as_bytes());
        self.projected_insert_delta_prehashed(key, value, hash)
    }

    // ── TTL operations ─────────────────────────────────────────────

    /// Insert a key-value pair with an explicit TTL deadline and optional LSN version.
    /// Returns the old value if the key existed.
    ///
    /// # Examples
    /// ```rust
    /// use vortex_common::{VortexKey, VortexValue};
    ///
    /// let mut table = vortex_engine::SwissTable::new();
    /// table.insert_with(
    ///    VortexKey::from("key1"),
    ///    VortexValue::from("value1"),
    ///    1234567890,
    ///    None
    /// );
    /// assert_eq!(table.get(&VortexKey::from("key1")), Some(&VortexValue::from("value1")));
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the new allocation during resize exceeds `usize::MAX` or
    /// if the key/value memory usage causes overflow in memory accounting.
    pub fn insert_with(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        ttl_deadline: u64,
        lsn: Option<u64>,
    ) -> Option<VortexValue> {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let h2 = h2_from_hash(hash);

        // Phase 1: probe for existing key.
        if let Some(slot) = self.find_slot(key_bytes, hash) {
            let old_bytes = self.slot_memory_usage(slot);
            let previous = self.replace_slot(slot, h2, key, value, ttl_deadline, lsn);
            let new_bytes = self.slot_memory_usage(slot);
            self.record_memory_delta(new_bytes as isize - old_bytes as isize);
            return previous;
        }

        // Phase 2: key not found — insert into first available slot.
        let slot = self.find_insert_slot(hash);
        let was_empty = self.raw.ctrl(slot) == CTRL_EMPTY;

        self.write_new_slot(slot, h2, key, value, ttl_deadline, lsn);

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(self.slot_memory_usage(slot) as isize);
        None
    }

    /// Returns the TTL deadline (nanos) for the entry at `slot`, or 0 if
    /// the slot is empty/deleted or has no TTL.
    #[inline]
    pub fn slot_entry_ttl(&self, slot: usize) -> u64 {
        if slot >= self.raw.num_slots() {
            return 0;
        }

        let ctrl = self.raw.ctrl(slot);
        if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
            return 0;
        }

        self.raw.entry(slot).ttl_deadline()
    }

    /// Remove a key and return `(value, ttl_deadline)`.
    ///
    /// # Examples
    /// ```rust
    /// use vortex_common::{VortexKey, VortexValue};
    ///
    /// let mut table = vortex_engine::SwissTable::new();
    /// table.insert_with(
    ///     VortexKey::from("key1"),
    ///     VortexValue::from("value1"),
    ///     1234567890,
    ///     None
    /// );
    /// assert_eq!(table.remove_with_ttl(&VortexKey::from("key1")), Some((VortexValue::from("value1"), 1234567890)));
    /// assert_eq!(table.remove_with_ttl(&VortexKey::from("key2")), None);
    /// ```
    pub fn remove_with_ttl(&mut self, key: &VortexKey) -> Option<(VortexValue, u64)> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        self.remove_with_ttl_prehashed(key_bytes, hash)
    }

    /// Get `(&value, ttl_deadline)` for a key (no expiry check).
    ///
    /// # Examples
    /// ```rust
    /// use vortex_common::{VortexKey, VortexValue};
    ///
    /// let mut table = vortex_engine::SwissTable::new();
    /// table.insert_with(
    ///     VortexKey::from("key1"),
    ///     VortexValue::from("value1"),
    ///     1234567890,
    ///     None
    /// );
    /// assert_eq!(table.get_with_ttl(&VortexKey::from("key1")), Some((&VortexValue::from("value1"), 1234567890)));
    /// assert_eq!(table.get_with_ttl(&VortexKey::from("key2")), None);
    /// ```
    pub fn get_with_ttl(&self, key: &VortexKey) -> Option<(&VortexValue, u64)> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let slot = self.find_slot(key_bytes, hash)?;
        let ttl = self.raw.entry(slot).ttl_deadline();
        let value = self.values[slot].as_ref()?;
        Some((value, ttl))
    }

    /// Set or update the TTL deadline on an existing key.
    /// Returns `true` if the key was found and updated.
    ///
    /// # Examples
    /// ```rust
    /// use vortex_common::{VortexKey, VortexValue};
    ///
    /// let mut table = vortex_engine::SwissTable::new();
    /// table.insert(VortexKey::from("key1"), VortexValue::from("value1"));
    /// assert!(table.set_entry_ttl(&VortexKey::from("key1"), 1234567890));
    /// assert!(!table.set_entry_ttl(&VortexKey::from("key2"), 1234567890));
    /// ```
    pub fn set_entry_ttl(&mut self, key: &VortexKey, deadline_nanos: u64) -> bool {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return false;
        };

        let entry = self.raw.entry_mut(slot);
        entry.set_ttl(deadline_nanos);
        true
    }

    /// Clear TTL from an existing key (PERSIST).
    /// Returns `true` if the key was found and had a TTL.
    pub fn clear_entry_ttl(&mut self, key: &VortexKey) -> bool {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return false;
        };

        let entry = self.raw.entry_mut(slot);
        let had_ttl = entry.ttl_deadline() != 0;
        entry.clear_ttl();
        had_ttl
    }

    /// Get the TTL deadline of a key, or `None` if the key doesn't exist.
    /// Returns `0` if the key has no TTL.
    pub fn get_entry_ttl(&self, key: &VortexKey) -> Option<u64> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let slot = self.find_slot(key_bytes, hash)?;
        Some(self.raw.entry(slot).ttl_deadline())
    }
}

/**
 * Internal helper methods for slot management, memory accounting, and entry writing.
 **/
impl SwissTable {
    /// Applies a shard-local memory delta without immediately touching the global counter.
    #[inline]
    fn record_memory_delta(&mut self, delta: isize) {
        if delta >= 0 {
            self.memory_used = self.memory_used.saturating_add(delta as usize);
        } else {
            self.memory_used = self.memory_used.saturating_sub(delta.unsigned_abs());
        }

        self.memory_drift += delta;
    }

    #[inline]
    const fn capacity(&self) -> usize {
        self.raw.num_slots()
    }

    #[inline]
    const fn growth_limit(&self) -> usize {
        self.capacity() * LOAD_FACTOR_N / LOAD_FACTOR_D
    }

    #[inline]
    fn entry_memory_usage(key: &VortexKey, value: &VortexValue) -> usize {
        size_of::<Entry>() + key.memory_usage() + value.memory_usage()
    }

    #[inline]
    fn value_from_bytes_reusing(previous: VortexValue, bytes: &[u8]) -> VortexValue {
        if bytes.len() <= vortex_common::MAX_INLINE_VALUE_LEN {
            return VortexValue::from_bytes(bytes);
        }

        if let VortexValue::String(existing) = previous {
            match existing.try_into_mut() {
                Ok(mut buffer) if buffer.capacity() >= bytes.len() => {
                    buffer.clear();
                    buffer.extend_from_slice(bytes);
                    return VortexValue::String(buffer.freeze());
                }
                Ok(_) | Err(_) => {}
            }
        }

        VortexValue::from_bytes(bytes)
    }

    #[inline]
    fn slot_memory_usage(&self, slot: usize) -> usize {
        let key = self.keys[slot].as_ref().expect("live slot must have key");
        let value = self.values[slot]
            .as_ref()
            .expect("live slot must have value");
        Self::entry_memory_usage(key, value)
    }

    #[inline(always)]
    fn rewrite_slot_entry(&mut self, slot: usize, h2: u8, ttl: u64, lsn: Option<u64>) {
        let key = self.keys[slot].as_ref().expect("live slot must have key");
        let value = self.values[slot]
            .as_ref()
            .expect("live slot must have value");

        let entry = self.raw.entry_mut(slot);
        Self::write_entry(entry, h2, key, value, ttl);
        if let Some(lsn) = lsn {
            entry.set_lsn_version(lsn);
        }
        self.raw.set_ctrl(slot, h2);
    }

    #[inline(always)]
    fn write_new_slot(
        &mut self,
        slot: usize,
        h2: u8,
        key: VortexKey,
        value: VortexValue,
        ttl: u64,
        lsn: Option<u64>,
    ) {
        self.keys[slot] = Some(key);
        self.values[slot] = Some(value);
        self.rewrite_slot_entry(slot, h2, ttl, lsn);
    }

    #[inline(always)]
    fn replace_slot(
        &mut self,
        slot: usize,
        h2: u8,
        key: VortexKey,
        value: VortexValue,
        ttl: u64,
        lsn: Option<u64>,
    ) -> Option<VortexValue> {
        self.keys[slot] = Some(key);
        let previous = self.values[slot].replace(value);
        self.rewrite_slot_entry(slot, h2, ttl, lsn);
        previous
    }

    #[inline(always)]
    fn replace_slot_value(
        &mut self,
        slot: usize,
        h2: u8,
        value: VortexValue,
        ttl: u64,
        lsn: Option<u64>,
    ) -> Option<VortexValue> {
        let previous = self.values[slot].replace(value);
        self.rewrite_slot_entry(slot, h2, ttl, lsn);
        previous
    }

    #[inline]
    fn group_mask(&self) -> usize {
        self.raw.num_groups - 1
    }

    /// Hash key bytes using ahash.
    #[inline]
    fn hash_key(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Find the slot of an existing key, or `None`.
    #[inline]
    fn find_slot(&self, key_bytes: &[u8], hash: u64) -> Option<usize> {
        let h2 = h2_from_hash(hash);
        let h1 = h1_from_hash(hash);
        let mask = self.group_mask();
        let mut probe = ProbeSeq::new(h1, mask);

        loop {
            let ctrl_ptr = self.raw.ctrl_group(probe.pos);
            let matches = Group::match_h2(ctrl_ptr, h2);

            for bit in matches {
                let slot = probe.pos * GROUP_SIZE + bit;
                let entry = self.raw.entry(slot);
                if entry.matches_key(key_bytes) {
                    return Some(slot);
                }
            }

            let empties = Group::match_empty(ctrl_ptr);
            if empties.any_set() {
                return None;
            }

            probe.advance();
        }
    }

    /// Find the first EMPTY or DELETED slot along the probe chain.
    #[inline]
    fn find_insert_slot(&self, hash: u64) -> usize {
        debug_assert!(
            self.occupied < self.capacity(),
            "find_insert_slot requires at least one available slot"
        );

        let h1 = h1_from_hash(hash);
        let mask = self.group_mask();
        let mut probe = ProbeSeq::new(h1, mask);

        loop {
            let ctrl_ptr = self.raw.ctrl_group(probe.pos);
            let candidates = Group::match_empty_or_deleted(ctrl_ptr);
            if let Some(bit) = candidates.lowest() {
                return probe.pos * GROUP_SIZE + bit;
            }
            probe.advance();
        }
    }

    /// Double the table and rehash all live entries. Clears tombstones.
    fn resize(&mut self) {
        let new_num_groups = (self.raw.num_groups * 2).max(MIN_GROUPS);
        let new_num_slots = new_num_groups * GROUP_SIZE;
        let mut new_raw = RawTable::allocate(new_num_groups);
        let new_mask = new_num_groups - 1;

        let old_num_slots = self.raw.num_slots();
        let mut new_keys: Vec<Option<VortexKey>> = vec![None; new_num_slots];
        let mut new_values: Vec<Option<VortexValue>> = vec![None; new_num_slots];

        for slot in 0..old_num_slots {
            let ctrl = self.raw.ctrl(slot);
            if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
                continue;
            }

            let old_entry = self.raw.entry(slot);
            let ttl = old_entry.ttl_deadline();
            let lsn = old_entry.lsn_version();
            let key_bytes = self.keys[slot]
                .as_ref()
                .expect("live slot must have key")
                .as_bytes();
            let hash = self.hash_key(key_bytes);
            let h2 = h2_from_hash(hash);
            let h1 = h1_from_hash(hash);

            // Find empty slot in new table.
            let mut probe = ProbeSeq::new(h1, new_mask);
            let new_slot = loop {
                let gctrl = new_raw.ctrl_group(probe.pos);
                let empties = Group::match_empty(gctrl);
                if let Some(bit) = empties.lowest() {
                    break probe.pos * GROUP_SIZE + bit;
                }
                probe.advance();
            };

            new_keys[new_slot] = self.keys[slot].take();
            new_values[new_slot] = self.values[slot].take();

            let key = new_keys[new_slot]
                .as_ref()
                .expect("rehash slot must have key");
            let value = new_values[new_slot]
                .as_ref()
                .expect("rehash slot must have value");

            let entry = new_raw.entry_mut(new_slot);
            Self::write_entry(entry, h2, key, value, ttl);
            entry.set_lsn_version(lsn);
            new_raw.set_ctrl(new_slot, h2);
        }

        self.raw = new_raw;
        self.keys = new_keys;
        self.values = new_values;
        self.occupied = self.len; // Tombstones are gone.
    }

    /// Write key+value metadata into the 64-byte entry.
    #[inline]
    fn write_entry(entry: &mut Entry, h2: u8, key: &VortexKey, value: &VortexValue, ttl: u64) {
        // SAFETY: the caller passes references to slot-owned key/value data,
        // and the table rewrites entry pointers on overwrite/resize.
        unsafe { entry.write_borrowed(h2, key, value, ttl) };
    }

    /// Records an access against a live slot index.
    #[inline]
    fn record_access_slot(&self, slot: usize, random: u64) -> bool {
        let Some(entry) = self.slot_entry(slot) else {
            return false;
        };
        entry.record_access(random);
        true
    }
}

/**
 * Prehased operations are used by batch pipelines to reduce redundant hashing
 * and key materialization.
 */
impl SwissTable {
    /// Estimates the local-memory delta of inserting or replacing `key` using `hash`.
    #[inline]
    pub fn projected_insert_delta_prehashed(
        &self,
        key: &VortexKey,
        value: &VortexValue,
        hash: u64,
    ) -> isize {
        let new_bytes = Self::entry_memory_usage(key, value) as isize;
        match self.find_slot(key.as_bytes(), hash) {
            Some(slot) => new_bytes - self.slot_memory_usage(slot) as isize,
            None => new_bytes,
        }
    }

    /// Inserts a key known to be absent from the table and records an LSN.
    pub fn insert_new_prehashed_and_lsn(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        hash: u64,
        lsn: u64,
    ) {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        debug_assert_eq!(
            self.hash_key_bytes(key.as_bytes()),
            hash,
            "insert_new_prehashed_and_lsn requires a hash computed for `key`"
        );
        debug_assert!(
            self.find_slot(key.as_bytes(), hash).is_none(),
            "insert_new_prehashed_and_lsn requires an absent key"
        );

        let h2 = h2_from_hash(hash);
        let slot = self.find_insert_slot(hash);
        let was_empty = self.raw.ctrl(slot) == CTRL_EMPTY;

        self.write_new_slot(slot, h2, key, value, 0, Some(lsn));

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(self.slot_memory_usage(slot) as isize);
    }

    /// Same as [`insert_no_ttl_prehashed`](Self::insert_no_ttl_prehashed) but also stores `lsn`.
    #[inline]
    pub fn insert_no_ttl_prehashed_and_lsn(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        hash: u64,
        lsn: u64,
    ) -> (Option<VortexValue>, bool) {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let h2 = h2_from_hash(hash);
        let key_bytes = key.as_bytes();

        if let Some(slot) = self.find_slot(key_bytes, hash) {
            let old_bytes = self.slot_memory_usage(slot);
            let old_ttl = self.raw.entry(slot).ttl_deadline();
            let previous = self.replace_slot(slot, h2, key, value, 0, Some(lsn));
            let new_bytes = self.slot_memory_usage(slot);
            self.record_memory_delta(new_bytes as isize - old_bytes as isize);
            return (previous, old_ttl != 0);
        }

        let slot = self.find_insert_slot(hash);
        let was_empty = self.raw.ctrl(slot) == CTRL_EMPTY;

        self.write_new_slot(slot, h2, key, value, 0, Some(lsn));

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(self.slot_memory_usage(slot) as isize);
        (None, false)
    }

    /// Same as [`insert_no_ttl_bytes_prehashed`](Self::insert_no_ttl_bytes_prehashed) but also stores `lsn`.
    #[inline]
    pub fn insert_no_ttl_bytes_prehashed_and_lsn(
        &mut self,
        key_bytes: &[u8],
        value: VortexValue,
        hash: u64,
        lsn: u64,
    ) -> (Option<VortexValue>, bool) {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let h2 = h2_from_hash(hash);

        if let Some(slot) = self.find_slot(key_bytes, hash) {
            let old_bytes = self.slot_memory_usage(slot);
            let old_ttl = self.raw.entry(slot).ttl_deadline();
            let previous = self.replace_slot_value(slot, h2, value, 0, Some(lsn));
            let new_bytes = self.slot_memory_usage(slot);
            self.record_memory_delta(new_bytes as isize - old_bytes as isize);
            return (previous, old_ttl != 0);
        }

        let slot = self.find_insert_slot(hash);
        let was_empty = self.raw.ctrl(slot) == CTRL_EMPTY;

        self.write_new_slot(slot, h2, VortexKey::from(key_bytes), value, 0, Some(lsn));

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(self.slot_memory_usage(slot) as isize);
        (None, false)
    }

    /// Same as [`insert_no_ttl_raw_value_prehashed`](Self::insert_no_ttl_raw_value_prehashed) but also stores `lsn`.
    #[inline]
    pub fn insert_no_ttl_raw_value_prehashed_and_lsn(
        &mut self,
        key_bytes: &[u8],
        value_bytes: &[u8],
        hash: u64,
        lsn: u64,
    ) -> bool {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let h2 = h2_from_hash(hash);

        if let Some(slot) = self.find_slot(key_bytes, hash) {
            let old_bytes = self.slot_memory_usage(slot);
            let old_ttl = self.raw.entry(slot).ttl_deadline();
            let previous = self.values[slot].take().expect("live slot must have value");
            self.values[slot] = Some(Self::value_from_bytes_reusing(previous, value_bytes));
            self.rewrite_slot_entry(slot, h2, 0, Some(lsn));
            let new_bytes = self.slot_memory_usage(slot);
            self.record_memory_delta(new_bytes as isize - old_bytes as isize);
            return old_ttl != 0;
        }

        let slot = self.find_insert_slot(hash);
        let was_empty = self.raw.ctrl(slot) == CTRL_EMPTY;

        self.write_new_slot(
            slot,
            h2,
            VortexKey::from(key_bytes),
            VortexValue::from_bytes(value_bytes),
            0,
            Some(lsn),
        );

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(self.slot_memory_usage(slot) as isize);
        false
    }

    /// Like [`remove_with_ttl`](Self::remove_with_ttl) but uses raw bytes and a
    /// pre-computed hash so delete-style commands can skip `VortexKey`
    /// materialization on the hot path.
    #[inline]
    pub fn remove_with_ttl_prehashed(
        &mut self,
        key_bytes: &[u8],
        hash: u64,
    ) -> Option<(VortexValue, u64)> {
        let slot = self.find_slot(key_bytes, hash)?;

        let ttl = self.raw.entry(slot).ttl_deadline();

        let value = self.delete_slot(slot)?;
        Some((value, ttl))
    }

    /// Like [`get_with_ttl`](Self::get_with_ttl) but uses raw bytes and a
    /// pre-computed hash so batch pipelines do not rebuild `VortexKey`s or
    /// re-hash the same key on the hot path.
    #[inline]
    pub fn get_with_ttl_prehashed(
        &self,
        key_bytes: &[u8],
        hash: u64,
    ) -> Option<(&VortexValue, u64)> {
        let slot = self.find_slot(key_bytes, hash)?;
        let ttl = self.raw.entry(slot).ttl_deadline();
        let value = self.values[slot].as_ref()?;
        Some((value, ttl))
    }

    /// Like `get_or_expire` but uses a pre-computed hash (for MGET batching).
    pub fn get_or_expire_prehashed(
        &mut self,
        key_bytes: &[u8],
        hash: u64,
        now_nanos: u64,
    ) -> Option<&VortexValue> {
        let slot = self.find_slot(key_bytes, hash)?;

        let entry = self.raw.entry(slot);
        if entry.is_expired(now_nanos) {
            let _ = self.delete_slot(slot);
            return None;
        }

        self.values[slot].as_ref()
    }

    /// Replace an existing value while preserving its current TTL.
    ///
    /// This is the safe mutation path for commands that transform a value
    /// in-place semantically (`INCR`, `APPEND`, etc.). It rewrites the 64-byte
    /// entry metadata and updates memory accounting in the same operation.
    pub fn replace_value_preserving_ttl_prehashed_and_lsn(
        &mut self,
        key_bytes: &[u8],
        value: VortexValue,
        hash: u64,
        lsn: u64,
    ) -> Option<VortexValue> {
        let slot = self.find_slot(key_bytes, hash)?;
        let old_bytes = self.slot_memory_usage(slot);
        let ttl = self.raw.entry(slot).ttl_deadline();
        let h2 = h2_from_hash(hash);
        let previous = self.replace_slot_value(slot, h2, value, ttl, Some(lsn));
        let new_bytes = self.slot_memory_usage(slot);
        self.record_memory_delta(new_bytes as isize - old_bytes as isize);
        previous
    }

    /// Returns the stored LSN/version for `key_bytes` when present.
    #[inline]
    pub fn get_lsn_version_prehashed(&self, key_bytes: &[u8], hash: u64) -> Option<u64> {
        let slot = self.find_slot(key_bytes, hash)?;
        Some(self.raw.entry(slot).lsn_version())
    }

    /// Updates the stored LSN/version for `key_bytes` when present.
    #[inline]
    pub fn set_lsn_version_prehashed(&mut self, key_bytes: &[u8], hash: u64, lsn: u64) -> bool {
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return false;
        };
        let entry = self.raw.entry_mut(slot);
        entry.set_lsn_version(lsn);
        true
    }

    /// Check existence with a pre-computed hash (no rehashing).
    #[inline]
    pub fn contains_key_prehashed(&self, key_bytes: &[u8], hash: u64) -> bool {
        self.find_slot(key_bytes, hash).is_some()
    }

    /// Records an access on a known slot identified by a precomputed hash.
    #[inline]
    pub fn record_access_prehashed(&self, key_bytes: &[u8], hash: u64, random: u64) -> bool {
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return false;
        };
        self.record_access_slot(slot, random)
    }
}

/**
 * Prefetching is an optimization for SwissTable's SIMD probing, which can touch
 * multiple cache lines per operation. By prefetching the control byte group and first entry slot for a given hash.
 */
impl SwissTable {
    /// Prefetch the control byte group **and** first entry slot for a given hash.
    /// Used by MGET/MSET/DEL/EXISTS batch pipelines to hide memory latency.
    ///
    /// # Examples
    /// ```rust
    /// let table = vortex_engine::SwissTable::new();
    /// let hash = table.hash_key_bytes(b"my_key");
    /// table.prefetch_group(hash);
    /// ```
    #[inline]
    pub fn prefetch_group(&self, hash: u64) {
        let h1 = h1_from_hash(hash);
        let group_idx = h1 & self.group_mask();
        // Prefetch the 16-byte control array for this group.
        let ctrl_ptr = self.raw.ctrl_group(group_idx);
        crate::prefetch::prefetch_read(ctrl_ptr);
        // Prefetch the first entry slot in the group (64-byte cache line).
        let entry_ptr = self.raw.entry_ptr(group_idx * GROUP_SIZE);
        crate::prefetch::prefetch_read(entry_ptr);
    }

    /// Prefetch with **write** intent (for insert/delete paths).
    /// Used by batch pipelines to hide latency on insert/delete operations that will write to the first probed slot.
    ///
    /// # Examples
    ///
    /// ```rust
    /// let table = vortex_engine::SwissTable::new();
    /// let hash = table.hash_key_bytes(b"my_key");
    /// table.prefetch_group_write(hash);
    /// ```
    #[inline]
    pub fn prefetch_group_write(&self, hash: u64) {
        let h1 = h1_from_hash(hash);
        let group_idx = h1 & self.group_mask();
        let ctrl_ptr = self.raw.ctrl_group(group_idx);
        crate::prefetch::prefetch_write(ctrl_ptr);
        let entry_ptr = self.raw.entry_ptr(group_idx * GROUP_SIZE);
        crate::prefetch::prefetch_write(entry_ptr);
    }
}

impl Default for SwissTable {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: SwissTable's raw pointers (ctrl, entries) are owned heap allocations
// not shared with any other owner. All stored data (VortexKey, VortexValue, Entry)
// is Send + Sync. The raw pointers exist solely as an implementation detail of
// the custom SIMD-probed hash table layout. When behind a RwLock, the lock
// ensures exclusive write access and shared read access.
unsafe impl Send for SwissTable {}
// SAFETY: Read access to SwissTable through &SwissTable only touches immutable
// data (find_slot, get, contains_key, iter, total_slots). All mutation requires
// &mut SwissTable. Combined with parking_lot::RwLock, this guarantees safety.
unsafe impl Sync for SwissTable {}

#[cfg(test)]
mod safe_slot_access_tests {
    use super::*;

    #[test]
    fn out_of_bounds_slot_access_is_safe() {
        let table = SwissTable::new();
        let slot = table.total_slots();

        assert!(table.slot_key_value(slot).is_none());
        assert!(table.slot_entry(slot).is_none());
        assert_eq!(table.slot_memory_bytes(slot), 0);
        assert_eq!(table.slot_entry_ttl(slot), 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("test_key");
        let val = VortexValue::from("test_value");

        assert!(table.insert(key.clone(), val).is_none());
        assert!(table.contains_key(&key));
        assert_eq!(table.len(), 1);

        let retrieved = table.get(&key).unwrap();
        assert!(matches!(retrieved, VortexValue::InlineString(_)));
    }

    #[test]
    fn remove() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("key");
        table.insert(key.clone(), VortexValue::from(42i64));

        let removed = table.remove(&key);
        assert!(removed.is_some());
        assert!(table.is_empty());
    }

    #[test]
    fn local_memory_accounting_tracks_insert_replace_remove() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("mem-key");

        assert_eq!(table.memory_used(), 0);

        table.insert(key.clone(), VortexValue::from("one"));
        let first = table.memory_used();
        assert!(first > 0);

        table.insert(key.clone(), VortexValue::from_bytes(&[b'x'; 128]));
        let second = table.memory_used();
        assert!(second > first);

        table.remove(&key);
        assert_eq!(table.memory_used(), 0);
    }

    #[test]
    fn local_memory_drift_flushes_in_threshold_chunks() {
        let mut table = SwissTable::new();
        let global = AtomicUsize::new(0);
        let key = VortexKey::from("flush-key");
        let value = VortexValue::from_bytes(&vec![b'x'; 20_000]);

        table.insert(key.clone(), value);
        assert!(table.memory_used() > 0);
        assert_eq!(global.load(Ordering::Relaxed), 0);

        table.flush_memory_drift_with(&global, false);
        let flushed = global.load(Ordering::Relaxed);
        assert!(flushed >= MEMORY_ACCOUNTING_FLUSH_THRESHOLD);
        assert_eq!(table.memory_drift(), 0);

        table.remove(&key);
        table.flush_memory_drift_with(&global, false);
        assert!(global.load(Ordering::Relaxed) < flushed);
    }

    #[test]
    fn overwrite() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("k");
        table.insert(key.clone(), VortexValue::from(1i64));
        let old = table.insert(key.clone(), VortexValue::from(2i64));
        assert_eq!(old, Some(VortexValue::from(1i64)));
        assert_eq!(table.get(&key), Some(&VortexValue::from(2i64)));
    }

    #[test]
    fn h2_never_sentinel() {
        // H₂ must never be EMPTY (0xFF) or DELETED (0x80).
        for i in 0u64..1024 {
            let h2 = h2_from_hash(i << 57);
            assert_ne!(h2, CTRL_EMPTY, "H₂ must not be EMPTY");
            assert_ne!(h2, CTRL_DELETED, "H₂ must not be DELETED");
            assert!(h2 >= 0x81, "H₂ must have high bit set");
        }
    }

    #[test]
    fn many_inserts_trigger_resize() {
        let mut table = SwissTable::new();
        // Insert more than one group's worth of entries.
        for i in 0..200 {
            let key = VortexKey::from(format!("key:{i:04}").as_str());
            table.insert(key, VortexValue::Integer(i));
        }
        assert_eq!(table.len(), 200);

        // Verify all entries are retrievable.
        for i in 0..200 {
            let key = VortexKey::from(format!("key:{i:04}").as_str());
            let val = table.get(&key);
            assert_eq!(val, Some(&VortexValue::Integer(i)), "missing key:{i:04}");
        }
    }

    #[test]
    fn delete_heavy_workload() {
        let mut table = SwissTable::new();
        // Insert 100 entries.
        for i in 0..100 {
            let key = VortexKey::from(format!("k{i}").as_str());
            table.insert(key, VortexValue::Integer(i));
        }
        // Delete even keys.
        for i in (0..100).step_by(2) {
            let key = VortexKey::from(format!("k{i}").as_str());
            assert!(table.remove(&key).is_some());
        }
        assert_eq!(table.len(), 50);

        // Verify odd keys survive.
        for i in (1..100).step_by(2) {
            let key = VortexKey::from(format!("k{i}").as_str());
            assert_eq!(table.get(&key), Some(&VortexValue::Integer(i)));
        }
        // Verify even keys are gone.
        for i in (0..100).step_by(2) {
            let key = VortexKey::from(format!("k{i}").as_str());
            assert!(table.get(&key).is_none());
        }
    }

    #[test]
    fn resize_clears_tombstones() {
        let mut table = SwissTable::with_capacity(16);

        // Fill to trigger resize with some tombstones.
        for i in 0..32 {
            let key = VortexKey::from(format!("k{i}").as_str());
            table.insert(key, VortexValue::Integer(i));
        }
        for i in 0..16 {
            let key = VortexKey::from(format!("k{i}").as_str());
            table.remove(&key);
        }
        // Tombstones exist. Now insert more to trigger resize.
        for i in 32..100 {
            let key = VortexKey::from(format!("k{i}").as_str());
            table.insert(key, VortexValue::Integer(i));
        }
        // After resize, occupied == len (tombstones cleaned).
        assert_eq!(table.len, table.occupied);
    }

    #[test]
    fn large_table_100k() {
        // Under Miri, reduce from 100K to 100 entries — still exercises
        // resize, SIMD probing, and tombstone handling without the ~hours
        // of interpretation overhead.
        let n = if cfg!(miri) { 100 } else { 100_000 };
        let step = if cfg!(miri) { 10 } else { 1000 };
        let mut table = SwissTable::with_capacity(n);
        for i in 0..n {
            let key = VortexKey::from(format!("key:{i:08}").as_str());
            table.insert(key, VortexValue::Integer(i as i64));
        }
        assert_eq!(table.len(), n);

        // Spot-check some keys.
        for i in (0..n).step_by(step) {
            let key = VortexKey::from(format!("key:{i:08}").as_str());
            assert_eq!(table.get(&key), Some(&VortexValue::Integer(i as i64)));
        }

        // Check a miss.
        let missing = VortexKey::from("nonexistent");
        assert!(table.get(&missing).is_none());
    }

    #[test]
    fn iterator_returns_all_live() {
        let mut table = SwissTable::new();
        for i in 0..50 {
            let key = VortexKey::from(format!("k{i}").as_str());
            table.insert(key, VortexValue::Integer(i));
        }
        // Delete some.
        for i in 0..10 {
            let key = VortexKey::from(format!("k{i}").as_str());
            table.remove(&key);
        }
        let count = table.iter().count();
        assert_eq!(count, 40);
    }

    #[test]
    fn replace_value_updates_entry_metadata() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("x");
        let hash = table.hash_key_bytes(key.as_bytes());
        table.insert_with(key.clone(), VortexValue::Integer(10), 123, None);

        table
            .replace_value_preserving_ttl_prehashed_and_lsn(
                key.as_bytes(),
                VortexValue::from("twenty"),
                hash,
                77,
            )
            .expect("key exists");

        assert_eq!(table.get(&key), Some(&VortexValue::from("twenty")));
        assert_eq!(table.get_entry_ttl(&key), Some(123));
    }

    #[test]
    fn empty_table_operations() {
        let table = SwissTable::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
        assert!(table.get(&VortexKey::from("x")).is_none());
        assert!(!table.contains_key(&VortexKey::from("x")));
    }

    #[test]
    fn inline_string_values() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("greeting");
        let val = VortexValue::from("hello, world!");
        table.insert(key.clone(), val.clone());
        assert_eq!(table.get(&key), Some(&val));
    }

    #[test]
    fn insert_after_remove_reuses_tombstone() {
        let mut table = SwissTable::with_capacity(16);
        let key = VortexKey::from("reuse");
        table.insert(key.clone(), VortexValue::Integer(1));
        table.remove(&key);
        assert!(table.is_empty());

        // Re-insert — should reuse the tombstoned slot.
        table.insert(key.clone(), VortexValue::Integer(2));
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(&key), Some(&VortexValue::Integer(2)));
    }

    #[test]
    fn resize_preserves_entry_lsn_versions() {
        let mut table = SwissTable::with_capacity(1);
        let watched = VortexKey::from("watched");
        let watched_hash = table.hash_key_bytes(watched.as_bytes());

        let _ = table.insert_no_ttl_prehashed_and_lsn(
            watched.clone(),
            VortexValue::from("value"),
            watched_hash,
            42,
        );
        assert_eq!(
            table.get_lsn_version_prehashed(watched.as_bytes(), watched_hash),
            Some(42)
        );

        for i in 0..256 {
            let key = VortexKey::from(format!("key:{i:04}").as_str());
            table.insert(key, VortexValue::Integer(i));
        }

        let watched_hash = table.hash_key_bytes(watched.as_bytes());
        assert_eq!(
            table.get_lsn_version_prehashed(watched.as_bytes(), watched_hash),
            Some(42)
        );
    }
}

// ── Property tests (validate against HashMap as oracle) ─────────────
// Excluded from Miri: each proptest case runs 1..500 random operations on the
// Swiss Table. Even 16 cases × 500 ops interpreted by Miri would take hours.
// The deterministic tests above already cover all unsafe code paths.

#[cfg(all(test, not(miri)))]
mod proptests {
    use std::collections::HashMap;

    use proptest::prelude::*;
    use vortex_common::{VortexKey, VortexValue};

    use super::SwissTable;

    /// Generate a random key string 1..=30 bytes.
    fn arb_key() -> impl Strategy<Value = String> {
        "[a-z0-9]{1,30}"
    }

    /// Generate a random integer value.
    fn arb_int_value() -> impl Strategy<Value = i64> {
        proptest::num::i64::ANY
    }

    /// Operations the model can execute.
    #[derive(Clone, Debug)]
    enum Op {
        Insert(String, i64),
        Get(String),
        Remove(String),
        ContainsKey(String),
    }

    fn arb_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (arb_key(), arb_int_value()).prop_map(|(k, v)| Op::Insert(k, v)),
            arb_key().prop_map(Op::Get),
            arb_key().prop_map(Op::Remove),
            arb_key().prop_map(Op::ContainsKey),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// Swiss Table behaves identically to HashMap for any sequence of ops.
        #[test]
        fn swiss_table_matches_hashmap(ops in proptest::collection::vec(arb_op(), 1..500)) {
            let mut st = SwissTable::new();
            let mut hm: HashMap<String, i64> = HashMap::new();

            for op in ops {
                match op {
                    Op::Insert(k, v) => {
                        let st_old = st.insert(VortexKey::from(k.as_str()), VortexValue::Integer(v));
                        let hm_old = hm.insert(k, v);
                        // Both return old value or None.
                        match (st_old, hm_old) {
                            (Some(VortexValue::Integer(a)), Some(b)) => prop_assert_eq!(a, b),
                            (None, None) => {}
                            other => prop_assert!(false, "insert mismatch: {other:?}"),
                        }
                    }
                    Op::Get(k) => {
                        let st_val = st.get(&VortexKey::from(k.as_str()));
                        let hm_val = hm.get(&k);
                        match (st_val, hm_val) {
                            (Some(VortexValue::Integer(a)), Some(b)) => prop_assert_eq!(a, b),
                            (None, None) => {}
                            other => prop_assert!(false, "get mismatch: {other:?}"),
                        }
                    }
                    Op::Remove(k) => {
                        let st_removed = st.remove(&VortexKey::from(k.as_str()));
                        let hm_removed = hm.remove(&k);
                        match (st_removed, hm_removed) {
                            (Some(VortexValue::Integer(a)), Some(b)) => prop_assert_eq!(a, b),
                            (None, None) => {}
                            other => prop_assert!(false, "remove mismatch: {other:?}"),
                        }
                    }
                    Op::ContainsKey(k) => {
                        let st_has = st.contains_key(&VortexKey::from(k.as_str()));
                        let hm_has = hm.contains_key(&k);
                        prop_assert_eq!(st_has, hm_has);
                    }
                }
                // Invariant: lengths always match.
                prop_assert_eq!(st.len(), hm.len());
            }
        }

        /// Every inserted key is retrievable after bulk insert.
        #[test]
        fn bulk_insert_all_retrievable(entries in proptest::collection::vec(
            (arb_key(), arb_int_value()), 1..1000
        )) {
            let mut st = SwissTable::new();
            let mut expected: HashMap<String, i64> = HashMap::new();

            for (k, v) in &entries {
                st.insert(VortexKey::from(k.as_str()), VortexValue::Integer(*v));
                expected.insert(k.clone(), *v);
            }

            prop_assert_eq!(st.len(), expected.len());

            for (k, v) in &expected {
                let result = st.get(&VortexKey::from(k.as_str()));
                prop_assert_eq!(result, Some(&VortexValue::Integer(*v)));
            }
        }

        /// Insert-remove-reinsert cycle preserves correctness.
        #[test]
        fn insert_remove_reinsert(
            keys in proptest::collection::vec(arb_key(), 1..200),
        ) {
            let mut st = SwissTable::new();

            // Insert all.
            for (i, k) in keys.iter().enumerate() {
                st.insert(VortexKey::from(k.as_str()), VortexValue::Integer(i as i64));
            }

            // Remove all.
            for k in &keys {
                st.remove(&VortexKey::from(k.as_str()));
            }
            prop_assert!(st.is_empty());

            // Reinsert all with different values.
            for (i, k) in keys.iter().enumerate() {
                st.insert(VortexKey::from(k.as_str()), VortexValue::Integer(i as i64 + 1000));
            }

            // Verify — use HashMap to get expected deduped set.
            let mut expected: HashMap<String, i64> = HashMap::new();
            for (i, k) in keys.iter().enumerate() {
                expected.insert(k.clone(), i as i64 + 1000);
            }
            prop_assert_eq!(st.len(), expected.len());
            for (k, v) in &expected {
                let result = st.get(&VortexKey::from(k.as_str()));
                prop_assert_eq!(result, Some(&VortexValue::Integer(*v)));
            }
        }
    }
}
