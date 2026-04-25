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
pub(crate) const MEMORY_ACCOUNTING_FLUSH_THRESHOLD: isize = 16 * 1024;

// ── H₂ fingerprint ─────────────────────────────────────────────────

/// Extract the 7-bit H₂ fingerprint from a 64-bit hash.
///
/// Result is in `0x81..=0xFE` — never `EMPTY` (0xFF) or `DELETED` (0x80).
#[inline(always)]
fn h2_from_hash(hash: u64) -> u8 {
    let raw = ((hash >> 57) as u8) | 0x81;
    if raw == CTRL_EMPTY { 0xFE } else { raw }
}

/// Extract H₁ — the low bits used for group indexing.
#[inline(always)]
const fn h1_from_hash(hash: u64) -> usize {
    hash as usize
}

// ── BitMask ─────────────────────────────────────────────────────────

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

// ── Group — SIMD probe of 16 control bytes ──────────────────────────

/// SIMD operations on a group of 16 control bytes.
struct Group;

impl Group {
    /// Find slots in a group matching `h2`.
    ///
    /// # Safety
    /// `ctrl` must point to ≥16 readable bytes.
    #[inline]
    unsafe fn match_h2(ctrl: *const u8, h2: u8) -> BitMask {
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

    /// Find EMPTY slots.
    ///
    /// # Safety
    /// `ctrl` must point to ≥16 readable bytes.
    #[inline]
    unsafe fn match_empty(ctrl: *const u8) -> BitMask {
        // SAFETY: caller guarantees ctrl validity.
        unsafe { Self::match_h2(ctrl, CTRL_EMPTY) }
    }

    /// Find EMPTY or DELETED slots (candidates for insertion).
    ///
    /// A byte is empty-or-deleted iff `(byte & 0x7F) == 0`:
    ///   EMPTY=0xFF → 0x7F ≠ 0 — wait, that fails.
    /// Actually: EMPTY=0xFF → 0xFF & 0x7F = 0x7F ≠ 0. So that trick doesn't work.
    /// Instead: a slot is "available" if `byte >= 0x80` AND `(byte == 0x80 || byte == 0xFF)`.
    /// Simpler: just OR the two masks.
    ///
    /// # Safety
    /// `ctrl` must point to ≥16 readable bytes.
    #[inline]
    unsafe fn match_empty_or_deleted(ctrl: *const u8) -> BitMask {
        // SAFETY: caller guarantees ctrl validity.
        unsafe {
            let empty = Self::match_h2(ctrl, CTRL_EMPTY);
            let deleted = Self::match_h2(ctrl, CTRL_DELETED);
            BitMask(empty.0 | deleted.0)
        }
    }

    /// Scalar fallback for `match_h2`.
    ///
    /// # Safety
    /// `ctrl` must point to ≥16 readable bytes.
    #[allow(dead_code)]
    #[inline]
    unsafe fn match_byte_scalar(ctrl: *const u8, byte: u8) -> BitMask {
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

// ── Probe sequence ──────────────────────────────────────────────────

/// Triangular probing: pos = (pos + step); step += 1 mod num_groups.
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

// ── RawTable — allocation of ctrl bytes + entries ───────────────────

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
        let total = (self.num_groups + 1) * GROUP_SIZE;
        // SAFETY: ctrl is valid for `total` bytes.
        unsafe {
            core::ptr::write_bytes(self.ctrl, byte, total);
        }
    }

    #[inline]
    unsafe fn ctrl_group(&self, group_idx: usize) -> *const u8 {
        // SAFETY: group_idx < num_groups+1 (sentinel), pointer arithmetic valid.
        unsafe { self.ctrl.add(group_idx * GROUP_SIZE) }
    }

    #[inline]
    unsafe fn ctrl_at(&self, slot: usize) -> *mut u8 {
        // SAFETY: slot < total ctrl bytes, pointer arithmetic valid.
        unsafe { self.ctrl.add(slot) }
    }

    #[inline]
    unsafe fn entry(&self, slot: usize) -> &Entry {
        // SAFETY: slot < num_slots, entries valid for that count.
        unsafe { &*self.entries.add(slot) }
    }

    #[inline]
    unsafe fn entry_mut(&mut self, slot: usize) -> &mut Entry {
        // SAFETY: slot < num_slots, entries valid for that count, &mut exclusive.
        unsafe { &mut *self.entries.add(slot) }
    }

    /// Set a control byte + update the sentinel mirror for group 0.
    #[inline]
    unsafe fn set_ctrl(&self, slot: usize, ctrl: u8) {
        // SAFETY: slot is valid, ctrl_at returns valid ptr.
        unsafe {
            *self.ctrl_at(slot) = ctrl;
            // Mirror: if this slot is in the first group, also write to sentinel.
            if slot < GROUP_SIZE {
                let mirror = self.num_groups * GROUP_SIZE + slot;
                *self.ctrl_at(mirror) = ctrl;
            }
        }
    }

    /// # Safety
    /// Must only be called once. Table must not be used after.
    unsafe fn dealloc(&self) {
        // SAFETY: layout matches the one used in allocate(). Called once.
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.alloc_size, 64);
            alloc::dealloc(self.ctrl, layout);
        }
    }

    #[inline]
    const fn num_slots(&self) -> usize {
        self.num_groups * GROUP_SIZE
    }
}

// ── SwissTable ──────────────────────────────────────────────────────

/// SIMD-probed open-addressing hash table.
///
/// Stores `VortexKey` → `VortexValue`. Keys are hashed with `ahash`.
/// Small keys (≤23 B) and values (≤21 B) are stored inline in 64-byte
/// cache-line-aligned entries. A parallel `values` array stores owned
/// `VortexValue` for borrow semantics (`get() -> Option<&VortexValue>`).
/// A parallel `keys` array stores full `VortexKey` for keys >23 bytes.
pub struct SwissTable {
    raw: RawTable,
    hasher: RandomState,
    /// Parallel key store — needed for keys >23 bytes (heap keys) and
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
    local_memory_used: usize,
    /// Pending delta not yet flushed to the global atomic.
    local_memory_drift: isize,
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
            local_memory_used: 0,
            local_memory_drift: 0,
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

    #[inline]
    pub fn local_memory_used(&self) -> usize {
        self.local_memory_used
    }

    #[inline]
    pub fn local_memory_drift(&self) -> isize {
        self.local_memory_drift
    }

    #[inline]
    pub fn record_memory_delta(&mut self, delta: isize) {
        if delta >= 0 {
            self.local_memory_used = self.local_memory_used.saturating_add(delta as usize);
        } else {
            self.local_memory_used = self.local_memory_used.saturating_sub((-delta) as usize);
        }
        self.local_memory_drift += delta;
    }

    #[inline]
    pub fn flush_memory_drift(&mut self, global_memory_used: &AtomicUsize) {
        self.flush_memory_drift_with_mode(global_memory_used, false);
    }

    #[inline]
    pub fn flush_memory_drift_force(&mut self, global_memory_used: &AtomicUsize) {
        self.flush_memory_drift_with_mode(global_memory_used, true);
    }

    #[inline]
    fn flush_memory_drift_with_mode(&mut self, global_memory_used: &AtomicUsize, force: bool) {
        let drift = self.local_memory_drift;
        if drift == 0 {
            return;
        }
        if !force && drift.unsigned_abs() <= MEMORY_ACCOUNTING_FLUSH_THRESHOLD as usize {
            return;
        }

        if drift > 0 {
            global_memory_used.fetch_add(drift as usize, Ordering::Relaxed);
        } else {
            let bytes = drift.unsigned_abs();
            let _ =
                global_memory_used.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(bytes))
                });
        }

        self.local_memory_drift = 0;
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.raw.num_slots()
    }

    #[inline]
    fn growth_limit(&self) -> usize {
        self.capacity() * LOAD_FACTOR_N / LOAD_FACTOR_D
    }

    #[inline]
    fn entry_memory_usage(key: &VortexKey, value: &VortexValue) -> usize {
        size_of::<Entry>() + key.memory_usage() + value.memory_usage()
    }

    #[inline]
    fn value_from_bytes_reusing(previous: VortexValue, bytes: &[u8]) -> VortexValue {
        if bytes.len() <= 23 {
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

    #[inline]
    pub fn projected_insert_delta(&self, key: &VortexKey, value: &VortexValue) -> isize {
        let hash = self.hash_key(key.as_bytes());
        self.projected_insert_delta_prehashed(key, value, hash)
    }

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

    pub fn delete_slot(&mut self, slot: usize) -> Option<VortexValue> {
        let bytes = self.slot_memory_usage(slot);

        let entry = unsafe { self.raw.entry_mut(slot) };
        entry.mark_deleted();
        unsafe {
            self.raw.set_ctrl(slot, CTRL_DELETED);
        }

        self.len -= 1;
        self.record_memory_delta(-(bytes as isize));
        self.keys[slot] = None;
        self.values[slot].take()
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

    // ── Core operations ─────────────────────────────────────────────

    /// Insert a key-value pair. Returns the old value if the key existed.
    pub fn insert(&mut self, key: VortexKey, value: VortexValue) -> Option<VortexValue> {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let h2 = h2_from_hash(hash);

        // Phase 1: probe for existing key.
        if let Some(slot) = self.find_slot(key_bytes, hash) {
            let old_bytes = self.slot_memory_usage(slot);
            let ttl = unsafe { self.raw.entry(slot) }.ttl_deadline();
            self.keys[slot] = Some(key);
            let previous = self.values[slot].replace(value);

            let key_ptr =
                self.keys[slot].as_ref().expect("live slot must have key") as *const VortexKey;
            let value_ptr = self.values[slot]
                .as_ref()
                .expect("live slot must have value")
                as *const VortexValue;

            let entry = unsafe { self.raw.entry_mut(slot) };
            // SAFETY: slot ownership remains stable until the next overwrite/resize,
            // and resize rewrites every borrowed pointer into the new table.
            Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, ttl);
            unsafe { self.raw.set_ctrl(slot, h2) };
            let new_bytes = Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe { &*value_ptr });
            self.record_memory_delta(new_bytes as isize - old_bytes as isize);
            return previous;
        }

        // Phase 2: key not found — insert into first available slot.
        let slot = self.find_insert_slot(hash);
        let was_empty = unsafe { *self.raw.ctrl_at(slot) == CTRL_EMPTY };

        self.keys[slot] = Some(key);
        self.values[slot] = Some(value);

        let key_ptr = self.keys[slot].as_ref().expect("new slot must have key") as *const VortexKey;
        let value_ptr = self.values[slot]
            .as_ref()
            .expect("new slot must have value") as *const VortexValue;

        let entry = unsafe { self.raw.entry_mut(slot) };
        // SAFETY: slot ownership remains stable until the next overwrite/resize,
        // and resize rewrites every borrowed pointer into the new table.
        Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, 0);
        unsafe {
            self.raw.set_ctrl(slot, h2);
        }

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe {
            &*value_ptr
        }) as isize);
        None
    }

    /// Insert a key-value pair with TTL=0, returning `(old_value, old_had_ttl)`.
    ///
    /// Fuses the `get_entry_ttl` + `insert` into a single probe sequence,
    /// eliminating redundant table lookups on the SET hot path.
    pub fn insert_no_ttl(
        &mut self,
        key: VortexKey,
        value: VortexValue,
    ) -> (Option<VortexValue>, bool) {
        let hash = self.hash_key(key.as_bytes());
        self.insert_no_ttl_prehashed(key, value, hash)
    }

    /// Same as `insert_no_ttl` but accepts a pre-computed table hash.
    /// Callers can compute the hash BEFORE acquiring the shard write lock
    /// to reduce lock hold time.
    #[inline]
    pub fn insert_no_ttl_prehashed(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        hash: u64,
    ) -> (Option<VortexValue>, bool) {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let h2 = h2_from_hash(hash);
        let key_bytes = key.as_bytes();

        if let Some(slot) = self.find_slot(key_bytes, hash) {
            let old_bytes = self.slot_memory_usage(slot);
            let old_ttl = unsafe { self.raw.entry(slot) }.ttl_deadline();
            self.keys[slot] = Some(key);
            let previous = self.values[slot].replace(value);

            let key_ptr =
                self.keys[slot].as_ref().expect("live slot must have key") as *const VortexKey;
            let value_ptr = self.values[slot]
                .as_ref()
                .expect("live slot must have value")
                as *const VortexValue;

            let entry = unsafe { self.raw.entry_mut(slot) };
            Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, 0);
            unsafe { self.raw.set_ctrl(slot, h2) };
            let new_bytes = Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe { &*value_ptr });
            self.record_memory_delta(new_bytes as isize - old_bytes as isize);
            return (previous, old_ttl != 0);
        }

        let slot = self.find_insert_slot(hash);
        let was_empty = unsafe { *self.raw.ctrl_at(slot) == CTRL_EMPTY };

        self.keys[slot] = Some(key);
        self.values[slot] = Some(value);

        let key_ptr = self.keys[slot].as_ref().expect("new slot must have key") as *const VortexKey;
        let value_ptr = self.values[slot]
            .as_ref()
            .expect("new slot must have value") as *const VortexValue;

        let entry = unsafe { self.raw.entry_mut(slot) };
        Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, 0);
        unsafe {
            self.raw.set_ctrl(slot, h2);
        }

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe {
            &*value_ptr
        }) as isize);
        (None, false)
    }

    /// Plain SET overwrite path using borrowed key bytes.
    ///
    /// Existing-key updates keep the stored key allocation in place and only
    /// replace the value/TTL metadata. Misses allocate an owned `VortexKey` once
    /// at the insertion slot.
    #[inline]
    pub fn insert_no_ttl_bytes_prehashed(
        &mut self,
        key_bytes: &[u8],
        value: VortexValue,
        hash: u64,
    ) -> (Option<VortexValue>, bool) {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let h2 = h2_from_hash(hash);

        if let Some(slot) = self.find_slot(key_bytes, hash) {
            let old_bytes = self.slot_memory_usage(slot);
            let old_ttl = unsafe { self.raw.entry(slot) }.ttl_deadline();
            let previous = self.values[slot].replace(value);

            let key_ptr =
                self.keys[slot].as_ref().expect("live slot must have key") as *const VortexKey;
            let value_ptr = self.values[slot]
                .as_ref()
                .expect("live slot must have value")
                as *const VortexValue;

            let entry = unsafe { self.raw.entry_mut(slot) };
            Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, 0);
            unsafe { self.raw.set_ctrl(slot, h2) };
            let new_bytes = Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe { &*value_ptr });
            self.record_memory_delta(new_bytes as isize - old_bytes as isize);
            return (previous, old_ttl != 0);
        }

        let slot = self.find_insert_slot(hash);
        let was_empty = unsafe { *self.raw.ctrl_at(slot) == CTRL_EMPTY };

        self.keys[slot] = Some(VortexKey::from(key_bytes));
        self.values[slot] = Some(value);

        let key_ptr = self.keys[slot].as_ref().expect("new slot must have key") as *const VortexKey;
        let value_ptr = self.values[slot]
            .as_ref()
            .expect("new slot must have value") as *const VortexValue;

        let entry = unsafe { self.raw.entry_mut(slot) };
        Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, 0);
        unsafe {
            self.raw.set_ctrl(slot, h2);
        }

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe {
            &*value_ptr
        }) as isize);
        (None, false)
    }

    /// Plain SET overwrite path using borrowed key and value bytes.
    ///
    /// Existing heap string allocations are reused when `Bytes` proves unique
    /// ownership and the allocation has enough capacity for the new value.
    /// This removes allocator churn from fixed-size overwrite workloads while
    /// preserving immutable `Bytes` safety for concurrent GET responses.
    #[inline]
    pub fn insert_no_ttl_raw_value_prehashed(
        &mut self,
        key_bytes: &[u8],
        value_bytes: &[u8],
        hash: u64,
    ) -> bool {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let h2 = h2_from_hash(hash);

        if let Some(slot) = self.find_slot(key_bytes, hash) {
            let old_bytes = self.slot_memory_usage(slot);
            let old_ttl = unsafe { self.raw.entry(slot) }.ttl_deadline();
            let previous = self.values[slot].take().expect("live slot must have value");
            self.values[slot] = Some(Self::value_from_bytes_reusing(previous, value_bytes));

            let key_ptr =
                self.keys[slot].as_ref().expect("live slot must have key") as *const VortexKey;
            let value_ptr = self.values[slot]
                .as_ref()
                .expect("live slot must have value")
                as *const VortexValue;

            let entry = unsafe { self.raw.entry_mut(slot) };
            Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, 0);
            unsafe { self.raw.set_ctrl(slot, h2) };
            let new_bytes = Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe { &*value_ptr });
            self.record_memory_delta(new_bytes as isize - old_bytes as isize);
            return old_ttl != 0;
        }

        let slot = self.find_insert_slot(hash);
        let was_empty = unsafe { *self.raw.ctrl_at(slot) == CTRL_EMPTY };

        self.keys[slot] = Some(VortexKey::from(key_bytes));
        self.values[slot] = Some(VortexValue::from_bytes(value_bytes));

        let key_ptr = self.keys[slot].as_ref().expect("new slot must have key") as *const VortexKey;
        let value_ptr = self.values[slot]
            .as_ref()
            .expect("new slot must have value") as *const VortexValue;

        let entry = unsafe { self.raw.entry_mut(slot) };
        Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, 0);
        unsafe {
            self.raw.set_ctrl(slot, h2);
        }

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe {
            &*value_ptr
        }) as isize);
        false
    }

    /// Get a reference to the value for a key.
    pub fn get(&self, key: &VortexKey) -> Option<&VortexValue> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let slot = self.find_slot(key_bytes, hash)?;
        self.values[slot].as_ref()
    }

    /// Get a mutable reference to the value for a key.
    pub fn get_mut(&mut self, key: &VortexKey) -> Option<&mut VortexValue> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let slot = self.find_slot(key_bytes, hash)?;
        self.values[slot].as_mut()
    }

    /// Single-probe upsert: returns `(value_ref, existed)`.
    ///
    /// If the key exists, returns a mutable reference to the existing value
    /// and `true`. If the key does not exist, inserts the value produced by
    /// `default_fn`, returns a mutable reference to it, and `false`.
    ///
    /// This fuses `find_slot` + `find_insert_slot` into one probe traverse,
    /// eliminating the redundant hash+probe that `get_mut` → miss → `insert`
    /// would perform.
    pub fn get_or_insert_with<F>(
        &mut self,
        key: VortexKey,
        default_fn: F,
    ) -> (&mut VortexValue, bool)
    where
        F: FnOnce() -> VortexValue,
    {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let h2 = h2_from_hash(hash);
        let h1 = h1_from_hash(hash);
        let mask = self.group_mask();
        let mut probe = ProbeSeq::new(h1, mask);
        let mut insert_candidate: Option<usize> = None;

        loop {
            let ctrl_ptr = unsafe { self.raw.ctrl_group(probe.pos) };

            // Check for matching H₂ — key might already exist.
            let matches = unsafe { Group::match_h2(ctrl_ptr, h2) };
            for bit in matches {
                let slot = probe.pos * GROUP_SIZE + bit;
                let entry = unsafe { self.raw.entry(slot) };
                if entry.matches_key(key_bytes) {
                    // Key found — return existing value.
                    return (
                        self.values[slot].as_mut().expect("live slot has value"),
                        true,
                    );
                }
            }

            // Record first usable insertion point (EMPTY or DELETED).
            if insert_candidate.is_none() {
                let candidates = unsafe { Group::match_empty_or_deleted(ctrl_ptr) };
                if let Some(bit) = candidates.lowest() {
                    insert_candidate = Some(probe.pos * GROUP_SIZE + bit);
                }
            }

            // If there's an EMPTY slot in this group, the key definitely doesn't exist.
            let empties = unsafe { Group::match_empty(ctrl_ptr) };
            if empties.any_set() {
                break;
            }

            probe.advance();
        }

        // Key not found — insert at the recorded candidate slot.
        let slot = insert_candidate.expect("must find an insert slot before EMPTY terminator");
        let was_empty = unsafe { *self.raw.ctrl_at(slot) == CTRL_EMPTY };

        let value = default_fn();
        self.keys[slot] = Some(key);
        self.values[slot] = Some(value);

        let key_ptr = self.keys[slot].as_ref().expect("new slot must have key") as *const VortexKey;
        let value_ptr = self.values[slot]
            .as_ref()
            .expect("new slot must have value") as *const VortexValue;

        let entry = unsafe { self.raw.entry_mut(slot) };
        // SAFETY: slot ownership remains stable until the next overwrite/resize,
        // and resize rewrites every borrowed pointer into the new table.
        Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, 0);
        unsafe {
            self.raw.set_ctrl(slot, h2);
        }

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }

        self.record_memory_delta(Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe {
            &*value_ptr
        }) as isize);

        (self.values[slot].as_mut().expect("just inserted"), false)
    }

    /// Remove a key and return its value.
    pub fn remove(&mut self, key: &VortexKey) -> Option<VortexValue> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let slot = self.find_slot(key_bytes, hash)?;

        self.delete_slot(slot)
    }

    /// Check existence with a pre-computed hash (no rehashing).
    #[inline]
    pub fn contains_key_prehashed(&self, key_bytes: &[u8], hash: u64) -> bool {
        self.find_slot(key_bytes, hash).is_some()
    }

    #[inline]
    pub fn find_slot_prehashed(&self, key_bytes: &[u8], hash: u64) -> Option<usize> {
        self.find_slot(key_bytes, hash)
    }

    #[inline]
    pub fn record_access_prehashed(&self, key_bytes: &[u8], hash: u64, random: u64) -> bool {
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return false;
        };
        self.record_access_slot(slot, random)
    }

    /// Insert a key known to be absent from the table, using a pre-computed hash.
    ///
    /// **Caller must guarantee the key does not already exist** — this method
    /// skips the `find_slot` existence check and goes directly to
    /// `find_insert_slot`, saving one full SIMD probe traversal.
    pub fn insert_new_prehashed(&mut self, key: VortexKey, value: VortexValue, hash: u64) {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }

        let h2 = h2_from_hash(hash);
        let slot = self.find_insert_slot(hash);
        let was_empty = unsafe { *self.raw.ctrl_at(slot) == CTRL_EMPTY };

        self.keys[slot] = Some(key);
        self.values[slot] = Some(value);

        let key_ptr = self.keys[slot].as_ref().expect("new slot must have key") as *const VortexKey;
        let value_ptr = self.values[slot]
            .as_ref()
            .expect("new slot must have value") as *const VortexValue;

        let entry = unsafe { self.raw.entry_mut(slot) };
        Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, 0);
        unsafe {
            self.raw.set_ctrl(slot, h2);
        }

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe {
            &*value_ptr
        }) as isize);
    }

    /// Returns `true` if the key exists.
    pub fn contains_key(&self, key: &VortexKey) -> bool {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        self.find_slot(key_bytes, hash).is_some()
    }

    // ── Probing ─────────────────────────────────────────────────────

    /// Find the slot of an existing key, or `None`.
    #[inline]
    fn find_slot(&self, key_bytes: &[u8], hash: u64) -> Option<usize> {
        let h2 = h2_from_hash(hash);
        let h1 = h1_from_hash(hash);
        let mask = self.group_mask();
        let mut probe = ProbeSeq::new(h1, mask);

        loop {
            let ctrl_ptr = unsafe { self.raw.ctrl_group(probe.pos) };
            let matches = unsafe { Group::match_h2(ctrl_ptr, h2) };

            for bit in matches {
                let slot = probe.pos * GROUP_SIZE + bit;
                let entry = unsafe { self.raw.entry(slot) };
                if entry.matches_key(key_bytes) {
                    return Some(slot);
                }
            }

            let empties = unsafe { Group::match_empty(ctrl_ptr) };
            if empties.any_set() {
                return None;
            }

            probe.advance();
        }
    }

    /// Find the first EMPTY or DELETED slot along the probe chain.
    #[inline]
    fn find_insert_slot(&self, hash: u64) -> usize {
        let h1 = h1_from_hash(hash);
        let mask = self.group_mask();
        let mut probe = ProbeSeq::new(h1, mask);

        loop {
            let ctrl_ptr = unsafe { self.raw.ctrl_group(probe.pos) };
            let candidates = unsafe { Group::match_empty_or_deleted(ctrl_ptr) };
            if let Some(bit) = candidates.lowest() {
                return probe.pos * GROUP_SIZE + bit;
            }
            probe.advance();
        }
    }

    // ── Resize ──────────────────────────────────────────────────────

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
            let ctrl = unsafe { *self.raw.ctrl_at(slot) };
            if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
                continue;
            }

            let ttl = unsafe { self.raw.entry(slot) }.ttl_deadline();
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
                let gctrl = unsafe { new_raw.ctrl_group(probe.pos) };
                let empties = unsafe { Group::match_empty(gctrl) };
                if let Some(bit) = empties.lowest() {
                    break probe.pos * GROUP_SIZE + bit;
                }
                probe.advance();
            };

            new_keys[new_slot] = self.keys[slot].take();
            new_values[new_slot] = self.values[slot].take();

            let key_ptr = new_keys[new_slot]
                .as_ref()
                .expect("rehash slot must have key") as *const VortexKey;
            let value_ptr = new_values[new_slot]
                .as_ref()
                .expect("rehash slot must have value")
                as *const VortexValue;

            let entry = unsafe { new_raw.entry_mut(new_slot) };
            // SAFETY: the new slot owns these key/value objects for the lifetime of
            // the new table, and future resizes rewrite pointers again.
            Self::write_entry(entry, h2, unsafe { &*key_ptr }, unsafe { &*value_ptr }, ttl);
            unsafe { new_raw.set_ctrl(new_slot, h2) };
        }

        // Free the old allocation + swap.
        unsafe {
            self.raw.dealloc();
        }
        self.raw = new_raw;
        self.keys = new_keys;
        self.values = new_values;
        self.occupied = self.len; // Tombstones are gone.
    }

    // ── Entry I/O helpers ───────────────────────────────────────────

    /// Write key+value metadata into the 64-byte entry.
    #[inline]
    fn write_entry(entry: &mut Entry, h2: u8, key: &VortexKey, value: &VortexValue, ttl: u64) {
        let key_bytes = key.as_bytes();
        let inline_string = match value {
            VortexValue::InlineString(bytes) => Some(bytes.as_bytes()),
            VortexValue::String(bytes) if bytes.len() <= 21 => Some(bytes.as_ref()),
            _ => None,
        };

        if let VortexValue::Integer(integer) = value {
            if key_bytes.len() <= 23 {
                entry.write_integer(h2, key_bytes, *integer, ttl);
            } else {
                // SAFETY: the caller passes references to slot-owned key/value data,
                // and the table rewrites entry pointers on overwrite/resize.
                unsafe { entry.write_heap(h2, key, value, ttl) };
            }
            return;
        }

        if key_bytes.len() <= 23
            && let Some(bytes) = inline_string
        {
            entry.write_inline(h2, key_bytes, bytes, ttl);
            return;
        }

        // SAFETY: the caller passes references to slot-owned key/value data,
        // and the table rewrites entry pointers on overwrite/resize.
        unsafe { entry.write_heap(h2, key, value, ttl) };
    }

    // ── Cursor / scan helpers ───────────────────────────────────────

    /// Total number of slots in the table (always a power of two).
    #[inline]
    pub fn total_slots(&self) -> usize {
        self.raw.num_slots()
    }

    /// Returns `(key, value)` at `slot` if occupied, else `None`.
    #[inline]
    pub fn slot_key_value(&self, slot: usize) -> Option<(&VortexKey, &VortexValue)> {
        if slot >= self.raw.num_slots() {
            return None;
        }
        let ctrl = unsafe { *self.raw.ctrl_at(slot) };
        if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
            return None;
        }
        let key = self.keys[slot].as_ref()?;
        let value = self.values[slot].as_ref()?;
        Some((key, value))
    }

    #[inline]
    pub fn slot_entry(&self, slot: usize) -> Option<&Entry> {
        if slot >= self.raw.num_slots() {
            return None;
        }
        let ctrl = unsafe { *self.raw.ctrl_at(slot) };
        if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
            return None;
        }
        Some(unsafe { self.raw.entry(slot) })
    }

    #[inline]
    pub fn slot_memory_bytes(&self, slot: usize) -> usize {
        if slot >= self.raw.num_slots() {
            return 0;
        }
        let ctrl = unsafe { *self.raw.ctrl_at(slot) };
        if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
            return 0;
        }
        self.slot_memory_usage(slot)
    }

    #[inline]
    pub fn record_access_slot(&self, slot: usize, random: u64) -> bool {
        let Some(entry) = self.slot_entry(slot) else {
            return false;
        };
        entry.record_access(random);
        true
    }

    /// Returns the TTL deadline (nanos) for the entry at `slot`, or 0 if
    /// the slot is empty/deleted or has no TTL.
    #[inline]
    pub fn slot_entry_ttl(&self, slot: usize) -> u64 {
        if slot >= self.raw.num_slots() {
            return 0;
        }
        let ctrl = unsafe { *self.raw.ctrl_at(slot) };
        if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
            return 0;
        }
        unsafe { self.raw.entry(slot) }.ttl_deadline()
    }

    #[inline]
    pub fn try_slot_memory_bytes(&self, slot: usize) -> Option<usize> {
        if slot >= self.raw.num_slots() {
            return None;
        }
        Some(self.slot_memory_bytes(slot))
    }

    #[inline]
    pub fn try_slot_entry_ttl(&self, slot: usize) -> Option<u64> {
        if slot >= self.raw.num_slots() {
            return None;
        }
        Some(self.slot_entry_ttl(slot))
    }

    /// Remove a key and return `(value, ttl_deadline)`.
    pub fn remove_with_ttl(&mut self, key: &VortexKey) -> Option<(VortexValue, u64)> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        self.remove_with_ttl_prehashed(key_bytes, hash)
    }

    /// Like [`remove_with_ttl`](Self::remove_with_ttl) but uses raw bytes and a
    /// pre-computed hash so delete-style commands can skip `VortexKey`
    /// materialization on the hot path.
    pub fn remove_with_ttl_prehashed(
        &mut self,
        key_bytes: &[u8],
        hash: u64,
    ) -> Option<(VortexValue, u64)> {
        let slot = self.find_slot(key_bytes, hash)?;

        let ttl = unsafe { self.raw.entry(slot) }.ttl_deadline();

        let value = self.delete_slot(slot)?;
        Some((value, ttl))
    }

    /// Get `(&value, ttl_deadline)` for a key (no expiry check).
    pub fn get_with_ttl(&self, key: &VortexKey) -> Option<(&VortexValue, u64)> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let slot = self.find_slot(key_bytes, hash)?;
        let ttl = unsafe { self.raw.entry(slot) }.ttl_deadline();
        let value = self.values[slot].as_ref()?;
        Some((value, ttl))
    }

    /// Mutable variant of [`get_with_ttl`](Self::get_with_ttl).
    /// Returns `(&mut VortexValue, ttl_deadline)` in a single probe.
    pub fn get_with_ttl_mut(&mut self, key: &VortexKey) -> Option<(&mut VortexValue, u64)> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let slot = self.find_slot(key_bytes, hash)?;
        let ttl = unsafe { self.raw.entry(slot) }.ttl_deadline();
        let value = self.values[slot].as_mut()?;
        Some((value, ttl))
    }

    /// Like [`get_with_ttl`](Self::get_with_ttl) but uses raw bytes and a
    /// pre-computed hash so batch pipelines do not rebuild `VortexKey`s or
    /// re-hash the same key on the hot path.
    pub fn get_with_ttl_prehashed(
        &self,
        key_bytes: &[u8],
        hash: u64,
    ) -> Option<(&VortexValue, u64)> {
        let slot = self.find_slot(key_bytes, hash)?;
        let ttl = unsafe { self.raw.entry(slot) }.ttl_deadline();
        let value = self.values[slot].as_ref()?;
        Some((value, ttl))
    }

    // ── Iteration ───────────────────────────────────────────────────

    /// Iterate over all live `(&VortexKey, &VortexValue)` pairs,
    /// reconstructed from entries and the value store.
    ///
    /// Note: Returns `(VortexKey, &VortexValue)` — the key is reconstructed
    /// from inline bytes (allocation-free if ≤23 bytes).
    pub fn iter(&self) -> impl Iterator<Item = (&VortexKey, &VortexValue)> {
        let num_slots = self.raw.num_slots();
        (0..num_slots).filter_map(move |slot| {
            // SAFETY: slot < num_slots.
            let ctrl = unsafe { *self.raw.ctrl_at(slot) };
            if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
                return None;
            }
            let key = self.keys[slot].as_ref()?;
            let value = self.values[slot].as_ref()?;
            Some((key, value))
        })
    }

    /// Iterate over live entries (low-level access to the 64-byte `Entry`).
    pub fn iter_entries(&self) -> impl Iterator<Item = &Entry> {
        let num_slots = self.raw.num_slots();
        (0..num_slots).filter_map(move |slot| {
            let ctrl = unsafe { *self.raw.ctrl_at(slot) };
            if ctrl == CTRL_EMPTY || ctrl == CTRL_DELETED {
                return None;
            }
            Some(unsafe { self.raw.entry(slot) })
        })
    }

    // ── TTL operations ─────────────────────────────────────────────

    /// Insert a key-value pair with an explicit TTL deadline.
    /// Returns the old value if the key existed.
    pub fn insert_with_ttl(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        ttl_deadline: u64,
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
            self.keys[slot] = Some(key);
            let previous = self.values[slot].replace(value);

            let key_ptr =
                self.keys[slot].as_ref().expect("live slot must have key") as *const VortexKey;
            let value_ptr = self.values[slot]
                .as_ref()
                .expect("live slot must have value")
                as *const VortexValue;

            let entry = unsafe { self.raw.entry_mut(slot) };
            Self::write_entry(
                entry,
                h2,
                unsafe { &*key_ptr },
                unsafe { &*value_ptr },
                ttl_deadline,
            );
            unsafe { self.raw.set_ctrl(slot, h2) };
            let new_bytes = Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe { &*value_ptr });
            self.record_memory_delta(new_bytes as isize - old_bytes as isize);
            return previous;
        }

        // Phase 2: key not found — insert into first available slot.
        let slot = self.find_insert_slot(hash);
        let was_empty = unsafe { *self.raw.ctrl_at(slot) == CTRL_EMPTY };

        self.keys[slot] = Some(key);
        self.values[slot] = Some(value);

        let key_ptr = self.keys[slot].as_ref().expect("new slot must have key") as *const VortexKey;
        let value_ptr = self.values[slot]
            .as_ref()
            .expect("new slot must have value") as *const VortexValue;

        let entry = unsafe { self.raw.entry_mut(slot) };
        Self::write_entry(
            entry,
            h2,
            unsafe { &*key_ptr },
            unsafe { &*value_ptr },
            ttl_deadline,
        );
        unsafe {
            self.raw.set_ctrl(slot, h2);
        }

        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        self.record_memory_delta(Self::entry_memory_usage(unsafe { &*key_ptr }, unsafe {
            &*value_ptr
        }) as isize);
        None
    }

    /// GET with lazy expiry: returns the value, or `None` if the key
    /// doesn't exist or is expired. Expired entries are tombstoned in place.
    pub fn get_or_expire(&mut self, key: &VortexKey, now_nanos: u64) -> Option<&VortexValue> {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        self.get_or_expire_prehashed(key_bytes, hash, now_nanos)
    }

    /// Like `get_or_expire` but uses a pre-computed hash (for MGET batching).
    pub fn get_or_expire_prehashed(
        &mut self,
        key_bytes: &[u8],
        hash: u64,
        now_nanos: u64,
    ) -> Option<&VortexValue> {
        let slot = self.find_slot(key_bytes, hash)?;

        let entry = unsafe { self.raw.entry(slot) };
        if entry.is_expired(now_nanos) {
            let _ = self.delete_slot(slot);
            return None;
        }

        self.values[slot].as_ref()
    }

    /// Check existence with lazy expiry.
    pub fn contains_key_or_expire(&mut self, key: &VortexKey, now_nanos: u64) -> bool {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return false;
        };

        let entry = unsafe { self.raw.entry(slot) };
        if entry.is_expired(now_nanos) {
            let _ = self.delete_slot(slot);
            return false;
        }

        true
    }

    /// Check existence with lazy expiry, using a pre-computed hash.
    pub fn contains_key_or_expire_prehashed(
        &mut self,
        key_bytes: &[u8],
        hash: u64,
        now_nanos: u64,
    ) -> bool {
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return false;
        };

        let entry = unsafe { self.raw.entry(slot) };
        if entry.is_expired(now_nanos) {
            let _ = self.delete_slot(slot);
            return false;
        }

        true
    }

    /// Set or update the TTL deadline on an existing key.
    /// Returns `true` if the key was found and updated.
    pub fn set_entry_ttl(&mut self, key: &VortexKey, deadline_nanos: u64) -> bool {
        let key_bytes = key.as_bytes();
        let hash = self.hash_key(key_bytes);
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return false;
        };

        let entry = unsafe { self.raw.entry_mut(slot) };
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

        let entry = unsafe { self.raw.entry_mut(slot) };
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
        Some(unsafe { self.raw.entry(slot) }.ttl_deadline())
    }

    /// Remove the entry matching `hash` whose `ttl_deadline` equals
    /// `deadline_nanos`. Used by the active expiry sweep.
    ///
    /// Returns `true` if an entry was found and removed.
    pub fn remove_expired_by_hash(&mut self, hash: u64, deadline_nanos: u64) -> bool {
        let h2 = h2_from_hash(hash);
        let h1 = h1_from_hash(hash);
        let mask = self.group_mask();
        let mut probe = ProbeSeq::new(h1, mask);

        loop {
            let ctrl_ptr = unsafe { self.raw.ctrl_group(probe.pos) };
            let matches = unsafe { Group::match_h2(ctrl_ptr, h2) };

            for bit in matches {
                let slot = probe.pos * GROUP_SIZE + bit;
                let entry = unsafe { self.raw.entry(slot) };
                if entry.ttl_deadline() == deadline_nanos
                    && self.keys[slot]
                        .as_ref()
                        .is_some_and(|key| self.hash_key_bytes(key.as_bytes()) == hash)
                {
                    let _ = self.delete_slot(slot);
                    return true;
                }
            }

            let empties = unsafe { Group::match_empty(ctrl_ptr) };
            if empties.any_set() {
                return false;
            }

            probe.advance();
        }
    }

    /// Expose the hash function for external callers (e.g. ExpiryWheel).
    #[inline]
    pub fn hash_key_bytes(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Prefetch the control byte group **and** first entry slot for a given hash.
    /// Used by MGET/MSET/DEL/EXISTS batch pipelines to hide memory latency.
    #[inline]
    pub fn prefetch_group(&self, hash: u64) {
        let h1 = h1_from_hash(hash);
        let group_idx = h1 & self.group_mask();
        // Prefetch the 16-byte control array for this group.
        let ctrl_ptr = unsafe { self.raw.ctrl_group(group_idx) };
        crate::prefetch::prefetch_read(ctrl_ptr);
        // Prefetch the first entry slot in the group (64-byte cache line).
        let entry_ptr = unsafe { self.raw.entry(group_idx * GROUP_SIZE) as *const Entry };
        crate::prefetch::prefetch_read(entry_ptr);
    }

    /// Prefetch with **write** intent (for insert/delete paths).
    #[inline]
    pub fn prefetch_group_write(&self, hash: u64) {
        let h1 = h1_from_hash(hash);
        let group_idx = h1 & self.group_mask();
        let ctrl_ptr = unsafe { self.raw.ctrl_group(group_idx) };
        crate::prefetch::prefetch_write(ctrl_ptr);
        let entry_ptr = unsafe { self.raw.entry(group_idx * GROUP_SIZE) as *const Entry };
        crate::prefetch::prefetch_write(entry_ptr);
    }
}

impl Default for SwissTable {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SwissTable {
    fn drop(&mut self) {
        // SAFETY: We own the allocation and this is the final use.
        unsafe {
            self.raw.dealloc();
        }
    }
}

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
        assert!(table.try_slot_memory_bytes(slot).is_none());
        assert!(table.try_slot_entry_ttl(slot).is_none());
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

// ── Tests ───────────────────────────────────────────────────────────

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

        assert_eq!(table.local_memory_used(), 0);

        table.insert(key.clone(), VortexValue::from("one"));
        let first = table.local_memory_used();
        assert!(first > 0);

        table.insert(key.clone(), VortexValue::from_bytes(&[b'x'; 128]));
        let second = table.local_memory_used();
        assert!(second > first);

        table.remove(&key);
        assert_eq!(table.local_memory_used(), 0);
    }

    #[test]
    fn local_memory_drift_flushes_in_threshold_chunks() {
        let mut table = SwissTable::new();
        let global = AtomicUsize::new(0);
        let key = VortexKey::from("flush-key");
        let value = VortexValue::from_bytes(&vec![b'x'; 20_000]);

        table.insert(key.clone(), value);
        assert!(table.local_memory_used() > 0);
        assert_eq!(global.load(Ordering::Relaxed), 0);

        table.flush_memory_drift(&global);
        let flushed = global.load(Ordering::Relaxed);
        assert!(flushed >= MEMORY_ACCOUNTING_FLUSH_THRESHOLD as usize);
        assert_eq!(table.local_memory_drift(), 0);

        table.remove(&key);
        table.flush_memory_drift(&global);
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
    fn remove_expired_by_hash_requires_full_hash_match() {
        let mut table = SwissTable::with_capacity(1);
        let key = VortexKey::from("ttl-key");
        let deadline = 5;

        table.insert_with_ttl(key.clone(), VortexValue::from("value"), deadline);

        let real_hash = table.hash_key_bytes(key.as_bytes());
        let fake_hash = real_hash ^ 1;

        assert_ne!(fake_hash, real_hash);
        assert_eq!(h2_from_hash(fake_hash), h2_from_hash(real_hash));
        assert!(table.find_slot(key.as_bytes(), fake_hash).is_some());

        assert!(!table.remove_expired_by_hash(fake_hash, deadline));
        assert!(table.contains_key(&key));
        assert_eq!(table.len(), 1);

        assert!(table.remove_expired_by_hash(real_hash, deadline));
        assert!(!table.contains_key(&key));
        assert_eq!(table.len(), 0);
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
    fn get_mut_updates_value() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("x");
        table.insert(key.clone(), VortexValue::Integer(10));

        if let Some(v) = table.get_mut(&key) {
            *v = VortexValue::Integer(20);
        }
        assert_eq!(table.get(&key), Some(&VortexValue::Integer(20)));
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
