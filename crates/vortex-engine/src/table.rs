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
    __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_or_si128, _mm_set1_epi8,
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

/// Slot index known to be within this table's slot range.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SlotIndex(usize);

impl SlotIndex {
    #[inline]
    fn checked(slot: usize, slots: usize) -> Option<Self> {
        (slot < slots).then_some(Self(slot))
    }

    #[inline]
    const fn from_group_offset(group: GroupIndex, offset: usize) -> Self {
        Self(group.get() * GROUP_SIZE + offset)
    }

    #[inline]
    const fn get(self) -> usize {
        self.0
    }
}

/// Probe group index known to be within this table's primary group range.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GroupIndex(usize);

impl GroupIndex {
    #[inline]
    const fn masked(hash_h1: usize, mask: usize) -> Self {
        Self(hash_h1 & mask)
    }

    #[inline]
    const fn get(self) -> usize {
        self.0
    }
}

/// Hash computed with the SwissTable hasher that owns the target table.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TableHash(u64);

impl TableHash {
    #[inline]
    pub(crate) const fn from_u64(hash: u64) -> Self {
        Self(hash)
    }

    #[inline]
    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    #[inline(always)]
    fn h1(self) -> usize {
        h1_from_hash(self.0)
    }

    #[inline(always)]
    fn h2(self) -> u8 {
        h2_from_hash(self.0)
    }
}

/// Slot index proven live by observing a non-empty, non-deleted control byte.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LiveSlot(SlotIndex);

impl LiveSlot {
    #[inline]
    const fn new(slot: SlotIndex) -> Self {
        Self(slot)
    }

    #[inline]
    const fn index(self) -> SlotIndex {
        self.0
    }

    #[inline]
    const fn get(self) -> usize {
        self.0.get()
    }
}

/// Result of one prehashed slot lookup.
///
/// The cursor owns the mutable table borrow while it is alive, so callers can
/// observe TTL/value state and perform the matching mutation without
/// re-looking-up the same key in the same lock scope.
pub(crate) enum SlotCursor<'a> {
    Live(LiveSlotCursor<'a>),
    Expired(ExpiredSlot<'a>),
    Vacant(VacantSlot<'a>),
}

/// Cursor for a live, non-expired slot.
pub(crate) struct LiveSlotCursor<'a> {
    table: &'a mut SwissTable,
    slot: LiveSlot,
    hash: TableHash,
    ttl_deadline: u64,
}

/// Cursor for a slot that was live at lookup time but expired at `now_nanos`.
pub(crate) struct ExpiredSlot<'a> {
    table: &'a mut SwissTable,
    slot: LiveSlot,
    ttl_deadline: u64,
}

/// Cursor for an absent key.
pub(crate) struct VacantSlot<'a> {
    table: &'a mut SwissTable,
    hash: TableHash,
}

/// Value removed from a table slot, plus the metadata observed before removal.
#[allow(dead_code)]
pub(crate) struct SlotRemoval {
    value: VortexValue,
    old_ttl: u64,
    old_memory_bytes: usize,
}

#[allow(dead_code)]
impl SlotRemoval {
    #[inline]
    pub(crate) fn into_value(self) -> VortexValue {
        self.value
    }

    #[inline]
    pub(crate) const fn old_ttl(&self) -> u64 {
        self.old_ttl
    }

    #[inline]
    pub(crate) const fn old_had_ttl(&self) -> bool {
        self.old_ttl != 0
    }

    #[inline]
    pub(crate) const fn old_memory_bytes(&self) -> usize {
        self.old_memory_bytes
    }
}

/// Metadata emitted by cursor mutations.
#[allow(dead_code)]
pub(crate) struct SlotMutationReport {
    live_slot: Option<LiveSlot>,
    previous: Option<VortexValue>,
    old_ttl: u64,
    new_ttl: u64,
    old_memory_bytes: usize,
    new_memory_bytes: usize,
    entry_lsn_stamped: bool,
}

#[allow(dead_code)]
impl SlotMutationReport {
    #[inline]
    fn inserted(
        live_slot: LiveSlot,
        new_ttl: u64,
        new_memory_bytes: usize,
        entry_lsn_stamped: bool,
    ) -> Self {
        Self {
            live_slot: Some(live_slot),
            previous: None,
            old_ttl: 0,
            new_ttl,
            old_memory_bytes: 0,
            new_memory_bytes,
            entry_lsn_stamped,
        }
    }

    #[inline]
    fn replaced(
        live_slot: LiveSlot,
        previous: Option<VortexValue>,
        old_ttl: u64,
        new_ttl: u64,
        old_memory_bytes: usize,
        new_memory_bytes: usize,
        entry_lsn_stamped: bool,
    ) -> Self {
        Self {
            live_slot: Some(live_slot),
            previous,
            old_ttl,
            new_ttl,
            old_memory_bytes,
            new_memory_bytes,
            entry_lsn_stamped,
        }
    }

    #[inline]
    fn ttl_only(
        live_slot: LiveSlot,
        old_ttl: u64,
        new_ttl: u64,
        memory_bytes: usize,
        entry_lsn_stamped: bool,
    ) -> Self {
        Self {
            live_slot: Some(live_slot),
            previous: None,
            old_ttl,
            new_ttl,
            old_memory_bytes: memory_bytes,
            new_memory_bytes: memory_bytes,
            entry_lsn_stamped,
        }
    }

    #[inline]
    pub(crate) fn into_previous(self) -> Option<VortexValue> {
        self.previous
    }

    #[inline]
    pub(crate) fn take_previous(&mut self) -> Option<VortexValue> {
        self.previous.take()
    }

    #[inline]
    pub(crate) const fn old_ttl(&self) -> u64 {
        self.old_ttl
    }

    #[inline]
    pub(crate) const fn new_ttl(&self) -> u64 {
        self.new_ttl
    }

    #[inline]
    pub(crate) const fn old_had_ttl(&self) -> bool {
        self.old_ttl != 0
    }

    #[inline]
    pub(crate) const fn new_has_ttl(&self) -> bool {
        self.new_ttl != 0
    }

    #[inline]
    pub(crate) const fn old_memory_bytes(&self) -> usize {
        self.old_memory_bytes
    }

    #[inline]
    pub(crate) const fn new_memory_bytes(&self) -> usize {
        self.new_memory_bytes
    }

    #[inline]
    pub(crate) const fn entry_lsn_stamped(&self) -> bool {
        self.entry_lsn_stamped
    }
}

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
        #[cfg(all(feature = "simd", target_arch = "x86_64"))]
        {
            // SAFETY: SSE2 is baseline on x86_64. ctrl valid per caller.
            unsafe {
                let group = _mm_loadu_si128(ctrl.cast::<__m128i>());
                let empty = _mm_cmpeq_epi8(group, _mm_set1_epi8(CTRL_EMPTY as i8));
                let deleted = _mm_cmpeq_epi8(group, _mm_set1_epi8(CTRL_DELETED as i8));
                BitMask(_mm_movemask_epi8(_mm_or_si128(empty, deleted)) as u16)
            }
        }

        #[cfg(all(feature = "simd", target_arch = "aarch64"))]
        {
            // SAFETY: ctrl valid per caller, exactly 16 bytes.
            let slice = unsafe { core::slice::from_raw_parts(ctrl, GROUP_SIZE) };
            let group = Simd::<u8, 16>::from_slice(slice);
            let empty = group.simd_eq(Simd::<u8, 16>::splat(CTRL_EMPTY));
            let deleted = group.simd_eq(Simd::<u8, 16>::splat(CTRL_DELETED));
            BitMask((empty | deleted).to_bitmask() as u16)
        }

        #[cfg(not(any(
            all(feature = "simd", target_arch = "x86_64"),
            all(feature = "simd", target_arch = "aarch64"),
        )))]
        {
            // SAFETY: ctrl valid per caller.
            unsafe { Self::match_empty_or_deleted_scalar(ctrl) }
        }
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

    #[allow(dead_code)]
    #[inline]
    unsafe fn match_empty_or_deleted_scalar(ctrl: *const u8) -> BitMask {
        let mut mask: u16 = 0;
        for i in 0..GROUP_SIZE {
            // SAFETY: ctrl valid for GROUP_SIZE bytes per caller.
            let byte = unsafe { *ctrl.add(i) };
            if byte == CTRL_EMPTY || byte == CTRL_DELETED {
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
    pos: GroupIndex,
    stride: usize,
    mask: usize,
}

impl ProbeSeq {
    #[inline]
    fn new(h1: usize, mask: usize) -> Self {
        Self {
            pos: GroupIndex::masked(h1, mask),
            stride: 0,
            mask,
        }
    }

    #[inline]
    fn advance(&mut self) {
        self.stride += 1;
        self.pos = GroupIndex((self.pos.get() + self.stride) & self.mask);
    }
}

/// Raw storage: contiguous control byte array + entry array.
///
/// Layout: `[ctrl: (num_groups+1)*16 bytes] [pad to 64] [entries: num_groups*16*64 bytes]`
///
/// The extra `+1` group of ctrl bytes is a sentinel mirror of group 0,
/// so SIMD loads at the boundary don't read out of bounds.
#[derive(Clone, Copy, Debug)]
struct TableLayout {
    num_groups: usize,
    num_slots: usize,
    ctrl_bytes: usize,
    ctrl_padded: usize,
    entries_bytes: usize,
    raw_alloc_size: usize,
    key_slots_bytes: usize,
    value_slots_bytes: usize,
    total_allocated_bytes: usize,
    raw_layout: Layout,
}

impl TableLayout {
    fn for_capacity(cap: usize) -> Self {
        Self::try_for_capacity(cap).expect("SwissTable: capacity overflow")
    }

    fn try_for_capacity(cap: usize) -> Option<Self> {
        let min_slots = if cap == 0 { GROUP_SIZE } else { cap };
        let required = min_slots.checked_mul(LOAD_FACTOR_D)? / LOAD_FACTOR_N;
        let num_groups = required
            .div_ceil(GROUP_SIZE)
            .checked_next_power_of_two()?
            .max(MIN_GROUPS);
        Self::try_for_groups(num_groups)
    }

    fn for_groups(num_groups: usize) -> Self {
        Self::try_for_groups(num_groups).expect("SwissTable: layout overflow")
    }

    fn try_for_groups(num_groups: usize) -> Option<Self> {
        if !num_groups.is_power_of_two() {
            return None;
        }

        let ctrl_bytes = num_groups.checked_add(1)?.checked_mul(GROUP_SIZE)?;
        let ctrl_padded = align_up_to(ctrl_bytes, 64)?;
        let num_slots = num_groups.checked_mul(GROUP_SIZE)?;
        let entries_bytes = num_slots.checked_mul(size_of::<Entry>())?;
        let raw_alloc_size = ctrl_padded.checked_add(entries_bytes)?;
        let key_slots_bytes = num_slots.checked_mul(size_of::<Option<VortexKey>>())?;
        let value_slots_bytes = num_slots.checked_mul(size_of::<Option<VortexValue>>())?;
        let total_allocated_bytes = raw_alloc_size
            .checked_add(key_slots_bytes)?
            .checked_add(value_slots_bytes)?;
        let raw_layout = Layout::from_size_align(raw_alloc_size, 64).ok()?;

        Some(Self {
            num_groups,
            num_slots,
            ctrl_bytes,
            ctrl_padded,
            entries_bytes,
            raw_alloc_size,
            key_slots_bytes,
            value_slots_bytes,
            total_allocated_bytes,
            raw_layout,
        })
    }
}

#[inline]
fn align_up_to(size: usize, align: usize) -> Option<usize> {
    let mask = align - 1;
    Some(size.checked_add(mask)? & !mask)
}

struct RawTable {
    ctrl: *mut u8,
    entries: *mut Entry,
    layout: TableLayout,
}

impl RawTable {
    /// Allocate a new raw table with a checked layout.
    /// Control bytes are initialized to `CTRL_EMPTY`, and entries are initialized to empty.
    ///
    /// # Panics
    /// Panics if memory allocation fails.
    fn allocate(layout: TableLayout) -> Self {
        debug_assert_eq!(
            layout.entries_bytes,
            layout.num_slots * size_of::<Entry>(),
            "checked table layout entry byte count must match slot count"
        );
        // SAFETY: TableLayout validates size and alignment.
        let ptr = unsafe { alloc::alloc_zeroed(layout.raw_layout) };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout.raw_layout);
        }

        let ctrl = ptr;
        // SAFETY: `ctrl_padded <= alloc_size`, so the entry region starts within the
        // allocation and remains aligned because both bases are 64-byte aligned.
        let entries = unsafe { ptr.add(layout.ctrl_padded).cast::<Entry>() };

        let mut raw = Self {
            ctrl,
            entries,
            layout,
        };
        raw.fill_ctrl(CTRL_EMPTY);

        // Initialize entries to empty.
        for i in 0..raw.layout.num_slots {
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
    fn ctrl_group(&self, group_idx: GroupIndex) -> *const u8 {
        debug_assert!(group_idx.get() <= self.num_groups());

        // SAFETY: `group_idx <= num_groups` includes the sentinel group, and each
        // group starts within the control-byte allocation.
        unsafe { self.ctrl.add(group_idx.get() * GROUP_SIZE) }
    }

    #[inline]
    fn ctrl(&self, slot: SlotIndex) -> u8 {
        debug_assert!(slot.get() < self.num_slots());

        // SAFETY: live slots are within the primary control-byte array.
        unsafe { *self.ctrl.add(slot.get()) }
    }

    #[inline]
    fn entry(&self, slot: LiveSlot) -> &Entry {
        debug_assert!(slot.get() < self.num_slots());

        // SAFETY: `slot < num_slots`, so the element lies within the entry array.
        unsafe { &*self.entries.add(slot.get()) }
    }

    #[inline]
    fn entry_mut(&mut self, slot: LiveSlot) -> &mut Entry {
        debug_assert!(slot.get() < self.num_slots());

        // SAFETY: `slot < num_slots` and `&mut self` guarantees exclusive access.
        unsafe { &mut *self.entries.add(slot.get()) }
    }

    #[inline]
    fn entry_mut_for_publish(&mut self, slot: SlotIndex) -> &mut Entry {
        debug_assert!(slot.get() < self.num_slots());

        // SAFETY: `slot < num_slots` and `&mut self` guarantees exclusive access.
        // This path is used while publishing a new slot or rewriting a slot whose
        // key/value owners are already installed.
        unsafe { &mut *self.entries.add(slot.get()) }
    }

    #[inline]
    fn entry_ptr(&self, slot: SlotIndex) -> *const Entry {
        debug_assert!(slot.get() < self.num_slots());

        // SAFETY: `slot < num_slots`, so the computed pointer stays within the
        // entry array.
        unsafe { self.entries.add(slot.get()) }
    }

    /// Set a control byte + update the sentinel mirror for group 0.
    #[inline]
    fn set_ctrl(&self, slot: SlotIndex, ctrl: u8) {
        debug_assert!(slot.get() < self.num_slots());

        // SAFETY: `slot < num_slots`, and the mirrored sentinel slot also lies within
        // the control-byte allocation.
        unsafe {
            *self.ctrl.add(slot.get()) = ctrl;
            // Mirror: if this slot is in the first group, also write to sentinel.
            if slot.get() < GROUP_SIZE {
                let mirror = self.num_groups() * GROUP_SIZE + slot.get();
                *self.ctrl.add(mirror) = ctrl;
            }
        }
    }

    #[inline]
    const fn num_slots(&self) -> usize {
        self.layout.num_slots
    }

    #[inline]
    const fn num_groups(&self) -> usize {
        self.layout.num_groups
    }

    #[inline]
    const fn ctrl_bytes(&self) -> usize {
        self.layout.ctrl_bytes
    }
}

impl Drop for RawTable {
    fn drop(&mut self) {
        // SAFETY: `allocate` created this allocation with the same size/alignment,
        // and `RawTable` is its unique owner.
        unsafe {
            alloc::dealloc(self.ctrl, self.layout.raw_layout);
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
        let layout = TableLayout::for_capacity(cap);
        let num_slots = layout.num_slots;

        Self {
            raw: RawTable::allocate(layout),
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
    pub fn occupied_slots(&self) -> usize {
        self.occupied
    }

    #[inline]
    pub fn tombstone_slots(&self) -> usize {
        self.occupied.saturating_sub(self.len)
    }

    #[inline]
    pub fn load_factor(&self) -> f64 {
        let total_slots = self.total_slots();
        if total_slots == 0 {
            0.0
        } else {
            self.len as f64 / total_slots as f64
        }
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
            let slot = SlotIndex(slot);
            let live_slot = self.live_slot(slot)?;
            let slot = live_slot.get();
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
            let slot = SlotIndex(slot);
            let live_slot = self.live_slot(slot)?;
            Some(self.raw.entry(live_slot))
        })
    }

    #[inline]
    pub fn memory_used(&self) -> usize {
        self.memory_used
    }

    #[inline]
    pub fn allocated_bytes(&self) -> usize {
        let key_slots_bytes = self.keys.capacity() * size_of::<Option<VortexKey>>();
        let value_slots_bytes = self.values.capacity() * size_of::<Option<VortexValue>>();

        if key_slots_bytes == self.raw.layout.key_slots_bytes
            && value_slots_bytes == self.raw.layout.value_slots_bytes
        {
            debug_assert_eq!(
                self.raw.layout.total_allocated_bytes,
                self.raw.layout.raw_alloc_size + key_slots_bytes + value_slots_bytes
            );
            return self.raw.layout.total_allocated_bytes;
        }

        self.raw.layout.raw_alloc_size + key_slots_bytes + value_slots_bytes
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
            let bytes = drift as usize;
            let _ = memory_used
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current.checked_add(bytes)
                })
                .expect("SwissTable: global memory_used overflow");
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
    #[inline(always)]
    pub fn insert(&mut self, key: VortexKey, value: VortexValue) -> Option<VortexValue> {
        self.insert_with_lsn(key, value, None)
    }

    /// Inserts a key-value pair and records `lsn` in the entry metadata.
    #[inline(always)]
    pub fn insert_with_lsn(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        lsn: Option<u64>,
    ) -> Option<VortexValue> {
        let hash = self.hash_key(key.as_bytes());
        self.upsert_internal(key, value, hash, MutationPolicy::preserve_ttl(lsn))
            .previous
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
        self.values[slot.get()].as_ref()
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
        self.delete_live_slot(slot)
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
        let slot = self.slot_index(slot)?;
        let live_slot = self.live_slot(slot)?;
        let slot_idx = live_slot.get();
        let key = self.keys[slot_idx].as_ref()?;
        let value = self.values[slot_idx].as_ref()?;
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
        let slot = self.slot_index(slot)?;
        let live_slot = self.live_slot(slot)?;
        Some(self.raw.entry(live_slot))
    }

    /// Deletes a live slot by index and returns its stored value.
    ///
    /// Note: This is a low-level API that directly accesses the slot by index.
    /// If the slot is out of bounds or not live, it returns `None`.
    #[inline]
    pub fn delete_slot(&mut self, slot: usize) -> Option<VortexValue> {
        let slot = self.slot_index(slot)?;
        let live_slot = self.live_slot(slot)?;
        self.delete_live_slot(live_slot)
    }

    #[inline]
    fn delete_live_slot(&mut self, live_slot: LiveSlot) -> Option<VortexValue> {
        let slot = live_slot.index();
        let bytes = self.slot_memory_usage(live_slot);
        let entry = self.raw.entry_mut(live_slot);
        entry.mark_deleted();
        self.raw.set_ctrl(slot, CTRL_DELETED);

        self.len -= 1;
        self.record_memory_delta(-Self::positive_memory_delta(bytes));
        let slot_idx = slot.get();
        self.keys[slot_idx] = None;
        self.values[slot_idx].take()
    }

    /// Returns the memory usage of `slot`, or `0` when the slot is not live.
    ///
    /// Note: This is a low-level API that directly accesses the slot by index.
    /// It checks the control byte to determine if the slot is occupied, and if so,
    /// calculates the memory usage of the key and value stored in the slot.
    #[inline]
    pub fn slot_memory_bytes(&self, slot: usize) -> usize {
        self.slot_index(slot)
            .and_then(|slot| self.live_slot(slot))
            .map_or(0, |slot| self.slot_memory_usage(slot))
    }

    /// Expose the hash function for external callers (e.g. ExpiryWheel).
    #[inline]
    pub fn hash_key_bytes(&self, key: &[u8]) -> u64 {
        self.hash_key(key).get()
    }

    /// Hash key bytes with this table's hasher for crate-internal prehashed APIs.
    #[inline]
    pub(crate) fn table_hash_key_bytes(&self, key: &[u8]) -> TableHash {
        self.hash_key(key)
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
    #[inline(always)]
    pub fn insert_with(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        ttl_deadline: u64,
        lsn: Option<u64>,
    ) -> Option<VortexValue> {
        let hash = self.hash_key(key.as_bytes());
        self.upsert_internal(key, value, hash, MutationPolicy::set(ttl_deadline, lsn))
            .previous
    }

    /// Returns the TTL deadline (nanos) for the entry at `slot`, or 0 if
    /// the slot is empty/deleted or has no TTL.
    #[inline]
    pub fn slot_entry_ttl(&self, slot: usize) -> u64 {
        self.slot_index(slot)
            .and_then(|slot| self.live_slot(slot))
            .map_or(0, |slot| self.raw.entry(slot).ttl_deadline())
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
        let value = self.values[slot.get()].as_ref()?;
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

#[derive(Clone, Copy)]
enum TtlPolicy {
    Clear,
    Preserve,
    Set(u64),
}

#[derive(Clone, Copy)]
pub(crate) struct MutationPolicy {
    ttl: TtlPolicy,
    lsn: Option<u64>,
}

impl MutationPolicy {
    #[inline(always)]
    pub(crate) const fn clear(lsn: Option<u64>) -> Self {
        Self {
            ttl: TtlPolicy::Clear,
            lsn,
        }
    }

    #[inline(always)]
    pub(crate) const fn preserve_ttl(lsn: Option<u64>) -> Self {
        Self {
            ttl: TtlPolicy::Preserve,
            lsn,
        }
    }

    #[inline(always)]
    pub(crate) const fn set(ttl_deadline: u64, lsn: Option<u64>) -> Self {
        Self {
            ttl: TtlPolicy::Set(ttl_deadline),
            lsn,
        }
    }

    #[inline(always)]
    const fn ttl_for_new(self) -> u64 {
        match self.ttl {
            TtlPolicy::Clear | TtlPolicy::Preserve => 0,
            TtlPolicy::Set(ttl_deadline) => ttl_deadline,
        }
    }

    #[inline(always)]
    const fn ttl_for_existing(self, current_ttl: u64) -> u64 {
        match self.ttl {
            TtlPolicy::Clear => 0,
            TtlPolicy::Preserve => current_ttl,
            TtlPolicy::Set(ttl_deadline) => ttl_deadline,
        }
    }
}

#[allow(dead_code)]
impl<'a> SlotCursor<'a> {
    #[inline]
    pub(crate) fn old_ttl(&self) -> Option<u64> {
        match self {
            Self::Live(cursor) => Some(cursor.ttl_deadline()),
            Self::Expired(cursor) => Some(cursor.ttl_deadline()),
            Self::Vacant(_) => None,
        }
    }

    #[inline]
    pub(crate) fn is_live(&self) -> bool {
        matches!(self, Self::Live(_))
    }
}

#[allow(dead_code)]
impl<'a> LiveSlotCursor<'a> {
    #[inline]
    pub(crate) const fn ttl_deadline(&self) -> u64 {
        self.ttl_deadline
    }

    #[inline]
    pub(crate) const fn had_ttl(&self) -> bool {
        self.ttl_deadline != 0
    }

    #[inline]
    pub(crate) fn value(&self) -> &VortexValue {
        self.table.values[self.slot.get()]
            .as_ref()
            .expect("live slot cursor must reference a value")
    }

    #[inline]
    pub(crate) fn cloned_value(&self) -> VortexValue {
        self.value().clone()
    }

    #[inline]
    pub(crate) fn memory_bytes(&self) -> usize {
        self.table.slot_memory_usage(self.slot)
    }

    #[inline]
    pub(crate) fn lsn_version(&self) -> u64 {
        self.table.raw.entry(self.slot).lsn_version()
    }

    #[inline]
    pub(crate) fn set_ttl(&mut self, deadline_nanos: u64, lsn: Option<u64>) -> SlotMutationReport {
        let old_ttl = self.ttl_deadline;
        let memory_bytes = self.memory_bytes();
        let entry = self.table.raw.entry_mut(self.slot);
        entry.set_ttl(deadline_nanos);
        if let Some(lsn) = lsn {
            entry.set_lsn_version(lsn);
        }
        self.ttl_deadline = deadline_nanos;
        SlotMutationReport::ttl_only(
            self.slot,
            old_ttl,
            deadline_nanos,
            memory_bytes,
            lsn.is_some(),
        )
    }

    #[inline]
    pub(crate) fn clear_ttl(&mut self, lsn: Option<u64>) -> SlotMutationReport {
        self.set_ttl(0, lsn)
    }

    #[inline]
    pub(crate) fn replace_value(
        self,
        value: VortexValue,
        policy: MutationPolicy,
    ) -> SlotMutationReport {
        let old_ttl = self.ttl_deadline;
        let new_ttl = policy.ttl_for_existing(old_ttl);
        let old_bytes = self.table.slot_memory_usage(self.slot);
        let previous =
            self.table
                .replace_slot_value(self.slot, self.hash.h2(), value, new_ttl, policy.lsn);
        let new_bytes = self.table.slot_memory_usage(self.slot);
        self.table
            .record_memory_delta(SwissTable::memory_delta_between(new_bytes, old_bytes));
        SlotMutationReport::replaced(
            self.slot,
            previous,
            old_ttl,
            new_ttl,
            old_bytes,
            new_bytes,
            policy.lsn.is_some(),
        )
    }

    #[inline]
    pub(crate) fn remove(self) -> Option<SlotRemoval> {
        remove_cursor_slot(self.table, self.slot, self.ttl_deadline)
    }
}

#[allow(dead_code)]
impl<'a> ExpiredSlot<'a> {
    #[inline]
    pub(crate) const fn ttl_deadline(&self) -> u64 {
        self.ttl_deadline
    }

    #[inline]
    pub(crate) fn remove(self) -> Option<SlotRemoval> {
        remove_cursor_slot(self.table, self.slot, self.ttl_deadline)
    }
}

impl<'a> VacantSlot<'a> {
    #[inline]
    pub(crate) fn insert(
        self,
        key: VortexKey,
        value: VortexValue,
        policy: MutationPolicy,
    ) -> SlotMutationReport {
        let ttl = policy.ttl_for_new();
        self.table.ensure_capacity_for_insert();

        let slot = self.table.find_insert_slot(self.hash);
        let was_empty = self.table.raw.ctrl(slot) == CTRL_EMPTY;
        let h2 = self.hash.h2();
        self.table
            .write_new_slot(slot, h2, key, value, ttl, policy.lsn);
        self.table.finish_new_slot_insert(slot, was_empty);
        let live_slot = self
            .table
            .live_slot(slot)
            .expect("inserted slot must be live");
        SlotMutationReport::inserted(
            live_slot,
            ttl,
            self.table.slot_memory_usage(live_slot),
            policy.lsn.is_some(),
        )
    }
}

#[inline]
fn remove_cursor_slot(
    table: &mut SwissTable,
    slot: LiveSlot,
    ttl_deadline: u64,
) -> Option<SlotRemoval> {
    let old_memory_bytes = table.slot_memory_usage(slot);
    table.delete_live_slot(slot).map(|value| SlotRemoval {
        value,
        old_ttl: ttl_deadline,
        old_memory_bytes,
    })
}

pub(crate) struct BorrowedKey<'a>(pub(crate) &'a [u8]);

pub(crate) struct RawValueBytes<'a>(pub(crate) &'a [u8]);

pub(crate) trait TableMutationKey {
    fn as_bytes(&self) -> &[u8];
    fn replace_existing(self, table: &mut SwissTable, slot: LiveSlot);
    fn into_owned(self) -> VortexKey;
}

impl TableMutationKey for VortexKey {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    #[inline]
    fn replace_existing(self, table: &mut SwissTable, slot: LiveSlot) {
        table.keys[slot.get()] = Some(self);
    }

    #[inline]
    fn into_owned(self) -> VortexKey {
        self
    }
}

impl TableMutationKey for BorrowedKey<'_> {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.0
    }

    #[inline]
    fn replace_existing(self, _table: &mut SwissTable, _slot: LiveSlot) {}

    #[inline]
    fn into_owned(self) -> VortexKey {
        VortexKey::from(self.0)
    }
}

pub(crate) trait TableMutationValue {
    fn replace_existing(
        self,
        table: &mut SwissTable,
        slot: LiveSlot,
        h2: u8,
        ttl: u64,
        lsn: Option<u64>,
    ) -> Option<VortexValue>;

    fn insert_new(
        self,
        table: &mut SwissTable,
        slot: SlotIndex,
        h2: u8,
        key: VortexKey,
        ttl: u64,
        lsn: Option<u64>,
    );
}

impl TableMutationValue for VortexValue {
    #[inline(always)]
    fn replace_existing(
        self,
        table: &mut SwissTable,
        slot: LiveSlot,
        h2: u8,
        ttl: u64,
        lsn: Option<u64>,
    ) -> Option<VortexValue> {
        table.replace_slot_value(slot, h2, self, ttl, lsn)
    }

    #[inline(always)]
    fn insert_new(
        self,
        table: &mut SwissTable,
        slot: SlotIndex,
        h2: u8,
        key: VortexKey,
        ttl: u64,
        lsn: Option<u64>,
    ) {
        table.write_new_slot(slot, h2, key, self, ttl, lsn);
    }
}

impl TableMutationValue for RawValueBytes<'_> {
    #[inline(always)]
    fn replace_existing(
        self,
        table: &mut SwissTable,
        slot: LiveSlot,
        h2: u8,
        ttl: u64,
        lsn: Option<u64>,
    ) -> Option<VortexValue> {
        let slot_idx = slot.get();
        let previous = table.values[slot_idx]
            .take()
            .expect("live slot must have value");
        table.values[slot_idx] = Some(VortexValue::from_bytes_reusing(previous, self.0));
        table.rewrite_slot_entry(slot.index(), h2, ttl, lsn);
        None
    }

    #[inline(always)]
    fn insert_new(
        self,
        table: &mut SwissTable,
        slot: SlotIndex,
        h2: u8,
        key: VortexKey,
        ttl: u64,
        lsn: Option<u64>,
    ) {
        table.write_new_slot(slot, h2, key, VortexValue::from_bytes(self.0), ttl, lsn);
    }
}

#[must_use]
pub(crate) struct UpsertOutcome {
    previous: Option<VortexValue>,
    had_ttl: bool,
}

impl UpsertOutcome {
    #[inline]
    fn inserted() -> Self {
        Self {
            previous: None,
            had_ttl: false,
        }
    }

    #[inline]
    fn replaced(previous: Option<VortexValue>, had_ttl: bool) -> Self {
        Self { previous, had_ttl }
    }

    #[inline(always)]
    pub(crate) const fn had_ttl(&self) -> bool {
        self.had_ttl
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
            self.memory_used = self
                .memory_used
                .checked_add(delta as usize)
                .expect("SwissTable: memory_used overflow");
        } else {
            self.memory_used = self
                .memory_used
                .checked_sub(delta.unsigned_abs())
                .expect("SwissTable: memory_used underflow");
        }

        self.memory_drift = self
            .memory_drift
            .checked_add(delta)
            .expect("SwissTable: memory_drift overflow");
    }

    #[inline]
    fn positive_memory_delta(bytes: usize) -> isize {
        isize::try_from(bytes).expect("SwissTable: memory delta exceeds isize::MAX")
    }

    #[inline]
    fn memory_delta_between(new_bytes: usize, old_bytes: usize) -> isize {
        if new_bytes >= old_bytes {
            Self::positive_memory_delta(new_bytes - old_bytes)
        } else {
            -Self::positive_memory_delta(old_bytes - new_bytes)
        }
    }

    #[inline]
    const fn capacity(&self) -> usize {
        self.raw.num_slots()
    }

    #[inline]
    fn slot_index(&self, slot: usize) -> Option<SlotIndex> {
        SlotIndex::checked(slot, self.capacity())
    }

    #[inline]
    fn live_slot(&self, slot: SlotIndex) -> Option<LiveSlot> {
        let ctrl = self.raw.ctrl(slot);
        (ctrl != CTRL_EMPTY && ctrl != CTRL_DELETED).then_some(LiveSlot::new(slot))
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
    fn slot_memory_usage(&self, slot: LiveSlot) -> usize {
        let slot_idx = slot.get();
        let key = self.keys[slot_idx]
            .as_ref()
            .expect("live slot must have key");
        let value = self.values[slot_idx]
            .as_ref()
            .expect("live slot must have value");
        Self::entry_memory_usage(key, value)
    }

    #[inline(always)]
    fn ensure_capacity_for_insert(&mut self) {
        if self.occupied >= self.growth_limit() {
            self.resize();
        }
    }

    #[inline(always)]
    fn finish_new_slot_insert(&mut self, slot: SlotIndex, was_empty: bool) {
        self.len += 1;
        if was_empty {
            self.occupied += 1;
        }
        let live_slot = self.live_slot(slot).expect("new slot must be live");
        self.record_memory_delta(Self::positive_memory_delta(
            self.slot_memory_usage(live_slot),
        ));
    }

    #[inline(always)]
    fn rewrite_slot_entry(&mut self, slot: SlotIndex, h2: u8, ttl: u64, lsn: Option<u64>) {
        let slot_idx = slot.get();
        let key = self.keys[slot_idx]
            .as_ref()
            .expect("live slot must have key");
        let value = self.values[slot_idx]
            .as_ref()
            .expect("live slot must have value");

        let entry = self.raw.entry_mut_for_publish(slot);
        Self::write_entry(entry, h2, key, value, ttl);
        if let Some(lsn) = lsn {
            entry.set_lsn_version(lsn);
        }
        self.raw.set_ctrl(slot, h2);
    }

    #[inline(always)]
    fn write_new_slot(
        &mut self,
        slot: SlotIndex,
        h2: u8,
        key: VortexKey,
        value: VortexValue,
        ttl: u64,
        lsn: Option<u64>,
    ) {
        let slot_idx = slot.get();
        self.keys[slot_idx] = Some(key);
        self.values[slot_idx] = Some(value);
        self.rewrite_slot_entry(slot, h2, ttl, lsn);
    }

    #[inline(always)]
    fn replace_slot_value(
        &mut self,
        slot: LiveSlot,
        h2: u8,
        value: VortexValue,
        ttl: u64,
        lsn: Option<u64>,
    ) -> Option<VortexValue> {
        let slot_idx = slot.get();
        let previous = self.values[slot_idx].replace(value);
        self.rewrite_slot_entry(slot.index(), h2, ttl, lsn);
        previous
    }

    #[inline(always)]
    fn upsert_internal<K, V>(
        &mut self,
        key: K,
        value: V,
        hash: TableHash,
        policy: MutationPolicy,
    ) -> UpsertOutcome
    where
        K: TableMutationKey,
        V: TableMutationValue,
    {
        let h2 = hash.h2();

        if let Some(slot) = self.find_slot(key.as_bytes(), hash) {
            let old_ttl = self.raw.entry(slot).ttl_deadline();
            let ttl = policy.ttl_for_existing(old_ttl);
            let old_bytes = self.slot_memory_usage(slot);
            key.replace_existing(self, slot);
            let previous = value.replace_existing(self, slot, h2, ttl, policy.lsn);
            let new_bytes = self.slot_memory_usage(slot);
            self.record_memory_delta(Self::memory_delta_between(new_bytes, old_bytes));
            return UpsertOutcome::replaced(previous, old_ttl != 0);
        }

        self.ensure_capacity_for_insert();

        let slot = self.find_insert_slot(hash);
        let was_empty = self.raw.ctrl(slot) == CTRL_EMPTY;
        let ttl = policy.ttl_for_new();
        let key = key.into_owned();
        value.insert_new(self, slot, h2, key, ttl, policy.lsn);
        self.finish_new_slot_insert(slot, was_empty);
        UpsertOutcome::inserted()
    }

    #[inline]
    fn group_mask(&self) -> usize {
        self.raw.num_groups() - 1
    }

    /// Hash key bytes using ahash.
    #[inline]
    fn hash_key(&self, key: &[u8]) -> TableHash {
        TableHash(self.hasher.hash_one(key))
    }

    /// Find the slot of an existing key, or `None`.
    #[inline]
    fn find_slot(&self, key_bytes: &[u8], hash: TableHash) -> Option<LiveSlot> {
        let h2 = hash.h2();
        let h1 = hash.h1();
        let mask = self.group_mask();
        let mut probe = ProbeSeq::new(h1, mask);

        loop {
            let ctrl_ptr = self.raw.ctrl_group(probe.pos);
            let matches = Group::match_h2(ctrl_ptr, h2);

            for bit in matches {
                let slot = SlotIndex::from_group_offset(probe.pos, bit);
                let live_slot = LiveSlot::new(slot);
                let entry = self.raw.entry(live_slot);
                if entry.matches_key(key_bytes) {
                    return Some(live_slot);
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
    fn find_insert_slot(&self, hash: TableHash) -> SlotIndex {
        debug_assert!(
            self.occupied < self.capacity(),
            "find_insert_slot requires at least one available slot"
        );

        let h1 = hash.h1();
        let mask = self.group_mask();
        let mut probe = ProbeSeq::new(h1, mask);

        loop {
            let ctrl_ptr = self.raw.ctrl_group(probe.pos);
            let candidates = Group::match_empty_or_deleted(ctrl_ptr);
            if let Some(bit) = candidates.lowest() {
                return SlotIndex::from_group_offset(probe.pos, bit);
            }
            probe.advance();
        }
    }

    /// Double the table and rehash all live entries. Clears tombstones.
    fn resize(&mut self) {
        let new_num_groups = self
            .raw
            .num_groups()
            .checked_mul(2)
            .expect("SwissTable: group count overflow during resize")
            .max(MIN_GROUPS);
        let layout = TableLayout::for_groups(new_num_groups);
        let new_num_slots = layout.num_slots;
        let mut new_raw = RawTable::allocate(layout);
        let new_mask = new_num_groups - 1;

        let old_num_slots = self.raw.num_slots();
        let mut new_keys: Vec<Option<VortexKey>> = vec![None; new_num_slots];
        let mut new_values: Vec<Option<VortexValue>> = vec![None; new_num_slots];

        for slot in 0..old_num_slots {
            let slot_index = SlotIndex(slot);
            let Some(live_slot) = self.live_slot(slot_index) else {
                continue;
            };

            let old_entry = self.raw.entry(live_slot);
            let ttl = old_entry.ttl_deadline();
            let lsn = old_entry.lsn_version();
            let morris_counter = old_entry.morris_counter();
            let access_profile = old_entry.access_profile();
            let key_bytes = self.keys[slot]
                .as_ref()
                .expect("live slot must have key")
                .as_bytes();
            let hash = self.hash_key(key_bytes);
            let h2 = hash.h2();
            let h1 = hash.h1();

            // Find empty slot in new table.
            let mut probe = ProbeSeq::new(h1, new_mask);
            let new_slot = loop {
                let gctrl = new_raw.ctrl_group(probe.pos);
                let empties = Group::match_empty(gctrl);
                if let Some(bit) = empties.lowest() {
                    break SlotIndex::from_group_offset(probe.pos, bit);
                }
                probe.advance();
            };

            let new_slot_idx = new_slot.get();
            new_keys[new_slot_idx] = self.keys[slot].take();
            new_values[new_slot_idx] = self.values[slot].take();

            let key = new_keys[new_slot_idx]
                .as_ref()
                .expect("rehash slot must have key");
            let value = new_values[new_slot_idx]
                .as_ref()
                .expect("rehash slot must have value");

            let entry = new_raw.entry_mut_for_publish(new_slot);
            Self::write_entry(entry, h2, key, value, ttl);
            entry.set_morris_counter(morris_counter);
            entry.set_access_profile(access_profile);
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
    fn record_access_slot(&self, slot: LiveSlot, random: u64) {
        let entry = self.raw.entry(slot);
        entry.record_access(random);
    }
}

/**
 * Prehashed operations are used by batch pipelines to reduce redundant hashing
 * and key materialization.
 *
 * These APIs accept [`TableHash`] rather than a bare `u64`. The hash must come
 * from this table's hasher, or from the shared table hasher owned by
 * `ConcurrentKeyspace`; shard-routing hashes are a different contract.
 */
impl SwissTable {
    /// Estimates the local-memory delta of inserting or replacing `key` using `hash`.
    #[inline]
    pub(crate) fn projected_insert_delta_prehashed(
        &self,
        key: &VortexKey,
        value: &VortexValue,
        hash: TableHash,
    ) -> isize {
        let new_bytes = Self::entry_memory_usage(key, value);
        match self.find_slot(key.as_bytes(), hash) {
            Some(slot) => Self::memory_delta_between(new_bytes, self.slot_memory_usage(slot)),
            None => Self::positive_memory_delta(new_bytes),
        }
    }

    #[inline(always)]
    pub(crate) fn mutate_prehashed<K, V>(
        &mut self,
        key: K,
        value: V,
        hash: TableHash,
        policy: MutationPolicy,
    ) -> UpsertOutcome
    where
        K: TableMutationKey,
        V: TableMutationValue,
    {
        self.upsert_internal(key, value, hash, policy)
    }

    #[inline(always)]
    pub(crate) fn replace_prehashed<V>(
        &mut self,
        key_bytes: &[u8],
        value: V,
        hash: TableHash,
        policy: MutationPolicy,
    ) -> Option<VortexValue>
    where
        V: TableMutationValue,
    {
        let slot = self.find_slot(key_bytes, hash)?;
        let ttl = policy.ttl_for_existing(self.raw.entry(slot).ttl_deadline());
        let h2 = hash.h2();
        let old_bytes = self.slot_memory_usage(slot);
        let previous = value.replace_existing(self, slot, h2, ttl, policy.lsn);
        let new_bytes = self.slot_memory_usage(slot);
        self.record_memory_delta(Self::memory_delta_between(new_bytes, old_bytes));
        previous
    }

    /// Like [`remove_with_ttl`](Self::remove_with_ttl) but uses raw bytes and a
    /// pre-computed hash so delete-style commands can skip `VortexKey`
    /// materialization on the hot path.
    #[inline]
    pub(crate) fn remove_with_ttl_prehashed(
        &mut self,
        key_bytes: &[u8],
        hash: TableHash,
    ) -> Option<(VortexValue, u64)> {
        let slot = self.find_slot(key_bytes, hash)?;

        let ttl = self.raw.entry(slot).ttl_deadline();

        let value = self.delete_live_slot(slot)?;
        Some((value, ttl))
    }

    /// Like [`get_with_ttl`](Self::get_with_ttl) but uses raw bytes and a
    /// pre-computed hash so batch pipelines do not rebuild `VortexKey`s or
    /// re-hash the same key on the hot path.
    #[inline]
    pub(crate) fn get_with_ttl_prehashed(
        &self,
        key_bytes: &[u8],
        hash: TableHash,
    ) -> Option<(&VortexValue, u64)> {
        let slot = self.find_slot(key_bytes, hash)?;
        let ttl = self.raw.entry(slot).ttl_deadline();
        let value = self.values[slot.get()].as_ref()?;
        Some((value, ttl))
    }

    /// Returns value, TTL, and entry LSN/version from one prehashed lookup.
    #[inline]
    pub(crate) fn get_value_ttl_lsn_prehashed(
        &self,
        key_bytes: &[u8],
        hash: TableHash,
    ) -> Option<(&VortexValue, u64, u64)> {
        let slot = self.find_slot(key_bytes, hash)?;
        let entry = self.raw.entry(slot);
        let value = self.values[slot.get()].as_ref()?;
        Some((value, entry.ttl_deadline(), entry.lsn_version()))
    }

    /// Returns a typed cursor for one prehashed slot lookup.
    ///
    /// The cursor keeps the mutable table borrow, so live-slot mutations can
    /// reuse the observed slot instead of repeating the same key probe.
    #[inline]
    pub(crate) fn slot_cursor_prehashed(
        &mut self,
        key_bytes: &[u8],
        hash: TableHash,
        now_nanos: u64,
    ) -> SlotCursor<'_> {
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return SlotCursor::Vacant(VacantSlot { table: self, hash });
        };

        let ttl_deadline = self.raw.entry(slot).ttl_deadline();
        if ttl_deadline != 0 && ttl_deadline <= now_nanos {
            return SlotCursor::Expired(ExpiredSlot {
                table: self,
                slot,
                ttl_deadline,
            });
        }

        SlotCursor::Live(LiveSlotCursor {
            table: self,
            slot,
            hash,
            ttl_deadline,
        })
    }

    /// Stamp the slot referenced by a cursor mutation report without another
    /// key probe.
    #[inline]
    pub(crate) fn stamp_report_lsn(
        &mut self,
        report: &mut SlotMutationReport,
        lsn: Option<u64>,
    ) -> bool {
        let (Some(slot), Some(lsn)) = (report.live_slot, lsn) else {
            return false;
        };
        debug_assert!(
            self.live_slot(slot.index()).is_some(),
            "cursor mutation report must reference a live slot"
        );
        self.raw.entry_mut(slot).set_lsn_version(lsn);
        report.entry_lsn_stamped = true;
        true
    }

    /// Like `get_or_expire` but uses a pre-computed hash.
    #[allow(dead_code)]
    pub(crate) fn get_or_expire_prehashed(
        &mut self,
        key_bytes: &[u8],
        hash: TableHash,
        now_nanos: u64,
    ) -> Option<&VortexValue> {
        let slot = self.find_slot(key_bytes, hash)?;

        let entry = self.raw.entry(slot);
        if entry.is_expired(now_nanos) {
            let _ = self.delete_live_slot(slot);
            return None;
        }

        self.values[slot.get()].as_ref()
    }

    /// Returns the stored LSN/version for `key_bytes` when present.
    #[inline]
    pub(crate) fn get_lsn_version_prehashed(
        &self,
        key_bytes: &[u8],
        hash: TableHash,
    ) -> Option<u64> {
        let slot = self.find_slot(key_bytes, hash)?;
        Some(self.raw.entry(slot).lsn_version())
    }

    /// Updates the stored LSN/version for `key_bytes` when present.
    #[inline]
    pub(crate) fn set_lsn_version_prehashed(
        &mut self,
        key_bytes: &[u8],
        hash: TableHash,
        lsn: u64,
    ) -> bool {
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return false;
        };
        let entry = self.raw.entry_mut(slot);
        entry.set_lsn_version(lsn);
        true
    }

    /// Check existence with a pre-computed hash (no rehashing).
    #[inline]
    pub(crate) fn contains_key_prehashed(&self, key_bytes: &[u8], hash: TableHash) -> bool {
        self.find_slot(key_bytes, hash).is_some()
    }

    /// Records an access on a known slot identified by a precomputed hash.
    #[inline]
    pub(crate) fn record_access_prehashed(
        &self,
        key_bytes: &[u8],
        hash: TableHash,
        random: u64,
    ) -> bool {
        let Some(slot) = self.find_slot(key_bytes, hash) else {
            return false;
        };
        self.record_access_slot(slot, random);
        true
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
        let hash = TableHash::from_u64(hash);
        let group_idx = GroupIndex::masked(hash.h1(), self.group_mask());
        // Prefetch the 16-byte control array for this group.
        let ctrl_ptr = self.raw.ctrl_group(group_idx);
        crate::prefetch::prefetch_read(ctrl_ptr);
        // Prefetch the first entry slot in the group (64-byte cache line).
        let entry_ptr = self
            .raw
            .entry_ptr(SlotIndex::from_group_offset(group_idx, 0));
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
        let hash = TableHash::from_u64(hash);
        let group_idx = GroupIndex::masked(hash.h1(), self.group_mask());
        let ctrl_ptr = self.raw.ctrl_group(group_idx);
        crate::prefetch::prefetch_write(ctrl_ptr);
        let entry_ptr = self
            .raw
            .entry_ptr(SlotIndex::from_group_offset(group_idx, 0));
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

    #[test]
    fn empty_and_deleted_slots_do_not_expose_entries() {
        let mut table = SwissTable::with_capacity(128);

        for slot in 0..table.total_slots() {
            assert!(table.slot_entry(slot).is_none());
            assert_eq!(table.slot_entry_ttl(slot), 0);
        }

        let key = VortexKey::from("live-slot-safety");
        table.insert(key.clone(), VortexValue::Integer(1));
        let live_slot = (0..table.total_slots())
            .find(|&slot| table.slot_entry(slot).is_some())
            .expect("inserted key must publish one live entry");

        assert!(table.remove(&key).is_some());
        assert!(table.slot_entry(live_slot).is_none());
        assert_eq!(table.slot_entry_ttl(live_slot), 0);
    }
}

#[cfg(test)]
mod tests {
    use crate::entry::EntryValue;
    use crate::morph::AccessProfile;

    use super::*;

    fn live_slot_memory_sum(table: &SwissTable) -> usize {
        (0..table.total_slots())
            .map(|slot| table.slot_memory_bytes(slot))
            .sum()
    }

    fn tombstone_ratio(table: &SwissTable) -> f64 {
        if table.occupied == 0 {
            0.0
        } else {
            (table.occupied - table.len) as f64 / table.occupied as f64
        }
    }

    #[test]
    fn slot_cursor_replace_reports_ttl_memory_and_stamp_status() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("cursor-live");
        table.insert_with(key.clone(), VortexValue::from("old"), 123, Some(7));
        let hash = table.table_hash_key_bytes(key.as_bytes());

        let mut report = match table.slot_cursor_prehashed(key.as_bytes(), hash, 100) {
            SlotCursor::Live(live) => {
                assert_eq!(live.ttl_deadline(), 123);
                assert_eq!(live.lsn_version(), 7);
                live.replace_value(
                    VortexValue::from("new-value"),
                    MutationPolicy::preserve_ttl(None),
                )
            }
            SlotCursor::Expired(_) | SlotCursor::Vacant(_) => panic!("expected live cursor"),
        };

        assert_eq!(report.old_ttl(), 123);
        assert_eq!(report.new_ttl(), 123);
        assert!(report.old_memory_bytes() > 0);
        assert!(report.new_memory_bytes() >= report.old_memory_bytes());
        assert!(!report.entry_lsn_stamped());
        assert!(table.stamp_report_lsn(&mut report, Some(99)));
        assert!(report.entry_lsn_stamped());
        assert_eq!(
            table.get_lsn_version_prehashed(key.as_bytes(), hash),
            Some(99)
        );
    }

    #[test]
    fn slot_cursor_expired_remove_reports_old_ttl_and_memory() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("cursor-expired");
        table.insert_with(key.clone(), VortexValue::from("value"), 10, Some(1));
        let hash = table.table_hash_key_bytes(key.as_bytes());

        let removed = match table.slot_cursor_prehashed(key.as_bytes(), hash, 11) {
            SlotCursor::Expired(expired) => expired.remove().expect("expired slot removal"),
            SlotCursor::Live(_) | SlotCursor::Vacant(_) => panic!("expected expired cursor"),
        };

        assert_eq!(removed.old_ttl(), 10);
        assert!(removed.old_had_ttl());
        assert!(removed.old_memory_bytes() > 0);
        assert!(table.get(&key).is_none());
        assert_eq!(table.tombstone_slots(), 1);
    }

    #[test]
    fn slot_cursor_vacant_insert_reports_new_memory() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("cursor-vacant");
        let hash = table.table_hash_key_bytes(key.as_bytes());

        let report = match table.slot_cursor_prehashed(key.as_bytes(), hash, 0) {
            SlotCursor::Vacant(vacant) => vacant.insert(
                key.clone(),
                VortexValue::from("value"),
                MutationPolicy::set(500, Some(3)),
            ),
            SlotCursor::Live(_) | SlotCursor::Expired(_) => panic!("expected vacant cursor"),
        };

        assert_eq!(report.old_ttl(), 0);
        assert_eq!(report.new_ttl(), 500);
        assert_eq!(report.old_memory_bytes(), 0);
        assert!(report.new_memory_bytes() > 0);
        assert!(report.entry_lsn_stamped());
        assert_eq!(table.get_entry_ttl(&key), Some(500));
        assert_eq!(
            table.get_lsn_version_prehashed(key.as_bytes(), hash),
            Some(3)
        );
    }

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
    fn allocated_bytes_include_slot_storage() {
        let table = SwissTable::with_capacity(128);

        assert!(table.allocated_bytes() > 0);
        assert!(table.allocated_bytes() >= table.raw.layout.raw_alloc_size);
    }

    #[test]
    fn table_layout_accounts_for_raw_and_parallel_slots() {
        let table = SwissTable::with_capacity(128);
        let layout = table.raw.layout;

        assert_eq!(layout.num_slots, table.total_slots());
        assert_eq!(layout.ctrl_padded % 64, 0);
        assert_eq!(layout.entries_bytes, layout.num_slots * size_of::<Entry>());
        assert_eq!(
            layout.total_allocated_bytes,
            layout.raw_alloc_size + layout.key_slots_bytes + layout.value_slots_bytes
        );
        assert_eq!(table.allocated_bytes(), layout.total_allocated_bytes);
    }

    #[test]
    fn table_layout_rejects_overflowing_capacities() {
        assert!(TableLayout::try_for_capacity(usize::MAX).is_none());

        let mut groups = MIN_GROUPS;
        while let Some(next) = groups.checked_mul(2) {
            if TableLayout::try_for_groups(next).is_none() {
                assert!(next.is_power_of_two());
                return;
            }
            groups = next;
        }

        panic!("expected a checked layout rejection before usize group overflow");
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
    #[should_panic(expected = "SwissTable: global memory_used overflow")]
    fn global_memory_drift_overflow_panics() {
        let mut table = SwissTable::new();
        let global = AtomicUsize::new(usize::MAX);

        table.memory_drift = 1;
        table.flush_memory_drift_with(&global, true);
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
    fn empty_or_deleted_group_match_matches_scalar_contract() {
        for byte in u8::MIN..=u8::MAX {
            let ctrl = [byte; GROUP_SIZE];
            let mask = Group::match_empty_or_deleted(ctrl.as_ptr());
            let expected = if byte == CTRL_EMPTY || byte == CTRL_DELETED {
                u16::MAX
            } else {
                0
            };
            assert_eq!(
                mask.0, expected,
                "unexpected mask for ctrl byte {byte:#04x}"
            );
        }

        let mixed = [
            CTRL_EMPTY,
            0x81,
            CTRL_DELETED,
            0xFE,
            0x92,
            CTRL_EMPTY,
            CTRL_DELETED,
            0xA1,
            0xB2,
            0xC3,
            0xD4,
            0xE5,
            0xF6,
            CTRL_EMPTY,
            0x90,
            CTRL_DELETED,
        ];
        let mask = Group::match_empty_or_deleted(mixed.as_ptr());
        let expected = (1 << 0) | (1 << 2) | (1 << 5) | (1 << 6) | (1 << 13) | (1 << 15);
        assert_eq!(mask.0, expected);
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
    fn replacing_existing_key_at_growth_limit_does_not_resize() {
        let mut table = SwissTable::with_capacity(16);
        let growth_limit = table.growth_limit();
        let mut keys = Vec::with_capacity(growth_limit);

        for index in 0..growth_limit {
            let key = VortexKey::from(format!("growth:{index:04}").as_str());
            table.insert(key.clone(), VortexValue::Integer(index as i64));
            keys.push(key);
        }

        assert_eq!(table.occupied_slots(), growth_limit);
        let slots_before = table.total_slots();
        let memory_before = table.memory_used();
        let old = table.insert(keys[0].clone(), VortexValue::Integer(-1));

        assert_eq!(old, Some(VortexValue::Integer(0)));
        assert_eq!(table.total_slots(), slots_before);
        assert_eq!(table.occupied_slots(), growth_limit);
        assert_eq!(table.len(), growth_limit);
        assert_eq!(table.memory_used(), memory_before);
        assert_eq!(table.get(&keys[0]), Some(&VortexValue::Integer(-1)));
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
        let hash = table.table_hash_key_bytes(key.as_bytes());
        table.insert_with(key.clone(), VortexValue::Integer(10), 123, None);

        table
            .replace_prehashed(
                key.as_bytes(),
                VortexValue::from("twenty"),
                hash,
                MutationPolicy::preserve_ttl(Some(77)),
            )
            .expect("key exists");

        assert_eq!(table.get(&key), Some(&VortexValue::from("twenty")));
        assert_eq!(table.get_entry_ttl(&key), Some(123));
    }

    #[test]
    fn update_preserves_eviction_and_access_metadata() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("hot-key");
        let hash = table.table_hash_key_bytes(key.as_bytes());
        table.insert_with(key.clone(), VortexValue::Integer(1), 123, Some(7));

        let slot = table.find_slot(key.as_bytes(), hash).expect("key exists");
        let entry = table.raw.entry_mut(slot);
        let mut profile = AccessProfile::new();
        profile.record_read();
        profile.record_write();
        entry.set_morris_counter(37);
        entry.set_access_profile(profile);

        table.insert_with(key.clone(), VortexValue::from("updated"), 456, Some(8));

        let slot = table
            .find_slot(key.as_bytes(), hash)
            .expect("key still exists");
        let entry = table.raw.entry(slot);
        assert_eq!(entry.morris_counter(), 37);
        assert_eq!(entry.access_profile(), profile);
        assert_eq!(entry.ttl_deadline(), 456);
        assert_eq!(entry.lsn_version(), 8);
    }

    #[test]
    fn resize_preserves_eviction_access_ttl_and_lsn_metadata() {
        let mut table = SwissTable::with_capacity(1);
        let key = VortexKey::from("hot-resize-key");
        let hash = table.table_hash_key_bytes(key.as_bytes());
        table.insert_with(key.clone(), VortexValue::from("before"), 123, Some(41));

        let slot = table.find_slot(key.as_bytes(), hash).expect("key exists");
        let entry = table.raw.entry_mut(slot);
        let mut profile = AccessProfile::new();
        for _ in 0..4 {
            profile.record_read();
        }
        profile.record_write();
        entry.set_morris_counter(55);
        entry.set_access_profile(profile);

        let original_slots = table.total_slots();
        for i in 0..256 {
            let key = VortexKey::from(format!("resize-fill:{i:04}").as_str());
            table.insert(key, VortexValue::Integer(i));
        }
        assert!(table.total_slots() > original_slots);

        let hash = table.table_hash_key_bytes(b"hot-resize-key");
        let slot = table
            .find_slot(b"hot-resize-key", hash)
            .expect("key survives resize");
        let entry = table.raw.entry(slot);
        assert_eq!(entry.morris_counter(), 55);
        assert_eq!(entry.access_profile(), profile);
        assert_eq!(entry.ttl_deadline(), 123);
        assert_eq!(entry.lsn_version(), 41);
    }

    #[test]
    fn mutate_prehashed_clear_policy_clears_ttl_and_updates_lsn() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("clear-ttl");
        let hash = table.table_hash_key_bytes(key.as_bytes());

        table.insert_with(key.clone(), VortexValue::from("before"), 123, Some(7));

        let outcome = table.mutate_prehashed(
            BorrowedKey(key.as_bytes()),
            VortexValue::from("after"),
            hash,
            MutationPolicy::clear(Some(99)),
        );

        assert!(outcome.had_ttl());
        assert_eq!(table.get(&key), Some(&VortexValue::from("after")));
        assert_eq!(table.get_entry_ttl(&key), Some(0));
        assert_eq!(
            table.get_lsn_version_prehashed(key.as_bytes(), hash),
            Some(99)
        );
    }

    #[test]
    fn replace_prehashed_preserve_policy_keeps_ttl() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("preserve-ttl");
        let hash = table.table_hash_key_bytes(key.as_bytes());

        table.insert_with(key.clone(), VortexValue::from("before"), 456, Some(11));

        let previous = table.replace_prehashed(
            key.as_bytes(),
            VortexValue::from("after"),
            hash,
            MutationPolicy::preserve_ttl(Some(12)),
        );

        assert_eq!(previous, Some(VortexValue::from("before")));
        assert_eq!(table.get(&key), Some(&VortexValue::from("after")));
        assert_eq!(table.get_entry_ttl(&key), Some(456));
        assert_eq!(
            table.get_lsn_version_prehashed(key.as_bytes(), hash),
            Some(12)
        );
    }

    #[test]
    fn featureless_update_without_lsn_preserves_entry_version_by_contract() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("featureless-version");
        let hash = table.table_hash_key_bytes(key.as_bytes());

        table.insert_with(key.clone(), VortexValue::from("before"), 0, Some(77));
        table.insert_with_lsn(key.clone(), VortexValue::from("after"), None);

        assert_eq!(table.get(&key), Some(&VortexValue::from("after")));
        assert_eq!(
            table.get_lsn_version_prehashed(key.as_bytes(), hash),
            Some(77)
        );
    }

    #[test]
    fn mutate_prehashed_raw_bytes_reports_old_ttl() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("raw-bytes");
        let hash = table.table_hash_key_bytes(key.as_bytes());

        table.insert_with(key.clone(), VortexValue::from("before"), 789, Some(1));

        let had_ttl = table
            .mutate_prehashed(
                BorrowedKey(key.as_bytes()),
                RawValueBytes(b"payload"),
                hash,
                MutationPolicy::clear(Some(2)),
            )
            .had_ttl();

        assert!(had_ttl);
        assert_eq!(table.get(&key), Some(&VortexValue::from("payload")));
        assert_eq!(table.get_entry_ttl(&key), Some(0));
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
        let watched_hash = table.table_hash_key_bytes(watched.as_bytes());

        let _ = table.mutate_prehashed(
            watched.clone(),
            VortexValue::from("value"),
            watched_hash,
            MutationPolicy::clear(Some(42)),
        );
        assert_eq!(
            table.get_lsn_version_prehashed(watched.as_bytes(), watched_hash),
            Some(42)
        );

        for i in 0..256 {
            let key = VortexKey::from(format!("key:{i:04}").as_str());
            table.insert(key, VortexValue::Integer(i));
        }

        let watched_hash = table.table_hash_key_bytes(watched.as_bytes());
        assert_eq!(
            table.get_lsn_version_prehashed(watched.as_bytes(), watched_hash),
            Some(42)
        );
    }

    #[test]
    fn resize_after_pointer_move_rewrites_heap_entry_pointers() {
        let mut table = SwissTable::with_capacity(1);
        let key_bytes = [b'k'; vortex_common::MAX_INLINE_KEY_LEN + 17];
        let value_bytes = [b'v'; vortex_common::MAX_INLINE_VALUE_LEN + 65];
        let key = VortexKey::from_bytes(&key_bytes);
        let value = VortexValue::from_bytes(&value_bytes);
        let hash = table.table_hash_key_bytes(key.as_bytes());

        table.insert_with(key.clone(), value.clone(), 987_654, Some(1234));
        let original_slots = table.total_slots();

        for i in 0..512 {
            let fill_key = VortexKey::from_bytes(format!("heap-fill-key:{i:04}:suffix").as_bytes());
            let fill_value = VortexValue::from_bytes(&vec![b'x'; 64 + (i % 7)]);
            table.insert(fill_key, fill_value);
        }
        assert!(table.total_slots() > original_slots);

        assert_eq!(table.get(&key), Some(&value));
        assert_eq!(table.get_entry_ttl(&key), Some(987_654));
        assert_eq!(
            table.get_lsn_version_prehashed(key.as_bytes(), hash),
            Some(1234)
        );

        let entry = table
            .iter_entries()
            .find(|entry| entry.matches_key(key.as_bytes()))
            .expect("heap entry survives resize");
        assert_eq!(entry.read_key(), key.as_bytes());
        assert_eq!(entry.read_value(), EntryValue::Heap(&value));
    }

    #[test]
    fn memory_and_tombstone_ratios_remain_consistent_after_delete_heavy_workload() {
        let mut table = SwissTable::with_capacity(128);

        for i in 0..96 {
            let key = VortexKey::from(format!("ratio-key:{i:04}").as_str());
            let value = VortexValue::from_bytes(&vec![b'v'; 8 + (i % 40)]);
            table.insert(key, value);
        }

        let allocated_before_delete = table.allocated_bytes();
        for i in (0..96).step_by(3) {
            let key = VortexKey::from(format!("ratio-key:{i:04}").as_str());
            assert!(table.remove(&key).is_some());
        }

        assert_eq!(table.memory_used(), live_slot_memory_sum(&table));
        assert_eq!(table.len(), 64);
        assert_eq!(table.occupied, 96);
        assert_eq!(tombstone_ratio(&table), 32.0 / 96.0);
        assert_eq!(table.allocated_bytes(), allocated_before_delete);
        assert!(table.allocated_bytes() >= table.memory_used());
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

    use super::{BorrowedKey, MutationPolicy, RawValueBytes, SwissTable};

    /// Generate a random key string 1..=30 bytes.
    fn arb_key() -> impl Strategy<Value = String> {
        "[a-z0-9]{1,30}"
    }

    /// Generate a random integer value.
    fn arb_int_value() -> impl Strategy<Value = i64> {
        proptest::num::i64::ANY
    }

    fn arb_key_bytes() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            proptest::collection::vec(any::<u8>(), 0..=vortex_common::MAX_INLINE_KEY_LEN),
            proptest::collection::vec(any::<u8>(), vortex_common::MAX_INLINE_KEY_LEN + 1..=64),
        ]
    }

    fn arb_value_bytes() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            proptest::collection::vec(any::<u8>(), 0..=vortex_common::MAX_INLINE_VALUE_LEN),
            proptest::collection::vec(any::<u8>(), vortex_common::MAX_INLINE_VALUE_LEN + 1..=96),
        ]
    }

    fn arb_optional_lsn() -> impl Strategy<Value = Option<u64>> {
        prop_oneof![Just(None), (0u64..1_000_000).prop_map(Some)]
    }

    fn value_bytes(value: &VortexValue) -> Vec<u8> {
        value
            .as_string_bytes()
            .expect("storage metadata proptest only writes string values")
            .to_vec()
    }

    fn live_slot_memory_sum(table: &SwissTable) -> usize {
        (0..table.total_slots())
            .map(|slot| table.slot_memory_bytes(slot))
            .sum()
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

    #[derive(Clone, Debug)]
    struct ModelEntry {
        value: Vec<u8>,
        ttl: u64,
        lsn: u64,
    }

    #[derive(Clone, Debug)]
    enum StorageOp {
        Insert {
            key: Vec<u8>,
            value: Vec<u8>,
            ttl: u64,
            lsn: Option<u64>,
        },
        RawBytes {
            key: Vec<u8>,
            value: Vec<u8>,
            ttl: u64,
            lsn: Option<u64>,
        },
        Remove(Vec<u8>),
        Get(Vec<u8>),
    }

    fn arb_storage_op() -> impl Strategy<Value = StorageOp> {
        prop_oneof![
            (
                arb_key_bytes(),
                arb_value_bytes(),
                0u64..1_000_000,
                arb_optional_lsn(),
            )
                .prop_map(|(key, value, ttl, lsn)| StorageOp::Insert {
                    key,
                    value,
                    ttl,
                    lsn,
                }),
            (
                arb_key_bytes(),
                arb_value_bytes(),
                0u64..1_000_000,
                arb_optional_lsn(),
            )
                .prop_map(|(key, value, ttl, lsn)| StorageOp::RawBytes {
                    key,
                    value,
                    ttl,
                    lsn,
                }),
            arb_key_bytes().prop_map(StorageOp::Remove),
            arb_key_bytes().prop_map(StorageOp::Get),
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

        #[test]
        fn storage_metadata_invariants_match_model(
            ops in proptest::collection::vec(arb_storage_op(), 1..200),
        ) {
            let mut st = SwissTable::with_capacity(1);
            let mut model: HashMap<Vec<u8>, ModelEntry> = HashMap::new();

            for op in ops {
                match op {
                    StorageOp::Insert { key, value, ttl, lsn } => {
                        let old_model = model.get(&key).cloned();
                        let old_value = old_model.as_ref().map(|entry| entry.value.clone());
                        let old_lsn = old_model.as_ref().map_or(0, |entry| entry.lsn);
                        let returned = st.insert_with(
                            VortexKey::from_bytes(&key),
                            VortexValue::from_bytes(&value),
                            ttl,
                            lsn,
                        );
                        prop_assert_eq!(returned.as_ref().map(value_bytes), old_value);
                        model.insert(
                            key,
                            ModelEntry {
                                value,
                                ttl,
                                lsn: lsn.unwrap_or(old_lsn),
                            },
                        );
                    }
                    StorageOp::RawBytes { key, value, ttl, lsn } => {
                        let old_model = model.get(&key).cloned();
                        let old_lsn = old_model.as_ref().map_or(0, |entry| entry.lsn);
                        let hash = st.table_hash_key_bytes(&key);
                        let had_ttl = st
                            .mutate_prehashed(
                                BorrowedKey(&key),
                                RawValueBytes(&value),
                                hash,
                                MutationPolicy::set(ttl, lsn),
                            )
                            .had_ttl();
                        prop_assert_eq!(had_ttl, old_model.as_ref().is_some_and(|entry| entry.ttl != 0));
                        model.insert(
                            key,
                            ModelEntry {
                                value,
                                ttl,
                                lsn: lsn.unwrap_or(old_lsn),
                            },
                        );
                    }
                    StorageOp::Remove(key) => {
                        let returned = st.remove(&VortexKey::from_bytes(&key));
                        let old = model.remove(&key);
                        prop_assert_eq!(
                            returned.as_ref().map(value_bytes),
                            old.as_ref().map(|entry| entry.value.clone())
                        );
                    }
                    StorageOp::Get(key) => {
                        let returned = st.get(&VortexKey::from_bytes(&key));
                        prop_assert_eq!(
                            returned.map(value_bytes),
                            model.get(&key).map(|entry| entry.value.clone())
                        );
                    }
                }

                prop_assert_eq!(st.len(), model.len());
                prop_assert_eq!(st.memory_used(), live_slot_memory_sum(&st));
                prop_assert!(st.allocated_bytes() >= st.raw.layout.raw_alloc_size);
                prop_assert!(st.occupied >= st.len);
                prop_assert!(st.occupied <= st.total_slots());
            }

            for (key, expected) in &model {
                let key = VortexKey::from_bytes(key);
                let hash = st.table_hash_key_bytes(key.as_bytes());
                prop_assert_eq!(
                    st.get(&key).map(value_bytes),
                    Some(expected.value.clone())
                );
                prop_assert_eq!(st.get_entry_ttl(&key), Some(expected.ttl));
                prop_assert_eq!(
                    st.get_lsn_version_prehashed(key.as_bytes(), hash),
                    Some(expected.lsn)
                );
            }
        }
    }
}
