//! 64-byte cache-line-aligned hash table entry.
//!
//! Stores key + value + metadata in a single cache line to minimize
//! pointer chasing and memory accesses.
//!
//! Layout:
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  --------
//!  0       1    control        — H₂ fingerprint (7 bits, MSB=1) or EMPTY/DELETED
//!  1       1    key_len        — inline key length (0..=24) or 0 for heap keys
//!  2       1    flags          — inline/heap markers, integer flag, TTL flag, value type nibble
//!  3       1    morris_cnt     — Morris counter for probabilistic eviction (0..=255, saturating)
//!  4       4    access_profile — AccessProfile payload, AtomicU32 for lock-free read
//!  8       8    ttl_deadline   — absolute monotonic nanosecond deadline
//! 16       6    lsn_version    — 48-bit monotonic version / LSN
//! 22       1    value_tag      — inline value len, HEAP tag, or INTEGER tag
//! 23       1    _reserved      — explicit pad so key/value payloads stay aligned
//! 24      24    key_data       — inline key bytes, or heap key ptr+len metadata
//! 48      16    value_data     — inline value bytes, i64 bytes, or heap value pointer
//! ```
//!

use core::{
    mem::{align_of, size_of},
    slice,
    sync::atomic::{AtomicU32, Ordering},
};
use std::sync::atomic::AtomicU8;

use vortex_common::{VortexKey, VortexValue};

use crate::morph::AccessProfile;

// ── Control byte sentinels ──────────────────────────────────────────

/// An empty slot — never been written.
pub const CTRL_EMPTY: u8 = 0xFF;
/// A deleted (tombstone) slot — was occupied, now logically removed.
pub const CTRL_DELETED: u8 = 0x80;

/// ── Entry flags ─────────────────────────────────────────────────────
pub const FLAG_INLINE_KEY: u8 = 0x01;
pub const FLAG_INLINE_VALUE: u8 = 0x02;
pub const FLAG_INTEGER_VALUE: u8 = 0x04;
pub const FLAG_HAS_TTL: u8 = 0x08;
/// Value-type nibble stored in the high 4 bits of `flags`.
pub const VTYPE_SHIFT: u8 = 4;
pub const VTYPE_MASK: u8 = 0xF0;
pub const VTYPE_INTEGER: u8 = 0;
pub const VTYPE_INLINE_STRING: u8 = 1;
pub const VTYPE_STRING: u8 = 2;
pub const VTYPE_LIST: u8 = 3;
pub const VTYPE_HASH: u8 = 4;
pub const VTYPE_SET: u8 = 5;
pub const VTYPE_ZSET: u8 = 6;
pub const VTYPE_STREAM: u8 = 7;

/// Morris counter bits used for probabilistic eviction. Saturates at 255.
pub const EVICTION_COUNTER_MAX: u8 = 255;

const PTR_BYTES: usize = size_of::<usize>();
const HEAP_KEY_LEN_OFFSET: usize = PTR_BYTES;
const HEAP_KEY_META_LEN: usize = PTR_BYTES + size_of::<u32>();
const HEAP_VALUE_TAG: u8 = 0xFE;
const INTEGER_VALUE_TAG: u8 = 0xFF;
pub(crate) const MAX_STORED_LSN_VERSION: u64 = (1u64 << 48) - 1;

/// Physical value view returned by [`Entry::read_value`].
///
/// This describes where the slot payload is stored. The logical Redis value
/// kind is tracked separately in the value-type nibble.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EntryValue<'a> {
    Inline(&'a [u8]),
    Heap(&'a VortexValue),
    Integer(i64),
}

/// The 64-byte, cache-line-aligned entry stored in every Swiss Table slot.
#[repr(C, align(64))]
pub struct Entry {
    /// H₂ fingerprint for SIMD probing. EMPTY = 0xFF, DELETED = 0x80.
    pub control: u8,
    /// Inline key length. Heap keys encode their length in `key_data`.
    pub key_len: u8,
    /// Entry flags (type tag, heap indicators, TTL).
    pub flags: u8,
    /// Morris counter for probabilistic eviction (0..=255, saturating).
    pub morris_cnt: AtomicU8,
    /// Access-profile payload. Lock-free tracking.
    pub access_profile: AtomicU32,
    /// Absolute monotonic TTL deadline in nanoseconds.
    pub ttl_deadline_nanos: u64,
    /// 48-bit monotonic version / LSN.
    pub lsn_version: [u8; 6],
    /// Inline value length, or HEAP / INTEGER sentinel.
    pub value_tag: u8,
    /// Explicit pad so `key_data` starts on an 8-byte boundary.
    pub _reserved: u8,
    /// Inline key bytes, or heap key metadata (ptr + len).
    pub key_data: [u8; vortex_common::MAX_INLINE_KEY_LEN],
    /// Inline value bytes, integer bytes, or heap value pointer.
    pub value_data: [u8; vortex_common::MAX_INLINE_VALUE_LEN],
}

const _: () = assert!(size_of::<Entry>() == 64);
const _: () = assert!(align_of::<Entry>() == 64);

impl Entry {
    /// Creates a new EMPTY entry (all zeros except control = EMPTY).
    #[inline]
    pub const fn empty() -> Self {
        Self {
            control: CTRL_EMPTY,
            key_len: 0,
            flags: 0,
            morris_cnt: AtomicU8::new(0),
            access_profile: AtomicU32::new(0),
            ttl_deadline_nanos: 0,
            lsn_version: [0; 6],
            value_tag: 0,
            _reserved: 0,
            key_data: [0; vortex_common::MAX_INLINE_KEY_LEN],
            value_data: [0; vortex_common::MAX_INLINE_VALUE_LEN],
        }
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.control == CTRL_EMPTY
    }

    #[inline]
    pub const fn is_deleted(&self) -> bool {
        self.control == CTRL_DELETED
    }

    #[inline]
    pub const fn is_full(&self) -> bool {
        self.control != CTRL_EMPTY && self.control != CTRL_DELETED
    }

    #[inline]
    pub fn has_flag(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }

    #[inline]
    pub fn value_type(&self) -> u8 {
        (self.flags & VTYPE_MASK) >> VTYPE_SHIFT
    }

    #[inline]
    pub fn morris_counter(&self) -> u8 {
        self.morris_cnt.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_morris_counter(&self, counter: u8) {
        self.morris_cnt.store(counter, Ordering::Relaxed);
    }

    #[inline]
    pub fn decrement_eviction_counter(&self) -> bool {
        self.morris_cnt
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count > 0).then_some(count.saturating_sub(1))
            })
            .is_ok()
    }

    #[inline]
    pub fn record_access(&self, random: u64) {
        let counter = self.morris_counter();
        if counter == EVICTION_COUNTER_MAX {
            return;
        }

        let mask = if counter == 0 {
            0
        } else {
            (1u64 << counter) - 1
        };

        if random & mask != 0 {
            return;
        }

        let _ = self
            .morris_cnt
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < EVICTION_COUNTER_MAX).then_some(count + 1)
            });
    }

    #[inline]
    pub fn ttl_deadline(&self) -> u64 {
        self.ttl_deadline_nanos
    }

    /// Returns the stored 48-bit LSN/version.
    #[inline]
    pub fn lsn_version(&self) -> u64 {
        let mut buf = [0u8; 8];
        buf[..6].copy_from_slice(&self.lsn_version);
        u64::from_le_bytes(buf)
    }

    /// Stores a new 48-bit LSN/version in the entry.
    ///
    /// # Panics
    /// Panics if `lsn` exceeds the 48-bit storage budget of the slot layout.
    #[inline]
    pub fn set_lsn_version(&mut self, lsn: u64) {
        assert!(lsn <= MAX_STORED_LSN_VERSION, "lsn exceeds 48-bit storage");
        self.lsn_version.copy_from_slice(&lsn.to_le_bytes()[..6]);
    }

    /// Writes an inline key and inline string value into this entry.
    ///
    /// # Panics
    /// Panics if `key` or `value` exceed the inline storage limits of the slot.
    #[inline]
    pub fn write_inline_string(&mut self, h2: u8, key: &[u8], value: &[u8], ttl_deadline: u64) {
        assert!(
            key.len() <= vortex_common::MAX_INLINE_KEY_LEN,
            "inline entry key exceeds MAX_INLINE_KEY_LEN"
        );
        assert!(
            value.len() <= vortex_common::MAX_INLINE_VALUE_LEN,
            "inline entry value exceeds MAX_INLINE_VALUE_LEN"
        );

        self.reset(h2, ttl_deadline);
        self.store_inline_key(key);
        self.store_inline_value(value);
        self.set_value_type(VTYPE_INLINE_STRING);
    }

    /// Write an entry that borrows heap-backed key/value storage owned elsewhere.
    ///
    /// # Safety
    /// The caller must ensure the referenced key bytes and value outlive this
    /// entry, or that the entry is rewritten before those owners move or drop.
    #[inline]
    pub(super) unsafe fn write_borrowed(
        &mut self,
        h2: u8,
        key: &VortexKey,
        value: &VortexValue,
        ttl_deadline: u64,
    ) {
        self.reset(h2, ttl_deadline);

        if key.len() <= vortex_common::MAX_INLINE_KEY_LEN {
            self.store_inline_key(key.as_bytes());
        } else {
            // SAFETY: caller guarantees the borrowed key bytes outlive this entry.
            unsafe { self.store_heap_key(key.as_bytes()) };
        }

        match value {
            VortexValue::Integer(integer) => self.store_integer_value(*integer),
            VortexValue::InlineString(bytes)
                if bytes.len() <= vortex_common::MAX_INLINE_VALUE_LEN =>
            {
                self.store_inline_value(bytes.as_bytes());
            }
            VortexValue::String(bytes) if bytes.len() <= vortex_common::MAX_INLINE_VALUE_LEN => {
                self.store_inline_value(bytes.as_ref());
            }
            _ => {
                // SAFETY: caller guarantees the borrowed value outlives this entry.
                unsafe { self.store_heap_value(value) };
            }
        }

        self.set_value_type(Self::value_type_for(value));
    }

    /// Writes an inline key and integer value into this entry.
    ///
    /// # Panics
    /// Panics if `key` exceeds the inline storage limit of the slot.
    #[inline]
    pub fn write_inline_integer(&mut self, h2: u8, key: &[u8], value: i64, ttl_deadline: u64) {
        assert!(
            key.len() <= vortex_common::MAX_INLINE_KEY_LEN,
            "inline integer key exceeds MAX_INLINE_KEY_LEN"
        );

        self.reset(h2, ttl_deadline);
        self.store_inline_key(key);
        self.store_integer_value(value);
        self.set_value_type(VTYPE_INTEGER);
    }

    /// Returns the key bytes stored in an occupied entry.
    ///
    /// # Panics
    /// Panics if the entry is empty or deleted.
    #[inline]
    pub fn read_key(&self) -> &[u8] {
        self.assert_full("Entry::read_key");

        if self.has_flag(FLAG_INLINE_KEY) {
            &self.key_data[..self.key_len as usize]
        } else {
            let (ptr, len) = self.heap_key_parts();
            assert!(!ptr.is_null(), "heap key pointer missing");
            // SAFETY: `write_borrowed` stores a valid borrowed key pointer + len.
            unsafe { slice::from_raw_parts(ptr, len) }
        }
    }

    /// Returns the value stored in an occupied entry.
    ///
    /// # Panics
    /// Panics if the entry is empty or deleted, or if its value metadata is corrupt.
    #[inline]
    pub fn read_value(&self) -> EntryValue<'_> {
        self.assert_full("Entry::read_value");

        if self.has_flag(FLAG_INTEGER_VALUE) {
            assert_eq!(
                self.value_tag, INTEGER_VALUE_TAG,
                "integer entry missing integer tag"
            );
            return EntryValue::Integer(self.read_integer().expect("integer flag set"));
        }

        if self.has_flag(FLAG_INLINE_VALUE) {
            assert!(
                self.value_tag as usize <= vortex_common::MAX_INLINE_VALUE_LEN,
                "inline value tag exceeds MAX_INLINE_VALUE_LEN"
            );
            return EntryValue::Inline(&self.value_data[..self.value_tag as usize]);
        }

        assert_eq!(
            self.value_tag, HEAP_VALUE_TAG,
            "heap-backed entry missing heap tag"
        );
        // SAFETY: `write_borrowed` stores a valid borrowed value pointer.
        EntryValue::Heap(unsafe { self.heap_value_ref() })
    }

    /// Returns whether this occupied entry's key matches `key`.
    #[inline]
    pub fn matches_key(&self, key: &[u8]) -> bool {
        if !self.is_full() {
            return false;
        }

        let stored = self.read_key();
        stored.len() == key.len() && stored == key
    }

    /// Returns whether the entry has expired at `now_nanos`.
    #[inline]
    pub fn is_expired(&self, now_nanos: u64) -> bool {
        self.has_flag(FLAG_HAS_TTL) && now_nanos >= self.ttl_deadline()
    }

    /// Sets the absolute TTL deadline, or clears TTL when `deadline_nanos` is zero.
    #[inline]
    pub fn set_ttl(&mut self, deadline_nanos: u64) {
        if deadline_nanos == 0 {
            self.ttl_deadline_nanos = 0;
            self.store_flags(self.flags() & !FLAG_HAS_TTL);
        } else {
            self.ttl_deadline_nanos = deadline_nanos;
            self.store_flags(self.flags() | FLAG_HAS_TTL);
        }
    }

    /// Removes any TTL from the entry.
    #[inline]
    pub fn clear_ttl(&mut self) {
        self.set_ttl(0);
    }

    /// Read the access profile payload.
    #[inline]
    pub fn access_profile(&self) -> AccessProfile {
        AccessProfile::from_u32(self.access_profile.load(Ordering::Relaxed))
    }

    /// Write an access profile payload.
    #[inline]
    pub fn set_access_profile(&self, profile: AccessProfile) {
        self.access_profile
            .store(profile.as_u32(), Ordering::Relaxed);
    }

    /// Mark this entry as DELETED (tombstone) and zero payload data.
    #[inline]
    pub fn mark_deleted(&mut self) {
        self.control = CTRL_DELETED;
        self.key_len = 0;
        self.store_flags(0);
        self.morris_cnt.store(0, Ordering::Relaxed);
        self.access_profile.store(0, Ordering::Relaxed);
        self.ttl_deadline_nanos = 0;
        self.lsn_version = [0; 6];
        self.value_tag = 0;
        self._reserved = 0;
        self.key_data = [0; vortex_common::MAX_INLINE_KEY_LEN];
        self.value_data = [0; vortex_common::MAX_INLINE_VALUE_LEN];
    }
}

/*
* Internal helper methods for reading/writing entry payloads and metadata.
*/
impl Entry {
    #[inline]
    fn read_integer(&self) -> Option<i64> {
        if !self.has_flag(FLAG_INTEGER_VALUE) {
            return None;
        }

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.value_data[..8]);
        Some(i64::from_ne_bytes(buf))
    }

    #[inline]
    #[track_caller]
    fn assert_full(&self, operation: &str) {
        assert!(self.is_full(), "{operation} requires an occupied entry");
    }

    #[inline]
    fn reset(&mut self, h2: u8, ttl_deadline: u64) {
        let old_lsn = self.lsn_version;
        self.control = h2;
        self.key_len = 0;
        self.store_flags(0);
        self.morris_cnt.store(0, Ordering::Relaxed);
        self.access_profile.store(0, Ordering::Relaxed);
        self.ttl_deadline_nanos = 0;
        self.lsn_version = old_lsn;
        self.value_tag = 0;
        self._reserved = 0;
        self.key_data = [0; vortex_common::MAX_INLINE_KEY_LEN];
        self.value_data = [0; vortex_common::MAX_INLINE_VALUE_LEN];

        if ttl_deadline != 0 {
            self.set_ttl(ttl_deadline);
        }
    }

    #[inline]
    fn set_value_type(&mut self, value_type: u8) {
        let flags = self.flags();
        self.store_flags((flags & !VTYPE_MASK) | (value_type << VTYPE_SHIFT));
    }

    #[inline]
    fn store_inline_key(&mut self, key: &[u8]) {
        debug_assert!(key.len() <= vortex_common::MAX_INLINE_KEY_LEN);
        self.store_flags(self.flags() | FLAG_INLINE_KEY);
        self.key_len = key.len() as u8;
        self.key_data[..key.len()].copy_from_slice(key);
    }

    /// Stores heap-key metadata for a borrowed key.
    ///
    /// # Safety
    /// `key` must outlive every future read from this entry until the slot is
    /// rewritten or deleted.
    #[inline]
    unsafe fn store_heap_key(&mut self, key: &[u8]) {
        debug_assert!(key.len() > vortex_common::MAX_INLINE_KEY_LEN);
        debug_assert!(key.len() <= u32::MAX as usize);

        let ptr = key.as_ptr() as usize;
        self.key_data[..PTR_BYTES].copy_from_slice(&ptr.to_ne_bytes());
        self.key_data[HEAP_KEY_LEN_OFFSET..HEAP_KEY_META_LEN]
            .copy_from_slice(&(key.len() as u32).to_ne_bytes());
    }

    #[inline]
    fn store_inline_value(&mut self, value: &[u8]) {
        debug_assert!(value.len() <= vortex_common::MAX_INLINE_VALUE_LEN);
        self.store_flags(self.flags() | FLAG_INLINE_VALUE);
        self.value_tag = value.len() as u8;
        self.value_data[..value.len()].copy_from_slice(value);
    }

    /// Stores a borrowed pointer to a heap-backed value.
    ///
    /// # Safety
    /// `value` must outlive every future read from this entry until the slot is
    /// rewritten or deleted.
    #[inline]
    unsafe fn store_heap_value(&mut self, value: &VortexValue) {
        let ptr = value as *const VortexValue as usize;
        self.value_tag = HEAP_VALUE_TAG;
        self.value_data[..PTR_BYTES].copy_from_slice(&ptr.to_ne_bytes());
    }

    #[inline]
    fn store_integer_value(&mut self, value: i64) {
        self.store_flags(self.flags() | FLAG_INTEGER_VALUE);
        self.value_tag = INTEGER_VALUE_TAG;
        self.value_data[..8].copy_from_slice(&value.to_ne_bytes());
    }

    #[inline(always)]
    fn flags(&self) -> u8 {
        self.flags
    }

    #[inline]
    fn store_flags(&mut self, flags: u8) {
        self.flags = flags;
    }

    #[inline]
    fn heap_key_parts(&self) -> (*const u8, usize) {
        let mut ptr_buf = [0u8; PTR_BYTES];
        ptr_buf.copy_from_slice(&self.key_data[..PTR_BYTES]);

        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&self.key_data[HEAP_KEY_LEN_OFFSET..HEAP_KEY_META_LEN]);

        (
            usize::from_ne_bytes(ptr_buf) as *const u8,
            u32::from_ne_bytes(len_buf) as usize,
        )
    }

    /// Reconstructs the borrowed heap-backed value reference stored in the slot.
    ///
    /// # Safety
    /// The entry must still contain a valid pointer previously written by
    /// [`Self::store_heap_value`], and that pointee must still be alive.
    #[inline]
    unsafe fn heap_value_ref(&self) -> &VortexValue {
        let mut ptr_buf = [0u8; PTR_BYTES];
        ptr_buf.copy_from_slice(&self.value_data[..PTR_BYTES]);
        let ptr = usize::from_ne_bytes(ptr_buf) as *const VortexValue;
        assert!(!ptr.is_null(), "heap value pointer missing");

        // SAFETY: `write_borrowed` stored a valid borrowed pointer.
        unsafe { &*ptr }
    }

    #[inline]
    const fn value_type_for(value: &VortexValue) -> u8 {
        match value {
            VortexValue::InlineString(_) => VTYPE_INLINE_STRING,
            VortexValue::String(_) => VTYPE_STRING,
            VortexValue::Integer(_) => VTYPE_INTEGER,
            VortexValue::List(_) => VTYPE_LIST,
            VortexValue::Hash(_) => VTYPE_HASH,
            VortexValue::Set(_) => VTYPE_SET,
            VortexValue::SortedSet(_) => VTYPE_ZSET,
            VortexValue::Stream(_) => VTYPE_STREAM,
        }
    }
}

#[cfg(test)]
mod tests {
    use core::mem::offset_of;

    use vortex_common::value::{InlineBytes, VortexList};

    use super::*;

    fn write_borrowed_for_test(
        entry: &mut Entry,
        h2: u8,
        key: &VortexKey,
        value: &VortexValue,
        ttl_deadline: u64,
    ) {
        // SAFETY: each test keeps `key` and `value` alive until all reads from
        // `entry` are complete.
        unsafe {
            entry.write_borrowed(h2, key, value, ttl_deadline);
        }
    }

    #[test]
    fn entry_size_and_alignment() {
        assert_eq!(size_of::<Entry>(), 64);
        assert_eq!(align_of::<Entry>(), 64);
        assert_eq!(offset_of!(Entry, control), 0);
        assert_eq!(offset_of!(Entry, key_len), 1);
        assert_eq!(offset_of!(Entry, flags), 2);
        assert_eq!(offset_of!(Entry, morris_cnt), 3);
        assert_eq!(offset_of!(Entry, access_profile), 4);
        assert_eq!(offset_of!(Entry, ttl_deadline_nanos), 8);
        assert_eq!(offset_of!(Entry, lsn_version), 16);
        assert_eq!(offset_of!(Entry, value_tag), 22);
        assert_eq!(offset_of!(Entry, _reserved), 23);
        assert_eq!(offset_of!(Entry, key_data), 24);
        assert_eq!(offset_of!(Entry, value_data), 48);
    }

    #[test]
    fn empty_entry() {
        let e = Entry::empty();
        assert!(e.is_empty());
        assert!(!e.is_full());
        assert!(!e.is_deleted());
    }

    #[test]
    fn write_inline_and_read() {
        let mut e = Entry::empty();
        let value = VortexValue::from("world");
        let key = VortexKey::from("hello");
        write_borrowed_for_test(&mut e, 0x92, &key, &value, 0);

        assert!(e.is_full());
        assert_eq!(e.control, 0x92);
        assert_eq!(e.read_key(), b"hello");
        assert_eq!(e.read_value(), EntryValue::Inline(b"world"));
        assert!(e.read_integer().is_none());
        assert!(e.has_flag(FLAG_INLINE_KEY));
        assert!(e.has_flag(FLAG_INLINE_VALUE));
        assert!(!e.has_flag(FLAG_INTEGER_VALUE));
        assert_eq!(e.value_type(), VTYPE_INLINE_STRING);
    }

    #[test]
    fn write_entry_key_and_value() {
        let mut e = Entry::empty();
        let key = VortexKey::from("this:key:is:definitely:longer:than:23");
        let value = VortexValue::from("this value is much longer than twenty-one bytes");
        let deadline = 123_456_789;

        write_borrowed_for_test(&mut e, 0x93, &key, &value, deadline);

        assert!(!e.has_flag(FLAG_INLINE_KEY));
        assert!(!e.has_flag(FLAG_INLINE_VALUE));
        assert_eq!(e.read_key(), key.as_bytes());
        assert_eq!(e.read_value(), EntryValue::Heap(&value));
        assert_eq!(e.ttl_deadline(), deadline);
    }

    #[test]
    fn write_entry_key_inline_value() {
        let mut e = Entry::empty();
        let key = VortexKey::from("this:key:is:definitely:longer:than:23");
        let value = VortexValue::from("short-inline");

        write_borrowed_for_test(&mut e, 0x91, &key, &value, 0);

        assert!(!e.has_flag(FLAG_INLINE_KEY));
        assert!(e.has_flag(FLAG_INLINE_VALUE));
        assert_eq!(e.read_key(), key.as_bytes());
        assert_eq!(e.read_value(), EntryValue::Inline(b"short-inline"));
    }

    #[test]
    fn write_heap_container_value() {
        let mut e = Entry::empty();
        let key = VortexKey::from("small-key");
        let value = VortexValue::List(Box::default());

        write_borrowed_for_test(&mut e, 0x88, &key, &value, 0);

        assert!(e.has_flag(FLAG_INLINE_KEY));
        assert!(!e.has_flag(FLAG_INLINE_VALUE));
        assert_eq!(e.read_key(), b"small-key");
        match e.read_value() {
            EntryValue::Heap(VortexValue::List(_)) => {}
            other => panic!("expected heap list, got {other:?}"),
        }
        assert_eq!(e.value_type(), VTYPE_LIST);
    }

    #[test]
    fn write_integer_and_read() {
        let mut e = Entry::empty();
        let value = VortexValue::Integer(42);
        let key = VortexKey::from("counter");
        write_borrowed_for_test(&mut e, 0xA3, &key, &value, 0);

        assert!(e.is_full());
        assert_eq!(e.read_key(), key.as_bytes());
        assert_eq!(e.read_value(), EntryValue::Integer(42));
        assert_eq!(e.read_integer(), Some(42));
        assert!(e.has_flag(FLAG_INTEGER_VALUE));
        assert_eq!(e.value_type(), VTYPE_INTEGER);
    }

    #[test]
    fn write_integer_negative() {
        let mut e = Entry::empty();
        let value = VortexValue::Integer(-999_999);
        let key = VortexKey::from("neg");
        write_borrowed_for_test(&mut e, 0xB0, &key, &value, 0);
        assert_eq!(e.read_integer(), Some(-999_999));
    }

    #[test]
    fn key_matching() {
        let mut e = Entry::empty();
        let key = VortexKey::from("test_key");
        let value = VortexValue::from("val");
        write_borrowed_for_test(&mut e, 0x92, &key, &value, 0);

        assert!(e.matches_key(b"test_key"));
        assert!(e.control == 0x92);
        assert!(e.control != 0x93);
        assert!(!e.matches_key(b"test_ke"));
        assert!(!e.matches_key(b"test_key!"));
        assert!(!e.matches_key(b"other"));

        // Test larger key that doesn't fit inline.
        let long_key = VortexKey::from("this:is:a:long:key:that:exceeds:inline:limit");
        let long_value = VortexValue::from("some value");
        write_borrowed_for_test(&mut e, 0x92, &long_key, &long_value, 0);
        assert!(e.matches_key(b"this:is:a:long:key:that:exceeds:inline:limit"));
        assert!(!e.matches_key(b"this:is:a:long:key:that:exceeds:inline:limi"));
    }

    #[test]
    fn ttl_operations() {
        let mut e = Entry::empty();
        let key = VortexKey::from("k");
        let value = VortexValue::from("v");
        write_borrowed_for_test(&mut e, 0x92, &key, &value, 1_000_000);

        assert!(e.has_flag(FLAG_HAS_TTL));
        assert_eq!(e.ttl_deadline(), 1_000_000);
        assert!(!e.is_expired(999_999));
        assert!(e.is_expired(1_000_000));

        e.clear_ttl();
        assert!(!e.has_flag(FLAG_HAS_TTL));
        assert_eq!(e.ttl_deadline(), 0);
        assert!(!e.is_expired(999_999_999));

        e.set_ttl(5_000);
        assert!(e.has_flag(FLAG_HAS_TTL));
        assert_eq!(e.ttl_deadline(), 5_000);
        assert!(!e.is_expired(4_999));
        assert!(e.is_expired(5_000));
    }

    #[test]
    fn mark_deleted() {
        let mut e = Entry::empty();
        let key = VortexKey::from("secret");
        let value = VortexValue::from("data");
        write_borrowed_for_test(&mut e, 0x92, &key, &value, 0);

        e.mark_deleted();
        assert!(e.is_deleted());
        assert!(!e.is_full());
        assert_eq!(e.key_len, 0);
        assert_eq!(e.key_data, [0; vortex_common::MAX_INLINE_KEY_LEN]);
    }

    #[test]
    fn matches_key_returns_false_for_deleted_entry() {
        let mut entry = Entry::empty();
        let key = VortexKey::from("secret");
        let value = VortexValue::from("data");

        write_borrowed_for_test(&mut entry, 0x92, &key, &value, 0);
        entry.mark_deleted();

        assert!(!entry.matches_key(key.as_bytes()));
    }

    #[test]
    #[should_panic(expected = "Entry::read_key requires an occupied entry")]
    fn read_key_panics_for_empty_entry() {
        let entry = Entry::empty();
        let _ = entry.read_key();
    }

    #[test]
    #[should_panic(expected = "Entry::read_value requires an occupied entry")]
    fn read_value_panics_for_empty_entry() {
        let entry = Entry::empty();
        let _ = entry.read_value();
    }

    #[test]
    fn eviction_counter() {
        let e = Entry::empty();
        assert_eq!(e.morris_counter(), 0);

        e.set_morris_counter(5);
        assert_eq!(e.morris_counter(), 5);

        assert!(e.decrement_eviction_counter());
        assert_eq!(e.morris_counter(), 4);

        e.set_morris_counter(EVICTION_COUNTER_MAX);
        assert_eq!(e.morris_counter(), EVICTION_COUNTER_MAX);
        assert!(e.decrement_eviction_counter());
        assert_eq!(e.morris_counter(), EVICTION_COUNTER_MAX - 1);
    }

    #[test]
    fn flags_and_value_type() {
        let mut e = Entry::empty();
        assert_eq!(e.flags(), 0);
        assert_eq!(e.value_type(), 0);

        e.store_flags(FLAG_INLINE_KEY | FLAG_HAS_TTL);
        assert!(e.has_flag(FLAG_INLINE_KEY));
        assert!(e.has_flag(FLAG_HAS_TTL));
        assert!(!e.has_flag(FLAG_INLINE_VALUE));
        assert_eq!(e.value_type(), 0);

        e.set_value_type(VTYPE_HASH);
        assert_eq!(e.value_type(), VTYPE_HASH);

        let mut e2 = Entry::empty();
        let ttl_deadline = 1_000_000;
        let value = VortexValue::List(Box::new(VortexList::new()));
        let key = VortexKey::from("vortex");
        write_borrowed_for_test(&mut e2, 0x24, &key, &value, ttl_deadline);
        assert!(e2.has_flag(FLAG_INLINE_KEY));
        assert!(!e2.has_flag(FLAG_INLINE_VALUE));
        assert_eq!(e2.value_type(), VTYPE_LIST);
        assert!(e2.is_expired(1_000_001));
    }

    #[test]
    fn value_type_round_trip() {
        let mut entry = Entry::empty();
        let key = VortexKey::from("k");
        let value = VortexValue::from("v");
        write_borrowed_for_test(&mut entry, 0x81, &key, &value, 0);
        assert_eq!(entry.value_type(), VTYPE_INLINE_STRING);
        let list = VortexValue::List(Box::new(VortexList::new()));
        let list_key = VortexKey::from("k");
        write_borrowed_for_test(&mut entry, 0x81, &list_key, &list, 0);
        assert_eq!(entry.value_type(), VTYPE_LIST);
    }

    #[test]
    #[should_panic(expected = "lsn exceeds 48-bit storage")]
    fn set_lsn_version_panics_when_value_exceeds_48_bits() {
        let mut entry = Entry::empty();
        entry.set_lsn_version(MAX_STORED_LSN_VERSION + 1);
    }

    #[test]
    fn max_inline_key_value() {
        let mut e = Entry::empty();
        let key = [b'K'; vortex_common::MAX_INLINE_KEY_LEN];
        let val = [b'V'; vortex_common::MAX_INLINE_VALUE_LEN];
        e.write_inline_string(0x82, &key, &val, 0);

        assert_eq!(e.read_key(), b"KKKKKKKKKKKKKKKKKKKKKKKK");
        assert_eq!(e.value_type(), VTYPE_INLINE_STRING);
        assert_eq!(e.read_value(), EntryValue::Inline(b"VVVVVVVVVVVVVVVV"));
    }

    #[test]
    fn zero_length_key_value() {
        let mut e = Entry::empty();
        let key = VortexKey::from("");
        let value = VortexValue::InlineString(InlineBytes::from_slice(b""));
        write_borrowed_for_test(&mut e, 0x82, &key, &value, 0);

        assert!(e.is_full());
        assert_eq!(e.read_key(), b"");
        assert_eq!(e.value_type(), VTYPE_INLINE_STRING);
        assert_eq!(e.read_value(), EntryValue::Inline(b""));
    }
}
