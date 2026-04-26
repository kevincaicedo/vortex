//! 64-byte cache-line-aligned hash table entry.
//!
//! Stores key + value + metadata in a single cache line to minimize
//! pointer chasing and memory accesses. Layout:
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  --------
//!  0       1    control      — H₂ fingerprint (7 bits, MSB=1) or EMPTY/DELETED
//!  1       1    key_len      — inline key length (0..=23) or 0 for heap keys
//!  2       2    flags        — inline/heap markers, integer flag, TTL flag, value type nibble
//!  4       4    _pad0        — AccessProfile payload (current owner of the 4-byte slack field)
//!  8       8    ttl_deadline — absolute nanos, 0 = no expiry
//! 16      23    key_data     — inline key bytes, or heap key ptr+len metadata
//! 39       1    value_tag    — inline value len, HEAP tag, or INTEGER tag
//! 40      21    value_data   — inline value bytes, i64 bytes, or heap value pointer
//! 61       3    _pad1        — pad to 64 bytes
//! ```
//!
//! Exactly **64 bytes = 1 cache line** on all modern CPUs.

use core::{
    mem::{align_of, size_of},
    slice,
    sync::atomic::{AtomicU16, Ordering},
};

use vortex_common::{VortexKey, VortexValue};

use crate::morph::AccessProfile;

// ── Control byte sentinels ──────────────────────────────────────────

/// An empty slot — never been written.
pub const CTRL_EMPTY: u8 = 0xFF;
/// A deleted (tombstone) slot — was occupied, now logically removed.
pub const CTRL_DELETED: u8 = 0x80;

// ── Entry flags ─────────────────────────────────────────────────────

pub const FLAG_INLINE_KEY: u16 = 0x0001;
pub const FLAG_INLINE_VALUE: u16 = 0x0002;
pub const FLAG_INTEGER_VALUE: u16 = 0x0004;
pub const FLAG_HAS_TTL: u16 = 0x0008;
pub const EVICTION_COUNTER_SHIFT: u32 = 4;
pub const EVICTION_COUNTER_MASK: u16 = 0x00F0;
pub const EVICTION_COUNTER_MAX: u8 = 15;

/// Value-type nibble stored in the high 4 bits of `flags`.
pub const VTYPE_SHIFT: u32 = 12;
pub const VTYPE_MASK: u16 = 0xF000;
pub const VTYPE_STRING: u16 = 0;
pub const VTYPE_LIST: u16 = 1;
pub const VTYPE_HASH: u16 = 2;
pub const VTYPE_SET: u16 = 3;
pub const VTYPE_ZSET: u16 = 4;
pub const VTYPE_STREAM: u16 = 5;

const PTR_BYTES: usize = size_of::<usize>();
const HEAP_KEY_LEN_OFFSET: usize = PTR_BYTES;
const HEAP_KEY_META_LEN: usize = PTR_BYTES + size_of::<u32>();
const HEAP_VALUE_TAG: u8 = 0xFE;
const INTEGER_VALUE_TAG: u8 = 0xFF;

/// Typed value view returned by [`Entry::read_value`].
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
    pub flags: AtomicU16,
    /// Access-profile payload. Alpha WATCH/version tracking stays in cold side tables.
    pub _pad0: u32,
    /// TTL deadline in nanoseconds (0 = no expiry).
    pub ttl_deadline: u64,
    /// Inline key bytes, or heap key metadata (ptr + len).
    pub key_data: [u8; 23],
    /// Inline value length, or HEAP / INTEGER sentinel.
    pub value_tag: u8,
    /// Inline value bytes, integer bytes, or heap value pointer.
    pub value_data: [u8; 21],
    /// Padding to reach 64 bytes.
    pub _pad1: [u8; 3],
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
            flags: AtomicU16::new(0),
            _pad0: 0,
            ttl_deadline: 0,
            key_data: [0; 23],
            value_tag: 0,
            value_data: [0; 21],
            _pad1: [0; 3],
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
    pub const fn is_empty_or_deleted(&self) -> bool {
        !self.is_full()
    }

    #[inline]
    pub fn has_flag(&self, flag: u16) -> bool {
        self.flags() & flag != 0
    }

    #[inline]
    pub fn value_type(&self) -> u16 {
        (self.flags() & VTYPE_MASK) >> VTYPE_SHIFT
    }

    #[inline]
    pub fn eviction_counter(&self) -> u8 {
        ((self.flags() & EVICTION_COUNTER_MASK) >> EVICTION_COUNTER_SHIFT) as u8
    }

    #[inline]
    pub fn clear_eviction_counter(&self) {
        self.set_eviction_counter(0);
    }

    #[inline]
    pub fn set_eviction_counter(&self, counter: u8) {
        let counter = counter.min(EVICTION_COUNTER_MAX);
        let flags = self.flags();
        self.store_flags(
            (flags & !EVICTION_COUNTER_MASK) | ((counter as u16) << EVICTION_COUNTER_SHIFT),
        );
    }

    #[inline]
    pub fn decrement_eviction_counter(&self) -> bool {
        self.flags
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |flags| {
                let counter = ((flags & EVICTION_COUNTER_MASK) >> EVICTION_COUNTER_SHIFT) as u8;
                (counter > 0).then_some(
                    (flags & !EVICTION_COUNTER_MASK)
                        | (((counter.saturating_sub(1)) as u16) << EVICTION_COUNTER_SHIFT),
                )
            })
            .is_ok()
    }

    #[inline]
    pub fn record_access(&self, random: u64) {
        let counter = self.eviction_counter();
        if counter >= EVICTION_COUNTER_MAX {
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
            .flags
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |flags| {
                let current = ((flags & EVICTION_COUNTER_MASK) >> EVICTION_COUNTER_SHIFT) as u8;
                (current < EVICTION_COUNTER_MAX).then_some(
                    (flags & !EVICTION_COUNTER_MASK)
                        | (((current + 1) as u16) << EVICTION_COUNTER_SHIFT),
                )
            });
    }

    #[inline]
    pub const fn ttl_deadline(&self) -> u64 {
        self.ttl_deadline
    }

    /// Write an inline key + inline value into this entry.
    #[inline]
    pub fn write_inline(&mut self, h2: u8, key: &[u8], value: &[u8], ttl_deadline: u64) {
        debug_assert!(key.len() <= 23);
        debug_assert!(value.len() <= 21);

        self.reset(h2, ttl_deadline);
        self.store_inline_key(key);
        self.store_inline_value(value);
        self.set_value_type(VTYPE_STRING);
    }

    /// Write an entry that borrows heap-backed key/value storage owned elsewhere.
    ///
    /// # Safety
    /// The caller must ensure the referenced key bytes and value outlive this
    /// entry, or that the entry is rewritten before those owners move or drop.
    #[inline]
    pub unsafe fn write_heap(
        &mut self,
        h2: u8,
        key: &VortexKey,
        value: &VortexValue,
        ttl_deadline: u64,
    ) {
        self.reset(h2, ttl_deadline);

        if key.len() <= 23 {
            self.store_inline_key(key.as_bytes());
        } else {
            self.store_heap_key(key.as_bytes());
        }

        match value {
            VortexValue::Integer(integer) => self.store_integer_value(*integer),
            VortexValue::InlineString(bytes) if bytes.len() <= 21 => {
                self.store_inline_value(bytes.as_bytes());
            }
            VortexValue::String(bytes) if bytes.len() <= 21 => {
                self.store_inline_value(bytes.as_ref());
            }
            _ => self.store_heap_value(value),
        }

        self.set_value_type(Self::value_type_for(value));
    }

    /// Write an integer value with an inline key fast path.
    #[inline]
    pub fn write_integer(&mut self, h2: u8, key: &[u8], value: i64, ttl_deadline: u64) {
        debug_assert!(key.len() <= 23);

        self.reset(h2, ttl_deadline);
        self.store_inline_key(key);
        self.store_integer_value(value);
        self.set_value_type(VTYPE_STRING);
    }

    /// Read the key bytes, inline or heap-borrowed.
    #[inline]
    pub fn read_key(&self) -> &[u8] {
        if self.has_flag(FLAG_INLINE_KEY) {
            &self.key_data[..self.key_len as usize]
        } else {
            let (ptr, len) = self.heap_key_parts();
            // SAFETY: `write_heap` stores a valid borrowed key pointer + len.
            unsafe { slice::from_raw_parts(ptr, len) }
        }
    }

    /// Read the entry value as either inline bytes, heap value, or integer.
    #[inline]
    pub fn read_value(&self) -> EntryValue<'_> {
        if self.has_flag(FLAG_INTEGER_VALUE) {
            return EntryValue::Integer(self.read_integer().expect("integer flag set"));
        }

        if self.has_flag(FLAG_INLINE_VALUE) {
            return EntryValue::Inline(&self.value_data[..self.value_tag as usize]);
        }

        // SAFETY: `write_heap` stores a valid borrowed value pointer.
        EntryValue::Heap(unsafe { self.heap_value_ref() })
    }

    /// Read string bytes if the value is a string-like payload.
    #[inline]
    pub fn read_value_bytes(&self) -> Option<&[u8]> {
        match self.read_value() {
            EntryValue::Inline(bytes) => Some(bytes),
            EntryValue::Heap(VortexValue::InlineString(bytes)) => Some(bytes.as_bytes()),
            EntryValue::Heap(VortexValue::String(bytes)) => Some(bytes.as_ref()),
            EntryValue::Heap(_) | EntryValue::Integer(_) => None,
        }
    }

    #[inline]
    pub fn read_integer(&self) -> Option<i64> {
        if !self.has_flag(FLAG_INTEGER_VALUE) {
            return None;
        }

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.value_data[..8]);
        Some(i64::from_ne_bytes(buf))
    }

    /// Check if this entry's key matches the given key bytes.
    #[inline]
    pub fn matches_key(&self, key: &[u8]) -> bool {
        let stored = self.read_key();
        stored.len() == key.len() && stored == key
    }

    /// Check key equality after first validating the control byte fingerprint.
    #[inline]
    pub fn matches_key_with_ctrl(&self, key: &[u8], h2: u8) -> bool {
        self.control == h2 && self.matches_key(key)
    }

    #[inline]
    pub fn is_expired(&self, now_nanos: u64) -> bool {
        self.ttl_deadline != 0 && now_nanos >= self.ttl_deadline
    }

    #[inline]
    pub fn set_ttl(&mut self, deadline_nanos: u64) {
        self.ttl_deadline = deadline_nanos;
        if deadline_nanos != 0 {
            self.store_flags(self.flags() | FLAG_HAS_TTL);
        } else {
            self.store_flags(self.flags() & !FLAG_HAS_TTL);
        }
    }

    #[inline]
    pub fn clear_ttl(&mut self) {
        self.set_ttl(0);
    }

    // ── Access profile (stored in _pad0 — zero additional overhead) ─

    /// Read the access profile packed into the `_pad0` padding field.
    #[inline]
    pub const fn access_profile(&self) -> AccessProfile {
        AccessProfile::from_u32(self._pad0)
    }

    /// Write an access profile into the `_pad0` padding field.
    #[inline]
    pub fn set_access_profile(&mut self, profile: AccessProfile) {
        self._pad0 = profile.as_u32();
    }

    /// Mark this entry as DELETED (tombstone) and zero payload data.
    #[inline]
    pub fn mark_deleted(&mut self) {
        self.control = CTRL_DELETED;
        self.key_len = 0;
        self.store_flags(0);
        self._pad0 = 0;
        self.ttl_deadline = 0;
        self.key_data = [0; 23];
        self.value_tag = 0;
        self.value_data = [0; 21];
        self._pad1 = [0; 3];
    }

    #[inline]
    fn reset(&mut self, h2: u8, ttl_deadline: u64) {
        self.control = h2;
        self.key_len = 0;
        self.store_flags(0);
        self._pad0 = 0;
        self.ttl_deadline = ttl_deadline;
        self.key_data = [0; 23];
        self.value_tag = 0;
        self.value_data = [0; 21];
        self._pad1 = [0; 3];

        if ttl_deadline != 0 {
            self.store_flags(self.flags() | FLAG_HAS_TTL);
        }
    }

    #[inline]
    fn set_value_type(&mut self, value_type: u16) {
        let flags = self.flags();
        self.store_flags((flags & !VTYPE_MASK) | (value_type << VTYPE_SHIFT));
    }

    #[inline]
    fn store_inline_key(&mut self, key: &[u8]) {
        debug_assert!(key.len() <= 23);
        self.store_flags(self.flags() | FLAG_INLINE_KEY);
        self.key_len = key.len() as u8;
        self.key_data[..key.len()].copy_from_slice(key);
    }

    #[inline]
    fn store_heap_key(&mut self, key: &[u8]) {
        debug_assert!(key.len() > 23);
        debug_assert!(key.len() <= u32::MAX as usize);

        let ptr = key.as_ptr() as usize;
        self.key_data[..PTR_BYTES].copy_from_slice(&ptr.to_ne_bytes());
        self.key_data[HEAP_KEY_LEN_OFFSET..HEAP_KEY_META_LEN]
            .copy_from_slice(&(key.len() as u32).to_ne_bytes());
    }

    #[inline]
    fn store_inline_value(&mut self, value: &[u8]) {
        debug_assert!(value.len() <= 21);
        self.store_flags(self.flags() | FLAG_INLINE_VALUE);
        self.value_tag = value.len() as u8;
        self.value_data[..value.len()].copy_from_slice(value);
    }

    #[inline]
    fn store_heap_value(&mut self, value: &VortexValue) {
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

    #[inline]
    fn flags(&self) -> u16 {
        self.flags.load(Ordering::Relaxed)
    }

    #[inline]
    fn store_flags(&self, flags: u16) {
        self.flags.store(flags, Ordering::Relaxed);
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

    #[inline]
    unsafe fn heap_value_ref(&self) -> &VortexValue {
        let mut ptr_buf = [0u8; PTR_BYTES];
        ptr_buf.copy_from_slice(&self.value_data[..PTR_BYTES]);
        let ptr = usize::from_ne_bytes(ptr_buf) as *const VortexValue;

        // SAFETY: `write_heap` stored a valid borrowed pointer.
        unsafe { &*ptr }
    }

    #[inline]
    const fn value_type_for(value: &VortexValue) -> u16 {
        match value {
            VortexValue::InlineString(_) | VortexValue::String(_) | VortexValue::Integer(_) => {
                VTYPE_STRING
            }
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
    use vortex_common::value::VortexList;

    use super::*;

    #[test]
    fn entry_size_and_alignment() {
        assert_eq!(size_of::<Entry>(), 64);
        assert_eq!(align_of::<Entry>(), 64);
    }

    #[test]
    fn empty_entry() {
        let e = Entry::empty();
        assert!(e.is_empty());
        assert!(!e.is_full());
        assert!(!e.is_deleted());
        assert!(e.is_empty_or_deleted());
    }

    #[test]
    fn write_inline_and_read() {
        let mut e = Entry::empty();
        e.write_inline(0x92, b"hello", b"world", 0);

        assert!(e.is_full());
        assert_eq!(e.control, 0x92);
        assert_eq!(e.read_key(), b"hello");
        assert_eq!(e.read_value(), EntryValue::Inline(b"world"));
        assert_eq!(e.read_value_bytes(), Some(b"world".as_slice()));
        assert!(e.read_integer().is_none());
        assert!(e.has_flag(FLAG_INLINE_KEY));
        assert!(e.has_flag(FLAG_INLINE_VALUE));
        assert!(!e.has_flag(FLAG_INTEGER_VALUE));
        assert_eq!(e.value_type(), VTYPE_STRING);
    }

    #[test]
    fn write_heap_key_and_value() {
        let mut e = Entry::empty();
        let key = VortexKey::from("this:key:is:definitely:longer:than:23");
        let value = VortexValue::from("this value is much longer than twenty-one bytes");

        // SAFETY: `key` and `value` live for the duration of the assertions.
        unsafe {
            e.write_heap(0x93, &key, &value, 123);
        }

        assert!(!e.has_flag(FLAG_INLINE_KEY));
        assert!(!e.has_flag(FLAG_INLINE_VALUE));
        assert_eq!(e.read_key(), key.as_bytes());
        assert_eq!(e.read_value(), EntryValue::Heap(&value));
        assert_eq!(e.read_value_bytes(), Some(value.readable_bytes()));
        assert_eq!(e.ttl_deadline(), 123);
    }

    #[test]
    fn write_heap_key_inline_value() {
        let mut e = Entry::empty();
        let key = VortexKey::from("this:key:is:definitely:longer:than:23");
        let value = VortexValue::from("short-inline");

        // SAFETY: `key` and `value` live for the duration of the assertions.
        unsafe {
            e.write_heap(0x91, &key, &value, 0);
        }

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

        // SAFETY: `key` and `value` live for the duration of the assertions.
        unsafe {
            e.write_heap(0x88, &key, &value, 0);
        }

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
        e.write_integer(0xA3, b"counter", 42, 0);

        assert!(e.is_full());
        assert_eq!(e.read_key(), b"counter");
        assert_eq!(e.read_value(), EntryValue::Integer(42));
        assert_eq!(e.read_integer(), Some(42));
        assert!(e.read_value_bytes().is_none());
        assert!(e.has_flag(FLAG_INTEGER_VALUE));
        assert_eq!(e.value_type(), VTYPE_STRING);
    }

    #[test]
    fn write_integer_negative() {
        let mut e = Entry::empty();
        e.write_integer(0xB0, b"neg", -999_999, 0);
        assert_eq!(e.read_integer(), Some(-999_999));
    }

    #[test]
    fn key_matching() {
        let mut e = Entry::empty();
        e.write_inline(0x92, b"test_key", b"val", 0);

        assert!(e.matches_key(b"test_key"));
        assert!(e.matches_key_with_ctrl(b"test_key", 0x92));
        assert!(!e.matches_key_with_ctrl(b"test_key", 0x93));
        assert!(!e.matches_key(b"test_ke"));
        assert!(!e.matches_key(b"test_key!"));
        assert!(!e.matches_key(b"other"));
    }

    #[test]
    fn ttl_operations() {
        let mut e = Entry::empty();
        e.write_inline(0x92, b"k", b"v", 1_000_000);

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
        assert!(e.is_expired(5_000));
    }

    #[test]
    fn mark_deleted() {
        let mut e = Entry::empty();
        e.write_inline(0x92, b"secret", b"data", 0);

        e.mark_deleted();
        assert!(e.is_deleted());
        assert!(!e.is_full());
        assert!(e.is_empty_or_deleted());
        assert_eq!(e.key_len, 0);
        assert_eq!(e.key_data, [0; 23]);
    }

    #[test]
    fn value_type_round_trip() {
        let mut entry = Entry::empty();
        entry.write_inline(0x81, b"k", b"v", 0);
        assert_eq!(entry.value_type(), VTYPE_STRING);

        let list = VortexValue::List(Box::new(VortexList::new()));
        // SAFETY: borrowed values stay alive for the duration of this test.
        unsafe {
            entry.write_heap(0x81, &VortexKey::from("k"), &list, 0);
        }
        assert_eq!(entry.value_type(), VTYPE_LIST);
    }

    #[test]
    fn max_inline_key_value() {
        let mut e = Entry::empty();
        let key = [b'K'; 23];
        let val = [b'V'; 21];
        e.write_inline(0x82, &key, &val, 0);

        assert_eq!(e.read_key(), &key);
        assert_eq!(e.read_value_bytes(), Some(val.as_slice()));
    }

    #[test]
    fn zero_length_key_value() {
        let mut e = Entry::empty();
        e.write_inline(0x82, b"", b"", 0);

        assert!(e.is_full());
        assert_eq!(e.read_key(), b"");
        assert_eq!(e.read_value_bytes(), Some(b"".as_slice()));
    }

    trait ReadableBytes {
        fn readable_bytes(&self) -> &[u8];
    }

    impl ReadableBytes for VortexValue {
        fn readable_bytes(&self) -> &[u8] {
            match self {
                VortexValue::InlineString(bytes) => bytes.as_bytes(),
                VortexValue::String(bytes) => bytes.as_ref(),
                other => panic!("expected string-like value, got {other:?}"),
            }
        }
    }
}
