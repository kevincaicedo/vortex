//! Adaptive Morphing Framework — access profile tracking and morph decision infrastructure.
//!
//! Phase 3 provides the **framework only** — no actual encoding transitions occur.
//! Phase 4 data structures register with the morph monitor and transition
//! between encodings based on access patterns.
//!
//! # AccessProfile bit layout (32 bits / 4 bytes)
//!
//! ```text
//! Bit   Width  Field
//! ────  ─────  ──────────────
//!  0-3    4    read_log₂     — saturating read intensity counter (0–15)
//!  4-7    4    write_log₂    — saturating write intensity counter (0–15)
//!    8    1    sequential    — sequential access hint
//!  9-11   3    size_class    — compact size bucket (0–7)
//! 12-15   4    encoding      — current Encoding variant (packed via to_u4)
//! 16-25  10    access_count  — wrapping counter; should_check() fires every 1024 accesses
//! 26-31   6    (reserved)    — future use
//! ```
//!
//! Stored in `Entry::_pad0` (the existing 4-byte padding field) for **zero
//! additional memory overhead** per key.

use vortex_common::Encoding;

// ── Bit-field constants ─────────────────────────────────────────────

const READ_MASK: u32 = 0xF; // bits 0-3

const WRITE_SHIFT: u32 = 4;
const WRITE_MASK: u32 = 0xF << WRITE_SHIFT; // bits 4-7

const SEQ_SHIFT: u32 = 8;
const SEQ_BIT: u32 = 1 << SEQ_SHIFT; // bit 8

const SIZE_SHIFT: u32 = 9;
const SIZE_MASK: u32 = 0x7 << SIZE_SHIFT; // bits 9-11

const ENC_SHIFT: u32 = 12;
const ENC_MASK: u32 = 0xF << ENC_SHIFT; // bits 12-15

const COUNTER_SHIFT: u32 = 16;
const COUNTER_WIDTH: u32 = 10;
const COUNTER_MAX: u32 = (1 << COUNTER_WIDTH) - 1; // 1023
const COUNTER_MASK: u32 = COUNTER_MAX << COUNTER_SHIFT; // bits 16-25

/// Initial counter value: 1 so `should_check()` first fires after 1023
/// more accesses (i.e. on the 1024th total access from creation).
const INIT_COUNTER: u32 = 1 << COUNTER_SHIFT;

// ── AccessProfile ───────────────────────────────────────────────────

/// 4-byte compact access profile for adaptive morphing decisions.
///
/// All mutation methods are O(1) with saturating / wrapping arithmetic
/// and zero allocations. The struct is `Copy` and can be stored directly
/// in an `Entry`'s `_pad0` field.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AccessProfile(u32);

impl AccessProfile {
    /// New profile with all counters at zero and the access counter
    /// initialized so that `should_check()` first fires after 1023 accesses.
    #[inline]
    pub const fn new() -> Self {
        Self(INIT_COUNTER)
    }

    /// New profile pre-set with an encoding.
    #[inline]
    pub const fn with_encoding(enc: Encoding) -> Self {
        Self(INIT_COUNTER | ((enc.to_u4() as u32) << ENC_SHIFT))
    }

    // ── Read / write intensity ──────────────────────────────────────

    /// Record a read access. Saturating-increments the 4-bit read counter
    /// and bumps the access counter (for `should_check()` gating).
    #[inline]
    pub fn record_read(&mut self) {
        let r = self.0 & READ_MASK;
        // `min` compiles to `cmp; csel` on aarch64 — branchless.
        let new_r = (r + 1).min(15);
        self.0 = (self.0 & !READ_MASK) | new_r;
        self.bump_counter();
    }

    /// Record a write access. Same mechanics as [`record_read`].
    #[inline]
    pub fn record_write(&mut self) {
        let w = (self.0 & WRITE_MASK) >> WRITE_SHIFT;
        let new_w = (w + 1).min(15);
        self.0 = (self.0 & !WRITE_MASK) | (new_w << WRITE_SHIFT);
        self.bump_counter();
    }

    /// Current read intensity (0–15).
    #[inline]
    pub const fn read_count(&self) -> u8 {
        (self.0 & READ_MASK) as u8
    }

    /// Current write intensity (0–15).
    #[inline]
    pub const fn write_count(&self) -> u8 {
        ((self.0 & WRITE_MASK) >> WRITE_SHIFT) as u8
    }

    // ── Sequential hint ─────────────────────────────────────────────

    #[inline]
    pub const fn is_sequential(&self) -> bool {
        (self.0 & SEQ_BIT) != 0
    }

    /// Branchless set: `bool as u32` is 0 or 1.
    #[inline]
    pub fn set_sequential(&mut self, seq: bool) {
        self.0 = (self.0 & !SEQ_BIT) | ((seq as u32) << SEQ_SHIFT);
    }

    // ── Size class ──────────────────────────────────────────────────

    /// 3-bit size bucket (0–7).
    #[inline]
    pub const fn size_class(&self) -> u8 {
        ((self.0 & SIZE_MASK) >> SIZE_SHIFT) as u8
    }

    #[inline]
    pub fn set_size_class(&mut self, class: u8) {
        debug_assert!(class <= 7);
        self.0 = (self.0 & !SIZE_MASK) | (((class as u32) & 0x7) << SIZE_SHIFT);
    }

    // ── Encoding ────────────────────────────────────────────────────

    /// Current encoding stored in the profile.
    #[inline]
    pub const fn encoding(&self) -> Encoding {
        Encoding::from_u4(((self.0 >> ENC_SHIFT) & 0xF) as u8)
    }

    #[inline]
    pub fn set_encoding(&mut self, enc: Encoding) {
        self.0 = (self.0 & !ENC_MASK) | ((enc.to_u4() as u32) << ENC_SHIFT);
    }

    // ── Morph-check gating ──────────────────────────────────────────

    /// Returns `true` every 1024th access (10-bit wrapping counter).
    #[inline]
    pub const fn should_check(&self) -> bool {
        (self.0 & COUNTER_MASK) == 0
    }

    // ── Raw round-trip (for Entry storage) ──────────────────────────

    #[inline]
    pub const fn as_u32(&self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn from_u32(bits: u32) -> Self {
        Self(bits)
    }

    // ── Internal ────────────────────────────────────────────────────

    #[inline]
    fn bump_counter(&mut self) {
        let c = ((self.0 >> COUNTER_SHIFT) & COUNTER_MAX).wrapping_add(1) & COUNTER_MAX;
        self.0 = (self.0 & !COUNTER_MASK) | (c << COUNTER_SHIFT);
    }
}

impl core::fmt::Debug for AccessProfile {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AccessProfile")
            .field("read", &self.read_count())
            .field("write", &self.write_count())
            .field("sequential", &self.is_sequential())
            .field("size_class", &self.size_class())
            .field("encoding", &self.encoding())
            .field("should_check", &self.should_check())
            .finish()
    }
}

// ── MorphMonitor trait ──────────────────────────────────────────────

/// Decides whether a value should transition to a different encoding.
///
/// Called by the engine when `AccessProfile::should_check()` returns `true`
/// (i.e. every 1024 accesses). Implementations **must** be cheap — this
/// runs on the hot command path.
pub trait MorphMonitor {
    /// Returns `Some(target)` if the value should morph, `None` to keep current encoding.
    fn should_morph(&self, profile: &AccessProfile, current_size: usize) -> Option<Encoding>;
}

// ── DefaultMorphMonitor ─────────────────────────────────────────────

/// Default morph monitor with Phase 4 transition rules registered for
/// forward compatibility.
///
/// - **Strings:** never morph (always `Inline`)
/// - **Lists:** `FlatArray` (< 128, sequential) → `UnrolledList` (< 4096) → `BPlusTree`
/// - **Hashes / Sets:** `SortedArray` (< 64) → `SwissTable`
pub struct DefaultMorphMonitor;

impl MorphMonitor for DefaultMorphMonitor {
    #[inline]
    fn should_morph(&self, profile: &AccessProfile, current_size: usize) -> Option<Encoding> {
        let current = profile.encoding();

        match current {
            // ── Strings never morph ─────────────────────────────────
            Encoding::Inline => None,

            // ── List encodings ──────────────────────────────────────
            Encoding::FlatArray | Encoding::UnrolledList | Encoding::BPlusTree => {
                let target = if current_size < 128 && profile.is_sequential() {
                    Encoding::FlatArray
                } else if current_size < 4096 {
                    Encoding::UnrolledList
                } else {
                    Encoding::BPlusTree
                };
                if target != current {
                    Some(target)
                } else {
                    None
                }
            }

            // ── Hash / Set encodings ────────────────────────────────
            Encoding::SortedArray | Encoding::SwissTable | Encoding::RobinHood => {
                let target = if current_size < 64 {
                    Encoding::SortedArray
                } else {
                    Encoding::SwissTable
                };
                if target != current {
                    Some(target)
                } else {
                    None
                }
            }

            // All other encodings: no morphing rules yet
            _ => None,
        }
    }
}

// ── DisabledMorphMonitor ────────────────────────────────────────────

/// No-op monitor used when `adaptive_structures = false` (Redis-compatible
/// static thresholds). Always returns `None`.
pub struct DisabledMorphMonitor;

impl MorphMonitor for DisabledMorphMonitor {
    #[inline]
    fn should_morph(&self, _profile: &AccessProfile, _current_size: usize) -> Option<Encoding> {
        None
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── AccessProfile construction ──────────────────────────────────

    #[test]
    fn size_is_4_bytes() {
        assert_eq!(core::mem::size_of::<AccessProfile>(), 4);
    }

    #[test]
    fn new_profile_defaults() {
        let p = AccessProfile::new();
        assert_eq!(p.read_count(), 0);
        assert_eq!(p.write_count(), 0);
        assert!(!p.is_sequential());
        assert_eq!(p.size_class(), 0);
        assert_eq!(p.encoding(), Encoding::Inline);
        // Counter starts at 1, not 0 — should_check is false initially.
        assert!(!p.should_check());
    }

    #[test]
    fn with_encoding_roundtrip() {
        for enc in [
            Encoding::Inline,
            Encoding::FlatArray,
            Encoding::SwissTable,
            Encoding::RobinHood,
            Encoding::BPlusTree,
            Encoding::UnrolledList,
            Encoding::SortedArray,
            Encoding::Bitset,
            Encoding::ART,
            Encoding::DeltaLog,
            Encoding::SkipList,
        ] {
            let p = AccessProfile::with_encoding(enc);
            assert_eq!(p.encoding(), enc, "roundtrip failed for {enc:?}");
        }
    }

    // ── Field getters / setters ─────────────────────────────────────

    #[test]
    fn read_write_count_roundtrip() {
        let mut p = AccessProfile::new();
        for i in 1..=15u8 {
            p.record_read();
            assert_eq!(p.read_count(), i);
        }
        for i in 1..=15u8 {
            p.record_write();
            assert_eq!(p.write_count(), i);
        }
    }

    #[test]
    fn saturating_read_does_not_overflow() {
        let mut p = AccessProfile::new();
        for _ in 0..100 {
            p.record_read();
        }
        assert_eq!(p.read_count(), 15);
        // Ensure other fields are untouched.
        assert_eq!(p.encoding(), Encoding::Inline);
        assert!(!p.is_sequential());
    }

    #[test]
    fn saturating_write_does_not_overflow() {
        let mut p = AccessProfile::new();
        for _ in 0..100 {
            p.record_write();
        }
        assert_eq!(p.write_count(), 15);
    }

    #[test]
    fn sequential_roundtrip() {
        let mut p = AccessProfile::new();
        assert!(!p.is_sequential());
        p.set_sequential(true);
        assert!(p.is_sequential());
        p.set_sequential(false);
        assert!(!p.is_sequential());
    }

    #[test]
    fn size_class_roundtrip() {
        let mut p = AccessProfile::new();
        for c in 0..=7u8 {
            p.set_size_class(c);
            assert_eq!(p.size_class(), c);
        }
    }

    #[test]
    fn encoding_set_get() {
        let mut p = AccessProfile::new();
        p.set_encoding(Encoding::BPlusTree);
        assert_eq!(p.encoding(), Encoding::BPlusTree);
        p.set_encoding(Encoding::SwissTable);
        assert_eq!(p.encoding(), Encoding::SwissTable);
    }

    #[test]
    fn fields_are_independent() {
        let mut p = AccessProfile::new();
        p.set_encoding(Encoding::UnrolledList);
        p.set_size_class(5);
        p.set_sequential(true);
        for _ in 0..10 {
            p.record_read();
        }
        for _ in 0..7 {
            p.record_write();
        }

        // Verify all fields survived concurrent mutation.
        assert_eq!(p.encoding(), Encoding::UnrolledList);
        assert_eq!(p.size_class(), 5);
        assert!(p.is_sequential());
        assert_eq!(p.read_count(), 10);
        assert_eq!(p.write_count(), 7);
    }

    // ── should_check gating ─────────────────────────────────────────

    #[test]
    fn should_check_fires_at_1024() {
        let mut p = AccessProfile::new();
        // Counter starts at 1. After 1023 bumps it wraps to 0.
        let mut checks = 0u32;
        for _ in 0..2048 {
            p.record_read();
            if p.should_check() {
                checks += 1;
            }
        }
        // Expect exactly 2 firings: at access 1023 and 2047 (0-indexed from init).
        // Actually: counter starts at 1. After 1023 reads → counter = 0 → fires.
        // Then after 1024 more reads → counter = 0 again → fires.
        assert_eq!(checks, 2);
    }

    #[test]
    fn should_check_fires_mixed_read_write() {
        let mut p = AccessProfile::new();
        let mut checks = 0u32;
        for i in 0..1024 {
            if i % 2 == 0 {
                p.record_read();
            } else {
                p.record_write();
            }
            if p.should_check() {
                checks += 1;
            }
        }
        // 1023 bumps → fire, then 1 more bump (total 1024) → no fire yet.
        assert_eq!(checks, 1);
    }

    // ── u32 round-trip ──────────────────────────────────────────────

    #[test]
    fn u32_roundtrip() {
        let mut p = AccessProfile::with_encoding(Encoding::SortedArray);
        p.set_size_class(3);
        p.set_sequential(true);
        p.record_read();
        p.record_write();

        let bits = p.as_u32();
        let p2 = AccessProfile::from_u32(bits);
        assert_eq!(p, p2);
        assert_eq!(p2.encoding(), Encoding::SortedArray);
        assert_eq!(p2.size_class(), 3);
        assert!(p2.is_sequential());
        assert_eq!(p2.read_count(), 1);
        assert_eq!(p2.write_count(), 1);
    }

    // ── DefaultMorphMonitor ─────────────────────────────────────────

    #[test]
    fn monitor_strings_never_morph() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::Inline);
        assert_eq!(m.should_morph(&p, 0), None);
        assert_eq!(m.should_morph(&p, 1_000_000), None);
    }

    #[test]
    fn monitor_list_flat_to_unrolled() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::FlatArray);
        // Non-sequential + size ≥ 128 → UnrolledList
        assert_eq!(m.should_morph(&p, 128), Some(Encoding::UnrolledList));
        assert_eq!(m.should_morph(&p, 200), Some(Encoding::UnrolledList));
    }

    #[test]
    fn monitor_list_stays_flat_if_small_sequential() {
        let m = DefaultMorphMonitor;
        let mut p = AccessProfile::with_encoding(Encoding::FlatArray);
        p.set_sequential(true);
        assert_eq!(m.should_morph(&p, 64), None);
        assert_eq!(m.should_morph(&p, 127), None);
    }

    #[test]
    fn monitor_list_unrolled_to_bplus() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::UnrolledList);
        assert_eq!(m.should_morph(&p, 4096), Some(Encoding::BPlusTree));
        assert_eq!(m.should_morph(&p, 10_000), Some(Encoding::BPlusTree));
    }

    #[test]
    fn monitor_list_bplus_stays() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::BPlusTree);
        assert_eq!(m.should_morph(&p, 5000), None);
    }

    #[test]
    fn monitor_hash_sorted_to_swiss() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::SortedArray);
        assert_eq!(m.should_morph(&p, 64), Some(Encoding::SwissTable));
        assert_eq!(m.should_morph(&p, 1000), Some(Encoding::SwissTable));
    }

    #[test]
    fn monitor_hash_stays_sorted_if_small() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::SortedArray);
        assert_eq!(m.should_morph(&p, 63), None);
        assert_eq!(m.should_morph(&p, 0), None);
    }

    #[test]
    fn monitor_swiss_stays_swiss() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::SwissTable);
        assert_eq!(m.should_morph(&p, 1000), None);
    }

    #[test]
    fn monitor_swiss_shrinks_to_sorted() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::SwissTable);
        assert_eq!(m.should_morph(&p, 32), Some(Encoding::SortedArray));
    }

    // ── DisabledMorphMonitor ────────────────────────────────────────

    #[test]
    fn disabled_monitor_always_none() {
        let m = DisabledMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::FlatArray);
        assert_eq!(m.should_morph(&p, 0), None);
        assert_eq!(m.should_morph(&p, 10_000), None);
    }

    // ── Edge cases ──────────────────────────────────────────────────

    #[test]
    fn monitor_unknown_encoding_returns_none() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::Bitset);
        assert_eq!(m.should_morph(&p, 100), None);
    }

    #[test]
    fn list_flat_non_sequential_small_morphs_to_unrolled() {
        let m = DefaultMorphMonitor;
        // FlatArray, NOT sequential, size < 128 → still UnrolledList
        // because the condition is `size < 128 AND sequential` for FlatArray
        let p = AccessProfile::with_encoding(Encoding::FlatArray);
        assert_eq!(m.should_morph(&p, 64), Some(Encoding::UnrolledList));
    }

    #[test]
    fn list_flat_to_bplus_directly() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::FlatArray);
        assert_eq!(m.should_morph(&p, 5000), Some(Encoding::BPlusTree));
    }

    #[test]
    fn bplus_shrinks_to_unrolled() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::BPlusTree);
        assert_eq!(m.should_morph(&p, 100), Some(Encoding::UnrolledList));
    }

    #[test]
    fn robin_hood_morphs_like_hash() {
        let m = DefaultMorphMonitor;
        let p = AccessProfile::with_encoding(Encoding::RobinHood);
        assert_eq!(m.should_morph(&p, 10), Some(Encoding::SortedArray));
        assert_eq!(m.should_morph(&p, 100), Some(Encoding::SwissTable));
    }
}
