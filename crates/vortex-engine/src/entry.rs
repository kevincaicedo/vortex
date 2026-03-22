/// 64-byte cache-line-aligned hash table entry.
///
/// Stores key + value + metadata in a single cache line to minimize
/// pointer chasing and memory accesses.  The TTL deadline is stored
/// as a raw `u64` (nanos since epoch, 0 = no expiry); bit 0 of
/// `flags` distinguishes second vs millisecond resolution.
///
/// Phase 0: Struct definition with correct layout.
/// TODO: Phase 3: Full SIMD-integrated Swiss Table entry.
#[repr(C, align(64))]
pub struct Entry {
    /// H₇ fingerprint (top 7 bits of hash) for SIMD probing.
    pub control: u8,
    /// Key length (inline keys only; 0 if heap-allocated).
    pub key_len: u8,
    /// Entry flags (type tag, heap indicators, TTL resolution bit 0).
    pub flags: u16,
    /// Padding for alignment.
    _pad0: u32,
    /// TTL deadline in nanoseconds (0 = no expiry).
    pub ttl_deadline: u64,
    /// Inline key data (≤23 bytes).
    pub key_data: [u8; 23],
    /// Value type tag.
    pub value_tag: u8,
    /// Inline value data (≤21 bytes).
    pub value_data: [u8; 21],
    /// Padding to reach 64 bytes.
    _pad1: [u8; 3],
}

// Static assertion: Entry must be exactly 64 bytes.
const _: () = assert!(std::mem::size_of::<Entry>() == 64);
const _: () = assert!(std::mem::align_of::<Entry>() == 64);

impl Entry {
    /// Creates a new empty entry.
    pub const fn empty() -> Self {
        Self {
            control: 0,
            key_len: 0,
            flags: 0,
            _pad0: 0,
            ttl_deadline: 0,
            key_data: [0; 23],
            value_tag: 0,
            value_data: [0; 21],
            _pad1: [0; 3],
        }
    }

    /// Returns true if this entry slot is empty.
    pub const fn is_empty(&self) -> bool {
        self.control == 0 && self.key_len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_size_and_alignment() {
        assert_eq!(std::mem::size_of::<Entry>(), 64);
        assert_eq!(std::mem::align_of::<Entry>(), 64);
    }

    #[test]
    fn empty_entry() {
        let e = Entry::empty();
        assert!(e.is_empty());
    }
}
