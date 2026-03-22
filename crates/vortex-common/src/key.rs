use std::fmt;
use std::hash::{Hash, Hasher};

use crate::MAX_INLINE_KEY_LEN;

/// Small-string-optimized key type for VortexDB.
///
/// Keys ≤ 23 bytes are stored inline without heap allocation, covering >90%
/// of real-world keys. Larger keys fall back to heap allocation.
///
/// Total size: 32 bytes (inline variant uses 24, but we pad to 32 for alignment).
#[derive(Clone)]
pub enum VortexKey {
    /// Keys that fit within 23 bytes — stored inline, zero allocation.
    Inline { len: u8, data: [u8; MAX_INLINE_KEY_LEN] },
    /// Keys exceeding 23 bytes — heap allocated.
    Heap(Vec<u8>),
}

impl VortexKey {
    /// Creates a new key from a byte slice.
    ///
    /// If the slice fits in 23 bytes, it's stored inline.
    /// Otherwise it's heap-allocated.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        if bytes.len() <= MAX_INLINE_KEY_LEN {
            let mut data = [0u8; MAX_INLINE_KEY_LEN];
            data[..bytes.len()].copy_from_slice(bytes);
            Self::Inline {
                len: bytes.len() as u8,
                data,
            }
        } else {
            Self::Heap(bytes.to_vec())
        }
    }

    /// Returns the key as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Inline { len, data } => &data[..*len as usize],
            Self::Heap(v) => v,
        }
    }

    /// Returns the length of the key in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => *len as usize,
            Self::Heap(v) => v.len(),
        }
    }

    /// Returns true if the key is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if the key is stored inline (no heap allocation).
    #[inline]
    pub fn is_inline(&self) -> bool {
        matches!(self, Self::Inline { .. })
    }
}

impl AsRef<[u8]> for VortexKey {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<&[u8]> for VortexKey {
    fn from(bytes: &[u8]) -> Self {
        Self::from_bytes(bytes)
    }
}

impl From<&str> for VortexKey {
    fn from(s: &str) -> Self {
        Self::from_bytes(s.as_bytes())
    }
}

impl From<Vec<u8>> for VortexKey {
    fn from(v: Vec<u8>) -> Self {
        if v.len() <= MAX_INLINE_KEY_LEN {
            Self::from_bytes(&v)
        } else {
            Self::Heap(v)
        }
    }
}

impl From<String> for VortexKey {
    fn from(s: String) -> Self {
        Self::from(s.into_bytes())
    }
}

impl PartialEq for VortexKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for VortexKey {}

impl PartialOrd for VortexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VortexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl Hash for VortexKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl fmt::Debug for VortexKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(self.as_bytes()) {
            Ok(s) => write!(f, "VortexKey({s:?})"),
            Err(_) => write!(f, "VortexKey({:?})", self.as_bytes()),
        }
    }
}

impl fmt::Display for VortexKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(self.as_bytes()) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => write!(f, "{:?}", self.as_bytes()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_key_roundtrip() {
        let key = VortexKey::from("hello");
        assert!(key.is_inline());
        assert_eq!(key.as_bytes(), b"hello");
        assert_eq!(key.len(), 5);
    }

    #[test]
    fn max_inline_key() {
        let data = vec![b'x'; MAX_INLINE_KEY_LEN];
        let key = VortexKey::from_bytes(&data);
        assert!(key.is_inline());
        assert_eq!(key.as_bytes(), &data[..]);
    }

    #[test]
    fn heap_key_roundtrip() {
        let data = vec![b'y'; MAX_INLINE_KEY_LEN + 1];
        let key = VortexKey::from_bytes(&data);
        assert!(!key.is_inline());
        assert_eq!(key.as_bytes(), &data[..]);
    }

    #[test]
    fn empty_key() {
        let key = VortexKey::from("");
        assert!(key.is_inline());
        assert!(key.is_empty());
        assert_eq!(key.len(), 0);
    }

    #[test]
    fn key_equality() {
        let k1 = VortexKey::from("test");
        let k2 = VortexKey::from("test");
        let k3 = VortexKey::from("other");
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }

    #[test]
    fn key_ordering() {
        let k1 = VortexKey::from("aaa");
        let k2 = VortexKey::from("bbb");
        assert!(k1 < k2);
    }

    #[test]
    fn key_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;

        let k1 = VortexKey::from("test_key");
        let k2 = VortexKey::from("test_key");

        let hash = |k: &VortexKey| {
            let mut h = DefaultHasher::new();
            k.hash(&mut h);
            h.finish()
        };

        assert_eq!(hash(&k1), hash(&k2));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_any_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..1024)) {
            let key = VortexKey::from_bytes(&bytes);
            prop_assert_eq!(key.as_bytes(), &bytes[..]);
            prop_assert_eq!(key.len(), bytes.len());
        }

        #[test]
        fn inline_threshold(bytes in proptest::collection::vec(any::<u8>(), 0..=MAX_INLINE_KEY_LEN)) {
            let key = VortexKey::from_bytes(&bytes);
            prop_assert!(key.is_inline());
        }

        #[test]
        fn heap_threshold(bytes in proptest::collection::vec(any::<u8>(), (MAX_INLINE_KEY_LEN + 1)..512)) {
            let key = VortexKey::from_bytes(&bytes);
            prop_assert!(!key.is_inline());
        }
    }
}
