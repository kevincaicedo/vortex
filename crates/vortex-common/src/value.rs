use bytes::Bytes;

use crate::encoding::Encoding;

/// Tagged enum representing all VortexDB value types.
///
/// Small values (≤23 bytes) are stored inline to avoid heap allocation.
/// Integer values store `i64` inline — zero allocation for counters.
#[derive(Debug, Clone, PartialEq)]
pub enum VortexValue {
    /// Small string stored inline (≤23 bytes).
    InlineString(InlineBytes),
    /// Heap-allocated string.
    String(Bytes),
    /// Integer value (INCR/DECR counters).
    Integer(i64),
    /// List data structure.
    List(Box<VortexList>),
    /// Hash (field→value map).
    Hash(Box<VortexHash>),
    /// Set data structure.
    Set(Box<VortexSet>),
    /// Sorted set with scores.
    SortedSet(Box<VortexSortedSet>),
    /// Stream data structure.
    Stream(Box<VortexStream>),
}

/// Inline byte storage — fits within a hash table entry without allocation.
#[derive(Clone, PartialEq, Eq)]
pub struct InlineBytes {
    len: u8,
    data: [u8; 23],
}

impl InlineBytes {
    /// Creates inline bytes from a slice. Panics if slice exceeds 23 bytes.
    pub fn from_slice(bytes: &[u8]) -> Self {
        assert!(bytes.len() <= 23, "InlineBytes: slice exceeds 23 bytes");
        let mut data = [0u8; 23];
        data[..bytes.len()].copy_from_slice(bytes);
        Self {
            len: bytes.len() as u8,
            data,
        }
    }

    /// Returns the stored bytes as a slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }

    /// Returns the length.
    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Returns true if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl std::fmt::Debug for InlineBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match std::str::from_utf8(self.as_bytes()) {
            Ok(s) => write!(f, "InlineBytes({s:?})"),
            Err(_) => write!(f, "InlineBytes({:?})", self.as_bytes()),
        }
    }
}

// TODO: Placeholder types for container values.
// Real implementations come in later phases.

/// TODO: Placeholder list type. Real implementation in Phase 3 (AMS).
#[derive(Debug, Clone, PartialEq)]
pub struct VortexList {
    entries: Vec<Bytes>,
}

impl VortexList {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }
}

impl Default for VortexList {
    fn default() -> Self {
        Self::new()
    }
}

/// TODO: Placeholder hash type. Real implementation in Phase 3 (AMS).
#[derive(Debug, Clone, PartialEq)]
pub struct VortexHash {
    entries: Vec<(Bytes, Bytes)>,
}

impl VortexHash {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }
}

impl Default for VortexHash {
    fn default() -> Self {
        Self::new()
    }
}

/// TODO: Placeholder set type. Real implementation in Phase 3 (AMS).
#[derive(Debug, Clone, PartialEq)]
pub struct VortexSet {
    entries: Vec<Bytes>,
}

impl VortexSet {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }
}

impl Default for VortexSet {
    fn default() -> Self {
        Self::new()
    }
}

/// TODO: Placeholder sorted set type. Real implementation in Phase 3 (AMS).
#[derive(Debug, Clone, PartialEq)]
pub struct VortexSortedSet {
    entries: Vec<(f64, Bytes)>,
}

impl VortexSortedSet {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }
}

impl Default for VortexSortedSet {
    fn default() -> Self {
        Self::new()
    }
}

/// TODO: Placeholder stream type. Real implementation in Phase 4.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexStream {
    entries: Vec<(u64, Vec<(Bytes, Bytes)>)>,
}

impl VortexStream {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }
}

impl Default for VortexStream {
    fn default() -> Self {
        Self::new()
    }
}

impl VortexValue {
    /// Returns the current encoding of this value.
    pub fn encoding(&self) -> Encoding {
        match self {
            Self::InlineString(_) | Self::Integer(_) => Encoding::Inline,
            Self::String(_) => Encoding::Inline,
            Self::List(_) => Encoding::FlatArray,
            Self::Hash(_) => Encoding::FlatArray,
            Self::Set(_) => Encoding::FlatArray,
            Self::SortedSet(_) => Encoding::SortedArray,
            Self::Stream(_) => Encoding::DeltaLog,
        }
    }

    /// Returns a rough estimate of memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        match self {
            Self::InlineString(ib) => std::mem::size_of::<InlineBytes>() + ib.len(),
            Self::String(b) => std::mem::size_of::<Bytes>() + b.len(),
            Self::Integer(_) => std::mem::size_of::<i64>(),
            Self::List(l) => {
                std::mem::size_of::<VortexList>()
                    + l.entries.iter().map(|e| e.len()).sum::<usize>()
            }
            Self::Hash(h) => {
                std::mem::size_of::<VortexHash>()
                    + h.entries
                        .iter()
                        .map(|(k, v)| k.len() + v.len())
                        .sum::<usize>()
            }
            Self::Set(s) => {
                std::mem::size_of::<VortexSet>()
                    + s.entries.iter().map(|e| e.len()).sum::<usize>()
            }
            Self::SortedSet(ss) => {
                std::mem::size_of::<VortexSortedSet>()
                    + ss.entries
                        .iter()
                        .map(|(_, v)| std::mem::size_of::<f64>() + v.len())
                        .sum::<usize>()
            }
            Self::Stream(st) => {
                std::mem::size_of::<VortexStream>()
                    + st.entries
                        .iter()
                        .map(|(_, fields)| {
                            8 + fields.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
                        })
                        .sum::<usize>()
            }
        }
    }
}

impl From<i64> for VortexValue {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}

impl From<&str> for VortexValue {
    fn from(s: &str) -> Self {
        if s.len() <= 23 {
            Self::InlineString(InlineBytes::from_slice(s.as_bytes()))
        } else {
            Self::String(Bytes::copy_from_slice(s.as_bytes()))
        }
    }
}

impl From<Bytes> for VortexValue {
    fn from(b: Bytes) -> Self {
        if b.len() <= 23 {
            Self::InlineString(InlineBytes::from_slice(&b))
        } else {
            Self::String(b)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_string_value() {
        let val = VortexValue::from("hello");
        assert!(matches!(val, VortexValue::InlineString(_)));
        assert_eq!(val.encoding(), Encoding::Inline);
    }

    #[test]
    fn heap_string_value() {
        let long = "a]".repeat(24);
        let val = VortexValue::from(long.as_str());
        assert!(matches!(val, VortexValue::String(_)));
    }

    #[test]
    fn integer_value() {
        let val = VortexValue::from(42i64);
        assert!(matches!(val, VortexValue::Integer(42)));
        assert_eq!(val.encoding(), Encoding::Inline);
    }

    #[test]
    fn inline_bytes_roundtrip() {
        let ib = InlineBytes::from_slice(b"test");
        assert_eq!(ib.as_bytes(), b"test");
        assert_eq!(ib.len(), 4);
        assert!(!ib.is_empty());
    }
}
