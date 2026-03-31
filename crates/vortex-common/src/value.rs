use bytes::Bytes;

use crate::encoding::Encoding;

/// Returns the number of characters in the decimal representation of an i64.
#[inline]
fn itoa_len(mut n: i64) -> usize {
    if n == 0 {
        return 1;
    }
    let mut len = 0usize;
    if n < 0 {
        len += 1;
        // Handle i64::MIN specially (abs overflows).
        if n == i64::MIN {
            return 20; // "-9223372036854775808"
        }
        n = -n;
    }
    let mut v = n as u64;
    while v > 0 {
        v /= 10;
        len += 1;
    }
    len
}

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

    /// Tries to append `extra` bytes in-place. Returns `true` if the combined
    /// length fits within the 23-byte inline capacity, `false` otherwise
    /// (contents unchanged on failure).
    #[inline]
    pub fn try_extend(&mut self, extra: &[u8]) -> bool {
        let old = self.len as usize;
        let new_len = old + extra.len();
        if new_len > 23 {
            return false;
        }
        self.data[old..new_len].copy_from_slice(extra);
        self.len = new_len as u8;
        true
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
        Self {
            entries: Vec::new(),
        }
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
        Self {
            entries: Vec::new(),
        }
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
        Self {
            entries: Vec::new(),
        }
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
        Self {
            entries: Vec::new(),
        }
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
        Self {
            entries: Vec::new(),
        }
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

    /// Returns `true` if this value is a string type (inline, heap, or integer-encoded).
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(
            self,
            Self::InlineString(_) | Self::String(_) | Self::Integer(_)
        )
    }

    /// Returns the Redis-compatible type name for this value.
    #[inline]
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::InlineString(_) | Self::String(_) | Self::Integer(_) => "string",
            Self::List(_) => "list",
            Self::Hash(_) => "hash",
            Self::Set(_) => "set",
            Self::SortedSet(_) => "zset",
            Self::Stream(_) => "stream",
        }
    }

    /// Returns the byte representation of a string-typed value.
    ///
    /// - `InlineString` → inline bytes
    /// - `String` → heap bytes
    /// - `Integer` → `None` (use `try_as_integer` instead)
    /// - Other types → `None`
    #[inline]
    pub fn as_string_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::InlineString(ib) => Some(ib.as_bytes()),
            Self::String(b) => Some(b.as_ref()),
            _ => None,
        }
    }

    /// Returns the integer value if this is an `Integer` variant.
    #[inline]
    pub fn try_as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// Returns the length of the string representation.
    ///
    /// For `InlineString`/`String`: byte length.
    /// For `Integer`: length of the decimal representation.
    /// For other types: 0.
    #[inline]
    pub fn strlen(&self) -> usize {
        match self {
            Self::InlineString(ib) => ib.len(),
            Self::String(b) => b.len(),
            Self::Integer(n) => itoa_len(*n),
            _ => 0,
        }
    }

    /// Creates a VortexValue from raw bytes, using inline storage when possible.
    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        if bytes.len() <= 23 {
            Self::InlineString(InlineBytes::from_slice(bytes))
        } else {
            Self::String(Bytes::copy_from_slice(bytes))
        }
    }

    /// Returns a rough estimate of memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        match self {
            Self::InlineString(ib) => std::mem::size_of::<InlineBytes>() + ib.len(),
            Self::String(b) => std::mem::size_of::<Bytes>() + b.len(),
            Self::Integer(_) => std::mem::size_of::<i64>(),
            Self::List(l) => {
                std::mem::size_of::<VortexList>() + l.entries.iter().map(|e| e.len()).sum::<usize>()
            }
            Self::Hash(h) => {
                std::mem::size_of::<VortexHash>()
                    + h.entries
                        .iter()
                        .map(|(k, v)| k.len() + v.len())
                        .sum::<usize>()
            }
            Self::Set(s) => {
                std::mem::size_of::<VortexSet>() + s.entries.iter().map(|e| e.len()).sum::<usize>()
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

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn arb_short_string()(s in "[a-z]{0,23}") -> String {
            s
        }
    }

    prop_compose! {
        fn arb_long_string()(s in "[a-z]{24,128}") -> String {
            s
        }
    }

    proptest! {
        #[test]
        fn short_strings_are_inline(s in "[a-z]{0,23}") {
            let val = VortexValue::from(s.as_str());
            prop_assert!(matches!(val, VortexValue::InlineString(_)));
        }

        #[test]
        fn long_strings_are_heap(s in "[a-z]{24,128}") {
            let val = VortexValue::from(s.as_str());
            prop_assert!(matches!(val, VortexValue::String(_)));
        }

        #[test]
        fn integer_roundtrip(n: i64) {
            let val = VortexValue::from(n);
            match val {
                VortexValue::Integer(v) => prop_assert_eq!(v, n),
                _ => prop_assert!(false, "expected Integer variant"),
            }
        }

        #[test]
        fn bytes_from_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..128)) {
            let b = Bytes::from(data.clone());
            let val = VortexValue::from(b);
            match &val {
                VortexValue::InlineString(ib) if data.len() <= 23 => {
                    prop_assert_eq!(ib.as_bytes(), &data[..]);
                }
                VortexValue::String(s) if data.len() > 23 => {
                    prop_assert_eq!(s.as_ref(), &data[..]);
                }
                _ => prop_assert!(false, "unexpected variant for len={}", data.len()),
            }
        }

        #[test]
        fn memory_usage_positive(n: i64) {
            let val = VortexValue::from(n);
            prop_assert!(val.memory_usage() > 0);
        }
    }
}
