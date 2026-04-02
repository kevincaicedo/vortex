/// Data structure encoding variants.
///
/// Each VortexDB value can be stored in different internal encodings
/// depending on size, access pattern, and optimization phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Encoding {
    /// Value stored inline within the hash table entry.
    Inline = 0,
    /// Flat packed array — cache-line contiguous.
    FlatArray = 1,
    /// Swiss Table with SIMD probing.
    SwissTable = 2,
    /// Robin Hood hash table.
    RobinHood = 3,
    /// Cache-optimized B+ tree.
    BPlusTree = 4,
    /// Unrolled linked list with 64-element nodes.
    UnrolledList = 5,
    /// Sorted flat array with binary search.
    SortedArray = 6,
    /// Bitset encoding for integer sets.
    Bitset = 7,
    /// Adaptive Radix Tree.
    ART = 8,
    /// Delta-compressed packed log.
    DeltaLog = 9,
    /// Skip list variant.
    SkipList = 10,
}

impl Encoding {
    /// Pack into a 4-bit nibble (0–15). All current variants fit.
    #[inline]
    pub const fn to_u4(self) -> u8 {
        self as u8
    }

    /// Unpack from a 4-bit nibble. Unknown values map to `Inline`.
    #[inline]
    pub const fn from_u4(v: u8) -> Self {
        match v & 0x0F {
            0 => Self::Inline,
            1 => Self::FlatArray,
            2 => Self::SwissTable,
            3 => Self::RobinHood,
            4 => Self::BPlusTree,
            5 => Self::UnrolledList,
            6 => Self::SortedArray,
            7 => Self::Bitset,
            8 => Self::ART,
            9 => Self::DeltaLog,
            10 => Self::SkipList,
            _ => Self::Inline,
        }
    }
}
