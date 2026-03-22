/// Data structure encoding variants.
///
/// Each VortexDB value can be stored in different internal encodings
/// depending on size, access pattern, and optimization phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// Value stored inline within the hash table entry.
    Inline,
    /// Flat packed array — cache-line contiguous.
    FlatArray,
    /// Swiss Table with SIMD probing.
    SwissTable,
    /// Robin Hood hash table.
    RobinHood,
    /// Cache-optimized B+ tree.
    BPlusTree,
    /// Unrolled linked list with 64-element nodes.
    UnrolledList,
    /// Sorted flat array with binary search.
    SortedArray,
    /// Bitset encoding for integer sets.
    Bitset,
    /// Adaptive Radix Tree.
    ART,
    /// Delta-compressed packed log.
    DeltaLog,
    /// Skip list variant.
    SkipList,
}
