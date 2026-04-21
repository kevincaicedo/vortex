# SwissTable & The 64-Byte Entry Design

Whenever a shard is successfully accessed via an RwLock, the underlying collection driving the actual data store is a heavily engineered `SwissTable`. While standard hash maps (like Redis's separate chaining dictionaries) allocate elements over the heap chaotically, Vortex uses an **open-addressing, cache-line-friendly layout**.

## SIMD Map Probing

Vortex organizes the elements layout into contiguous arrays, breaking data mapping down into smaller chunks called *probe groups*.

With the `simd` feature enabled, evaluating slots works extremely fast:
1. Every group of 16 slots has its own 16 control bytes.
2. The hash map breaks the hashing integer `ahash` down into two parts: H1 and H2.
3. H1 guides the search logic to roughly which probe group the key is located in.
4. H2 is a "fingerprint." The SIMD instruction (e.g. `_mm_cmpeq_epi8` in AVX2) checks if the fingerprint exists anywhere in the entire 16 control bytes **simultaneously**, costing a single CPU instruction instead of looping 16 times over scattered memory values like classic chains do.

## The Optimal 64-Byte Memory Engine Entry

Redis handles an incredible amount of tiny keys (Counters, flags, Session tokens). Traditionally, objects in languages like C or Rust place the dictionary table values directly out onto the heap, paying the cost of a pointer-chase just to locate the data block. 

Vortex's structural design guarantees that every internal database block fits elegantly within exactly **64 Bytes**, mapping directly back to an L1 CPU cache-line size. 

```rust
#[repr(C, align(64))]
pub struct Entry {
    pub control: u8,        // H2 fingerprint / EMPTY / DELETED
    pub key_len: u8,        // Inline key length (0..=23)
    pub flags: u16,         // System flags defining integer type, heap location or TTL
    pub _pad0: u32,         // Reserved padding, handles Access Profile metrics.
    pub ttl_deadline: u64,  // 0 if it never expires, else nanoseconds timestamp
    pub key_data: [u8; 23], // Directly stores keys physically if they are small enough
    pub value_tag: u8,      // Helps differentiate data types.
    pub value_data: [u8; 21],// Inline value array, stores data up to 21 bytes.
    pub _pad1: [u8; 3],     // Alignment filler
}
```

### Inline Magic

Because we have pre-reserved chunks of the block:
- `key_data`: 23 bytes reserved.
- `value_data`: 21 bytes reserved.

If a User sends `SET my_token short_data` to VortexDB:
1. "my_token" is exactly 8 bytes (fits well within 23 bytes).
2. "short_data" is exactly 10 bytes (fits well within 21 bytes).

The entire execution bypasses heap allocation logic entirely. It stamps the bytes directly inside the `Entry` itself. A single cache-line retrieval delivers the lookup success, the TTL, the key, and the data, representing an extremely dense mechanical sympathy and saving gigabytes of heap memory pointers compared to prior datastores.

### Larger Values (Heap Allocation)
If the string values or complex datastructures span larger lengths (e.g., Megabytes), the 23 / 21 array space limits cannot satisfy it natively. Instead, the `Entry` takes those `value_data` fields and interprets them as **metapointers**, holding the address location directing the engine to standard `Box` or `Vec` heap allocations.
