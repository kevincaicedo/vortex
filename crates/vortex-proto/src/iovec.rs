//! Scatter-gather I/O writer for RESP serialization.
//!
//! [`IovecWriter`] accumulates `iovec` segments that can be passed directly to
//! `writev` / io_uring `IORING_OP_WRITEV` without copying into a contiguous
//! buffer. Static segments (pre-computed responses, LUT entries) are
//! zero-copy pointers into `.rodata`.  Dynamic segments (formatted integers,
//! encoded lengths) are stored in a small inline scratch buffer.

use std::io::IoSlice;

/// Maximum number of inline iovec segments before spilling to the heap.
/// 16 covers a 10-element array response + overhead and fits in a cache line.
const INLINE_CAP: usize = 16;

/// Scratch buffer for dynamically formatted fragments (integer digits, length
/// prefixes).  512 bytes covers ~50 RESP length lines at 10 bytes each.
const SCRATCH_CAP: usize = 512;

/// Scatter-gather writer that accumulates `iovec` segments for `writev`.
///
/// # Lifetime Safety
///
/// The writer borrows static slices (pre-computed responses) and owns dynamic
/// data in `scratch`. All `IoSlice` entries returned by [`as_io_slices`]
/// borrow `&self`, ensuring the backing memory outlives the slices.
pub struct IovecWriter {
    /// Borrowed pointers or offsets into `scratch`, with segment lengths.
    segments: IovecSegments,
    /// Inline scratch buffer for dynamically formatted bytes.
    scratch: ScratchBuf,
}

/// Small-vector for iovec segment descriptors.
#[allow(clippy::large_enum_variant)] // Intentional: inline avoids heap allocation on hot path
enum IovecSegments {
    Inline { buf: [Segment; INLINE_CAP], len: u8 },
    Heap(Vec<Segment>),
}

/// Inline + overflow scratch buffer.
#[allow(clippy::large_enum_variant)] // Intentional: inline avoids heap allocation on hot path
enum ScratchBuf {
    Inline { buf: [u8; SCRATCH_CAP], len: u16 },
    Heap(Vec<u8>),
}

/// A single iovec segment descriptor.
#[derive(Clone, Copy)]
struct Segment {
    /// Borrowed pointer bits or scratch offset.
    data: usize,
    /// Length plus kind bit. Keeping this as one word keeps `Segment` compact.
    len_and_kind: usize,
}

// SAFETY: IovecWriter is used single-threaded on the reactor thread.
// `Segment::data` stores either a borrowed pointer that the caller guarantees
// outlives the writer or an offset into our owned scratch buffer. Scratch
// offsets are resolved and bounds-checked before building slices.
unsafe impl Send for IovecWriter {}

impl Segment {
    #[inline]
    fn borrowed(buf: &[u8]) -> Self {
        Self::new(buf.as_ptr() as usize, buf.len(), false)
    }

    #[inline]
    fn scratch(offset: usize, len: usize) -> Self {
        Self::new(offset, len, true)
    }

    #[inline]
    fn new(data: usize, len: usize, scratch: bool) -> Self {
        assert!(
            len <= SEGMENT_LEN_MASK,
            "iovec segment length exceeds representable range"
        );
        let kind = if scratch { SEGMENT_SCRATCH_BIT } else { 0 };
        Self {
            data,
            len_and_kind: kind | len,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len_and_kind & SEGMENT_LEN_MASK
    }

    #[inline]
    fn is_scratch(&self) -> bool {
        (self.len_and_kind & SEGMENT_SCRATCH_BIT) != 0
    }
}

impl IovecWriter {
    /// Create an empty writer.
    #[inline]
    pub fn new() -> Self {
        Self {
            segments: IovecSegments::Inline {
                buf: [Segment {
                    data: 0,
                    len_and_kind: 0,
                }; INLINE_CAP],
                len: 0,
            },
            scratch: ScratchBuf::Inline {
                buf: [0u8; SCRATCH_CAP],
                len: 0,
            },
        }
    }

    /// Number of iovec segments accumulated.
    #[inline]
    pub fn segment_count(&self) -> usize {
        match &self.segments {
            IovecSegments::Inline { len, .. } => *len as usize,
            IovecSegments::Heap(v) => v.len(),
        }
    }

    /// Total bytes across all segments.
    #[inline]
    pub fn total_len(&self) -> usize {
        let mut total = 0usize;
        self.for_each_segment(|seg| {
            total = total
                .checked_add(seg.len())
                .expect("iovec total length overflow");
        });
        total
    }

    /// Returns `true` if no segments have been pushed.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.segment_count() == 0
    }

    /// Push a static-lifetime slice (pre-computed response, LUT entry).
    /// Zero-copy: the iovec points directly into `.rodata`.
    #[inline]
    pub fn push_static(&mut self, buf: &'static [u8]) {
        self.push_segment(Segment::borrowed(buf));
    }

    /// Push a `Bytes` reference slice. The caller must ensure the `Bytes`
    /// outlives the writer (typically true — the read buffer backing is pinned).
    #[inline]
    pub fn push_bytes(&mut self, buf: &[u8]) {
        if buf.is_empty() {
            return;
        }
        self.push_segment(Segment::borrowed(buf));
    }

    /// Write dynamically formatted bytes into the scratch buffer and push a
    /// segment pointing to the written data. Used for integer formatting, length
    /// prefixes, etc.
    #[inline]
    pub fn push_scratch(&mut self, data: &[u8]) {
        let start = self.scratch_len();
        self.scratch_extend(data);
        // The segment points into our scratch buffer. The pointer is stable
        // because we re-derive it from scratch_ptr() + start at as_iovecs time.
        // Store the start offset and length; resolved views validate bounds.
        self.push_segment(Segment::scratch(start, data.len()));
    }

    /// Flatten all segments into a contiguous byte vector.
    /// Used for testing and when writev is not available.
    pub fn flatten(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.total_len());
        self.for_each_resolved(|ptr, len| {
            // SAFETY: ptr is valid and len bytes are readable.
            let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
            out.extend_from_slice(slice);
        });
        out
    }

    /// Build `IoSlice` array suitable for `writev`. Returns a `Vec<IoSlice>`
    /// with resolved pointers (scratch offsets are converted to real pointers).
    pub fn as_io_slices(&self) -> Vec<IoSlice<'_>> {
        let count = self.segment_count();
        let mut slices = Vec::with_capacity(count);
        self.for_each_resolved(|ptr, len| {
            // SAFETY: ptr is valid for len bytes. The borrow on &self ensures
            // the scratch buffer (and static data) outlives the IoSlice.
            let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
            slices.push(IoSlice::new(slice));
        });
        slices
    }

    /// Build raw `libc::iovec` array for direct syscall use.
    pub fn as_raw_iovecs(&self) -> Vec<libc::iovec> {
        let count = self.segment_count();
        let mut iovs = Vec::with_capacity(count);
        self.write_raw_iovecs(&mut iovs);
        iovs
    }

    /// Write raw `libc::iovec` entries into an existing vector.
    ///
    /// This lets reactor-owned write state reuse the raw-iovec allocation
    /// across steady-state responses instead of allocating a fresh vector for
    /// each `writev` submission.
    pub fn write_raw_iovecs(&self, out: &mut Vec<libc::iovec>) {
        out.clear();
        out.reserve(self.segment_count());
        self.for_each_resolved(|ptr, len| {
            out.push(libc::iovec {
                iov_base: ptr as *mut libc::c_void,
                iov_len: len,
            });
        });
    }

    /// Reset the writer for reuse, clearing all segments and scratch data.
    #[inline]
    pub fn clear(&mut self) {
        match &mut self.segments {
            IovecSegments::Inline { len, .. } => *len = 0,
            IovecSegments::Heap(v) => v.clear(),
        }
        match &mut self.scratch {
            ScratchBuf::Inline { len, .. } => *len = 0,
            ScratchBuf::Heap(v) => v.clear(),
        }
    }

    // ── Internal helpers ────────────────────────────────────────────

    #[inline]
    fn push_segment(&mut self, seg: Segment) {
        match &mut self.segments {
            IovecSegments::Inline { buf, len } => {
                let idx = *len as usize;
                if idx < INLINE_CAP {
                    buf[idx] = seg;
                    *len += 1;
                } else {
                    // Spill to heap.
                    let mut v = Vec::with_capacity(INLINE_CAP * 2);
                    v.extend_from_slice(&buf[..]);
                    v.push(seg);
                    self.segments = IovecSegments::Heap(v);
                }
            }
            IovecSegments::Heap(v) => v.push(seg),
        }
    }

    #[inline]
    fn scratch_len(&self) -> usize {
        match &self.scratch {
            ScratchBuf::Inline { len, .. } => *len as usize,
            ScratchBuf::Heap(v) => v.len(),
        }
    }

    #[inline]
    fn scratch_extend(&mut self, data: &[u8]) {
        match &mut self.scratch {
            ScratchBuf::Inline { buf, len } => {
                let start = *len as usize;
                let end = start
                    .checked_add(data.len())
                    .expect("iovec scratch length overflow");
                if end <= SCRATCH_CAP {
                    buf[start..end].copy_from_slice(data);
                    *len = end as u16;
                } else {
                    // Spill to heap.
                    let mut v = Vec::with_capacity(end.max(SCRATCH_CAP * 2));
                    v.extend_from_slice(&buf[..start]);
                    v.extend_from_slice(data);
                    self.scratch = ScratchBuf::Heap(v);
                }
            }
            ScratchBuf::Heap(v) => v.extend_from_slice(data),
        }
    }

    #[inline]
    fn scratch_ptr(&self) -> *const u8 {
        match &self.scratch {
            ScratchBuf::Inline { buf, .. } => buf.as_ptr(),
            ScratchBuf::Heap(v) => v.as_ptr(),
        }
    }

    /// Iterate over segments, resolving scratch pointers to real addresses.
    #[inline]
    fn for_each_resolved(&self, mut f: impl FnMut(*const u8, usize)) {
        let scratch_base = self.scratch_ptr();
        self.for_each_segment(|seg| {
            let len = seg.len();
            let ptr = if seg.is_scratch() {
                let offset = seg.data;
                let end = offset
                    .checked_add(len)
                    .expect("iovec scratch segment length overflow");
                assert!(
                    end <= self.scratch_len(),
                    "iovec scratch segment exceeds scratch buffer"
                );
                // SAFETY: offset..end is within the scratch buffer.
                unsafe { scratch_base.add(offset) }
            } else {
                seg.data as *const u8
            };
            f(ptr, len);
        });
    }

    #[inline]
    fn for_each_segment(&self, mut f: impl FnMut(&Segment)) {
        match &self.segments {
            IovecSegments::Inline { buf, len } => {
                for seg in buf.iter().take(*len as usize) {
                    f(seg);
                }
            }
            IovecSegments::Heap(v) => {
                for seg in v {
                    f(seg);
                }
            }
        }
    }
}

impl Default for IovecWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// High bit used in `Segment::len_and_kind` to mark scratch-backed segments.
const SEGMENT_SCRATCH_BIT: usize = 1usize << (usize::BITS - 1);
const SEGMENT_LEN_MASK: usize = SEGMENT_SCRATCH_BIT - 1;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_writer() {
        let w = IovecWriter::new();
        assert!(w.is_empty());
        assert_eq!(w.segment_count(), 0);
        assert_eq!(w.total_len(), 0);
        assert!(w.flatten().is_empty());
    }

    #[test]
    fn push_static_roundtrip() {
        let mut w = IovecWriter::new();
        w.push_static(b"+OK\r\n");
        assert_eq!(w.segment_count(), 1);
        assert_eq!(w.total_len(), 5);
        assert_eq!(w.flatten(), b"+OK\r\n");
    }

    #[test]
    fn push_bytes_roundtrip() {
        let mut w = IovecWriter::new();
        let data = b"hello world";
        w.push_bytes(data);
        assert_eq!(w.flatten(), b"hello world");
    }

    #[test]
    fn push_scratch_roundtrip() {
        let mut w = IovecWriter::new();
        w.push_scratch(b":42\r\n");
        assert_eq!(w.flatten(), b":42\r\n");
    }

    #[test]
    fn mixed_segments() {
        let mut w = IovecWriter::new();
        w.push_static(b"*3\r\n");
        w.push_scratch(b"$3\r\n");
        w.push_static(b"SET\r\n");
        w.push_scratch(b"$5\r\n");
        let key = b"mykey";
        w.push_bytes(key);
        w.push_static(b"\r\n");
        assert_eq!(w.flatten(), b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n");
    }

    #[test]
    fn io_slices_match_flatten() {
        let mut w = IovecWriter::new();
        w.push_static(b"+PONG\r\n");
        w.push_scratch(b":99\r\n");
        w.push_bytes(b"data");

        let flat = w.flatten();
        let slices = w.as_io_slices();
        let mut reconstructed = Vec::new();
        for s in &slices {
            reconstructed.extend_from_slice(s);
        }
        assert_eq!(reconstructed, flat);
    }

    #[test]
    fn raw_iovecs_match_flatten() {
        let mut w = IovecWriter::new();
        w.push_static(b"+OK\r\n");
        w.push_scratch(b":1\r\n");

        let flat = w.flatten();
        let iovs = w.as_raw_iovecs();
        let mut reconstructed = Vec::new();
        for iov in &iovs {
            let slice =
                unsafe { std::slice::from_raw_parts(iov.iov_base as *const u8, iov.iov_len) };
            reconstructed.extend_from_slice(slice);
        }
        assert_eq!(reconstructed, flat);
    }

    #[test]
    fn segment_size_remains_two_words() {
        assert_eq!(
            std::mem::size_of::<Segment>(),
            std::mem::size_of::<usize>() * 2
        );
    }

    #[test]
    fn clear_resets() {
        let mut w = IovecWriter::new();
        w.push_static(b"+OK\r\n");
        w.push_scratch(b":1\r\n");
        assert!(!w.is_empty());

        w.clear();
        assert!(w.is_empty());
        assert_eq!(w.total_len(), 0);
        assert_eq!(w.segment_count(), 0);
    }

    #[test]
    fn spill_to_heap_segments() {
        let mut w = IovecWriter::new();
        // Push more than INLINE_CAP segments.
        for i in 0..20 {
            w.push_static(if i % 2 == 0 { b"+OK\r\n" } else { b":0\r\n" });
        }
        assert_eq!(w.segment_count(), 20);
        // Verify total length: 10 * 5 (OK) + 10 * 4 (:0) = 90
        assert_eq!(w.total_len(), 90);
    }

    #[test]
    fn spill_to_heap_scratch() {
        let mut w = IovecWriter::new();
        // Fill scratch past SCRATCH_CAP (512).
        let chunk = [b'X'; 100];
        for _ in 0..6 {
            w.push_scratch(&chunk);
        }
        // 6 × 100 = 600 bytes total, should spill.
        assert_eq!(w.total_len(), 600);
        let flat = w.flatten();
        assert!(flat.iter().all(|&b| b == b'X'));
    }

    #[test]
    fn scratch_offsets_above_u16_max_resolve() {
        let mut w = IovecWriter::new();
        let chunk = [b'X'; 100];
        for _ in 0..700 {
            w.push_scratch(&chunk);
        }

        assert_eq!(w.total_len(), 70_000);
        let flat = w.flatten();
        assert_eq!(flat.len(), 70_000);
        assert!(flat.iter().all(|&b| b == b'X'));

        let iovs = w.as_raw_iovecs();
        let mut reconstructed = Vec::with_capacity(70_000);
        for iov in &iovs {
            // SAFETY: raw iovecs were resolved from the writer's own valid segments.
            let slice =
                unsafe { std::slice::from_raw_parts(iov.iov_base as *const u8, iov.iov_len) };
            reconstructed.extend_from_slice(slice);
        }
        assert_eq!(reconstructed, flat);
    }

    #[test]
    fn multi_segment_pipeline() {
        let mut w = IovecWriter::new();
        // Simulate 100 OK responses.
        for _ in 0..100 {
            w.push_static(b"+OK\r\n");
        }
        assert_eq!(w.total_len(), 500);
        assert_eq!(w.flatten().len(), 500);
    }
}
