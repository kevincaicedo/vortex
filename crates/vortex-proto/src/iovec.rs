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
    /// (ptr, len) pairs — offsets into either static slices or `scratch`.
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
    /// Pointer to the start of the segment data.
    ptr: *const u8,
    /// Length of the segment.
    len: u32,
}

// SAFETY: IovecWriter is used single-threaded on the reactor thread.
// The raw pointers in Segment point to either:
// 1. Static data (&'static [u8]) which is valid for 'static.
// 2. Data within our owned scratch buffer.
// Both are valid for the lifetime of the IovecWriter.
unsafe impl Send for IovecWriter {}

impl IovecWriter {
    /// Create an empty writer.
    #[inline]
    pub fn new() -> Self {
        Self {
            segments: IovecSegments::Inline {
                buf: [Segment {
                    ptr: std::ptr::null(),
                    len: 0,
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
        self.for_each_segment(|seg| total += seg.len as usize);
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
        self.push_segment(Segment {
            ptr: buf.as_ptr(),
            len: buf.len() as u32,
        });
    }

    /// Push a `Bytes` reference slice. The caller must ensure the `Bytes`
    /// outlives the writer (typically true — the read buffer backing is pinned).
    #[inline]
    pub fn push_bytes(&mut self, buf: &[u8]) {
        if buf.is_empty() {
            return;
        }
        self.push_segment(Segment {
            ptr: buf.as_ptr(),
            len: buf.len() as u32,
        });
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
        // Store the start offset in ptr and the length — we'll fix up ptrs later.
        self.push_segment(Segment {
            // Use a sentinel: store offset as a tagged pointer (top bit set).
            ptr: SCRATCH_TAG.wrapping_add(start) as *const u8,
            len: data.len() as u32,
        });
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
        self.for_each_resolved(|ptr, len| {
            iovs.push(libc::iovec {
                iov_base: ptr as *mut libc::c_void,
                iov_len: len,
            });
        });
        iovs
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
                let end = start + data.len();
                if end <= SCRATCH_CAP {
                    buf[start..end].copy_from_slice(data);
                    *len = end as u16;
                } else {
                    // Spill to heap.
                    let mut v = Vec::with_capacity(SCRATCH_CAP * 2);
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
            let ptr = if is_scratch_ptr(seg.ptr) {
                let offset = (seg.ptr as usize).wrapping_sub(SCRATCH_TAG);
                // SAFETY: offset < scratch_len(), scratch_base is valid.
                unsafe { scratch_base.add(offset) }
            } else {
                seg.ptr
            };
            f(ptr, seg.len as usize);
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

/// Tag value used to mark scratch-buffer pointers. We use a high address that
/// is guaranteed not to be a real pointer on any supported platform.
const SCRATCH_TAG: usize = 0xDEAD_0000_0000_0000_usize;

/// Check if a pointer is a tagged scratch offset.
#[inline]
fn is_scratch_ptr(ptr: *const u8) -> bool {
    (ptr as usize) >= SCRATCH_TAG && (ptr as usize) < SCRATCH_TAG + (u16::MAX as usize)
}

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
