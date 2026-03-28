#[cfg(all(feature = "simd", target_arch = "x86_64"))]
use core::arch::x86_64::{
    __m128i, __m256i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8,
    _mm_set1_epi8, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8,
    _mm256_set1_epi8,
};

#[cfg(all(feature = "simd", any(target_arch = "aarch64", test)))]
use std::simd::{cmp::SimdPartialEq, Simd};
#[cfg(all(feature = "simd", target_arch = "x86_64"))]
use std::sync::{
    Once,
    atomic::{AtomicU8, Ordering},
};

const INLINE_CAPACITY: usize = 64;
const SMALL_BUFFER_MAX: usize = u16::MAX as usize;
#[cfg(feature = "simd")]
const NO_PENDING_CR: usize = usize::MAX;

#[cfg(all(feature = "simd", target_arch = "x86_64"))]
const SIMD_LEVEL_UNKNOWN: u8 = 0;
#[cfg(all(feature = "simd", target_arch = "x86_64"))]
const SIMD_LEVEL_AVX2: u8 = 1;
#[cfg(all(feature = "simd", target_arch = "x86_64"))]
const SIMD_LEVEL_SSE2: u8 = 2;

/// CRLF positions discovered in a buffer.
///
/// Small buffers stay entirely on the stack for the common case. Large buffers
/// switch to a dedicated `u32` backing store so offsets remain compact but safe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrlfPositions {
    storage: CrlfStorage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CrlfStorage {
    Small(SmallCrlfPositions),
    Large(LargeCrlfPositions),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SmallCrlfPositions {
    inline: [u16; INLINE_CAPACITY],
    len: u16,
    overflow: Option<Vec<u16>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LargeCrlfPositions {
    positions: Vec<u32>,
}

impl Default for CrlfPositions {
    fn default() -> Self {
        Self::with_buffer_len(0)
    }
}

impl CrlfPositions {
    #[must_use]
    pub fn with_buffer_len(buffer_len: usize) -> Self {
        let storage = if buffer_len > SMALL_BUFFER_MAX {
            CrlfStorage::Large(LargeCrlfPositions::with_buffer_len(buffer_len))
        } else {
            CrlfStorage::Small(SmallCrlfPositions::default())
        };
        Self { storage }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match &self.storage {
            CrlfStorage::Small(small) => usize::from(small.len),
            CrlfStorage::Large(large) => large.positions.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<usize> {
        (index < self.len()).then(|| self.position_at(index))
    }

    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        (0..self.len()).map(move |index| self.position_at(index))
    }

    #[must_use]
    pub fn next_after(&self, offset: usize) -> Option<usize> {
        let mut low = 0;
        let mut high = self.len();

        while low < high {
            let mid = low + ((high - low) >> 1);
            if self.position_at(mid) < offset {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        self.get(low)
    }

    #[inline(always)]
    fn position_at(&self, index: usize) -> usize {
        match &self.storage {
            CrlfStorage::Small(small) => {
                let inline_len = small.inline_len();
                if index < inline_len {
                    usize::from(small.inline[index])
                } else {
                    usize::from(
                        small
                            .overflow
                            .as_ref()
                            .and_then(|overflow| overflow.get(index - inline_len))
                            .copied()
                            .unwrap_or_else(|| unreachable!("index already bounds checked")),
                    )
                }
            }
            CrlfStorage::Large(large) => usize::try_from(
                large
                    .positions
                    .get(index)
                    .copied()
                    .unwrap_or_else(|| unreachable!("index already bounds checked")),
            )
            .unwrap_or_else(|_| unreachable!("u32 always fits in usize on supported targets")),
        }
    }

    #[inline(always)]
    fn push(&mut self, position: usize) {
        match &mut self.storage {
            CrlfStorage::Small(small) => small.push(
                u16::try_from(position)
                    .unwrap_or_else(|_| unreachable!("small buffer offsets must fit in u16")),
            ),
            CrlfStorage::Large(large) => large.push(position),
        }
    }
}

impl Default for SmallCrlfPositions {
    fn default() -> Self {
        Self {
            inline: [0; INLINE_CAPACITY],
            len: 0,
            overflow: None,
        }
    }
}

impl SmallCrlfPositions {
    #[inline(always)]
    fn inline_len(&self) -> usize {
        usize::min(usize::from(self.len), INLINE_CAPACITY)
    }

    #[inline(always)]
    fn push(&mut self, position: u16) {
        let len = usize::from(self.len);
        if len < INLINE_CAPACITY {
            self.inline[len] = position;
        } else {
            self.overflow
                .get_or_insert_with(|| Vec::with_capacity(INLINE_CAPACITY))
                .push(position);
        }

        self.len += 1;
    }
}

impl LargeCrlfPositions {
    #[must_use]
    fn with_buffer_len(buffer_len: usize) -> Self {
        Self {
            positions: Vec::with_capacity(buffer_len.saturating_div(2)),
        }
    }

    #[inline(always)]
    fn push(&mut self, position: usize) {
        self.positions.push(
            u32::try_from(position)
                .unwrap_or_else(|_| panic!("CRLF offset exceeds u32 range: {position}")),
        );
    }
}

/// Scans a buffer for all `\r\n` delimiters using the fastest implementation
/// available on the current platform.
#[must_use]
#[inline]
pub fn scan_crlf(buf: &[u8]) -> CrlfPositions {
    if buf.len() < 2 {
        return CrlfPositions::with_buffer_len(buf.len());
    }

    #[cfg(all(feature = "simd", target_arch = "x86_64"))]
    {
        x86_scan_crlf(buf)
    }

    #[cfg(all(feature = "simd", target_arch = "aarch64"))]
    {
        portable_simd_scan_crlf(buf)
    }

    #[cfg(any(
        not(feature = "simd"),
        all(feature = "simd", not(any(target_arch = "x86_64", target_arch = "aarch64")))
    ))]
    {
        scalar_scan_crlf(buf)
    }
}

/// Scalar CRLF scanner used as the correctness oracle for SIMD verification.
#[doc(hidden)]
#[must_use]
pub fn scalar_scan_crlf(buf: &[u8]) -> CrlfPositions {
    let mut positions = CrlfPositions::with_buffer_len(buf.len());
    if buf.len() < 2 {
        return positions;
    }

    for offset in 0..(buf.len() - 1) {
        if buf[offset] == b'\r' && buf[offset + 1] == b'\n' {
            positions.push(offset);
        }
    }

    positions
}

#[cfg(all(feature = "simd", target_arch = "x86_64"))]
#[inline]
fn x86_scan_crlf(buf: &[u8]) -> CrlfPositions {
    static SIMD_LEVEL: AtomicU8 = AtomicU8::new(SIMD_LEVEL_UNKNOWN);
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        let level = if std::arch::is_x86_feature_detected!("avx2") {
            SIMD_LEVEL_AVX2
        } else {
            SIMD_LEVEL_SSE2
        };
        SIMD_LEVEL.store(level, Ordering::Relaxed);
    });

    match SIMD_LEVEL.load(Ordering::Relaxed) {
        SIMD_LEVEL_AVX2 => {
            // SAFETY: Runtime dispatch guarantees AVX2 support before calling.
            unsafe { avx2_scan_crlf(buf) }
        }
        SIMD_LEVEL_SSE2 | SIMD_LEVEL_UNKNOWN => sse2_scan_crlf(buf),
        _ => sse2_scan_crlf(buf),
    }
}

#[cfg(all(feature = "simd", target_arch = "x86_64"))]
#[must_use]
fn sse2_scan_crlf(buf: &[u8]) -> CrlfPositions {
    const CHUNK: usize = 16;

    let mut positions = CrlfPositions::with_buffer_len(buf.len());
    let mut offset = 0;
    let mut pending_cr = NO_PENDING_CR;
    let ptr = buf.as_ptr();
    let cr = unsafe { _mm_set1_epi8(b'\r' as i8) };

    while offset + CHUNK <= buf.len() {
        let mask = unsafe {
            // SAFETY: `offset + CHUNK <= buf.len()`, so the load stays in-bounds.
            // `_mm_loadu_si128` accepts unaligned addresses.
            let chunk = _mm_loadu_si128(ptr.add(offset).cast::<__m128i>());
            _mm_movemask_epi8(_mm_cmpeq_epi8(chunk, cr)) as u32
        };

        process_cr_mask(buf, offset, CHUNK, mask, &mut positions, &mut pending_cr);
        offset += CHUNK;
    }

    scan_tail(buf, offset, &mut positions, &mut pending_cr);
    positions
}

#[cfg(all(feature = "simd", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
#[must_use]
unsafe fn avx2_scan_crlf(buf: &[u8]) -> CrlfPositions {
    const CHUNK: usize = 32;

    let mut positions = CrlfPositions::with_buffer_len(buf.len());
    let mut offset = 0;
    let mut pending_cr = NO_PENDING_CR;
    let ptr = buf.as_ptr();
    let cr = _mm256_set1_epi8(b'\r' as i8);

    while offset + CHUNK <= buf.len() {
        let mask = unsafe {
            // SAFETY: `offset + CHUNK <= buf.len()`, so the unaligned 32-byte load
            // stays within the backing slice.
            let chunk = _mm256_loadu_si256(ptr.add(offset).cast::<__m256i>());
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, cr)) as u32
        };

        process_cr_mask(buf, offset, CHUNK, mask, &mut positions, &mut pending_cr);
        offset += CHUNK;
    }

    scan_tail(buf, offset, &mut positions, &mut pending_cr);
    positions
}

#[cfg(all(feature = "simd", any(target_arch = "aarch64", test)))]
#[must_use]
fn portable_simd_scan_crlf(buf: &[u8]) -> CrlfPositions {
    const CHUNK: usize = 16;

    let mut positions = CrlfPositions::with_buffer_len(buf.len());
    let mut offset = 0;
    let mut pending_cr = NO_PENDING_CR;
    let cr = Simd::<u8, CHUNK>::splat(b'\r');

    while offset + CHUNK <= buf.len() {
        let chunk = Simd::<u8, CHUNK>::from_slice(&buf[offset..offset + CHUNK]);
        let mask = chunk.simd_eq(cr).to_bitmask() as u32;
        process_cr_mask(buf, offset, CHUNK, mask, &mut positions, &mut pending_cr);
        offset += CHUNK;
    }

    scan_tail(buf, offset, &mut positions, &mut pending_cr);
    positions
}

#[cfg(feature = "simd")]
#[inline(always)]
fn process_cr_mask(
    buf: &[u8],
    base: usize,
    width: usize,
    mut mask: u32,
    positions: &mut CrlfPositions,
    pending_cr: &mut usize,
) {
    resolve_pending_cr(buf, base, positions, pending_cr);

    while mask != 0 {
        let lane = mask.trailing_zeros() as usize;
        let offset = base + lane;

        if lane + 1 < width {
            if buf[offset + 1] == b'\n' {
                positions.push(offset);
            }
        } else {
            *pending_cr = offset;
        }

        mask &= mask - 1;
    }
}

#[cfg(feature = "simd")]
#[inline(always)]
fn scan_tail(
    buf: &[u8],
    start: usize,
    positions: &mut CrlfPositions,
    pending_cr: &mut usize,
) {
    resolve_pending_cr(buf, start, positions, pending_cr);

    let last = buf.len().saturating_sub(1);
    if start >= last {
        return;
    }

    for offset in start..last {
        if buf[offset] == b'\r' && buf[offset + 1] == b'\n' {
            positions.push(offset);
        }
    }
}

#[cfg(feature = "simd")]
#[inline(always)]
fn resolve_pending_cr(
    buf: &[u8],
    next_index: usize,
    positions: &mut CrlfPositions,
    pending_cr: &mut usize,
) {
    if *pending_cr == NO_PENDING_CR {
        return;
    }

    if next_index < buf.len() && buf[next_index] == b'\n' {
        positions.push(*pending_cr);
    }

    *pending_cr = NO_PENDING_CR;
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn collect(positions: &CrlfPositions) -> Vec<usize> {
        positions.iter().collect()
    }

    fn assert_all_paths(input: &[u8], expected: &[usize]) {
        let expected = expected.to_vec();

        assert_eq!(collect(&scalar_scan_crlf(input)), expected);
        assert_eq!(collect(&scan_crlf(input)), expected);

        #[cfg(feature = "simd")]
        assert_eq!(collect(&portable_simd_scan_crlf(input)), expected);

        #[cfg(all(feature = "simd", target_arch = "x86_64"))]
        {
            assert_eq!(collect(&sse2_scan_crlf(input)), expected);

            if std::arch::is_x86_feature_detected!("avx2") {
                // SAFETY: Runtime feature detection guarantees AVX2 support.
                let avx2 = unsafe { avx2_scan_crlf(input) };
                assert_eq!(collect(&avx2), expected);
            }
        }
    }

    #[test]
    fn empty_buffer_returns_no_positions() {
        assert_all_paths(b"", &[]);
    }

    #[test]
    fn single_byte_buffer_returns_no_positions() {
        assert_all_paths(b"\r", &[]);
    }

    #[test]
    fn finds_single_crlf_at_fixed_offsets() {
        for offset in [0_usize, 1, 15, 16, 31, 32] {
            let mut buf = vec![b'x'; offset + 2];
            buf[offset] = b'\r';
            buf[offset + 1] = b'\n';
            assert_all_paths(&buf, &[offset]);
        }
    }

    #[test]
    fn finds_multiple_crlfs_in_one_chunk() {
        let input = b"aa\r\nbb\r\ncc\r\ndd";
        assert_all_paths(input, &[2, 6, 10]);
    }

    #[test]
    fn handles_sse_chunk_boundary_split() {
        let mut buf = vec![b'x'; 17];
        buf[15] = b'\r';
        buf[16] = b'\n';
        assert_all_paths(&buf, &[15]);
    }

    #[test]
    fn handles_avx_chunk_boundary_split() {
        let mut buf = vec![b'x'; 33];
        buf[31] = b'\r';
        buf[32] = b'\n';
        assert_all_paths(&buf, &[31]);
    }

    #[test]
    fn rejects_lone_carriage_return() {
        assert_all_paths(b"abc\rxyz", &[]);
    }

    #[test]
    fn rejects_lone_newline() {
        assert_all_paths(b"abc\nxyz", &[]);
    }

    #[test]
    fn exact_chunk_without_tail_is_scanned() {
        let mut buf = [b'x'; 32];
        buf[30] = b'\r';
        buf[31] = b'\n';
        assert_all_paths(&buf, &[30]);
    }

    #[test]
    fn all_carriage_returns_do_not_produce_false_positives() {
        let buf = vec![b'\r'; 256];
        assert_all_paths(&buf, &[]);
    }

    #[test]
    fn binary_data_without_crlf_returns_empty() {
        let buf: Vec<u8> = (0..=255).collect();
        let expected: Vec<usize> = scalar_scan_crlf(&buf).iter().collect();
        assert_all_paths(&buf, &expected);
    }

    #[test]
    fn heap_overflow_path_is_used_after_inline_capacity() {
        let buf = [b'\r', b'\n'].repeat(80);
        let positions = scan_crlf(&buf);
        let expected: Vec<usize> = (0..80).map(|index| index * 2).collect();

        assert_eq!(collect(&positions), expected);
        match &positions.storage {
            CrlfStorage::Small(small) => {
                assert_eq!(usize::from(small.len), 80);
                assert!(small.overflow.is_some());
            }
            CrlfStorage::Large(_) => panic!("80-byte scan should stay in small storage"),
        }
    }

    #[test]
    fn large_buffers_switch_to_u32_storage() {
        let mut buf = vec![b'x'; SMALL_BUFFER_MAX + 2];
        buf[SMALL_BUFFER_MAX] = b'\r';
        buf[SMALL_BUFFER_MAX + 1] = b'\n';

        let positions = scan_crlf(&buf);
        assert_eq!(collect(&positions), vec![SMALL_BUFFER_MAX]);

        match &positions.storage {
            CrlfStorage::Large(_) => {}
            CrlfStorage::Small(_) => panic!("large buffers must use u32-backed storage"),
        }
    }

    #[test]
    fn next_after_uses_sorted_positions() {
        let positions = scan_crlf(b"a\r\nb\r\nc\r\n");

        assert_eq!(positions.next_after(0), Some(1));
        assert_eq!(positions.next_after(2), Some(4));
        assert_eq!(positions.next_after(7), Some(7));
        assert_eq!(positions.next_after(8), None);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1_000))]

        #[test]
        fn simd_matches_scalar_for_random_inputs(input in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let scalar = collect(&scalar_scan_crlf(&input));
            let dispatch = collect(&scan_crlf(&input));
            prop_assert_eq!(&dispatch, &scalar);

            #[cfg(feature = "simd")]
            prop_assert_eq!(collect(&portable_simd_scan_crlf(&input)), scalar.clone());

            #[cfg(all(feature = "simd", target_arch = "x86_64"))]
            {
                prop_assert_eq!(collect(&sse2_scan_crlf(&input)), scalar.clone());

                if std::arch::is_x86_feature_detected!("avx2") {
                    // SAFETY: Runtime feature detection guarantees AVX2 support.
                    let avx2 = unsafe { avx2_scan_crlf(&input) };
                    prop_assert_eq!(collect(&avx2), scalar.clone());
                }
            }
        }
    }
}