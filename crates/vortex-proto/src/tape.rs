//! Flat tape RESP parser — simdjson-style zero-allocation frame output.
//!
//! Instead of building a tree of [`RespFrame`](crate::RespFrame) values with
//! per-aggregate `Vec` heap allocations, the tape parser writes a flat
//! `Vec<TapeEntry>` where each entry is a compact 12-byte record referencing
//! byte ranges in a shared `Bytes` backing buffer.
//!
//! # Performance
//!
//! For a pipeline of 1 000 SET commands the tape uses:
//! - **1 allocation**: the `Vec<TapeEntry>` (pre-sized, rarely grows)
//! - **48 KB**: 4 000 entries × 12 bytes (fits in L1)
//!
//! vs the tree parser's 1 001 allocations and ~200–350 KB of overhead.

use bytes::Bytes;

use crate::{
    parser::{parse_short_int, ParseError, MAX_ARRAY_ELEMENTS, MAX_BULK_STRING_LEN, MAX_NESTING_DEPTH},
    swar::swar_parse_int,
};

type TapeResult<T> = Result<T, ParseError>;

// ── Tag constants ───────────────────────────────────────────────────────────
// Always defined (even without `resp3`) so `entry_span_end` and `FrameRef`
// work unconditionally.

pub const TAG_SIMPLE_STRING: u8 = 1;
pub const TAG_ERROR: u8 = 2;
pub const TAG_INTEGER: u8 = 3;
pub const TAG_BULK_STRING: u8 = 4;
pub const TAG_NULL_BULK_STRING: u8 = 5;
pub const TAG_ARRAY: u8 = 6;
pub const TAG_NULL_ARRAY: u8 = 7;
pub const TAG_NULL: u8 = 8;
pub const TAG_BOOLEAN: u8 = 9;
pub const TAG_DOUBLE: u8 = 10;
pub const TAG_BIG_NUMBER: u8 = 11;
pub const TAG_BULK_ERROR: u8 = 12;
pub const TAG_VERBATIM_STRING: u8 = 13;
pub const TAG_MAP: u8 = 14;
pub const TAG_SET: u8 = 15;
pub const TAG_PUSH: u8 = 16;
pub const TAG_ATTRIBUTE: u8 = 17;

// ── TapeEntry ───────────────────────────────────────────────────────────────

/// A single 12-byte entry in the flat RESP parse tape.
///
/// Interpretation of `a` and `b` depends on `tag`:
///
/// | Tag family | `a` | `b` |
/// |---|---|---|
/// | String (Simple, Error, Bulk, BigNumber, BulkError, Verbatim) | start offset | end offset |
/// | Integer | low 32 bits of i64 | high 32 bits |
/// | Boolean | 0 or 1 | 0 |
/// | Double | low 32 bits of f64 | high 32 bits |
/// | Array, Set, Push | element count | tape index past last child |
/// | Map, Attribute | pair count | tape index past last child |
/// | Null types | 0 | 0 |
///
/// For `VerbatimString`, the 3-byte encoding prefix is stored in `extra`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct TapeEntry {
    pub tag: u8,
    /// VerbatimString encoding bytes; zero for all other types.
    pub extra: [u8; 3],
    pub a: u32,
    pub b: u32,
}

const _: () = assert!(std::mem::size_of::<TapeEntry>() == 12);

impl TapeEntry {
    #[inline(always)]
    const fn new(tag: u8, a: u32, b: u32) -> Self {
        Self {
            tag,
            extra: [0; 3],
            a,
            b,
        }
    }

    #[inline(always)]
    const fn null(tag: u8) -> Self {
        Self::new(tag, 0, 0)
    }

    #[inline(always)]
    const fn from_i64(value: i64) -> Self {
        let bits = value as u64;
        Self::new(TAG_INTEGER, bits as u32, (bits >> 32) as u32)
    }

    #[inline(always)]
    fn from_f64(value: f64) -> Self {
        let bits = f64::to_bits(value);
        Self::new(TAG_DOUBLE, bits as u32, (bits >> 32) as u32)
    }

    /// Reconstruct the `i64` stored in fields `a` and `b`.
    #[inline(always)]
    pub fn as_i64(&self) -> i64 {
        ((self.a as u64) | ((self.b as u64) << 32)) as i64
    }

    /// Reconstruct the `f64` stored in fields `a` and `b`.
    #[inline(always)]
    pub fn as_f64(&self) -> f64 {
        f64::from_bits((self.a as u64) | ((self.b as u64) << 32))
    }

    /// Boolean value from field `a`.
    #[inline(always)]
    pub fn as_bool(&self) -> bool {
        self.a != 0
    }
}

// ── RespTape ────────────────────────────────────────────────────────────────

/// Flat tape output from the RESP parser.
///
/// Contains a contiguous `Vec<TapeEntry>` and a shared `Bytes` backing buffer.
/// All string payloads reference byte ranges into the backing buffer.
pub struct RespTape {
    entries: Vec<TapeEntry>,
    backing: Bytes,
    frame_count: usize,
    consumed: usize,
}

impl RespTape {
    /// Parse a pipeline from a byte slice (copies input into `Bytes`).
    pub fn parse_pipeline(buf: &[u8]) -> Result<Self, ParseError> {
        if buf.is_empty() {
            return Err(ParseError::NeedMoreData);
        }
        Self::parse_pipeline_bytes(Bytes::copy_from_slice(buf))
    }

    /// Parse a pipeline from caller-owned `Bytes` — zero copy.
    pub fn parse_pipeline_bytes(backing: Bytes) -> Result<Self, ParseError> {
        if backing.is_empty() {
            return Err(ParseError::NeedMoreData);
        }
        let bytes = backing.as_ref();
        let mut offset: usize = 0;
        // Pre-size: typical SET command ≈ 37 bytes → 4 entries → ~0.11 entries/byte.
        let mut entries = Vec::with_capacity(bytes.len() / 8);
        let mut frame_count: usize = 0;

        while offset < bytes.len() {
            let snap_entries = entries.len();
            let snap_offset = offset;
            match tape_parse_frame(bytes, &mut offset, &mut entries, 0) {
                Ok(()) => frame_count += 1,
                Err(error) if frame_count == 0 => return Err(error),
                Err(_) => {
                    entries.truncate(snap_entries);
                    offset = snap_offset;
                    break;
                }
            }
        }

        if frame_count == 0 {
            return Err(ParseError::NeedMoreData);
        }

        Ok(Self {
            entries,
            backing,
            frame_count,
            consumed: offset,
        })
    }

    /// Number of top-level frames parsed.
    #[inline]
    pub fn frame_count(&self) -> usize {
        self.frame_count
    }

    /// Total bytes consumed from the input.
    #[inline]
    pub fn consumed(&self) -> usize {
        self.consumed
    }

    /// Raw tape entries.
    #[inline]
    pub fn entries(&self) -> &[TapeEntry] {
        &self.entries
    }

    /// Shared backing buffer.
    #[inline]
    pub fn backing(&self) -> &Bytes {
        &self.backing
    }

    /// Iterate over top-level frames.
    #[inline]
    pub fn iter(&self) -> TapeIter<'_> {
        TapeIter {
            entries: &self.entries,
            backing: self.backing.as_ref(),
            index: 0,
        }
    }
}

// ── Span helper ─────────────────────────────────────────────────────────────

/// Returns the tape index past the last entry belonging to the frame at `idx`.
#[inline]
pub fn entry_span_end(entries: &[TapeEntry], idx: usize) -> usize {
    match entries[idx].tag {
        TAG_ARRAY | TAG_MAP | TAG_SET | TAG_PUSH | TAG_ATTRIBUTE => entries[idx].b as usize,
        _ => idx + 1,
    }
}

// ── FrameRef ────────────────────────────────────────────────────────────────

/// Zero-copy reference to a single frame in the tape.
#[derive(Clone, Copy)]
pub struct FrameRef<'a> {
    entries: &'a [TapeEntry],
    start: usize,
    backing: &'a [u8],
}

impl<'a> FrameRef<'a> {
    /// Tag byte identifying the frame type.
    #[inline]
    pub fn tag(&self) -> u8 {
        self.entries[self.start].tag
    }

    /// Byte content for string-typed frames.
    #[inline]
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        let e = &self.entries[self.start];
        match e.tag {
            TAG_SIMPLE_STRING | TAG_ERROR | TAG_BULK_STRING | TAG_BIG_NUMBER | TAG_BULK_ERROR
            | TAG_VERBATIM_STRING => Some(&self.backing[e.a as usize..e.b as usize]),
            _ => None,
        }
    }

    /// Integer value.
    #[inline]
    pub fn as_integer(&self) -> Option<i64> {
        let e = &self.entries[self.start];
        if e.tag == TAG_INTEGER {
            Some(e.as_i64())
        } else {
            None
        }
    }

    /// Element count for aggregate frames.
    #[inline]
    pub fn element_count(&self) -> Option<u32> {
        let e = &self.entries[self.start];
        match e.tag {
            TAG_ARRAY | TAG_SET | TAG_PUSH => Some(e.a),
            TAG_MAP | TAG_ATTRIBUTE => Some(e.a * 2),
            _ => None,
        }
    }

    /// Iterate over children of an aggregate frame.
    #[inline]
    pub fn children(&self) -> Option<ChildIter<'a>> {
        let e = &self.entries[self.start];
        let end = match e.tag {
            TAG_ARRAY | TAG_MAP | TAG_SET | TAG_PUSH | TAG_ATTRIBUTE => e.b as usize,
            _ => return None,
        };
        Some(ChildIter {
            entries: self.entries,
            backing: self.backing,
            index: self.start + 1,
            end,
        })
    }

    /// Returns the command name when this frame is a command array.
    ///
    /// Follows the same convention as [`RespFrame::command_name()`]: returns the
    /// byte payload of the first child element if `self` is a non-empty array
    /// (or attribute-wrapped array).
    #[inline]
    pub fn command_name(&self) -> Option<&'a [u8]> {
        let e = &self.entries[self.start];
        match e.tag {
            TAG_ARRAY if e.a > 0 => {
                // First child is at start + 1.
                let child = &self.entries[self.start + 1];
                match child.tag {
                    TAG_SIMPLE_STRING | TAG_BULK_STRING => {
                        Some(&self.backing[child.a as usize..child.b as usize])
                    }
                    _ => None,
                }
            }
            #[cfg(feature = "resp3")]
            TAG_ATTRIBUTE => {
                // The inner frame is after all attribute entries.
                let attr_end = e.b as usize;
                // The data frame is the last child — but we need to find it.
                // For attribute, the layout is: [ATTR header] [key1] [val1] ... [keyN] [valN] [data frame]
                // The data frame starts at `start + 1 + 2*pair_count` tape entries... but that's
                // not correct because entries can be multi-entry. We need to skip `e.a` pairs.
                // Simpler: the data frame is the entry right before `attr_end` span?
                // Actually: iterate children, the last one is the data frame.
                let mut last_start = None;
                let mut idx = self.start + 1;
                while idx < attr_end {
                    last_start = Some(idx);
                    idx = entry_span_end(self.entries, idx);
                }
                if let Some(ds) = last_start {
                    let data_ref = FrameRef {
                        entries: self.entries,
                        start: ds,
                        backing: self.backing,
                    };
                    data_ref.command_name()
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Iterator over children of an aggregate tape entry.
pub struct ChildIter<'a> {
    entries: &'a [TapeEntry],
    backing: &'a [u8],
    index: usize,
    end: usize,
}

impl<'a> Iterator for ChildIter<'a> {
    type Item = FrameRef<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.end {
            return None;
        }
        let start = self.index;
        self.index = entry_span_end(self.entries, start);
        Some(FrameRef {
            entries: self.entries,
            start,
            backing: self.backing,
        })
    }
}

/// Iterator over top-level frames in the tape.
pub struct TapeIter<'a> {
    entries: &'a [TapeEntry],
    backing: &'a [u8],
    index: usize,
}

impl<'a> Iterator for TapeIter<'a> {
    type Item = FrameRef<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.entries.len() {
            return None;
        }
        let start = self.index;
        self.index = entry_span_end(self.entries, start);
        Some(FrameRef {
            entries: self.entries,
            start,
            backing: self.backing,
        })
    }
}

// ── Fused helpers ───────────────────────────────────────────────────────────
// These replace the two-pass CrlfPositions approach with direct inline
// scanning. RESP header lines are 3–15 bytes; a simple byte loop beats any
// SIMD setup for these distances.

/// Find next `\r\n` starting from `from`. Returns the offset of `\r`.
#[inline(always)]
fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let end = buf.len().wrapping_sub(1);
    let mut i = from;
    while i < end {
        if unsafe { *buf.get_unchecked(i) == b'\r' && *buf.get_unchecked(i + 1) == b'\n' } {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Parse a number line starting at `start` up to the next `\r\n`.
/// Returns `(value, crlf_offset)`.
#[inline(always)]
fn tape_number_line(buf: &[u8], start: usize) -> TapeResult<(i64, usize)> {
    let crlf = find_crlf(buf, start).ok_or(ParseError::NeedMoreData)?;
    let line = &buf[start..crlf];
    if let Some(value) = parse_short_int(line) {
        return Ok((value, crlf));
    }
    let (value, consumed) = swar_parse_int(line).ok_or(ParseError::InvalidFrame)?;
    if consumed != line.len() {
        return Err(ParseError::InvalidFrame);
    }
    Ok((value, crlf))
}

/// Verify the trailing `\r\n` of a bulk string at a known offset.
#[inline(always)]
fn tape_validate_crlf(buf: &[u8], offset: usize) -> TapeResult<()> {
    if offset + 1 >= buf.len() {
        return Err(ParseError::NeedMoreData);
    }
    if buf[offset] != b'\r' || buf[offset + 1] != b'\n' {
        return Err(ParseError::InvalidFrame);
    }
    Ok(())
}

// ── Dispatch table ──────────────────────────────────────────────────────────

type TapeParseFn = fn(&[u8], &mut usize, &mut Vec<TapeEntry>, u8) -> TapeResult<()>;

const TAPE_DISPATCH: [TapeParseFn; 256] = build_tape_dispatch();

const fn build_tape_dispatch() -> [TapeParseFn; 256] {
    let mut table = [tape_parse_inline as TapeParseFn; 256];
    table[b'+' as usize] = tape_parse_simple_string;
    table[b'-' as usize] = tape_parse_error;
    table[b':' as usize] = tape_parse_integer;
    table[b'$' as usize] = tape_parse_bulk_string;
    table[b'*' as usize] = tape_parse_array;
    #[cfg(feature = "resp3")]
    {
        table[b'_' as usize] = tape_parse_null;
        table[b'#' as usize] = tape_parse_boolean;
        table[b',' as usize] = tape_parse_double;
        table[b'(' as usize] = tape_parse_big_number;
        table[b'!' as usize] = tape_parse_bulk_error;
        table[b'=' as usize] = tape_parse_verbatim_string;
        table[b'%' as usize] = tape_parse_map;
        table[b'~' as usize] = tape_parse_set;
        table[b'>' as usize] = tape_parse_push;
        table[b'|' as usize] = tape_parse_attribute;
    }
    table
}

// ── Tape frame parsers ──────────────────────────────────────────────────────

#[inline(always)]
fn tape_parse_frame(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    depth: u8,
) -> TapeResult<()> {
    if depth > MAX_NESTING_DEPTH {
        return Err(ParseError::NestingTooDeep);
    }
    let byte = *buf.get(*offset).ok_or(ParseError::NeedMoreData)?;
    let parser = TAPE_DISPATCH[byte as usize];
    let mut trial = *offset;
    parser(buf, &mut trial, entries, depth)?;
    *offset = trial;
    Ok(())
}

// ── RESP2 parsers ───────────────────────────────────────────────────────────

#[inline(always)]
fn tape_parse_simple_string(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let line_end = find_crlf(buf, start + 1).ok_or(ParseError::NeedMoreData)?;
    *offset = line_end + 2;
    entries.push(TapeEntry::new(
        TAG_SIMPLE_STRING,
        (start + 1) as u32,
        line_end as u32,
    ));
    Ok(())
}

#[inline(always)]
fn tape_parse_error(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let line_end = find_crlf(buf, start + 1).ok_or(ParseError::NeedMoreData)?;
    *offset = line_end + 2;
    entries.push(TapeEntry::new(
        TAG_ERROR,
        (start + 1) as u32,
        line_end as u32,
    ));
    Ok(())
}

#[inline(always)]
fn tape_parse_integer(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let (value, line_end) = tape_number_line(buf, start + 1)?;
    *offset = line_end + 2;
    entries.push(TapeEntry::from_i64(value));
    Ok(())
}

#[inline(always)]
fn tape_parse_bulk_string(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let (len, line_end) = tape_number_line(buf, start + 1)?;
    let data_start = line_end + 2;

    if len == -1 {
        *offset = data_start;
        entries.push(TapeEntry::null(TAG_NULL_BULK_STRING));
        return Ok(());
    }
    if len < -1 {
        return Err(ParseError::InvalidFrame);
    }

    let len = usize::try_from(len).map_err(|_| ParseError::FrameTooLarge)?;
    if len > MAX_BULK_STRING_LEN {
        return Err(ParseError::FrameTooLarge);
    }

    let data_end = data_start
        .checked_add(len)
        .ok_or(ParseError::FrameTooLarge)?;
    tape_validate_crlf(buf, data_end)?;
    *offset = data_end + 2;
    entries.push(TapeEntry::new(
        TAG_BULK_STRING,
        data_start as u32,
        data_end as u32,
    ));
    Ok(())
}

#[inline(always)]
fn tape_parse_array(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let (count, line_end) = tape_number_line(buf, start + 1)?;
    *offset = line_end + 2;

    if count == -1 {
        entries.push(TapeEntry::null(TAG_NULL_ARRAY));
        return Ok(());
    }
    if count < -1 {
        return Err(ParseError::InvalidFrame);
    }

    let count = u32::try_from(count).map_err(|_| ParseError::FrameTooLarge)?;
    if count as usize > MAX_ARRAY_ELEMENTS {
        return Err(ParseError::FrameTooLarge);
    }

    let header_idx = entries.len();
    entries.push(TapeEntry::new(TAG_ARRAY, count, 0));

    for _ in 0..count {
        tape_parse_frame(buf, offset, entries, depth + 1)?;
    }

    entries[header_idx].b = entries.len() as u32;
    Ok(())
}

// ── RESP3 parsers ───────────────────────────────────────────────────────────

#[cfg(feature = "resp3")]
#[inline(always)]
fn tape_parse_null(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let line_end = find_crlf(buf, start + 1).ok_or(ParseError::NeedMoreData)?;
    if line_end != start + 1 {
        return Err(ParseError::InvalidFrame);
    }
    *offset = line_end + 2;
    entries.push(TapeEntry::null(TAG_NULL));
    Ok(())
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn tape_parse_boolean(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let line_end = find_crlf(buf, start + 1).ok_or(ParseError::NeedMoreData)?;
    if line_end != start + 2 {
        return Err(ParseError::InvalidFrame);
    }
    let value = match buf[start + 1] {
        b't' => true,
        b'f' => false,
        _ => return Err(ParseError::InvalidFrame),
    };
    *offset = line_end + 2;
    entries.push(TapeEntry::new(TAG_BOOLEAN, value as u32, 0));
    Ok(())
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn tape_parse_double(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let line_end = find_crlf(buf, start + 1).ok_or(ParseError::NeedMoreData)?;
    let text =
        std::str::from_utf8(&buf[start + 1..line_end]).map_err(|_| ParseError::InvalidFrame)?;
    let value: f64 = text.parse().map_err(|_| ParseError::InvalidFrame)?;
    *offset = line_end + 2;
    entries.push(TapeEntry::from_f64(value));
    Ok(())
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn tape_parse_big_number(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let line_end = find_crlf(buf, start + 1).ok_or(ParseError::NeedMoreData)?;
    if line_end == start + 1 {
        return Err(ParseError::InvalidFrame);
    }
    *offset = line_end + 2;
    entries.push(TapeEntry::new(
        TAG_BIG_NUMBER,
        (start + 1) as u32,
        line_end as u32,
    ));
    Ok(())
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn tape_parse_bulk_error(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let (len, line_end) = tape_number_line(buf, start + 1)?;
    if len < 0 {
        return Err(ParseError::InvalidFrame);
    }

    let len = usize::try_from(len).map_err(|_| ParseError::FrameTooLarge)?;
    if len > MAX_BULK_STRING_LEN {
        return Err(ParseError::FrameTooLarge);
    }

    let data_start = line_end + 2;
    let data_end = data_start
        .checked_add(len)
        .ok_or(ParseError::FrameTooLarge)?;
    tape_validate_crlf(buf, data_end)?;
    *offset = data_end + 2;
    entries.push(TapeEntry::new(
        TAG_BULK_ERROR,
        data_start as u32,
        data_end as u32,
    ));
    Ok(())
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn tape_parse_verbatim_string(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let (len, line_end) = tape_number_line(buf, start + 1)?;
    if len < 4 {
        return Err(ParseError::InvalidFrame);
    }

    let len = usize::try_from(len).map_err(|_| ParseError::FrameTooLarge)?;
    if len > MAX_BULK_STRING_LEN {
        return Err(ParseError::FrameTooLarge);
    }

    let data_start = line_end + 2;
    let data_end = data_start
        .checked_add(len)
        .ok_or(ParseError::FrameTooLarge)?;
    tape_validate_crlf(buf, data_end)?;

    if buf.get(data_start + 3) != Some(&b':') {
        return Err(ParseError::InvalidFrame);
    }

    let mut encoding = [0u8; 3];
    encoding.copy_from_slice(&buf[data_start..data_start + 3]);
    *offset = data_end + 2;
    let mut entry = TapeEntry::new(TAG_VERBATIM_STRING, (data_start + 4) as u32, data_end as u32);
    entry.extra = encoding;
    entries.push(entry);
    Ok(())
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn tape_parse_map(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let (count, line_end) = tape_number_line(buf, start + 1)?;
    if count < 0 {
        return Err(ParseError::InvalidFrame);
    }

    let count = u32::try_from(count).map_err(|_| ParseError::FrameTooLarge)?;
    if count as usize > MAX_ARRAY_ELEMENTS {
        return Err(ParseError::FrameTooLarge);
    }

    *offset = line_end + 2;
    let header_idx = entries.len();
    entries.push(TapeEntry::new(TAG_MAP, count, 0));

    for _ in 0..count {
        tape_parse_frame(buf, offset, entries, depth + 1)?;
        tape_parse_frame(buf, offset, entries, depth + 1)?;
    }

    entries[header_idx].b = entries.len() as u32;
    Ok(())
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn tape_parse_set(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let (count, line_end) = tape_number_line(buf, start + 1)?;
    if count < 0 {
        return Err(ParseError::InvalidFrame);
    }

    let count = u32::try_from(count).map_err(|_| ParseError::FrameTooLarge)?;
    if count as usize > MAX_ARRAY_ELEMENTS {
        return Err(ParseError::FrameTooLarge);
    }

    *offset = line_end + 2;
    let header_idx = entries.len();
    entries.push(TapeEntry::new(TAG_SET, count, 0));

    for _ in 0..count {
        tape_parse_frame(buf, offset, entries, depth + 1)?;
    }

    entries[header_idx].b = entries.len() as u32;
    Ok(())
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn tape_parse_push(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let (count, line_end) = tape_number_line(buf, start + 1)?;
    if count <= 0 {
        return Err(ParseError::InvalidFrame);
    }

    let count = u32::try_from(count).map_err(|_| ParseError::FrameTooLarge)?;
    if count as usize > MAX_ARRAY_ELEMENTS {
        return Err(ParseError::FrameTooLarge);
    }

    *offset = line_end + 2;
    let header_idx = entries.len();
    entries.push(TapeEntry::new(TAG_PUSH, count, 0));

    for _ in 0..count {
        tape_parse_frame(buf, offset, entries, depth + 1)?;
    }

    entries[header_idx].b = entries.len() as u32;
    Ok(())
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn tape_parse_attribute(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let (count, line_end) = tape_number_line(buf, start + 1)?;
    if count < 0 {
        return Err(ParseError::InvalidFrame);
    }

    let count = u32::try_from(count).map_err(|_| ParseError::FrameTooLarge)?;
    if count as usize > MAX_ARRAY_ELEMENTS {
        return Err(ParseError::FrameTooLarge);
    }

    *offset = line_end + 2;
    let header_idx = entries.len();
    entries.push(TapeEntry::new(TAG_ATTRIBUTE, count, 0));

    // Key-value pairs.
    for _ in 0..count {
        tape_parse_frame(buf, offset, entries, depth + 1)?;
        tape_parse_frame(buf, offset, entries, depth + 1)?;
    }
    // Data frame follows the pairs.
    tape_parse_frame(buf, offset, entries, depth + 1)?;

    entries[header_idx].b = entries.len() as u32;
    Ok(())
}

// ── Inline ──────────────────────────────────────────────────────────────────

#[inline(always)]
fn tape_parse_inline(
    buf: &[u8],
    offset: &mut usize,
    entries: &mut Vec<TapeEntry>,
    _depth: u8,
) -> TapeResult<()> {
    let start = *offset;
    let line_end = find_crlf(buf, start).ok_or(ParseError::NeedMoreData)?;
    let header_idx = entries.len();
    entries.push(TapeEntry::new(TAG_ARRAY, 0, 0)); // placeholder

    let mut cursor = start;
    let mut count: u32 = 0;
    while cursor < line_end {
        while cursor < line_end && buf[cursor] == b' ' {
            cursor += 1;
        }
        if cursor == line_end {
            break;
        }
        let part_start = cursor;
        while cursor < line_end && buf[cursor] != b' ' {
            cursor += 1;
        }
        entries.push(TapeEntry::new(
            TAG_BULK_STRING,
            part_start as u32,
            cursor as u32,
        ));
        count += 1;
    }

    if count == 0 {
        entries.pop(); // remove placeholder
        return Err(ParseError::InvalidFrame);
    }

    *offset = line_end + 2;
    entries[header_idx].a = count;
    entries[header_idx].b = entries.len() as u32;
    Ok(())
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::RespFrame;
    use crate::RespParser;

    #[test]
    fn simple_string() {
        let tape = RespTape::parse_pipeline(b"+OK\r\n").unwrap();
        assert_eq!(tape.frame_count(), 1);
        assert_eq!(tape.consumed(), 5);
        let entries = tape.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tag, TAG_SIMPLE_STRING);
        assert_eq!(
            &tape.backing()[entries[0].a as usize..entries[0].b as usize],
            b"OK"
        );
    }

    #[test]
    fn error() {
        let tape = RespTape::parse_pipeline(b"-ERR bad\r\n").unwrap();
        assert_eq!(tape.frame_count(), 1);
        let e = &tape.entries()[0];
        assert_eq!(e.tag, TAG_ERROR);
        assert_eq!(
            &tape.backing()[e.a as usize..e.b as usize],
            b"ERR bad"
        );
    }

    #[test]
    fn integer() {
        let tape = RespTape::parse_pipeline(b":42\r\n").unwrap();
        assert_eq!(tape.frame_count(), 1);
        let e = &tape.entries()[0];
        assert_eq!(e.tag, TAG_INTEGER);
        assert_eq!(e.as_i64(), 42);
    }

    #[test]
    fn negative_integer() {
        let tape = RespTape::parse_pipeline(b":-1\r\n").unwrap();
        let e = &tape.entries()[0];
        assert_eq!(e.as_i64(), -1);
    }

    #[test]
    fn large_integer() {
        let tape = RespTape::parse_pipeline(b":9223372036854775807\r\n").unwrap();
        let e = &tape.entries()[0];
        assert_eq!(e.as_i64(), i64::MAX);
    }

    #[test]
    fn bulk_string() {
        let tape = RespTape::parse_pipeline(b"$5\r\nhello\r\n").unwrap();
        assert_eq!(tape.frame_count(), 1);
        let e = &tape.entries()[0];
        assert_eq!(e.tag, TAG_BULK_STRING);
        assert_eq!(
            &tape.backing()[e.a as usize..e.b as usize],
            b"hello"
        );
    }

    #[test]
    fn null_bulk_string() {
        let tape = RespTape::parse_pipeline(b"$-1\r\n").unwrap();
        let e = &tape.entries()[0];
        assert_eq!(e.tag, TAG_NULL_BULK_STRING);
    }

    #[test]
    fn null_array() {
        let tape = RespTape::parse_pipeline(b"*-1\r\n").unwrap();
        let e = &tape.entries()[0];
        assert_eq!(e.tag, TAG_NULL_ARRAY);
    }

    #[test]
    fn empty_array() {
        let tape = RespTape::parse_pipeline(b"*0\r\n").unwrap();
        assert_eq!(tape.frame_count(), 1);
        let e = &tape.entries()[0];
        assert_eq!(e.tag, TAG_ARRAY);
        assert_eq!(e.a, 0);
        assert_eq!(e.b, 1); // tape_end = header+1
    }

    #[test]
    fn array_set_cmd() {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let tape = RespTape::parse_pipeline(input).unwrap();
        assert_eq!(tape.frame_count(), 1);
        assert_eq!(tape.consumed(), input.len());
        let entries = tape.entries();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].tag, TAG_ARRAY);
        assert_eq!(entries[0].a, 3);
        assert_eq!(entries[0].b, 4);
        assert_eq!(entries[1].tag, TAG_BULK_STRING);
        assert_eq!(
            &tape.backing()[entries[1].a as usize..entries[1].b as usize],
            b"SET"
        );
        assert_eq!(
            &tape.backing()[entries[2].a as usize..entries[2].b as usize],
            b"foo"
        );
        assert_eq!(
            &tape.backing()[entries[3].a as usize..entries[3].b as usize],
            b"bar"
        );
    }

    #[test]
    fn pipeline_frame_count() {
        let mut buf = Vec::new();
        for _ in 0..10 {
            buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        }
        let tape = RespTape::parse_pipeline(&buf).unwrap();
        assert_eq!(tape.frame_count(), 10);
        assert_eq!(tape.consumed(), buf.len());
        assert_eq!(tape.entries().len(), 40);
    }

    #[test]
    fn frame_ref_children() {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let tape = RespTape::parse_pipeline(input).unwrap();
        let cmd = tape.iter().next().unwrap();
        assert_eq!(cmd.tag(), TAG_ARRAY);
        assert_eq!(cmd.element_count(), Some(3));

        let children: Vec<_> = cmd.children().unwrap().collect();
        assert_eq!(children.len(), 3);
        assert_eq!(children[0].as_bytes(), Some(b"SET".as_slice()));
        assert_eq!(children[1].as_bytes(), Some(b"foo".as_slice()));
        assert_eq!(children[2].as_bytes(), Some(b"bar".as_slice()));
    }

    #[test]
    fn tape_iter_pipeline() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"+OK\r\n");
        buf.extend_from_slice(b":42\r\n");
        buf.extend_from_slice(b"$5\r\nhello\r\n");
        let tape = RespTape::parse_pipeline(&buf).unwrap();
        assert_eq!(tape.frame_count(), 3);
        let mut it = tape.iter();
        let f0 = it.next().unwrap();
        assert_eq!(f0.tag(), TAG_SIMPLE_STRING);
        assert_eq!(f0.as_bytes(), Some(b"OK".as_slice()));
        let f1 = it.next().unwrap();
        assert_eq!(f1.tag(), TAG_INTEGER);
        assert_eq!(f1.as_integer(), Some(42));
        let f2 = it.next().unwrap();
        assert_eq!(f2.tag(), TAG_BULK_STRING);
        assert_eq!(f2.as_bytes(), Some(b"hello".as_slice()));
        assert!(it.next().is_none());
    }

    #[test]
    fn nested_array() {
        let input = b"*1\r\n*2\r\n:1\r\n:2\r\n";
        let tape = RespTape::parse_pipeline(input).unwrap();
        assert_eq!(tape.frame_count(), 1);
        let entries = tape.entries();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0], TapeEntry::new(TAG_ARRAY, 1, 4));
        assert_eq!(entries[1], TapeEntry::new(TAG_ARRAY, 2, 4));
        assert_eq!(entries[2], TapeEntry::from_i64(1));
        assert_eq!(entries[3], TapeEntry::from_i64(2));
    }

    #[test]
    fn inline_command() {
        let tape = RespTape::parse_pipeline(b"PING\r\n").unwrap();
        assert_eq!(tape.frame_count(), 1);
        let entries = tape.entries();
        assert_eq!(entries[0].tag, TAG_ARRAY);
        assert_eq!(entries[0].a, 1);
        assert_eq!(entries[1].tag, TAG_BULK_STRING);
        assert_eq!(
            &tape.backing()[entries[1].a as usize..entries[1].b as usize],
            b"PING"
        );
    }

    #[test]
    fn inline_multi_word() {
        let tape = RespTape::parse_pipeline(b"SET foo bar\r\n").unwrap();
        assert_eq!(tape.frame_count(), 1);
        let entries = tape.entries();
        assert_eq!(entries[0].tag, TAG_ARRAY);
        assert_eq!(entries[0].a, 3);
        assert_eq!(
            &tape.backing()[entries[1].a as usize..entries[1].b as usize],
            b"SET"
        );
        assert_eq!(
            &tape.backing()[entries[2].a as usize..entries[2].b as usize],
            b"foo"
        );
        assert_eq!(
            &tape.backing()[entries[3].a as usize..entries[3].b as usize],
            b"bar"
        );
    }

    /// Cross-validate tape parser against tree (`RespFrame`) parser.
    #[test]
    fn cross_validate_pipeline() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"+OK\r\n");
        buf.extend_from_slice(b"-ERR no\r\n");
        buf.extend_from_slice(b":42\r\n");
        buf.extend_from_slice(b"$5\r\nhello\r\n");
        buf.extend_from_slice(b"$-1\r\n");
        buf.extend_from_slice(b"*-1\r\n");
        buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        buf.extend_from_slice(b"*0\r\n");

        let (tree_frames, tree_consumed) = RespParser::parse_pipeline(&buf).unwrap();
        let tape = RespTape::parse_pipeline(&buf).unwrap();

        assert_eq!(tape.frame_count(), tree_frames.len());
        assert_eq!(tape.consumed(), tree_consumed);

        for (frame_ref, tree_frame) in tape.iter().zip(tree_frames.iter()) {
            assert_tape_matches_frame(&tape, &frame_ref, tree_frame);
        }
    }

    fn assert_tape_matches_frame(_tape: &RespTape, fr: &FrameRef<'_>, tree: &RespFrame) {
        match tree {
            RespFrame::SimpleString(b) => {
                assert_eq!(fr.tag(), TAG_SIMPLE_STRING);
                assert_eq!(fr.as_bytes().unwrap(), b.as_ref());
            }
            RespFrame::Error(b) => {
                assert_eq!(fr.tag(), TAG_ERROR);
                assert_eq!(fr.as_bytes().unwrap(), b.as_ref());
            }
            RespFrame::Integer(v) => {
                assert_eq!(fr.tag(), TAG_INTEGER);
                assert_eq!(fr.as_integer().unwrap(), *v);
            }
            RespFrame::BulkString(Some(b)) => {
                assert_eq!(fr.tag(), TAG_BULK_STRING);
                assert_eq!(fr.as_bytes().unwrap(), b.as_ref());
            }
            RespFrame::BulkString(None) => {
                assert_eq!(fr.tag(), TAG_NULL_BULK_STRING);
            }
            RespFrame::Array(None) => {
                assert_eq!(fr.tag(), TAG_NULL_ARRAY);
            }
            RespFrame::Array(Some(items)) => {
                assert_eq!(fr.tag(), TAG_ARRAY);
                assert_eq!(fr.element_count().unwrap(), items.len() as u32);
                let children: Vec<_> = fr.children().unwrap().collect();
                assert_eq!(children.len(), items.len());
                for (child_ref, child_tree) in children.iter().zip(items.iter()) {
                    assert_tape_matches_frame(_tape, child_ref, child_tree);
                }
            }
            _ => {} // resp3 types tested separately
        }
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn resp3_types() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"_\r\n");
        buf.extend_from_slice(b"#t\r\n");
        buf.extend_from_slice(b"#f\r\n");
        buf.extend_from_slice(b",1.23\r\n");
        buf.extend_from_slice(b"(12345678901234567890\r\n");
        buf.extend_from_slice(b"!5\r\nERROR\r\n");
        buf.extend_from_slice(b"=10\r\ntxt:hello!\r\n");
        buf.extend_from_slice(b"%1\r\n+key\r\n+val\r\n");
        buf.extend_from_slice(b"~2\r\n:1\r\n:2\r\n");

        let (tree_frames, _) = RespParser::parse_pipeline(&buf).unwrap();
        let tape = RespTape::parse_pipeline(&buf).unwrap();
        assert_eq!(tape.frame_count(), tree_frames.len());

        let entries = tape.entries();
        assert_eq!(entries[0].tag, TAG_NULL);
        assert_eq!(entries[1].tag, TAG_BOOLEAN);
        assert!(entries[1].as_bool());
        assert_eq!(entries[2].tag, TAG_BOOLEAN);
        assert!(!entries[2].as_bool());
        assert_eq!(entries[3].tag, TAG_DOUBLE);
        {
            assert!((entries[3].as_f64() - 1.23).abs() < 1e-10);
        }
        assert_eq!(entries[4].tag, TAG_BIG_NUMBER);
        assert_eq!(
            &tape.backing()[entries[4].a as usize..entries[4].b as usize],
            b"12345678901234567890"
        );
        assert_eq!(entries[5].tag, TAG_BULK_ERROR);
        assert_eq!(
            &tape.backing()[entries[5].a as usize..entries[5].b as usize],
            b"ERROR"
        );
        assert_eq!(entries[6].tag, TAG_VERBATIM_STRING);
        assert_eq!(entries[6].extra, *b"txt");
        assert_eq!(
            &tape.backing()[entries[6].a as usize..entries[6].b as usize],
            b"hello!"
        );
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn resp3_push_frame() {
        let input = b">2\r\n$7\r\nmessage\r\n$5\r\nhello\r\n";
        let tape = RespTape::parse_pipeline(input).unwrap();
        assert_eq!(tape.frame_count(), 1);
        let entries = tape.entries();
        assert_eq!(entries[0].tag, TAG_PUSH);
        assert_eq!(entries[0].a, 2);
        assert_eq!(entries.len(), 3);
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn resp3_attribute_frame() {
        let input = b"|1\r\n+key\r\n+val\r\n:42\r\n";
        let tape = RespTape::parse_pipeline(input).unwrap();
        assert_eq!(tape.frame_count(), 1);
        let entries = tape.entries();
        assert_eq!(entries[0].tag, TAG_ATTRIBUTE);
        assert_eq!(entries[0].a, 1);
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[3].tag, TAG_INTEGER);
        assert_eq!(entries[3].as_i64(), 42);
    }

    #[test]
    fn tape_entry_size() {
        assert_eq!(std::mem::size_of::<TapeEntry>(), 12);
    }

    #[test]
    fn need_more_data() {
        assert!(matches!(
            RespTape::parse_pipeline(b""),
            Err(ParseError::NeedMoreData)
        ));
        assert!(matches!(
            RespTape::parse_pipeline(b"+OK"),
            Err(ParseError::NeedMoreData)
        ));
        assert!(matches!(
            RespTape::parse_pipeline(b"$5\r\nhel"),
            Err(ParseError::NeedMoreData)
        ));
    }

    #[test]
    fn large_pipeline_1000() {
        let mut buf = Vec::new();
        for i in 0..1000 {
            let key = format!("key:{i}");
            let val = format!("val:{i}");
            buf.extend_from_slice(
                format!("*3\r\n$3\r\nSET\r\n${}\r\n", key.len()).as_bytes(),
            );
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
            buf.extend_from_slice(val.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        let tape = RespTape::parse_pipeline(&buf).unwrap();
        assert_eq!(tape.frame_count(), 1000);
        assert_eq!(tape.consumed(), buf.len());
        assert_eq!(tape.entries().len(), 4000);
    }
}
