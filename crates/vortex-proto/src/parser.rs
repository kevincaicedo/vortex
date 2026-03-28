use bytes::Bytes;

use crate::{
    frame::RespFrame,
    scanner::{CrlfPositions, scan_crlf},
    swar::swar_parse_int,
};

/// Parse result error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    /// The buffer ends before a full frame is available.
    NeedMoreData,
    /// The frame exceeds configured limits.
    FrameTooLarge,
    /// Recursive containers exceed the configured nesting limit.
    NestingTooDeep,
    /// The buffer does not contain a valid RESP frame.
    InvalidFrame,
}

/// Backward-compatible alias for callers that still refer to the old error type.
pub type NeedMoreData = ParseError;

/// RESP2/RESP3 parser.
///
/// Phase 2 uses a pre-scanned CRLF position table, SWAR integer parsing, and a
/// jump-table dispatch to keep the parser on predictable, cache-friendly code
/// paths. Byte-backed payloads are carved from one parser-owned `Bytes`
/// snapshot of the input buffer, so nested frames reuse shared backing without
/// a second materialization pass.
pub struct RespParser;

/// Maximum number of elements in a single RESP aggregate frame.
pub(crate) const MAX_ARRAY_ELEMENTS: usize = 1_048_576;
/// Maximum nesting depth for recursive frame types.
pub(crate) const MAX_NESTING_DEPTH: u8 = 32;
/// Maximum accepted bulk string length (512 MiB).
pub(crate) const MAX_BULK_STRING_LEN: usize = 512 * 1024 * 1024;

pub(crate) type ParseResult<T> = Result<T, ParseError>;
type ParseFn = fn(&[u8], &Bytes, &CrlfPositions, &mut ParserState, u8) -> ParseResult<RespFrame>;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ParserState {
    pub(crate) offset: usize,
    pub(crate) crlf_index: usize,
}

const DISPATCH: [ParseFn; 256] = build_dispatch_table();

const fn build_dispatch_table() -> [ParseFn; 256] {
    let mut table = [parse_inline_frame as ParseFn; 256];
    table[b'+' as usize] = parse_simple_string_frame;
    table[b'-' as usize] = parse_error_frame;
    table[b':' as usize] = parse_integer_frame;
    table[b'$' as usize] = parse_bulk_string_frame;
    table[b'*' as usize] = parse_array_frame;
    #[cfg(feature = "resp3")]
    {
        table[b'_' as usize] = parse_null_frame;
        table[b'#' as usize] = parse_boolean_frame;
        table[b',' as usize] = parse_double_frame;
        table[b'(' as usize] = parse_big_number_frame;
        table[b'!' as usize] = parse_bulk_error_frame;
        table[b'=' as usize] = parse_verbatim_string_frame;
        table[b'%' as usize] = parse_map_frame;
        table[b'~' as usize] = parse_set_frame;
        table[b'>' as usize] = parse_push_frame;
        table[b'|' as usize] = parse_attribute_frame;
    }
    table
}

impl RespParser {
    /// Attempts to parse a single RESP frame from the buffer.
    pub fn parse(buf: &[u8]) -> ParseResult<(RespFrame, usize)> {
        if buf.is_empty() {
            return Err(ParseError::NeedMoreData);
        }

        Self::parse_bytes(Bytes::copy_from_slice(buf))
    }

    /// Attempts to parse as many complete RESP frames as possible from the buffer.
    ///
    /// If the buffer ends with a partial frame, all complete frames parsed before
    /// the incomplete suffix are returned along with the consumed byte count.
    pub fn parse_pipeline(buf: &[u8]) -> ParseResult<(Vec<RespFrame>, usize)> {
        if buf.is_empty() {
            return Err(ParseError::NeedMoreData);
        }

        let backing = Bytes::copy_from_slice(buf);
        Self::parse_pipeline_inner(backing)
    }

    /// Parses a single RESP frame from a caller-owned `Bytes` without copying.
    ///
    /// Parsed frames share the underlying allocation of `backing`, avoiding
    /// the `Bytes::copy_from_slice` in [`parse`].
    pub fn parse_bytes(backing: Bytes) -> ParseResult<(RespFrame, usize)> {
        if backing.is_empty() {
            return Err(ParseError::NeedMoreData);
        }

        let bytes = backing.as_ref();
        let positions = scan_crlf(bytes);
        let mut state = ParserState::default();
        let frame = parse_frame(bytes, &backing, &positions, &mut state, 0)?;
        Ok((frame, state.offset))
    }

    /// Parses a pipeline of RESP frames from a caller-owned `Bytes` without copying.
    ///
    /// Parsed frames share the underlying allocation of `backing`, avoiding
    /// the `Bytes::copy_from_slice` in [`parse_pipeline`].
    pub fn parse_pipeline_bytes(backing: Bytes) -> ParseResult<(Vec<RespFrame>, usize)> {
        if backing.is_empty() {
            return Err(ParseError::NeedMoreData);
        }

        Self::parse_pipeline_inner(backing)
    }

    fn parse_pipeline_inner(backing: Bytes) -> ParseResult<(Vec<RespFrame>, usize)> {
        let bytes = backing.as_ref();
        let positions = scan_crlf(bytes);
        let mut state = ParserState::default();
        let mut frames = Vec::with_capacity((positions.len() / 3).max(1));

        while state.offset < bytes.len() {
            match parse_frame(bytes, &backing, &positions, &mut state, 0) {
                Ok(frame) => frames.push(frame),
                Err(error) if frames.is_empty() => return Err(error),
                Err(_) => break,
            }
        }

        if frames.is_empty() {
            Err(ParseError::NeedMoreData)
        } else {
            Ok((frames, state.offset))
        }
    }
}

fn parse_frame(
    buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    depth: u8,
) -> ParseResult<RespFrame> {
    if depth > MAX_NESTING_DEPTH {
        return Err(ParseError::NestingTooDeep);
    }

    let byte = *buf.get(state.offset).ok_or(ParseError::NeedMoreData)?;
    let parser = DISPATCH[byte as usize];
    let mut trial = *state;
    let frame = parser(buf, backing, positions, &mut trial, depth)?;
    *state = trial;
    Ok(frame)
}

#[inline(always)]
pub(crate) fn consume_next_crlf(
    positions: &CrlfPositions,
    state: &mut ParserState,
    search_from: usize,
) -> ParseResult<usize> {
    let mut index = state.crlf_index;
    while let Some(position) = positions.get(index) {
        if position < search_from {
            index += 1;
            continue;
        }

        state.crlf_index = index + 1;
        return Ok(position);
    }

    Err(ParseError::NeedMoreData)
}

/// Scalar fast path for integer lines short enough that SWAR setup overhead
/// dominates. Handles 1-7 byte lines (positive up to 7 digits, negative up
/// to 6 digits), which covers >99% of RESP lengths and counts.
#[inline(always)]
pub(crate) fn parse_short_int(line: &[u8]) -> Option<i64> {
    let len = line.len();
    if len == 0 || len > 7 {
        return None;
    }

    // Single digit — most common case (array counts, small bulk lengths).
    if len == 1 {
        let d = line[0].wrapping_sub(b'0');
        return if d <= 9 { Some(i64::from(d)) } else { None };
    }

    let (digits, negative) = if line[0] == b'-' {
        (&line[1..], true)
    } else {
        (line, false)
    };

    if digits.is_empty() {
        return None;
    }

    let mut val: u64 = 0;
    let mut i = 0;
    while i < digits.len() {
        let d = digits[i].wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        val = val * 10 + u64::from(d);
        i += 1;
    }

    Some(if negative { -(val as i64) } else { val as i64 })
}

#[inline(always)]
pub(crate) fn parse_number_line(
    buf: &[u8],
    positions: &CrlfPositions,
    state: &mut ParserState,
    start: usize,
) -> ParseResult<(i64, usize)> {
    let line_end = consume_next_crlf(positions, state, start)?;
    let line = &buf[start..line_end];
    // Scalar fast path for short integer lines (covers >99% of RESP traffic).
    if let Some(value) = parse_short_int(line) {
        return Ok((value, line_end));
    }
    // SWAR path for longer integers (8+ digits, where it outperforms scalar).
    let (value, consumed) = swar_parse_int(line).ok_or(ParseError::InvalidFrame)?;
    if consumed != line.len() {
        return Err(ParseError::InvalidFrame);
    }
    Ok((value, line_end))
}

#[inline(always)]
pub(crate) fn validate_trailing_crlf(
    buf: &[u8],
    positions: &CrlfPositions,
    state: &mut ParserState,
    expected_offset: usize,
) -> ParseResult<()> {
    if expected_offset + 1 >= buf.len() {
        return Err(ParseError::NeedMoreData);
    }
    if buf[expected_offset] != b'\r' || buf[expected_offset + 1] != b'\n' {
        return Err(ParseError::InvalidFrame);
    }

    let position = consume_next_crlf(positions, state, expected_offset)?;
    if position != expected_offset {
        return Err(ParseError::InvalidFrame);
    }

    Ok(())
}

#[inline(always)]
fn parse_simple_string_frame(
    _buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let line_end = consume_next_crlf(positions, state, start + 1)?;
    state.offset = line_end + 2;
    Ok(RespFrame::SimpleString(backing.slice(start + 1..line_end)))
}

#[inline(always)]
fn parse_error_frame(
    _buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let line_end = consume_next_crlf(positions, state, start + 1)?;
    state.offset = line_end + 2;
    Ok(RespFrame::Error(backing.slice(start + 1..line_end)))
}

#[inline(always)]
fn parse_integer_frame(
    buf: &[u8],
    _backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let (value, line_end) = parse_number_line(buf, positions, state, start + 1)?;
    state.offset = line_end + 2;
    Ok(RespFrame::Integer(value))
}

#[inline(always)]
fn parse_bulk_string_frame(
    buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let (len, line_end) = parse_number_line(buf, positions, state, start + 1)?;
    let data_start = line_end + 2;

    if len == -1 {
        state.offset = data_start;
        return Ok(RespFrame::BulkString(None));
    }
    if len < -1 {
        return Err(ParseError::InvalidFrame);
    }

    let len = usize::try_from(len).map_err(|_| ParseError::FrameTooLarge)?;
    if len > MAX_BULK_STRING_LEN {
        return Err(ParseError::FrameTooLarge);
    }

    let data_end = data_start.checked_add(len).ok_or(ParseError::FrameTooLarge)?;
    validate_trailing_crlf(buf, positions, state, data_end)?;
    state.offset = data_end + 2;
    Ok(RespFrame::BulkString(Some(backing.slice(data_start..data_end))))
}

#[inline(always)]
fn parse_array_frame(
    buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let (count, line_end) = parse_number_line(buf, positions, state, start + 1)?;
    state.offset = line_end + 2;

    if count == -1 {
        return Ok(RespFrame::Array(None));
    }
    if count < -1 {
        return Err(ParseError::InvalidFrame);
    }

    let count = usize::try_from(count).map_err(|_| ParseError::FrameTooLarge)?;
    if count > MAX_ARRAY_ELEMENTS {
        return Err(ParseError::FrameTooLarge);
    }

    let mut frames = Vec::with_capacity(count);
    for _ in 0..count {
        frames.push(parse_frame(buf, backing, positions, state, depth + 1)?);
    }

    Ok(RespFrame::Array(Some(frames)))
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn parse_null_frame(
    _buf: &[u8],
    _backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let line_end = consume_next_crlf(positions, state, start + 1)?;
    if line_end != start + 1 {
        return Err(ParseError::InvalidFrame);
    }
    state.offset = line_end + 2;
    Ok(RespFrame::Null)
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn parse_boolean_frame(
    buf: &[u8],
    _backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let line_end = consume_next_crlf(positions, state, start + 1)?;
    if line_end != start + 2 {
        return Err(ParseError::InvalidFrame);
    }

    let value = match buf[start + 1] {
        b't' => true,
        b'f' => false,
        _ => return Err(ParseError::InvalidFrame),
    };

    state.offset = line_end + 2;
    Ok(RespFrame::Boolean(value))
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn parse_double_frame(
    buf: &[u8],
    _backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let line_end = consume_next_crlf(positions, state, start + 1)?;
    let text = std::str::from_utf8(&buf[start + 1..line_end]).map_err(|_| ParseError::InvalidFrame)?;
    let value = text.parse().map_err(|_| ParseError::InvalidFrame)?;
    state.offset = line_end + 2;
    Ok(RespFrame::Double(value))
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn parse_big_number_frame(
    _buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let line_end = consume_next_crlf(positions, state, start + 1)?;
    if line_end == start + 1 {
        return Err(ParseError::InvalidFrame);
    }
    state.offset = line_end + 2;
    Ok(RespFrame::BigNumber(backing.slice(start + 1..line_end)))
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn parse_bulk_error_frame(
    buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let (len, line_end) = parse_number_line(buf, positions, state, start + 1)?;
    if len < 0 {
        return Err(ParseError::InvalidFrame);
    }

    let len = usize::try_from(len).map_err(|_| ParseError::FrameTooLarge)?;
    if len > MAX_BULK_STRING_LEN {
        return Err(ParseError::FrameTooLarge);
    }

    let data_start = line_end + 2;
    let data_end = data_start.checked_add(len).ok_or(ParseError::FrameTooLarge)?;
    validate_trailing_crlf(buf, positions, state, data_end)?;
    state.offset = data_end + 2;
    Ok(RespFrame::BulkError(backing.slice(data_start..data_end)))
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn parse_verbatim_string_frame(
    buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let (len, line_end) = parse_number_line(buf, positions, state, start + 1)?;
    if len < 4 {
        return Err(ParseError::InvalidFrame);
    }

    let len = usize::try_from(len).map_err(|_| ParseError::FrameTooLarge)?;
    if len > MAX_BULK_STRING_LEN {
        return Err(ParseError::FrameTooLarge);
    }

    let data_start = line_end + 2;
    let data_end = data_start.checked_add(len).ok_or(ParseError::FrameTooLarge)?;
    validate_trailing_crlf(buf, positions, state, data_end)?;

    if buf.get(data_start + 3) != Some(&b':') {
        return Err(ParseError::InvalidFrame);
    }

    let mut encoding = [0; 3];
    encoding.copy_from_slice(&buf[data_start..data_start + 3]);
    state.offset = data_end + 2;
    Ok(RespFrame::VerbatimString {
        encoding,
        data: backing.slice(data_start + 4..data_end),
    })
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn parse_map_frame(
    buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let (count, line_end) = parse_number_line(buf, positions, state, start + 1)?;
    if count < 0 {
        return Err(ParseError::InvalidFrame);
    }

    let count = usize::try_from(count).map_err(|_| ParseError::FrameTooLarge)?;
    if count > MAX_ARRAY_ELEMENTS {
        return Err(ParseError::FrameTooLarge);
    }

    state.offset = line_end + 2;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key = parse_frame(buf, backing, positions, state, depth + 1)?;
        let value = parse_frame(buf, backing, positions, state, depth + 1)?;
        entries.push((key, value));
    }

    Ok(RespFrame::Map(entries))
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn parse_set_frame(
    buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let (count, line_end) = parse_number_line(buf, positions, state, start + 1)?;
    if count < 0 {
        return Err(ParseError::InvalidFrame);
    }

    let count = usize::try_from(count).map_err(|_| ParseError::FrameTooLarge)?;
    if count > MAX_ARRAY_ELEMENTS {
        return Err(ParseError::FrameTooLarge);
    }

    state.offset = line_end + 2;
    let mut frames = Vec::with_capacity(count);
    for _ in 0..count {
        frames.push(parse_frame(buf, backing, positions, state, depth + 1)?);
    }

    Ok(RespFrame::Set(frames))
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn parse_push_frame(
    buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let (count, line_end) = parse_number_line(buf, positions, state, start + 1)?;
    if count <= 0 {
        return Err(ParseError::InvalidFrame);
    }

    let count = usize::try_from(count).map_err(|_| ParseError::FrameTooLarge)?;
    if count > MAX_ARRAY_ELEMENTS {
        return Err(ParseError::FrameTooLarge);
    }

    state.offset = line_end + 2;
    let kind_frame = parse_frame(buf, backing, positions, state, depth + 1)?;
    let kind = match kind_frame {
        RespFrame::BulkString(Some(bytes)) | RespFrame::SimpleString(bytes) => bytes,
        _ => return Err(ParseError::InvalidFrame),
    };

    let mut data = Vec::with_capacity(count - 1);
    for _ in 1..count {
        data.push(parse_frame(buf, backing, positions, state, depth + 1)?);
    }

    Ok(RespFrame::Push { kind, data })
}

#[cfg(feature = "resp3")]
#[inline(always)]
fn parse_attribute_frame(
    buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let (count, line_end) = parse_number_line(buf, positions, state, start + 1)?;
    if count < 0 {
        return Err(ParseError::InvalidFrame);
    }

    let count = usize::try_from(count).map_err(|_| ParseError::FrameTooLarge)?;
    if count > MAX_ARRAY_ELEMENTS {
        return Err(ParseError::FrameTooLarge);
    }

    state.offset = line_end + 2;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key = parse_frame(buf, backing, positions, state, depth + 1)?;
        let value = parse_frame(buf, backing, positions, state, depth + 1)?;
        entries.push((key, value));
    }

    let data = Box::new(parse_frame(buf, backing, positions, state, depth + 1)?);
    Ok(RespFrame::Attribute { entries, data })
}

#[inline(always)]
fn parse_inline_frame(
    buf: &[u8],
    backing: &Bytes,
    positions: &CrlfPositions,
    state: &mut ParserState,
    _depth: u8,
) -> ParseResult<RespFrame> {
    let start = state.offset;
    let line_end = consume_next_crlf(positions, state, start)?;
    let mut cursor = start;
    let mut frames = Vec::with_capacity(4);

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
        frames.push(RespFrame::BulkString(Some(backing.slice(part_start..cursor))));
    }

    if frames.is_empty() {
        return Err(ParseError::InvalidFrame);
    }

    state.offset = line_end + 2;
    Ok(RespFrame::Array(Some(frames)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_pipeline_loop(buf: &[u8]) -> Result<(Vec<RespFrame>, usize), ParseError> {
        let mut frames = Vec::new();
        let mut consumed = 0;

        while consumed < buf.len() {
            match RespParser::parse(&buf[consumed..]) {
                Ok((frame, used)) => {
                    frames.push(frame);
                    consumed += used;
                }
                Err(ParseError::NeedMoreData) => break,
                Err(error) if frames.is_empty() => return Err(error),
                Err(_) => break,
            }
        }

        if frames.is_empty() {
            Err(ParseError::NeedMoreData)
        } else {
            Ok((frames, consumed))
        }
    }

    fn ping_pipeline(count: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        for _ in 0..count {
            buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        }
        buf
    }

    fn nested_array(depth: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        for _ in 0..depth {
            buf.extend_from_slice(b"*1\r\n");
        }
        buf.extend_from_slice(b":1\r\n");
        buf
    }

    #[test]
    fn parse_simple_string() {
        let buf = b"+OK\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        assert_eq!(frame, RespFrame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn parse_error() {
        let buf = b"-ERR unknown command\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        assert_eq!(
            frame,
            RespFrame::Error(Bytes::from_static(b"ERR unknown command"))
        );
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn parse_integer() {
        let buf = b":42\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        assert_eq!(frame, RespFrame::Integer(42));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn parse_negative_integer() {
        let buf = b":-100\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        assert_eq!(frame, RespFrame::Integer(-100));
        assert_eq!(consumed, 7);
    }

    #[test]
    fn parse_rejects_invalid_integer_line() {
        assert_eq!(RespParser::parse(b":12a\r\n"), Err(ParseError::InvalidFrame));
    }

    #[test]
    fn parse_bulk_string() {
        let buf = b"$5\r\nhello\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        assert_eq!(
            frame,
            RespFrame::BulkString(Some(Bytes::from_static(b"hello")))
        );
        assert_eq!(consumed, 11);
    }

    #[test]
    fn parse_null_bulk_string() {
        let buf = b"$-1\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        assert_eq!(frame, RespFrame::BulkString(None));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn parse_empty_bulk_string() {
        let buf = b"$0\r\n\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        assert_eq!(frame, RespFrame::BulkString(Some(Bytes::from_static(b""))));
        assert_eq!(consumed, 6);
    }

    #[test]
    fn parse_rejects_invalid_bulk_length() {
        assert_eq!(RespParser::parse(b"$2a\r\nhi\r\n"), Err(ParseError::InvalidFrame));
    }

    #[test]
    fn parse_array() {
        let buf = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        match frame {
            RespFrame::Array(Some(ref frames)) => {
                assert_eq!(frames.len(), 2);
            }
            _ => panic!("Expected array"),
        }
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn parse_null_array() {
        let buf = b"*-1\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        assert_eq!(frame, RespFrame::Array(None));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn parse_nested_array() {
        let buf = b"*1\r\n*2\r\n:1\r\n:2\r\n";
        let (frame, _) = RespParser::parse(buf).unwrap();
        match frame {
            RespFrame::Array(Some(ref outer)) => {
                assert_eq!(outer.len(), 1);
                match &outer[0] {
                    RespFrame::Array(Some(inner)) => assert_eq!(inner.len(), 2),
                    _ => panic!("Expected inner array"),
                }
            }
            _ => panic!("Expected outer array"),
        }
    }

    #[test]
    fn parse_inline_command() {
        let buf = b"PING\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        match frame {
            RespFrame::Array(Some(ref frames)) => {
                assert_eq!(frames.len(), 1);
            }
            _ => panic!("Expected array from inline"),
        }
        assert_eq!(consumed, 6);
    }

    #[test]
    fn incomplete_data() {
        assert_eq!(RespParser::parse(b"+OK\r"), Err(ParseError::NeedMoreData));
        assert_eq!(RespParser::parse(b"$5\r\nhel"), Err(ParseError::NeedMoreData));
        assert_eq!(RespParser::parse(b""), Err(ParseError::NeedMoreData));
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_resp3_null() {
        let buf = b"_\r\n";
        let (frame, consumed) = RespParser::parse(buf).unwrap();
        assert_eq!(frame, RespFrame::Null);
        assert_eq!(consumed, 3);
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_resp3_boolean() {
        let (frame, _) = RespParser::parse(b"#t\r\n").unwrap();
        assert_eq!(frame, RespFrame::Boolean(true));

        let (frame, _) = RespParser::parse(b"#f\r\n").unwrap();
        assert_eq!(frame, RespFrame::Boolean(false));
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_resp3_double() {
        let (frame, _) = RespParser::parse(b",2.72\r\n").unwrap();
        assert_eq!(frame, RespFrame::Double(2.72));
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_resp3_big_number() {
        let (frame, consumed) = RespParser::parse(b"(3492890328409238509324850943850943825024385\r\n").unwrap();
        assert_eq!(
            frame,
            RespFrame::BigNumber(Bytes::from_static(b"3492890328409238509324850943850943825024385"))
        );
        assert_eq!(consumed, 46);
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_resp3_bulk_error() {
        let (frame, _) = RespParser::parse(b"!11\r\nERR failure\r\n").unwrap();
        assert_eq!(frame, RespFrame::BulkError(Bytes::from_static(b"ERR failure")));
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_resp3_verbatim_string() {
        let (frame, _) = RespParser::parse(b"=15\r\ntxt:hello world\r\n").unwrap();
        assert_eq!(
            frame,
            RespFrame::VerbatimString {
                encoding: *b"txt",
                data: Bytes::from_static(b"hello world"),
            }
        );
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_resp3_map() {
        let (frame, _) = RespParser::parse(b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n").unwrap();
        assert_eq!(
            frame,
            RespFrame::Map(vec![
                (
                    RespFrame::SimpleString(Bytes::from_static(b"first")),
                    RespFrame::Integer(1),
                ),
                (
                    RespFrame::SimpleString(Bytes::from_static(b"second")),
                    RespFrame::Integer(2),
                ),
            ])
        );
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_resp3_set() {
        let (frame, _) = RespParser::parse(b"~2\r\n+orange\r\n+blue\r\n").unwrap();
        assert_eq!(
            frame,
            RespFrame::Set(vec![
                RespFrame::SimpleString(Bytes::from_static(b"orange")),
                RespFrame::SimpleString(Bytes::from_static(b"blue")),
            ])
        );
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_resp3_push() {
        let (frame, _) = RespParser::parse(b">2\r\n+message\r\n+payload\r\n").unwrap();
        assert_eq!(
            frame,
            RespFrame::Push {
                kind: Bytes::from_static(b"message"),
                data: vec![RespFrame::SimpleString(Bytes::from_static(b"payload"))],
            }
        );
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_resp3_attribute() {
        let (frame, _) = RespParser::parse(b"|1\r\n+meta\r\n+value\r\n:1\r\n").unwrap();
        assert_eq!(
            frame,
            RespFrame::Attribute {
                entries: vec![(
                    RespFrame::SimpleString(Bytes::from_static(b"meta")),
                    RespFrame::SimpleString(Bytes::from_static(b"value")),
                )],
                data: Box::new(RespFrame::Integer(1)),
            }
        );
    }

    #[test]
    fn parse_pipeline_multiple_frames() {
        let buf = ping_pipeline(5);
        let (frames, consumed) = RespParser::parse_pipeline(&buf).unwrap();
        let (loop_frames, loop_consumed) = parse_pipeline_loop(&buf).unwrap();

        assert_eq!(frames.len(), 5);
        assert_eq!(consumed, buf.len());
        assert_eq!(frames, loop_frames);
        assert_eq!(consumed, loop_consumed);
    }

    #[test]
    fn parse_pipeline_hundreds_of_frames() {
        let buf = ping_pipeline(100);
        let (frames, consumed) = RespParser::parse_pipeline(&buf).unwrap();
        assert_eq!(frames.len(), 100);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn parse_pipeline_returns_complete_prefix_before_partial_tail() {
        let mut buf = ping_pipeline(1);
        buf.extend_from_slice(b"*1\r\n$4\r\nPIN");

        let (frames, consumed) = RespParser::parse_pipeline(&buf).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(consumed, 14);
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn parse_pipeline_handles_mixed_frame_types() {
        let buf = b"+OK\r\n:1\r\n#t\r\n$5\r\nhello\r\n";
        let (frames, consumed) = RespParser::parse_pipeline(buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(
            frames,
            vec![
                RespFrame::SimpleString(Bytes::from_static(b"OK")),
                RespFrame::Integer(1),
                RespFrame::Boolean(true),
                RespFrame::BulkString(Some(Bytes::from_static(b"hello"))),
            ]
        );
    }

    #[test]
    fn parse_pipeline_stops_before_invalid_suffix() {
        let mut buf = ping_pipeline(1);
        buf.extend_from_slice(b":12a\r\n");

        let (frames, consumed) = RespParser::parse_pipeline(&buf).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(consumed, 14);
        assert_eq!(RespParser::parse(&buf[consumed..]), Err(ParseError::InvalidFrame));
    }

    #[test]
    fn parse_nesting_too_deep() {
        let buf = nested_array(33);
        assert_eq!(RespParser::parse(&buf), Err(ParseError::NestingTooDeep));
    }

    #[test]
    fn parse_bulk_string_too_large() {
        let buf = format!("${}\r\n", MAX_BULK_STRING_LEN + 1);
        assert_eq!(RespParser::parse(buf.as_bytes()), Err(ParseError::FrameTooLarge));
    }

    #[test]
    fn materialized_frames_share_one_backing_snapshot() {
        let buf = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let backing = Bytes::copy_from_slice(buf);
        let positions = scan_crlf(backing.as_ref());
        let mut state = ParserState::default();
        let frame = parse_frame(backing.as_ref(), &backing, &positions, &mut state, 0).unwrap();

        let RespFrame::Array(Some(frames)) = frame else {
            panic!("expected array frame");
        };

        let first = frames[0].as_bytes().unwrap();
        let second = frames[1].as_bytes().unwrap();
        let base = backing.as_ptr() as usize;

        assert_eq!(first.as_ptr() as usize - base, 8);
        assert_eq!(second.as_ptr() as usize - base, 17);
    }

    #[test]
    fn reject_oversized_array_count() {
        // Regression tests for fuzzer crash inputs that caused OOM via
        // Vec::with_capacity with absurdly large counts.
        let cases: &[&[u8]] = &[
            b"*2422222211032835\r\n\n",
            b"*7370955161\r\n\r\r\r",
            b"*222000064541065670\r\n-2425\r\n\r\n",
            b"*10000007370955\r\n*0\r\n",
        ];
        for input in cases {
            assert!(matches!(
                RespParser::parse(input),
                Err(ParseError::FrameTooLarge | ParseError::InvalidFrame)
            ));
        }
    }
}
