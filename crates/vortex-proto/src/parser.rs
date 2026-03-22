use bytes::Bytes;

use crate::frame::RespFrame;

/// Indicates that more data is needed to complete a parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeedMoreData;

/// Scalar RESP2/RESP3 parser.
///
/// Phase 0: byte-by-byte scanning for `\r\n`.
/// TODO: Phase 2: SIMD-accelerated CRLF scanning via `_mm256_cmpeq_epi8`.
///
/// The parser is stateless — call `parse()` with a complete or partial
/// buffer and it returns either a completed frame + bytes consumed, or
/// `NeedMoreData`.
pub struct RespParser;

impl RespParser {
    /// Attempts to parse a single RESP frame from the buffer.
    ///
    /// Returns `Ok((frame, bytes_consumed))` on success, or
    /// `Err(NeedMoreData)` if the buffer doesn't contain a complete frame.
    pub fn parse(buf: &[u8]) -> Result<(RespFrame, usize), NeedMoreData> {
        if buf.is_empty() {
            return Err(NeedMoreData);
        }

        match buf[0] {
            b'+' => Self::parse_simple_string(buf),
            b'-' => Self::parse_error(buf),
            b':' => Self::parse_integer(buf),
            b'$' => Self::parse_bulk_string(buf),
            b'*' => Self::parse_array(buf),
            #[cfg(feature = "resp3")]
            b'_' => Self::parse_null(buf),
            #[cfg(feature = "resp3")]
            b'#' => Self::parse_boolean(buf),
            #[cfg(feature = "resp3")]
            b',' => Self::parse_double(buf),
            _ => {
                // Inline command support (Redis compatibility)
                Self::parse_inline(buf)
            }
        }
    }

    /// Finds the next `\r\n` in the buffer, starting from `offset`.
    fn find_crlf(buf: &[u8], offset: usize) -> Option<usize> {
        // Phase 0: scalar scan. Phase 2 replaces with SIMD.
        let search = &buf[offset..];
        for i in 0..search.len().saturating_sub(1) {
            if search[i] == b'\r' && search[i + 1] == b'\n' {
                return Some(offset + i);
            }
        }
        None
    }

    /// Parses a length/integer from the buffer line (after type byte, before CRLF).
    fn parse_length(buf: &[u8], start: usize, end: usize) -> Option<i64> {
        let slice = &buf[start..end];
        let s = std::str::from_utf8(slice).ok()?;
        s.parse::<i64>().ok()
    }

    fn parse_simple_string(buf: &[u8]) -> Result<(RespFrame, usize), NeedMoreData> {
        let crlf = Self::find_crlf(buf, 1).ok_or(NeedMoreData)?;
        let data = Bytes::copy_from_slice(&buf[1..crlf]);
        Ok((RespFrame::SimpleString(data), crlf + 2))
    }

    fn parse_error(buf: &[u8]) -> Result<(RespFrame, usize), NeedMoreData> {
        let crlf = Self::find_crlf(buf, 1).ok_or(NeedMoreData)?;
        let data = Bytes::copy_from_slice(&buf[1..crlf]);
        Ok((RespFrame::Error(data), crlf + 2))
    }

    fn parse_integer(buf: &[u8]) -> Result<(RespFrame, usize), NeedMoreData> {
        let crlf = Self::find_crlf(buf, 1).ok_or(NeedMoreData)?;
        let n = Self::parse_length(buf, 1, crlf)
            .ok_or(NeedMoreData)?;
        Ok((RespFrame::Integer(n), crlf + 2))
    }

    fn parse_bulk_string(buf: &[u8]) -> Result<(RespFrame, usize), NeedMoreData> {
        let crlf = Self::find_crlf(buf, 1).ok_or(NeedMoreData)?;
        let len = Self::parse_length(buf, 1, crlf).ok_or(NeedMoreData)?;

        if len < 0 {
            return Ok((RespFrame::BulkString(None), crlf + 2));
        }

        let len = len as usize;
        let data_start = crlf + 2;
        let data_end = data_start + len;

        if buf.len() < data_end + 2 {
            return Err(NeedMoreData);
        }

        let data = Bytes::copy_from_slice(&buf[data_start..data_end]);
        Ok((RespFrame::BulkString(Some(data)), data_end + 2))
    }

    fn parse_array(buf: &[u8]) -> Result<(RespFrame, usize), NeedMoreData> {
        let crlf = Self::find_crlf(buf, 1).ok_or(NeedMoreData)?;
        let count = Self::parse_length(buf, 1, crlf).ok_or(NeedMoreData)?;

        if count < 0 {
            return Ok((RespFrame::Array(None), crlf + 2));
        }

        let count = count as usize;
        let mut offset = crlf + 2;
        let mut frames = Vec::with_capacity(count);

        for _ in 0..count {
            let (frame, consumed) = Self::parse(&buf[offset..])?;
            frames.push(frame);
            offset += consumed;
        }

        Ok((RespFrame::Array(Some(frames)), offset))
    }

    #[cfg(feature = "resp3")]
    fn parse_null(buf: &[u8]) -> Result<(RespFrame, usize), NeedMoreData> {
        let crlf = Self::find_crlf(buf, 1).ok_or(NeedMoreData)?;
        Ok((RespFrame::Null, crlf + 2))
    }

    #[cfg(feature = "resp3")]
    fn parse_boolean(buf: &[u8]) -> Result<(RespFrame, usize), NeedMoreData> {
        let crlf = Self::find_crlf(buf, 1).ok_or(NeedMoreData)?;
        if crlf < 2 {
            return Err(NeedMoreData);
        }
        let val = match buf[1] {
            b't' => true,
            b'f' => false,
            _ => return Err(NeedMoreData),
        };
        Ok((RespFrame::Boolean(val), crlf + 2))
    }

    #[cfg(feature = "resp3")]
    fn parse_double(buf: &[u8]) -> Result<(RespFrame, usize), NeedMoreData> {
        let crlf = Self::find_crlf(buf, 1).ok_or(NeedMoreData)?;
        let s = std::str::from_utf8(&buf[1..crlf]).map_err(|_| NeedMoreData)?;
        let val: f64 = s.parse().map_err(|_| NeedMoreData)?;
        Ok((RespFrame::Double(val), crlf + 2))
    }

    /// Parse inline commands (e.g., `PING\r\n` without RESP framing).
    fn parse_inline(buf: &[u8]) -> Result<(RespFrame, usize), NeedMoreData> {
        let crlf = Self::find_crlf(buf, 0).ok_or(NeedMoreData)?;
        let line = &buf[..crlf];

        let parts: Vec<&[u8]> = line
            .split(|&b| b == b' ')
            .filter(|s| !s.is_empty())
            .collect();

        if parts.is_empty() {
            return Err(NeedMoreData);
        }

        let frames: Vec<RespFrame> = parts
            .into_iter()
            .map(|p| RespFrame::BulkString(Some(Bytes::copy_from_slice(p))))
            .collect();

        Ok((RespFrame::Array(Some(frames)), crlf + 2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(
            frame,
            RespFrame::BulkString(Some(Bytes::from_static(b"")))
        );
        assert_eq!(consumed, 6);
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
        assert!(RespParser::parse(b"+OK\r").is_err());
        assert!(RespParser::parse(b"$5\r\nhel").is_err());
        assert!(RespParser::parse(b"").is_err());
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
        let (frame, _) = RespParser::parse(b",3.14\r\n").unwrap();
        assert_eq!(frame, RespFrame::Double(3.14));
    }
}
