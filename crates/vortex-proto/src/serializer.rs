use bytes::{BufMut, BytesMut};

use crate::frame::RespFrame;

/// RESP response serializer.
///
/// Encodes `RespFrame` values into wire-format bytes. Includes pre-computed
/// common responses for maximum throughput.
pub struct RespSerializer;

/// Pre-computed common RESP responses.
static RESP_OK: &[u8] = b"+OK\r\n";
static RESP_QUEUED: &[u8] = b"+QUEUED\r\n";
static RESP_NIL: &[u8] = b"$-1\r\n";
static RESP_EMPTY_ARRAY: &[u8] = b"*0\r\n";
static RESP_ZERO: &[u8] = b":0\r\n";
static RESP_ONE: &[u8] = b":1\r\n";

impl RespSerializer {
    /// Serializes a `RespFrame` into the provided buffer.
    pub fn serialize(frame: &RespFrame, buf: &mut BytesMut) {
        match frame {
            RespFrame::SimpleString(s) => {
                if s.as_ref() == b"OK" {
                    buf.extend_from_slice(RESP_OK);
                } else if s.as_ref() == b"QUEUED" {
                    buf.extend_from_slice(RESP_QUEUED);
                } else {
                    buf.put_u8(b'+');
                    buf.extend_from_slice(s);
                    buf.extend_from_slice(b"\r\n");
                }
            }
            RespFrame::Error(e) => {
                buf.put_u8(b'-');
                buf.extend_from_slice(e);
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::Integer(n) => {
                if *n == 0 {
                    buf.extend_from_slice(RESP_ZERO);
                } else if *n == 1 {
                    buf.extend_from_slice(RESP_ONE);
                } else {
                    buf.put_u8(b':');
                    Self::write_integer(*n, buf);
                    buf.extend_from_slice(b"\r\n");
                }
            }
            RespFrame::BulkString(None) => {
                buf.extend_from_slice(RESP_NIL);
            }
            RespFrame::BulkString(Some(data)) => {
                buf.put_u8(b'$');
                Self::write_integer(data.len() as i64, buf);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::Array(None) => {
                buf.extend_from_slice(RESP_NIL);
            }
            RespFrame::Array(Some(frames)) => {
                if frames.is_empty() {
                    buf.extend_from_slice(RESP_EMPTY_ARRAY);
                } else {
                    buf.put_u8(b'*');
                    Self::write_integer(frames.len() as i64, buf);
                    buf.extend_from_slice(b"\r\n");
                    for f in frames {
                        Self::serialize(f, buf);
                    }
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Null => {
                buf.extend_from_slice(b"_\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::Boolean(b) => {
                if *b {
                    buf.extend_from_slice(b"#t\r\n");
                } else {
                    buf.extend_from_slice(b"#f\r\n");
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Double(d) => {
                buf.put_u8(b',');
                let s = format!("{d}");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::BigNumber(n) => {
                buf.put_u8(b'(');
                buf.extend_from_slice(n);
                buf.extend_from_slice(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::BulkError(e) => {
                buf.put_u8(b'!');
                Self::write_integer(e.len() as i64, buf);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(e);
                buf.extend_from_slice(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::VerbatimString { encoding, data } => {
                let total = 3 + 1 + data.len(); // encoding + ':' + data
                buf.put_u8(b'=');
                Self::write_integer(total as i64, buf);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(encoding);
                buf.put_u8(b':');
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::Map(entries) => {
                buf.put_u8(b'%');
                Self::write_integer(entries.len() as i64, buf);
                buf.extend_from_slice(b"\r\n");
                for (k, v) in entries {
                    Self::serialize(k, buf);
                    Self::serialize(v, buf);
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Set(frames) => {
                buf.put_u8(b'~');
                Self::write_integer(frames.len() as i64, buf);
                buf.extend_from_slice(b"\r\n");
                for f in frames {
                    Self::serialize(f, buf);
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Attribute { entries, data } => {
                buf.put_u8(b'|');
                Self::write_integer(entries.len() as i64, buf);
                buf.extend_from_slice(b"\r\n");
                for (k, v) in entries {
                    Self::serialize(k, buf);
                    Self::serialize(v, buf);
                }
                Self::serialize(data, buf);
            }
            #[cfg(feature = "resp3")]
            RespFrame::Push { kind, data } => {
                buf.put_u8(b'>');
                Self::write_integer((1 + data.len()) as i64, buf);
                buf.extend_from_slice(b"\r\n");
                // kind as bulk string
                buf.put_u8(b'$');
                Self::write_integer(kind.len() as i64, buf);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(kind);
                buf.extend_from_slice(b"\r\n");
                for f in data {
                    Self::serialize(f, buf);
                }
            }
        }
    }

    /// Writes an integer in ASCII to the buffer.
    fn write_integer(n: i64, buf: &mut BytesMut) {
        let mut itoa_buf = itoa::Buffer::new();
        let s = itoa_buf.format(n);
        buf.extend_from_slice(s.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn serialize(frame: &RespFrame) -> Vec<u8> {
        let mut buf = BytesMut::new();
        RespSerializer::serialize(frame, &mut buf);
        buf.to_vec()
    }

    #[test]
    fn serialize_ok() {
        let frame = RespFrame::SimpleString(Bytes::from_static(b"OK"));
        assert_eq!(serialize(&frame), b"+OK\r\n");
    }

    #[test]
    fn serialize_error() {
        let frame = RespFrame::Error(Bytes::from_static(b"ERR bad"));
        assert_eq!(serialize(&frame), b"-ERR bad\r\n");
    }

    #[test]
    fn serialize_integer() {
        assert_eq!(serialize(&RespFrame::Integer(0)), b":0\r\n");
        assert_eq!(serialize(&RespFrame::Integer(1)), b":1\r\n");
        assert_eq!(serialize(&RespFrame::Integer(42)), b":42\r\n");
        assert_eq!(serialize(&RespFrame::Integer(-1)), b":-1\r\n");
    }

    #[test]
    fn serialize_bulk_string() {
        let frame = RespFrame::BulkString(Some(Bytes::from_static(b"hello")));
        assert_eq!(serialize(&frame), b"$5\r\nhello\r\n");
    }

    #[test]
    fn serialize_null() {
        assert_eq!(serialize(&RespFrame::BulkString(None)), b"$-1\r\n");
    }

    #[test]
    fn serialize_array() {
        let frame = RespFrame::Array(Some(vec![RespFrame::Integer(1), RespFrame::Integer(2)]));
        assert_eq!(serialize(&frame), b"*2\r\n:1\r\n:2\r\n");
    }

    #[test]
    fn roundtrip() {
        use crate::parser::RespParser;

        let original = RespFrame::Array(Some(vec![
            RespFrame::BulkString(Some(Bytes::from_static(b"SET"))),
            RespFrame::BulkString(Some(Bytes::from_static(b"key"))),
            RespFrame::BulkString(Some(Bytes::from_static(b"value"))),
        ]));

        let mut buf = BytesMut::new();
        RespSerializer::serialize(&original, &mut buf);

        let (parsed, _) = RespParser::parse(&buf).unwrap();
        assert_eq!(parsed, original);
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn roundtrip_attribute() {
        use crate::parser::RespParser;

        let original = RespFrame::Attribute {
            entries: vec![(
                RespFrame::SimpleString(Bytes::from_static(b"meta")),
                RespFrame::SimpleString(Bytes::from_static(b"value")),
            )],
            data: Box::new(RespFrame::Integer(1)),
        };

        let mut buf = BytesMut::new();
        RespSerializer::serialize(&original, &mut buf);

        let (parsed, _) = RespParser::parse(&buf).unwrap();
        assert_eq!(parsed, original);
    }
}
