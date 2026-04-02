use bytes::Bytes;

/// Full RESP2/RESP3 frame type.
///
/// Covers all frame types defined in the RESP3 specification.
/// RESP2 types are a subset (SimpleString, Error, Integer, BulkString, Array).
#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    /// `+OK\r\n` — Simple string (no newlines).
    SimpleString(Bytes),

    /// `-ERR message\r\n` — Error message.
    Error(Bytes),

    /// `:42\r\n` — 64-bit signed integer.
    Integer(i64),

    /// `$5\r\nhello\r\n` — Bulk string. `None` = null bulk string (`$-1\r\n`).
    BulkString(Option<Bytes>),

    /// `*2\r\n...` — Array of frames. `None` = null array (`*-1\r\n`).
    Array(Option<Vec<RespFrame>>),

    /// `_\r\n` — RESP3 null type.
    #[cfg(feature = "resp3")]
    Null,

    /// `#t\r\n` / `#f\r\n` — RESP3 boolean.
    #[cfg(feature = "resp3")]
    Boolean(bool),

    /// `,1.23\r\n` — RESP3 double.
    #[cfg(feature = "resp3")]
    Double(f64),

    /// `(3492890328409238509324850943850943825024385\r\n` — RESP3 big number.
    #[cfg(feature = "resp3")]
    BigNumber(Bytes),

    /// `!<len>\r\n<error>\r\n` — RESP3 bulk error.
    #[cfg(feature = "resp3")]
    BulkError(Bytes),

    /// `=<len>\r\ntxt:<data>\r\n` — RESP3 verbatim string.
    #[cfg(feature = "resp3")]
    VerbatimString { encoding: [u8; 3], data: Bytes },

    /// `%<count>\r\n...` — RESP3 map.
    #[cfg(feature = "resp3")]
    Map(Vec<(RespFrame, RespFrame)>),

    /// `~<count>\r\n...` — RESP3 set.
    #[cfg(feature = "resp3")]
    Set(Vec<RespFrame>),

    /// `|<count>\r\n...<frame>` — RESP3 attributes attached to the next frame.
    #[cfg(feature = "resp3")]
    Attribute {
        entries: Vec<(RespFrame, RespFrame)>,
        data: Box<RespFrame>,
    },

    /// `><count>\r\n...` — RESP3 push (pub/sub, invalidation).
    #[cfg(feature = "resp3")]
    Push { kind: Bytes, data: Vec<RespFrame> },
}

impl RespFrame {
    /// Returns `true` if this frame represents a null value.
    pub fn is_null(&self) -> bool {
        match self {
            Self::BulkString(None) | Self::Array(None) => true,
            #[cfg(feature = "resp3")]
            Self::Null => true,
            _ => false,
        }
    }

    /// Convenience: creates a simple string frame.
    pub fn simple_string(s: impl Into<Bytes>) -> Self {
        Self::SimpleString(s.into())
    }

    /// Convenience: creates an error frame.
    pub fn error(s: impl Into<Bytes>) -> Self {
        Self::Error(s.into())
    }

    /// Convenience: creates a bulk string frame.
    pub fn bulk_string(s: impl Into<Bytes>) -> Self {
        Self::BulkString(Some(s.into()))
    }

    /// Convenience: creates a null bulk string.
    pub fn null_bulk_string() -> Self {
        Self::BulkString(None)
    }

    /// Convenience: creates an integer frame.
    pub fn integer(n: i64) -> Self {
        Self::Integer(n)
    }

    /// Returns the frame payload as bytes when the frame is byte-backed.
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Self::SimpleString(bytes) | Self::Error(bytes) => Some(bytes),
            Self::BulkString(Some(bytes)) => Some(bytes),
            #[cfg(feature = "resp3")]
            Self::BigNumber(bytes) | Self::BulkError(bytes) => Some(bytes),
            #[cfg(feature = "resp3")]
            Self::VerbatimString { data, .. } => Some(data),
            _ => None,
        }
    }

    /// Returns the command name when this frame is a command array.
    pub fn command_name(&self) -> Option<&Bytes> {
        match self {
            Self::Array(Some(frames)) if !frames.is_empty() => frames[0].as_bytes(),
            #[cfg(feature = "resp3")]
            Self::Attribute { data, .. } => data.command_name(),
            _ => None,
        }
    }

    /// Pre-computed OK response.
    pub fn ok() -> Self {
        Self::simple_string_ok()
    }

    fn simple_string_ok() -> Self {
        Self::SimpleString(Bytes::from_static(b"OK"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_checks() {
        assert!(RespFrame::null_bulk_string().is_null());
        assert!(RespFrame::Array(None).is_null());
        assert!(!RespFrame::integer(42).is_null());
    }

    #[test]
    fn frame_constructors() {
        let ok = RespFrame::simple_string("OK");
        assert!(matches!(ok, RespFrame::SimpleString(_)));

        let err = RespFrame::error("ERR bad");
        assert!(matches!(err, RespFrame::Error(_)));

        let bulk = RespFrame::bulk_string("hello");
        assert!(matches!(bulk, RespFrame::BulkString(Some(_))));
    }

    #[test]
    fn command_name_extraction() {
        let frame = RespFrame::Array(Some(vec![RespFrame::bulk_string("PING")]));
        assert_eq!(frame.command_name(), Some(&Bytes::from_static(b"PING")));
    }
}
