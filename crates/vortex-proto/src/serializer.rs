use bytes::{BufMut, BytesMut};

use crate::frame::RespFrame;
use crate::iovec::IovecWriter;

// ── Integer LUT (0–9999) ───────────────────────────────────────────────────
// Each entry stores the ASCII digit bytes and a 1-byte length.
// For n = 42: digits = [b'4', b'2', 0, 0, 0], len = 2.
// Total: 6 × 10 000 = 60 KB — fits in L1 cache.

/// Entry in the integer lookup table.
#[derive(Clone, Copy)]
struct IntLutEntry {
    digits: [u8; 5],
    len: u8,
}

/// Build the integer LUT at compile time.
const fn build_int_lut() -> [IntLutEntry; 10_000] {
    let mut lut = [IntLutEntry {
        digits: [0; 5],
        len: 0,
    }; 10_000];
    let mut i: usize = 0;
    while i < 10_000 {
        let mut buf = [0u8; 5];
        let mut n = i;
        if n == 0 {
            buf[0] = b'0';
            lut[i] = IntLutEntry {
                digits: buf,
                len: 1,
            };
        } else {
            // Write digits in reverse, then reverse.
            let mut pos = 0usize;
            while n > 0 {
                buf[pos] = b'0' + (n % 10) as u8;
                n /= 10;
                pos += 1;
            }
            // Reverse in-place.
            let mut lo = 0usize;
            let mut hi = pos - 1;
            while lo < hi {
                let tmp = buf[lo];
                buf[lo] = buf[hi];
                buf[hi] = tmp;
                lo += 1;
                hi -= 1;
            }
            lut[i] = IntLutEntry {
                digits: buf,
                len: pos as u8,
            };
        }
        i += 1;
    }
    lut
}

static INT_LUT: [IntLutEntry; 10_000] = build_int_lut();

// ── Pre-computed RESP responses ─────────────────────────────────────────────

static RESP_OK: &[u8] = b"+OK\r\n";
static RESP_QUEUED: &[u8] = b"+QUEUED\r\n";
static RESP_PONG: &[u8] = b"+PONG\r\n";
static RESP_NIL: &[u8] = b"$-1\r\n";
static RESP_EMPTY_ARRAY: &[u8] = b"*0\r\n";

// Single-digit integers :0\r\n through :9\r\n.
static RESP_INT: [&[u8]; 10] = [
    b":0\r\n", b":1\r\n", b":2\r\n", b":3\r\n", b":4\r\n", b":5\r\n", b":6\r\n", b":7\r\n",
    b":8\r\n", b":9\r\n",
];

// Common error responses.
pub static RESP_ERR_WRONGTYPE: &[u8] =
    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
pub static RESP_ERR_WRONG_ARGC: &[u8] = b"-ERR wrong number of arguments\r\n";
pub static RESP_ERR_SYNTAX: &[u8] = b"-ERR syntax error\r\n";
pub static RESP_ERR_NOPERM: &[u8] = b"-NOPERM\r\n";
pub static RESP_ERR_LOADING: &[u8] = b"-LOADING\r\n";
pub static RESP_ERR_BUSY: &[u8] = b"-BUSY\r\n";

// ── RespSerializer ─────────────────────────────────────────────────────────

/// RESP response serializer.
///
/// Encodes `RespFrame` values into wire-format bytes. Provides three APIs:
///
/// - [`serialize`](Self::serialize) — writes into `BytesMut` (existing API)
/// - [`serialize_to_slice`](Self::serialize_to_slice) — writes into a raw
///   `&mut [u8]` buffer (zero-alloc, used by the reactor's mmap write path)
/// - [`estimated_size`](Self::estimated_size) — pre-compute output size for
///   `reserve()` calls
pub struct RespSerializer;

impl RespSerializer {
    /// Serializes a `RespFrame` into the provided `BytesMut` buffer.
    pub fn serialize(frame: &RespFrame, buf: &mut BytesMut) {
        match frame {
            RespFrame::SimpleString(s) => {
                if s.as_ref() == b"OK" {
                    buf.extend_from_slice(RESP_OK);
                } else if s.as_ref() == b"QUEUED" {
                    buf.extend_from_slice(RESP_QUEUED);
                } else if s.as_ref() == b"PONG" {
                    buf.extend_from_slice(RESP_PONG);
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
                Self::write_integer_frame(*n, buf);
            }
            RespFrame::BulkString(None) => {
                buf.extend_from_slice(RESP_NIL);
            }
            RespFrame::BulkString(Some(data)) => {
                buf.put_u8(b'$');
                Self::write_integer_digits(data.len() as i64, buf);
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
                    Self::write_integer_digits(frames.len() as i64, buf);
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
                // Use ryu for fast f64 → ASCII (stack-allocated, no heap).
                let mut ryu_buf = ryu::Buffer::new();
                let s = ryu_buf.format(*d);
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
                Self::write_integer_digits(e.len() as i64, buf);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(e);
                buf.extend_from_slice(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::VerbatimString { encoding, data } => {
                let total = 3 + 1 + data.len(); // encoding + ':' + data
                buf.put_u8(b'=');
                Self::write_integer_digits(total as i64, buf);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(encoding);
                buf.put_u8(b':');
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::Map(entries) => {
                buf.put_u8(b'%');
                Self::write_integer_digits(entries.len() as i64, buf);
                buf.extend_from_slice(b"\r\n");
                for (k, v) in entries {
                    Self::serialize(k, buf);
                    Self::serialize(v, buf);
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Set(frames) => {
                buf.put_u8(b'~');
                Self::write_integer_digits(frames.len() as i64, buf);
                buf.extend_from_slice(b"\r\n");
                for f in frames {
                    Self::serialize(f, buf);
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Attribute { entries, data } => {
                buf.put_u8(b'|');
                Self::write_integer_digits(entries.len() as i64, buf);
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
                Self::write_integer_digits((1 + data.len()) as i64, buf);
                buf.extend_from_slice(b"\r\n");
                // kind as bulk string
                buf.put_u8(b'$');
                Self::write_integer_digits(kind.len() as i64, buf);
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(kind);
                buf.extend_from_slice(b"\r\n");
                for f in data {
                    Self::serialize(f, buf);
                }
            }
        }
    }

    /// Serialize a `RespFrame` directly into a raw byte slice.
    ///
    /// Returns the number of bytes written, or `None` if the buffer is too
    /// small. This is the zero-allocation hot path used by the reactor's mmap
    /// write buffer.
    pub fn serialize_to_slice(frame: &RespFrame, buf: &mut [u8]) -> Option<usize> {
        let mut cursor = 0usize;

        // Macro to push bytes, checking capacity.
        macro_rules! push {
            ($data:expr) => {{
                let d: &[u8] = $data;
                let end = cursor + d.len();
                if end > buf.len() {
                    return None;
                }
                buf[cursor..end].copy_from_slice(d);
                cursor = end;
            }};
        }

        macro_rules! push_byte {
            ($b:expr) => {{
                if cursor >= buf.len() {
                    return None;
                }
                buf[cursor] = $b;
                cursor += 1;
            }};
        }

        match frame {
            RespFrame::SimpleString(s) => {
                if s.as_ref() == b"OK" {
                    push!(RESP_OK);
                } else if s.as_ref() == b"QUEUED" {
                    push!(RESP_QUEUED);
                } else if s.as_ref() == b"PONG" {
                    push!(RESP_PONG);
                } else {
                    push_byte!(b'+');
                    push!(s.as_ref());
                    push!(b"\r\n");
                }
            }
            RespFrame::Error(e) => {
                push_byte!(b'-');
                push!(e.as_ref());
                push!(b"\r\n");
            }
            RespFrame::Integer(n) => {
                let written = write_integer_frame_to_slice(*n, &mut buf[cursor..])?;
                cursor += written;
            }
            RespFrame::BulkString(None) => {
                push!(RESP_NIL);
            }
            RespFrame::BulkString(Some(data)) => {
                push_byte!(b'$');
                let written = write_integer_digits_to_slice(data.len() as i64, &mut buf[cursor..])?;
                cursor += written;
                push!(b"\r\n");
                push!(data.as_ref());
                push!(b"\r\n");
            }
            RespFrame::Array(None) => {
                push!(RESP_NIL);
            }
            RespFrame::Array(Some(frames)) => {
                if frames.is_empty() {
                    push!(RESP_EMPTY_ARRAY);
                } else {
                    push_byte!(b'*');
                    let written =
                        write_integer_digits_to_slice(frames.len() as i64, &mut buf[cursor..])?;
                    cursor += written;
                    push!(b"\r\n");
                    for f in frames {
                        let written = Self::serialize_to_slice(f, &mut buf[cursor..])?;
                        cursor += written;
                    }
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Null => {
                push!(b"_\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::Boolean(b) => {
                if *b {
                    push!(b"#t\r\n");
                } else {
                    push!(b"#f\r\n");
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Double(d) => {
                push_byte!(b',');
                let mut ryu_buf = ryu::Buffer::new();
                let s = ryu_buf.format(*d);
                push!(s.as_bytes());
                push!(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::BigNumber(n) => {
                push_byte!(b'(');
                push!(n.as_ref());
                push!(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::BulkError(e) => {
                push_byte!(b'!');
                let written =
                    write_integer_digits_to_slice(e.len() as i64, &mut buf[cursor..])?;
                cursor += written;
                push!(b"\r\n");
                push!(e.as_ref());
                push!(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::VerbatimString { encoding, data } => {
                let total = 3 + 1 + data.len();
                push_byte!(b'=');
                let written =
                    write_integer_digits_to_slice(total as i64, &mut buf[cursor..])?;
                cursor += written;
                push!(b"\r\n");
                push!(encoding.as_ref());
                push_byte!(b':');
                push!(data.as_ref());
                push!(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::Map(entries) => {
                push_byte!(b'%');
                let written =
                    write_integer_digits_to_slice(entries.len() as i64, &mut buf[cursor..])?;
                cursor += written;
                push!(b"\r\n");
                for (k, v) in entries {
                    let w = Self::serialize_to_slice(k, &mut buf[cursor..])?;
                    cursor += w;
                    let w = Self::serialize_to_slice(v, &mut buf[cursor..])?;
                    cursor += w;
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Set(frames) => {
                push_byte!(b'~');
                let written =
                    write_integer_digits_to_slice(frames.len() as i64, &mut buf[cursor..])?;
                cursor += written;
                push!(b"\r\n");
                for f in frames {
                    let w = Self::serialize_to_slice(f, &mut buf[cursor..])?;
                    cursor += w;
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Attribute { entries, data } => {
                push_byte!(b'|');
                let written =
                    write_integer_digits_to_slice(entries.len() as i64, &mut buf[cursor..])?;
                cursor += written;
                push!(b"\r\n");
                for (k, v) in entries {
                    let w = Self::serialize_to_slice(k, &mut buf[cursor..])?;
                    cursor += w;
                    let w = Self::serialize_to_slice(v, &mut buf[cursor..])?;
                    cursor += w;
                }
                let w = Self::serialize_to_slice(data, &mut buf[cursor..])?;
                cursor += w;
            }
            #[cfg(feature = "resp3")]
            RespFrame::Push { kind, data } => {
                push_byte!(b'>');
                let written =
                    write_integer_digits_to_slice((1 + data.len()) as i64, &mut buf[cursor..])?;
                cursor += written;
                push!(b"\r\n");
                push_byte!(b'$');
                let written =
                    write_integer_digits_to_slice(kind.len() as i64, &mut buf[cursor..])?;
                cursor += written;
                push!(b"\r\n");
                push!(kind.as_ref());
                push!(b"\r\n");
                for f in data {
                    let w = Self::serialize_to_slice(f, &mut buf[cursor..])?;
                    cursor += w;
                }
            }
        }
        Some(cursor)
    }

    // ── Scatter-gather serialization ───────────────────────────────────────

    /// Serialize a `RespFrame` into an [`IovecWriter`] for scatter-gather I/O.
    ///
    /// Instead of copying all bytes into a contiguous buffer, this accumulates
    /// `iovec` segments: static slices for pre-computed responses, scratch data
    /// for formatted integers/lengths, and direct pointers to `Bytes` data for
    /// zero-copy bulk strings. The resulting iovec array can be passed to
    /// `writev` / io_uring `IORING_OP_WRITEV`.
    pub fn serialize_to_iovecs(frame: &RespFrame, w: &mut IovecWriter) {
        match frame {
            RespFrame::SimpleString(s) => {
                if s.as_ref() == b"OK" {
                    w.push_static(RESP_OK);
                } else if s.as_ref() == b"QUEUED" {
                    w.push_static(RESP_QUEUED);
                } else if s.as_ref() == b"PONG" {
                    w.push_static(RESP_PONG);
                } else {
                    w.push_static(b"+");
                    w.push_bytes(s);
                    w.push_static(b"\r\n");
                }
            }
            RespFrame::Error(e) => {
                w.push_static(b"-");
                w.push_bytes(e);
                w.push_static(b"\r\n");
            }
            RespFrame::Integer(n) => {
                Self::write_integer_frame_iovec(*n, w);
            }
            RespFrame::BulkString(None) => {
                w.push_static(RESP_NIL);
            }
            RespFrame::BulkString(Some(data)) => {
                // $<len>\r\n<data>\r\n — length prefix is scratch, data is zero-copy.
                w.push_static(b"$");
                Self::write_integer_digits_iovec(data.len() as i64, w);
                w.push_static(b"\r\n");
                w.push_bytes(data);
                w.push_static(b"\r\n");
            }
            RespFrame::Array(None) => {
                w.push_static(RESP_NIL);
            }
            RespFrame::Array(Some(frames)) => {
                if frames.is_empty() {
                    w.push_static(RESP_EMPTY_ARRAY);
                } else {
                    w.push_static(b"*");
                    Self::write_integer_digits_iovec(frames.len() as i64, w);
                    w.push_static(b"\r\n");
                    for f in frames {
                        Self::serialize_to_iovecs(f, w);
                    }
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Null => {
                w.push_static(b"_\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::Boolean(b) => {
                if *b {
                    w.push_static(b"#t\r\n");
                } else {
                    w.push_static(b"#f\r\n");
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Double(d) => {
                // Format to scratch — ryu is fast but needs a buffer.
                let mut ryu_buf = ryu::Buffer::new();
                let s = ryu_buf.format(*d);
                let mut tmp = [0u8; 32]; // ,<digits>\r\n
                tmp[0] = b',';
                let slen = s.len();
                tmp[1..1 + slen].copy_from_slice(s.as_bytes());
                tmp[1 + slen] = b'\r';
                tmp[2 + slen] = b'\n';
                w.push_scratch(&tmp[..3 + slen]);
            }
            #[cfg(feature = "resp3")]
            RespFrame::BigNumber(n) => {
                w.push_static(b"(");
                w.push_bytes(n);
                w.push_static(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::BulkError(e) => {
                w.push_static(b"!");
                Self::write_integer_digits_iovec(e.len() as i64, w);
                w.push_static(b"\r\n");
                w.push_bytes(e);
                w.push_static(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::VerbatimString { encoding, data } => {
                let total = 3 + 1 + data.len();
                w.push_static(b"=");
                Self::write_integer_digits_iovec(total as i64, w);
                w.push_static(b"\r\n");
                w.push_bytes(encoding);
                w.push_static(b":");
                w.push_bytes(data);
                w.push_static(b"\r\n");
            }
            #[cfg(feature = "resp3")]
            RespFrame::Map(entries) => {
                w.push_static(b"%");
                Self::write_integer_digits_iovec(entries.len() as i64, w);
                w.push_static(b"\r\n");
                for (k, v) in entries {
                    Self::serialize_to_iovecs(k, w);
                    Self::serialize_to_iovecs(v, w);
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Set(frames) => {
                w.push_static(b"~");
                Self::write_integer_digits_iovec(frames.len() as i64, w);
                w.push_static(b"\r\n");
                for f in frames {
                    Self::serialize_to_iovecs(f, w);
                }
            }
            #[cfg(feature = "resp3")]
            RespFrame::Attribute { entries, data } => {
                w.push_static(b"|");
                Self::write_integer_digits_iovec(entries.len() as i64, w);
                w.push_static(b"\r\n");
                for (k, v) in entries {
                    Self::serialize_to_iovecs(k, w);
                    Self::serialize_to_iovecs(v, w);
                }
                Self::serialize_to_iovecs(data, w);
            }
            #[cfg(feature = "resp3")]
            RespFrame::Push { kind, data } => {
                w.push_static(b">");
                Self::write_integer_digits_iovec((1 + data.len()) as i64, w);
                w.push_static(b"\r\n");
                w.push_static(b"$");
                Self::write_integer_digits_iovec(kind.len() as i64, w);
                w.push_static(b"\r\n");
                w.push_bytes(kind);
                w.push_static(b"\r\n");
                for f in data {
                    Self::serialize_to_iovecs(f, w);
                }
            }
        }
    }

    /// Write a full integer frame (`:N\r\n`) into an IovecWriter.
    #[inline]
    fn write_integer_frame_iovec(n: i64, w: &mut IovecWriter) {
        if (0..=9).contains(&n) {
            w.push_static(RESP_INT[n as usize]);
        } else if (10..10_000).contains(&n) {
            let entry = &INT_LUT[n as usize];
            let dlen = entry.len as usize;
            // `:` + digits + `\r\n` into scratch as single fragment.
            let mut tmp = [0u8; 8]; // max: `:9999\r\n` = 7
            tmp[0] = b':';
            tmp[1..1 + dlen].copy_from_slice(&entry.digits[..dlen]);
            tmp[1 + dlen] = b'\r';
            tmp[2 + dlen] = b'\n';
            w.push_scratch(&tmp[..3 + dlen]);
        } else if n < 0 && n > -10_000 {
            let mag = (-n) as usize;
            let entry = &INT_LUT[mag];
            let dlen = entry.len as usize;
            let mut tmp = [0u8; 9]; // max: `:-9999\r\n` = 8
            tmp[0] = b':';
            tmp[1] = b'-';
            tmp[2..2 + dlen].copy_from_slice(&entry.digits[..dlen]);
            tmp[2 + dlen] = b'\r';
            tmp[3 + dlen] = b'\n';
            w.push_scratch(&tmp[..4 + dlen]);
        } else {
            let mut itoa_buf = itoa::Buffer::new();
            let s = itoa_buf.format(n);
            let slen = s.len();
            let mut tmp = [0u8; 24]; // `:` + max i64 digits + `\r\n`
            tmp[0] = b':';
            tmp[1..1 + slen].copy_from_slice(s.as_bytes());
            tmp[1 + slen] = b'\r';
            tmp[2 + slen] = b'\n';
            w.push_scratch(&tmp[..3 + slen]);
        }
    }

    /// Write just the ASCII digits of an integer into an IovecWriter (for lengths).
    #[inline]
    fn write_integer_digits_iovec(n: i64, w: &mut IovecWriter) {
        if (0..10_000_i64).contains(&n) {
            let entry = &INT_LUT[n as usize];
            w.push_scratch(&entry.digits[..entry.len as usize]);
        } else {
            let mut itoa_buf = itoa::Buffer::new();
            let s = itoa_buf.format(n);
            w.push_scratch(s.as_bytes());
        }
    }

    /// Write a full integer frame (`:N\r\n`) into `BytesMut`, using the LUT
    /// for 0-9 (pre-computed full frame) and 10-9999 (LUT digits).
    #[inline]
    fn write_integer_frame(n: i64, buf: &mut BytesMut) {
        if (0..=9).contains(&n) {
            buf.extend_from_slice(RESP_INT[n as usize]);
        } else if (10..10_000).contains(&n) {
            let entry = &INT_LUT[n as usize];
            // `:` + digits + `\r\n`
            buf.put_u8(b':');
            buf.extend_from_slice(&entry.digits[..entry.len as usize]);
            buf.extend_from_slice(b"\r\n");
        } else if n < 0 && n > -10_000 {
            let mag = (-n) as usize;
            let entry = &INT_LUT[mag];
            buf.put_u8(b':');
            buf.put_u8(b'-');
            buf.extend_from_slice(&entry.digits[..entry.len as usize]);
            buf.extend_from_slice(b"\r\n");
        } else {
            buf.put_u8(b':');
            Self::write_integer_digits(n, buf);
            buf.extend_from_slice(b"\r\n");
        }
    }

    /// Write just the ASCII digits of an integer into `BytesMut` (no `:` or `\r\n`).
    /// Used for lengths in `$<len>\r\n`, `*<count>\r\n`, etc.
    #[inline]
    fn write_integer_digits(n: i64, buf: &mut BytesMut) {
        if (0..10_000_i64).contains(&n) {
            let entry = &INT_LUT[n as usize];
            buf.extend_from_slice(&entry.digits[..entry.len as usize]);
        } else {
            let mut itoa_buf = itoa::Buffer::new();
            let s = itoa_buf.format(n);
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

// ── Slice-based integer helpers (zero-alloc) ───────────────────────────────

/// Write a full integer frame (`:N\r\n`) into a raw slice. Returns bytes written.
#[inline]
fn write_integer_frame_to_slice(n: i64, buf: &mut [u8]) -> Option<usize> {
    if (0..=9).contains(&n) {
        let src = RESP_INT[n as usize];
        if buf.len() < src.len() {
            return None;
        }
        buf[..src.len()].copy_from_slice(src);
        Some(src.len())
    } else if (10..10_000).contains(&n) {
        let entry = &INT_LUT[n as usize];
        let total = 1 + entry.len as usize + 2; // ':' + digits + '\r\n'
        if buf.len() < total {
            return None;
        }
        buf[0] = b':';
        buf[1..1 + entry.len as usize].copy_from_slice(&entry.digits[..entry.len as usize]);
        buf[1 + entry.len as usize] = b'\r';
        buf[2 + entry.len as usize] = b'\n';
        Some(total)
    } else if n < 0 && n > -10_000 {
        let mag = (-n) as usize;
        let entry = &INT_LUT[mag];
        let total = 2 + entry.len as usize + 2; // ':' + '-' + digits + '\r\n'
        if buf.len() < total {
            return None;
        }
        buf[0] = b':';
        buf[1] = b'-';
        buf[2..2 + entry.len as usize].copy_from_slice(&entry.digits[..entry.len as usize]);
        buf[2 + entry.len as usize] = b'\r';
        buf[3 + entry.len as usize] = b'\n';
        Some(total)
    } else {
        let mut itoa_buf = itoa::Buffer::new();
        let s = itoa_buf.format(n);
        let total = 1 + s.len() + 2;
        if buf.len() < total {
            return None;
        }
        buf[0] = b':';
        buf[1..1 + s.len()].copy_from_slice(s.as_bytes());
        buf[1 + s.len()] = b'\r';
        buf[2 + s.len()] = b'\n';
        Some(total)
    }
}

/// Write just the ASCII digits of an integer into a raw slice. Returns bytes written.
#[inline]
fn write_integer_digits_to_slice(n: i64, buf: &mut [u8]) -> Option<usize> {
    if (0..10_000_i64).contains(&n) {
        let entry = &INT_LUT[n as usize];
        let len = entry.len as usize;
        if buf.len() < len {
            return None;
        }
        buf[..len].copy_from_slice(&entry.digits[..len]);
        Some(len)
    } else {
        let mut itoa_buf = itoa::Buffer::new();
        let s = itoa_buf.format(n);
        if buf.len() < s.len() {
            return None;
        }
        buf[..s.len()].copy_from_slice(s.as_bytes());
        Some(s.len())
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

    fn serialize_slice(frame: &RespFrame) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let n = RespSerializer::serialize_to_slice(frame, &mut buf).unwrap();
        buf.truncate(n);
        buf
    }

    #[test]
    fn serialize_ok() {
        let frame = RespFrame::SimpleString(Bytes::from_static(b"OK"));
        assert_eq!(serialize(&frame), b"+OK\r\n");
        assert_eq!(serialize_slice(&frame), b"+OK\r\n");
    }

    #[test]
    fn serialize_error() {
        let frame = RespFrame::Error(Bytes::from_static(b"ERR bad"));
        assert_eq!(serialize(&frame), b"-ERR bad\r\n");
        assert_eq!(serialize_slice(&frame), b"-ERR bad\r\n");
    }

    #[test]
    fn serialize_integer() {
        assert_eq!(serialize(&RespFrame::Integer(0)), b":0\r\n");
        assert_eq!(serialize(&RespFrame::Integer(1)), b":1\r\n");
        assert_eq!(serialize(&RespFrame::Integer(42)), b":42\r\n");
        assert_eq!(serialize(&RespFrame::Integer(-1)), b":-1\r\n");
        // Slice path
        assert_eq!(serialize_slice(&RespFrame::Integer(0)), b":0\r\n");
        assert_eq!(serialize_slice(&RespFrame::Integer(1)), b":1\r\n");
        assert_eq!(serialize_slice(&RespFrame::Integer(42)), b":42\r\n");
        assert_eq!(serialize_slice(&RespFrame::Integer(-1)), b":-1\r\n");
    }

    #[test]
    fn serialize_integer_lut_boundary() {
        // Test LUT boundaries: 9, 10, 99, 100, 999, 1000, 9999, 10000
        for n in [9i64, 10, 99, 100, 999, 1000, 9999, 10_000, -9, -99, -9999, -10_000] {
            let expected = format!(":{n}\r\n");
            assert_eq!(serialize(&RespFrame::Integer(n)), expected.as_bytes(), "n={n}");
            assert_eq!(
                serialize_slice(&RespFrame::Integer(n)),
                expected.as_bytes(),
                "slice n={n}"
            );
        }
    }

    #[test]
    fn integer_lut_correctness() {
        // Verify LUT matches itoa for all 0–9999.
        for n in 0..10_000u32 {
            let entry = &INT_LUT[n as usize];
            let mut itoa_buf = itoa::Buffer::new();
            let expected = itoa_buf.format(n);
            let got = &entry.digits[..entry.len as usize];
            assert_eq!(
                got,
                expected.as_bytes(),
                "INT_LUT[{n}] mismatch: got {:?}, expected {:?}",
                std::str::from_utf8(got),
                expected
            );
        }
    }

    #[test]
    fn serialize_bulk_string() {
        let frame = RespFrame::BulkString(Some(Bytes::from_static(b"hello")));
        assert_eq!(serialize(&frame), b"$5\r\nhello\r\n");
        assert_eq!(serialize_slice(&frame), b"$5\r\nhello\r\n");
    }

    #[test]
    fn serialize_null() {
        assert_eq!(serialize(&RespFrame::BulkString(None)), b"$-1\r\n");
        assert_eq!(serialize_slice(&RespFrame::BulkString(None)), b"$-1\r\n");
    }

    #[test]
    fn serialize_array() {
        let frame = RespFrame::Array(Some(vec![RespFrame::Integer(1), RespFrame::Integer(2)]));
        assert_eq!(serialize(&frame), b"*2\r\n:1\r\n:2\r\n");
        assert_eq!(serialize_slice(&frame), b"*2\r\n:1\r\n:2\r\n");
    }

    #[test]
    fn serialize_to_slice_returns_none_if_too_small() {
        let frame = RespFrame::SimpleString(Bytes::from_static(b"OK"));
        let mut tiny = [0u8; 2];
        assert!(RespSerializer::serialize_to_slice(&frame, &mut tiny).is_none());
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

    #[test]
    fn roundtrip_via_slice() {
        use crate::parser::RespParser;

        let original = RespFrame::Array(Some(vec![
            RespFrame::BulkString(Some(Bytes::from_static(b"SET"))),
            RespFrame::BulkString(Some(Bytes::from_static(b"key"))),
            RespFrame::BulkString(Some(Bytes::from_static(b"value"))),
        ]));

        let mut buf = [0u8; 256];
        let n = RespSerializer::serialize_to_slice(&original, &mut buf).unwrap();

        let (parsed, _) = RespParser::parse(&buf[..n]).unwrap();
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

    // ── IovecWriter serialization tests ────────────────────────────────

    fn serialize_iovec(frame: &RespFrame) -> Vec<u8> {
        let mut w = crate::iovec::IovecWriter::new();
        RespSerializer::serialize_to_iovecs(frame, &mut w);
        w.flatten()
    }

    #[test]
    fn iovec_ok() {
        let frame = RespFrame::SimpleString(Bytes::from_static(b"OK"));
        assert_eq!(serialize_iovec(&frame), b"+OK\r\n");
    }

    #[test]
    fn iovec_error() {
        let frame = RespFrame::Error(Bytes::from_static(b"ERR bad"));
        assert_eq!(serialize_iovec(&frame), b"-ERR bad\r\n");
    }

    #[test]
    fn iovec_integer_precomputed() {
        for n in 0..=9i64 {
            let expected = format!(":{n}\r\n");
            assert_eq!(serialize_iovec(&RespFrame::Integer(n)), expected.as_bytes());
        }
    }

    #[test]
    fn iovec_integer_lut() {
        for n in [10i64, 42, 99, 100, 999, 1000, 9999] {
            let expected = format!(":{n}\r\n");
            assert_eq!(
                serialize_iovec(&RespFrame::Integer(n)),
                expected.as_bytes(),
                "n={n}"
            );
        }
    }

    #[test]
    fn iovec_integer_negative() {
        for n in [-1i64, -42, -9999, -10_000, -123_456] {
            let expected = format!(":{n}\r\n");
            assert_eq!(
                serialize_iovec(&RespFrame::Integer(n)),
                expected.as_bytes(),
                "n={n}"
            );
        }
    }

    #[test]
    fn iovec_bulk_string() {
        let frame = RespFrame::BulkString(Some(Bytes::from_static(b"hello")));
        assert_eq!(serialize_iovec(&frame), b"$5\r\nhello\r\n");
    }

    #[test]
    fn iovec_null() {
        assert_eq!(serialize_iovec(&RespFrame::BulkString(None)), b"$-1\r\n");
    }

    #[test]
    fn iovec_array() {
        let frame = RespFrame::Array(Some(vec![RespFrame::Integer(1), RespFrame::Integer(2)]));
        assert_eq!(serialize_iovec(&frame), b"*2\r\n:1\r\n:2\r\n");
    }

    #[test]
    fn iovec_nested_array_10() {
        let items: Vec<_> = (0..10)
            .map(|_| RespFrame::BulkString(Some(Bytes::from_static(b"val"))))
            .collect();
        let frame = RespFrame::Array(Some(items));
        let iovec_bytes = serialize_iovec(&frame);
        let bytesmut_bytes = serialize(&frame);
        assert_eq!(iovec_bytes, bytesmut_bytes);
    }

    #[test]
    fn iovec_matches_serialize_for_all_types() {
        // Comprehensive: verify iovec path matches BytesMut path.
        let frames = vec![
            RespFrame::SimpleString(Bytes::from_static(b"OK")),
            RespFrame::SimpleString(Bytes::from_static(b"PONG")),
            RespFrame::SimpleString(Bytes::from_static(b"QUEUED")),
            RespFrame::SimpleString(Bytes::from_static(b"custom")),
            RespFrame::Error(Bytes::from_static(b"ERR some error")),
            RespFrame::Integer(0),
            RespFrame::Integer(5),
            RespFrame::Integer(42),
            RespFrame::Integer(9999),
            RespFrame::Integer(10_000),
            RespFrame::Integer(-1),
            RespFrame::Integer(-9999),
            RespFrame::Integer(-10_000),
            RespFrame::Integer(i64::MAX),
            RespFrame::Integer(i64::MIN),
            RespFrame::BulkString(None),
            RespFrame::BulkString(Some(Bytes::from_static(b"hello"))),
            RespFrame::BulkString(Some(Bytes::from_static(b""))),
            RespFrame::Array(None),
            RespFrame::Array(Some(vec![])),
            RespFrame::Array(Some(vec![
                RespFrame::BulkString(Some(Bytes::from_static(b"SET"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"key"))),
                RespFrame::BulkString(Some(Bytes::from_static(b"value"))),
            ])),
        ];

        for frame in &frames {
            let expected = serialize(frame);
            let got = serialize_iovec(frame);
            assert_eq!(got, expected, "mismatch for frame: {frame:?}");
        }
    }

    #[test]
    fn iovec_roundtrip() {
        use crate::parser::RespParser;

        let original = RespFrame::Array(Some(vec![
            RespFrame::BulkString(Some(Bytes::from_static(b"SET"))),
            RespFrame::BulkString(Some(Bytes::from_static(b"key"))),
            RespFrame::BulkString(Some(Bytes::from_static(b"value"))),
        ]));

        let bytes = serialize_iovec(&original);
        let (parsed, _) = RespParser::parse(&bytes).unwrap();
        assert_eq!(parsed, original);
    }

    #[cfg(feature = "resp3")]
    #[test]
    fn iovec_resp3_types() {
        // Verify all RESP3 types match BytesMut serialization.
        let frames = vec![
            RespFrame::Null,
            RespFrame::Boolean(true),
            RespFrame::Boolean(false),
            RespFrame::Double(1.5),
            RespFrame::BigNumber(Bytes::from_static(b"12345678901234567890")),
            RespFrame::BulkError(Bytes::from_static(b"ERROR")),
            RespFrame::VerbatimString {
                encoding: *b"txt",
                data: Bytes::from_static(b"hello"),
            },
            RespFrame::Map(vec![(
                RespFrame::SimpleString(Bytes::from_static(b"key")),
                RespFrame::Integer(42),
            )]),
            RespFrame::Set(vec![RespFrame::Integer(1), RespFrame::Integer(2)]),
        ];

        for frame in &frames {
            let expected = serialize(frame);
            let got = serialize_iovec(frame);
            assert_eq!(got, expected, "resp3 mismatch for frame: {frame:?}");
        }
    }
}
