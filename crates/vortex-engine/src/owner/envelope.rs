//! Remote command envelope ownership for the shared-nothing branch.
//!
//! Local commands may borrow parser frame data only for the current reactor
//! turn. Remote commands must carry either an inline copy for small payloads or
//! an owned buffer lease for larger payloads before they can cross to another
//! owner.

use std::marker::PhantomData;
use std::ops::Range;
use std::rc::Rc;

use bytes::Bytes;
use vortex_common::{MAX_INLINE_KEY_LEN, MAX_INLINE_VALUE_LEN, VortexKey, VortexValue};

/// Shared-nothing single-key operation carried by an owner envelope.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RemoteEnvelopeOp {
    /// GET key.
    Get,
    /// Plain SET key value without options.
    SetPlain,
    /// DEL key.
    Del,
    /// EXISTS key.
    Exists,
    /// TTL key.
    Ttl,
    /// PTTL key.
    Pttl,
    /// TYPE key.
    Type,
}

/// Borrowed local command envelope.
///
/// This type is intentionally `!Send` because it may contain references into a
/// reactor-owned parse buffer. It must be consumed in the same reactor turn and
/// cannot be sent to an owner queue.
///
/// ```compile_fail
/// use vortex_engine::owner::LocalBorrowedEnvelope;
///
/// fn requires_send<T: Send>(value: T) {
///     let _ = value;
/// }
///
/// let envelope = LocalBorrowedEnvelope::set_plain(b"k", b"v");
/// requires_send(envelope);
/// ```
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct LocalBorrowedEnvelope<'a> {
    op: RemoteEnvelopeOp,
    key: &'a [u8],
    value: Option<&'a [u8]>,
    _same_reactor_only: PhantomData<Rc<()>>,
}

impl<'a> LocalBorrowedEnvelope<'a> {
    /// Creates a borrowed GET envelope for same-reactor execution only.
    #[inline]
    pub const fn get(key: &'a [u8]) -> Self {
        Self {
            op: RemoteEnvelopeOp::Get,
            key,
            value: None,
            _same_reactor_only: PhantomData,
        }
    }

    /// Creates a borrowed plain SET envelope for same-reactor execution only.
    #[inline]
    pub const fn set_plain(key: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            op: RemoteEnvelopeOp::SetPlain,
            key,
            value: Some(value),
            _same_reactor_only: PhantomData,
        }
    }

    /// Envelope operation.
    #[inline]
    pub const fn op(self) -> RemoteEnvelopeOp {
        self.op
    }

    /// Borrowed key bytes.
    #[inline]
    pub const fn key_bytes(self) -> &'a [u8] {
        self.key
    }

    /// Borrowed value bytes for write commands.
    #[inline]
    pub const fn value_bytes(self) -> Option<&'a [u8]> {
        self.value
    }
}

/// Immutable connection-buffer lease used by remote envelopes.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteBufferLease {
    bytes: Bytes,
}

impl RemoteBufferLease {
    /// Wraps an immutable buffer that can outlive the parser turn.
    #[inline]
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    /// Creates a zero-copy slice into the leased buffer.
    #[inline]
    pub fn slice(&self, range: Range<usize>) -> Bytes {
        self.bytes.slice(range)
    }

    /// Returns the lease length.
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns true when the lease is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Remote command envelope that is safe to enqueue across owner reactors.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemoteCommandEnvelope {
    /// Inline key/value payload for small single-key commands.
    Small(RemoteSmallEnvelope),
    /// Ref-counted buffer slices for larger payloads.
    Lease(RemoteLeaseEnvelope),
}

impl RemoteCommandEnvelope {
    /// Builds a remote plain SET envelope from borrowed bytes.
    ///
    /// Small key/value pairs are copied into fixed inline storage. Larger
    /// payloads are copied into owned `Bytes`, which is safe but not zero-copy.
    #[inline]
    pub fn set_plain_copied(key: &[u8], value: &[u8]) -> Self {
        if let Some(small) = RemoteSmallEnvelope::set_plain(key, value) {
            Self::Small(small)
        } else {
            Self::Lease(RemoteLeaseEnvelope::set_plain_copied(key, value))
        }
    }

    /// Builds a remote plain SET envelope from leased buffer slices.
    ///
    /// The slices are ref-counted handles into a parser or network buffer and
    /// avoid copying large values at envelope construction.
    #[inline]
    pub fn set_plain_leased(key: Bytes, value: Bytes) -> Self {
        if key.len() <= MAX_INLINE_KEY_LEN && value.len() <= MAX_INLINE_VALUE_LEN {
            Self::Small(
                RemoteSmallEnvelope::set_plain(&key, &value)
                    .expect("inline size checked before small envelope build"),
            )
        } else {
            Self::Lease(RemoteLeaseEnvelope::set_plain_leased(key, value))
        }
    }

    /// Envelope operation.
    #[inline]
    pub const fn op(&self) -> RemoteEnvelopeOp {
        match self {
            Self::Small(small) => small.op(),
            Self::Lease(lease) => lease.op(),
        }
    }

    /// Key bytes.
    #[inline]
    pub fn key_bytes(&self) -> &[u8] {
        match self {
            Self::Small(small) => small.key_bytes(),
            Self::Lease(lease) => lease.key_bytes(),
        }
    }

    /// Value bytes for write commands.
    #[inline]
    pub fn value_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Small(small) => small.value_bytes(),
            Self::Lease(lease) => lease.value_bytes(),
        }
    }

    /// Returns envelope ownership accounting.
    #[inline]
    pub fn accounting(&self) -> RemoteEnvelopeAccounting {
        match self {
            Self::Small(small) => small.accounting(),
            Self::Lease(lease) => lease.accounting(),
        }
    }

    /// Converts a plain SET envelope into engine key/value types.
    ///
    /// Large leased values become `VortexValue::String(Bytes)` without copying
    /// the value bytes. `VortexKey` still owns large keys as `Vec<u8>` in the
    /// current engine representation.
    #[inline]
    pub fn into_set_key_value(self) -> Option<(VortexKey, VortexValue)> {
        match self {
            Self::Small(small) => small.into_set_key_value(),
            Self::Lease(lease) => lease.into_set_key_value(),
        }
    }
}

/// Inline remote envelope for small payloads.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSmallEnvelope {
    op: RemoteEnvelopeOp,
    key: InlinePayload<MAX_INLINE_KEY_LEN>,
    value: Option<InlinePayload<MAX_INLINE_VALUE_LEN>>,
}

impl RemoteSmallEnvelope {
    /// Builds an inline plain SET envelope when key and value fit.
    #[inline]
    pub fn set_plain(key: &[u8], value: &[u8]) -> Option<Self> {
        Some(Self {
            op: RemoteEnvelopeOp::SetPlain,
            key: InlinePayload::new(key)?,
            value: Some(InlinePayload::new(value)?),
        })
    }

    /// Envelope operation.
    #[inline]
    pub const fn op(&self) -> RemoteEnvelopeOp {
        self.op
    }

    /// Key bytes.
    #[inline]
    pub fn key_bytes(&self) -> &[u8] {
        self.key.as_bytes()
    }

    /// Value bytes for write commands.
    #[inline]
    pub fn value_bytes(&self) -> Option<&[u8]> {
        self.value.as_ref().map(InlinePayload::as_bytes)
    }

    #[inline]
    fn accounting(&self) -> RemoteEnvelopeAccounting {
        let inline_bytes = self.key.len() + self.value.as_ref().map_or(0, InlinePayload::len);
        RemoteEnvelopeAccounting {
            payload_bytes: inline_bytes,
            inline_bytes,
            copied_bytes: inline_bytes,
            leased_bytes: 0,
        }
    }

    #[inline]
    fn into_set_key_value(self) -> Option<(VortexKey, VortexValue)> {
        if self.op != RemoteEnvelopeOp::SetPlain {
            return None;
        }
        let value = self.value?;
        Some((
            VortexKey::from_bytes(self.key.as_bytes()),
            VortexValue::from_bytes(value.as_bytes()),
        ))
    }
}

/// Leased remote envelope for larger payloads.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteLeaseEnvelope {
    op: RemoteEnvelopeOp,
    key: Bytes,
    value: Option<Bytes>,
    copied_bytes: usize,
}

impl RemoteLeaseEnvelope {
    /// Builds a leased plain SET envelope by copying borrowed bytes into owned
    /// buffers. This is safe fallback behavior when the parser buffer cannot be
    /// leased.
    #[inline]
    pub fn set_plain_copied(key: &[u8], value: &[u8]) -> Self {
        Self {
            op: RemoteEnvelopeOp::SetPlain,
            key: Bytes::copy_from_slice(key),
            value: Some(Bytes::copy_from_slice(value)),
            copied_bytes: key.len() + value.len(),
        }
    }

    /// Builds a zero-copy leased plain SET envelope from ref-counted slices.
    #[inline]
    pub fn set_plain_leased(key: Bytes, value: Bytes) -> Self {
        Self {
            op: RemoteEnvelopeOp::SetPlain,
            key,
            value: Some(value),
            copied_bytes: 0,
        }
    }

    /// Envelope operation.
    #[inline]
    pub const fn op(&self) -> RemoteEnvelopeOp {
        self.op
    }

    /// Key bytes.
    #[inline]
    pub fn key_bytes(&self) -> &[u8] {
        &self.key
    }

    /// Value bytes for write commands.
    #[inline]
    pub fn value_bytes(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    #[inline]
    fn accounting(&self) -> RemoteEnvelopeAccounting {
        let payload_bytes = self.key.len() + self.value.as_ref().map_or(0, Bytes::len);
        RemoteEnvelopeAccounting {
            payload_bytes,
            inline_bytes: 0,
            copied_bytes: self.copied_bytes,
            leased_bytes: payload_bytes.saturating_sub(self.copied_bytes),
        }
    }

    #[inline]
    fn into_set_key_value(self) -> Option<(VortexKey, VortexValue)> {
        if self.op != RemoteEnvelopeOp::SetPlain {
            return None;
        }
        let value = self.value?;
        let key = if self.key.len() <= MAX_INLINE_KEY_LEN {
            VortexKey::from_bytes(&self.key)
        } else {
            VortexKey::from(self.key.to_vec())
        };
        let value = if value.len() <= MAX_INLINE_VALUE_LEN {
            VortexValue::from_bytes(&value)
        } else {
            VortexValue::from(value)
        };
        Some((key, value))
    }
}

/// Envelope byte accounting.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RemoteEnvelopeAccounting {
    /// Logical key/value bytes carried by the envelope.
    pub payload_bytes: usize,
    /// Payload bytes copied into inline storage.
    pub inline_bytes: usize,
    /// Payload bytes copied into owned buffers at envelope construction.
    pub copied_bytes: usize,
    /// Payload bytes represented by ref-counted buffer leases.
    pub leased_bytes: usize,
}

/// Owned response buffer accounting for remote replies.
#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemoteReplyBuffer {
    buffers: Vec<Bytes>,
    bytes: usize,
}

impl RemoteReplyBuffer {
    /// Adds an owned response buffer.
    #[inline]
    pub fn push_owned(&mut self, bytes: Bytes) {
        self.bytes += bytes.len();
        self.buffers.push(bytes);
    }

    /// Copies a static or borrowed response into an owned response buffer.
    #[inline]
    pub fn push_copied(&mut self, bytes: &[u8]) {
        self.push_owned(Bytes::copy_from_slice(bytes));
    }

    /// Number of response buffers retained.
    #[inline]
    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    /// Total retained response bytes.
    #[inline]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Clears all retained response buffers.
    #[inline]
    pub fn clear(&mut self) {
        self.buffers.clear();
        self.bytes = 0;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InlinePayload<const N: usize> {
    len: u8,
    data: [u8; N],
}

impl<const N: usize> InlinePayload<N> {
    #[inline]
    fn new(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > N || bytes.len() > u8::MAX as usize {
            return None;
        }
        let mut data = [0u8; N];
        data[..bytes.len()].copy_from_slice(bytes);
        Some(Self {
            len: bytes.len() as u8,
            data,
        })
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }

    #[inline]
    const fn len(&self) -> usize {
        self.len as usize
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use super::*;

    #[test]
    fn small_remote_set_uses_inline_payload() {
        let envelope = RemoteCommandEnvelope::set_plain_copied(b"key", b"value");

        assert!(matches!(envelope, RemoteCommandEnvelope::Small(_)));
        assert_eq!(envelope.key_bytes(), b"key");
        assert_eq!(envelope.value_bytes(), Some(&b"value"[..]));
        assert_eq!(
            envelope.accounting(),
            RemoteEnvelopeAccounting {
                payload_bytes: 8,
                inline_bytes: 8,
                copied_bytes: 8,
                leased_bytes: 0,
            }
        );
    }

    #[test]
    fn large_remote_set_can_use_leased_value_without_payload_copy() {
        let mut wire = BytesMut::new();
        wire.extend_from_slice(b"remote-key");
        let value_start = wire.len();
        wire.extend_from_slice(&[b'v'; 4096]);
        let value_end = wire.len();
        let lease = RemoteBufferLease::new(wire.freeze());
        let key = lease.slice(0..value_start);
        let value = lease.slice(value_start..value_end);

        let envelope = RemoteCommandEnvelope::set_plain_leased(key, value);
        let accounting = envelope.accounting();

        assert!(matches!(envelope, RemoteCommandEnvelope::Lease(_)));
        assert_eq!(accounting.payload_bytes, 4106);
        assert_eq!(accounting.copied_bytes, 0);
        assert_eq!(accounting.leased_bytes, 4106);

        let (key, value) = envelope.into_set_key_value().expect("plain SET");
        assert_eq!(key.as_bytes(), b"remote-key");
        match value {
            VortexValue::String(bytes) => assert_eq!(bytes.len(), 4096),
            other => panic!("expected heap string, got {other:?}"),
        }
    }

    #[test]
    fn leased_payload_survives_original_buffer_drop_and_reuse() {
        let mut wire = BytesMut::new();
        wire.extend_from_slice(b"lease-key");
        let value_start = wire.len();
        wire.extend_from_slice(&[b'x'; 1024]);
        let value_end = wire.len();
        let lease = RemoteBufferLease::new(wire.freeze());
        let key = lease.slice(0..value_start);
        let value = lease.slice(value_start..value_end);
        drop(lease);

        let mut reused = BytesMut::from(&b"reused-buffer"[..]);
        reused.clear();
        reused.extend_from_slice(b"different");

        let envelope = RemoteCommandEnvelope::set_plain_leased(key, value);
        assert_eq!(envelope.key_bytes(), b"lease-key");
        assert_eq!(envelope.value_bytes().expect("value")[0], b'x');
        assert_eq!(envelope.value_bytes().expect("value").len(), 1024);
    }

    #[test]
    fn copied_large_remote_set_reports_copied_bytes() {
        let value = vec![b'x'; 2048];
        let envelope = RemoteCommandEnvelope::set_plain_copied(b"key", &value);
        let accounting = envelope.accounting();

        assert!(matches!(envelope, RemoteCommandEnvelope::Lease(_)));
        assert_eq!(accounting.payload_bytes, 2051);
        assert_eq!(accounting.copied_bytes, 2051);
        assert_eq!(accounting.leased_bytes, 0);
    }

    #[test]
    fn reply_buffer_tracks_owned_response_bytes() {
        let mut replies = RemoteReplyBuffer::default();
        replies.push_copied(b"+OK\r\n");
        replies.push_owned(Bytes::from_static(b"$3\r\nhey\r\n"));

        assert_eq!(replies.buffer_count(), 2);
        assert_eq!(replies.bytes(), 14);
        replies.clear();
        assert_eq!(replies.buffer_count(), 0);
        assert_eq!(replies.bytes(), 0);
    }
}
