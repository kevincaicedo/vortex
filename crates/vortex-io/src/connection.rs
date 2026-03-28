use slab::Slab;

// ---------------------------------------------------------------------------
// Connection state machine
// ---------------------------------------------------------------------------

/// Connection lifecycle states.
///
/// Transitions: `New → Active`, `New → Closing`, `Active → Closing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectionState {
    /// Just accepted, awaiting first successful read.
    New = 0,
    /// Normal connected state — actively reading/writing.
    Active = 1,
    /// Teardown in progress, awaiting close CQE.
    Closing = 2,
}

impl ConnectionState {
    /// Decode from a raw `u8`.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::New),
            1 => Some(Self::Active),
            2 => Some(Self::Closing),
            _ => None,
        }
    }
}

/// Error returned when an invalid state transition is attempted.
#[derive(Debug, Clone, Copy)]
pub struct InvalidTransition {
    pub from: ConnectionState,
    pub to: ConnectionState,
}

impl std::fmt::Display for InvalidTransition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid transition: {:?} → {:?}", self.from, self.to)
    }
}

// ---------------------------------------------------------------------------
// Connection metadata — 64-byte cache-line-aligned
// ---------------------------------------------------------------------------

/// Per-connection metadata packed into a single 64-byte cache line.
///
/// Field layout is `#[repr(C)]` to guarantee the documented offsets.
/// All pointers are replaced by indices into reactor-owned arrays so that
/// the struct contains no pointers and is trivially `Copy`.
///
/// ```text
/// Offset  Size  Field
/// ──────  ────  ────────────────
///  0      4     fd
///  4      1     state
///  5      1     flags
///  6      2     shard_id
///  8      4     read_buf_offset
/// 12      4     read_buf_len
/// 16      4     write_buf_offset
/// 20      4     write_buf_len
/// 24      4     last_active
/// 28      4     timer_slot
/// 32      4     addr_v4
/// 36      2     addr_port
/// 38      26    _pad
/// ──────  ────
///  0      64    TOTAL
/// ```
#[repr(C, align(64))]
pub struct ConnectionMeta {
    /// OS file descriptor (`-1` when uninitialised).
    pub fd: i32,
    /// Encoded [`ConnectionState`] (`u8`).
    state: u8,
    /// Bitflags (see [`ConnectionFlags`]).
    pub flags: u8,
    /// Assigned shard index (Phase 3+).
    pub shard_id: u16,
    /// Index into the reactor's read-buffer array.
    pub read_buf_offset: u32,
    /// Bytes currently in the read buffer.
    pub read_buf_len: u32,
    /// Index into the reactor's write-buffer array.
    pub write_buf_offset: u32,
    /// Bytes currently in the write buffer.
    pub write_buf_len: u32,
    /// Seconds since reactor start — for idle-timeout checks.
    pub last_active: u32,
    /// Timer wheel entry index (for O(1) cancel).
    pub timer_slot: u32,
    /// IPv4 address octets of the remote peer.
    pub addr_v4: [u8; 4],
    /// TCP port of the remote peer.
    pub addr_port: u16,
    /// Padding to fill the cache line.
    _pad: [u8; 26],
}

// Compile-time size check.
const _: () = assert!(
    std::mem::size_of::<ConnectionMeta>() == 64,
    "ConnectionMeta must be exactly 64 bytes (one cache line)"
);

/// Sentinel for an uninitialised timer slot.
pub const TIMER_SLOT_NONE: u32 = u32::MAX;

impl ConnectionMeta {
    /// Creates a fresh connection with the given file descriptor and buffer
    /// index (typically equal to the slab token).
    #[inline]
    pub fn new(fd: i32, buf_index: u32) -> Self {
        Self {
            fd,
            state: ConnectionState::New as u8,
            flags: 0,
            shard_id: 0,
            read_buf_offset: buf_index,
            read_buf_len: 0,
            write_buf_offset: buf_index,
            write_buf_len: 0,
            last_active: 0,
            timer_slot: TIMER_SLOT_NONE,
            addr_v4: [0; 4],
            addr_port: 0,
            _pad: [0; 26],
        }
    }

    /// Returns the current [`ConnectionState`].
    #[inline]
    pub fn state(&self) -> ConnectionState {
        // SAFETY: we only ever write valid discriminants.
        ConnectionState::from_u8(self.state).unwrap_or(ConnectionState::Closing)
    }

    /// Attempt a state transition.
    ///
    /// Valid transitions: `New → Active`, `New → Closing`, `Active → Closing`.
    /// In debug builds, invalid transitions panic. In release builds they
    /// return an error.
    #[inline]
    pub fn transition(&mut self, to: ConnectionState) -> Result<(), InvalidTransition> {
        let from = self.state();
        let valid = matches!(
            (from, to),
            (ConnectionState::New, ConnectionState::Active)
                | (ConnectionState::New, ConnectionState::Closing)
                | (ConnectionState::Active, ConnectionState::Closing)
        );
        debug_assert!(valid, "invalid state transition: {from:?} → {to:?}");
        if valid {
            self.state = to as u8;
            Ok(())
        } else {
            Err(InvalidTransition { from, to })
        }
    }

    /// Returns `true` when the connection is in the [`Closing`] state.
    #[inline]
    pub fn is_closing(&self) -> bool {
        self.state == ConnectionState::Closing as u8
    }
}

// ---------------------------------------------------------------------------
// Connection flags (bitfield)
// ---------------------------------------------------------------------------

/// Bitflags stored in `ConnectionMeta.flags`.
pub struct ConnectionFlags;

impl ConnectionFlags {
    pub const READABLE: u8 = 1 << 0;
    pub const WRITABLE: u8 = 1 << 1;
    pub const CLOSE_AFTER_WRITE: u8 = 1 << 2;
}

// ---------------------------------------------------------------------------
// Connection slab
// ---------------------------------------------------------------------------

/// Slab-allocated connection pool.
///
/// Dense array with O(1) insert/remove. Pre-allocates to `max_connections`
/// to avoid runtime reallocation.
pub struct ConnectionSlab {
    inner: Slab<ConnectionMeta>,
}

impl ConnectionSlab {
    /// Creates a new connection slab with pre-allocated capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: Slab::with_capacity(cap),
        }
    }

    /// Inserts connection metadata and returns its slab token.
    pub fn insert(&mut self, meta: ConnectionMeta) -> usize {
        self.inner.insert(meta)
    }

    /// Removes a connection by slab token.
    pub fn remove(&mut self, token: usize) -> ConnectionMeta {
        self.inner.remove(token)
    }

    /// Gets a shared reference to a connection by slab token.
    pub fn get(&self, token: usize) -> Option<&ConnectionMeta> {
        self.inner.get(token)
    }

    /// Gets a mutable reference to a connection by slab token.
    pub fn get_mut(&mut self, token: usize) -> Option<&mut ConnectionMeta> {
        self.inner.get_mut(token)
    }

    /// Returns the number of active connections.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if no connections are tracked.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns an iterator over `(token, &ConnectionMeta)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (usize, &ConnectionMeta)> + '_ {
        self.inner.iter()
    }

    /// Returns an iterator over all connection IDs (slab tokens).
    pub fn ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.inner.iter().map(|(id, _)| id)
    }

    /// Count connections in the given state.
    pub fn count_by_state(&self, state: ConnectionState) -> usize {
        self.inner
            .iter()
            .filter(|(_, m)| m.state() == state)
            .count()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── 1.2.8  Connection state machine tests ───────────────────────

    #[test]
    fn meta_size_is_64_bytes() {
        assert_eq!(std::mem::size_of::<ConnectionMeta>(), 64);
    }

    #[test]
    fn meta_alignment_is_64() {
        assert_eq!(std::mem::align_of::<ConnectionMeta>(), 64);
    }

    #[test]
    fn transition_new_to_active() {
        let mut m = ConnectionMeta::new(-1, 0);
        assert_eq!(m.state(), ConnectionState::New);
        m.transition(ConnectionState::Active).unwrap();
        assert_eq!(m.state(), ConnectionState::Active);
    }

    #[test]
    fn transition_new_to_closing() {
        let mut m = ConnectionMeta::new(-1, 0);
        m.transition(ConnectionState::Closing).unwrap();
        assert_eq!(m.state(), ConnectionState::Closing);
    }

    #[test]
    fn transition_active_to_closing() {
        let mut m = ConnectionMeta::new(-1, 0);
        m.transition(ConnectionState::Active).unwrap();
        m.transition(ConnectionState::Closing).unwrap();
        assert_eq!(m.state(), ConnectionState::Closing);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "invalid state transition")]
    fn transition_active_to_new_panics() {
        let mut m = ConnectionMeta::new(-1, 0);
        m.transition(ConnectionState::Active).unwrap();
        let _ = m.transition(ConnectionState::New);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "invalid state transition")]
    fn transition_closing_to_active_panics() {
        let mut m = ConnectionMeta::new(-1, 0);
        m.transition(ConnectionState::Closing).unwrap();
        let _ = m.transition(ConnectionState::Active);
    }

    #[test]
    fn is_closing_flag() {
        let mut m = ConnectionMeta::new(5, 0);
        assert!(!m.is_closing());
        m.transition(ConnectionState::Closing).unwrap();
        assert!(m.is_closing());
    }

    // ── Slab tests ──────────────────────────────────────────────────

    #[test]
    fn slab_insert_remove() {
        let mut slab = ConnectionSlab::with_capacity(16);
        let token = slab.insert(ConnectionMeta::new(42, 0));
        assert_eq!(slab.len(), 1);

        let conn = slab.get(token).unwrap();
        assert_eq!(conn.fd, 42);
        assert_eq!(conn.state(), ConnectionState::New);

        slab.remove(token);
        assert!(slab.is_empty());
    }

    #[test]
    fn slab_iter_and_count_by_state() {
        let mut slab = ConnectionSlab::with_capacity(16);
        let t1 = slab.insert(ConnectionMeta::new(1, 0));
        let _t2 = slab.insert(ConnectionMeta::new(2, 1));
        slab.get_mut(t1)
            .unwrap()
            .transition(ConnectionState::Active)
            .unwrap();

        assert_eq!(slab.count_by_state(ConnectionState::Active), 1);
        assert_eq!(slab.count_by_state(ConnectionState::New), 1);
        assert_eq!(slab.iter().count(), 2);
    }
}
