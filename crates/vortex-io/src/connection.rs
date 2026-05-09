// ---------------------------------------------------------------------------
// Connection slot state machine
// ---------------------------------------------------------------------------

/// Connection slot lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// The slot is available for reuse.
    Vacant,
    /// The slot owns a live connection.
    Active,
    /// The slot owns resources waiting for terminal backend completions.
    Closing,
    /// The slot resources have been proven terminal and moved out for cleanup.
    Drained,
}

/// Error returned when an invalid state transition is attempted.
#[derive(Debug, Clone, Copy)]
pub struct InvalidTransition {
    pub from: ConnectionState,
    pub to: ConnectionState,
}

impl std::fmt::Display for InvalidTransition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid transition: {:?} -> {:?}", self.from, self.to)
    }
}

// ---------------------------------------------------------------------------
// Connection metadata — 64-byte cache-line-aligned
// ---------------------------------------------------------------------------

/// Per-connection metadata packed into a single 64-byte cache line.
///
/// Field layout is `#[repr(C)]` to guarantee the documented offsets. Lifecycle
/// state lives in [`ConnSlot`], so this struct contains only resources and
/// scheduler metadata valid for an active or closing connection.
///
/// ```text
/// Offset  Size  Field
/// ------  ----  ----------------
///  0      4     fd
///  4      1     flags
///  5      3     _reserved
///  8      4     read_buf_offset
/// 12      4     read_buf_len
/// 16      4     write_buf_offset
/// 20      4     write_buf_len
/// 24      4     last_active
/// 28      4     timer_slot
/// 32      4     addr_v4
/// 36      2     addr_port
/// 38      26    _pad
/// ------  ----
///  0      64    TOTAL
/// ```
#[derive(Debug)]
#[repr(C, align(64))]
pub struct ConnectionMeta {
    /// OS file descriptor owned by the connection slot.
    pub fd: i32,
    /// Bitflags (see [`ConnectionFlags`]).
    pub flags: u8,
    _reserved: [u8; 3],
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
    /// Creates a fresh connection metadata block.
    #[inline]
    pub fn new(fd: i32, buf_index: u32) -> Self {
        Self {
            fd,
            flags: 0,
            _reserved: [0; 3],
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
}

/// Live connection resources.
pub struct ActiveConn {
    meta: ConnectionMeta,
}

impl ActiveConn {
    /// Wraps metadata for an active connection slot.
    #[inline]
    pub fn new(meta: ConnectionMeta) -> Self {
        Self { meta }
    }

    #[inline]
    fn into_closing(self) -> ClosingConn {
        ClosingConn {
            meta: self.meta,
            close_submitted: false,
        }
    }

    #[inline]
    fn into_meta(self) -> ConnectionMeta {
        self.meta
    }
}

/// Closing connection resources that remain live until terminal proof.
pub struct ClosingConn {
    meta: ConnectionMeta,
    close_submitted: bool,
}

impl ClosingConn {
    /// Marks that the backend owns close completion for this fd.
    #[inline]
    pub fn mark_close_submitted(&mut self) {
        self.close_submitted = true;
    }

    #[inline]
    fn drain(self) -> DrainedConn {
        DrainedConn {
            meta: self.meta,
            close_submitted: self.close_submitted,
        }
    }

    #[inline]
    fn into_meta(self) -> ConnectionMeta {
        self.meta
    }
}

/// Resources moved out of a closing slot after terminal proof.
pub struct DrainedConn {
    meta: ConnectionMeta,
    close_submitted: bool,
}

impl DrainedConn {
    /// Returns `true` when the reactor still needs to close the fd directly.
    #[inline]
    pub fn needs_direct_close(&self) -> bool {
        !self.close_submitted
    }

    /// Returns metadata for final reactor cleanup.
    #[inline]
    pub fn into_meta(self) -> ConnectionMeta {
        self.meta
    }
}

/// Typed connection slot.
pub enum ConnSlot {
    /// The slot is available for reuse.
    Vacant,
    /// The slot owns active connection resources.
    Active(ActiveConn),
    /// The slot owns terminal-unknown resources during close.
    Closing(ClosingConn),
    /// The slot has been drained and is awaiting cleanup.
    Drained(DrainedConn),
}

impl ConnSlot {
    #[inline]
    fn state(&self) -> ConnectionState {
        match self {
            Self::Vacant => ConnectionState::Vacant,
            Self::Active(_) => ConnectionState::Active,
            Self::Closing(_) => ConnectionState::Closing,
            Self::Drained(_) => ConnectionState::Drained,
        }
    }

    #[inline]
    fn meta(&self) -> Option<&ConnectionMeta> {
        match self {
            Self::Active(conn) => Some(&conn.meta),
            Self::Closing(conn) => Some(&conn.meta),
            Self::Vacant | Self::Drained(_) => None,
        }
    }

    #[inline]
    fn meta_mut(&mut self) -> Option<&mut ConnectionMeta> {
        match self {
            Self::Active(conn) => Some(&mut conn.meta),
            Self::Closing(conn) => Some(&mut conn.meta),
            Self::Vacant | Self::Drained(_) => None,
        }
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

/// Slot-indexed connection pool with explicit lifecycle states.
pub struct ConnectionSlab {
    slots: Vec<ConnSlot>,
    free: Vec<usize>,
    len: usize,
}

impl ConnectionSlab {
    /// Creates a new connection slab with pre-allocated capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            slots: Vec::with_capacity(cap),
            free: Vec::new(),
            len: 0,
        }
    }

    /// Inserts an active connection and returns its slot token.
    pub fn insert(&mut self, meta: ConnectionMeta) -> usize {
        let slot = ConnSlot::Active(ActiveConn::new(meta));
        self.len += 1;
        if let Some(token) = self.free.pop() {
            debug_assert!(matches!(self.slots.get(token), Some(ConnSlot::Vacant)));
            self.slots[token] = slot;
            token
        } else {
            self.slots.push(slot);
            self.slots.len() - 1
        }
    }

    /// Removes a connection by slot token.
    pub fn remove(&mut self, token: usize) -> ConnectionMeta {
        match self.take_slot(token) {
            ConnSlot::Active(conn) => conn.into_meta(),
            ConnSlot::Closing(conn) => conn.into_meta(),
            ConnSlot::Drained(conn) => conn.into_meta(),
            ConnSlot::Vacant => panic!("attempted to remove vacant connection slot"),
        }
    }

    /// Moves an active slot into closing state.
    pub fn transition_to_closing(&mut self, token: usize) -> Result<(), InvalidTransition> {
        let Some(slot) = self.slots.get_mut(token) else {
            return Err(InvalidTransition {
                from: ConnectionState::Vacant,
                to: ConnectionState::Closing,
            });
        };
        match slot.state() {
            ConnectionState::Active => {
                let previous = std::mem::replace(slot, ConnSlot::Vacant);
                let ConnSlot::Active(active) = previous else {
                    unreachable!("state checked above");
                };
                *slot = ConnSlot::Closing(active.into_closing());
                Ok(())
            }
            ConnectionState::Closing => Ok(()),
            from => Err(InvalidTransition {
                from,
                to: ConnectionState::Closing,
            }),
        }
    }

    /// Marks that the backend accepted the close operation for this slot.
    pub fn mark_close_submitted(&mut self, token: usize) {
        if let Some(ConnSlot::Closing(conn)) = self.slots.get_mut(token) {
            conn.mark_close_submitted();
        }
    }

    /// Moves a closing slot into a drained resource bundle and frees the slot.
    pub fn drain_closing(&mut self, token: usize) -> Option<DrainedConn> {
        let slot = self.slots.get_mut(token)?;
        if !matches!(slot, ConnSlot::Closing(_)) {
            return None;
        }

        let previous = std::mem::replace(slot, ConnSlot::Vacant);
        let ConnSlot::Closing(closing) = previous else {
            unreachable!("state checked above");
        };
        *slot = ConnSlot::Drained(closing.drain());
        let drained = std::mem::replace(slot, ConnSlot::Vacant);
        let ConnSlot::Drained(drained) = drained else {
            unreachable!("drained state installed above");
        };
        self.len -= 1;
        self.free.push(token);
        Some(drained)
    }

    /// Gets a shared reference to connection metadata.
    pub fn get(&self, token: usize) -> Option<&ConnectionMeta> {
        self.slots.get(token).and_then(ConnSlot::meta)
    }

    /// Gets a mutable reference to connection metadata.
    pub fn get_mut(&mut self, token: usize) -> Option<&mut ConnectionMeta> {
        self.slots.get_mut(token).and_then(ConnSlot::meta_mut)
    }

    /// Returns `true` when the slot is closing.
    pub fn is_closing(&self, token: usize) -> bool {
        matches!(self.slots.get(token), Some(ConnSlot::Closing(_)))
    }

    /// Returns the lifecycle state for a slot token.
    pub fn state(&self, token: usize) -> ConnectionState {
        self.slots
            .get(token)
            .map_or(ConnectionState::Vacant, ConnSlot::state)
    }

    /// Returns the number of active or closing connections.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if no connections are tracked.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns an iterator over `(token, &ConnectionMeta)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (usize, &ConnectionMeta)> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(id, slot)| slot.meta().map(|meta| (id, meta)))
    }

    /// Returns an iterator over all active or closing connection IDs.
    pub fn ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.iter().map(|(id, _)| id)
    }

    /// Count slots in the given state.
    pub fn count_by_state(&self, state: ConnectionState) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.state() == state)
            .count()
    }

    fn take_slot(&mut self, token: usize) -> ConnSlot {
        let slot = self
            .slots
            .get_mut(token)
            .expect("connection slot token out of range");
        let previous = std::mem::replace(slot, ConnSlot::Vacant);
        if !matches!(previous, ConnSlot::Vacant) {
            self.len -= 1;
            self.free.push(token);
        }
        previous
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_size_is_64_bytes() {
        assert_eq!(std::mem::size_of::<ConnectionMeta>(), 64);
    }

    #[test]
    fn meta_alignment_is_64() {
        assert_eq!(std::mem::align_of::<ConnectionMeta>(), 64);
    }

    #[test]
    fn slab_insert_starts_active() {
        let mut slab = ConnectionSlab::with_capacity(16);
        let token = slab.insert(ConnectionMeta::new(42, 0));

        assert_eq!(slab.state(token), ConnectionState::Active);
        assert_eq!(slab.get(token).unwrap().fd, 42);
    }

    #[test]
    fn transition_active_to_closing() {
        let mut slab = ConnectionSlab::with_capacity(16);
        let token = slab.insert(ConnectionMeta::new(42, 0));

        slab.transition_to_closing(token).unwrap();

        assert_eq!(slab.state(token), ConnectionState::Closing);
        assert!(slab.is_closing(token));
    }

    #[test]
    fn transition_closing_to_closing_is_idempotent() {
        let mut slab = ConnectionSlab::with_capacity(16);
        let token = slab.insert(ConnectionMeta::new(42, 0));

        slab.transition_to_closing(token).unwrap();
        slab.transition_to_closing(token).unwrap();

        assert_eq!(slab.state(token), ConnectionState::Closing);
    }

    #[test]
    fn transition_vacant_to_closing_is_rejected() {
        let mut slab = ConnectionSlab::with_capacity(16);

        let error = slab.transition_to_closing(3).unwrap_err();

        assert_eq!(error.from, ConnectionState::Vacant);
        assert_eq!(error.to, ConnectionState::Closing);
    }

    #[test]
    fn slab_insert_remove() {
        let mut slab = ConnectionSlab::with_capacity(16);
        let token = slab.insert(ConnectionMeta::new(42, 0));
        assert_eq!(slab.len(), 1);

        let conn = slab.get(token).unwrap();
        assert_eq!(conn.fd, 42);
        assert_eq!(slab.state(token), ConnectionState::Active);

        slab.remove(token);
        assert!(slab.is_empty());
        assert_eq!(slab.state(token), ConnectionState::Vacant);
    }

    #[test]
    fn drain_closing_moves_to_vacant_and_reuses_slot() {
        let mut slab = ConnectionSlab::with_capacity(16);
        let token = slab.insert(ConnectionMeta::new(42, 0));
        slab.transition_to_closing(token).unwrap();
        slab.mark_close_submitted(token);

        let drained = slab.drain_closing(token).unwrap();

        assert!(!drained.needs_direct_close());
        assert_eq!(drained.into_meta().fd, 42);
        assert!(slab.is_empty());
        assert_eq!(slab.state(token), ConnectionState::Vacant);

        let reused = slab.insert(ConnectionMeta::new(43, 1));
        assert_eq!(reused, token);
        assert_eq!(slab.get(reused).unwrap().fd, 43);
    }

    #[test]
    fn slab_iter_and_count_by_state() {
        let mut slab = ConnectionSlab::with_capacity(16);
        let t1 = slab.insert(ConnectionMeta::new(1, 0));
        let _t2 = slab.insert(ConnectionMeta::new(2, 1));
        slab.transition_to_closing(t1).unwrap();

        assert_eq!(slab.count_by_state(ConnectionState::Active), 1);
        assert_eq!(slab.count_by_state(ConnectionState::Closing), 1);
        assert_eq!(slab.count_by_state(ConnectionState::Vacant), 0);
        assert_eq!(slab.iter().count(), 2);
    }
}
