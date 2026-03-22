use slab::Slab;
use std::net::SocketAddr;

/// Connection states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection accepted, awaiting first data.
    New,
    /// Normal connected state.
    Active,
    /// Connection is closing.
    Closing,
}

/// Per-client connection state.
pub struct Connection {
    /// Remote peer address.
    pub addr: SocketAddr,
    /// Current connection state.
    pub state: ConnectionState,
    /// Read buffer.
    pub read_buf: Vec<u8>,
    /// Write buffer.
    pub write_buf: Vec<u8>,
    /// Last activity timestamp (nanoseconds).
    pub last_active: u64,
}

impl Connection {
    /// Creates a new connection.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            state: ConnectionState::New,
            read_buf: Vec::with_capacity(4096),
            write_buf: Vec::with_capacity(4096),
            last_active: 0,
        }
    }
}

/// Slab-allocated connection pool.
///
/// Dense array with O(1) insert/remove. Uses the `slab` crate internally.
pub struct ConnectionSlab {
    inner: Slab<Connection>,
}

impl ConnectionSlab {
    /// Creates a new connection slab with pre-allocated capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: Slab::with_capacity(cap),
        }
    }

    /// Inserts a connection and returns its token.
    pub fn insert(&mut self, conn: Connection) -> usize {
        self.inner.insert(conn)
    }

    /// Removes a connection by token.
    pub fn remove(&mut self, token: usize) -> Connection {
        self.inner.remove(token)
    }

    /// Gets a reference to a connection by token.
    pub fn get(&self, token: usize) -> Option<&Connection> {
        self.inner.get(token)
    }

    /// Gets a mutable reference to a connection by token.
    pub fn get_mut(&mut self, token: usize) -> Option<&mut Connection> {
        self.inner.get_mut(token)
    }

    /// Returns the number of active connections.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if no connections are tracked.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slab_insert_remove() {
        let mut slab = ConnectionSlab::with_capacity(16);
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        let token = slab.insert(Connection::new(addr));
        assert_eq!(slab.len(), 1);

        let conn = slab.get(token).unwrap();
        assert_eq!(conn.addr, addr);
        assert_eq!(conn.state, ConnectionState::New);

        slab.remove(token);
        assert!(slab.is_empty());
    }
}
