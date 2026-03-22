/// Unified error type for VortexDB.
///
/// Produces Redis-compatible error strings where appropriate
/// (e.g., `WRONGTYPE Operation against a key holding the wrong kind of value`).
#[derive(Debug, thiserror::Error)]
pub enum VortexError {
    /// RESP protocol errors (malformed frame, wrong type, etc.)
    #[error("ERR protocol error: {0}")]
    Protocol(String),

    /// Storage / data-type errors.
    #[error("{0}")]
    Storage(StorageError),

    /// I/O errors from the reactor or persistence layer.
    #[error("ERR I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Configuration errors.
    #[error("ERR config error: {0}")]
    Config(String),

    /// Replication errors.
    #[error("ERR replication error: {0}")]
    Replication(String),

    /// Cluster protocol errors.
    #[error("{0}")]
    Cluster(ClusterError),

    /// Authentication / ACL errors.
    #[error("{0}")]
    Auth(AuthError),
}

/// Storage-layer errors with Redis-compatible messages.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR no such key")]
    KeyNotFound,

    #[error("ERR value is not an integer or out of range")]
    NotAnInteger,

    #[error("ERR index out of range")]
    IndexOutOfRange,

    #[error("ERR syntax error")]
    SyntaxError,

    #[error("ERR {0}")]
    Other(String),
}

/// Cluster-specific errors.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("MOVED {slot} {addr}")]
    Moved { slot: u16, addr: String },

    #[error("ASK {slot} {addr}")]
    Ask { slot: u16, addr: String },

    #[error("CLUSTERDOWN The cluster is down")]
    ClusterDown,
}

/// Authentication errors.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("NOAUTH Authentication required.")]
    NoAuth,

    #[error("ERR invalid password")]
    InvalidPassword,

    #[error("NOPERM this user has no permissions to run the '{command}' command")]
    NoPermission { command: String },
}

/// Convenience result alias.
pub type VortexResult<T> = Result<T, VortexError>;

impl From<StorageError> for VortexError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}

impl From<ClusterError> for VortexError {
    fn from(e: ClusterError) -> Self {
        Self::Cluster(e)
    }
}

impl From<AuthError> for VortexError {
    fn from(e: AuthError) -> Self {
        Self::Auth(e)
    }
}
