//! Typed AOF error classification.

use std::error::Error;
use std::fmt;
use std::io;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AofErrorKind {
    Append,
    Flush,
    Fsync,
    WorkerStopped,
    UnsupportedFormat,
    CorruptHeader,
    CorruptRecord,
    TruncatedTail,
}

#[derive(Debug)]
pub struct AofError {
    kind: AofErrorKind,
    message: String,
    source: Option<io::Error>,
}

impl AofError {
    pub fn new(kind: AofErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(kind: AofErrorKind, message: impl Into<String>, source: io::Error) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(source),
        }
    }

    #[inline]
    pub const fn kind(&self) -> AofErrorKind {
        self.kind
    }
}

impl fmt::Display for AofError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for AofError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|error| error as &(dyn Error + 'static))
    }
}

pub(crate) fn aof_error(kind: AofErrorKind, message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, AofError::new(kind, message))
}

pub(crate) fn aof_invalid_input(kind: AofErrorKind, message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, AofError::new(kind, message))
}

pub(crate) fn aof_io_error(
    kind: AofErrorKind,
    message: impl Into<String>,
    error: io::Error,
) -> io::Error {
    let io_kind = error.kind();
    io::Error::new(io_kind, AofError::with_source(kind, message, error))
}

pub fn aof_error_kind(error: &io::Error) -> Option<AofErrorKind> {
    error
        .get_ref()?
        .downcast_ref::<AofError>()
        .map(AofError::kind)
}
