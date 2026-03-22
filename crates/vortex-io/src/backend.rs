/// I/O backend trait — abstraction over io_uring and polling.
///
/// TODO: Phase 1 will implement the full trait with io_uring and polling backends.
/// Phase 0 defines the interface only.
pub trait IoBackend {
    /// Initialize the backend.
    fn init(&mut self) -> std::io::Result<()>;

    /// Wait for I/O events, blocking up to `timeout`.
    fn wait(&mut self, timeout: Option<std::time::Duration>) -> std::io::Result<usize>;

    /// Register a file descriptor for read readiness.
    fn register_read(&mut self, fd: i32, token: usize) -> std::io::Result<()>;

    /// Register a file descriptor for write readiness.
    fn register_write(&mut self, fd: i32, token: usize) -> std::io::Result<()>;

    /// Deregister a file descriptor.
    fn deregister(&mut self, fd: i32) -> std::io::Result<()>;
}

/// TODO: Placeholder polling backend (Phase 0 stub).
///
/// TODO: Phase 1 will implement this using the `polling` crate, wrapping
/// epoll (Linux) / kqueue (macOS/BSD) / IOCP (Windows).
pub struct PollingBackend;

impl IoBackend for PollingBackend {
    fn init(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn wait(&mut self, _timeout: Option<std::time::Duration>) -> std::io::Result<usize> {
        Ok(0)
    }

    fn register_read(&mut self, _fd: i32, _token: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn register_write(&mut self, _fd: i32, _token: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn deregister(&mut self, _fd: i32) -> std::io::Result<()> {
        Ok(())
    }
}

/// TODO: io_uring backend stub (Linux only).
#[cfg(target_os = "linux")]
pub struct IoUringBackend;

#[cfg(target_os = "linux")]
impl IoBackend for IoUringBackend {
    fn init(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn wait(&mut self, _timeout: Option<std::time::Duration>) -> std::io::Result<usize> {
        Ok(0)
    }

    fn register_read(&mut self, _fd: i32, _token: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn register_write(&mut self, _fd: i32, _token: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn deregister(&mut self, _fd: i32) -> std::io::Result<()> {
        Ok(())
    }
}
