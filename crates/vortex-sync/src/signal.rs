//! Cross-thread wakeup primitive backed by an OS notification file descriptor.
//!
//! [`ShutdownSignal`] combines an [`AtomicBool`] flag with an OS-level
//! notification mechanism:
//!
//! - **Linux**: `eventfd(0, EFD_NONBLOCK)` — write `1u64` to signal, read to clear.
//! - **macOS/other**: `pipe` with `O_NONBLOCK` — write 1 byte to signal, read to clear.
//!
//! The raw file descriptor can be registered with io_uring or a poller to
//! wake a reactor blocked in `io_uring_enter()` or `Poller::wait()`.

use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};

/// Cross-thread wakeup signal backed by an OS file descriptor.
///
/// Allows one thread to wake another that is blocked on an event loop
/// (io_uring or polling) without busy-spinning.
pub struct ShutdownSignal {
    /// Fast-path flag — checked with a relaxed load.
    signaled: AtomicBool,
    /// Platform-specific notification state.
    inner: PlatformSignal,
}

/// Linux implementation using `eventfd`.
#[cfg(target_os = "linux")]
struct PlatformSignal {
    fd: RawFd,
}

/// macOS/other implementation using a pipe pair.
#[cfg(not(target_os = "linux"))]
struct PlatformSignal {
    /// Read end — registered with the poller/io_uring.
    read_fd: RawFd,
    /// Write end — written to on signal.
    write_fd: RawFd,
}

// --- Linux: eventfd ---

#[cfg(target_os = "linux")]
impl ShutdownSignal {
    /// Creates a new shutdown signal.
    pub fn new() -> std::io::Result<Self> {
        // SAFETY: eventfd creates a new file descriptor. EFD_NONBLOCK ensures
        // reads/writes never block. EFD_CLOEXEC prevents leaking to child processes.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(Self {
            signaled: AtomicBool::new(false),
            inner: PlatformSignal { fd },
        })
    }

    /// Signal the shutdown — sets the flag and writes to the eventfd.
    pub fn signal(&self) {
        if self.signaled.swap(true, Ordering::Release) {
            // Already signaled — avoid redundant write.
            return;
        }
        let val: u64 = 1;
        // SAFETY: `self.inner.fd` is a valid eventfd. Writing an 8-byte u64
        // is the documented eventfd write protocol. Non-blocking so this
        // never blocks.
        unsafe {
            libc::write(self.inner.fd, (&val as *const u64).cast(), 8);
        }
    }

    /// Returns the raw file descriptor for registration with io_uring or a poller.
    #[inline]
    pub fn as_raw_fd(&self) -> RawFd {
        self.inner.fd
    }

    /// Clears the signal by reading from the eventfd.
    pub fn clear(&self) {
        self.signaled.store(false, Ordering::Release);
        let mut buf: u64 = 0;
        // SAFETY: Reading 8 bytes from an eventfd is the documented clear protocol.
        unsafe {
            libc::read(self.inner.fd, (&mut buf as *mut u64).cast(), 8);
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for ShutdownSignal {
    fn drop(&mut self) {
        // SAFETY: `self.inner.fd` is a valid eventfd we own.
        unsafe {
            libc::close(self.inner.fd);
        }
    }
}

// --- macOS/other: pipe ---

#[cfg(not(target_os = "linux"))]
impl ShutdownSignal {
    /// Creates a new shutdown signal backed by a pipe pair.
    pub fn new() -> std::io::Result<Self> {
        let mut fds = [0i32; 2];
        // SAFETY: `pipe` creates two new file descriptors in the provided array.
        let ret = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if ret < 0 {
            return Err(std::io::Error::last_os_error());
        }

        // Set both ends to non-blocking and close-on-exec.
        for &fd in &fds {
            // SAFETY: `fd` is a valid file descriptor from `pipe`.
            unsafe {
                let flags = libc::fcntl(fd, libc::F_GETFL);
                libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
        }

        Ok(Self {
            signaled: AtomicBool::new(false),
            inner: PlatformSignal {
                read_fd: fds[0],
                write_fd: fds[1],
            },
        })
    }

    /// Signal the shutdown — sets the flag and writes to the pipe.
    pub fn signal(&self) {
        if self.signaled.swap(true, Ordering::Release) {
            return;
        }
        let byte: u8 = 1;
        // SAFETY: `write_fd` is a valid pipe write end. Writing 1 byte is safe.
        unsafe {
            libc::write(self.inner.write_fd, (&byte as *const u8).cast(), 1);
        }
    }

    /// Returns the read end of the pipe for registration with a poller.
    #[inline]
    pub fn as_raw_fd(&self) -> RawFd {
        self.inner.read_fd
    }

    /// Clears the signal by draining the pipe.
    pub fn clear(&self) {
        self.signaled.store(false, Ordering::Release);
        let mut buf = [0u8; 64];
        // SAFETY: Reads up to 64 bytes from the pipe read end to drain it.
        unsafe {
            libc::read(self.inner.read_fd, buf.as_mut_ptr().cast(), buf.len());
        }
    }
}

#[cfg(not(target_os = "linux"))]
impl Drop for ShutdownSignal {
    fn drop(&mut self) {
        // SAFETY: Both fds are valid pipe file descriptors we own.
        unsafe {
            libc::close(self.inner.read_fd);
            libc::close(self.inner.write_fd);
        }
    }
}

// --- Shared API ---

impl ShutdownSignal {
    /// Returns `true` if the signal has been set.
    #[inline]
    pub fn is_signaled(&self) -> bool {
        self.signaled.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn signal_and_check() {
        let sig = ShutdownSignal::new().unwrap();
        assert!(!sig.is_signaled());

        sig.signal();
        assert!(sig.is_signaled());
    }

    #[test]
    fn signal_is_idempotent() {
        let sig = ShutdownSignal::new().unwrap();
        sig.signal();
        sig.signal();
        assert!(sig.is_signaled());
    }

    #[test]
    fn clear_resets_state() {
        let sig = ShutdownSignal::new().unwrap();
        sig.signal();
        assert!(sig.is_signaled());

        sig.clear();
        assert!(!sig.is_signaled());
    }

    #[test]
    fn fd_is_valid() {
        let sig = ShutdownSignal::new().unwrap();
        assert!(sig.as_raw_fd() >= 0);
    }

    #[test]
    fn cross_thread_signal() {
        let sig = Arc::new(ShutdownSignal::new().unwrap());
        let sig2 = Arc::clone(&sig);

        let handle = std::thread::spawn(move || {
            sig2.signal();
        });

        handle.join().unwrap();
        assert!(sig.is_signaled());
    }

    #[test]
    fn fd_readable_after_signal() {
        let sig = ShutdownSignal::new().unwrap();
        sig.signal();

        // Verify the fd is readable via poll.
        let mut pfd = libc::pollfd {
            fd: sig.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };

        // SAFETY: Valid pollfd struct, 0ms timeout (non-blocking check).
        let ret = unsafe { libc::poll(&mut pfd, 1, 0) };
        assert_eq!(ret, 1, "fd should be readable after signal");
        assert_ne!(pfd.revents & libc::POLLIN, 0);
    }
}
