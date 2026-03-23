//! Listener creation with `SO_REUSEPORT` and platform-specific optimizations.
//!
//! Centralises TCP listener socket setup so both [`Reactor::new`] and
//! [`ReactorPool::spawn`] use identical options. On Linux, additionally sets
//! `SO_INCOMING_CPU` to hint the kernel to route incoming connections to the
//! specified CPU core's receive queue, reducing cross-core cache migration.

use std::os::fd::RawFd;

use socket2::{Domain, Protocol, SockAddr, Socket, Type};

/// Create a TCP listener socket configured for multi-reactor use.
///
/// Sets `SO_REUSEADDR`, `SO_REUSEPORT`, `TCP_NODELAY`, and non-blocking mode.
/// On Linux, additionally sets `SO_INCOMING_CPU` when `core_hint` is provided.
pub fn create_listener(
    bind_addr: std::net::SocketAddr,
    core_hint: Option<usize>,
) -> std::io::Result<RawFd> {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.set_nodelay(true)?;

    // On Linux, hint the kernel to direct incoming packets to this CPU's
    // receive queue, reducing cross-core cache migration.
    #[cfg(target_os = "linux")]
    if let Some(cpu) = core_hint {
        use std::os::fd::AsRawFd;
        // SAFETY: `socket` is a valid, open socket fd. The pointer and length
        // arguments are correct for a `c_int` value.
        unsafe {
            let cpu_val: libc::c_int = cpu as libc::c_int;
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_INCOMING_CPU,
                &cpu_val as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }

    // Suppress unused variable warning on non-Linux platforms.
    let _ = core_hint;

    let addr = SockAddr::from(bind_addr);
    socket.bind(&addr)?;
    socket.listen(1024)?;

    let fd = {
        use std::os::fd::IntoRawFd;
        socket.into_raw_fd()
    };

    tracing::debug!(fd, addr = %bind_addr, "listener created");
    Ok(fd)
}
