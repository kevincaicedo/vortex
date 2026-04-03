//! Multi-reactor orchestration: spawning, CPU pinning, and cross-reactor messaging.
//!
//! [`ReactorPool`] spawns one reactor per CPU core (or a configured count),
//! pins each thread to its core, creates the SPSC cross-reactor messaging
//! mesh, and manages lifecycle (startup → running → drain → shutdown).

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use vortex_sync::SpscRingBuffer;

use crate::reactor::{AofConfig, Reactor, ReactorConfig};
use crate::shutdown::ShutdownCoordinator;

/// Capacity of each cross-reactor SPSC ring buffer.
pub const CROSS_CHANNEL_CAP: usize = 4096;

/// Type alias for a cross-reactor SPSC channel (shared via `Arc`).
pub type CrossChannel = Arc<SpscRingBuffer<CrossMessage, CROSS_CHANNEL_CAP>>;

/// Messages exchanged between reactors via SPSC channels.
#[derive(Debug)]
pub enum CrossMessage {
    /// Shutdown signal — reactor enters drain mode.
    Shutdown,
    /// Health-check ping.
    Ping,
    // TODO(Phase 3): TransferConnection { fd: RawFd, addr: SocketAddr }
}

/// Configuration for the reactor pool.
pub struct ReactorPoolConfig {
    /// Address to bind all reactor listeners on.
    pub bind_addr: std::net::SocketAddr,
    /// Number of reactor threads (0 = auto-detect from CPU count).
    pub threads: usize,
    /// Maximum total client connections (distributed across reactors).
    pub max_connections: usize,
    /// Read buffer size in bytes per connection.
    pub buffer_size: usize,
    /// Total pre-allocated I/O buffers (distributed across reactors).
    pub buffer_count: usize,
    /// Idle connection timeout in seconds (0 = disabled).
    pub connection_timeout: u32,
    /// AOF persistence configuration (None = disabled).
    pub aof_config: Option<AofConfig>,
}

impl Default for ReactorPoolConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:6379".parse().expect("valid default addr"),
            threads: 0,
            max_connections: 10_000,
            buffer_size: 16_384,
            buffer_count: 1024,
            connection_timeout: 300,
            aof_config: None,
        }
    }
}

/// Handle to a running reactor thread.
struct ReactorHandle {
    thread: Option<JoinHandle<()>>,
    reactor_id: usize,
    #[allow(dead_code)]
    core_id: Option<core_affinity::CoreId>,
}

/// Pool of reactor threads — one per CPU core.
///
/// Each reactor is pinned to a CPU core and owns its own listener socket
/// with `SO_REUSEPORT`. Cross-reactor communication uses per-pair SPSC
/// ring buffers with no locks on the hot path.
pub struct ReactorPool {
    handles: Vec<ReactorHandle>,
    coordinator: Arc<ShutdownCoordinator>,
    /// Channel mesh: `cross_channels[from][to]` for sending from reactor
    /// `from` to reactor `to`. Entry is `None` when `from == to`.
    #[allow(dead_code)]
    cross_channels: Vec<Vec<Option<CrossChannel>>>,
}

impl ReactorPool {
    /// Spawn `N` reactor threads (one per CPU core by default).
    ///
    /// Each thread is pinned to a CPU core via `core_affinity`, creates its
    /// own SO_REUSEPORT listener, and runs an independent event loop.
    pub fn spawn(config: ReactorPoolConfig) -> std::io::Result<Self> {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        let num_reactors = if config.threads == 0 {
            core_ids.len().max(1)
        } else {
            config.threads
        };

        let coordinator = Arc::new(ShutdownCoordinator::new(num_reactors));

        // Build the N×N cross-reactor SPSC channel mesh.
        let mut cross_channels: Vec<Vec<Option<CrossChannel>>> = Vec::with_capacity(num_reactors);
        for from in 0..num_reactors {
            let mut row = Vec::with_capacity(num_reactors);
            for to in 0..num_reactors {
                if from == to {
                    row.push(None);
                } else {
                    row.push(Some(Arc::new(SpscRingBuffer::new())));
                }
            }
            cross_channels.push(row);
        }

        // Distribute resources evenly across reactors.
        let per_reactor_conns = (config.max_connections / num_reactors).max(1);
        let per_reactor_bufs = (config.buffer_count / num_reactors).max(1);

        let mut handles = Vec::with_capacity(num_reactors);

        for (i, _) in cross_channels.iter().enumerate().take(num_reactors) {
            let core_id = core_ids.get(i).copied();
            let coord_clone = Arc::clone(&coordinator);

            // Gather incoming channels for this reactor: channels[j][i] for all j ≠ i.
            let incoming: Vec<CrossChannel> = (0..num_reactors)
                .filter(|&j| j != i)
                .filter_map(|j| cross_channels[j][i].as_ref().map(Arc::clone))
                .collect();

            let reactor_config = ReactorConfig {
                bind_addr: config.bind_addr,
                max_connections: per_reactor_conns,
                buffer_size: config.buffer_size,
                buffer_count: per_reactor_bufs,
                connection_timeout: config.connection_timeout,
                aof_config: config.aof_config.clone(),
            };

            let thread = std::thread::Builder::new()
                .name(format!("vortex-reactor-{i}"))
                .spawn(move || {
                    // Pin to CPU core.
                    if let Some(cid) = core_id {
                        if !core_affinity::set_for_current(cid) {
                            tracing::warn!(
                                reactor_id = i,
                                "failed to pin to CPU core, continuing without affinity"
                            );
                        } else {
                            tracing::debug!(reactor_id = i, "pinned to CPU core {:?}", cid);
                        }
                    }

                    let mut reactor = match Reactor::new(i, reactor_config, coord_clone) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!(reactor_id = i, error = %e, "failed to create reactor");
                            return;
                        }
                    };

                    // Install cross-reactor incoming channels.
                    reactor.set_cross_channels(incoming);

                    reactor.run();
                })?;

            handles.push(ReactorHandle {
                thread: Some(thread),
                reactor_id: i,
                core_id,
            });
        }

        tracing::info!(num_reactors, "reactor pool started");

        Ok(Self {
            handles,
            coordinator,
            cross_channels,
        })
    }

    /// Signal all reactors to shut down gracefully.
    pub fn shutdown(&self) {
        self.coordinator.initiate();
    }

    /// Wait for all reactor threads to finish.
    pub fn join(&mut self) {
        for handle in &mut self.handles {
            if let Some(thread) = handle.thread.take() {
                if let Err(e) = thread.join() {
                    tracing::error!(
                        reactor_id = handle.reactor_id,
                        "reactor thread panicked: {:?}",
                        e
                    );
                }
            }
        }
    }

    /// Block until all reactors finish or the timeout expires.
    ///
    /// Returns `true` on clean shutdown, `false` if force-kill was triggered.
    /// After this returns, call [`join`](Self::join) to reap threads.
    pub fn wait_for_shutdown(&self, timeout: Duration) -> bool {
        self.coordinator.wait_for_shutdown(timeout)
    }

    /// Returns the number of reactor threads.
    pub fn reactor_count(&self) -> usize {
        self.handles.len()
    }

    /// Returns a reference to the shared shutdown coordinator.
    pub fn coordinator(&self) -> &Arc<ShutdownCoordinator> {
        &self.coordinator
    }
}

impl Drop for ReactorPool {
    fn drop(&mut self) {
        // Ensure all reactor threads are stopped and joined on drop.
        self.coordinator.initiate();
        self.join();
    }
}
