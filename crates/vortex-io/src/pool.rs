//! Multi-reactor orchestration: spawning and CPU pinning.
//!
//! [`ReactorPool`] spawns one reactor per CPU core (or a configured count),
//! pins each thread to its core, and manages lifecycle
//! (startup → running → drain → shutdown).
//!
//! The pool creates a single shared [`ConcurrentKeyspace`] and distributes
//! `Arc` handles to every reactor. AOF replay happens once at the pool level
//! before spawning reactor threads.

use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use vortex_engine::eviction::EvictionPolicy;
use vortex_engine::keyspace::{ConcurrentKeyspace, DEFAULT_SHARD_COUNT};
use vortex_persist::aof::reader::AofReader;

use crate::reactor::{AofConfig, AofFatalState, Reactor, ReactorConfig};
use crate::shutdown::ShutdownCoordinator;

/// I/O backend selection for pool-managed reactors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IoBackendMode {
    /// Prefer io_uring when it is available, otherwise fall back to polling.
    #[default]
    Auto,
    /// Require io_uring and fail startup if it cannot be constructed.
    Uring,
    /// Use the polling backend.
    Polling,
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
    /// Number of ConcurrentKeyspace shards (must be power of 2, default 4096).
    pub shard_count: usize,
    /// Max memory in bytes for eviction enforcement (0 = unlimited).
    pub max_memory: usize,
    /// Runtime eviction policy for the shared keyspace.
    pub eviction_policy: EvictionPolicy,
    /// I/O backend selection.
    pub io_backend: IoBackendMode,
    /// io_uring submission queue size.
    pub ring_size: u32,
    /// SQPOLL idle timeout in milliseconds.
    pub sqpoll_idle_ms: u32,
}

impl Default for ReactorPoolConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:6379".parse().expect("valid default addr"),
            threads: 0,
            max_connections: 10_000,
            buffer_size: 16_384,
            buffer_count: 20_000,
            connection_timeout: 300,
            aof_config: None,
            shard_count: DEFAULT_SHARD_COUNT,
            max_memory: 0,
            eviction_policy: EvictionPolicy::NoEviction,
            io_backend: IoBackendMode::Auto,
            ring_size: 4096,
            sqpoll_idle_ms: 1000,
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
/// All reactors share a single [`ConcurrentKeyspace`] via `Arc`. The pool
/// owns the keyspace lifetime and handles AOF replay before spawning.
pub struct ReactorPool {
    handles: Vec<ReactorHandle>,
    coordinator: Arc<ShutdownCoordinator>,
    /// Shared keyspace across all reactors.
    keyspace: Arc<ConcurrentKeyspace>,
}

fn bounded_reactor_count(requested: usize, max_connections: usize) -> usize {
    requested.min(max_connections.max(1))
}

fn share_evenly(total: usize, buckets: usize, index: usize) -> usize {
    debug_assert!(buckets > 0);
    debug_assert!(index < buckets);

    let base = total / buckets;
    let remainder = total % buckets;
    base + usize::from(index < remainder)
}

fn reactor_resource_share(
    total_connections: usize,
    total_buffers: usize,
    num_reactors: usize,
    reactor_idx: usize,
) -> (usize, usize) {
    let reactor_connections = share_evenly(total_connections, num_reactors, reactor_idx);
    let required_buffers = total_connections.saturating_mul(2);
    let spare_buffers = total_buffers - required_buffers;
    let reactor_buffers = reactor_connections.saturating_mul(2)
        + share_evenly(spare_buffers, num_reactors, reactor_idx);
    (reactor_connections, reactor_buffers)
}

fn join_handles(handles: &mut [ReactorHandle]) {
    for handle in handles {
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

impl ReactorPool {
    /// Spawn `N` reactor threads (one per CPU core by default).
    ///
    /// Creates a shared `ConcurrentKeyspace`, replays AOF if configured,
    /// then pins each reactor thread to a CPU core with its own
    /// SO_REUSEPORT listener.
    pub fn spawn(config: ReactorPoolConfig) -> std::io::Result<Self> {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        let requested_reactors = if config.threads == 0 {
            core_ids.len().max(1)
        } else {
            config.threads
        };
        let num_reactors = bounded_reactor_count(requested_reactors, config.max_connections);

        let min_buffer_count = Reactor::min_buffer_count_for_connections(config.max_connections)?;
        if config.buffer_count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "buffer_count must be greater than zero",
            ));
        }
        if config.buffer_count < min_buffer_count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "buffer_count ({}) must be at least max_connections * 2 ({min_buffer_count})",
                    config.buffer_count
                ),
            ));
        }

        let coordinator = Arc::new(ShutdownCoordinator::new(num_reactors));
        let aof_fatal_state = Arc::new(AofFatalState::default());

        // ── Create shared ConcurrentKeyspace ────────────────────────
        let keyspace = Arc::new(ConcurrentKeyspace::new_with_runtime_slots(
            config.shard_count,
            num_reactors,
        ));
        tracing::info!(
            shard_count = config.shard_count,
            max_memory = config.max_memory,
            eviction_policy = config.eviction_policy.as_str(),
            "shared ConcurrentKeyspace created"
        );

        // ── AOF replay into shared keyspace ─────────────────────────
        // Collect all per-reactor AOF file paths, then replay via K-Way
        // merge (ordered by global LSN) into the shared keyspace ONCE,
        // before spawning reactor threads.
        if let Some(ref aof_cfg) = config.aof_config {
            let mut aof_paths = Vec::with_capacity(num_reactors);
            for i in 0..num_reactors {
                let aof_path = if i == 0 {
                    aof_cfg.path.clone()
                } else {
                    let stem = aof_cfg
                        .path
                        .file_stem()
                        .unwrap_or_default()
                        .to_string_lossy();
                    let ext = aof_cfg
                        .path
                        .extension()
                        .unwrap_or_default()
                        .to_string_lossy();
                    aof_cfg
                        .path
                        .with_file_name(format!("{stem}-shard{i}.{ext}"))
                };
                aof_paths.push(aof_path);
            }

            tracing::info!(
                num_files = aof_paths.len(),
                "starting K-Way merge AOF replay into shared keyspace..."
            );
            let stats = AofReader::replay_merge(&aof_paths, &keyspace)?;
            tracing::info!(
                files_merged = stats.files_merged,
                commands = stats.commands_replayed,
                bytes = stats.bytes_read,
                max_lsn = stats.max_lsn,
                duration_ms = stats.duration_ms,
                "AOF K-Way merge replay complete"
            );
        }

        keyspace.configure_eviction(config.max_memory, config.eviction_policy);

        let mut handles = Vec::with_capacity(num_reactors);
        let (startup_tx, startup_rx) = mpsc::channel();

        for i in 0..num_reactors {
            let (reactor_max_connections, reactor_buffer_count) = reactor_resource_share(
                config.max_connections,
                config.buffer_count,
                num_reactors,
                i,
            );
            let core_id = core_ids.get(i).copied();
            let coord_clone = Arc::clone(&coordinator);
            let ks_clone = Arc::clone(&keyspace);
            let aof_fatal_state_clone = Arc::clone(&aof_fatal_state);
            let startup_tx = startup_tx.clone();

            let reactor_config = ReactorConfig {
                bind_addr: config.bind_addr,
                max_connections: reactor_max_connections,
                buffer_size: config.buffer_size,
                buffer_count: reactor_buffer_count,
                connection_timeout: config.connection_timeout,
                aof_config: config.aof_config.clone(),
                io_backend: config.io_backend,
                ring_size: config.ring_size,
                sqpoll_idle_ms: config.sqpoll_idle_ms,
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

                    let mut reactor = match Reactor::with_shared_keyspace_and_aof_state(
                        i,
                        reactor_config,
                        Arc::clone(&coord_clone),
                        Arc::clone(&ks_clone),
                        Arc::clone(&aof_fatal_state_clone),
                        num_reactors,
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            let _ = startup_tx.send((i, Err(e)));
                            coord_clone.reactor_finished(i);
                            tracing::error!(
                                reactor_id = i,
                                error = "startup failed before entering reactor loop",
                                "failed to create reactor"
                            );
                            return;
                        }
                    };

                    if startup_tx.send((i, Ok(()))).is_err() {
                        coord_clone.reactor_finished(i);
                        return;
                    }

                    reactor.run();
                })?;

            handles.push(ReactorHandle {
                thread: Some(thread),
                reactor_id: i,
                core_id,
            });
        }

        drop(startup_tx);

        let mut started_reactors = 0usize;
        let mut startup_failure: Option<(usize, std::io::Error)> = None;

        for _ in 0..num_reactors {
            match startup_rx.recv() {
                Ok((_, Ok(()))) => started_reactors += 1,
                Ok((reactor_id, Err(error))) => {
                    if startup_failure.is_none() {
                        startup_failure = Some((reactor_id, error));
                    }
                }
                Err(recv_error) => {
                    if startup_failure.is_none() {
                        startup_failure = Some((
                            usize::MAX,
                            std::io::Error::other(format!(
                                "reactor startup channel closed unexpectedly: {recv_error}"
                            )),
                        ));
                    }
                    break;
                }
            }
        }

        if let Some((reactor_id, error)) = startup_failure {
            coordinator.initiate();
            join_handles(&mut handles);

            let reactor_label = if reactor_id == usize::MAX {
                "unknown".to_owned()
            } else {
                reactor_id.to_string()
            };

            return Err(std::io::Error::new(
                error.kind(),
                format!(
                    "reactor pool startup failed after starting {started_reactors}/{num_reactors} reactors; reactor {reactor_label} failed: {error}"
                ),
            ));
        }

        tracing::info!(num_reactors, "reactor pool started");

        Ok(Self {
            handles,
            coordinator,
            keyspace,
        })
    }

    /// Signal all reactors to shut down gracefully.
    pub fn shutdown(&self) {
        self.coordinator.initiate();
    }

    /// Wait for all reactor threads to finish.
    pub fn join(&mut self) {
        join_handles(&mut self.handles);
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

    /// Returns a reference to the shared keyspace.
    pub fn keyspace(&self) -> &Arc<ConcurrentKeyspace> {
        &self.keyspace
    }
}

impl Drop for ReactorPool {
    fn drop(&mut self) {
        // Ensure all reactor threads are stopped and joined on drop.
        self.coordinator.initiate();
        self.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_reactor_count_respects_connection_budget() {
        assert_eq!(bounded_reactor_count(8, 3), 3);
        assert_eq!(bounded_reactor_count(4, 0), 1);
        assert_eq!(bounded_reactor_count(2, 10), 2);
    }

    #[test]
    fn reactor_resource_share_preserves_totals_and_invariant() {
        let shares: Vec<_> = (0..2)
            .map(|index| reactor_resource_share(5, 12, 2, index))
            .collect();

        assert_eq!(shares, vec![(3, 7), (2, 5)]);
        assert_eq!(
            shares
                .iter()
                .map(|(connections, _)| connections)
                .sum::<usize>(),
            5
        );
        assert_eq!(shares.iter().map(|(_, buffers)| buffers).sum::<usize>(), 12);
        assert!(
            shares
                .iter()
                .all(|(connections, buffers)| *buffers >= (*connections * 2))
        );
    }

    #[test]
    fn spawn_returns_error_when_listener_port_is_already_bound() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let bind_addr = listener.local_addr().unwrap();

        let error = match ReactorPool::spawn(ReactorPoolConfig {
            bind_addr,
            threads: 1,
            max_connections: 1,
            buffer_count: 2,
            io_backend: IoBackendMode::Polling,
            ..ReactorPoolConfig::default()
        }) {
            Ok(_) => panic!("expected spawn to fail when the listener port is already bound"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        assert!(error.to_string().contains("reactor pool startup failed"));
    }
}
