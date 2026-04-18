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
use std::thread::JoinHandle;
use std::time::Duration;

use vortex_engine::concurrent_keyspace::{ConcurrentKeyspace, DEFAULT_SHARD_COUNT};
use vortex_persist::aof::reader::AofReader;

use crate::reactor::{AofConfig, Reactor, ReactorConfig};
use crate::shutdown::ShutdownCoordinator;

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
            shard_count: DEFAULT_SHARD_COUNT,
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

impl ReactorPool {
    /// Spawn `N` reactor threads (one per CPU core by default).
    ///
    /// Creates a shared `ConcurrentKeyspace`, replays AOF if configured,
    /// then pins each reactor thread to a CPU core with its own
    /// SO_REUSEPORT listener.
    pub fn spawn(config: ReactorPoolConfig) -> std::io::Result<Self> {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        let num_reactors = if config.threads == 0 {
            core_ids.len().max(1)
        } else {
            config.threads
        };

        let coordinator = Arc::new(ShutdownCoordinator::new(num_reactors));

        // ── Create shared ConcurrentKeyspace ────────────────────────
        let keyspace = Arc::new(ConcurrentKeyspace::new(config.shard_count));
        tracing::info!(
            shard_count = config.shard_count,
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
            match AofReader::replay_merge(&aof_paths, &keyspace) {
                Ok(stats) => {
                    tracing::info!(
                        files_merged = stats.files_merged,
                        commands = stats.commands_replayed,
                        bytes = stats.bytes_read,
                        max_lsn = stats.max_lsn,
                        duration_ms = stats.duration_ms,
                        "AOF K-Way merge replay complete"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "AOF K-Way merge replay failed — data may be missing"
                    );
                }
            }
        }

        // Distribute resources evenly across reactors.
        let per_reactor_conns = (config.max_connections / num_reactors).max(1);
        let per_reactor_bufs = (config.buffer_count / num_reactors).max(1);

        let mut handles = Vec::with_capacity(num_reactors);

        for i in 0..num_reactors {
            let core_id = core_ids.get(i).copied();
            let coord_clone = Arc::clone(&coordinator);
            let ks_clone = Arc::clone(&keyspace);

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

                    let mut reactor = match Reactor::with_shared_keyspace(
                        i,
                        reactor_config,
                        coord_clone,
                        ks_clone,
                        num_reactors,
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::error!(
                                reactor_id = i,
                                error = %e,
                                "failed to create reactor"
                            );
                            return;
                        }
                    };

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
            keyspace,
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
