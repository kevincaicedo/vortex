//! VortexDB — next-generation in-memory database server.

use std::sync::Arc;
use std::time::Duration;

use vortex_io::{ReactorPool, ReactorPoolConfig};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Default shutdown timeout before force-kill (30 seconds).
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

const BANNER: &str = r"
 __     __       _            ____  ____
 \ \   / /___  _| |_ _____  _|  _ \| __ )
  \ \ / / _ \| '_| __/ _ \ \/ / | | |  _ \
   \ V / (_) | |  | ||  __/>  <| |_| | |_) |
    \_/ \___/|_|   \__\___/_/\_\____/|____/
";

fn main() {
    let config = match vortex_config::VortexConfig::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("fatal: {e}");
            std::process::exit(1);
        }
    };

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    eprintln!("{BANNER}");
    tracing::info!(
        "VortexDB v{} starting — bind={}, threads={}, io_backend={}, max_memory={}",
        env!("CARGO_PKG_VERSION"),
        config.bind,
        config.threads,
        config.io_backend,
        config.max_memory,
    );

    // ── Spawn reactor pool ─────────────────────────────────────────
    let pool_config = ReactorPoolConfig {
        bind_addr: config.bind,
        threads: config.threads,
        max_connections: config.max_clients,
        buffer_size: config.buffer_size,
        buffer_count: config.fixed_buffers,
        connection_timeout: config.connection_timeout_secs as u32,
    };

    let mut pool = match ReactorPool::spawn(pool_config) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "failed to spawn reactor pool");
            std::process::exit(1);
        }
    };

    tracing::info!(
        reactors = pool.reactor_count(),
        "server ready — awaiting connections"
    );

    // ── Signal handler (1.4.2) ─────────────────────────────────────
    // TODO(Phase 1.5): Replace ctrlc with signalfd (Linux) / kqueue EVFILT_SIGNAL
    // (macOS) integration into each reactor's event loop for zero-polling signal
    // delivery. Also add SIGHUP handler for config-reload stub.
    {
        let coordinator = Arc::clone(pool.coordinator());
        ctrlc::set_handler(move || {
            if coordinator.initiate() {
                tracing::info!("shutdown signal received — draining connections");
            } else {
                // Second signal — escalate to force-kill.
                tracing::warn!("second signal received — forcing immediate shutdown");
                coordinator.force_kill();
            }
        })
        .expect("failed to set signal handler");
    }

    // ── Wait for shutdown ──────────────────────────────────────────
    let clean = pool.wait_for_shutdown(SHUTDOWN_TIMEOUT);
    pool.join();

    // ── Persistence flush stub (1.4.5) ─────────────────────────────
    persistence_flush();

    if clean {
        tracing::info!("VortexDB shutting down — goodbye");
        std::process::exit(0);
    } else {
        tracing::warn!("VortexDB forced shutdown — goodbye");
        std::process::exit(1);
    }
}

/// Flush persistence state to disk before exit.
///
/// TODO(Phase 5): Replace with actual AOF flush and final snapshot write.
fn persistence_flush() {
    tracing::info!("persistence flush (stub) — no persistence configured yet");
}
