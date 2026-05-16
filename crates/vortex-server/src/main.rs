//! VortexDB — next-generation in-memory database server.

use std::sync::Arc;
use std::time::Duration;

use vortex_engine::eviction::EvictionPolicy;
use vortex_engine::keyspace::RuntimeTelemetryMode;
use vortex_io::{
    AcceptBudget, AofConfig, CommandBudget, CompletionBudget, EngineTopologyMode,
    FixedBufferRegistrationMode, IoBackendMode, MaintenanceBudget, ReactorBudgets, ReactorPool,
    ReactorPoolConfig, TimeBudget, WritevBudget,
};

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

fn invalid_budget(name: &'static str) -> ! {
    tracing::error!(
        budget = name,
        "invalid zero reactor budget after config validation"
    );
    std::process::exit(1);
}

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
        "VortexDB v{} starting — bind={}, threads={}, shard_count={}, engine_topology={}, io_backend={}, fixed_buffer_registration={}, telemetry_mode={}, max_memory={}",
        env!("CARGO_PKG_VERSION"),
        config.bind,
        config.threads,
        config.shard_count,
        config.engine_topology,
        config.io_backend,
        config.fixed_buffer_registration,
        config.telemetry_mode,
        config.max_memory,
    );

    let effective_max_clients = config.effective_max_clients();
    if effective_max_clients < config.max_clients {
        tracing::warn!(
            requested_max_clients = config.max_clients,
            effective_max_clients,
            fixed_buffers = config.fixed_buffers,
            buffer_size = config.buffer_size,
            "fixed buffer pool can sustain fewer active clients than max_clients; capping runtime connection budget"
        );
    }

    // ── Spawn reactor pool ─────────────────────────────────────────
    let aof_config = if config.aof_enabled {
        let policy = match config.aof_fsync.as_str() {
            "always" => vortex_persist::aof::AofFsyncPolicy::Always,
            "no" => vortex_persist::aof::AofFsyncPolicy::No,
            _ => vortex_persist::aof::AofFsyncPolicy::Everysec,
        };
        tracing::info!(
            path = %config.aof_path.display(),
            fsync = %config.aof_fsync,
            max_pending_fsync_bytes = config.aof_max_pending_fsync_bytes,
            "AOF persistence enabled"
        );
        Some(AofConfig {
            path: config.aof_path.clone(),
            fsync_policy: policy,
            max_pending_fsync_bytes: config.aof_max_pending_fsync_bytes,
        })
    } else {
        None
    };

    let budgets = ReactorBudgets {
        completion: CompletionBudget::new(config.reactor_completion_budget)
            .unwrap_or_else(|| invalid_budget("completion")),
        command: CommandBudget::new(config.reactor_command_budget)
            .unwrap_or_else(|| invalid_budget("command")),
        accept: AcceptBudget::new(config.reactor_accept_budget)
            .unwrap_or_else(|| invalid_budget("accept")),
        writev: WritevBudget::new(config.reactor_writev_budget)
            .unwrap_or_else(|| invalid_budget("writev")),
        maintenance: MaintenanceBudget::new(config.reactor_maintenance_budget)
            .unwrap_or_else(|| invalid_budget("maintenance")),
        time: TimeBudget::from_micros(config.reactor_time_budget_us),
    };

    let pool_config = ReactorPoolConfig {
        bind_addr: config.bind,
        threads: config.threads,
        max_connections: effective_max_clients,
        buffer_size: config.buffer_size,
        max_request_bytes: config.max_request_bytes,
        connection_caps: vortex_io::ConnectionMemoryCaps {
            max_parser_accumulator_bytes: config.max_parser_accumulator_bytes,
            max_pending_response_bytes: config.max_pending_response_bytes,
            max_multi_queue_commands: config.max_multi_queue_commands,
            max_multi_queue_bytes: config.max_multi_queue_bytes,
            max_watch_registrations: config.max_watch_registrations,
            max_writev_chunks: config.max_writev_chunks,
        },
        overload_policy: vortex_io::ReactorOverloadPolicy {
            accept_throttle_connection_percent: config.reactor_overload_accept_connection_percent,
            read_disable_pending_response_bytes: config.reactor_overload_pending_response_bytes,
            read_disable_parser_accumulator_bytes: config.reactor_overload_parser_accumulator_bytes,
            aof_pending_bytes: config.reactor_overload_aof_pending_bytes,
            writev_backlog_bytes: config.reactor_overload_writev_backlog_bytes,
            maintenance_debt: config.reactor_overload_maintenance_debt,
        },
        buffer_count: config.fixed_buffers,
        fixed_buffer_registration: match config.fixed_buffer_registration {
            vortex_config::FixedBufferRegistrationKind::Auto => FixedBufferRegistrationMode::Auto,
            vortex_config::FixedBufferRegistrationKind::On => FixedBufferRegistrationMode::On,
            vortex_config::FixedBufferRegistrationKind::Off => FixedBufferRegistrationMode::Off,
        },
        connection_timeout: config.connection_timeout_secs as u32,
        aof_config,
        shard_count: config.shard_count,
        max_memory: config.max_memory as usize,
        eviction_policy: EvictionPolicy::parse_bytes(config.eviction_policy.as_bytes())
            .unwrap_or(EvictionPolicy::NoEviction),
        engine_topology: match config.engine_topology {
            vortex_config::EngineTopologyKind::SharedKeyspace => EngineTopologyMode::SharedKeyspace,
            vortex_config::EngineTopologyKind::SharedNothing => EngineTopologyMode::SharedNothing,
        },
        io_backend: match config.io_backend {
            vortex_config::IoBackendKind::Auto => IoBackendMode::Auto,
            vortex_config::IoBackendKind::Uring => IoBackendMode::Uring,
            vortex_config::IoBackendKind::Polling => IoBackendMode::Polling,
        },
        ring_size: config.ring_size,
        sqpoll_idle_ms: config.sqpoll_idle_ms,
        budgets,
        telemetry_mode: match config.telemetry_mode {
            vortex_config::TelemetryModeKind::Minimal => RuntimeTelemetryMode::Minimal,
            #[cfg(feature = "profile-telemetry")]
            vortex_config::TelemetryModeKind::Profile => RuntimeTelemetryMode::Profile,
        },
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

    #[cfg(feature = "lock-profile")]
    if let Some(path) = std::env::var_os("VORTEX_LOCK_PROFILE_JSON") {
        let snapshot = pool.keyspace().lock_profile_snapshot();
        if let Err(error) = std::fs::write(&path, snapshot.to_json()) {
            tracing::warn!(?path, %error, "failed to write lock profile snapshot");
        }
    }

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
