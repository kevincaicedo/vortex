//! VortexDB — next-generation in-memory database server.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use vortex_engine::eviction::EvictionPolicy;
use vortex_engine::keyspace::RuntimeTelemetryMode;
use vortex_io::{
    AcceptBudget, AofConfig, CommandBudget, CompletionBudget, FixedBufferRegistrationMode,
    IoBackendMode, MaintenanceBudget, ReactorBudgets, ReactorPool, ReactorPoolConfig, TimeBudget,
    WritevBudget,
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

fn main() -> ExitCode {
    let config = match vortex_config::VortexConfig::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("fatal: {e}");
            return ExitCode::FAILURE;
        }
    };

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    eprintln!("{BANNER}");
    tracing::info!(
        "VortexDB v{} starting — bind={}, threads={}, shard_count={}, io_backend={}, fixed_buffer_registration={}, telemetry_mode={}, max_memory={}",
        env!("CARGO_PKG_VERSION"),
        config.bind,
        config.threads,
        config.shard_count,
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

    let Some(completion_budget) = CompletionBudget::new(config.reactor_completion_budget) else {
        tracing::error!(
            budget = "completion",
            "invalid zero reactor budget after config validation"
        );
        return ExitCode::FAILURE;
    };
    let Some(command_budget) = CommandBudget::new(config.reactor_command_budget) else {
        tracing::error!(
            budget = "command",
            "invalid zero reactor budget after config validation"
        );
        return ExitCode::FAILURE;
    };
    let Some(accept_budget) = AcceptBudget::new(config.reactor_accept_budget) else {
        tracing::error!(
            budget = "accept",
            "invalid zero reactor budget after config validation"
        );
        return ExitCode::FAILURE;
    };
    let Some(writev_budget) = WritevBudget::new(config.reactor_writev_budget) else {
        tracing::error!(
            budget = "writev",
            "invalid zero reactor budget after config validation"
        );
        return ExitCode::FAILURE;
    };
    let Some(maintenance_budget) = MaintenanceBudget::new(config.reactor_maintenance_budget) else {
        tracing::error!(
            budget = "maintenance",
            "invalid zero reactor budget after config validation"
        );
        return ExitCode::FAILURE;
    };

    let budgets = ReactorBudgets {
        completion: completion_budget,
        command: command_budget,
        accept: accept_budget,
        writev: writev_budget,
        maintenance: maintenance_budget,
        time: TimeBudget::from_micros(config.reactor_time_budget_us),
    };

    let telemetry_mode = match config.telemetry_mode {
        vortex_config::TelemetryModeKind::Minimal => RuntimeTelemetryMode::Minimal,
        vortex_config::TelemetryModeKind::Standard => RuntimeTelemetryMode::Standard,
        #[cfg(feature = "profile-telemetry")]
        vortex_config::TelemetryModeKind::Profile => RuntimeTelemetryMode::Profile,
    };
    let telemetry_local_sample_rate = match telemetry_mode {
        RuntimeTelemetryMode::Minimal => 0,
        RuntimeTelemetryMode::Standard => config.telemetry_local_sample_rate,
        #[cfg(feature = "profile-telemetry")]
        RuntimeTelemetryMode::Profile => 1,
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
        io_backend: match config.io_backend {
            vortex_config::IoBackendKind::Auto => IoBackendMode::Auto,
            vortex_config::IoBackendKind::Uring => IoBackendMode::Uring,
            vortex_config::IoBackendKind::Polling => IoBackendMode::Polling,
        },
        ring_size: config.ring_size,
        sqpoll_idle_ms: config.sqpoll_idle_ms,
        budgets,
        telemetry_mode,
        telemetry_local_sample_rate,
        telemetry_flush_interval_nanos: config
            .telemetry_flush_interval_ms
            .saturating_mul(1_000_000),
    };

    let mut pool = match ReactorPool::spawn(pool_config) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "failed to spawn reactor pool");
            return ExitCode::FAILURE;
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
        if let Err(error) = ctrlc::set_handler(move || {
            if coordinator.initiate() {
                tracing::info!("shutdown signal received — draining connections");
            } else {
                // Second signal — escalate to force-kill.
                tracing::warn!("second signal received — forcing immediate shutdown");
                coordinator.force_kill();
            }
        }) {
            tracing::error!(%error, "failed to set signal handler");
            pool.shutdown();
            let clean = pool.wait_for_shutdown(SHUTDOWN_TIMEOUT);
            if !clean {
                tracing::warn!("forced shutdown after signal handler setup failure");
            }
            pool.join();
            return ExitCode::FAILURE;
        }
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

    if clean {
        tracing::info!("VortexDB shutting down — goodbye");
        ExitCode::SUCCESS
    } else {
        tracing::warn!("VortexDB forced shutdown — goodbye");
        ExitCode::FAILURE
    }
}
