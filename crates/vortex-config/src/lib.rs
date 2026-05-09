//! # vortex-config
//!
//! Configuration management for VortexDB.
//!
//! Parses command-line arguments (via `clap`), TOML config files, and
//! environment variables (`VORTEX_*`) into a single strongly-typed
//! [`VortexConfig`] struct.
//!
//! **Loading priority:** CLI args > environment vars > `vortex.toml` > defaults.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

/// I/O backend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum IoBackendKind {
    /// Auto-detect: use io_uring on Linux if available, otherwise polling.
    #[default]
    Auto,
    /// Force io_uring backend (Linux only — fails fast if unavailable).
    Uring,
    /// Force cross-platform polling backend.
    Polling,
}

impl fmt::Display for IoBackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::Uring => write!(f, "uring"),
            Self::Polling => write!(f, "polling"),
        }
    }
}

/// io_uring fixed-buffer registration policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum FixedBufferRegistrationKind {
    /// Register when supported and representable; auto-disable only when the
    /// backend itself was auto-selected.
    #[default]
    Auto,
    /// Require registration and fail startup if the backend or buffer range
    /// cannot support it.
    On,
    /// Keep the fixed buffer pool as reactor staging memory, but submit normal
    /// read/write operations without io_uring buf_index references.
    Off,
}

impl fmt::Display for FixedBufferRegistrationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::On => write!(f, "on"),
            Self::Off => write!(f, "off"),
        }
    }
}

/// Runtime telemetry cost policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum TelemetryModeKind {
    /// Release/default mode: correctness/status metrics stay available, but
    /// profiler-only phase timers are disabled.
    #[default]
    Minimal,
    /// Profiling-only mode: enable timestamped phase timers for benchmark
    /// evidence. This variant is not compiled into normal release builds.
    #[cfg(feature = "profile-telemetry")]
    Profile,
}

impl fmt::Display for TelemetryModeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Minimal => write!(f, "minimal"),
            #[cfg(feature = "profile-telemetry")]
            Self::Profile => write!(f, "profile"),
        }
    }
}

/// Default unsynced AOF backlog allowed for `everysec` before backpressure.
pub const DEFAULT_AOF_MAX_PENDING_FSYNC_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_REACTOR_COMPLETION_BUDGET: usize = 256;
pub const DEFAULT_REACTOR_COMMAND_BUDGET: usize = 1024;
pub const DEFAULT_REACTOR_ACCEPT_BUDGET: usize = 64;
pub const DEFAULT_REACTOR_WRITEV_BUDGET: usize = 1024;
pub const DEFAULT_REACTOR_MAINTENANCE_BUDGET: usize = 4;
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_PARSER_ACCUMULATOR_BYTES: usize = DEFAULT_MAX_REQUEST_BYTES;
pub const DEFAULT_MAX_PENDING_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_MULTI_QUEUE_COMMANDS: usize = 128;
pub const DEFAULT_MAX_MULTI_QUEUE_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_WATCH_REGISTRATIONS: usize = 1024;
pub const DEFAULT_MAX_WRITEV_CHUNKS: usize = 64;
pub const DEFAULT_REACTOR_OVERLOAD_ACCEPT_CONNECTION_PERCENT: u8 = 95;
pub const DEFAULT_REACTOR_OVERLOAD_PENDING_RESPONSE_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_REACTOR_OVERLOAD_PARSER_ACCUMULATOR_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_REACTOR_OVERLOAD_AOF_PENDING_BYTES: u64 = DEFAULT_AOF_MAX_PENDING_FSYNC_BYTES;
pub const DEFAULT_REACTOR_OVERLOAD_WRITEV_BACKLOG_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_REACTOR_OVERLOAD_MAINTENANCE_DEBT: usize = 1024;
pub const DEFAULT_SHARD_COUNT: usize = 4096;
pub const MIN_SHARD_COUNT: usize = 64;
pub const MAX_SHARD_COUNT: usize = 131_072;

/// Master configuration struct for VortexDB.
#[derive(Debug, Clone, Parser, Deserialize)]
#[command(
    name = "vortex-server",
    about = "VortexDB — Next-generation in-memory data engine",
    version = env!("CARGO_PKG_VERSION"),
)]
#[serde(default)]
pub struct VortexConfig {
    /// Bind address and port.
    #[arg(long, default_value = "127.0.0.1:6379", env = "VORTEX_BIND")]
    pub bind: SocketAddr,

    /// Number of reactor threads (0 = auto-detect CPU count).
    #[arg(long, default_value = "0", env = "VORTEX_THREADS")]
    pub threads: usize,

    /// Number of engine keyspace shards. Must be a power of two.
    #[arg(long, default_value_t = DEFAULT_SHARD_COUNT, env = "VORTEX_SHARD_COUNT")]
    pub shard_count: usize,

    /// Maximum number of client connections.
    ///
    /// The server may cap the effective active-client budget further when the
    /// configured fixed buffer pool cannot supply two buffers per connection.
    #[arg(long, default_value = "10000", env = "VORTEX_MAX_CLIENTS")]
    pub max_clients: usize,

    /// Maximum memory in bytes (0 = unlimited).
    #[arg(long, default_value = "0", env = "VORTEX_MAX_MEMORY")]
    pub max_memory: u64,

    /// Memory eviction policy.
    #[arg(long, default_value = "noeviction", env = "VORTEX_EVICTION_POLICY")]
    pub eviction_policy: String,

    /// I/O backend: auto, uring, or polling.
    #[arg(long, default_value = "auto", env = "VORTEX_IO_BACKEND", value_enum)]
    pub io_backend: IoBackendKind,

    /// io_uring submission queue size (must be a power of two).
    #[arg(long, default_value = "4096", env = "VORTEX_RING_SIZE")]
    pub ring_size: u32,

    /// Number of fixed I/O buffers pre-registered with io_uring.
    ///
    /// Each active connection leases one read buffer and one write buffer, so
    /// the fixed-buffer pool can sustain `fixed_buffers / 2` active clients.
    #[arg(long, default_value = "1024", env = "VORTEX_FIXED_BUFFERS")]
    pub fixed_buffers: usize,

    /// io_uring fixed-buffer registration policy: auto, on, or off.
    #[arg(
        long,
        default_value = "auto",
        env = "VORTEX_FIXED_BUFFER_REGISTRATION",
        value_enum
    )]
    pub fixed_buffer_registration: FixedBufferRegistrationKind,

    /// Size of each I/O buffer in bytes (minimum 4096).
    #[arg(long, default_value = "16384", env = "VORTEX_BUFFER_SIZE")]
    pub buffer_size: usize,

    /// Maximum bytes retained for one in-flight request or pipeline.
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_REQUEST_BYTES,
        env = "VORTEX_MAX_REQUEST_BYTES"
    )]
    pub max_request_bytes: usize,

    /// Maximum bytes retained for one incomplete parser accumulator.
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_PARSER_ACCUMULATOR_BYTES,
        env = "VORTEX_MAX_PARSER_ACCUMULATOR_BYTES"
    )]
    pub max_parser_accumulator_bytes: usize,

    /// Maximum serialized response bytes pending for one connection.
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_PENDING_RESPONSE_BYTES,
        env = "VORTEX_MAX_PENDING_RESPONSE_BYTES"
    )]
    pub max_pending_response_bytes: usize,

    /// Maximum queued MULTI commands retained for one connection.
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_MULTI_QUEUE_COMMANDS,
        env = "VORTEX_MAX_MULTI_QUEUE_COMMANDS"
    )]
    pub max_multi_queue_commands: usize,

    /// Maximum serialized MULTI command bytes retained for one connection.
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_MULTI_QUEUE_BYTES,
        env = "VORTEX_MAX_MULTI_QUEUE_BYTES"
    )]
    pub max_multi_queue_bytes: usize,

    /// Maximum WATCH registrations retained for one connection.
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_WATCH_REGISTRATIONS,
        env = "VORTEX_MAX_WATCH_REGISTRATIONS"
    )]
    pub max_watch_registrations: usize,

    /// Maximum deferred writev submit chunks retained for one response batch.
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_WRITEV_CHUNKS,
        env = "VORTEX_MAX_WRITEV_CHUNKS"
    )]
    pub max_writev_chunks: usize,

    /// Percent of reactor-local connection capacity at which accepts throttle.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_OVERLOAD_ACCEPT_CONNECTION_PERCENT,
        env = "VORTEX_REACTOR_OVERLOAD_ACCEPT_CONNECTION_PERCENT"
    )]
    pub reactor_overload_accept_connection_percent: u8,

    /// Reactor queued response bytes that disable new reads.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_OVERLOAD_PENDING_RESPONSE_BYTES,
        env = "VORTEX_REACTOR_OVERLOAD_PENDING_RESPONSE_BYTES"
    )]
    pub reactor_overload_pending_response_bytes: usize,

    /// Reactor parser accumulator bytes that disable new reads.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_OVERLOAD_PARSER_ACCUMULATOR_BYTES,
        env = "VORTEX_REACTOR_OVERLOAD_PARSER_ACCUMULATOR_BYTES"
    )]
    pub reactor_overload_parser_accumulator_bytes: usize,

    /// Reactor AOF pending bytes that defer AOF-bearing write commands.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_OVERLOAD_AOF_PENDING_BYTES,
        env = "VORTEX_REACTOR_OVERLOAD_AOF_PENDING_BYTES"
    )]
    pub reactor_overload_aof_pending_bytes: u64,

    /// Reactor writev backlog bytes that disable new reads.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_OVERLOAD_WRITEV_BACKLOG_BYTES,
        env = "VORTEX_REACTOR_OVERLOAD_WRITEV_BACKLOG_BYTES"
    )]
    pub reactor_overload_writev_backlog_bytes: usize,

    /// Reactor maintenance debt units that defer foreground work.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_OVERLOAD_MAINTENANCE_DEBT,
        env = "VORTEX_REACTOR_OVERLOAD_MAINTENANCE_DEBT"
    )]
    pub reactor_overload_maintenance_debt: usize,

    /// Idle connection timeout in seconds (0 = disabled).
    #[arg(long, default_value = "300", env = "VORTEX_CONNECTION_TIMEOUT")]
    pub connection_timeout_secs: u64,

    /// SQPOLL kernel thread idle timeout in milliseconds (0 = disabled).
    #[arg(long, default_value = "0", env = "VORTEX_SQPOLL_IDLE_MS")]
    pub sqpoll_idle_ms: u32,

    /// Runtime telemetry mode.
    ///
    /// Normal release builds support only `minimal`; profiling builds compiled
    /// with `profile-telemetry` also support `profile`.
    #[arg(
        long,
        default_value = "minimal",
        env = "VORTEX_TELEMETRY_MODE",
        value_enum
    )]
    pub telemetry_mode: TelemetryModeKind,

    /// Max backend completions processed by one reactor loop activation.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_COMPLETION_BUDGET,
        env = "VORTEX_REACTOR_COMPLETION_BUDGET"
    )]
    pub reactor_completion_budget: usize,

    /// Max commands executed from one connection per activation.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_COMMAND_BUDGET,
        env = "VORTEX_REACTOR_COMMAND_BUDGET"
    )]
    pub reactor_command_budget: usize,

    /// Max extra accepts drained after one accept readiness completion.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_ACCEPT_BUDGET,
        env = "VORTEX_REACTOR_ACCEPT_BUDGET"
    )]
    pub reactor_accept_budget: usize,

    /// Max writev segments submitted per write completion cycle.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_WRITEV_BUDGET,
        env = "VORTEX_REACTOR_WRITEV_BUDGET"
    )]
    pub reactor_writev_budget: usize,

    /// Max maintenance slices consumed per loop activation.
    #[arg(
        long,
        default_value_t = DEFAULT_REACTOR_MAINTENANCE_BUDGET,
        env = "VORTEX_REACTOR_MAINTENANCE_BUDGET"
    )]
    pub reactor_maintenance_budget: usize,

    /// Optional command activation time budget in microseconds (0 = disabled).
    #[arg(long, default_value = "0", env = "VORTEX_REACTOR_TIME_BUDGET_US")]
    pub reactor_time_budget_us: u64,

    /// Enable AOF persistence.
    #[arg(long, env = "VORTEX_AOF_ENABLED")]
    pub aof_enabled: bool,

    /// AOF sync policy: "always", "everysec", "no".
    #[arg(long, default_value = "everysec", env = "VORTEX_AOF_FSYNC")]
    pub aof_fsync: String,

    /// AOF file path.
    #[arg(long, default_value = "vortex.aof", env = "VORTEX_AOF_PATH")]
    pub aof_path: PathBuf,

    /// Max unsynced AOF bytes allowed in everysec mode before backpressure.
    #[arg(
        long,
        default_value_t = DEFAULT_AOF_MAX_PENDING_FSYNC_BYTES,
        env = "VORTEX_AOF_MAX_PENDING_FSYNC_BYTES"
    )]
    pub aof_max_pending_fsync_bytes: u64,

    /// Enable snapshot persistence.
    #[arg(long, env = "VORTEX_SNAPSHOT_ENABLED")]
    pub snapshot_enabled: bool,

    /// Snapshot interval in seconds.
    #[arg(long, default_value = "3600", env = "VORTEX_SNAPSHOT_INTERVAL")]
    pub snapshot_interval: u64,

    /// Snapshot file path.
    #[arg(long, default_value = "vortex.vxf", env = "VORTEX_SNAPSHOT_PATH")]
    pub snapshot_path: PathBuf,

    /// Require authentication password (empty = no auth).
    #[arg(long, default_value = "", env = "VORTEX_REQUIREPASS")]
    pub requirepass: String,

    /// Log level: "trace", "debug", "info", "warn", "error".
    #[arg(long, default_value = "info", env = "VORTEX_LOG_LEVEL")]
    pub log_level: String,

    /// Path to TOML config file.
    #[arg(long, short = 'c', env = "VORTEX_CONFIG")]
    pub config: Option<PathBuf>,

    /// Prometheus metrics port (None = disabled).
    #[arg(long, env = "VORTEX_METRICS_PORT")]
    pub metrics_port: Option<u16>,

    /// Enable adaptive morphing structures (runtime encoding transitions).
    /// When `false`, data structures use Redis-compatible static thresholds.
    #[arg(long, default_value = "true", env = "VORTEX_ADAPTIVE_STRUCTURES")]
    pub adaptive_structures: bool,
}

impl Default for VortexConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:6379".parse().expect("valid default addr"),
            threads: 0,
            shard_count: DEFAULT_SHARD_COUNT,
            max_clients: 10_000,
            max_memory: 0,
            eviction_policy: "noeviction".to_string(),
            io_backend: IoBackendKind::Auto,
            ring_size: 4096,
            fixed_buffers: 1_024,
            fixed_buffer_registration: FixedBufferRegistrationKind::Auto,
            buffer_size: 16_384,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_parser_accumulator_bytes: DEFAULT_MAX_PARSER_ACCUMULATOR_BYTES,
            max_pending_response_bytes: DEFAULT_MAX_PENDING_RESPONSE_BYTES,
            max_multi_queue_commands: DEFAULT_MAX_MULTI_QUEUE_COMMANDS,
            max_multi_queue_bytes: DEFAULT_MAX_MULTI_QUEUE_BYTES,
            max_watch_registrations: DEFAULT_MAX_WATCH_REGISTRATIONS,
            max_writev_chunks: DEFAULT_MAX_WRITEV_CHUNKS,
            reactor_overload_accept_connection_percent:
                DEFAULT_REACTOR_OVERLOAD_ACCEPT_CONNECTION_PERCENT,
            reactor_overload_pending_response_bytes:
                DEFAULT_REACTOR_OVERLOAD_PENDING_RESPONSE_BYTES,
            reactor_overload_parser_accumulator_bytes:
                DEFAULT_REACTOR_OVERLOAD_PARSER_ACCUMULATOR_BYTES,
            reactor_overload_aof_pending_bytes: DEFAULT_REACTOR_OVERLOAD_AOF_PENDING_BYTES,
            reactor_overload_writev_backlog_bytes: DEFAULT_REACTOR_OVERLOAD_WRITEV_BACKLOG_BYTES,
            reactor_overload_maintenance_debt: DEFAULT_REACTOR_OVERLOAD_MAINTENANCE_DEBT,
            connection_timeout_secs: 300,
            sqpoll_idle_ms: 0,
            telemetry_mode: TelemetryModeKind::Minimal,
            reactor_completion_budget: DEFAULT_REACTOR_COMPLETION_BUDGET,
            reactor_command_budget: DEFAULT_REACTOR_COMMAND_BUDGET,
            reactor_accept_budget: DEFAULT_REACTOR_ACCEPT_BUDGET,
            reactor_writev_budget: DEFAULT_REACTOR_WRITEV_BUDGET,
            reactor_maintenance_budget: DEFAULT_REACTOR_MAINTENANCE_BUDGET,
            reactor_time_budget_us: 0,
            aof_enabled: false,
            aof_fsync: "everysec".to_string(),
            aof_path: PathBuf::from("vortex.aof"),
            aof_max_pending_fsync_bytes: DEFAULT_AOF_MAX_PENDING_FSYNC_BYTES,
            snapshot_enabled: false,
            snapshot_interval: 3600,
            snapshot_path: PathBuf::from("vortex.vxf"),
            requirepass: String::new(),
            log_level: "info".to_string(),
            config: None,
            metrics_port: None,
            adaptive_structures: true,
        }
    }
}

impl VortexConfig {
    /// Load config from CLI args, falling back to TOML file + env vars.
    ///
    /// Priority: CLI args > env vars > TOML file > defaults.
    pub fn load() -> Result<Self, String> {
        let mut config = Self::parse();

        // If a config file is specified, merge TOML values for unset fields.
        if let Some(ref path) = config.config {
            let contents = std::fs::read_to_string(path)
                .map_err(|e| format!("Failed to read config file {}: {e}", path.display()))?;
            let file_config: VortexConfig = toml::from_str(&contents)
                .map_err(|e| format!("Failed to parse config file: {e}"))?;

            // TOML values serve as defaults; CLI args take precedence.
            config.merge_defaults(file_config);
        }

        config.resolve_threads();
        config.validate()?;

        Ok(config)
    }

    /// Load config from explicit args (for testing).
    pub fn from_args(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut config = Self::try_parse_from(args).map_err(|error| error.to_string())?;
        config.resolve_threads();
        config.validate()?;
        Ok(config)
    }

    /// Resolve auto-detected values.
    fn resolve_threads(&mut self) {
        if self.threads == 0 {
            self.threads = std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1);
        }
    }

    /// Validate configuration.
    fn validate(&self) -> Result<(), String> {
        if self.threads == 0 {
            return Err("threads must be > 0".to_string());
        }
        if self.shard_count < MIN_SHARD_COUNT
            || self.shard_count > MAX_SHARD_COUNT
            || !self.shard_count.is_power_of_two()
        {
            return Err(format!(
                "shard_count must be a power of two in {MIN_SHARD_COUNT}..={MAX_SHARD_COUNT}, got {}",
                self.shard_count
            ));
        }
        if !self.ring_size.is_power_of_two() {
            return Err(format!(
                "ring_size must be a power of two, got {}",
                self.ring_size
            ));
        }
        if self.buffer_size < 4096 {
            return Err(format!(
                "buffer_size must be >= 4096, got {}",
                self.buffer_size
            ));
        }
        if self.max_request_bytes == 0 {
            return Err("max_request_bytes must be > 0".to_string());
        }
        if self.max_request_bytes < self.buffer_size {
            return Err(format!(
                "max_request_bytes must be >= buffer_size ({}), got {}",
                self.buffer_size, self.max_request_bytes
            ));
        }
        if self.max_parser_accumulator_bytes == 0 {
            return Err("max_parser_accumulator_bytes must be > 0".to_string());
        }
        if self.max_pending_response_bytes == 0 {
            return Err("max_pending_response_bytes must be > 0".to_string());
        }
        if self.max_multi_queue_commands == 0 {
            return Err("max_multi_queue_commands must be > 0".to_string());
        }
        if self.max_multi_queue_bytes == 0 {
            return Err("max_multi_queue_bytes must be > 0".to_string());
        }
        if self.max_watch_registrations == 0 {
            return Err("max_watch_registrations must be > 0".to_string());
        }
        if self.max_writev_chunks == 0 {
            return Err("max_writev_chunks must be > 0".to_string());
        }
        if !(1..=100).contains(&self.reactor_overload_accept_connection_percent) {
            return Err(
                "reactor_overload_accept_connection_percent must be in 1..=100".to_string(),
            );
        }
        if self.reactor_overload_pending_response_bytes == 0 {
            return Err("reactor_overload_pending_response_bytes must be > 0".to_string());
        }
        if self.reactor_overload_parser_accumulator_bytes == 0 {
            return Err("reactor_overload_parser_accumulator_bytes must be > 0".to_string());
        }
        if self.reactor_overload_aof_pending_bytes == 0 {
            return Err("reactor_overload_aof_pending_bytes must be > 0".to_string());
        }
        if self.reactor_overload_writev_backlog_bytes == 0 {
            return Err("reactor_overload_writev_backlog_bytes must be > 0".to_string());
        }
        if self.reactor_overload_maintenance_debt == 0 {
            return Err("reactor_overload_maintenance_debt must be > 0".to_string());
        }
        if self.fixed_buffers < 1 {
            return Err(format!(
                "fixed_buffers must be at least 1 (one fixed read buffer), got {}",
                self.fixed_buffers
            ));
        }
        if !["always", "everysec", "no"].contains(&self.aof_fsync.as_str()) {
            return Err(format!(
                "invalid aof_fsync value '{}': must be always, everysec, or no",
                self.aof_fsync
            ));
        }
        if self.aof_max_pending_fsync_bytes == 0 {
            return Err("aof_max_pending_fsync_bytes must be > 0".to_string());
        }
        if self.reactor_completion_budget == 0 {
            return Err("reactor_completion_budget must be > 0".to_string());
        }
        if self.reactor_command_budget == 0 {
            return Err("reactor_command_budget must be > 0".to_string());
        }
        if self.reactor_accept_budget == 0 {
            return Err("reactor_accept_budget must be > 0".to_string());
        }
        if self.reactor_writev_budget == 0 {
            return Err("reactor_writev_budget must be > 0".to_string());
        }
        if self.reactor_maintenance_budget == 0 {
            return Err("reactor_maintenance_budget must be > 0".to_string());
        }
        if ![
            "noeviction",
            "allkeys-lru",
            "volatile-lru",
            "allkeys-random",
            "volatile-random",
            "volatile-ttl",
            "allkeys-lfu",
            "volatile-lfu",
        ]
        .contains(&self.eviction_policy.as_str())
        {
            return Err(format!(
                "invalid eviction_policy '{}': must be noeviction, allkeys-lru, volatile-lru, allkeys-random, volatile-random, volatile-ttl, allkeys-lfu, or volatile-lfu",
                self.eviction_policy
            ));
        }
        Ok(())
    }

    /// Merge fields from a TOML-loaded config into `self`.
    ///
    /// For each field: if the CLI-parsed value equals the `Default` sentinel,
    /// take the TOML value instead. This implements CLI > TOML > defaults priority.
    fn merge_defaults(&mut self, defaults: VortexConfig) {
        let sentinel = VortexConfig::default();

        // Numeric/enum fields: CLI default sentinel → take TOML value.
        if self.threads == sentinel.threads {
            self.threads = defaults.threads;
        }
        if self.shard_count == sentinel.shard_count {
            self.shard_count = defaults.shard_count;
        }
        if self.max_clients == sentinel.max_clients {
            self.max_clients = defaults.max_clients;
        }
        if self.max_memory == sentinel.max_memory {
            self.max_memory = defaults.max_memory;
        }
        if self.io_backend == sentinel.io_backend {
            self.io_backend = defaults.io_backend;
        }
        if self.ring_size == sentinel.ring_size {
            self.ring_size = defaults.ring_size;
        }
        if self.fixed_buffers == sentinel.fixed_buffers {
            self.fixed_buffers = defaults.fixed_buffers;
        }
        if self.fixed_buffer_registration == sentinel.fixed_buffer_registration {
            self.fixed_buffer_registration = defaults.fixed_buffer_registration;
        }
        if self.buffer_size == sentinel.buffer_size {
            self.buffer_size = defaults.buffer_size;
        }
        if self.max_request_bytes == sentinel.max_request_bytes {
            self.max_request_bytes = defaults.max_request_bytes;
        }
        if self.max_parser_accumulator_bytes == sentinel.max_parser_accumulator_bytes {
            self.max_parser_accumulator_bytes = defaults.max_parser_accumulator_bytes;
        }
        if self.max_pending_response_bytes == sentinel.max_pending_response_bytes {
            self.max_pending_response_bytes = defaults.max_pending_response_bytes;
        }
        if self.max_multi_queue_commands == sentinel.max_multi_queue_commands {
            self.max_multi_queue_commands = defaults.max_multi_queue_commands;
        }
        if self.max_multi_queue_bytes == sentinel.max_multi_queue_bytes {
            self.max_multi_queue_bytes = defaults.max_multi_queue_bytes;
        }
        if self.max_watch_registrations == sentinel.max_watch_registrations {
            self.max_watch_registrations = defaults.max_watch_registrations;
        }
        if self.max_writev_chunks == sentinel.max_writev_chunks {
            self.max_writev_chunks = defaults.max_writev_chunks;
        }
        if self.reactor_overload_accept_connection_percent
            == sentinel.reactor_overload_accept_connection_percent
        {
            self.reactor_overload_accept_connection_percent =
                defaults.reactor_overload_accept_connection_percent;
        }
        if self.reactor_overload_pending_response_bytes
            == sentinel.reactor_overload_pending_response_bytes
        {
            self.reactor_overload_pending_response_bytes =
                defaults.reactor_overload_pending_response_bytes;
        }
        if self.reactor_overload_parser_accumulator_bytes
            == sentinel.reactor_overload_parser_accumulator_bytes
        {
            self.reactor_overload_parser_accumulator_bytes =
                defaults.reactor_overload_parser_accumulator_bytes;
        }
        if self.reactor_overload_aof_pending_bytes == sentinel.reactor_overload_aof_pending_bytes {
            self.reactor_overload_aof_pending_bytes = defaults.reactor_overload_aof_pending_bytes;
        }
        if self.reactor_overload_writev_backlog_bytes
            == sentinel.reactor_overload_writev_backlog_bytes
        {
            self.reactor_overload_writev_backlog_bytes =
                defaults.reactor_overload_writev_backlog_bytes;
        }
        if self.reactor_overload_maintenance_debt == sentinel.reactor_overload_maintenance_debt {
            self.reactor_overload_maintenance_debt = defaults.reactor_overload_maintenance_debt;
        }
        if self.connection_timeout_secs == sentinel.connection_timeout_secs {
            self.connection_timeout_secs = defaults.connection_timeout_secs;
        }
        if self.sqpoll_idle_ms == sentinel.sqpoll_idle_ms {
            self.sqpoll_idle_ms = defaults.sqpoll_idle_ms;
        }
        if self.telemetry_mode == sentinel.telemetry_mode {
            self.telemetry_mode = defaults.telemetry_mode;
        }
        if self.reactor_completion_budget == sentinel.reactor_completion_budget {
            self.reactor_completion_budget = defaults.reactor_completion_budget;
        }
        if self.reactor_command_budget == sentinel.reactor_command_budget {
            self.reactor_command_budget = defaults.reactor_command_budget;
        }
        if self.reactor_accept_budget == sentinel.reactor_accept_budget {
            self.reactor_accept_budget = defaults.reactor_accept_budget;
        }
        if self.reactor_writev_budget == sentinel.reactor_writev_budget {
            self.reactor_writev_budget = defaults.reactor_writev_budget;
        }
        if self.reactor_maintenance_budget == sentinel.reactor_maintenance_budget {
            self.reactor_maintenance_budget = defaults.reactor_maintenance_budget;
        }
        if self.reactor_time_budget_us == sentinel.reactor_time_budget_us {
            self.reactor_time_budget_us = defaults.reactor_time_budget_us;
        }
        if self.aof_max_pending_fsync_bytes == sentinel.aof_max_pending_fsync_bytes {
            self.aof_max_pending_fsync_bytes = defaults.aof_max_pending_fsync_bytes;
        }

        // String fields: check against default sentinel strings.
        if self.eviction_policy == sentinel.eviction_policy {
            self.eviction_policy = defaults.eviction_policy;
        }
        if self.aof_fsync == sentinel.aof_fsync {
            self.aof_fsync = defaults.aof_fsync;
        }
        if self.log_level == sentinel.log_level {
            self.log_level = defaults.log_level;
        }
        if self.requirepass == sentinel.requirepass {
            self.requirepass = defaults.requirepass;
        }

        // Bool fields: only take TOML value if CLI didn't set them (bools default to false).
        if !self.aof_enabled && defaults.aof_enabled {
            self.aof_enabled = true;
        }
        if !self.snapshot_enabled && defaults.snapshot_enabled {
            self.snapshot_enabled = true;
        }

        // Path fields.
        if self.aof_path == sentinel.aof_path {
            self.aof_path = defaults.aof_path;
        }
        if self.snapshot_path == sentinel.snapshot_path {
            self.snapshot_path = defaults.snapshot_path;
        }
        if self.snapshot_interval == sentinel.snapshot_interval {
            self.snapshot_interval = defaults.snapshot_interval;
        }

        // SocketAddr: compare to default bind address.
        if self.bind == sentinel.bind {
            self.bind = defaults.bind;
        }

        // Optional fields: take TOML value if CLI didn't set.
        if self.metrics_port.is_none() {
            self.metrics_port = defaults.metrics_port;
        }

        // Adaptive structures: CLI default is true; take TOML value if CLI matches default.
        if self.adaptive_structures == sentinel.adaptive_structures {
            self.adaptive_structures = defaults.adaptive_structures;
        }
    }

    /// Returns the effective number of reactor threads.
    pub fn effective_threads(&self) -> usize {
        self.threads
    }

    /// Returns the active-client capacity implied by the fixed read buffer pool.
    pub fn fixed_buffer_client_capacity(&self) -> usize {
        self.fixed_buffers
    }

    /// Returns the effective active-client budget after accounting for buffer capacity.
    pub fn effective_max_clients(&self) -> usize {
        self.max_clients.min(self.fixed_buffer_client_capacity())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = VortexConfig::default();
        assert_eq!(config.bind.port(), 6379);
        assert_eq!(config.shard_count, DEFAULT_SHARD_COUNT);
        assert_eq!(config.max_clients, 10_000);
        assert_eq!(config.fixed_buffers, 1_024);
        assert_eq!(config.effective_max_clients(), 1_024);
        assert!(!config.aof_enabled);
        assert_eq!(config.io_backend, IoBackendKind::Auto);
        assert_eq!(config.ring_size, 4096);
        assert_eq!(
            config.fixed_buffer_registration,
            FixedBufferRegistrationKind::Auto
        );
        assert_eq!(config.sqpoll_idle_ms, 0);
        assert_eq!(config.buffer_size, 16_384);
        assert_eq!(config.max_request_bytes, DEFAULT_MAX_REQUEST_BYTES);
        assert_eq!(
            config.max_parser_accumulator_bytes,
            DEFAULT_MAX_PARSER_ACCUMULATOR_BYTES
        );
        assert_eq!(
            config.max_pending_response_bytes,
            DEFAULT_MAX_PENDING_RESPONSE_BYTES
        );
        assert_eq!(
            config.max_multi_queue_commands,
            DEFAULT_MAX_MULTI_QUEUE_COMMANDS
        );
        assert_eq!(config.max_multi_queue_bytes, DEFAULT_MAX_MULTI_QUEUE_BYTES);
        assert_eq!(
            config.max_watch_registrations,
            DEFAULT_MAX_WATCH_REGISTRATIONS
        );
        assert_eq!(config.max_writev_chunks, DEFAULT_MAX_WRITEV_CHUNKS);
        assert_eq!(
            config.reactor_overload_accept_connection_percent,
            DEFAULT_REACTOR_OVERLOAD_ACCEPT_CONNECTION_PERCENT
        );
        assert_eq!(
            config.reactor_overload_pending_response_bytes,
            DEFAULT_REACTOR_OVERLOAD_PENDING_RESPONSE_BYTES
        );
        assert_eq!(
            config.reactor_overload_parser_accumulator_bytes,
            DEFAULT_REACTOR_OVERLOAD_PARSER_ACCUMULATOR_BYTES
        );
        assert_eq!(
            config.reactor_overload_aof_pending_bytes,
            DEFAULT_REACTOR_OVERLOAD_AOF_PENDING_BYTES
        );
        assert_eq!(
            config.reactor_overload_writev_backlog_bytes,
            DEFAULT_REACTOR_OVERLOAD_WRITEV_BACKLOG_BYTES
        );
        assert_eq!(
            config.reactor_overload_maintenance_debt,
            DEFAULT_REACTOR_OVERLOAD_MAINTENANCE_DEBT
        );
        assert_eq!(config.connection_timeout_secs, 300);
        assert_eq!(config.telemetry_mode, TelemetryModeKind::Minimal);
        assert_eq!(
            config.aof_max_pending_fsync_bytes,
            DEFAULT_AOF_MAX_PENDING_FSYNC_BYTES
        );
        assert_eq!(
            config.reactor_completion_budget,
            DEFAULT_REACTOR_COMPLETION_BUDGET
        );
        assert_eq!(
            config.reactor_command_budget,
            DEFAULT_REACTOR_COMMAND_BUDGET
        );
        assert_eq!(config.reactor_accept_budget, DEFAULT_REACTOR_ACCEPT_BUDGET);
        assert_eq!(config.reactor_writev_budget, DEFAULT_REACTOR_WRITEV_BUDGET);
        assert_eq!(
            config.reactor_maintenance_budget,
            DEFAULT_REACTOR_MAINTENANCE_BUDGET
        );
        assert_eq!(config.reactor_time_budget_us, 0);
    }

    #[test]
    fn from_args_basic() {
        let config = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--bind".to_string(),
            "0.0.0.0:6380".to_string(),
            "--threads".to_string(),
            "4".to_string(),
            "--shard-count".to_string(),
            "16384".to_string(),
        ])
        .unwrap();

        assert_eq!(config.bind.port(), 6380);
        assert_eq!(config.threads, 4);
        assert_eq!(config.shard_count, 16_384);
    }

    #[test]
    fn validation_rejects_invalid_shard_count() {
        let error = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--threads".to_string(),
            "1".to_string(),
            "--shard-count".to_string(),
            "100".to_string(),
        ])
        .unwrap_err();

        assert!(error.contains("shard_count must be a power of two"));
    }

    #[test]
    fn from_args_io_backend_uring() {
        let config = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--threads".to_string(),
            "1".to_string(),
            "--io-backend".to_string(),
            "uring".to_string(),
            "--ring-size".to_string(),
            "2048".to_string(),
        ])
        .unwrap();

        assert_eq!(config.io_backend, IoBackendKind::Uring);
        assert_eq!(config.ring_size, 2048);
    }

    #[test]
    #[cfg(feature = "profile-telemetry")]
    fn from_args_telemetry_profile() {
        let config = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--threads".to_string(),
            "1".to_string(),
            "--telemetry-mode".to_string(),
            "profile".to_string(),
        ])
        .unwrap();

        assert_eq!(config.telemetry_mode, TelemetryModeKind::Profile);
    }

    #[test]
    #[cfg(not(feature = "profile-telemetry"))]
    fn release_build_rejects_profile_telemetry() {
        let result = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--threads".to_string(),
            "1".to_string(),
            "--telemetry-mode".to_string(),
            "profile".to_string(),
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn accepts_fixed_buffers_below_requested_max_clients() {
        let config = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--threads".to_string(),
            "1".to_string(),
            "--max-clients".to_string(),
            "2048".to_string(),
            "--fixed-buffers".to_string(),
            "1024".to_string(),
        ])
        .unwrap();

        assert_eq!(config.max_clients, 2048);
        assert_eq!(config.fixed_buffer_client_capacity(), 1024);
        assert_eq!(config.effective_max_clients(), 1024);
    }

    #[test]
    fn rejects_zero_fixed_buffers() {
        let error = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--threads".to_string(),
            "1".to_string(),
            "--fixed-buffers".to_string(),
            "0".to_string(),
        ])
        .unwrap_err();

        assert!(error.contains("fixed_buffers must be at least 1"));
    }

    #[test]
    fn from_args_io_backend_polling() {
        let config = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--threads".to_string(),
            "1".to_string(),
            "--io-backend".to_string(),
            "polling".to_string(),
        ])
        .unwrap();

        assert_eq!(config.io_backend, IoBackendKind::Polling);
    }

    #[test]
    fn from_args_fixed_buffer_registration_off() {
        let config = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--threads".to_string(),
            "1".to_string(),
            "--fixed-buffer-registration".to_string(),
            "off".to_string(),
        ])
        .unwrap();

        assert_eq!(
            config.fixed_buffer_registration,
            FixedBufferRegistrationKind::Off
        );
    }

    #[test]
    fn validation_rejects_bad_ring_size() {
        let config = VortexConfig {
            threads: 1,
            ring_size: 3000, // Not a power of two.
            ..VortexConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("ring_size must be a power of two"));
    }

    #[test]
    fn validation_rejects_small_buffer_size() {
        let config = VortexConfig {
            threads: 1,
            buffer_size: 1024, // Below minimum 4096.
            ..VortexConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("buffer_size must be >= 4096"));
    }

    #[test]
    fn validation_rejects_request_limit_below_buffer_size() {
        let config = VortexConfig {
            threads: 1,
            buffer_size: 16_384,
            max_request_bytes: 4096,
            ..VortexConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("max_request_bytes must be >= buffer_size"));
    }

    #[test]
    fn validation_rejects_bad_fsync() {
        let config = VortexConfig {
            threads: 1,
            aof_fsync: "invalid".to_string(),
            ..VortexConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validation_rejects_zero_aof_pending_limit() {
        let config = VortexConfig {
            threads: 1,
            aof_max_pending_fsync_bytes: 0,
            ..VortexConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validation_rejects_zero_reactor_budget() {
        let config = VortexConfig {
            threads: 1,
            reactor_command_budget: 0,
            ..VortexConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("reactor_command_budget"));
    }

    #[test]
    fn validation_accepts_lfu_eviction_policies() {
        let allkeys = VortexConfig {
            threads: 1,
            eviction_policy: "allkeys-lfu".to_string(),
            ..VortexConfig::default()
        };
        assert!(allkeys.validate().is_ok());

        let volatile = VortexConfig {
            threads: 1,
            eviction_policy: "volatile-lfu".to_string(),
            ..VortexConfig::default()
        };
        assert!(volatile.validate().is_ok());
    }

    #[test]
    fn auto_threads_resolution() {
        let mut config = VortexConfig::default();
        assert_eq!(config.threads, 0);
        config.resolve_threads();
        assert!(config.threads > 0);
    }

    #[test]
    fn merge_defaults_toml_fills_unset() {
        // Simulate: CLI has defaults, TOML overrides some fields.
        let mut cli = VortexConfig::default();
        let toml_conf = VortexConfig {
            threads: 8,
            ring_size: 2048,
            io_backend: IoBackendKind::Uring,
            max_memory: 1_073_741_824,
            ..VortexConfig::default()
        };

        cli.merge_defaults(toml_conf);

        // TOML values should win over defaults.
        assert_eq!(cli.threads, 8);
        assert_eq!(cli.ring_size, 2048);
        assert_eq!(cli.io_backend, IoBackendKind::Uring);
        assert_eq!(cli.max_memory, 1_073_741_824);
    }

    #[test]
    fn merge_defaults_cli_wins_over_toml() {
        // Simulate: CLI explicitly set threads=4, TOML says threads=8.
        let mut cli = VortexConfig {
            threads: 4, // Differs from default 0 → CLI override.
            ..VortexConfig::default()
        };
        let toml_conf = VortexConfig {
            threads: 8,
            ring_size: 2048,
            ..VortexConfig::default()
        };

        cli.merge_defaults(toml_conf);

        // CLI threads=4 should win because it differs from sentinel (0).
        assert_eq!(cli.threads, 4);
        // But ring_size should take TOML value because CLI was default.
        assert_eq!(cli.ring_size, 2048);
    }

    #[test]
    fn merge_defaults_untouched_stay_default() {
        let mut cli = VortexConfig::default();
        let toml_conf = VortexConfig::default();

        cli.merge_defaults(toml_conf);

        // All fields remain at defaults.
        assert_eq!(cli.ring_size, 4096);
        assert_eq!(cli.buffer_size, 16_384);
        assert_eq!(cli.io_backend, IoBackendKind::Auto);
    }
}
