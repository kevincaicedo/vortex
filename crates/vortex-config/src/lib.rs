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

    /// Maximum number of client connections.
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
    #[arg(long, default_value = "20000", env = "VORTEX_FIXED_BUFFERS")]
    pub fixed_buffers: usize,

    /// Size of each I/O buffer in bytes (minimum 4096).
    #[arg(long, default_value = "16384", env = "VORTEX_BUFFER_SIZE")]
    pub buffer_size: usize,

    /// Idle connection timeout in seconds (0 = disabled).
    #[arg(long, default_value = "300", env = "VORTEX_CONNECTION_TIMEOUT")]
    pub connection_timeout_secs: u64,

    /// SQPOLL kernel thread idle timeout in milliseconds.
    #[arg(long, default_value = "1000", env = "VORTEX_SQPOLL_IDLE_MS")]
    pub sqpoll_idle_ms: u32,

    /// Enable AOF persistence.
    #[arg(long, env = "VORTEX_AOF_ENABLED")]
    pub aof_enabled: bool,

    /// AOF sync policy: "always", "everysec", "no".
    #[arg(long, default_value = "everysec", env = "VORTEX_AOF_FSYNC")]
    pub aof_fsync: String,

    /// AOF file path.
    #[arg(long, default_value = "vortex.aof", env = "VORTEX_AOF_PATH")]
    pub aof_path: PathBuf,

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
            max_clients: 10_000,
            max_memory: 0,
            eviction_policy: "noeviction".to_string(),
            io_backend: IoBackendKind::Auto,
            ring_size: 4096,
            fixed_buffers: 20_000,
            buffer_size: 16_384,
            connection_timeout_secs: 300,
            sqpoll_idle_ms: 1000,
            aof_enabled: false,
            aof_fsync: "everysec".to_string(),
            aof_path: PathBuf::from("vortex.aof"),
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
        let mut config = Self::parse_from(args);
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
        let min_fixed_buffers = self.max_clients.checked_mul(2).ok_or_else(|| {
            format!(
                "max_clients is too large to derive the minimum fixed_buffers: {}",
                self.max_clients
            )
        })?;
        if self.fixed_buffers < min_fixed_buffers {
            return Err(format!(
                "fixed_buffers must be at least max_clients * 2 ({}), got {}",
                min_fixed_buffers, self.fixed_buffers
            ));
        }
        if !["always", "everysec", "no"].contains(&self.aof_fsync.as_str()) {
            return Err(format!(
                "invalid aof_fsync value '{}': must be always, everysec, or no",
                self.aof_fsync
            ));
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
        if self.buffer_size == sentinel.buffer_size {
            self.buffer_size = defaults.buffer_size;
        }
        if self.connection_timeout_secs == sentinel.connection_timeout_secs {
            self.connection_timeout_secs = defaults.connection_timeout_secs;
        }
        if self.sqpoll_idle_ms == sentinel.sqpoll_idle_ms {
            self.sqpoll_idle_ms = defaults.sqpoll_idle_ms;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = VortexConfig::default();
        assert_eq!(config.bind.port(), 6379);
        assert_eq!(config.max_clients, 10_000);
        assert_eq!(config.fixed_buffers, 20_000);
        assert!(!config.aof_enabled);
        assert_eq!(config.io_backend, IoBackendKind::Auto);
        assert_eq!(config.ring_size, 4096);
        assert_eq!(config.buffer_size, 16_384);
        assert_eq!(config.connection_timeout_secs, 300);
    }

    #[test]
    fn from_args_basic() {
        let config = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--bind".to_string(),
            "0.0.0.0:6380".to_string(),
            "--threads".to_string(),
            "4".to_string(),
        ])
        .unwrap();

        assert_eq!(config.bind.port(), 6380);
        assert_eq!(config.threads, 4);
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
    fn rejects_fixed_buffers_below_two_per_client() {
        let error = VortexConfig::from_args([
            "vortex-server".to_string(),
            "--threads".to_string(),
            "1".to_string(),
            "--max-clients".to_string(),
            "1024".to_string(),
            "--fixed-buffers".to_string(),
            "1024".to_string(),
        ])
        .unwrap_err();

        assert!(error.contains("fixed_buffers must be at least max_clients * 2"));
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
    fn validation_rejects_bad_fsync() {
        let config = VortexConfig {
            threads: 1,
            aof_fsync: "invalid".to_string(),
            ..VortexConfig::default()
        };
        assert!(config.validate().is_err());
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
