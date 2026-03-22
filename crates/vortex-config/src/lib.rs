//! # vortex-config
//!
//! Configuration management for VortexDB.
//!
//! Parses command-line arguments (via `clap`), TOML config files, and
//! environment variables (`VORTEX_*`) into a single strongly-typed
//! [`VortexConfig`] struct.
//!
//! **Loading priority:** CLI args > environment vars > `vortex.toml` > defaults.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

/// Master configuration struct for VortexDB.
#[derive(Debug, Clone, Parser, Deserialize)]
#[command(name = "vortex-server", about = "VortexDB — Next-generation in-memory data engine")]
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

    /// I/O backend: "uring" or "polling".
    #[arg(long, default_value = "auto", env = "VORTEX_IO_BACKEND")]
    pub io_backend: String,

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
}

impl Default for VortexConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:6379".parse().expect("valid default addr"),
            threads: 0,
            max_clients: 10_000,
            max_memory: 0,
            eviction_policy: "noeviction".to_string(),
            io_backend: "auto".to_string(),
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
        if !["always", "everysec", "no"].contains(&self.aof_fsync.as_str()) {
            return Err(format!(
                "invalid aof_fsync value '{}': must be always, everysec, or no",
                self.aof_fsync
            ));
        }
        if !["noeviction", "allkeys-lru", "volatile-lru", "allkeys-random", "volatile-random", "volatile-ttl"]
            .contains(&self.eviction_policy.as_str())
        {
            return Err(format!(
                "invalid eviction_policy '{}': must be noeviction, allkeys-lru, volatile-lru, allkeys-random, volatile-random, or volatile-ttl",
                self.eviction_policy
            ));
        }
        Ok(())
    }

    /// Merge unset fields from a TOML-loaded config.
    fn merge_defaults(&mut self, _defaults: VortexConfig) {
        // TODO: In Phase 0, CLI args always win. A more sophisticated merge
        // (checking which fields were explicitly set) comes in Phase 1.
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
        assert!(!config.aof_enabled);
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
    fn validation_rejects_bad_fsync() {
        let mut config = VortexConfig::default();
        config.threads = 1;
        config.aof_fsync = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn auto_threads_resolution() {
        let mut config = VortexConfig::default();
        assert_eq!(config.threads, 0);
        config.resolve_threads();
        assert!(config.threads > 0);
    }
}
