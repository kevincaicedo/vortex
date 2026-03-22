/// Maximum length for inline key storage (bytes).
/// Keys ≤ this length are stored directly in the key struct without heap allocation.
pub const MAX_INLINE_KEY_LEN: usize = 23;

/// Maximum length for inline value storage (bytes).
pub const MAX_INLINE_VALUE_LEN: usize = 21;

/// CPU cache line size in bytes (x86_64 / aarch64).
pub const CACHE_LINE_SIZE: usize = 64;

/// Number of hash slots for cluster mode
pub const HASH_SLOTS: u16 = 16384;

/// Default bind address.
pub const DEFAULT_BIND_ADDR: &str = "127.0.0.1";

/// Default port.
pub const DEFAULT_PORT: u16 = 6379;

/// VortexDB version string.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
