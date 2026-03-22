use std::time::{SystemTime, UNIX_EPOCH};

/// Monotonic nanosecond timestamp.
///
/// Wraps a `u64` representing nanoseconds since the Unix epoch.
/// Uses `CLOCK_MONOTONIC_COARSE` on Linux for fast (~4ns) timestamping
/// which is sufficient for TTL resolution (millisecond granularity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Returns the current timestamp.
    #[inline]
    pub fn now() -> Self {
        // Use SystemTime for portability in Phase 0.
        // TODO: Phase 1 switches to clock_gettime(CLOCK_MONOTONIC_COARSE) on Linux.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self(nanos)
    }

    /// Creates a timestamp from raw nanoseconds.
    #[inline]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Creates a timestamp from milliseconds since epoch.
    #[inline]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis * 1_000_000)
    }

    /// Creates a timestamp from seconds since epoch.
    #[inline]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs * 1_000_000_000)
    }

    /// Returns the raw nanosecond value.
    #[inline]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Returns the timestamp as milliseconds.
    #[inline]
    pub const fn as_millis(self) -> u64 {
        self.0 / 1_000_000
    }

    /// Returns the timestamp as seconds.
    #[inline]
    pub const fn as_secs(self) -> u64 {
        self.0 / 1_000_000_000
    }

    /// Returns the elapsed time since `earlier`.
    #[inline]
    pub fn elapsed_since(self, earlier: Timestamp) -> std::time::Duration {
        let diff = self.0.saturating_sub(earlier.0);
        std::time::Duration::from_nanos(diff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_now_is_nonzero() {
        let ts = Timestamp::now();
        assert!(ts.as_nanos() > 0);
    }

    #[test]
    fn timestamp_conversions() {
        let ts = Timestamp::from_secs(1);
        assert_eq!(ts.as_millis(), 1_000);
        assert_eq!(ts.as_nanos(), 1_000_000_000);
    }

    #[test]
    fn timestamp_elapsed() {
        let earlier = Timestamp::from_nanos(1_000_000);
        let later = Timestamp::from_nanos(2_000_000);
        let elapsed = later.elapsed_since(earlier);
        assert_eq!(elapsed, std::time::Duration::from_nanos(1_000_000));
    }

    #[test]
    fn timestamp_ordering() {
        let t1 = Timestamp::from_nanos(100);
        let t2 = Timestamp::from_nanos(200);
        assert!(t1 < t2);
    }
}
