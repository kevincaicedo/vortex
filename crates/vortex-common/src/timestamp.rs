/// Monotonic nanosecond timestamp.
///
/// Wraps a `u64` representing nanoseconds since the Unix epoch.
/// Uses `CLOCK_MONOTONIC_COARSE` on Linux for fast (~4ns) timestamping
/// which is sufficient for TTL resolution (millisecond granularity).
const NS_PER_SEC: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

#[inline]
pub fn current_unix_time_nanos() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };

    // SAFETY: `clock_gettime` writes into a valid `timespec` we own.
    let ret = unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts) };

    debug_assert_eq!(ret, 0, "clock_gettime failed");

    ts.tv_sec as u64 * NS_PER_SEC + ts.tv_nsec as u64
}

#[inline]
pub fn absolute_unix_nanos_to_deadline_nanos(
    absolute_unix_nanos: u64,
    monotonic_now_nanos: u64,
    unix_now_nanos: u64,
) -> u64 {
    monotonic_now_nanos.saturating_add(absolute_unix_nanos.saturating_sub(unix_now_nanos))
}

#[inline]
pub fn deadline_nanos_to_absolute_unix_nanos(
    deadline_nanos: u64,
    monotonic_now_nanos: u64,
    unix_now_nanos: u64,
) -> u64 {
    unix_now_nanos.saturating_add(deadline_nanos.saturating_sub(monotonic_now_nanos))
}

impl Timestamp {
    /// Returns the current timestamp using the fastest available monotonic clock.
    ///
    /// - **Linux**: `CLOCK_MONOTONIC_COARSE` (~4ns per call)
    /// - **macOS/other**: `CLOCK_MONOTONIC` (~30ns per call)
    #[inline]
    pub fn now() -> Self {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };

        // SAFETY: `clock_gettime` writes into a valid `timespec` we own.
        // The clock IDs used are always available on their respective platforms.
        #[cfg(target_os = "linux")]
        let ret = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut ts) };

        #[cfg(not(target_os = "linux"))]
        let ret = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };

        debug_assert_eq!(ret, 0, "clock_gettime failed");

        let nanos = ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64;
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

    #[test]
    fn timestamp_monotonic() {
        let t1 = Timestamp::now();
        // Spin briefly to ensure the clock advances.
        std::hint::spin_loop();
        let t2 = Timestamp::now();
        assert!(
            t2 >= t1,
            "Timestamp::now() must be monotonically non-decreasing"
        );
    }

    #[test]
    fn unix_now_is_nonzero() {
        assert!(current_unix_time_nanos() > 0);
    }

    #[test]
    fn clock_domain_conversion_round_trip() {
        let unix_now = 1_750_000_000 * NS_PER_SEC;
        let mono_now = 9_000 * NS_PER_SEC;
        let absolute = unix_now + 60 * NS_PER_SEC;

        let deadline = absolute_unix_nanos_to_deadline_nanos(absolute, mono_now, unix_now);
        assert_eq!(deadline, mono_now + 60 * NS_PER_SEC);

        let round_trip = deadline_nanos_to_absolute_unix_nanos(deadline, mono_now, unix_now);
        assert_eq!(round_trip, absolute);
    }
}
