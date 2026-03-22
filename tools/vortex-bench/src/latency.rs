use hdrhistogram::Histogram;

/// Latency percentile report.
#[derive(Debug, Clone, Copy)]
pub struct LatencyReport {
    pub p50: u64,
    pub p99: u64,
    pub p999: u64,
    pub p9999: u64,
    pub max: u64,
    pub count: u64,
}

/// HdrHistogram-backed latency recorder with coordinated omission correction.
pub struct LatencyRecorder {
    hist: Histogram<u64>,
}

impl LatencyRecorder {
    /// Creates a new recorder covering latencies from 1 ns to 60 seconds,
    /// with 3 significant digits of precision.
    pub fn new() -> Self {
        Self {
            hist: Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("valid histogram bounds"),
        }
    }

    /// Records a single latency sample (nanoseconds).
    pub fn record(&mut self, latency_ns: u64) {
        // Clamp to histogram max to avoid errors
        let val = latency_ns.min(self.hist.high());
        self.hist.record(val).ok();
    }

    /// Records a latency with coordinated omission correction.
    ///
    /// `expected_interval_ns` is the expected time between requests.
    /// If `latency_ns > expected_interval_ns`, synthesised intermediate
    /// samples are added to compensate for omitted requests.
    pub fn record_correct(&mut self, latency_ns: u64, expected_interval_ns: u64) {
        let val = latency_ns.min(self.hist.high());
        self.hist.record_correct(val, expected_interval_ns).ok();
    }

    /// Produces a latency percentile report.
    pub fn report(&self) -> LatencyReport {
        LatencyReport {
            p50: self.hist.value_at_quantile(0.50),
            p99: self.hist.value_at_quantile(0.99),
            p999: self.hist.value_at_quantile(0.999),
            p9999: self.hist.value_at_quantile(0.9999),
            max: self.hist.max(),
            count: self.hist.len(),
        }
    }

    /// Resets all recorded data.
    pub fn reset(&mut self) {
        self.hist.reset();
    }
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_recording() {
        let mut rec = LatencyRecorder::new();
        for i in 1..=1000 {
            rec.record(i * 1000); // 1µs to 1ms
        }
        let report = rec.report();
        assert_eq!(report.count, 1000);
        assert!(report.p50 > 0);
        assert!(report.p99 >= report.p50);
        assert!(report.p999 >= report.p99);
        assert!(report.max >= report.p999);
    }

    #[test]
    fn coordinated_omission_correction() {
        let mut rec = LatencyRecorder::new();
        // One normal request, one very slow request
        rec.record_correct(1_000, 1_000); // 1µs, expected 1µs
        rec.record_correct(100_000, 1_000); // 100µs, expected 1µs
        let report = rec.report();
        // With CO correction, many intermediate samples are synthesised
        assert!(report.count > 2);
    }

    #[test]
    fn reset_clears_data() {
        let mut rec = LatencyRecorder::new();
        rec.record(5000);
        assert_eq!(rec.report().count, 1);
        rec.reset();
        assert_eq!(rec.report().count, 0);
    }
}
