//! Test-only AOF fault injection primitives.
//!
//! These hooks are compiled only for crate tests or the `test-faults` feature.
//! They keep failure names and artifact labels stable without adding state,
//! atomics, timestamps, or branches to the production append/replay hot path.

use std::fmt;
use std::io::{self, Write};

/// Named AOF fault point used by crash and failure-injection tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AofFaultPoint {
    AppendShortWrite,
    AppendError,
    FlushError,
    FsyncError,
    WorkerStopped,
    TruncatedTail,
    CorruptMiddleRecord,
    DuplicateLsn,
    OutOfOrderLsn,
    RewriteTempFailure,
    ManifestRenameFailure,
    DirectoryFsyncFailure,
    WriterSwapFailure,
}

impl AofFaultPoint {
    /// All fault points required by the alpha crash/failure harness.
    pub const ALL: [Self; 13] = [
        Self::AppendShortWrite,
        Self::AppendError,
        Self::FlushError,
        Self::FsyncError,
        Self::WorkerStopped,
        Self::TruncatedTail,
        Self::CorruptMiddleRecord,
        Self::DuplicateLsn,
        Self::OutOfOrderLsn,
        Self::RewriteTempFailure,
        Self::ManifestRenameFailure,
        Self::DirectoryFsyncFailure,
        Self::WriterSwapFailure,
    ];

    /// Stable lowercase name for artifact paths and test output.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AppendShortWrite => "append-short-write",
            Self::AppendError => "append-error",
            Self::FlushError => "flush-error",
            Self::FsyncError => "fsync-error",
            Self::WorkerStopped => "worker-stopped",
            Self::TruncatedTail => "truncated-tail",
            Self::CorruptMiddleRecord => "corrupt-middle-record",
            Self::DuplicateLsn => "duplicate-lsn",
            Self::OutOfOrderLsn => "out-of-order-lsn",
            Self::RewriteTempFailure => "rewrite-temp-failure",
            Self::ManifestRenameFailure => "manifest-rename-failure",
            Self::DirectoryFsyncFailure => "directory-fsync-failure",
            Self::WriterSwapFailure => "writer-swap-failure",
        }
    }

    /// Whether this point is represented by a replay-corruption fixture.
    pub const fn is_replay_fixture(self) -> bool {
        matches!(
            self,
            Self::TruncatedTail
                | Self::CorruptMiddleRecord
                | Self::DuplicateLsn
                | Self::OutOfOrderLsn
        )
    }
}

impl fmt::Display for AofFaultPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Deterministic fault plan carried by tests and artifact names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AofFaultPlan {
    point: AofFaultPoint,
    seed: u64,
    fail_after: usize,
}

impl AofFaultPlan {
    /// Create a plan that injects at the first matching operation.
    pub const fn new(point: AofFaultPoint, seed: u64) -> Self {
        Self {
            point,
            seed,
            fail_after: 0,
        }
    }

    /// Create a plan that skips `fail_after` matching operations.
    pub const fn after(point: AofFaultPoint, seed: u64, fail_after: usize) -> Self {
        Self {
            point,
            seed,
            fail_after,
        }
    }

    pub const fn point(self) -> AofFaultPoint {
        self.point
    }

    pub const fn seed(self) -> u64 {
        self.seed
    }

    pub const fn fail_after(self) -> usize {
        self.fail_after
    }

    /// Stable artifact label that must be included in failure reports.
    pub fn artifact_label(self) -> String {
        format!(
            "seed-{:016x}-fault-{}-after-{}",
            self.seed,
            self.point.as_str(),
            self.fail_after
        )
    }
}

/// `Write` adapter for append/flush short-write and error injection tests.
#[derive(Debug)]
pub struct FaultingWrite<W> {
    inner: W,
    plan: AofFaultPlan,
    write_calls: usize,
    flush_calls: usize,
    injected: bool,
}

impl<W> FaultingWrite<W> {
    pub const fn new(inner: W, plan: AofFaultPlan) -> Self {
        Self {
            inner,
            plan,
            write_calls: 0,
            flush_calls: 0,
            injected: false,
        }
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    pub const fn injected(&self) -> bool {
        self.injected
    }

    fn injected_error(&self) -> io::Error {
        io::Error::other(format!("injected AOF fault {}", self.plan.artifact_label()))
    }

    fn should_inject(counter: usize, fail_after: usize) -> bool {
        counter >= fail_after
    }
}

impl<W: Write> Write for FaultingWrite<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.injected && Self::should_inject(self.write_calls, self.plan.fail_after()) {
            match self.plan.point() {
                AofFaultPoint::AppendError => {
                    self.injected = true;
                    return Err(self.injected_error());
                }
                AofFaultPoint::AppendShortWrite if !buf.is_empty() => {
                    self.injected = true;
                    let n = if buf.len() == 1 { 0 } else { buf.len() - 1 };
                    if n > 0 {
                        self.inner.write_all(&buf[..n])?;
                    }
                    return Ok(n);
                }
                _ => {}
            }
        }
        self.write_calls = self.write_calls.saturating_add(1);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.injected
            && self.plan.point() == AofFaultPoint::FlushError
            && Self::should_inject(self.flush_calls, self.plan.fail_after())
        {
            self.injected = true;
            return Err(self.injected_error());
        }
        self.flush_calls = self.flush_calls.saturating_add(1);
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_points_have_stable_artifact_labels() {
        let mut labels = std::collections::HashSet::new();
        for point in AofFaultPoint::ALL {
            let plan = AofFaultPlan::after(point, 0xaced, 2);
            let label = plan.artifact_label();
            assert!(label.contains(point.as_str()));
            assert!(label.contains("seed-000000000000aced"));
            assert!(labels.insert(label), "duplicate fault label for {point}");
        }
    }

    #[test]
    fn replay_fixture_faults_are_classified() {
        let replay_points = AofFaultPoint::ALL
            .into_iter()
            .filter(|point| point.is_replay_fixture())
            .count();
        assert_eq!(replay_points, 4);
    }

    #[test]
    fn faulting_write_injects_append_short_write() {
        let plan = AofFaultPlan::new(AofFaultPoint::AppendShortWrite, 7);
        let mut writer = FaultingWrite::new(Vec::new(), plan);
        assert_eq!(writer.write(b"abcd").unwrap(), 3);
        assert!(writer.injected());
        assert_eq!(writer.write(b"z").unwrap(), 1);
        assert_eq!(writer.into_inner(), b"abcz");
    }

    #[test]
    fn faulting_write_injects_append_and_flush_errors() {
        let plan = AofFaultPlan::new(AofFaultPoint::AppendError, 8);
        let mut writer = FaultingWrite::new(Vec::new(), plan);
        let error = writer.write(b"abc").unwrap_err();
        assert!(error.to_string().contains("append-error"));

        let plan = AofFaultPlan::new(AofFaultPoint::FlushError, 9);
        let mut writer = FaultingWrite::new(Vec::new(), plan);
        writer.write_all(b"abc").unwrap();
        let error = writer.flush().unwrap_err();
        assert!(error.to_string().contains("flush-error"));
    }
}
