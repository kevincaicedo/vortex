//! Cross-layer AOF durability contract.
//!
//! Vortex distinguishes Redis-visible command success from storage durability.
//! The reactor may release a response only after the selected policy reaches
//! the response-release point below:
//!
//! - `no`: response after userspace append into the reactor-owned `BufWriter`;
//!   a process crash can lose buffered bytes and an OS crash can lose any dirty
//!   page-cache bytes.
//! - `everysec`: response after userspace append; the periodic worker flushes
//!   to the kernel and calls `sync_data()`. Loss window is buffered bytes plus
//!   up to one fsync cadence of dirty page-cache data. If the worker cannot
//!   keep up, the reactor applies backpressure before pending bytes can grow
//!   without bound.
//! - `always`: response after `flush()` plus `sync_data()` completes for that
//!   record. Fsync is on the command critical path; a successful reply means
//!   the record reached the filesystem's durability point, subject to the
//!   device/filesystem honoring fsync.
//!
//! Append or fsync failure under an enabled AOF policy is strict write-stop:
//! the IO layer marks the pool failed and rejects future writes instead of
//! serving a divergent state as durable.

use vortex_engine::keyspace::AofLsn;

use super::format::AofFsyncPolicy;

/// Persistence milestone reached before a client response is released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AofCommitPoint {
    /// Bytes were accepted by the reactor-local userspace buffer only.
    UserspaceAppend,
    /// Bytes were flushed to the kernel page cache, but not explicitly fsynced.
    KernelPageCacheFlush,
    /// Bytes were flushed and `sync_data()` completed.
    FsyncDurable,
}

/// Runtime policy requirement for releasing a command response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AofDurabilityRequirement {
    release_after: AofCommitPoint,
}

impl AofDurabilityRequirement {
    #[inline]
    pub const fn new(release_after: AofCommitPoint) -> Self {
        Self { release_after }
    }

    #[inline]
    pub const fn release_after(self) -> AofCommitPoint {
        self.release_after
    }

    #[inline]
    pub const fn for_policy(policy: AofFsyncPolicy) -> Self {
        match policy {
            AofFsyncPolicy::Always => Self::new(AofCommitPoint::FsyncDurable),
            AofFsyncPolicy::Everysec | AofFsyncPolicy::No => {
                Self::new(AofCommitPoint::UserspaceAppend)
            }
        }
    }
}

/// Policy-level contract used in docs, tests, and runtime assertions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AofPolicyContract {
    pub policy: AofFsyncPolicy,
    pub response_release: AofCommitPoint,
    pub process_crash_may_lose_acknowledged: bool,
    pub os_crash_may_lose_acknowledged: bool,
}

impl AofPolicyContract {
    #[inline]
    pub const fn for_policy(policy: AofFsyncPolicy) -> Self {
        match policy {
            AofFsyncPolicy::No => Self {
                policy,
                response_release: AofCommitPoint::UserspaceAppend,
                process_crash_may_lose_acknowledged: true,
                os_crash_may_lose_acknowledged: true,
            },
            AofFsyncPolicy::Everysec => Self {
                policy,
                response_release: AofCommitPoint::UserspaceAppend,
                process_crash_may_lose_acknowledged: true,
                os_crash_may_lose_acknowledged: true,
            },
            AofFsyncPolicy::Always => Self {
                policy,
                response_release: AofCommitPoint::FsyncDurable,
                process_crash_may_lose_acknowledged: false,
                os_crash_may_lose_acknowledged: false,
            },
        }
    }

    #[inline]
    pub const fn requirement(self) -> AofDurabilityRequirement {
        AofDurabilityRequirement::new(self.response_release)
    }
}

/// Typed evidence returned after appending one AOF record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AofAppendOutcome {
    appended_lsn: AofLsn,
    response_release: AofCommitPoint,
    durable_lsn: Option<AofLsn>,
}

impl AofAppendOutcome {
    #[inline]
    pub const fn new(
        appended_lsn: AofLsn,
        response_release: AofCommitPoint,
        durable_lsn: Option<AofLsn>,
    ) -> Self {
        Self {
            appended_lsn,
            response_release,
            durable_lsn,
        }
    }

    #[inline]
    pub const fn appended_lsn(self) -> AofLsn {
        self.appended_lsn
    }

    #[inline]
    pub const fn response_release(self) -> AofCommitPoint {
        self.response_release
    }

    #[inline]
    pub const fn durable_lsn(self) -> Option<AofLsn> {
        self.durable_lsn
    }

    #[inline]
    pub const fn satisfies(self, requirement: AofDurabilityRequirement) -> bool {
        matches!(
            (self.response_release, requirement.release_after()),
            (AofCommitPoint::FsyncDurable, AofCommitPoint::FsyncDurable)
                | (
                    AofCommitPoint::FsyncDurable,
                    AofCommitPoint::KernelPageCacheFlush
                )
                | (
                    AofCommitPoint::FsyncDurable,
                    AofCommitPoint::UserspaceAppend
                )
                | (
                    AofCommitPoint::KernelPageCacheFlush,
                    AofCommitPoint::KernelPageCacheFlush
                )
                | (
                    AofCommitPoint::KernelPageCacheFlush,
                    AofCommitPoint::UserspaceAppend
                )
                | (
                    AofCommitPoint::UserspaceAppend,
                    AofCommitPoint::UserspaceAppend
                )
        )
    }
}
