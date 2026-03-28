//! Shutdown coordination for multi-reactor graceful shutdown.
//!
//! [`ShutdownCoordinator`] tracks the shutdown lifecycle across all reactor
//! threads. It is `Arc`-shared between the signal handler, the main thread,
//! and each reactor.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use crossbeam_utils::CachePadded;

/// Reactor is running normally.
pub const STATE_RUNNING: u8 = 0;
/// Graceful drain in progress — no new connections, flush writes, close.
pub const STATE_DRAINING: u8 = 1;
/// Forced shutdown — reactors exit immediately without flushing.
pub const STATE_FORCE_KILL: u8 = 2;

/// Coordinates graceful shutdown across all reactor threads.
///
/// The coordinator tracks a global shutdown state and per-reactor completion
/// flags. Cache-line padding prevents false sharing between the main thread
/// (polling `all_done`) and reactor threads (setting `reactor_done`).
pub struct ShutdownCoordinator {
    /// Global shutdown state: Running → Draining → ForceKill.
    state: CachePadded<AtomicU8>,
    /// Per-reactor completion flags (cache-line padded to avoid false sharing).
    reactor_done: Vec<CachePadded<AtomicBool>>,
}

impl ShutdownCoordinator {
    /// Create a new coordinator tracking `num_reactors` reactor threads.
    pub fn new(num_reactors: usize) -> Self {
        Self {
            state: CachePadded::new(AtomicU8::new(STATE_RUNNING)),
            reactor_done: (0..num_reactors)
                .map(|_| CachePadded::new(AtomicBool::new(false)))
                .collect(),
        }
    }

    /// Initiate graceful shutdown (CAS Running → Draining).
    ///
    /// Returns `true` if this call performed the transition. Concurrent calls
    /// from multiple signal deliveries are safe — only the first succeeds.
    pub fn initiate(&self) -> bool {
        self.state
            .compare_exchange(
                STATE_RUNNING,
                STATE_DRAINING,
                Ordering::SeqCst,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Escalate to forced shutdown (Draining → ForceKill).
    ///
    /// Called by the main thread when the shutdown timeout expires.
    pub fn force_kill(&self) {
        self.state.store(STATE_FORCE_KILL, Ordering::SeqCst);
    }

    /// Returns `true` if shutdown has been initiated (draining or force-kill).
    #[inline]
    pub fn is_draining(&self) -> bool {
        self.state.load(Ordering::Relaxed) >= STATE_DRAINING
    }

    /// Returns `true` if forced, immediate shutdown is active.
    #[inline]
    pub fn is_force_kill(&self) -> bool {
        self.state.load(Ordering::Relaxed) == STATE_FORCE_KILL
    }

    /// Mark a reactor as finished draining.
    ///
    /// Called by each reactor after its drain completes and event loop exits.
    pub fn reactor_finished(&self, id: usize) {
        if let Some(flag) = self.reactor_done.get(id) {
            flag.store(true, Ordering::Release);
        }
    }

    /// Returns `true` if all tracked reactors have finished.
    pub fn all_done(&self) -> bool {
        self.reactor_done
            .iter()
            .all(|flag| flag.load(Ordering::Acquire))
    }

    /// Block until all reactors finish or the timeout expires.
    ///
    /// If the timeout expires before all reactors finish, escalates to
    /// `ForceKill` and waits a brief grace period.
    ///
    /// Returns `true` on clean shutdown, `false` if force-kill was triggered.
    pub fn wait_for_shutdown(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let poll_interval = Duration::from_millis(10);

        while Instant::now() < deadline {
            if self.all_done() {
                return true;
            }
            std::thread::park_timeout(poll_interval);
        }

        if self.all_done() {
            return true;
        }

        // Timeout expired — force kill.
        tracing::warn!("shutdown timeout exceeded, forcing exit");
        self.force_kill();

        // Brief grace period for force-killed reactors to break out.
        let grace_deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < grace_deadline {
            if self.all_done() {
                break;
            }
            std::thread::park_timeout(poll_interval);
        }

        false
    }

    /// Returns the current shutdown state as a raw `u8`.
    #[inline]
    pub fn state(&self) -> u8 {
        self.state.load(Ordering::Relaxed)
    }

    /// Returns the number of reactors tracked by this coordinator.
    pub fn reactor_count(&self) -> usize {
        self.reactor_done.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn initiate_transitions_running_to_draining() {
        let coord = ShutdownCoordinator::new(2);
        assert!(!coord.is_draining());
        assert!(coord.initiate());
        assert!(coord.is_draining());
        assert!(!coord.is_force_kill());
        // Second initiate is a no-op.
        assert!(!coord.initiate());
    }

    #[test]
    fn force_kill_sets_state() {
        let coord = ShutdownCoordinator::new(1);
        coord.initiate();
        coord.force_kill();
        assert!(coord.is_force_kill());
        assert!(coord.is_draining()); // ForceKill >= Draining.
    }

    #[test]
    fn reactor_finished_and_all_done() {
        let coord = ShutdownCoordinator::new(3);
        assert!(!coord.all_done());

        coord.reactor_finished(0);
        assert!(!coord.all_done());

        coord.reactor_finished(1);
        assert!(!coord.all_done());

        coord.reactor_finished(2);
        assert!(coord.all_done());
    }

    #[test]
    fn wait_returns_true_on_clean_shutdown() {
        let coord = Arc::new(ShutdownCoordinator::new(2));
        coord.initiate();

        let c = Arc::clone(&coord);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            c.reactor_finished(0);
            c.reactor_finished(1);
        });

        let clean = coord.wait_for_shutdown(Duration::from_secs(5));
        assert!(clean);
    }

    #[test]
    fn wait_returns_false_on_timeout() {
        let coord = Arc::new(ShutdownCoordinator::new(2));
        coord.initiate();
        // Only finish one reactor — the other never finishes.
        coord.reactor_finished(0);

        let clean = coord.wait_for_shutdown(Duration::from_millis(100));
        assert!(!clean);
        assert!(coord.is_force_kill());
    }

    #[test]
    fn out_of_bounds_reactor_id_is_safe() {
        let coord = ShutdownCoordinator::new(2);
        // Should not panic.
        coord.reactor_finished(999);
        assert!(!coord.all_done());
    }
}
