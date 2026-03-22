use crate::connection::ConnectionSlab;

/// Single-threaded event loop reactor.
///
/// TODO: Phase 1 implements the full event loop. Phase 0 defines the structure
/// and ensures it compiles.
pub struct Reactor {
    /// Reactor ID (typically matches the CPU core index).
    pub id: usize,
    /// Connection slab for this reactor.
    pub connections: ConnectionSlab,
    /// Whether the reactor should keep running.
    pub running: bool,
}

impl Reactor {
    /// Creates a new reactor.
    pub fn new(id: usize) -> Self {
        Self {
            id,
            connections: ConnectionSlab::with_capacity(1024),
            running: false,
        }
    }

    /// Starts the reactor event loop (stub — returns immediately in Phase 0).
    pub fn run(&mut self) {
        self.running = true;
        tracing::info!(reactor_id = self.id, "reactor started (stub)");
        // TODO: Phase 1: full event loop with io_uring / polling
    }

    /// Signals the reactor to stop.
    pub fn stop(&mut self) {
        self.running = false;
        tracing::info!(reactor_id = self.id, "reactor stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reactor_lifecycle() {
        let mut reactor = Reactor::new(0);
        assert!(!reactor.running);

        reactor.run();
        assert!(reactor.running);

        reactor.stop();
        assert!(!reactor.running);
    }
}
