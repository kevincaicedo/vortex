/// Exponential backoff helper for CAS retry loops.
///
/// Progresses through three stages:
/// 1. **Spin** — `PAUSE` instruction (1–16 iterations)
/// 2. **Yield** — `thread::yield_now()` (16–32 iterations)
/// 3. **Park** — `thread::park()` or sleep
pub struct Backoff {
    step: u32,
}

impl Backoff {
    const SPIN_LIMIT: u32 = 16;
    const YIELD_LIMIT: u32 = 32;

    /// Creates a new backoff state.
    pub fn new() -> Self {
        Self { step: 0 }
    }

    /// Performs one backoff step.
    pub fn snooze(&mut self) {
        if self.step < Self::SPIN_LIMIT {
            for _ in 0..(1 << self.step) {
                std::hint::spin_loop();
            }
        } else if self.step < Self::YIELD_LIMIT {
            std::thread::yield_now();
        } else {
            std::thread::sleep(std::time::Duration::from_micros(1));
        }
        self.step = self.step.saturating_add(1);
    }

    /// Returns `true` if the backoff has reached the yield/park stage.
    pub fn is_completed(&self) -> bool {
        self.step >= Self::YIELD_LIMIT
    }

    /// Resets the backoff state.
    pub fn reset(&mut self) {
        self.step = 0;
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_progression() {
        let mut bo = Backoff::new();
        assert!(!bo.is_completed());

        for _ in 0..Backoff::YIELD_LIMIT {
            bo.snooze();
        }
        assert!(bo.is_completed());

        bo.reset();
        assert!(!bo.is_completed());
    }
}
