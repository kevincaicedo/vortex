use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_queue::SegQueue;

/// Multi-producer, single-consumer lock-free queue.
///
/// Used for work-stealing: multiple reactors can push commands to an
/// overloaded reactor's steal queue.
///
/// Backed by `crossbeam_queue::SegQueue` — a lock-free, unbounded,
/// multi-producer multi-consumer queue. We expose only the MPSC subset
/// of its API.
pub struct MpscQueue<T> {
    inner: SegQueue<T>,
    /// Approximate length tracked via atomic counter for O(1) `len()`.
    length: AtomicUsize,
}

impl<T> MpscQueue<T> {
    /// Creates a new empty MPSC queue.
    pub fn new() -> Self {
        Self {
            inner: SegQueue::new(),
            length: AtomicUsize::new(0),
        }
    }

    /// Pushes a value into the queue. Can be called from any thread (lock-free).
    pub fn push(&self, value: T) {
        self.inner.push(value);
        self.length.fetch_add(1, Ordering::Relaxed);
    }

    /// Pops a value from the queue. Should be called from the consumer thread.
    pub fn pop(&self) -> Option<T> {
        self.inner.pop().inspect(|_| {
            self.length.fetch_sub(1, Ordering::Relaxed);
        })
    }

    /// Returns true if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the approximate length.
    pub fn len(&self) -> usize {
        self.length.load(Ordering::Relaxed)
    }
}

impl<T> Default for MpscQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn basic_push_pop() {
        let q = MpscQueue::new();
        q.push(1);
        q.push(2);
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn multi_producer() {
        let q = Arc::new(MpscQueue::new());
        let mut handles = Vec::new();

        for t in 0..4 {
            let q = Arc::clone(&q);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    q.push(t * 1000 + i);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let mut collected = Vec::new();
        while let Some(v) = q.pop() {
            collected.push(v);
        }

        assert_eq!(collected.len(), 4000);
    }

    #[test]
    fn len_tracks_pushes_and_pops() {
        let q = MpscQueue::new();
        assert_eq!(q.len(), 0);
        assert!(q.is_empty());

        q.push(1);
        q.push(2);
        q.push(3);
        assert_eq!(q.len(), 3);

        q.pop();
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn stress_multi_producer_single_consumer() {
        let q = Arc::new(MpscQueue::new());
        let num_producers = 4;
        let items_per_producer = 10_000;
        let mut handles = Vec::new();

        for _ in 0..num_producers {
            let q = Arc::clone(&q);
            handles.push(thread::spawn(move || {
                for i in 0..items_per_producer {
                    q.push(i);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let mut count = 0;
        while q.pop().is_some() {
            count += 1;
        }

        assert_eq!(count, num_producers * items_per_producer);
        assert!(q.is_empty());
    }
}
