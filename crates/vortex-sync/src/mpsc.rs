use std::collections::VecDeque;
use std::sync::Mutex;

/// Multi-producer, single-consumer lock-free queue.
///
/// Used for work-stealing: multiple reactors can push commands to an
/// overloaded reactor's steal queue.
///
/// **Phase 0**: Uses a `Mutex<VecDeque<T>>` as a correct placeholder.
/// TODO(Phase 1.5): Replace with a true lock-free MPSC queue using atomic
/// linked list with `CachePadded` nodes.
pub struct MpscQueue<T> {
    inner: Mutex<VecDeque<T>>,
}

impl<T> MpscQueue<T> {
    /// Creates a new empty MPSC queue.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
        }
    }

    /// Pushes a value into the queue. Can be called from any thread.
    pub fn push(&self, value: T) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.push_back(value);
    }

    /// Pops a value from the queue. Should be called from the consumer thread.
    pub fn pop(&self) -> Option<T> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.pop_front()
    }

    /// Returns true if the queue is empty.
    pub fn is_empty(&self) -> bool {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.is_empty()
    }

    /// Returns the approximate length.
    pub fn len(&self) -> usize {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.len()
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
}
