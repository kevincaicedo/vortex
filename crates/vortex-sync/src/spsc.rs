use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

/// Single-producer, single-consumer lock-free ring buffer.
///
/// Fixed-size `N` capacity (must be power of two). Uses `CachePadded`
/// head/tail to prevent false sharing between producer and consumer threads.
///
/// # Safety
///
/// This structure uses `UnsafeCell` for the internal buffer. It is only safe
/// when exactly one thread writes (pushes) and exactly one thread reads (pops).
pub struct SpscRingBuffer<T, const N: usize> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    buffer: Box<[UnsafeCell<Option<T>>; N]>,
}

// SAFETY: SpscRingBuffer is safe to send between threads because the atomics
// enforce the correct ordering, and the SPSC contract means only one thread
// accesses each end.
unsafe impl<T: Send, const N: usize> Send for SpscRingBuffer<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for SpscRingBuffer<T, N> {}

impl<T, const N: usize> SpscRingBuffer<T, N> {
    /// Creates a new empty ring buffer. `N` must be a power of two.
    pub fn new() -> Self {
        assert!(N.is_power_of_two(), "SpscRingBuffer: N must be a power of two");
        assert!(N > 0, "SpscRingBuffer: N must be > 0");

        // SAFETY: We initialize every cell to None. UnsafeCell<Option<T>>
        // is safe to initialize this way because Option<T> implements Default.
        let buffer = {
            let mut v = Vec::with_capacity(N);
            for _ in 0..N {
                v.push(UnsafeCell::new(None));
            }
            let boxed_slice: Box<[UnsafeCell<Option<T>>]> = v.into_boxed_slice();
            // SAFETY: The length matches N, so this conversion is valid.
            let ptr = Box::into_raw(boxed_slice) as *mut [UnsafeCell<Option<T>>; N];
            unsafe { Box::from_raw(ptr) }
        };

        Self {
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            buffer,
        }
    }

    /// Attempts to push a value. Returns `Err(value)` if the buffer is full.
    ///
    /// Must only be called from the producer thread.
    pub fn push(&self, value: T) -> Result<(), T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let next_tail = (tail + 1) & (N - 1);

        if next_tail == self.head.load(Ordering::Acquire) {
            return Err(value); // Full
        }

        // SAFETY: We are the sole producer. The head check above guarantees
        // this slot is empty (consumer has already read it).
        unsafe {
            *self.buffer[tail].get() = Some(value);
        }

        self.tail.store(next_tail, Ordering::Release);
        Ok(())
    }

    /// Attempts to pop a value. Returns `None` if the buffer is empty.
    ///
    /// Must only be called from the consumer thread.
    pub fn pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);

        if head == self.tail.load(Ordering::Acquire) {
            return None; // Empty
        }

        // SAFETY: We are the sole consumer. The tail check above guarantees
        // this slot contains a value (producer has written it).
        let value = unsafe { (*self.buffer[head].get()).take() };

        let next_head = (head + 1) & (N - 1);
        self.head.store(next_head, Ordering::Release);

        value
    }

    /// Returns `true` if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Relaxed) == self.tail.load(Ordering::Relaxed)
    }

    /// Returns the current number of items in the buffer (approximate).
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        tail.wrapping_sub(head) & (N - 1)
    }

    /// Returns the capacity of the buffer.
    pub const fn capacity(&self) -> usize {
        N - 1 // One slot reserved to distinguish full from empty
    }
}

impl<T, const N: usize> Default for SpscRingBuffer<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_basic() {
        let rb = SpscRingBuffer::<u64, 4>::new();
        assert!(rb.is_empty());

        rb.push(1).unwrap();
        rb.push(2).unwrap();
        rb.push(3).unwrap();
        assert!(rb.push(4).is_err()); // Full (capacity = 3)

        assert_eq!(rb.pop(), Some(1));
        assert_eq!(rb.pop(), Some(2));
        assert_eq!(rb.pop(), Some(3));
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn wraparound() {
        let rb = SpscRingBuffer::<u32, 4>::new();

        for round in 0..10 {
            for i in 0..3 {
                rb.push(round * 3 + i).unwrap();
            }
            for i in 0..3 {
                assert_eq!(rb.pop(), Some(round * 3 + i));
            }
        }
    }

    #[test]
    fn len_tracking() {
        let rb = SpscRingBuffer::<u32, 8>::new();
        assert_eq!(rb.len(), 0);

        rb.push(1).unwrap();
        assert_eq!(rb.len(), 1);

        rb.push(2).unwrap();
        assert_eq!(rb.len(), 2);

        rb.pop();
        assert_eq!(rb.len(), 1);
    }

    #[test]
    fn cross_thread() {
        use std::sync::Arc;
        use std::thread;

        let rb = Arc::new(SpscRingBuffer::<u64, 1024>::new());
        let rb_producer = Arc::clone(&rb);
        let rb_consumer = Arc::clone(&rb);

        let count = 10_000u64;

        let producer = thread::spawn(move || {
            for i in 0..count {
                while rb_producer.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut received = Vec::with_capacity(count as usize);
            while received.len() < count as usize {
                if let Some(v) = rb_consumer.pop() {
                    received.push(v);
                } else {
                    std::hint::spin_loop();
                }
            }
            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();

        let expected: Vec<u64> = (0..count).collect();
        assert_eq!(received, expected);
    }
}
