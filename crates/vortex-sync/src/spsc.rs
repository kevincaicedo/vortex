use std::cell::UnsafeCell;
use std::sync::Arc;
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

/// Producer endpoint for a bounded SPSC ring.
///
/// This type is intentionally not `Clone`; one call to [`spsc_channel`] creates
/// exactly one producer endpoint and one consumer endpoint.
pub struct SpscSender<T, const N: usize> {
    ring: Arc<SpscRingBuffer<T, N>>,
}

/// Consumer endpoint for a bounded SPSC ring.
///
/// This type is intentionally not `Clone`; one call to [`spsc_channel`] creates
/// exactly one producer endpoint and one consumer endpoint.
pub struct SpscReceiver<T, const N: usize> {
    ring: Arc<SpscRingBuffer<T, N>>,
}

/// Creates one producer and one consumer endpoint for a bounded SPSC ring.
pub fn spsc_channel<T, const N: usize>() -> (SpscSender<T, N>, SpscReceiver<T, N>) {
    let ring = Arc::new(SpscRingBuffer::new());
    (
        SpscSender {
            ring: Arc::clone(&ring),
        },
        SpscReceiver { ring },
    )
}

// SAFETY: SpscRingBuffer is safe to send between threads because the atomics
// enforce the correct ordering, and the SPSC contract means only one thread
// accesses each end.
unsafe impl<T: Send, const N: usize> Send for SpscRingBuffer<T, N> {}
unsafe impl<T: Send, const N: usize> Sync for SpscRingBuffer<T, N> {}

impl<T, const N: usize> SpscRingBuffer<T, N> {
    /// Creates a new empty ring buffer. `N` must be a power of two.
    pub fn new() -> Self {
        assert!(
            N.is_power_of_two(),
            "SpscRingBuffer: N must be a power of two"
        );
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
    #[inline(always)]
    fn push(&self, value: T) -> Result<(), T> {
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
    #[inline(always)]
    fn pop(&self) -> Option<T> {
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

    /// Drains up to `limit` currently visible elements into `visit`.
    ///
    /// The consumer observes the producer's published tail once per batch, so
    /// messages published after this call starts may wait for the next drain.
    /// Must only be called from the consumer thread.
    #[inline]
    fn drain_batch<F>(&self, limit: usize, mut visit: F) -> usize
    where
        F: FnMut(T),
    {
        if limit == 0 {
            return 0;
        }

        let mut head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        let mut drained = 0;

        while drained < limit && head != tail {
            // SAFETY: The caller is the sole consumer. The acquired tail value
            // proves the producer published this slot before the tail update.
            let value = unsafe { (*self.buffer[head].get()).take() };
            let next_head = (head + 1) & (N - 1);
            self.head.store(next_head, Ordering::Release);
            head = next_head;

            let value = value.expect("SpscRingBuffer visible slot was empty");
            visit(value);

            drained += 1;
        }

        drained
    }

    /// Returns `true` if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Relaxed) == self.tail.load(Ordering::Relaxed)
    }

    /// Returns `true` if the buffer has no spare slot.
    #[inline]
    fn is_full(&self) -> bool {
        let tail = self.tail.load(Ordering::Relaxed);
        let next_tail = (tail + 1) & (N - 1);
        next_tail == self.head.load(Ordering::Acquire)
    }

    /// Returns the current number of items in the buffer (approximate).
    #[inline]
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        tail.wrapping_sub(head) & (N - 1)
    }

    /// Returns the capacity of the buffer.
    #[inline]
    pub const fn capacity(&self) -> usize {
        N - 1 // One slot reserved to distinguish full from empty
    }

    /// Returns the number of ring slots, including the sentinel slot.
    #[inline]
    pub const fn ring_slots(&self) -> usize {
        N
    }
}

impl<T, const N: usize> SpscSender<T, N> {
    /// Attempts to send `value`, returning it when the bounded ring is full.
    ///
    /// Must only be called by the producer owner of this endpoint.
    #[inline(always)]
    pub fn try_send(&self, value: T) -> Result<(), T> {
        self.ring.push(value)
    }

    /// Returns `true` if the queue has no spare slot.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.ring.is_full()
    }

    /// Returns the approximate number of elements in the queue.
    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Returns `true` if the queue is observed empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Returns the usable queue capacity.
    #[inline]
    pub const fn capacity(&self) -> usize {
        N - 1
    }

    /// Returns the ring slot count, including the sentinel slot.
    #[inline]
    pub const fn ring_slots(&self) -> usize {
        N
    }
}

impl<T, const N: usize> SpscReceiver<T, N> {
    /// Attempts to receive one value.
    ///
    /// Must only be called by the consumer owner of this endpoint.
    #[inline(always)]
    pub fn try_recv(&self) -> Option<T> {
        self.ring.pop()
    }

    /// Drains up to `limit` currently visible elements into `visit`.
    ///
    /// Must only be called by the consumer owner of this endpoint.
    #[inline]
    pub fn drain_batch<F>(&self, limit: usize, visit: F) -> usize
    where
        F: FnMut(T),
    {
        self.ring.drain_batch(limit, visit)
    }

    /// Returns `true` if the queue is observed empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Returns the approximate number of elements in the queue.
    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Returns the usable queue capacity.
    #[inline]
    pub const fn capacity(&self) -> usize {
        N - 1
    }

    /// Returns the ring slot count, including the sentinel slot.
    #[inline]
    pub const fn ring_slots(&self) -> usize {
        N
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
        assert!(!rb.is_full());

        rb.push(1).unwrap();
        assert_eq!(rb.len(), 1);

        rb.push(2).unwrap();
        assert_eq!(rb.len(), 2);

        rb.pop();
        assert_eq!(rb.len(), 1);
    }

    #[test]
    fn full_tracking() {
        let rb = SpscRingBuffer::<u32, 4>::new();

        rb.push(1).unwrap();
        rb.push(2).unwrap();
        assert!(!rb.is_full());

        rb.push(3).unwrap();
        assert!(rb.is_full());
        assert!(rb.push(4).is_err());

        assert_eq!(rb.pop(), Some(1));
        assert!(!rb.is_full());
    }

    #[test]
    fn drain_batch_respects_limit_and_fifo() {
        let rb = SpscRingBuffer::<u32, 8>::new();

        for value in 0..5 {
            rb.push(value).unwrap();
        }

        let mut first = Vec::new();
        let drained = rb.drain_batch(3, |value| first.push(value));
        assert_eq!(drained, 3);
        assert_eq!(first, vec![0, 1, 2]);
        assert_eq!(rb.len(), 2);

        let mut second = Vec::new();
        let drained = rb.drain_batch(8, |value| second.push(value));
        assert_eq!(drained, 2);
        assert_eq!(second, vec![3, 4]);
        assert!(rb.is_empty());
    }

    #[test]
    fn endpoint_halves_preserve_fifo_and_full_behavior() {
        let (sender, receiver) = spsc_channel::<u32, 4>();

        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        sender.try_send(3).unwrap();
        assert!(sender.is_full());
        assert_eq!(sender.try_send(4), Err(4));

        assert_eq!(receiver.try_recv(), Some(1));
        assert_eq!(receiver.try_recv(), Some(2));
        assert_eq!(receiver.try_recv(), Some(3));
        assert_eq!(receiver.try_recv(), None);
    }

    #[test]
    fn endpoint_batch_drain_respects_limit() {
        let (sender, receiver) = spsc_channel::<u32, 8>();

        for value in 0..5 {
            sender.try_send(value).unwrap();
        }

        let mut first = Vec::new();
        assert_eq!(receiver.drain_batch(2, |value| first.push(value)), 2);
        assert_eq!(first, vec![0, 1]);

        let mut second = Vec::new();
        assert_eq!(receiver.drain_batch(8, |value| second.push(value)), 3);
        assert_eq!(second, vec![2, 3, 4]);
    }

    #[test]
    fn cross_thread() {
        use std::thread;

        let (sender, receiver) = spsc_channel::<u64, 1024>();

        let count = if cfg!(miri) { 10 } else { 10_000u64 };

        let producer = thread::spawn(move || {
            for i in 0..count {
                let mut value = i;
                loop {
                    match sender.try_send(value) {
                        Ok(()) => break,
                        Err(rejected) => {
                            value = rejected;
                            std::hint::spin_loop();
                        }
                    }
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut received = Vec::with_capacity(count as usize);
            while received.len() < count as usize {
                if let Some(v) = receiver.try_recv() {
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
