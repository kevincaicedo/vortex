use std::collections::VecDeque;

/// A leased buffer from the pool.
pub struct Buffer {
    data: Vec<u8>,
}

impl Buffer {
    /// Returns a mutable slice to the buffer data.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Returns a slice to the buffer data.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Returns the buffer capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.len()
    }
}

/// Pre-allocated pool of fixed-size buffers.
///
/// Used for io_uring fixed buffer registration. Buffers are leased out
/// for I/O operations and returned when complete.
///
/// TODO(Phase 1.5): Upgrade to page-aligned, pinned memory with
/// `mmap` and io_uring buffer registration.
pub struct BufferPool {
    buffer_size: usize,
    available: VecDeque<Vec<u8>>,
    outstanding: usize,
}

impl BufferPool {
    /// Creates a new buffer pool with `count` buffers of `buffer_size` bytes each.
    pub fn new(count: usize, buffer_size: usize) -> Self {
        let mut available = VecDeque::with_capacity(count);
        for _ in 0..count {
            available.push_back(vec![0u8; buffer_size]);
        }
        Self {
            buffer_size,
            available,
            outstanding: 0,
        }
    }

    /// Creates a new buffer pool with optional NUMA node binding.
    ///
    /// When `numa_node` is `Some(n)`, buffers will be allocated on NUMA node `n`.
    /// TODO(Phase 1.5): Implement actual NUMA-local allocation via mmap + mbind.
    pub fn new_with_numa(count: usize, buffer_size: usize, _numa_node: Option<usize>) -> Self {
        Self::new(count, buffer_size)
    }

    /// Leases a buffer from the pool. Returns `None` if all buffers are in use.
    pub fn lease(&mut self) -> Option<Buffer> {
        self.available.pop_front().map(|data| {
            self.outstanding += 1;
            Buffer { data }
        })
    }

    /// Returns a buffer to the pool.
    pub fn release(&mut self, mut buf: Buffer) {
        // Zero the buffer before reuse to prevent data leakage.
        buf.data.iter_mut().for_each(|b| *b = 0);
        self.available.push_back(buf.data);
        self.outstanding -= 1;
    }

    /// Returns the number of buffers currently leased out.
    pub fn outstanding(&self) -> usize {
        self.outstanding
    }

    /// Returns the number of buffers available for lease.
    pub fn available(&self) -> usize {
        self.available.len()
    }

    /// Returns the size of each individual buffer.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_and_release() {
        let mut pool = BufferPool::new(4, 4096);
        assert_eq!(pool.available(), 4);
        assert_eq!(pool.outstanding(), 0);

        let buf = pool.lease().expect("should have buffers");
        assert_eq!(buf.capacity(), 4096);
        assert_eq!(pool.available(), 3);
        assert_eq!(pool.outstanding(), 1);

        pool.release(buf);
        assert_eq!(pool.available(), 4);
        assert_eq!(pool.outstanding(), 0);
    }

    #[test]
    fn exhaustion() {
        let mut pool = BufferPool::new(1, 512);
        let _buf = pool.lease().unwrap();
        assert!(pool.lease().is_none());
    }
}
