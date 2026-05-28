use std::collections::VecDeque;
use std::ptr;

/// Page size (4 KiB) — the minimum alignment for mmap-backed buffers.
const PAGE_SIZE: usize = 4096;

/// A leased buffer backed by mmap-allocated, page-aligned memory.
pub struct Buffer {
    /// Pointer to the start of the mmap region for this buffer.
    ptr: *mut u8,
    /// Size of the buffer in bytes.
    len: usize,
}

// SAFETY: The mmap region is exclusively owned by this Buffer and not aliased.
// It is safe to send across threads as no concurrent access is possible.
unsafe impl Send for Buffer {}

impl Buffer {
    /// Returns a mutable slice to the buffer data.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `self.ptr` is a valid mmap-allocated region of `self.len` bytes,
        // exclusively owned by this Buffer.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    /// Returns a slice to the buffer data.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `self.ptr` is a valid mmap-allocated region of `self.len` bytes.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Returns the buffer capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.len
    }
}

/// A contiguous mmap-backed memory region that holds all buffers.
struct MmapRegion {
    /// Pointer to the start of the mmap region.
    ptr: *mut u8,
    /// Total size of the region in bytes.
    total_bytes: usize,
}

impl MmapRegion {
    /// Allocates a page-aligned, anonymous mmap region of `total_bytes`.
    ///
    /// On Linux, also calls `mlock` to pin pages in physical memory
    /// (required for io_uring fixed buffer registration).
    fn new(total_bytes: usize) -> std::io::Result<Self> {
        if total_bytes == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "mmap region size must be greater than zero",
            ));
        }

        // SAFETY: We request anonymous private memory with no backing file.
        // MAP_ANONYMOUS | MAP_PRIVATE gives us zero-initialized pages.
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                total_bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        let ptr = ptr.cast::<u8>();

        // Fault pages during pool creation so the first client read/write does
        // not pay anonymous mmap first-touch latency in the benchmark window.
        // One byte per page is enough to commit the page while keeping startup
        // work linear in pages, not bytes.
        #[cfg(not(miri))]
        unsafe {
            let mut offset = 0usize;
            while offset < total_bytes {
                ptr.add(offset).write_volatile(0);
                offset += PAGE_SIZE;
            }
        }

        // On Linux, pin pages in physical memory for io_uring.
        #[cfg(all(target_os = "linux", not(miri)))]
        {
            // SAFETY: `ptr` is a valid mmap region of `total_bytes`.
            // mlock may fail (e.g. RLIMIT_MEMLOCK) — we log but don't fail.
            let ret = unsafe { libc::mlock(ptr.cast(), total_bytes) };
            if ret != 0 {
                tracing::warn!(
                    error = %std::io::Error::last_os_error(),
                    "mlock failed — io_uring fixed buffer registration may not work; \
                     consider increasing RLIMIT_MEMLOCK"
                );
            }
        }

        Ok(Self { ptr, total_bytes })
    }

    /// Returns a pointer to the `i`-th buffer of `buffer_size` bytes.
    #[inline]
    fn buffer_ptr(&self, index: usize, buffer_size: usize) -> *mut u8 {
        let Some(offset) = index.checked_mul(buffer_size) else {
            panic!("buffer index offset overflow");
        };
        let Some(end) = offset.checked_add(buffer_size) else {
            panic!("buffer index end overflow");
        };
        assert!(end <= self.total_bytes, "buffer index out of mmap region");
        // SAFETY: `offset..end` is within the mmap region, checked above.
        unsafe { self.ptr.add(offset) }
    }
}

impl Drop for MmapRegion {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` was allocated by mmap with `self.total_bytes`.
        #[cfg(all(target_os = "linux", not(miri)))]
        unsafe {
            libc::munlock(self.ptr.cast(), self.total_bytes);
        }

        // SAFETY: `self.ptr` was allocated by mmap and has not been unmapped.
        unsafe {
            libc::munmap(self.ptr.cast(), self.total_bytes);
        }
    }
}

/// Pre-allocated pool of fixed-size, page-aligned, mmap-backed buffers.
///
/// Suitable for io_uring fixed buffer registration. Buffers are leased out
/// for I/O operations and returned when complete.
pub struct BufferPool {
    /// Size of each individual buffer in bytes (page-aligned).
    buffer_size: usize,
    /// Total number of buffers.
    count: usize,
    /// Indices of available (not leased) buffers.
    available: VecDeque<usize>,
    /// Lease state by buffer index.
    leased: Vec<bool>,
    /// Number of currently leased buffers.
    outstanding: usize,
    /// The backing mmap region.
    region: MmapRegion,
}

impl BufferPool {
    /// Returns the page-aligned buffer size used by [`BufferPool::new`].
    #[inline]
    pub const fn aligned_buffer_size(buffer_size: usize) -> Option<usize> {
        let Some(adjusted) = buffer_size.checked_add(PAGE_SIZE - 1) else {
            return None;
        };
        Some(adjusted & !(PAGE_SIZE - 1))
    }

    /// Returns bytes reserved for a pool with `count` buffers.
    #[inline]
    pub const fn reserved_bytes_for(count: usize, buffer_size: usize) -> Option<usize> {
        let Some(aligned) = Self::aligned_buffer_size(buffer_size) else {
            return None;
        };
        count.checked_mul(aligned)
    }

    /// Creates a new buffer pool with `count` buffers of `buffer_size` bytes each.
    ///
    /// `buffer_size` is rounded up to the nearest page boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested size is zero, the rounded size
    /// overflows, or the mmap allocation fails.
    pub fn new(count: usize, buffer_size: usize) -> std::io::Result<Self> {
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "buffer_count must be greater than zero",
            ));
        }
        if buffer_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "buffer_size must be greater than zero",
            ));
        }
        let buffer_size = Self::aligned_buffer_size(buffer_size).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "buffer_size alignment overflow",
            )
        })?;
        let total_bytes = Self::reserved_bytes_for(count, buffer_size).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "buffer pool size overflow",
            )
        })?;

        let region = MmapRegion::new(total_bytes)?;

        let available = (0..count).collect();

        Ok(Self {
            buffer_size,
            count,
            available,
            leased: vec![false; count],
            outstanding: 0,
            region,
        })
    }

    /// Creates a new buffer pool with optional NUMA node binding.
    ///
    /// When `numa_node` is `Some(n)`, buffers will be allocated on NUMA node `n`.
    /// TODO(Phase 2): Implement actual NUMA-local allocation via mmap + mbind.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`BufferPool::new`].
    pub fn new_with_numa(
        count: usize,
        buffer_size: usize,
        _numa_node: Option<usize>,
    ) -> std::io::Result<Self> {
        Self::new(count, buffer_size)
    }

    /// Leases a buffer from the pool. Returns `None` if all buffers are in use.
    pub fn lease(&mut self) -> Option<Buffer> {
        self.available.pop_front().map(|index| {
            assert!(
                !self.leased[index],
                "buffer free-list contains leased index"
            );
            self.leased[index] = true;
            self.outstanding += 1;
            Buffer {
                ptr: self.region.buffer_ptr(index, self.buffer_size),
                len: self.buffer_size,
            }
        })
    }

    /// Returns a buffer to the pool.
    pub fn release(&mut self, buf: Buffer) {
        // Compute the buffer index from the pointer offset.
        let offset = (buf.ptr as usize)
            .checked_sub(self.region.ptr as usize)
            .expect("releasing a buffer not from this pool");
        assert_eq!(
            offset % self.buffer_size,
            0,
            "releasing an unaligned buffer pointer"
        );
        let index = offset / self.buffer_size;
        assert!(index < self.count, "releasing a buffer not from this pool");
        assert!(
            self.outstanding > 0,
            "releasing a buffer when none are leased"
        );
        assert!(self.leased[index], "releasing a buffer that is not leased");

        // Zero the buffer before reuse to prevent data leakage.
        // SAFETY: `buf.ptr` is a valid pointer to `buf.len` bytes within our mmap region.
        unsafe {
            ptr::write_bytes(buf.ptr, 0, buf.len);
        }

        self.available.push_back(index);
        self.leased[index] = false;
        self.outstanding -= 1;
    }

    /// Returns `iovec` descriptors for all buffers, suitable for
    /// `io_uring_register_buffers()`.
    pub fn as_iovecs(&self) -> Vec<libc::iovec> {
        (0..self.count)
            .map(|i| libc::iovec {
                iov_base: self.region.buffer_ptr(i, self.buffer_size).cast(),
                iov_len: self.buffer_size,
            })
            .collect()
    }

    /// Returns the total number of bytes managed by this pool.
    pub fn total_bytes(&self) -> usize {
        self.region.total_bytes
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

    /// Lease a buffer index from the pool without creating a `Buffer` object.
    ///
    /// Returns `None` if all buffers are in use. The caller accesses the
    /// buffer memory via [`ptr()`](Self::ptr).
    pub fn lease_index(&mut self) -> Option<usize> {
        let index = self.available.pop_front()?;
        assert!(
            !self.leased[index],
            "buffer free-list contains leased index"
        );
        self.leased[index] = true;
        self.outstanding += 1;
        Some(index)
    }

    /// Release a buffer index back to the pool (zeroes the buffer first).
    ///
    /// # Panics
    ///
    /// Panics if `index >= count`.
    pub fn release_index(&mut self, index: usize) {
        assert!(index < self.count, "releasing invalid buffer index");
        assert!(
            self.outstanding > 0,
            "releasing a buffer when none are leased"
        );
        assert!(self.leased[index], "releasing a buffer that is not leased");
        // SAFETY: `index` is within the mmap region (checked above).
        // The caller guarantees no outstanding I/O references this buffer.
        unsafe {
            ptr::write_bytes(
                self.region.buffer_ptr(index, self.buffer_size),
                0,
                self.buffer_size,
            );
        }
        self.available.push_back(index);
        self.leased[index] = false;
        self.outstanding -= 1;
    }

    /// Returns a mutable raw pointer to the buffer at `index`.
    ///
    /// The pointer is valid for `buffer_size()` bytes as long as the pool
    /// is alive and the buffer has not been released.
    ///
    /// # Panics
    ///
    /// Panics if `index >= count`.
    #[inline]
    pub fn ptr(&self, index: usize) -> *mut u8 {
        assert!(index < self.count, "buffer index out of range");
        self.region.buffer_ptr(index, self.buffer_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expect_pool_error(result: std::io::Result<BufferPool>) -> std::io::Error {
        match result {
            Ok(_) => panic!("buffer pool allocation should fail"),
            Err(error) => error,
        }
    }

    #[test]
    fn lease_and_release() {
        let mut pool = BufferPool::new(4, 4096).expect("buffer pool should allocate");
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
        let mut pool = BufferPool::new(1, 512).expect("buffer pool should allocate");
        let _buf = pool.lease().unwrap();
        assert!(pool.lease().is_none());
    }

    #[test]
    fn buffers_are_page_aligned() {
        let mut pool = BufferPool::new(4, 8192).expect("buffer pool should allocate");
        for _ in 0..4 {
            let buf = pool.lease().unwrap();
            assert_eq!(
                buf.ptr as usize % PAGE_SIZE,
                0,
                "buffer pointer must be page-aligned"
            );
            pool.release(buf);
        }
    }

    #[test]
    fn buffer_size_rounded_to_page() {
        // 5000 bytes should be rounded to 8192 (2 pages).
        let pool = BufferPool::new(2, 5000).expect("buffer pool should allocate");
        assert_eq!(pool.buffer_size(), 8192);
        assert_eq!(pool.total_bytes(), 2 * 8192);
        assert_eq!(BufferPool::aligned_buffer_size(5000), Some(8192));
        assert_eq!(BufferPool::reserved_bytes_for(2, 5000), Some(2 * 8192));
    }

    #[test]
    fn as_iovecs_returns_correct_count() {
        let pool = BufferPool::new(8, 4096).expect("buffer pool should allocate");
        let iovecs = pool.as_iovecs();
        assert_eq!(iovecs.len(), 8);
        for iov in &iovecs {
            assert_eq!(iov.iov_len, 4096);
            assert_eq!(iov.iov_base as usize % PAGE_SIZE, 0);
        }
    }

    #[test]
    fn total_bytes_correct() {
        let pool = BufferPool::new(16, 4096).expect("buffer pool should allocate");
        assert_eq!(pool.total_bytes(), 16 * 4096);
    }

    #[test]
    fn rejects_zero_sized_pool() {
        let err = expect_pool_error(BufferPool::new(0, 4096));
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

        let err = expect_pool_error(BufferPool::new(1, 0));
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_aligned_size_overflow() {
        let err = expect_pool_error(BufferPool::new(1, usize::MAX));
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_total_size_overflow() {
        let err = expect_pool_error(BufferPool::new(usize::MAX, PAGE_SIZE));
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    #[should_panic(expected = "buffer index out of range")]
    fn ptr_rejects_invalid_index_in_release_builds() {
        let pool = BufferPool::new(1, 4096).expect("buffer pool should allocate");
        let _ = pool.ptr(1);
    }

    #[test]
    #[should_panic(expected = "releasing invalid buffer index")]
    fn release_index_rejects_invalid_index_in_release_builds() {
        let mut pool = BufferPool::new(1, 4096).expect("buffer pool should allocate");
        pool.release_index(1);
    }

    #[test]
    #[should_panic(expected = "releasing a buffer when none are leased")]
    fn release_index_rejects_unleased_index_in_release_builds() {
        let mut pool = BufferPool::new(1, 4096).expect("buffer pool should allocate");
        pool.release_index(0);
    }

    #[test]
    #[should_panic(expected = "releasing a buffer that is not leased")]
    fn release_index_rejects_wrong_live_index() {
        let mut pool = BufferPool::new(2, 4096).expect("buffer pool should allocate");
        let _leased = pool.lease_index().expect("first index should lease");
        pool.release_index(1);
    }
}
