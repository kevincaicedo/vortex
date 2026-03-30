//! Platform-agnostic software prefetch intrinsics.
//!
//! Prefetch is a **hint** to the CPU's memory subsystem to start loading
//! a cache line before it is needed. It never traps, never faults, and
//! cannot affect program correctness — it only affects performance.
//!
//! # Platform support
//!
//! | Arch    | Read hint        | Write hint        |
//! |---------|------------------|-------------------|
//! | x86_64  | `_mm_prefetch T0`| `_mm_prefetch T0` |
//! | aarch64 | `PRFM PLDL1KEEP` | `PRFM PSTL1KEEP` |
//! | other   | no-op            | no-op             |

/// Prefetch a cache line for **reading** into L1 cache.
///
/// # Safety
/// This function is always safe — prefetch never traps. The `unsafe`
/// block is needed only for the platform intrinsic call, not because
/// the operation itself can cause undefined behavior.
#[inline(always)]
pub fn prefetch_read<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: _mm_prefetch is a hint; it never traps even on invalid
        // addresses. _MM_HINT_T0 loads into all cache levels (L1+L2+L3).
        unsafe {
            core::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86_64::_MM_HINT_T0);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: PRFM is a hint instruction; it never faults.
        // _prefetch(ptr, READ=0, LOCALITY_L1=3) → PRFM PLDL1KEEP, [ptr]
        unsafe {
            core::arch::aarch64::_prefetch(ptr.cast::<i8>(), 0, 3);
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = ptr;
    }
}

/// Prefetch a cache line for **writing** into L1 cache.
///
/// Uses write-intent hinting so the cache line is fetched in Exclusive
/// or Modified state, avoiding a subsequent upgrade miss when the store
/// actually happens.
#[inline(always)]
pub fn prefetch_write<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: _mm_prefetch is a hint; never traps.
        // Note: on x86, prefetchw (write) requires 3DNow!/PREFETCHW support.
        // _MM_HINT_ET0 = exclusive/write to L1 on CPUs that support it,
        // falls back to read prefetch on older CPUs.
        // Using T0 universally is safest and still highly effective.
        unsafe {
            core::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86_64::_MM_HINT_T0);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: PRFM is a hint instruction; it never faults.
        // _prefetch(ptr, WRITE=1, LOCALITY_L1=3) → PRFM PSTL1KEEP, [ptr]
        unsafe {
            core::arch::aarch64::_prefetch(ptr.cast::<i8>(), 1, 3);
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = ptr;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefetch_read_does_not_panic() {
        let data = [0u8; 128];
        prefetch_read(data.as_ptr());
        prefetch_read(core::ptr::null::<u8>()); // NULL is fine — hint never traps
    }

    #[test]
    fn prefetch_write_does_not_panic() {
        let data = [0u8; 128];
        prefetch_write(data.as_ptr());
        prefetch_write(core::ptr::null::<u8>()); // NULL is fine
    }

    #[test]
    fn prefetch_typed_pointer() {
        let val: u64 = 42;
        prefetch_read(&val as *const u64);
        prefetch_write(&val as *const u64);
    }
}
