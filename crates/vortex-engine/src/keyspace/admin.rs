use std::ffi::{CString, c_void};
use std::sync::atomic::Ordering;

use tikv_jemalloc_sys::mallctl;

use crate::table::SwissTable;

use super::{AofLsn, ConcurrentKeyspace};

/// Best-effort jemalloc cache and arena purge after FLUSHDB/FLUSHALL.
///
/// Flushes the calling thread's tcache and then purges dirty pages in every
/// arena. This reduces RSS after a large FLUSH without waiting for
/// jemalloc's background decay.
///
/// # Platform assumptions
///
/// - The process is linked against tikv-jemalloc-sys (guaranteed by the
///   crate dependency).
/// - `mallctl` follows the jemalloc 5.x ABI for `thread.tcache.flush`,
///   `arenas.narenas`, and `arena.<i>.purge`.
/// - `CString::new` can only fail if the MIB name contains an interior NUL,
///   which none of these names do; the `if let Ok` guard is defense-in-depth.
///
/// # Error handling
///
/// All `mallctl` return codes are intentionally ignored (`let _ = ...`).
/// Purge failures are non-fatal: the allocator will reclaim pages through
/// its normal background decay. Logging is omitted to avoid pulling I/O
/// dependencies into the engine crate.
fn purge_allocator_after_flush() {
    // SAFETY: All `mallctl` calls use well-known jemalloc 5.x MIB names via
    // valid null-terminated `CString` pointers. Pointer arguments are either
    // null (no value exchange) or point to stack-local variables with correct
    // size and alignment (`arena_count: c_uint`, `arena_len: usize`). The
    // jemalloc ABI guarantees thread-safety for these calls — `mallctl` is
    // internally synchronized. Return codes are ignored because purge is
    // best-effort: failure leaves jemalloc to reclaim pages via background
    // decay, which is the normal non-FLUSH path anyway.
    unsafe {
        if let Ok(name) = CString::new("thread.tcache.flush") {
            let _ = mallctl(
                name.as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            );
        }

        let mut arena_count: libc::c_uint = 0;
        let mut arena_len = std::mem::size_of::<libc::c_uint>();
        if let Ok(name) = CString::new("arenas.narenas") {
            let rc = mallctl(
                name.as_ptr(),
                (&mut arena_count as *mut libc::c_uint).cast::<c_void>(),
                &mut arena_len,
                std::ptr::null_mut(),
                0,
            );
            if rc == 0 {
                for arena_idx in 0..arena_count {
                    if let Ok(name) = CString::new(format!("arena.{arena_idx}.purge")) {
                        let _ = mallctl(
                            name.as_ptr(),
                            std::ptr::null_mut(),
                            std::ptr::null_mut(),
                            std::ptr::null_mut(),
                            0,
                        );
                    }
                }
            }
        }
    }
}

impl ConcurrentKeyspace {
    /// Exact live key and expiring-key counts from a consistent all-shard snapshot.
    ///
    /// This administrative path acquires read locks for every shard and counts
    /// only entries that are still live at `now_nanos`.
    pub(crate) fn exact_keyspace_counts(&self, now_nanos: u64) -> (usize, usize) {
        let guards: Vec<_> = self.shards.iter().map(|shard| shard.read()).collect();
        let mut keys = 0usize;
        let mut expires = 0usize;

        for guard in &guards {
            for entry in guard.iter_entries() {
                if entry.is_expired(now_nanos) {
                    continue;
                }
                keys += 1;
                if entry.ttl_deadline() != 0 {
                    expires += 1;
                }
            }
        }

        (keys, expires)
    }

    /// FLUSHDB / FLUSHALL: remove all keys from all shards.
    ///
    /// Acquires a write lock on each shard sequentially. Not atomic across
    /// shards — concurrent reads may see partial results during flush.
    ///
    /// Outstanding memory reservations are intentionally left untouched. A
    /// [`MemoryReservation`] is an owning token; only that token may release its
    /// bytes from `memory_reserved`.
    #[cfg(test)]
    pub(crate) fn flush_all(&self) {
        for shard in self.shards.iter() {
            let mut guard = shard.write();
            *guard = SwissTable::with_hasher(self.table_hasher.clone());
        }
        for count in self.expiry_key_count.iter() {
            count.store(0, Ordering::Relaxed);
        }
        self.expiry_key_total.store(0, Ordering::Relaxed);
        self.global_memory_used.store(0, Ordering::Relaxed);
        purge_allocator_after_flush();
        self.bump_all_watches();
    }

    /// FLUSHDB / FLUSHALL with an optional AOF LSN allocated while all shard
    /// write locks are held.
    ///
    /// Outstanding memory reservations are intentionally left untouched. A
    /// [`MemoryReservation`] is an owning token; only that token may release its
    /// bytes from `memory_reserved`.
    pub(crate) fn flush_all_with_lsn(&self) -> Option<AofLsn> {
        let mut guards = Vec::with_capacity(self.shards.len());
        for shard in self.shards.iter() {
            guards.push(shard.write());
        }

        let had_entries = guards.iter().any(|guard| !guard.is_empty());
        for guard in &mut guards {
            **guard = SwissTable::with_hasher(self.table_hasher.clone());
        }
        for count in self.expiry_key_count.iter() {
            count.store(0, Ordering::Relaxed);
        }
        self.expiry_key_total.store(0, Ordering::Relaxed);
        self.global_memory_used.store(0, Ordering::Relaxed);
        let aof_lsn = had_entries.then(|| self.next_aof_lsn()).flatten();
        drop(guards);
        purge_allocator_after_flush();
        if had_entries {
            self.bump_all_watches();
        }

        aof_lsn
    }

    /// Returns the exact memory usage across all shards.
    pub fn memory_used(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().memory_used())
            .sum()
    }

    /// Returns the approximate published memory counter used by OOM/eviction checks.
    /// It is intentionally cheap and may lag exact shard-local memory briefly.
    #[inline]
    pub fn approx_memory_used(&self) -> usize {
        self.global_memory_used.load(Ordering::Relaxed)
    }
}
