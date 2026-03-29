use vortex_common::{ShardId, VortexKey, VortexResult, VortexValue};

use crate::expiry::{ExpiryEntry, ExpiryWheel};
use crate::table::SwissTable;

/// Result of a SET command with options (NX/XX/GET).
pub enum SetResult {
    /// SET succeeded (no GET flag).
    Ok,
    /// SET was not performed (NX/XX condition failed, no GET flag).
    NotSet,
    /// SET succeeded — returns old value (GET flag was set).
    OkGet(Option<VortexValue>),
    /// SET was not performed — returns current value (GET flag + NX/XX failed).
    NotSetGet(Option<VortexValue>),
}

/// A single database shard.
///
/// Each reactor thread owns one shard. All operations on a shard
/// are single-threaded — no locks needed.
pub struct Shard {
    /// Shard identifier.
    pub id: ShardId,
    /// The key-value store.
    data: SwissTable,
    /// Dual timing wheel for TTL expiry.
    expiry: ExpiryWheel,
    /// Reusable buffer for expired entries from the timing wheel.
    expired_buf: Vec<ExpiryEntry>,
}

impl Shard {
    /// Creates a new empty shard.
    pub fn new(id: ShardId) -> Self {
        Self {
            id,
            data: SwissTable::new(),
            expiry: ExpiryWheel::new(),
            expired_buf: Vec::with_capacity(64),
        }
    }

    /// Creates a new empty shard with the expiry wheel initialized to `now_nanos`.
    pub fn new_with_time(id: ShardId, now_nanos: u64) -> Self {
        let mut s = Self::new(id);
        s.expiry.set_time(now_nanos);
        s
    }

    // ── Read operations (lazy expiry) ───────────────────────────────

    /// GET: retrieves the value for a key.
    ///
    /// Performs lazy expiry: if the entry has a TTL deadline that is past
    /// `now_nanos`, the entry is deleted transparently and `None` returned.
    pub fn get(&mut self, key: &VortexKey, now_nanos: u64) -> Option<&VortexValue> {
        self.data.get_or_expire(key, now_nanos)
    }

    /// EXISTS: checks if a key exists (with lazy expiry).
    pub fn exists(&mut self, key: &VortexKey, now_nanos: u64) -> bool {
        self.data.contains_key_or_expire(key, now_nanos)
    }

    // ── Write operations ────────────────────────────────────────────

    /// SET: sets a key-value pair (no TTL).
    pub fn set(&mut self, key: VortexKey, value: VortexValue) -> Option<VortexValue> {
        self.data.insert(key, value)
    }

    /// SET with TTL: sets a key-value pair with an absolute nanosecond deadline.
    ///
    /// Registers the key in the expiry wheel for active sweep.
    pub fn set_with_ttl(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        ttl_deadline_nanos: u64,
    ) -> Option<VortexValue> {
        let hash = self.data.hash_key_bytes(key.as_bytes());
        let prev = self.data.insert_with_ttl(key, value, ttl_deadline_nanos);
        if ttl_deadline_nanos != 0 {
            self.expiry.register(hash, ttl_deadline_nanos);
        }
        prev
    }

    /// DEL: deletes a key.
    pub fn del(&mut self, key: &VortexKey) -> bool {
        self.data.remove(key).is_some()
    }

    /// DEL returning the removed value (for GETDEL).
    pub fn remove(&mut self, key: &VortexKey) -> Option<VortexValue> {
        self.data.remove(key)
    }

    /// Returns a mutable reference to the value for a key (no lazy expiry).
    ///
    /// Used by INCR/APPEND for in-place modification.
    #[inline]
    pub fn get_mut(&mut self, key: &VortexKey) -> Option<&mut VortexValue> {
        self.data.get_mut(key)
    }

    /// SET + options: handles NX (only set if key doesn't exist),
    /// XX (only set if key exists), GET (return old value), KEEPTTL.
    ///
    /// Returns `SetResult` indicating what happened.
    pub fn set_with_options(
        &mut self,
        key: VortexKey,
        value: VortexValue,
        ttl_deadline: u64,
        nx: bool,
        xx: bool,
        get: bool,
        keepttl: bool,
    ) -> SetResult {
        let exists = self.data.contains_key(&key);

        // NX: only set if NOT exists
        if nx && exists {
            return if get {
                SetResult::NotSetGet(self.data.get(&key).cloned())
            } else {
                SetResult::NotSet
            };
        }
        // XX: only set if EXISTS
        if xx && !exists {
            return if get {
                SetResult::NotSetGet(None)
            } else {
                SetResult::NotSet
            };
        }

        // Preserve old TTL if KEEPTTL and key already exists.
        let effective_ttl = if keepttl && exists {
            self.data.get_entry_ttl(&key).unwrap_or(0)
        } else {
            ttl_deadline
        };

        let old = if effective_ttl != 0 {
            let hash = self.data.hash_key_bytes(key.as_bytes());
            let prev = self.data.insert_with_ttl(key, value, effective_ttl);
            self.expiry.register(hash, effective_ttl);
            prev
        } else {
            self.data.insert(key, value)
        };

        if get {
            SetResult::OkGet(old)
        } else {
            SetResult::Ok
        }
    }

    /// Prefetch the control byte group for a key (for MGET/MSET batching).
    #[inline]
    pub fn prefetch(&self, key: &VortexKey) {
        let hash = self.data.hash_key_bytes(key.as_bytes());
        self.data.prefetch_group(hash);
    }

    // ── TTL operations ──────────────────────────────────────────────

    /// EXPIRE / PEXPIRE: set a TTL on an existing key.
    ///
    /// Returns `true` if the key exists and the TTL was set.
    pub fn expire(&mut self, key: &VortexKey, deadline_nanos: u64) -> bool {
        if !self.data.set_entry_ttl(key, deadline_nanos) {
            return false;
        }
        let hash = self.data.hash_key_bytes(key.as_bytes());
        self.expiry.register(hash, deadline_nanos);
        true
    }

    /// PERSIST: remove the TTL from a key.
    ///
    /// The old expiry wheel entry becomes a ghost — it will be silently
    /// skipped during the next tick because the entry's deadline won't match.
    pub fn persist(&mut self, key: &VortexKey) -> bool {
        self.data.clear_entry_ttl(key)
    }

    /// TTL / PTTL: get remaining time to live.
    ///
    /// Returns:
    /// - `None` if the key doesn't exist
    /// - `Some(0)` if the key has no TTL (persistent)
    /// - `Some(remaining_nanos)` if the key has a deadline
    pub fn ttl(&self, key: &VortexKey) -> Option<u64> {
        self.data.get_entry_ttl(key)
    }

    // ── Active expiry sweep ─────────────────────────────────────────

    /// Run one round of active expiry.
    ///
    /// Ticks the timing wheel, probes the Swiss Table for each candidate,
    /// and removes entries whose deadline has truly passed.
    ///
    /// Returns `(expired_count, sampled_count)`. The caller should schedule
    /// another sweep if `expired_count > sampled_count / 4` (>25% rate).
    pub fn run_active_expiry(&mut self, now_nanos: u64, max_effort: usize) -> (usize, usize) {
        self.expired_buf.clear();
        self.expiry
            .tick(now_nanos, max_effort, &mut self.expired_buf);

        let sampled = self.expired_buf.len();
        let mut expired = 0;

        for i in 0..sampled {
            let entry = self.expired_buf[i];
            if self
                .data
                .remove_expired_by_hash(entry.key_hash, entry.deadline_nanos)
            {
                expired += 1;
            }
            // If remove failed, the entry is a ghost (re-EXPIRE'd or PERSIST'd).
        }

        (expired, sampled)
    }

    // ── Metadata ────────────────────────────────────────────────────

    /// Returns the number of keys in this shard.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the shard is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// DBSIZE: returns the number of keys.
    pub fn dbsize(&self) -> VortexResult<usize> {
        Ok(self.data.len())
    }

    /// FLUSHDB: removes all keys from this shard.
    pub fn flush(&mut self) {
        self.data = SwissTable::new();
        self.expiry = ExpiryWheel::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS_PER_SEC: u64 = 1_000_000_000;
    const NS_PER_MS: u64 = 1_000_000;

    #[test]
    fn shard_basic_ops() {
        let mut shard = Shard::new(ShardId::new(0));

        let key = VortexKey::from("hello");
        shard.set(key.clone(), VortexValue::from("world"));

        assert!(shard.exists(&key, 0));
        assert_eq!(shard.len(), 1);

        let val = shard.get(&key, 0).unwrap();
        assert!(matches!(val, VortexValue::InlineString(_)));

        assert!(shard.del(&key));
        assert!(!shard.exists(&key, 0));
        assert!(shard.is_empty());
    }

    #[test]
    fn shard_flush() {
        let mut shard = Shard::new(ShardId::new(0));
        for i in 0..100 {
            let key = VortexKey::from(format!("key:{i}").as_str());
            shard.set(key, VortexValue::from(i as i64));
        }
        assert_eq!(shard.len(), 100);

        shard.flush();
        assert!(shard.is_empty());
    }

    // ── Lazy expiry tests ───────────────────────────────────────────

    #[test]
    fn lazy_expiry_get_returns_none() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from("ephemeral");
        let deadline = 5 * NS_PER_SEC;

        shard.set_with_ttl(key.clone(), VortexValue::from("bye"), deadline);
        assert_eq!(shard.len(), 1);

        // Before deadline — key visible
        assert!(shard.get(&key, 4 * NS_PER_SEC).is_some());
        assert_eq!(shard.len(), 1);

        // After deadline — lazy expiry triggers
        assert!(shard.get(&key, 6 * NS_PER_SEC).is_none());
        assert_eq!(shard.len(), 0);
    }

    #[test]
    fn lazy_expiry_exists_returns_false() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from("temp");
        shard.set_with_ttl(key.clone(), VortexValue::from("data"), 5 * NS_PER_SEC);

        assert!(shard.exists(&key, 4 * NS_PER_SEC));
        assert!(!shard.exists(&key, 6 * NS_PER_SEC));
        assert_eq!(shard.len(), 0);
    }

    // ── TTL operation tests ─────────────────────────────────────────

    #[test]
    fn expire_and_ttl() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from("mykey");
        shard.set(key.clone(), VortexValue::from("val"));

        // No TTL initially
        assert_eq!(shard.ttl(&key), Some(0));

        // Set TTL
        assert!(shard.expire(&key, 10 * NS_PER_SEC));
        assert_eq!(shard.ttl(&key), Some(10 * NS_PER_SEC));

        // PERSIST removes TTL
        assert!(shard.persist(&key));
        assert_eq!(shard.ttl(&key), Some(0));
    }

    #[test]
    fn persist_on_nonexistent_key() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from("ghost");
        assert!(!shard.persist(&key));
    }

    #[test]
    fn expire_on_nonexistent_key() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from("ghost");
        assert!(!shard.expire(&key, 10 * NS_PER_SEC));
    }

    #[test]
    fn re_expire_ghosts_old_entry() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from("reexpire");
        shard.set_with_ttl(key.clone(), VortexValue::from("v"), 5 * NS_PER_SEC);

        // Re-expire with a later deadline
        shard.expire(&key, 20 * NS_PER_SEC);
        assert_eq!(shard.ttl(&key), Some(20 * NS_PER_SEC));

        // Key should still be alive at t=6s (old deadline passed, new hasn't)
        assert!(shard.get(&key, 6 * NS_PER_SEC).is_some());
    }

    // ── Active expiry tests ─────────────────────────────────────────

    #[test]
    fn active_expiry_deletes_expired_keys() {
        let mut shard = Shard::new(ShardId::new(0));

        for i in 0..10 {
            let key = VortexKey::from(format!("key:{i}").as_str());
            shard.set_with_ttl(key, VortexValue::from("val"), 5 * NS_PER_SEC);
        }
        assert_eq!(shard.len(), 10);

        // Run sweep after deadline
        let (expired, _sampled) = shard.run_active_expiry(6 * NS_PER_SEC, 100);
        assert_eq!(expired, 10);
        assert_eq!(shard.len(), 0);
    }

    #[test]
    fn active_expiry_skips_ghosts() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from("ghosted");

        // Set with TTL=5s
        shard.set_with_ttl(key.clone(), VortexValue::from("v1"), 5 * NS_PER_SEC);

        // PERSIST → old wheel entry becomes ghost
        shard.persist(&key);

        // Active sweep at t=6s should skip the ghost (deadline mismatch)
        let (expired, _) = shard.run_active_expiry(6 * NS_PER_SEC, 100);
        assert_eq!(expired, 0);
        assert_eq!(shard.len(), 1); // key still alive
    }

    #[test]
    fn active_expiry_bounded_effort() {
        let mut shard = Shard::new(ShardId::new(0));

        for i in 0..50 {
            let key = VortexKey::from(format!("k:{i}").as_str());
            shard.set_with_ttl(key, VortexValue::from("v"), 5 * NS_PER_SEC);
        }

        // Only allow 20 entries per sweep
        let (expired, _) = shard.run_active_expiry(6 * NS_PER_SEC, 20);
        assert!(expired <= 20);
        assert!(shard.len() >= 30); // at least 30 still alive

        // Another sweep gets the rest
        let (expired2, _) = shard.run_active_expiry(6 * NS_PER_SEC, 100);
        assert!(expired + expired2 >= 50 || shard.is_empty());
    }

    #[test]
    fn no_latency_impact_on_non_ttl_keys() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from("persistent");
        shard.set(key.clone(), VortexValue::from("forever"));

        // GET with now > 0 should still return the value (no TTL = deadline 0)
        assert!(shard.get(&key, 999 * NS_PER_SEC).is_some());
    }

    #[test]
    fn millis_precision_expiry() {
        let mut shard = Shard::new(ShardId::new(0));
        let key = VortexKey::from("fast");

        // PX 100ms
        let deadline = 100 * NS_PER_MS;
        shard.set_with_ttl(key.clone(), VortexValue::from("boom"), deadline);

        // At 50ms — still alive
        assert!(shard.get(&key, 50 * NS_PER_MS).is_some());

        // At 150ms — lazy expiry
        assert!(shard.get(&key, 150 * NS_PER_MS).is_none());
    }
}
