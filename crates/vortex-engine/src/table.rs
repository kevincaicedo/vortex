use std::collections::HashMap;

use ahash::RandomState;
use vortex_common::{VortexKey, VortexValue};

/// TODO: Swiss Table placeholder — wraps `HashMap` with `ahash` for Phase 0.
///
/// TODO: Phase 3 replaces this with a true SIMD-probed open-addressing
/// hash table with 16-slot groups and control byte arrays.
pub struct SwissTable {
    inner: HashMap<VortexKey, VortexValue, RandomState>,
}

impl SwissTable {
    /// Creates a new empty table.
    pub fn new() -> Self {
        Self {
            inner: HashMap::with_hasher(RandomState::new()),
        }
    }

    /// Creates a new table with pre-allocated capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: HashMap::with_capacity_and_hasher(cap, RandomState::new()),
        }
    }

    /// Inserts a key-value pair. Returns the old value if the key existed.
    pub fn insert(&mut self, key: VortexKey, value: VortexValue) -> Option<VortexValue> {
        self.inner.insert(key, value)
    }

    /// Gets a reference to the value for a key.
    pub fn get(&self, key: &VortexKey) -> Option<&VortexValue> {
        self.inner.get(key)
    }

    /// Gets a mutable reference to the value for a key.
    pub fn get_mut(&mut self, key: &VortexKey) -> Option<&mut VortexValue> {
        self.inner.get_mut(key)
    }

    /// Removes a key and returns its value.
    pub fn remove(&mut self, key: &VortexKey) -> Option<VortexValue> {
        self.inner.remove(key)
    }

    /// Returns true if the key exists.
    pub fn contains_key(&self, key: &VortexKey) -> bool {
        self.inner.contains_key(key)
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Default for SwissTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("test_key");
        let val = VortexValue::from("test_value");

        assert!(table.insert(key.clone(), val).is_none());
        assert!(table.contains_key(&key));
        assert_eq!(table.len(), 1);

        let retrieved = table.get(&key).unwrap();
        assert!(matches!(retrieved, VortexValue::InlineString(_)));
    }

    #[test]
    fn remove() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("key");
        table.insert(key.clone(), VortexValue::from(42i64));

        let removed = table.remove(&key);
        assert!(removed.is_some());
        assert!(table.is_empty());
    }

    #[test]
    fn overwrite() {
        let mut table = SwissTable::new();
        let key = VortexKey::from("k");
        table.insert(key.clone(), VortexValue::from(1i64));
        let old = table.insert(key.clone(), VortexValue::from(2i64));
        assert_eq!(old, Some(VortexValue::from(1i64)));
        assert_eq!(table.get(&key), Some(&VortexValue::from(2i64)));
    }
}
