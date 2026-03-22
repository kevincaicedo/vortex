use vortex_common::{VortexKey, VortexResult, VortexValue};

use crate::table::SwissTable;

/// A single database shard.
///
/// Each reactor thread owns one shard. All operations on a shard
/// are single-threaded — no locks needed.
pub struct Shard {
    /// Shard identifier.
    pub id: u16, // TODO: there is a ShardId type in vortex-common, should we use that instead?
    /// The key-value store.
    data: SwissTable,
}

impl Shard {
    /// Creates a new empty shard.
    pub fn new(id: u16) -> Self {
        Self {
            id,
            data: SwissTable::new(),
        }
    }

    /// GET: retrieves the value for a key.
    pub fn get(&self, key: &VortexKey) -> Option<&VortexValue> {
        self.data.get(key)
    }

    /// SET: sets a key-value pair.
    pub fn set(&mut self, key: VortexKey, value: VortexValue) -> Option<VortexValue> {
        self.data.insert(key, value)
    }

    /// DEL: deletes a key.
    pub fn del(&mut self, key: &VortexKey) -> bool {
        self.data.remove(key).is_some()
    }

    /// EXISTS: checks if a key exists.
    pub fn exists(&self, key: &VortexKey) -> bool {
        self.data.contains_key(key)
    }

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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_basic_ops() {
        let mut shard = Shard::new(0);

        let key = VortexKey::from("hello");
        shard.set(key.clone(), VortexValue::from("world"));

        assert!(shard.exists(&key));
        assert_eq!(shard.len(), 1);

        let val = shard.get(&key).unwrap();
        assert!(matches!(val, VortexValue::InlineString(_)));

        assert!(shard.del(&key));
        assert!(!shard.exists(&key));
        assert!(shard.is_empty());
    }

    #[test]
    fn shard_flush() {
        let mut shard = Shard::new(0);
        for i in 0..100 {
            let key = VortexKey::from(format!("key:{i}").as_str());
            shard.set(key, VortexValue::from(i as i64));
        }
        assert_eq!(shard.len(), 100);

        shard.flush();
        assert!(shard.is_empty());
    }
}
