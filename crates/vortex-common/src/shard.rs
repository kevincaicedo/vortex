use crate::HASH_SLOTS;

/// Shard identifier — a `u16` newtype.
///
/// Shards are assigned to each cluster slot (0..16383) based on the CRC-16/XMODEM hash of the key.
/// is Redis Cluster compatible: `crc16(key) % 16384`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardId(u16);

impl ShardId {
    /// Creates a `ShardId` from a raw slot number.
    #[inline]
    pub const fn new(slot: u16) -> Self {
        Self(slot)
    }

    /// Computes the shard for a given key using CRC-16/XMODEM (Redis-compatible).
    ///
    /// Supports Redis hash tags: if the key contains `{...}`, only the content
    /// between the first `{` and the next `}` is hashed.
    pub fn from_key(key: &[u8], num_shards: u16) -> Self {
        let hash_input = extract_hash_tag(key).unwrap_or(key);
        let crc = crc16(hash_input);
        Self(crc % num_shards)
    }

    /// Returns the slot number for cluster mode (0..16383).
    pub fn cluster_slot(key: &[u8]) -> u16 {
        let hash_input = extract_hash_tag(key).unwrap_or(key);
        crc16(hash_input) % HASH_SLOTS
    }

    /// Returns the raw slot value.
    #[inline]
    pub const fn slot(self) -> u16 {
        self.0
    }
}

impl From<u16> for ShardId {
    fn from(v: u16) -> Self {
        Self(v)
    }
}

impl From<ShardId> for u16 {
    fn from(s: ShardId) -> Self {
        s.0
    }
}

/// Extracts the Redis hash tag from a key, if present.
///
/// If the key contains `{...}` where `...` is non-empty, returns the content
/// between the first `{` and the next `}`.
fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|&b| b == b'{')?;
    let close = key[open + 1..].iter().position(|&b| b == b'}')?;
    if close > 0 {
        Some(&key[open + 1..open + 1 + close])
    } else {
        None
    }
}

/// CRC-16/XMODEM — the same algorithm Redis uses for cluster slot assignment.
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_from_key_deterministic() {
        let s1 = ShardId::from_key(b"mykey", 16);
        let s2 = ShardId::from_key(b"mykey", 16);
        assert_eq!(s1, s2);
    }

    #[test]
    fn shard_in_range() {
        for i in 0..100u32 {
            let key = format!("key:{i}");
            let shard = ShardId::from_key(key.as_bytes(), 16);
            assert!(shard.slot() < 16);
        }
    }

    #[test]
    fn hash_tag_extraction() {
        // Keys with matching hash tags should map to the same shard.
        let s1 = ShardId::from_key(b"user:{123}.name", 1024);
        let s2 = ShardId::from_key(b"user:{123}.email", 1024);
        assert_eq!(s1, s2);
    }

    #[test]
    fn hash_tag_empty_braces() {
        // Empty hash tag `{}` should hash the full key.
        let s1 = ShardId::from_key(b"key{}", 1024);
        let s2 = ShardId::from_key(b"key{}", 1024);
        assert_eq!(s1, s2);
    }

    #[test]
    fn cluster_slot_range() {
        let slot = ShardId::cluster_slot(b"hello");
        assert!(slot < HASH_SLOTS);
    }

    #[test]
    fn crc16_known_values() {
        // Verify against known Redis CRC-16/XMODEM values.
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }
}
