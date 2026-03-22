use crate::Timestamp;

/// Expiry / time-to-live representation.
///
/// - `None` — no expiry, key lives forever.
/// - `At` — expire at a specific absolute timestamp (second resolution, EX/EXPIREAT).
/// - `Px` — expire at a specific absolute timestamp (millisecond resolution, PX/PEXPIREAT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TTL {
    /// No expiry.
    None,
    /// Expire at this absolute timestamp (second resolution).
    At(Timestamp),
    /// Expire at this absolute timestamp (millisecond resolution).
    Px(Timestamp),
}

impl TTL {
    /// Returns `true` if this TTL has expired relative to `now`.
    #[inline]
    pub fn is_expired(&self, now: Timestamp) -> bool {
        match self {
            Self::None => false,
            Self::At(ts) | Self::Px(ts) => now >= *ts,
        }
    }

    /// Returns `true` if this key has no expiry.
    #[inline]
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Returns the absolute expiry timestamp, if any.
    #[inline]
    pub fn deadline(&self) -> Option<Timestamp> {
        match self {
            Self::None => None,
            Self::At(ts) | Self::Px(ts) => Some(*ts),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_none_never_expires() {
        let ttl = TTL::None;
        let far_future = Timestamp::from_secs(u64::MAX / 1_000_000_000);
        assert!(!ttl.is_expired(far_future));
        assert!(ttl.is_none());
    }

    #[test]
    fn ttl_at_expired() {
        let ttl = TTL::At(Timestamp::from_secs(100));
        assert!(ttl.is_expired(Timestamp::from_secs(101)));
        assert!(ttl.is_expired(Timestamp::from_secs(100)));
        assert!(!ttl.is_expired(Timestamp::from_secs(99)));
    }

    #[test]
    fn ttl_px_expired() {
        let ttl = TTL::Px(Timestamp::from_millis(5000));
        assert!(ttl.is_expired(Timestamp::from_millis(5001)));
        assert!(!ttl.is_expired(Timestamp::from_millis(4999)));
    }

    #[test]
    fn ttl_deadline() {
        assert_eq!(TTL::None.deadline(), None);
        let ts = Timestamp::from_secs(42);
        assert_eq!(TTL::At(ts).deadline(), Some(ts));
    }
}
