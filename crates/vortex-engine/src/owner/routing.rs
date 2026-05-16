//! Deterministic key-to-owner routing for the shared-nothing experiment.
//!
//! The routing formula is:
//!
//! ```text
//! route_hash      = ahash_fixed_seed(key)
//! capsule_bits    = log2(capsule_count)
//! capsule_index   = if capsule_bits == 0 { 0 } else { route_hash >> (64 - capsule_bits) }
//! owner_index     = capsule_to_owner[capsule_index]
//! ```
//!
//! `capsule_to_owner` is a static map built at topology construction. The
//! initial map assigns contiguous capsule ranges as evenly as possible across
//! owners.

use std::sync::atomic::{AtomicU64, Ordering};

use ahash::RandomState;
use crossbeam_utils::CachePadded;

const ROUTING_AHASH_SEED_0: u64 = 0x517c_c1b7_2722_0a95;
const ROUTING_AHASH_SEED_1: u64 = 0x6c62_272e_07bb_0142;
const ROUTING_AHASH_SEED_2: u64 = 0x8fbc_2d2b_9e3a_6ee8;
const ROUTING_AHASH_SEED_3: u64 = 0xcf41_41b0_ed82_a837;

/// Logical owner reactor identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct OwnerId(u32);

impl OwnerId {
    #[inline]
    #[doc(hidden)]
    pub const fn from_validated_index(index: usize) -> Self {
        debug_assert!(index <= u32::MAX as usize);
        Self(index as u32)
    }

    /// Returns this owner as a zero-based index.
    #[inline]
    pub const fn get(self) -> usize {
        self.0 as usize
    }
}

/// Stable key capsule identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct KeyCapsuleId(u32);

impl KeyCapsuleId {
    #[inline]
    pub(crate) const fn from_validated_index(index: usize) -> Self {
        debug_assert!(index <= u32::MAX as usize);
        Self(index as u32)
    }

    /// Returns this capsule as a zero-based index.
    #[inline]
    pub const fn get(self) -> usize {
        self.0 as usize
    }
}

/// Topology publication epoch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct TopologyEpoch(u64);

impl TopologyEpoch {
    /// Initial static topology epoch.
    pub const INITIAL: Self = Self(0);

    /// Creates an epoch from a persisted or externally published value.
    #[inline]
    pub const fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    /// Returns the raw epoch value.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Fixed-seed routing hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct RouteHash(u64);

impl RouteHash {
    /// Returns the raw 64-bit route hash.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Validated static topology configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopologyConfig {
    owner_count: usize,
    capsule_count: usize,
    epoch: TopologyEpoch,
}

impl TopologyConfig {
    /// Creates a validated topology config.
    ///
    /// `capsule_count` must be a power of two, and `owner_count` must be
    /// nonzero. Owner counts are allowed to follow physical core or NUMA
    /// topology and therefore do not need to be powers of two.
    pub fn new(owner_count: usize, capsule_count: usize) -> Result<Self, TopologyConfigError> {
        Self::with_epoch(owner_count, capsule_count, TopologyEpoch::INITIAL)
    }

    /// Creates a validated topology config with an explicit epoch.
    pub fn with_epoch(
        owner_count: usize,
        capsule_count: usize,
        epoch: TopologyEpoch,
    ) -> Result<Self, TopologyConfigError> {
        if owner_count == 0 {
            return Err(TopologyConfigError::OwnerCountZero);
        }
        validate_capsule_count(capsule_count)?;

        if owner_count > u32::MAX as usize + 1 {
            return Err(TopologyConfigError::OwnerCountTooLarge { owner_count });
        }
        if capsule_count > u32::MAX as usize + 1 {
            return Err(TopologyConfigError::CapsuleCountTooLarge { capsule_count });
        }
        if capsule_count < owner_count {
            return Err(TopologyConfigError::CapsuleCountLessThanOwnerCount {
                owner_count,
                capsule_count,
            });
        }

        Ok(Self {
            owner_count,
            capsule_count,
            epoch,
        })
    }

    /// Number of owner reactors in this topology.
    #[inline]
    pub const fn owner_count(self) -> usize {
        self.owner_count
    }

    /// Number of key capsules in this topology.
    #[inline]
    pub const fn capsule_count(self) -> usize {
        self.capsule_count
    }

    /// Published topology epoch.
    #[inline]
    pub const fn epoch(self) -> TopologyEpoch {
        self.epoch
    }

    #[inline]
    const fn capsule_bits(self) -> u32 {
        self.capsule_count.trailing_zeros()
    }
}

/// Topology configuration validation error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopologyConfigError {
    /// Owner count was zero.
    OwnerCountZero,
    /// Capsule count was zero.
    CapsuleCountZero,
    /// Capsule count was not a power of two.
    CapsuleCountNotPowerOfTwo { capsule_count: usize },
    /// Owner count cannot fit in the public `OwnerId` representation.
    OwnerCountTooLarge { owner_count: usize },
    /// Capsule count cannot fit in the public `KeyCapsuleId` representation.
    CapsuleCountTooLarge { capsule_count: usize },
    /// Static balanced mapping requires at least one capsule per owner.
    CapsuleCountLessThanOwnerCount {
        owner_count: usize,
        capsule_count: usize,
    },
}

impl std::fmt::Display for TopologyConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::OwnerCountZero => write!(f, "owner_count must be greater than zero"),
            Self::CapsuleCountZero => write!(f, "capsule_count must be greater than zero"),
            Self::CapsuleCountNotPowerOfTwo { capsule_count } => {
                write!(
                    f,
                    "capsule_count must be a power of two, got {capsule_count}"
                )
            }
            Self::OwnerCountTooLarge { owner_count } => {
                write!(f, "owner_count is too large for OwnerId: {owner_count}")
            }
            Self::CapsuleCountTooLarge { capsule_count } => {
                write!(
                    f,
                    "capsule_count is too large for KeyCapsuleId: {capsule_count}"
                )
            }
            Self::CapsuleCountLessThanOwnerCount {
                owner_count,
                capsule_count,
            } => write!(
                f,
                "capsule_count ({capsule_count}) must be >= owner_count ({owner_count})"
            ),
        }
    }
}

impl std::error::Error for TopologyConfigError {}

fn validate_capsule_count(count: usize) -> Result<(), TopologyConfigError> {
    if count == 0 {
        return Err(TopologyConfigError::CapsuleCountZero);
    }

    if count.is_power_of_two() {
        return Ok(());
    }

    Err(TopologyConfigError::CapsuleCountNotPowerOfTwo {
        capsule_count: count,
    })
}

/// Result of routing one key at a specific topology epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyRoute {
    hash: RouteHash,
    capsule: KeyCapsuleId,
    owner: OwnerId,
    epoch: TopologyEpoch,
}

impl KeyRoute {
    /// Routing hash used to select the capsule.
    #[inline]
    pub const fn hash(self) -> RouteHash {
        self.hash
    }

    /// Capsule selected by the routing hash.
    #[inline]
    pub const fn capsule(self) -> KeyCapsuleId {
        self.capsule
    }

    /// Owner responsible for the capsule.
    #[inline]
    pub const fn owner(self) -> OwnerId {
        self.owner
    }

    /// Topology epoch under which the route was computed.
    #[inline]
    pub const fn epoch(self) -> TopologyEpoch {
        self.epoch
    }

    /// Returns whether this route is local to `owner`.
    #[inline]
    pub const fn is_local_to(self, owner: OwnerId) -> bool {
        self.owner.0 == owner.0
    }
}

/// Snapshot of explicit routing debug counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RoutingDebugSnapshot {
    pub route_lookups: u64,
    pub local_routes: u64,
    pub remote_routes: u64,
    pub stale_epoch_reroutes: u64,
}

#[derive(Debug, Default)]
struct RoutingDebugCounters {
    route_lookups: CachePadded<AtomicU64>,
    local_routes: CachePadded<AtomicU64>,
    remote_routes: CachePadded<AtomicU64>,
    stale_epoch_reroutes: CachePadded<AtomicU64>,
}

impl RoutingDebugCounters {
    #[inline]
    fn record_route(&self, ingress_owner: OwnerId, route: KeyRoute) {
        self.route_lookups.fetch_add(1, Ordering::Relaxed);
        if route.is_local_to(ingress_owner) {
            self.local_routes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.remote_routes.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn record_stale_epoch_reroute(&self) {
        self.stale_epoch_reroutes.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn snapshot(&self) -> RoutingDebugSnapshot {
        RoutingDebugSnapshot {
            route_lookups: self.route_lookups.load(Ordering::Relaxed),
            local_routes: self.local_routes.load(Ordering::Relaxed),
            remote_routes: self.remote_routes.load(Ordering::Relaxed),
            stale_epoch_reroutes: self.stale_epoch_reroutes.load(Ordering::Relaxed),
        }
    }

    #[inline]
    fn reset(&self) {
        self.route_lookups.store(0, Ordering::Relaxed);
        self.local_routes.store(0, Ordering::Relaxed);
        self.remote_routes.store(0, Ordering::Relaxed);
        self.stale_epoch_reroutes.store(0, Ordering::Relaxed);
    }
}

/// Static owner routing topology.
#[derive(Debug)]
pub struct OwnerTopology {
    config: TopologyConfig,
    hasher: RandomState,
    capsule_to_owner: Box<[OwnerId]>,
    debug_counters: RoutingDebugCounters,
}

impl OwnerTopology {
    /// Builds a static balanced capsule-to-owner map.
    pub fn new(config: TopologyConfig) -> Self {
        let capsule_to_owner = (0..config.capsule_count())
            .map(|capsule| OwnerId::from_validated_index(owner_index_for_capsule(capsule, config)))
            .collect();

        Self {
            config,
            hasher: RandomState::with_seeds(
                ROUTING_AHASH_SEED_0,
                ROUTING_AHASH_SEED_1,
                ROUTING_AHASH_SEED_2,
                ROUTING_AHASH_SEED_3,
            ),
            capsule_to_owner,
            debug_counters: RoutingDebugCounters::default(),
        }
    }

    /// Returns the validated topology config.
    #[inline]
    pub const fn config(&self) -> TopologyConfig {
        self.config
    }

    /// Returns an owner ID for `index` if it is valid for this topology.
    #[inline]
    pub fn owner_id(&self, index: usize) -> Option<OwnerId> {
        (index < self.config.owner_count()).then(|| OwnerId::from_validated_index(index))
    }

    /// Returns a capsule ID for `index` if it is valid for this topology.
    #[inline]
    pub fn capsule_id(&self, index: usize) -> Option<KeyCapsuleId> {
        (index < self.config.capsule_count()).then(|| KeyCapsuleId::from_validated_index(index))
    }

    /// Computes the fixed-seed route hash for `key`.
    #[inline(always)]
    pub fn route_hash(&self, key: &[u8]) -> RouteHash {
        RouteHash(self.hasher.hash_one(key))
    }

    /// Routes a key without updating debug counters.
    #[inline(always)]
    pub fn route_key(&self, key: &[u8]) -> KeyRoute {
        self.route_hash_to_owner(self.route_hash(key))
    }

    /// Routes a key and records whether it was local to `ingress_owner`.
    #[inline]
    pub fn route_key_with_debug(&self, ingress_owner: OwnerId, key: &[u8]) -> KeyRoute {
        let route = self.route_key(key);
        self.debug_counters.record_route(ingress_owner, route);
        route
    }

    /// Records a stale-epoch reroute for future migration work.
    #[inline]
    pub fn record_stale_epoch_reroute(&self) {
        self.debug_counters.record_stale_epoch_reroute();
    }

    /// Returns a snapshot of routing debug counters.
    #[inline]
    pub fn routing_debug_counters(&self) -> RoutingDebugSnapshot {
        self.debug_counters.snapshot()
    }

    /// Resets routing debug counters.
    #[inline]
    pub fn reset_routing_debug_counters(&self) {
        self.debug_counters.reset();
    }

    /// Returns the owner currently assigned to `capsule`.
    #[inline]
    pub fn owner_for_capsule(&self, capsule: KeyCapsuleId) -> OwnerId {
        debug_assert!(capsule.get() < self.capsule_to_owner.len());
        self.capsule_to_owner[capsule.get()]
    }

    /// Routes a precomputed hash without updating debug counters.
    #[inline(always)]
    pub fn route_hash_to_owner(&self, hash: RouteHash) -> KeyRoute {
        let capsule = self.capsule_for_hash(hash);
        let owner = self.owner_for_capsule(capsule);
        KeyRoute {
            hash,
            capsule,
            owner,
            epoch: self.config.epoch(),
        }
    }

    #[inline(always)]
    fn capsule_for_hash(&self, hash: RouteHash) -> KeyCapsuleId {
        let bits = self.config.capsule_bits();
        let index = if bits == 0 {
            0
        } else {
            (hash.get() >> (u64::BITS - bits)) as usize
        };
        debug_assert!(index < self.config.capsule_count());
        KeyCapsuleId::from_validated_index(index)
    }
}

#[inline]
fn owner_index_for_capsule(capsule: usize, config: TopologyConfig) -> usize {
    ((capsule as u128 * config.owner_count() as u128) / config.capsule_count() as u128) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology() -> OwnerTopology {
        OwnerTopology::new(TopologyConfig::new(4, 64).expect("valid topology"))
    }

    #[test]
    fn routing_config_rejects_zero_owner_count() {
        assert_eq!(
            TopologyConfig::new(0, 64),
            Err(TopologyConfigError::OwnerCountZero)
        );
    }

    #[test]
    fn routing_config_accepts_non_power_of_two_owner_count() {
        let config = TopologyConfig::new(3, 64).expect("valid topology");

        assert_eq!(config.owner_count(), 3);
        assert_eq!(config.capsule_count(), 64);
    }

    #[test]
    fn routing_config_rejects_non_power_of_two_capsule_count() {
        assert_eq!(
            TopologyConfig::new(4, 96),
            Err(TopologyConfigError::CapsuleCountNotPowerOfTwo { capsule_count: 96 })
        );
    }

    #[test]
    fn routing_config_rejects_fewer_capsules_than_owners() {
        assert_eq!(
            TopologyConfig::new(64, 32),
            Err(TopologyConfigError::CapsuleCountLessThanOwnerCount {
                owner_count: 64,
                capsule_count: 32,
            })
        );
    }

    #[test]
    fn routing_static_map_assigns_balanced_contiguous_capsule_ranges() {
        let topology = topology();

        for owner_index in 0..4 {
            let owner = topology.owner_id(owner_index).expect("owner exists");
            let start = owner_index * 16;
            let end = start + 16;
            for capsule_index in start..end {
                let capsule = topology.capsule_id(capsule_index).expect("capsule exists");
                assert_eq!(topology.owner_for_capsule(capsule), owner);
            }
        }
    }

    #[test]
    fn routing_static_map_balances_non_power_of_two_owner_count() {
        let config = TopologyConfig::new(3, 64).expect("valid topology");
        let topology = OwnerTopology::new(config);
        let mut counts = [0usize; 3];

        for capsule_index in 0..config.capsule_count() {
            let capsule = topology.capsule_id(capsule_index).expect("capsule exists");
            let owner = topology.owner_for_capsule(capsule);
            assert!(owner.get() < config.owner_count());
            counts[owner.get()] += 1;
        }

        assert_eq!(counts, [22, 21, 21]);
    }

    #[test]
    fn routing_one_owner_one_capsule_is_valid() {
        let topology = OwnerTopology::new(TopologyConfig::new(1, 1).expect("valid topology"));
        let route = topology.route_key(b"single-capsule");

        assert_eq!(route.capsule().get(), 0);
        assert_eq!(route.owner().get(), 0);
    }

    #[test]
    fn routing_is_deterministic_across_topology_instances() {
        let left = topology();
        let right = topology();
        let keys: [&[u8]; 8] = [
            b"alpha",
            b"beta",
            b"gamma",
            b"delta",
            b"user:1001",
            b"user:1002",
            b"{account:42}:balance",
            b"{account:42}:ledger",
        ];

        for key in keys {
            assert_eq!(left.route_key(key), right.route_key(key));
        }
    }

    #[test]
    fn routing_uses_high_hash_bits_for_capsule_id() {
        let topology = topology();
        let hash = RouteHash(0xf123_4567_89ab_cdef);
        let route = topology.route_hash_to_owner(hash);

        assert_eq!(route.capsule().get(), 60);
        assert_eq!(route.owner().get(), 3);
    }

    #[test]
    fn routing_formula_matches_documented_equation() {
        let topology = topology();
        let hash = topology.route_hash(b"formula-key");
        let route = topology.route_hash_to_owner(hash);
        let capsule_bits = topology.config().capsule_count().trailing_zeros();
        let expected_capsule = if capsule_bits == 0 {
            0
        } else {
            (hash.get() >> (u64::BITS - capsule_bits)) as usize
        };
        let expected_owner = topology.owner_for_capsule(
            topology
                .capsule_id(expected_capsule)
                .expect("capsule from formula exists"),
        );

        assert_eq!(route.capsule().get(), expected_capsule);
        assert_eq!(route.owner(), expected_owner);
    }

    #[test]
    fn routing_owner_and_capsule_accessors_validate_bounds() {
        let topology = topology();

        assert_eq!(topology.owner_id(3).expect("owner exists").get(), 3);
        assert!(topology.owner_id(4).is_none());
        assert_eq!(topology.capsule_id(63).expect("capsule exists").get(), 63);
        assert!(topology.capsule_id(64).is_none());
    }

    #[test]
    fn routing_debug_counters_are_explicit_not_implicit() {
        let topology = topology();
        let owner_zero = topology.owner_id(0).expect("owner exists");

        let _ = topology.route_key(b"alpha");
        assert_eq!(
            topology.routing_debug_counters(),
            RoutingDebugSnapshot::default()
        );

        let local_key = (0u64..)
            .map(|index| format!("local:{index}"))
            .find(|key| topology.route_key(key.as_bytes()).is_local_to(owner_zero))
            .expect("local key found");
        let remote_key = (0u64..)
            .map(|index| format!("remote:{index}"))
            .find(|key| !topology.route_key(key.as_bytes()).is_local_to(owner_zero))
            .expect("remote key found");

        let _ = topology.route_key_with_debug(owner_zero, local_key.as_bytes());
        let _ = topology.route_key_with_debug(owner_zero, remote_key.as_bytes());
        topology.record_stale_epoch_reroute();

        assert_eq!(
            topology.routing_debug_counters(),
            RoutingDebugSnapshot {
                route_lookups: 2,
                local_routes: 1,
                remote_routes: 1,
                stale_epoch_reroutes: 1,
            }
        );
    }
}
