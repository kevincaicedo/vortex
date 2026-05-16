//! Deterministic connection-locality simulation for the shared-nothing branch.
//!
//! This module is not part of the hot command path. It models how connection
//! placement and key locality affect the percentage of commands that execute on
//! their ingress owner before `vortex-io` is wired into the experiment.

use super::{KeyCapsuleId, OwnerId, OwnerTopology, TopologyConfig, TopologyConfigError};

/// Connection-to-owner steering policy used by the locality simulator.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionSteering {
    /// Assign each connection to a deterministic pseudo-random owner,
    /// independent of command keys.
    Random,
    /// Assign each connection to `connection_index % owner_count`.
    RoundRobin,
    /// Ideal upper bound: route each command through the owner that owns its
    /// target capsule.
    OwnerAware,
    /// Assign each connection to the owner of a stable home capsule.
    ///
    /// This only improves locality when the workload itself has per-connection
    /// capsule affinity.
    StickyCapsule,
}

/// Key selection model used by the locality simulator.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum KeyWorkload {
    /// Uniform global key selection.
    Uniform,
    /// Zipfian global key selection over key rank.
    Zipf { theta: f64 },
    /// One hot key receives `hot_permille` of operations; the rest are uniform.
    HotKey {
        hot_key_index: usize,
        hot_permille: u16,
    },
    /// `capsule_local_permille` operations choose keys from the connection's
    /// home capsule; the rest choose uniform global keys.
    CapsuleLocalUniform { capsule_local_permille: u16 },
}

/// Simulation input.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LocalitySimulationConfig {
    /// Owner reactor count.
    pub owner_count: usize,
    /// Key capsule count. Must be a power of two.
    pub capsule_count: usize,
    /// Synthetic key count.
    pub key_count: usize,
    /// Number of simulated commands.
    pub operation_count: usize,
    /// Number of simulated client connections.
    pub connection_count: usize,
    /// Commands per selected connection turn.
    pub pipeline_width: usize,
    /// Connection steering policy.
    pub steering: ConnectionSteering,
    /// Key distribution.
    pub workload: KeyWorkload,
    /// Deterministic seed.
    pub seed: u64,
}

/// Simulation output.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub struct LocalitySimulationResult {
    /// Simulated command count.
    pub operations: u64,
    /// Commands where ingress owner equals target owner.
    pub local_ops: u64,
    /// Commands where ingress owner differs from target owner.
    pub remote_ops: u64,
    /// Local command ratio in `[0.0, 1.0]`.
    pub local_ratio: f64,
    /// Remote command ratio in `[0.0, 1.0]`.
    pub remote_ratio: f64,
    /// Commands targeting each owner.
    pub target_owner_load: Box<[u64]>,
    /// Commands entering through each owner.
    pub ingress_owner_load: Box<[u64]>,
    /// Max target-owner load divided by ideal average load.
    pub target_owner_saturation: f64,
    /// Max ingress-owner load divided by ideal average load.
    pub ingress_owner_saturation: f64,
}

/// Simulation validation error.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LocalitySimulationError {
    /// Topology validation failed.
    Topology(TopologyConfigError),
    /// Key count must be positive.
    KeyCountZero,
    /// Operation count must be positive.
    OperationCountZero,
    /// Connection count must be positive.
    ConnectionCountZero,
    /// Pipeline width must be positive.
    PipelineWidthZero,
    /// A permille input exceeded 1000.
    PermilleTooLarge { field: &'static str, value: u16 },
    /// Zipf theta must be finite and positive.
    InvalidZipfTheta,
    /// Hot-key index was outside the configured key range.
    HotKeyOutOfRange {
        hot_key_index: usize,
        key_count: usize,
    },
}

impl From<TopologyConfigError> for LocalitySimulationError {
    fn from(error: TopologyConfigError) -> Self {
        Self::Topology(error)
    }
}

#[derive(Clone, Copy)]
struct SimKey {
    capsule: KeyCapsuleId,
    owner: OwnerId,
}

/// Runs a deterministic connection-locality simulation.
#[doc(hidden)]
pub fn simulate_connection_locality(
    config: LocalitySimulationConfig,
) -> Result<LocalitySimulationResult, LocalitySimulationError> {
    validate_config(config)?;

    let topology = OwnerTopology::new(TopologyConfig::new(
        config.owner_count,
        config.capsule_count,
    )?);
    let keys = build_key_routes(&topology, config.key_count);
    let keys_by_capsule = build_keys_by_capsule(&keys, config.capsule_count);
    let random_connection_owners = random_connection_owners(&topology, config);
    let home_capsules = home_capsules(&topology, config);
    let sticky_connection_owners = sticky_connection_owners(&topology, &home_capsules);
    let zipf_cdf = zipf_cdf(config);
    let mut rng = SplitMix64::new(config.seed ^ 0x9d6c_8f1d_58a7_59b1);
    let mut local_ops = 0u64;
    let mut remote_ops = 0u64;
    let mut target_owner_load = vec![0u64; config.owner_count];
    let mut ingress_owner_load = vec![0u64; config.owner_count];

    for operation in 0..config.operation_count {
        let connection = (operation / config.pipeline_width) % config.connection_count;
        let key_index = choose_key_index(
            config,
            &keys_by_capsule,
            home_capsules[connection],
            zipf_cdf.as_deref(),
            &mut rng,
        );
        let key = keys[key_index];
        let ingress_owner = ingress_owner_for_command(
            config,
            connection,
            key.owner,
            &random_connection_owners,
            &sticky_connection_owners,
        );

        target_owner_load[key.owner.get()] += 1;
        ingress_owner_load[ingress_owner.get()] += 1;
        if ingress_owner == key.owner {
            local_ops += 1;
        } else {
            remote_ops += 1;
        }
    }

    let operations = config.operation_count as u64;
    Ok(LocalitySimulationResult {
        operations,
        local_ops,
        remote_ops,
        local_ratio: local_ops as f64 / operations as f64,
        remote_ratio: remote_ops as f64 / operations as f64,
        target_owner_saturation: owner_saturation(&target_owner_load),
        ingress_owner_saturation: owner_saturation(&ingress_owner_load),
        target_owner_load: target_owner_load.into_boxed_slice(),
        ingress_owner_load: ingress_owner_load.into_boxed_slice(),
    })
}

fn validate_config(config: LocalitySimulationConfig) -> Result<(), LocalitySimulationError> {
    if config.key_count == 0 {
        return Err(LocalitySimulationError::KeyCountZero);
    }
    if config.operation_count == 0 {
        return Err(LocalitySimulationError::OperationCountZero);
    }
    if config.connection_count == 0 {
        return Err(LocalitySimulationError::ConnectionCountZero);
    }
    if config.pipeline_width == 0 {
        return Err(LocalitySimulationError::PipelineWidthZero);
    }

    match config.workload {
        KeyWorkload::Uniform => {}
        KeyWorkload::Zipf { theta } if theta.is_finite() && theta > 0.0 => {}
        KeyWorkload::Zipf { .. } => return Err(LocalitySimulationError::InvalidZipfTheta),
        KeyWorkload::HotKey {
            hot_key_index,
            hot_permille,
        } => {
            validate_permille("hot_permille", hot_permille)?;
            if hot_key_index >= config.key_count {
                return Err(LocalitySimulationError::HotKeyOutOfRange {
                    hot_key_index,
                    key_count: config.key_count,
                });
            }
        }
        KeyWorkload::CapsuleLocalUniform {
            capsule_local_permille,
        } => validate_permille("capsule_local_permille", capsule_local_permille)?,
    }

    Ok(())
}

fn validate_permille(field: &'static str, value: u16) -> Result<(), LocalitySimulationError> {
    if value <= 1000 {
        return Ok(());
    }
    Err(LocalitySimulationError::PermilleTooLarge { field, value })
}

fn build_key_routes(topology: &OwnerTopology, key_count: usize) -> Box<[SimKey]> {
    (0..key_count)
        .map(|index| {
            let key = format!("sn004b-key:{index:08}");
            let route = topology.route_key(key.as_bytes());
            SimKey {
                capsule: route.capsule(),
                owner: route.owner(),
            }
        })
        .collect()
}

fn build_keys_by_capsule(keys: &[SimKey], capsule_count: usize) -> Vec<Vec<usize>> {
    let mut keys_by_capsule = vec![Vec::new(); capsule_count];
    for (index, key) in keys.iter().enumerate() {
        keys_by_capsule[key.capsule.get()].push(index);
    }
    keys_by_capsule
}

fn random_connection_owners(
    topology: &OwnerTopology,
    config: LocalitySimulationConfig,
) -> Box<[OwnerId]> {
    (0..config.connection_count)
        .map(|connection| {
            let index = deterministic_index(
                config.seed ^ 0x49db_282a_8f10_f4d7,
                connection as u64,
                config.owner_count,
            );
            topology.owner_id(index).expect("owner index is valid")
        })
        .collect()
}

fn home_capsules(
    topology: &OwnerTopology,
    config: LocalitySimulationConfig,
) -> Box<[KeyCapsuleId]> {
    (0..config.connection_count)
        .map(|connection| {
            let index = deterministic_index(
                config.seed ^ 0x1f59_3e73_5c41_a219,
                connection as u64,
                config.capsule_count,
            );
            topology.capsule_id(index).expect("capsule index is valid")
        })
        .collect()
}

fn sticky_connection_owners(
    topology: &OwnerTopology,
    home_capsules: &[KeyCapsuleId],
) -> Box<[OwnerId]> {
    home_capsules
        .iter()
        .map(|capsule| topology.owner_for_capsule(*capsule))
        .collect()
}

#[inline]
fn deterministic_index(seed: u64, value: u64, upper: usize) -> usize {
    let mut state = seed.wrapping_add(value.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let mixed = splitmix64_next(&mut state);
    bounded_index(mixed, upper)
}

fn zipf_cdf(config: LocalitySimulationConfig) -> Option<Box<[f64]>> {
    let KeyWorkload::Zipf { theta } = config.workload else {
        return None;
    };

    let mut cumulative = 0.0;
    let mut cdf = Vec::with_capacity(config.key_count);
    for rank in 1..=config.key_count {
        cumulative += 1.0 / (rank as f64).powf(theta);
        cdf.push(cumulative);
    }
    for value in &mut cdf {
        *value /= cumulative;
    }
    if let Some(last) = cdf.last_mut() {
        *last = 1.0;
    }
    Some(cdf.into_boxed_slice())
}

fn choose_key_index(
    config: LocalitySimulationConfig,
    keys_by_capsule: &[Vec<usize>],
    home_capsule: KeyCapsuleId,
    zipf_cdf: Option<&[f64]>,
    rng: &mut SplitMix64,
) -> usize {
    if let KeyWorkload::CapsuleLocalUniform {
        capsule_local_permille,
    } = config.workload
    {
        if rng.next_bounded(1000) < capsule_local_permille as usize {
            let home_keys = &keys_by_capsule[home_capsule.get()];
            if !home_keys.is_empty() {
                return home_keys[rng.next_bounded(home_keys.len())];
            }
        }
        return rng.next_bounded(config.key_count);
    }

    match config.workload {
        KeyWorkload::Uniform => rng.next_bounded(config.key_count),
        KeyWorkload::Zipf { .. } => sample_zipf(zipf_cdf.expect("zipf cdf exists"), rng),
        KeyWorkload::HotKey {
            hot_key_index,
            hot_permille,
        } => {
            if rng.next_bounded(1000) < hot_permille as usize {
                hot_key_index
            } else {
                rng.next_bounded(config.key_count)
            }
        }
        KeyWorkload::CapsuleLocalUniform { .. } => unreachable!("handled above"),
    }
}

fn sample_zipf(cdf: &[f64], rng: &mut SplitMix64) -> usize {
    let sample = rng.next_unit_f64();
    cdf.partition_point(|value| *value < sample)
}

#[inline]
fn ingress_owner_for_command(
    config: LocalitySimulationConfig,
    connection: usize,
    target_owner: OwnerId,
    random_connection_owners: &[OwnerId],
    sticky_connection_owners: &[OwnerId],
) -> OwnerId {
    match config.steering {
        ConnectionSteering::Random => random_connection_owners[connection],
        ConnectionSteering::RoundRobin => {
            OwnerId::from_validated_index(connection % config.owner_count)
        }
        ConnectionSteering::OwnerAware => target_owner,
        ConnectionSteering::StickyCapsule => sticky_connection_owners[connection],
    }
}

fn owner_saturation(loads: &[u64]) -> f64 {
    let total = loads.iter().sum::<u64>();
    if total == 0 || loads.is_empty() {
        return 0.0;
    }
    let ideal = total as f64 / loads.len() as f64;
    let max = loads.iter().copied().max().unwrap_or(0) as f64;
    max / ideal
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    #[inline]
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        splitmix64_next(&mut self.state)
    }

    #[inline]
    fn next_bounded(&mut self, upper: usize) -> usize {
        debug_assert!(upper > 0);
        bounded_index(self.next_u64(), upper)
    }

    #[inline]
    fn next_unit_f64(&mut self) -> f64 {
        let value = self.next_u64() >> 11;
        (value as f64) * (1.0 / ((1u64 << 53) as f64))
    }
}

#[inline]
fn splitmix64_next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[inline]
fn bounded_index(value: u64, upper: usize) -> usize {
    ((value as u128 * upper as u128) >> 64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> LocalitySimulationConfig {
        LocalitySimulationConfig {
            owner_count: 8,
            capsule_count: 64,
            key_count: 4096,
            operation_count: 100_000,
            connection_count: 256,
            pipeline_width: 1,
            steering: ConnectionSteering::Random,
            workload: KeyWorkload::Uniform,
            seed: 0x5eed,
        }
    }

    #[test]
    fn owner_aware_steering_is_all_local() {
        let result = simulate_connection_locality(LocalitySimulationConfig {
            steering: ConnectionSteering::OwnerAware,
            ..base_config()
        })
        .expect("simulation succeeds");

        assert_eq!(result.local_ops, result.operations);
        assert_eq!(result.remote_ops, 0);
    }

    #[test]
    fn random_uniform_approaches_inverse_owner_count() {
        let result = simulate_connection_locality(base_config()).expect("simulation succeeds");

        assert!(result.local_ratio > 0.10, "{}", result.local_ratio);
        assert!(result.local_ratio < 0.15, "{}", result.local_ratio);
    }

    #[test]
    fn sticky_capsule_locality_tracks_requested_locality() {
        let result = simulate_connection_locality(LocalitySimulationConfig {
            steering: ConnectionSteering::StickyCapsule,
            workload: KeyWorkload::CapsuleLocalUniform {
                capsule_local_permille: 950,
            },
            ..base_config()
        })
        .expect("simulation succeeds");

        assert!(result.local_ratio > 0.93, "{}", result.local_ratio);
    }

    #[test]
    fn hot_key_workload_exposes_target_owner_saturation() {
        let result = simulate_connection_locality(LocalitySimulationConfig {
            steering: ConnectionSteering::OwnerAware,
            workload: KeyWorkload::HotKey {
                hot_key_index: 0,
                hot_permille: 900,
            },
            ..base_config()
        })
        .expect("simulation succeeds");

        assert!(
            result.target_owner_saturation > 6.0,
            "{}",
            result.target_owner_saturation
        );
    }
}
