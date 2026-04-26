use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

pub const EVICTION_SWEEP_WINDOW: usize = 64;
pub const EVICTION_MAX_SHARDS_PER_ADMISSION: usize = 64;
pub const LFU_SKETCH_DEPTH: usize = 4;
pub const LFU_SKETCH_WIDTH: usize = 2048;
pub const LFU_READ_SAMPLE_MASK: u64 = 0x0F;

const LFU_SKETCH_MASK: usize = LFU_SKETCH_WIDTH - 1;
const LFU_SKETCH_DECAY_INTERVAL: u64 = 1 << 16;
const EVICTION_CONFIG_POLICY_SHIFT: u32 = 61;
const EVICTION_CONFIG_MAX_MEMORY_MASK: u64 = (1u64 << EVICTION_CONFIG_POLICY_SHIFT) - 1;
const EVICTION_RNG_SEED_STRIDE: u64 = 0x9E37_79B9_7F4A_7C15;
const LFU_SKETCH_SEEDS: [u64; LFU_SKETCH_DEPTH] = [
    0x9E37_79B9_7F4A_7C15,
    0xC2B2_AE3D_27D4_EB4F,
    0x1656_67B1_9E37_79F9,
    0x85EB_CA77_C2B2_AE63,
];

static EVICTION_RNG_SEED: AtomicU64 = AtomicU64::new(EVICTION_RNG_SEED_STRIDE);

thread_local! {
    static EVICTION_RNG_STATE: std::cell::Cell<u64> = std::cell::Cell::new(initial_rng_state());
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EvictionPolicy {
    #[default]
    NoEviction = 0,
    AllKeysLru = 1,
    VolatileLru = 2,
    AllKeysRandom = 3,
    VolatileRandom = 4,
    VolatileTtl = 5,
    AllKeysLfu = 6,
    VolatileLfu = 7,
}

impl EvictionPolicy {
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::VolatileLru => "volatile-lru",
            Self::AllKeysRandom => "allkeys-random",
            Self::VolatileRandom => "volatile-random",
            Self::VolatileTtl => "volatile-ttl",
            Self::AllKeysLfu => "allkeys-lfu",
            Self::VolatileLfu => "volatile-lfu",
        }
    }

    #[inline]
    pub const fn from_u8(raw: u8) -> Option<Self> {
        match raw {
            0 => Some(Self::NoEviction),
            1 => Some(Self::AllKeysLru),
            2 => Some(Self::VolatileLru),
            3 => Some(Self::AllKeysRandom),
            4 => Some(Self::VolatileRandom),
            5 => Some(Self::VolatileTtl),
            6 => Some(Self::AllKeysLfu),
            7 => Some(Self::VolatileLfu),
            _ => None,
        }
    }

    #[inline]
    pub fn parse_bytes(raw: &[u8]) -> Option<Self> {
        if raw.eq_ignore_ascii_case(b"noeviction") {
            return Some(Self::NoEviction);
        }
        if raw.eq_ignore_ascii_case(b"allkeys-lru") {
            return Some(Self::AllKeysLru);
        }
        if raw.eq_ignore_ascii_case(b"volatile-lru") {
            return Some(Self::VolatileLru);
        }
        if raw.eq_ignore_ascii_case(b"allkeys-random") {
            return Some(Self::AllKeysRandom);
        }
        if raw.eq_ignore_ascii_case(b"volatile-random") {
            return Some(Self::VolatileRandom);
        }
        if raw.eq_ignore_ascii_case(b"volatile-ttl") {
            return Some(Self::VolatileTtl);
        }
        if raw.eq_ignore_ascii_case(b"allkeys-lfu") {
            return Some(Self::AllKeysLfu);
        }
        if raw.eq_ignore_ascii_case(b"volatile-lfu") {
            return Some(Self::VolatileLfu);
        }
        None
    }

    #[inline]
    pub const fn is_volatile_only(self) -> bool {
        matches!(
            self,
            Self::VolatileLru | Self::VolatileRandom | Self::VolatileTtl | Self::VolatileLfu
        )
    }

    #[inline]
    pub const fn is_random(self) -> bool {
        matches!(self, Self::AllKeysRandom | Self::VolatileRandom)
    }

    #[inline]
    pub const fn is_lfu(self) -> bool {
        matches!(self, Self::AllKeysLfu | Self::VolatileLfu)
    }

    #[inline]
    pub const fn is_noeviction(self) -> bool {
        matches!(self, Self::NoEviction)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EvictionConfig {
    pub max_memory: usize,
    pub policy: EvictionPolicy,
}

#[derive(Debug, Default)]
pub struct EvictionConfigState {
    packed: AtomicU64,
}

#[derive(Debug)]
pub struct FrequencySketch {
    counters: Box<[AtomicU8]>,
    samples: AtomicU64,
    decay_in_progress: AtomicU8,
}

impl Default for FrequencySketch {
    fn default() -> Self {
        Self::new()
    }
}

impl FrequencySketch {
    pub fn new() -> Self {
        let counters = std::iter::repeat_with(|| AtomicU8::new(0))
            .take(LFU_SKETCH_DEPTH * LFU_SKETCH_WIDTH)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            counters,
            samples: AtomicU64::new(0),
            decay_in_progress: AtomicU8::new(0),
        }
    }

    #[inline]
    fn index(row: usize, hash: u64) -> usize {
        let mixed = hash
            .wrapping_add(LFU_SKETCH_SEEDS[row])
            .rotate_left(((row as u32) + 1) * 13)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15);
        row * LFU_SKETCH_WIDTH + ((mixed as usize) & LFU_SKETCH_MASK)
    }

    #[inline]
    pub fn record(&self, hash: u64) {
        let sample = self.samples.fetch_add(1, Ordering::Relaxed) + 1;
        if sample & (LFU_SKETCH_DECAY_INTERVAL - 1) == 0 {
            self.try_decay();
        }

        for row in 0..LFU_SKETCH_DEPTH {
            let counter = &self.counters[Self::index(row, hash)];
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                (value != u8::MAX).then_some(value + 1)
            });
        }
    }

    #[inline]
    pub fn estimate(&self, hash: u64) -> u8 {
        let mut minimum = u8::MAX;
        for row in 0..LFU_SKETCH_DEPTH {
            minimum = minimum.min(self.counters[Self::index(row, hash)].load(Ordering::Relaxed));
        }
        minimum
    }

    fn try_decay(&self) {
        if self
            .decay_in_progress
            .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        for counter in self.counters.iter() {
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                (value != 0).then_some(value >> 1)
            });
        }

        self.decay_in_progress.store(0, Ordering::Relaxed);
    }
}

#[inline]
pub const fn should_sample_lfu_read(random: u64) -> bool {
    random & LFU_READ_SAMPLE_MASK == 0
}

impl EvictionConfigState {
    #[inline]
    pub const fn new() -> Self {
        Self {
            packed: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn load(&self) -> EvictionConfig {
        Self::unpack(self.packed.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn store(&self, max_memory: usize, policy: EvictionPolicy) {
        self.packed
            .store(Self::pack(max_memory, policy), Ordering::Relaxed);
    }

    #[inline]
    pub fn set_max_memory(&self, max_memory: usize) {
        let max_memory = Self::normalize_max_memory(max_memory);
        let _ = self
            .packed
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some((current & !EVICTION_CONFIG_MAX_MEMORY_MASK) | max_memory)
            });
    }

    #[inline]
    pub fn set_policy(&self, policy: EvictionPolicy) {
        let policy_bits = (policy as u64) << EVICTION_CONFIG_POLICY_SHIFT;
        let _ = self
            .packed
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some((current & EVICTION_CONFIG_MAX_MEMORY_MASK) | policy_bits)
            });
    }

    #[inline]
    const fn unpack(raw: u64) -> EvictionConfig {
        let max_memory = (raw & EVICTION_CONFIG_MAX_MEMORY_MASK) as usize;
        let raw_policy = (raw >> EVICTION_CONFIG_POLICY_SHIFT) as u8;
        let policy = match EvictionPolicy::from_u8(raw_policy) {
            Some(policy) => policy,
            None => EvictionPolicy::NoEviction,
        };
        EvictionConfig { max_memory, policy }
    }

    #[inline]
    fn pack(max_memory: usize, policy: EvictionPolicy) -> u64 {
        ((policy as u64) << EVICTION_CONFIG_POLICY_SHIFT) | Self::normalize_max_memory(max_memory)
    }

    #[inline]
    fn normalize_max_memory(max_memory: usize) -> u64 {
        (max_memory as u64).min(EVICTION_CONFIG_MAX_MEMORY_MASK)
    }
}

#[inline]
fn initial_rng_state() -> u64 {
    let seed = EVICTION_RNG_SEED.fetch_add(EVICTION_RNG_SEED_STRIDE, Ordering::Relaxed);
    let mixed = splitmix64(seed);
    if mixed == 0 {
        EVICTION_RNG_SEED_STRIDE
    } else {
        mixed
    }
}

#[inline]
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(EVICTION_RNG_SEED_STRIDE);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[inline]
fn xorshift64(mut value: u64) -> u64 {
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    value
}

#[inline]
pub fn next_random_u64() -> u64 {
    EVICTION_RNG_STATE.with(|state| {
        let value = xorshift64(state.get());
        state.set(value);
        value
    })
}

#[cfg(test)]
mod tests {
    use super::{
        EvictionConfigState, EvictionPolicy, FrequencySketch, should_sample_lfu_read, xorshift64,
    };

    #[test]
    fn parses_known_policy_names() {
        assert_eq!(
            EvictionPolicy::parse_bytes(b"allkeys-lru"),
            Some(EvictionPolicy::AllKeysLru)
        );
        assert_eq!(
            EvictionPolicy::parse_bytes(b"Volatile-Random"),
            Some(EvictionPolicy::VolatileRandom)
        );
        assert_eq!(
            EvictionPolicy::parse_bytes(b"allkeys-lfu"),
            Some(EvictionPolicy::AllKeysLfu)
        );
        assert_eq!(EvictionPolicy::parse_bytes(b"bogus"), None);
    }

    #[test]
    fn state_round_trips() {
        let state = EvictionConfigState::new();
        state.store(1234, EvictionPolicy::VolatileTtl);

        let snapshot = state.load();
        assert_eq!(snapshot.max_memory, 1234);
        assert_eq!(snapshot.policy, EvictionPolicy::VolatileTtl);
    }

    #[test]
    fn state_updates_preserve_other_field() {
        let state = EvictionConfigState::new();
        state.store(1024, EvictionPolicy::AllKeysLru);
        state.set_policy(EvictionPolicy::VolatileLfu);
        state.set_max_memory(4096);

        let snapshot = state.load();
        assert_eq!(snapshot.max_memory, 4096);
        assert_eq!(snapshot.policy, EvictionPolicy::VolatileLfu);
    }

    #[test]
    fn xorshift64_step_matches_reference() {
        assert_eq!(xorshift64(0x1234_5678_9ABC_DEF1), 0xFE80_0D65_2978_3B0C);
    }

    #[test]
    fn frequency_sketch_prefers_hotter_key() {
        let sketch = FrequencySketch::new();
        let hot = 0x1234_5678_9ABC_DEF0;
        let cold = 0x0FED_CBA9_8765_4321;

        for _ in 0..32 {
            sketch.record(hot);
        }
        sketch.record(cold);

        assert!(sketch.estimate(hot) > sketch.estimate(cold));
    }

    #[test]
    fn lfu_read_sampling_uses_one_in_sixteen_mask() {
        let sampled: Vec<u64> = (0..16)
            .filter(|value| should_sample_lfu_read(*value))
            .collect();
        assert_eq!(sampled, vec![0]);
    }
}
