#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MutationFeatures(usize);

impl MutationFeatures {
    pub(crate) const MAXMEMORY: Self = Self(1 << 0);
    pub(crate) const WATCH: Self = Self(1 << 1);
    pub(crate) const AOF: Self = Self(1 << 2);

    const ALL_BITS: usize = Self::MAXMEMORY.0 | Self::WATCH.0 | Self::AOF.0;

    #[inline]
    pub(crate) const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub(crate) const fn from_bits(bits: usize) -> Self {
        Self(bits & Self::ALL_BITS)
    }

    #[inline]
    pub(crate) const fn bits(self) -> usize {
        self.0
    }

    #[inline]
    pub(crate) const fn is_empty(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub(crate) const fn maxmemory(self) -> bool {
        self.contains(Self::MAXMEMORY)
    }

    #[inline]
    pub(crate) const fn watch(self) -> bool {
        self.contains(Self::WATCH)
    }

    #[inline]
    pub(crate) const fn aof(self) -> bool {
        self.contains(Self::AOF)
    }

    #[inline]
    pub(crate) const fn entry_lsn_observed(self) -> bool {
        self.watch() || self.aof()
    }

    #[inline]
    const fn contains(self, feature: Self) -> bool {
        self.0 & feature.0 != 0
    }
}
