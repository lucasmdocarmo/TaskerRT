//! A tiny deterministic generator. Owned here, not taken from a crate, so a
//! `cargo update` can never silently reshape every recorded scenario.

/// SplitMix64: one 64-bit state, identical output on every platform.
#[derive(Clone, Debug)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    /// Seeds the generator.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        // `wrapping_*` is the whole point here: overflow is the mixing function.
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Roughly uniform in `[low, high)`. Modulo bias is irrelevant for fixtures.
    pub fn range(&mut self, low: u64, high: u64) -> u64 {
        debug_assert!(high > low);
        low + self.next_u64() % (high - low)
    }

    /// True with probability `permille / 1000`.
    pub fn chance(&mut self, permille: u32) -> bool {
        self.range(0, 1_000) < u64::from(permille)
    }
}
