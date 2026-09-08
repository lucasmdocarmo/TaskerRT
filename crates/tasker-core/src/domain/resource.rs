//! The resource vector shared by job requests and slot capacity.

/// CPU millicores, memory bytes, and whole GPUs. One type serves as both a
/// request and a capacity, so "fits" has exactly one definition.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Resources {
    pub cpu_millis: u32,
    pub mem_bytes: u64,
    pub gpus: u8,
}

/// What a job asks for.
pub type ResourceRequest = Resources;
/// What a slot offers.
pub type Capacity = Resources;

impl Resources {
    /// All zero.
    pub const ZERO: Self = Self {
        cpu_millis: 0,
        mem_bytes: 0,
        gpus: 0,
    };

    /// Builds a vector from its three dimensions.
    #[must_use]
    pub const fn new(cpu_millis: u32, mem_bytes: u64, gpus: u8) -> Self {
        Self {
            cpu_millis,
            mem_bytes,
            gpus,
        }
    }

    /// True when every dimension is `<=` the matching one in `capacity`.
    #[must_use]
    pub const fn fits_within(&self, capacity: &Self) -> bool {
        // `&&` short-circuits: later comparisons are skipped once one is false.
        self.cpu_millis <= capacity.cpu_millis
            && self.mem_bytes <= capacity.mem_bytes
            && self.gpus <= capacity.gpus
    }

    /// Componentwise sum, or `None` if any dimension would overflow.
    #[must_use]
    pub const fn checked_add(&self, other: &Self) -> Option<Self> {
        // `checked_add` yields `None` on overflow; `let ... else` bails out on it.
        let Some(cpu_millis) = self.cpu_millis.checked_add(other.cpu_millis) else {
            return None;
        };
        let Some(mem_bytes) = self.mem_bytes.checked_add(other.mem_bytes) else {
            return None;
        };
        let Some(gpus) = self.gpus.checked_add(other.gpus) else {
            return None;
        };
        Some(Self {
            cpu_millis,
            mem_bytes,
            gpus,
        })
    }

    /// Componentwise sum, clamped at each type's maximum.
    #[must_use]
    pub const fn saturating_add(&self, other: &Self) -> Self {
        Self {
            cpu_millis: self.cpu_millis.saturating_add(other.cpu_millis),
            mem_bytes: self.mem_bytes.saturating_add(other.mem_bytes),
            gpus: self.gpus.saturating_add(other.gpus),
        }
    }

    /// Componentwise difference, floored at zero.
    #[must_use]
    pub const fn saturating_sub(&self, other: &Self) -> Self {
        Self {
            cpu_millis: self.cpu_millis.saturating_sub(other.cpu_millis),
            mem_bytes: self.mem_bytes.saturating_sub(other.mem_bytes),
            gpus: self.gpus.saturating_sub(other.gpus),
        }
    }
}
