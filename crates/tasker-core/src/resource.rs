//! The resource vector shared by job requests and slot capacity.
//!
//! One type serves both roles: a request and a capacity are the same three
//! numbers, and giving them one implementation means `fits_within` cannot
//! disagree with itself.

/// CPU millicores, memory bytes, and whole GPUs.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Resources {
    pub cpu_millis: u32,
    pub mem_bytes: u64,
    pub gpus: u8,
}

/// What a job asks for (spec §5.1).
pub type ResourceRequest = Resources;
/// What a slot offers (spec §5.3).
pub type Capacity = Resources;

impl Resources {
    /// The additive identity.
    pub const ZERO: Self = Self {
        cpu_millis: 0,
        mem_bytes: 0,
        gpus: 0,
    };

    #[must_use]
    pub const fn new(cpu_millis: u32, mem_bytes: u64, gpus: u8) -> Self {
        Self {
            cpu_millis,
            mem_bytes,
            gpus,
        }
    }

    /// True when every dimension is within `capacity`.
    ///
    /// Resource fit is conjunctive: being under on CPU does not buy headroom on
    /// memory.
    #[must_use]
    pub const fn fits_within(&self, capacity: &Self) -> bool {
        self.cpu_millis <= capacity.cpu_millis
            && self.mem_bytes <= capacity.mem_bytes
            && self.gpus <= capacity.gpus
    }

    /// Componentwise addition, or `None` if any dimension overflows.
    #[must_use]
    pub const fn checked_add(&self, other: &Self) -> Option<Self> {
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

    /// Componentwise addition, saturating at each type's maximum.
    #[must_use]
    pub const fn saturating_add(&self, other: &Self) -> Self {
        Self {
            cpu_millis: self.cpu_millis.saturating_add(other.cpu_millis),
            mem_bytes: self.mem_bytes.saturating_add(other.mem_bytes),
            gpus: self.gpus.saturating_add(other.gpus),
        }
    }

    /// Componentwise subtraction, flooring at zero.
    #[must_use]
    pub const fn saturating_sub(&self, other: &Self) -> Self {
        Self {
            cpu_millis: self.cpu_millis.saturating_sub(other.cpu_millis),
            mem_bytes: self.mem_bytes.saturating_sub(other.mem_bytes),
            gpus: self.gpus.saturating_sub(other.gpus),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fits_within_requires_every_dimension() {
        let capacity = Resources::new(4_000, 8 << 30, 2);
        assert!(Resources::new(4_000, 8 << 30, 2).fits_within(&capacity));
        assert!(
            !Resources::new(4_001, 0, 0).fits_within(&capacity),
            "cpu over"
        );
        assert!(
            !Resources::new(0, (8 << 30) + 1, 0).fits_within(&capacity),
            "mem over"
        );
        assert!(!Resources::new(0, 0, 3).fits_within(&capacity), "gpu over");
    }

    #[test]
    fn zero_fits_within_anything() {
        assert!(Resources::ZERO.fits_within(&Resources::ZERO));
        assert!(Resources::ZERO.fits_within(&Resources::new(1, 1, 1)));
    }

    #[test]
    fn saturating_sub_floors_at_zero() {
        let got = Resources::new(100, 100, 1).saturating_sub(&Resources::new(500, 500, 5));
        assert_eq!(got, Resources::ZERO);
    }

    #[test]
    fn checked_add_detects_overflow() {
        let big = Resources::new(u32::MAX, u64::MAX, u8::MAX);
        assert_eq!(big.checked_add(&Resources::new(1, 0, 0)), None);
        assert_eq!(big.checked_add(&Resources::ZERO), Some(big));
    }

    #[test]
    fn add_then_sub_roundtrips() {
        let base = Resources::new(1_000, 1 << 30, 1);
        let delta = Resources::new(250, 1 << 28, 1);
        assert_eq!(base.saturating_add(&delta).saturating_sub(&delta), base);
    }
}
