//! Virtual time.
//!
//! The scheduling core never reads the wall clock; `clippy.toml` bans
//! `Instant::now` and `SystemTime::now` outright. Time is supplied by the
//! caller, which is what makes the M2 simulation harness exactly replayable.

/// A point on the virtual clock, in nanoseconds since the scheduler epoch.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct VirtualTime(u64);

/// A span of virtual time, in nanoseconds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct VirtualDuration(u64);

impl VirtualTime {
    /// The scheduler epoch.
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Advances by `delta`, saturating at `u64::MAX` rather than wrapping.
    #[must_use]
    pub const fn saturating_add(self, delta: VirtualDuration) -> Self {
        Self(self.0.saturating_add(delta.0))
    }

    /// Elapsed time since `earlier`, or [`VirtualDuration::ZERO`] if `earlier`
    /// is in fact later. Saturating rather than panicking keeps age arithmetic
    /// total across clock resets in the simulation harness.
    #[must_use]
    pub const fn saturating_sub_time(self, earlier: Self) -> VirtualDuration {
        VirtualDuration(self.0.saturating_sub(earlier.0))
    }
}

impl VirtualDuration {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }

    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_duration_advances_time() {
        let t = VirtualTime::from_nanos(100).saturating_add(VirtualDuration::from_nanos(50));
        assert_eq!(t.as_nanos(), 150);
    }

    #[test]
    fn subtracting_a_later_time_saturates_to_zero() {
        let earlier = VirtualTime::from_nanos(10);
        let later = VirtualTime::from_nanos(90);
        assert_eq!(earlier.saturating_sub_time(later), VirtualDuration::ZERO);
        assert_eq!(later.saturating_sub_time(earlier).as_nanos(), 80);
    }

    #[test]
    fn add_saturates_at_max() {
        let t = VirtualTime::from_nanos(u64::MAX).saturating_add(VirtualDuration::from_nanos(1));
        assert_eq!(t.as_nanos(), u64::MAX);
    }

    #[test]
    fn from_secs_converts() {
        assert_eq!(VirtualDuration::from_secs(2).as_nanos(), 2_000_000_000);
    }
}
