//! Virtual time. The core never reads the wall clock; callers pass time in.

/// A point on the virtual clock, in nanoseconds since the scheduler epoch.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct VirtualTime(u64);

/// A span of virtual time, in nanoseconds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct VirtualDuration(u64);

impl VirtualTime {
    /// The epoch.
    pub const ZERO: Self = Self(0);

    /// Wraps a nanosecond count.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// The nanosecond count.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// `self + delta`, clamped at `u64::MAX` instead of wrapping.
    #[must_use]
    pub const fn saturating_add(self, delta: VirtualDuration) -> Self {
        // `saturating_add` stops at the maximum rather than overflowing to zero.
        Self(self.0.saturating_add(delta.0))
    }

    /// `self - earlier` as a duration; zero if `earlier` is actually later.
    #[must_use]
    pub const fn saturating_sub_time(self, earlier: Self) -> VirtualDuration {
        // `saturating_sub` floors at 0 instead of underflowing to `u64::MAX`.
        VirtualDuration(self.0.saturating_sub(earlier.0))
    }
}

impl VirtualDuration {
    /// No time at all.
    pub const ZERO: Self = Self(0);

    /// Wraps a nanosecond count.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Converts whole seconds, clamping on overflow.
    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        // `saturating_mul` clamps so a huge input cannot wrap to a tiny duration.
        Self(secs.saturating_mul(1_000_000_000))
    }

    /// The nanosecond count.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }
}
