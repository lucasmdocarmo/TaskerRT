//! The virtual clock. Nothing else in a simulation may advance time.

use tasker_core::{VirtualDuration, VirtualTime};

/// Monotonic virtual clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct VirtualClock {
    now: VirtualTime,
}

impl VirtualClock {
    /// A clock at the epoch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            now: VirtualTime::ZERO,
        }
    }

    /// The current instant.
    #[must_use]
    pub const fn now(&self) -> VirtualTime {
        self.now
    }

    /// Jumps to `t`. Panics if `t` is in the past: time never runs backwards.
    pub fn advance_to(&mut self, t: VirtualTime) {
        assert!(t >= self.now, "virtual clock moved backwards");
        self.now = t;
    }

    /// Advances by `d`.
    pub fn advance_by(&mut self, d: VirtualDuration) {
        self.now = self.now.saturating_add(d);
    }
}
