//! Multifactor priority in fixed-point integers. Integers give a true total
//! order (no NaN) and deterministic results across machines.

use crate::domain::{Job, JobId, PriorityClass, ResourceRequest, VirtualDuration, VirtualTime};

/// A priority score. Higher runs sooner.
pub type Score = u64;

/// Each factor is scaled into `0..=FACTOR_SCALE` before weighting.
pub const FACTOR_SCALE: u64 = 1_000_000;

/// Relative weight of each factor.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PriorityWeights {
    pub age: u32,
    pub qos: u32,
    /// Weight of the account's fair-share factor.
    pub fairshare: u32,
    pub size: u32,
}

impl Default for PriorityWeights {
    fn default() -> Self {
        Self {
            age: 1_000,
            qos: 1_000,
            fairshare: 1_000,
            size: 100,
        }
    }
}

/// Everything scoring needs beyond the job itself.
#[derive(Clone, Copy, Debug)]
pub struct PriorityConfig {
    pub weights: PriorityWeights,
    /// Age at which the age factor stops growing.
    pub age_horizon: VirtualDuration,
    /// Reference request the size factor is measured against.
    pub largest_request: ResourceRequest,
}

impl PriorityConfig {
    /// Bundles the three inputs.
    #[must_use]
    pub const fn new(
        weights: PriorityWeights,
        age_horizon: VirtualDuration,
        largest_request: ResourceRequest,
    ) -> Self {
        Self {
            weights,
            age_horizon,
            largest_request,
        }
    }

    /// Smallest age step that can change the age factor.
    #[must_use]
    pub const fn age_quantum(&self) -> VirtualDuration {
        VirtualDuration::from_nanos(self.age_horizon.as_nanos() / FACTOR_SCALE)
    }
}

/// Scales `value` into `0..=FACTOR_SCALE` relative to `max`, clamping above.
/// A `max` of 0 disables the factor.
const fn factor(value: u64, max: u64) -> u64 {
    if max == 0 {
        return 0;
    }
    // Clamp before multiplying so the product cannot overflow.
    let clamped = if value > max { max } else { value };
    (clamped * FACTOR_SCALE) / max
}

/// The job's priority at `now`. `fairshare_factor` is the account's value from
/// `FairShare::factor`, already in `0..=FACTOR_SCALE`.
#[must_use]
pub fn score(job: &Job, now: VirtualTime, config: &PriorityConfig, fairshare_factor: u64) -> Score {
    let age = now.saturating_sub_time(job.submit_time).as_nanos();
    let age_factor = factor(age, config.age_horizon.as_nanos());

    // `u64::from` is a lossless widening conversion, unlike `as`.
    let qos_factor = factor(
        u64::from(job.priority_class.ordinal()),
        (PriorityClass::COUNT - 1) as u64,
    );

    // Clamp so a caller's bad value cannot break the overflow bound below.
    let fairshare_factor = fairshare_factor.min(FACTOR_SCALE);

    let size_factor = factor(
        u64::from(job.request.cpu_millis),
        u64::from(config.largest_request.cpu_millis),
    );

    // Weighted sum; each term is at most FACTOR_SCALE * u32::MAX, well inside u64.
    age_factor * u64::from(config.weights.age)
        + qos_factor * u64::from(config.weights.qos)
        + fairshare_factor * u64::from(config.weights.fairshare)
        + size_factor * u64::from(config.weights.size)
}

/// The total order the ready set sorts by: higher score first, then lower
/// `JobId` (older) first. Deriving `Ord` on the fields makes it total.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct OrderKey {
    score: Score,
    // `Reverse` flips the comparison so a smaller id sorts as "greater".
    tiebreak: core::cmp::Reverse<u64>,
}

impl OrderKey {
    /// Builds the key for a job at a score.
    #[must_use]
    pub const fn new(score: Score, id: JobId) -> Self {
        Self {
            score,
            tiebreak: core::cmp::Reverse(id.to_bits()),
        }
    }

    /// The score component.
    #[must_use]
    pub const fn score(self) -> Score {
        self.score
    }

    /// The job component.
    #[must_use]
    pub const fn job(self) -> JobId {
        // `.0` reaches into the `Reverse` tuple struct.
        JobId::from_bits(self.tiebreak.0)
    }
}
