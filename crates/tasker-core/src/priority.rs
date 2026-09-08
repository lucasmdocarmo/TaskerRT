//! Multifactor priority (spec §6 step 3).
//!
//! Fixed-point integers, not floats. Three reasons: `f64` is not `Ord` so it
//! cannot satisfy the total-order invariant in spec §9.1; integer arithmetic is
//! deterministic across platforms, which the M2 replay harness depends on; and
//! it avoids float latency in the hot path.
//!
//! Every factor is scaled to `0..=FACTOR_SCALE` and multiplied by a `u32`
//! weight. With four factors the maximum score is
//! `4 * FACTOR_SCALE * u32::MAX` ≈ 1.7e16, comfortably inside `u64`.

use crate::{Job, JobId, PriorityClass, ResourceRequest, VirtualDuration, VirtualTime};

/// A priority score. Higher runs sooner.
pub type Score = u64;

/// Factors are integers in `0..=FACTOR_SCALE`.
pub const FACTOR_SCALE: u64 = 1_000_000;

/// Relative weight of each factor. Configuration (spec §6 step 3).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PriorityWeights {
    pub age: u32,
    pub qos: u32,
    /// Contributes nothing until M5 adds fair-share accounting.
    pub fairshare: u32,
    pub size: u32,
}

impl Default for PriorityWeights {
    fn default() -> Self {
        Self {
            age: 1_000,
            qos: 1_000,
            fairshare: 0,
            size: 100,
        }
    }
}

/// Everything scoring needs beyond the job itself.
#[derive(Clone, Copy, Debug)]
pub struct PriorityConfig {
    pub weights: PriorityWeights,
    /// Age at which the age factor saturates. Beyond this a job stops gaining
    /// priority from waiting.
    pub age_horizon: VirtualDuration,
    /// Reference request the size factor normalizes against — in practice the
    /// largest slot capacity in the inventory.
    pub largest_request: ResourceRequest,
}

impl PriorityConfig {
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

    /// The smallest age increment that can change the age factor.
    ///
    /// Below this granularity the score is flat, which is why the
    /// anti-starvation property is stated in quanta rather than nanoseconds.
    #[must_use]
    pub const fn age_quantum(&self) -> VirtualDuration {
        VirtualDuration::from_nanos(self.age_horizon.as_nanos() / FACTOR_SCALE)
    }
}

/// Scales `value` into `0..=FACTOR_SCALE` relative to `max`, clamping above it.
/// Returns 0 when `max` is 0, so a disabled dimension contributes nothing.
const fn factor(value: u64, max: u64) -> u64 {
    if max == 0 {
        return 0;
    }
    let clamped = if value > max { max } else { value };
    // clamped <= max, so the product is at most max * FACTOR_SCALE; callers keep
    // max well below u64::MAX / FACTOR_SCALE.
    (clamped * FACTOR_SCALE) / max
}

/// The job's priority at `now` (spec §6 step 3).
#[must_use]
pub fn score(job: &Job, now: VirtualTime, config: &PriorityConfig) -> Score {
    let age = now.saturating_sub_time(job.submit_time).as_nanos();
    let age_factor = factor(age, config.age_horizon.as_nanos());

    let qos_factor = factor(
        u64::from(job.priority_class.ordinal()),
        (PriorityClass::COUNT - 1) as u64,
    );

    // Fair-share is identically zero until M5 (spec §6 step 3).
    let fairshare_factor = 0_u64;

    let size_factor = factor(
        u64::from(job.request.cpu_millis),
        u64::from(config.largest_request.cpu_millis),
    );

    age_factor * u64::from(config.weights.age)
        + qos_factor * u64::from(config.weights.qos)
        + fairshare_factor * u64::from(config.weights.fairshare)
        + size_factor * u64::from(config.weights.size)
}

/// The total order the ready set sorts by.
///
/// Higher score first; ties broken by lower [`JobId`], which is older-first
/// because the arena hands out ascending indices. Deriving `Ord` over
/// `(score, Reverse(id))` makes the order total by construction — there is no
/// incomparable pair, which is exactly what spec §9.1 asks for.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct OrderKey {
    score: Score,
    tiebreak: core::cmp::Reverse<u64>,
}

impl OrderKey {
    #[must_use]
    pub const fn new(score: Score, id: JobId) -> Self {
        Self {
            score,
            tiebreak: core::cmp::Reverse(id.to_bits()),
        }
    }

    #[must_use]
    pub const fn score(self) -> Score {
        self.score
    }

    #[must_use]
    pub const fn job(self) -> JobId {
        JobId::from_bits(self.tiebreak.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AccountId, Job, JobId, PriorityClass, ResourceRequest, VirtualDuration, VirtualTime,
    };

    fn config() -> PriorityConfig {
        PriorityConfig::new(
            PriorityWeights {
                age: 1_000,
                qos: 1_000,
                fairshare: 0,
                size: 1_000,
            },
            VirtualDuration::from_secs(3_600),
            ResourceRequest::new(4_000, 8 << 30, 2),
        )
    }

    fn job_at(submit: u64, class: PriorityClass, cpu: u32) -> Job {
        Job::new(
            AccountId::new(0),
            class,
            VirtualTime::from_nanos(submit),
            ResourceRequest::new(cpu, 0, 0),
            VirtualDuration::from_secs(60),
        )
    }

    #[test]
    fn a_brand_new_lowest_class_smallest_job_scores_zero() {
        let job = job_at(0, PriorityClass::Low, 0);
        assert_eq!(score(&job, VirtualTime::ZERO, &config()), 0);
    }

    #[test]
    fn age_raises_the_score() {
        let cfg = config();
        let job = job_at(0, PriorityClass::Low, 0);
        let young = score(&job, VirtualTime::from_nanos(0), &cfg);
        let old = score(&job, VirtualTime::from_nanos(1_800_000_000_000), &cfg);
        assert!(old > young, "{old} should exceed {young}");
    }

    #[test]
    fn age_factor_clamps_at_the_horizon() {
        let cfg = config();
        let job = job_at(0, PriorityClass::Low, 0);
        let at_horizon = score(&job, VirtualTime::from_nanos(3_600_000_000_000), &cfg);
        let past_horizon = score(&job, VirtualTime::from_nanos(36_000_000_000_000), &cfg);
        assert_eq!(at_horizon, past_horizon);
        assert_eq!(at_horizon, FACTOR_SCALE * 1_000);
    }

    #[test]
    fn higher_qos_class_scores_higher() {
        let cfg = config();
        let now = VirtualTime::ZERO;
        let low = score(&job_at(0, PriorityClass::Low, 0), now, &cfg);
        let urgent = score(&job_at(0, PriorityClass::Urgent, 0), now, &cfg);
        assert!(urgent > low);
    }

    #[test]
    fn larger_jobs_score_higher_at_equal_age_and_class() {
        let cfg = config();
        let now = VirtualTime::ZERO;
        let small = score(&job_at(0, PriorityClass::Normal, 500), now, &cfg);
        let large = score(&job_at(0, PriorityClass::Normal, 4_000), now, &cfg);
        assert!(large > small, "size factor favors larger jobs");
    }

    #[test]
    fn zero_weight_removes_a_factor_entirely() {
        let cfg = PriorityConfig::new(
            PriorityWeights {
                age: 0,
                qos: 1_000,
                fairshare: 0,
                size: 0,
            },
            VirtualDuration::from_secs(3_600),
            ResourceRequest::new(4_000, 8 << 30, 2),
        );
        let job = job_at(0, PriorityClass::Low, 4_000);
        let now = VirtualTime::from_nanos(3_600_000_000_000);
        assert_eq!(
            score(&job, now, &cfg),
            0,
            "only qos counts, and Low is ordinal 0"
        );
    }

    #[test]
    fn fairshare_contributes_nothing_until_m5() {
        let cfg = PriorityConfig::new(
            PriorityWeights {
                age: 0,
                qos: 0,
                fairshare: u32::MAX,
                size: 0,
            },
            VirtualDuration::from_secs(3_600),
            ResourceRequest::new(4_000, 8 << 30, 2),
        );
        assert_eq!(
            score(
                &job_at(0, PriorityClass::Urgent, 4_000),
                VirtualTime::ZERO,
                &cfg
            ),
            0
        );
    }

    #[test]
    fn order_key_ranks_higher_score_first_then_older_job() {
        let high = OrderKey::new(100, JobId::from_bits(9));
        let low = OrderKey::new(50, JobId::from_bits(1));
        assert!(high > low);

        let older = OrderKey::new(100, JobId::from_bits(1));
        let newer = OrderKey::new(100, JobId::from_bits(2));
        assert!(older > newer, "on a tie the lower JobId wins");
    }

    #[test]
    fn age_quantum_is_the_smallest_age_step_that_changes_the_score() {
        let cfg = config();
        assert_eq!(
            cfg.age_quantum().as_nanos(),
            3_600_000_000_000 / FACTOR_SCALE
        );
    }
}
