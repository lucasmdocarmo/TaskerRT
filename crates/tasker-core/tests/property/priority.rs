//! Properties: priority ordering is total, and age never lowers a score
//! (spec §9.1).

// The order-theoretic assertions below are written in their textbook form
// (`!(a < a)` for irreflexivity) on purpose; the minimal boolean rewrite reads
// worse and states a different-looking property.
#![allow(clippy::nonminimal_bool)]

use proptest::prelude::*;
use tasker_core::{
    AccountId, FACTOR_SCALE, Job, JobId, OrderKey, PriorityClass, PriorityConfig, PriorityWeights,
    ResourceRequest, VirtualDuration, VirtualTime, score,
};

const HORIZON_NANOS: u64 = 3_600_000_000_000;

fn config() -> PriorityConfig {
    PriorityConfig::new(
        PriorityWeights {
            age: 1_000,
            qos: 1_000,
            fairshare: 0,
            size: 100,
        },
        VirtualDuration::from_nanos(HORIZON_NANOS),
        ResourceRequest::new(4_000, 8 << 30, 2),
    )
}

fn key_strategy() -> impl Strategy<Value = OrderKey> {
    (0_u64..1_000_000_000, any::<u64>())
        .prop_map(|(s, bits)| OrderKey::new(s, JobId::from_bits(bits)))
}

proptest! {
    #[test]
    fn ordering_is_irreflexive_and_antisymmetric(a in key_strategy(), b in key_strategy()) {
        prop_assert!(!(a < a));
        if a < b {
            prop_assert!(b > a);
            prop_assert!(a != b);
        }
        if a == b {
            prop_assert!(!(a < b) && !(b < a));
        }
    }

    #[test]
    fn ordering_is_transitive(a in key_strategy(), b in key_strategy(), c in key_strategy()) {
        let mut sorted = [a, b, c];
        sorted.sort_unstable();
        prop_assert!(sorted[0] <= sorted[1] && sorted[1] <= sorted[2]);
        prop_assert!(sorted[0] <= sorted[2], "transitivity");
    }

    #[test]
    fn score_never_decreases_with_age(
        submit in 0_u64..1_000_000_000,
        elapsed_a in 0_u64..(HORIZON_NANOS * 4),
        extra in 0_u64..(HORIZON_NANOS * 4),
        class_ordinal in 0_usize..PriorityClass::COUNT,
        cpu in 0_u32..4_000,
    ) {
        let cfg = config();
        let job = Job::new(
            AccountId::new(0),
            PriorityClass::ALL[class_ordinal],
            VirtualTime::from_nanos(submit),
            ResourceRequest::new(cpu, 0, 0),
            VirtualDuration::from_secs(60),
        );
        let earlier = VirtualTime::from_nanos(submit.saturating_add(elapsed_a));
        let later = VirtualTime::from_nanos(
            submit.saturating_add(elapsed_a).saturating_add(extra),
        );
        prop_assert!(score(&job, later, &cfg) >= score(&job, earlier, &cfg));
    }

    #[test]
    fn score_strictly_increases_below_the_horizon(
        elapsed in 0_u64..(HORIZON_NANOS / 2),
        class_ordinal in 0_usize..PriorityClass::COUNT,
        cpu in 0_u32..4_000,
    ) {
        // Anti-starvation, in its testable form: below the age horizon and one
        // quantum apart, waiting strictly raises the score.
        let cfg = config();
        let quantum = HORIZON_NANOS / FACTOR_SCALE;
        let job = Job::new(
            AccountId::new(0),
            PriorityClass::ALL[class_ordinal],
            VirtualTime::ZERO,
            ResourceRequest::new(cpu, 0, 0),
            VirtualDuration::from_secs(60),
        );
        let before = score(&job, VirtualTime::from_nanos(elapsed), &cfg);
        let after = score(&job, VirtualTime::from_nanos(elapsed + quantum), &cfg);
        prop_assert!(after > before, "{after} should exceed {before}");
    }
}
