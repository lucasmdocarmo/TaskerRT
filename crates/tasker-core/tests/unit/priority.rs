use tasker_core::{
    AccountId, FACTOR_SCALE, Job, JobId, OrderKey, PriorityClass, PriorityConfig, PriorityWeights,
    ResourceRequest, VirtualDuration, VirtualTime, score,
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
