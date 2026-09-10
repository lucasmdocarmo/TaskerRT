//! Fair-share in the loop: two accounts competing for one slot.

use smallvec::smallvec;
use tasker_core::{
    AccountId, CycleConfig, FACTOR_SCALE, FairShareConfig, Job, PackBudget, PriorityClass,
    PriorityConfig, PriorityWeights, ResourceRequest, Resources, SlotInventory, VirtualDuration,
    VirtualTime,
};
use tasker_sim::{Action, Event, Outcome, Simulation};

const JOBS_PER_ACCOUNT: usize = 40;

fn secs(n: u64) -> VirtualDuration {
    VirtualDuration::from_secs(n)
}

fn config() -> CycleConfig {
    CycleConfig {
        priority: PriorityConfig::new(
            PriorityWeights::default(),
            secs(3_600),
            ResourceRequest::new(1_000, 0, 0),
        ),
        fairshare: FairShareConfig::new(secs(3_600)),
        budget: PackBudget::default(),
        max_candidates: 256,
    }
}

fn one_core_job(account: u32) -> Job {
    Job::new(
        AccountId::new(account),
        PriorityClass::Normal,
        VirtualTime::ZERO,
        ResourceRequest::new(1_000, 0, 0),
        secs(60),
    )
}

/// One 1-core slot, 1 s ticks, `JOBS_PER_ACCOUNT` identical 10 s jobs per
/// account, all submitted at t = 0. Account 0's jobs get the lower ids.
fn two_accounts(shares: [u32; 2]) -> Simulation {
    let mut sim = Simulation::new(
        SlotInventory::from_uniform(1, Resources::new(1_000, 0, 0)),
        config(),
        secs(1),
    );
    for (account, s) in shares.iter().enumerate() {
        let id = AccountId::new(u32::try_from(account).expect("two accounts"));
        sim.scheduler.set_shares(id, *s).unwrap();
    }
    for account in 0..2_u32 {
        for _ in 0..JOBS_PER_ACCOUNT {
            sim.schedule(
                VirtualTime::ZERO,
                Event::Submit {
                    job: one_core_job(account),
                    deps: smallvec![],
                    outcome: Outcome::Completes { after: secs(10) },
                },
            );
        }
    }
    sim
}

/// The account of every dispatch, in trace order.
fn dispatch_order(sim: &Simulation) -> Vec<u32> {
    sim.trace()
        .records()
        .iter()
        .filter_map(|r| match r.action {
            Action::Dispatched { .. } => r
                .job
                .and_then(|id| sim.jobs.get(id))
                .map(|j| j.account.get()),
            _ => None,
        })
        .collect()
}

#[test]
fn equal_shares_alternate_between_accounts() {
    let mut sim = two_accounts([1, 1]);
    assert!(sim.run_to_quiescence(2_000).unwrap());
    let order = dispatch_order(&sim);
    eprintln!("equal shares, first 12 dispatches: {:?}", &order[..12]);
    assert_eq!(order.len(), 2 * JOBS_PER_ACCOUNT);
    // Whoever just ran has the higher usage, so the other account goes next.
    let (mut a, mut b) = (0_i64, 0_i64);
    for acct in &order {
        if *acct == 0 {
            a += 1;
        } else {
            b += 1;
        }
        assert!((a - b).abs() <= 1, "lead exceeded one in {order:?}");
    }
}

#[test]
fn three_to_one_shares_yield_roughly_three_to_one_dispatches() {
    let mut sim = two_accounts([3, 1]);
    assert!(sim.run_to_quiescence(2_000).unwrap());
    let order = dispatch_order(&sim);
    eprintln!("3:1 shares, first 12 dispatches: {:?}", &order[..12]);
    let heavy = order.iter().take(40).filter(|a| **a == 0).count();
    // Expected 30; ties break toward account 0's lower ids.
    assert!(
        (27..=33).contains(&heavy),
        "heavy account got {heavy} of the first 40"
    );
}

#[test]
fn a_heavily_used_account_still_runs_when_alone() {
    let mut sim = Simulation::new(
        SlotInventory::from_uniform(1, Resources::new(1_000, 0, 0)),
        config(),
        secs(1),
    );
    for _ in 0..20 {
        sim.schedule(
            VirtualTime::ZERO,
            Event::Submit {
                job: one_core_job(0),
                deps: smallvec![],
                outcome: Outcome::Completes { after: secs(10) },
            },
        );
    }
    assert!(sim.run_to_quiescence(1_000).unwrap());
    assert_eq!(dispatch_order(&sim).len(), 20);
    // Alone, the account holds all usage and all shares: the factor settles at one half.
    assert_eq!(
        sim.scheduler.fairshare().factor(AccountId::new(0)),
        FACTOR_SCALE / 2
    );
}
