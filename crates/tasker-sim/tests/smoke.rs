use smallvec::smallvec;
use tasker_core::{
    AccountId, CycleConfig, FairShareConfig, Job, JobState, PackBudget, PriorityClass,
    PriorityConfig, PriorityWeights, ResourceRequest, Resources, SlotInventory, VirtualDuration,
    VirtualTime,
};
use tasker_sim::{Action, Event, Outcome, Simulation};

const SECOND: u64 = 1_000_000_000;

fn secs(n: u64) -> VirtualDuration {
    VirtualDuration::from_secs(n)
}

fn config() -> CycleConfig {
    CycleConfig {
        priority: PriorityConfig::new(
            PriorityWeights::default(),
            secs(3_600),
            ResourceRequest::new(4_000, 0, 0),
        ),
        fairshare: FairShareConfig::default(),
        budget: PackBudget::default(),
        max_candidates: 64,
    }
}

fn job(cpu: u32, walltime_secs: u64) -> Job {
    Job::new(
        AccountId::new(0),
        PriorityClass::Normal,
        VirtualTime::ZERO,
        ResourceRequest::new(cpu, 0, 0),
        secs(walltime_secs),
    )
}

fn sim() -> Simulation {
    let mut s = Simulation::new(
        SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0)),
        config(),
        secs(1),
    );
    s.check_invariants = true;
    s
}

/// Positions in the trace of every (job, action) pair, for ordering assertions.
fn position(s: &Simulation, submit_index: usize, action: Action) -> Option<usize> {
    let id = s.handle(submit_index)?;
    s.trace()
        .records()
        .iter()
        .position(|r| r.job == Some(id) && r.action == action)
}

#[test]
fn a_chain_runs_in_dependency_order() {
    let mut s = sim();
    s.schedule(
        VirtualTime::ZERO,
        Event::Submit {
            job: job(1_000, 60),
            deps: smallvec![],
            outcome: Outcome::Completes { after: secs(10) },
        },
    );
    s.schedule(
        VirtualTime::ZERO,
        Event::Submit {
            job: job(1_000, 60),
            deps: smallvec![0],
            outcome: Outcome::Completes { after: secs(5) },
        },
    );

    assert!(s.run_to_quiescence(1_000).unwrap(), "must drain");

    assert_eq!(
        s.jobs.get(s.handle(0).unwrap()).unwrap().state,
        JobState::Completed
    );
    assert_eq!(
        s.jobs.get(s.handle(1).unwrap()).unwrap().state,
        JobState::Completed
    );

    let a_done = position(&s, 0, Action::Completed).unwrap();
    let b_promoted = position(&s, 1, Action::Promoted).unwrap();
    let b_dispatched = position(&s, 1, Action::Dispatched { slot: 0 }).unwrap();
    assert!(a_done < b_promoted, "promotion follows completion");
    assert!(b_promoted < b_dispatched, "dispatch follows promotion");
    assert_eq!(
        position(
            &s,
            1,
            Action::Submitted {
                state: JobState::Blocked
            }
        ),
        Some(1),
        "b was Blocked at submit"
    );
}

#[test]
fn a_failed_dependency_cancels_the_dependent() {
    let mut s = sim();
    s.schedule(
        VirtualTime::ZERO,
        Event::Submit {
            job: job(1_000, 60),
            deps: smallvec![],
            outcome: Outcome::Fails { after: secs(3) },
        },
    );
    s.schedule(
        VirtualTime::ZERO,
        Event::Submit {
            job: job(1_000, 60),
            deps: smallvec![0],
            outcome: Outcome::Completes { after: secs(1) },
        },
    );

    assert!(s.run_to_quiescence(1_000).unwrap());
    assert_eq!(
        s.jobs.get(s.handle(0).unwrap()).unwrap().state,
        JobState::Failed
    );
    assert_eq!(
        s.jobs.get(s.handle(1).unwrap()).unwrap().state,
        JobState::Cancelled
    );
    assert!(
        position(&s, 1, Action::Dispatched { slot: 0 }).is_none(),
        "never ran"
    );
    assert!(
        s.inventory.free(0).unwrap().cpu_millis == 4_000,
        "capacity released"
    );
}

#[test]
fn exceeding_the_walltime_is_a_failure() {
    let mut s = sim();
    s.schedule(
        VirtualTime::ZERO,
        Event::Submit {
            job: job(1_000, 5),
            deps: smallvec![],
            outcome: Outcome::Completes { after: secs(50) }, // longer than the 5 s limit
        },
    );
    assert!(s.run_to_quiescence(1_000).unwrap());
    assert_eq!(
        s.jobs.get(s.handle(0).unwrap()).unwrap().state,
        JobState::Failed
    );
    let failed_at = s
        .trace()
        .records()
        .iter()
        .find(|r| r.action == Action::Failed)
        .map(|r| r.at.as_nanos())
        .unwrap();
    // Dispatched on the first 1 s tick, killed 5 s later.
    assert_eq!(failed_at, 6 * SECOND);
}

#[test]
fn cancelling_a_running_job_releases_its_slot() {
    let mut s = sim();
    s.schedule(
        VirtualTime::ZERO,
        Event::Submit {
            job: job(4_000, 60),
            deps: smallvec![],
            outcome: Outcome::Completes { after: secs(30) },
        },
    );
    s.schedule(
        VirtualTime::from_nanos(3 * SECOND),
        Event::Cancel { submit_index: 0 },
    );

    assert!(s.run_to_quiescence(1_000).unwrap());
    assert_eq!(
        s.jobs.get(s.handle(0).unwrap()).unwrap().state,
        JobState::Cancelled
    );
    assert_eq!(s.inventory.free(0).unwrap().cpu_millis, 4_000);
    assert!(s.running.is_empty());
}

#[test]
fn a_forward_dependency_is_a_scenario_error() {
    let mut s = sim();
    s.schedule(
        VirtualTime::ZERO,
        Event::Submit {
            job: job(1, 1),
            deps: smallvec![3],
            outcome: Outcome::Completes { after: secs(1) },
        },
    );
    assert!(s.step().is_err());
}
