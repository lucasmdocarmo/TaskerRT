//! Preemption in the loop: an urgent arrival evicts low work, victims rerun,
//! and an immune job is left alone.

use smallvec::smallvec;
use tasker_core::{
    AccountId, CycleConfig, FairShareConfig, Job, JobState, PackBudget, PreemptConfig,
    PriorityClass, PriorityConfig, PriorityWeights, ResourceRequest, Resources, SlotInventory,
    VirtualDuration, VirtualTime,
};
use tasker_sim::{Action, Event, Outcome, Simulation};

fn secs(n: u64) -> VirtualDuration {
    VirtualDuration::from_secs(n)
}

fn at(n: u64) -> VirtualTime {
    VirtualTime::from_nanos(n * 1_000_000_000)
}

fn config(max_preemptions: u8) -> CycleConfig {
    CycleConfig {
        priority: PriorityConfig::new(
            PriorityWeights::default(),
            secs(3_600),
            ResourceRequest::new(4_000, 0, 0),
        ),
        fairshare: FairShareConfig::default(),
        preempt: PreemptConfig {
            min_class: Some(PriorityClass::High),
            max_preemptions,
            grace: secs(5),
        },
        budget: PackBudget::default(),
        max_candidates: 64,
    }
}

fn sim(cpu: u32, max_preemptions: u8) -> Simulation {
    Simulation::new(
        SlotInventory::from_uniform(1, Resources::new(cpu, 0, 0)),
        config(max_preemptions),
        secs(1),
    )
}

/// Submits at `when` a job that completes `runs_for` seconds after each dispatch.
fn submit(sim: &mut Simulation, when: u64, class: PriorityClass, cpu: u32, runs_for: u64) {
    let job = Job::new(
        AccountId::new(0),
        class,
        at(when),
        ResourceRequest::new(cpu, 0, 0),
        secs(600),
    );
    sim.schedule(
        at(when),
        Event::Submit {
            job,
            deps: smallvec![],
            outcome: Outcome::Completes {
                after: secs(runs_for),
            },
        },
    );
}

fn count(sim: &Simulation, wanted: &Action) -> usize {
    sim.trace()
        .records()
        .iter()
        .filter(|r| &r.action == wanted)
        .count()
}

fn state(sim: &Simulation, submit_index: usize) -> JobState {
    sim.jobs
        .get(sim.handle(submit_index).unwrap())
        .unwrap()
        .state
}

#[test]
fn an_urgent_arrival_evicts_low_work_which_reruns_later() {
    // Four 1-core Low jobs fill the 4-core slot at t = 1. A 2-core High job
    // arrives at t = 5, cannot fit, and evicts two Low jobs; after the 5 s grace
    // they are Ready again and the High job starts.
    let mut sim = sim(4_000, 3);
    for _ in 0..4 {
        submit(&mut sim, 0, PriorityClass::Low, 1_000, 30);
    }
    submit(&mut sim, 5, PriorityClass::High, 2_000, 10);
    sim.run_until(at(12)).unwrap();
    assert_eq!(
        state(&sim, 4),
        JobState::Running,
        "High runs once the grace is over"
    );
    assert_eq!(count(&sim, &Action::Preempted), 2);
    assert_eq!(count(&sim, &Action::Requeued), 2);
    let evicted = (0..4)
        .filter(|i| sim.jobs.get(sim.handle(*i).unwrap()).unwrap().preemptions == 1)
        .count();
    assert_eq!(evicted, 2);
    // Everyone finishes: the victims rerun after the High job completes.
    assert!(sim.run_to_quiescence(300).unwrap());
    for i in 0..5 {
        assert_eq!(state(&sim, i), JobState::Completed, "job {i}");
    }
    assert_eq!(
        count(&sim, &Action::Preempted),
        2,
        "no second round of evictions"
    );
}

#[test]
fn an_immune_job_is_left_alone() {
    // max_preemptions = 1. The Low job is evicted once for the first High job,
    // reruns, and the second High job has to wait for it.
    let mut sim = sim(1_000, 1);
    submit(&mut sim, 0, PriorityClass::Low, 1_000, 100);
    submit(&mut sim, 5, PriorityClass::High, 1_000, 10);
    submit(&mut sim, 40, PriorityClass::High, 1_000, 10);
    assert!(sim.run_to_quiescence(400).unwrap());
    assert_eq!(count(&sim, &Action::Preempted), 1);
    assert_eq!(sim.jobs.get(sim.handle(0).unwrap()).unwrap().preemptions, 1);
    for i in 0..3 {
        assert_eq!(state(&sim, i), JobState::Completed, "job {i}");
    }
}
