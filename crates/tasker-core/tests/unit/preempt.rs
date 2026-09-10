use tasker_core::{
    AccountId, Arena, Eviction, Job, JobId, JobState, PreemptConfig, PriorityClass,
    ResourceRequest, Resources, RunningJob, SlotInventory, VirtualDuration, VirtualTime,
    plan_preemption,
};

fn secs(n: u64) -> VirtualDuration {
    VirtualDuration::from_secs(n)
}

fn at(secs: u64) -> VirtualTime {
    VirtualTime::from_nanos(secs * 1_000_000_000)
}

fn cfg() -> PreemptConfig {
    PreemptConfig {
        min_class: Some(PriorityClass::High),
        max_preemptions: 3,
        grace: secs(5),
    }
}

fn job(class: PriorityClass, cpu: u32) -> Job {
    Job::new(
        AccountId::new(0),
        class,
        VirtualTime::ZERO,
        ResourceRequest::new(cpu, 0, 0),
        secs(60),
    )
}

/// Inserts `job` as Running on `slot` since `started`, allocating its capacity.
fn run_on(
    jobs: &mut Arena<Job>,
    inv: &mut SlotInventory,
    running: &mut Vec<RunningJob>,
    job: Job,
    slot: u32,
    started: u64,
) -> JobId {
    let resources = job.request;
    let id = jobs.insert(job);
    jobs.get_mut(id).unwrap().state = JobState::Running;
    inv.try_allocate(slot, &resources).unwrap();
    running.push(RunningJob {
        job: id,
        slot,
        resources,
        ends_at: at(started).saturating_add(secs(60)),
    });
    id
}

fn victims(out: &[Eviction]) -> Vec<JobId> {
    out.iter().map(|e| e.job).collect()
}

#[test]
fn evicts_the_youngest_lowest_class_jobs_that_free_enough() {
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut running = Vec::new();
    let low_old = run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Low, 1_000),
        0,
        0,
    );
    let normal = run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Normal, 2_000),
        0,
        5,
    );
    let low_young = run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Low, 1_000),
        0,
        10,
    );
    let head = job(PriorityClass::High, 2_000);
    let mut out = Vec::new();
    let slot = plan_preemption(&head, &running, &jobs, &inv, at(20), &cfg(), &mut out);
    assert_eq!(slot, Some(0));
    // Two Low jobs free 2 000; the Normal one is spared and the youngest Low goes first.
    assert_eq!(victims(&out), vec![low_young, low_old]);
    assert!(!victims(&out).contains(&normal));
}

#[test]
fn never_evicts_the_same_class_or_an_immune_job() {
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut running = Vec::new();
    run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::High, 4_000),
        0,
        0,
    );
    let mut out = Vec::new();
    assert_eq!(
        plan_preemption(
            &job(PriorityClass::High, 1_000),
            &running,
            &jobs,
            &inv,
            at(1),
            &cfg(),
            &mut out
        ),
        None
    );
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut running = Vec::new();
    let mut jobs = Arena::new();
    let immune = run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Low, 4_000),
        0,
        0,
    );
    jobs.get_mut(immune).unwrap().preemptions = 3;
    assert_eq!(
        plan_preemption(
            &job(PriorityClass::Urgent, 1_000),
            &running,
            &jobs,
            &inv,
            at(1),
            &cfg(),
            &mut out
        ),
        None
    );
    assert!(out.is_empty());
}

#[test]
fn prefers_the_slot_that_needs_fewer_victims() {
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(2, Resources::new(4_000, 0, 0));
    let mut running = Vec::new();
    run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Low, 2_000),
        0,
        0,
    );
    run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Low, 2_000),
        0,
        0,
    );
    let single = run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Low, 4_000),
        1,
        0,
    );
    let mut out = Vec::new();
    let slot = plan_preemption(
        &job(PriorityClass::High, 4_000),
        &running,
        &jobs,
        &inv,
        at(9),
        &cfg(),
        &mut out,
    );
    assert_eq!(slot, Some(1));
    assert_eq!(victims(&out), vec![single]);
}

#[test]
fn nothing_when_disabled_too_low_or_not_enough_to_free() {
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut running = Vec::new();
    run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Low, 2_000),
        0,
        0,
    );
    run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Urgent, 2_000),
        0,
        0,
    );
    let mut out = Vec::new();
    let off = PreemptConfig {
        min_class: None,
        ..cfg()
    };
    assert_eq!(
        plan_preemption(
            &job(PriorityClass::Urgent, 1_000),
            &running,
            &jobs,
            &inv,
            at(1),
            &off,
            &mut out
        ),
        None
    );
    // Normal is below the High threshold.
    assert_eq!(
        plan_preemption(
            &job(PriorityClass::Normal, 1_000),
            &running,
            &jobs,
            &inv,
            at(1),
            &cfg(),
            &mut out
        ),
        None
    );
    // The Urgent job is a peer, so only the Low one can go: 2 000 freed, 4 000 wanted.
    assert_eq!(
        plan_preemption(
            &job(PriorityClass::Urgent, 4_000),
            &running,
            &jobs,
            &inv,
            at(1),
            &cfg(),
            &mut out
        ),
        None
    );
    assert!(out.is_empty());
}

#[test]
fn no_new_plan_while_an_eviction_is_in_flight() {
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut running = Vec::new();
    let evicting = run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Low, 2_000),
        0,
        0,
    );
    run_on(
        &mut jobs,
        &mut inv,
        &mut running,
        job(PriorityClass::Low, 2_000),
        0,
        0,
    );
    jobs.get_mut(evicting).unwrap().state = JobState::Preempted;
    let mut out = Vec::new();
    assert_eq!(
        plan_preemption(
            &job(PriorityClass::High, 2_000),
            &running,
            &jobs,
            &inv,
            at(1),
            &cfg(),
            &mut out
        ),
        None
    );
}
