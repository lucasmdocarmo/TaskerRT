use tasker_core::{
    AccountId, Arena, CycleConfig, Job, JobId, JobState, LifecycleError, PackBudget, PriorityClass,
    PriorityConfig, PriorityWeights, ResourceRequest, Resources, Scheduler, SlotInventory,
    VirtualDuration, VirtualTime,
};

fn config() -> CycleConfig {
    CycleConfig {
        priority: PriorityConfig::new(
            PriorityWeights {
                age: 1_000,
                qos: 1_000,
                fairshare: 0,
                size: 0,
            },
            VirtualDuration::from_secs(3_600),
            ResourceRequest::new(4_000, 0, 0),
        ),
        budget: PackBudget::default(),
        max_candidates: 256,
    }
}

fn new_job(cpu: u32, class: PriorityClass) -> Job {
    Job::new(
        AccountId::new(0),
        class,
        VirtualTime::ZERO,
        ResourceRequest::new(cpu, 0, 0),
        VirtualDuration::from_secs(60),
    )
}

fn job_depending_on(cpu: u32, deps: &[JobId]) -> Job {
    let mut j = new_job(cpu, PriorityClass::Normal);
    j.deps.extend_from_slice(deps);
    j
}

/// Inserts and submits, returning the handle and the state `submit` chose.
fn submit(
    s: &mut Scheduler,
    jobs: &mut Arena<Job>,
    job: Job,
    cfg: &CycleConfig,
) -> (JobId, JobState) {
    let id = jobs.insert(job);
    let state = s.submit(id, jobs, VirtualTime::ZERO, cfg).unwrap();
    (id, state)
}

fn run(
    s: &mut Scheduler,
    jobs: &mut Arena<Job>,
    inv: &mut SlotInventory,
    cfg: &CycleConfig,
) -> Vec<JobId> {
    let mut decisions = Vec::new();
    s.run_cycle(jobs, inv, &[], VirtualTime::ZERO, cfg, &mut decisions);
    decisions.into_iter().map(|d| d.job).collect()
}

#[test]
fn a_cycle_dispatches_what_fits_and_marks_jobs_running() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut s = Scheduler::with_slots(16);

    let (a, sa) = submit(
        &mut s,
        &mut jobs,
        new_job(1_000, PriorityClass::Normal),
        &cfg,
    );
    let (b, sb) = submit(
        &mut s,
        &mut jobs,
        new_job(1_000, PriorityClass::Normal),
        &cfg,
    );
    assert_eq!((sa, sb), (JobState::Ready, JobState::Ready));
    assert_eq!(s.pending(), 2);

    let dispatched = run(&mut s, &mut jobs, &mut inv, &cfg);
    assert_eq!(dispatched.len(), 2);
    assert_eq!(s.pending(), 0);
    assert_eq!(jobs.get(a).unwrap().state, JobState::Running);
    assert_eq!(jobs.get(b).unwrap().state, JobState::Running);
}

#[test]
fn a_job_that_does_not_fit_stays_pending_and_is_reported_as_head() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(1_000, 0, 0));
    let mut s = Scheduler::with_slots(16);
    let (big, _) = submit(
        &mut s,
        &mut jobs,
        new_job(4_000, PriorityClass::Urgent),
        &cfg,
    );

    let mut decisions = Vec::new();
    let outcome = s.run_cycle(
        &mut jobs,
        &mut inv,
        &[],
        VirtualTime::ZERO,
        &cfg,
        &mut decisions,
    );

    assert_eq!(outcome.dispatched, 0);
    assert_eq!(outcome.head, Some(big));
    assert_eq!(s.pending(), 1, "the head job returns to the ready set");
    assert_eq!(jobs.get(big).unwrap().state, JobState::Ready);
}

#[test]
fn class_order_is_respected_across_cycles() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(1_000, 0, 0));
    let mut s = Scheduler::with_slots(16);
    submit(&mut s, &mut jobs, new_job(1_000, PriorityClass::Low), &cfg);
    let (urgent, _) = submit(
        &mut s,
        &mut jobs,
        new_job(1_000, PriorityClass::Urgent),
        &cfg,
    );

    let dispatched = run(&mut s, &mut jobs, &mut inv, &cfg);
    assert_eq!(
        dispatched,
        vec![urgent],
        "Urgent outranks Low regardless of score"
    );
    assert_eq!(s.pending(), 1);
}

#[test]
fn a_removed_job_is_dropped_from_the_ready_set() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut s = Scheduler::with_slots(16);
    let (doomed, _) = submit(
        &mut s,
        &mut jobs,
        new_job(1_000, PriorityClass::Normal),
        &cfg,
    );
    jobs.remove(doomed);

    let dispatched = run(&mut s, &mut jobs, &mut inv, &cfg);
    assert!(dispatched.is_empty());
    assert_eq!(s.pending(), 0, "the stale entry is not reinserted");
}

#[test]
fn the_candidate_limit_bounds_the_cycle() {
    let mut cfg = config();
    cfg.max_candidates = 2;
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(400_000, 0, 0));
    let mut s = Scheduler::with_slots(64);
    for _ in 0..10 {
        submit(&mut s, &mut jobs, new_job(100, PriorityClass::Normal), &cfg);
    }

    let mut decisions = Vec::new();
    let outcome = s.run_cycle(
        &mut jobs,
        &mut inv,
        &[],
        VirtualTime::ZERO,
        &cfg,
        &mut decisions,
    );
    assert_eq!(outcome.candidates, 2);
    assert_eq!(outcome.dispatched, 2);
    assert_eq!(s.pending(), 8, "the rest wait for the next tick");
}

// ---- M2: dependencies ----

#[test]
fn submit_with_a_pending_dependency_blocks_and_stays_out_of_the_ready_set() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut s = Scheduler::with_slots(16);
    let (a, _) = submit(&mut s, &mut jobs, new_job(100, PriorityClass::Normal), &cfg);
    let (b, sb) = submit(&mut s, &mut jobs, job_depending_on(100, &[a]), &cfg);

    assert_eq!(sb, JobState::Blocked);
    assert_eq!(jobs.get(b).unwrap().state, JobState::Blocked);
    assert_eq!(s.pending(), 1, "only `a` is ready");
}

#[test]
fn completion_promotes_the_dependent_into_the_ready_set() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut s = Scheduler::with_slots(16);
    let (a, _) = submit(&mut s, &mut jobs, new_job(100, PriorityClass::Normal), &cfg);
    let (b, _) = submit(&mut s, &mut jobs, job_depending_on(100, &[a]), &cfg);

    assert_eq!(run(&mut s, &mut jobs, &mut inv, &cfg), vec![a]);
    assert_eq!(s.pending(), 0, "b is Blocked, not Ready");

    let mut promoted = Vec::new();
    s.on_completed(a, &mut jobs, VirtualTime::ZERO, &cfg, &mut promoted)
        .unwrap();
    assert_eq!(promoted, vec![b]);
    assert_eq!(jobs.get(a).unwrap().state, JobState::Completed);
    assert_eq!(jobs.get(b).unwrap().state, JobState::Ready);
    assert_eq!(s.pending(), 1);

    assert_eq!(run(&mut s, &mut jobs, &mut inv, &cfg), vec![b]);
}

#[test]
fn a_dependency_on_an_already_completed_job_is_ready_immediately() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut s = Scheduler::with_slots(16);
    let (a, _) = submit(&mut s, &mut jobs, new_job(100, PriorityClass::Normal), &cfg);
    run(&mut s, &mut jobs, &mut inv, &cfg);
    s.on_completed(a, &mut jobs, VirtualTime::ZERO, &cfg, &mut Vec::new())
        .unwrap();

    let (_, sb) = submit(&mut s, &mut jobs, job_depending_on(100, &[a]), &cfg);
    assert_eq!(sb, JobState::Ready);
}

#[test]
fn failure_cascades_cancellation_through_the_graph() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut s = Scheduler::with_slots(16);
    let (a, _) = submit(&mut s, &mut jobs, new_job(100, PriorityClass::Normal), &cfg);
    let (b, _) = submit(&mut s, &mut jobs, job_depending_on(100, &[a]), &cfg);
    let (c, _) = submit(&mut s, &mut jobs, job_depending_on(100, &[b]), &cfg);
    run(&mut s, &mut jobs, &mut inv, &cfg);

    let mut cancelled = Vec::new();
    s.on_failed(a, &mut jobs, &mut cancelled).unwrap();
    cancelled.sort();
    let mut expected = vec![b, c];
    expected.sort();
    assert_eq!(cancelled, expected);
    assert_eq!(jobs.get(a).unwrap().state, JobState::Failed);
    assert_eq!(jobs.get(b).unwrap().state, JobState::Cancelled);
    assert_eq!(jobs.get(c).unwrap().state, JobState::Cancelled);
    assert_eq!(s.pending(), 0);
}

#[test]
fn cancel_removes_a_ready_job_from_the_ready_set_and_cascades() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut s = Scheduler::with_slots(16);
    let (a, _) = submit(&mut s, &mut jobs, new_job(100, PriorityClass::Normal), &cfg);
    let (b, _) = submit(&mut s, &mut jobs, job_depending_on(100, &[a]), &cfg);
    assert_eq!(s.pending(), 1);

    let mut cancelled = Vec::new();
    s.cancel(a, &mut jobs, &mut cancelled).unwrap();
    assert_eq!(cancelled, vec![b]);
    assert_eq!(s.pending(), 0, "a left the ready set");
    assert_eq!(jobs.get(a).unwrap().state, JobState::Cancelled);
    assert_eq!(jobs.get(b).unwrap().state, JobState::Cancelled);
}

#[test]
fn submit_with_a_doomed_dependency_is_cancelled_immediately() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut s = Scheduler::with_slots(16);
    let (a, _) = submit(&mut s, &mut jobs, new_job(100, PriorityClass::Normal), &cfg);
    s.cancel(a, &mut jobs, &mut Vec::new()).unwrap();

    let (b, sb) = submit(&mut s, &mut jobs, job_depending_on(100, &[a]), &cfg);
    assert_eq!(sb, JobState::Cancelled);
    assert_eq!(jobs.get(b).unwrap().state, JobState::Cancelled);
    assert_eq!(s.pending(), 0);
}

#[test]
fn submit_rejects_a_job_that_is_not_submitted() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut s = Scheduler::with_slots(16);
    let (a, _) = submit(&mut s, &mut jobs, new_job(100, PriorityClass::Normal), &cfg);
    let err = s.submit(a, &mut jobs, VirtualTime::ZERO, &cfg).unwrap_err();
    assert!(
        matches!(err, LifecycleError::NotSubmitted { job, state: JobState::Ready } if job == a)
    );
}

#[test]
fn submit_rejects_an_unknown_dependency() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut s = Scheduler::with_slots(16);
    let ghost = jobs.insert(new_job(1, PriorityClass::Normal));
    jobs.remove(ghost);
    let b = jobs.insert(job_depending_on(100, &[ghost]));
    let err = s.submit(b, &mut jobs, VirtualTime::ZERO, &cfg).unwrap_err();
    assert!(matches!(err, LifecycleError::Dependency(e) if e.dependency == ghost));
    assert_eq!(
        jobs.get(b).unwrap().state,
        JobState::Submitted,
        "rejected: untouched"
    );
}

#[test]
fn requeue_returns_a_running_job_to_the_ready_set_via_preempted() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut s = Scheduler::with_slots(16);
    let (a, _) = submit(&mut s, &mut jobs, new_job(100, PriorityClass::Normal), &cfg);
    assert_eq!(run(&mut s, &mut jobs, &mut inv, &cfg), vec![a]);
    assert_eq!(s.pending(), 0);

    s.requeue(a, &mut jobs, VirtualTime::ZERO, &cfg).unwrap();
    assert_eq!(jobs.get(a).unwrap().state, JobState::Ready);
    assert_eq!(s.pending(), 1);
    // Capacity is the caller's to release; the slot still shows the job.
    assert_eq!(inv.free(0), Some(Resources::new(3_900, 0, 0)));
}

#[test]
fn requeue_rejects_a_job_that_is_not_running() {
    let cfg = config();
    let mut jobs = Arena::new();
    let mut s = Scheduler::with_slots(16);
    let (a, _) = submit(&mut s, &mut jobs, new_job(100, PriorityClass::Normal), &cfg);
    assert!(matches!(
        s.requeue(a, &mut jobs, VirtualTime::ZERO, &cfg)
            .unwrap_err(),
        LifecycleError::Transition(_)
    ));
}
