use tasker_core::{
    AccountId, Arena, Disposition, Job, JobState, PackBudget, PriorityClass, ReadyEntry,
    ResourceRequest, Resources, SlotInventory, VirtualDuration, VirtualTime, pack,
};

fn job_with(cpu: u32) -> Job {
    let mut job = Job::new(
        AccountId::new(0),
        PriorityClass::Normal,
        VirtualTime::ZERO,
        ResourceRequest::new(cpu, 0, 0),
        VirtualDuration::from_secs(60),
    );
    job.try_transition(JobState::Ready).unwrap();
    job
}

/// Builds an arena of jobs and the matching candidate list, highest score first.
fn fixture(cpus: &[u32]) -> (Arena<Job>, Vec<ReadyEntry>) {
    let mut arena = Arena::new();
    let mut candidates = Vec::new();
    for (rank, cpu) in cpus.iter().enumerate() {
        let job = arena.insert(job_with(*cpu));
        candidates.push(ReadyEntry {
            class: PriorityClass::Normal,
            job,
            score: (cpus.len() - rank) as u64,
        });
    }
    (arena, candidates)
}

#[test]
fn everything_that_fits_is_dispatched() {
    let (arena, candidates) = fixture(&[1_000, 1_000]);
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut disposition = vec![Disposition::Pending; candidates.len()];
    let mut decisions = Vec::new();

    let outcome = pack(
        &candidates,
        &arena,
        &mut inv,
        PackBudget::default(),
        &mut disposition,
        &mut decisions,
    );

    assert_eq!(outcome.dispatched, 2);
    assert_eq!(outcome.head, None);
    assert!(disposition.iter().all(|d| *d == Disposition::Dispatched));
    assert_eq!(inv.free(0), Some(Resources::new(2_000, 0, 0)));
}

#[test]
fn packing_stops_at_the_first_job_that_does_not_fit() {
    let (arena, candidates) = fixture(&[3_000, 3_000, 500]);
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut disposition = vec![Disposition::Pending; candidates.len()];
    let mut decisions = Vec::new();

    let outcome = pack(
        &candidates,
        &arena,
        &mut inv,
        PackBudget::default(),
        &mut disposition,
        &mut decisions,
    );

    assert_eq!(outcome.dispatched, 1);
    assert_eq!(outcome.head, Some(1), "index 1 is the head job");
    assert_eq!(disposition[0], Disposition::Dispatched);
    assert_eq!(disposition[1], Disposition::Pending);
    assert_eq!(
        disposition[2],
        Disposition::Pending,
        "packing does not consider anything past the head; backfill does"
    );
}

#[test]
fn a_stale_candidate_is_dropped_not_returned() {
    let mut arena: Arena<Job> = Arena::new();
    let job = arena.insert(job_with(100));
    arena.remove(job);
    let candidates = vec![ReadyEntry {
        class: PriorityClass::Normal,
        job,
        score: 1,
    }];
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut disposition = vec![Disposition::Pending; 1];
    let mut decisions = Vec::new();

    let outcome = pack(
        &candidates,
        &arena,
        &mut inv,
        PackBudget::default(),
        &mut disposition,
        &mut decisions,
    );

    assert_eq!(outcome.dispatched, 0);
    assert_eq!(outcome.head, None);
    assert_eq!(disposition[0], Disposition::Dropped);
}

#[test]
fn the_decision_budget_bounds_work_per_cycle() {
    let (arena, candidates) = fixture(&[100, 100, 100, 100]);
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    let mut disposition = vec![Disposition::Pending; candidates.len()];
    let mut decisions = Vec::new();

    let outcome = pack(
        &candidates,
        &arena,
        &mut inv,
        PackBudget {
            max_decisions: 2,
            max_scanned: 100,
        },
        &mut disposition,
        &mut decisions,
    );

    assert_eq!(outcome.dispatched, 2);
    assert!(outcome.budget_exhausted);
    assert_eq!(
        outcome.head, None,
        "running out of budget is not a head job"
    );
    assert_eq!(disposition[2], Disposition::Pending);
}

#[test]
fn dispatch_decisions_name_the_slot_used() {
    let (arena, candidates) = fixture(&[3_000, 3_000]);
    let mut inv = SlotInventory::from_uniform(2, Resources::new(4_000, 0, 0));
    let mut disposition = vec![Disposition::Pending; candidates.len()];
    let mut decisions = Vec::new();

    pack(
        &candidates,
        &arena,
        &mut inv,
        PackBudget::default(),
        &mut disposition,
        &mut decisions,
    );

    assert_eq!(decisions.len(), 2);
    assert_eq!(decisions[0].slot, 0);
    assert_eq!(
        decisions[1].slot, 1,
        "first-fit moves on when slot 0 is full"
    );
}
