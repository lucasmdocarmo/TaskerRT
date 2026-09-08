use tasker_core::{
    AccountId, Arena, BackfillScratch, Disposition, Job, JobId, JobState, PackBudget,
    PriorityClass, ReadyEntry, ResourceRequest, Resources, RunningJob, SlotInventory,
    VirtualDuration, VirtualTime, easy_backfill, reservation_time,
};

const SECOND: u64 = 1_000_000_000;

fn ready_job(cpu: u32, walltime_secs: u64) -> Job {
    let mut job = Job::new(
        AccountId::new(0),
        PriorityClass::Normal,
        VirtualTime::ZERO,
        ResourceRequest::new(cpu, 0, 0),
        VirtualDuration::from_secs(walltime_secs),
    );
    job.try_transition(JobState::Ready).unwrap();
    job
}

fn running(job: u64, slot: u32, cpu: u32, ends_secs: u64) -> RunningJob {
    RunningJob {
        job: JobId::from_bits(job),
        slot,
        resources: Resources::new(cpu, 0, 0),
        ends_at: VirtualTime::from_nanos(ends_secs * SECOND),
    }
}

#[test]
fn reservation_is_when_enough_running_jobs_have_finished() {
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    inv.try_allocate(0, &Resources::new(2_000, 0, 0)).unwrap();
    inv.try_allocate(0, &Resources::new(2_000, 0, 0)).unwrap();
    let running = [running(0, 0, 2_000, 10), running(1, 0, 2_000, 30)];
    let mut scratch = BackfillScratch::new();

    // Needs 3000: 2000 frees at 10s (not enough), 4000 free at 30s.
    let at = reservation_time(
        &ResourceRequest::new(3_000, 0, 0),
        &inv,
        &running,
        VirtualTime::ZERO,
        &mut scratch,
    );
    assert_eq!(at, Some(VirtualTime::from_nanos(30 * SECOND)));

    // Needs only 2000: satisfied by the first completion.
    let at = reservation_time(
        &ResourceRequest::new(2_000, 0, 0),
        &inv,
        &running,
        VirtualTime::ZERO,
        &mut scratch,
    );
    assert_eq!(at, Some(VirtualTime::from_nanos(10 * SECOND)));
}

#[test]
fn reservation_takes_the_earliest_slot() {
    let mut inv = SlotInventory::from_uniform(2, Resources::new(4_000, 0, 0));
    inv.try_allocate(0, &Resources::new(4_000, 0, 0)).unwrap();
    inv.try_allocate(1, &Resources::new(4_000, 0, 0)).unwrap();
    let running = [running(0, 0, 4_000, 50), running(1, 1, 4_000, 20)];
    let mut scratch = BackfillScratch::new();

    let at = reservation_time(
        &ResourceRequest::new(4_000, 0, 0),
        &inv,
        &running,
        VirtualTime::ZERO,
        &mut scratch,
    );
    assert_eq!(at, Some(VirtualTime::from_nanos(20 * SECOND)));
}

#[test]
fn no_reservation_when_no_slot_could_ever_hold_the_request() {
    let inv = SlotInventory::from_uniform(1, Resources::new(1_000, 0, 0));
    let mut scratch = BackfillScratch::new();
    assert_eq!(
        reservation_time(
            &ResourceRequest::new(9_000, 0, 0),
            &inv,
            &[],
            VirtualTime::ZERO,
            &mut scratch
        ),
        None
    );
}

/// Slot of 4000 with 3000 held until 100s; the head needs all 4000.
fn hole_scenario(
    filler_cpu: u32,
    filler_walltime: u64,
) -> (SlotInventory, [RunningJob; 1], Arena<Job>, Vec<ReadyEntry>) {
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    inv.try_allocate(0, &Resources::new(3_000, 0, 0)).unwrap();
    let running = [running(99, 0, 3_000, 100)];
    let mut arena = Arena::new();
    let head = arena.insert(ready_job(4_000, 500));
    let filler = arena.insert(ready_job(filler_cpu, filler_walltime));
    let candidates = vec![
        ReadyEntry {
            class: PriorityClass::Normal,
            job: head,
            score: 100,
        },
        ReadyEntry {
            class: PriorityClass::Normal,
            job: filler,
            score: 1,
        },
    ];
    (inv, running, arena, candidates)
}

#[test]
fn a_short_job_backfills_into_the_hole() {
    let (mut inv, running, arena, candidates) = hole_scenario(1_000, 10);
    let mut disposition = vec![Disposition::Pending; candidates.len()];
    let mut decisions = Vec::new();
    let mut scratch = BackfillScratch::new();

    let outcome = easy_backfill(
        &candidates,
        0,
        &arena,
        &mut inv,
        &running,
        VirtualTime::ZERO,
        PackBudget::default(),
        &mut disposition,
        &mut decisions,
        &mut scratch,
    );

    assert_eq!(
        outcome.reservation,
        Some(VirtualTime::from_nanos(100 * SECOND))
    );
    assert_eq!(outcome.backfilled, 1);
    assert_eq!(
        disposition[0],
        Disposition::Pending,
        "the head job still waits"
    );
    assert_eq!(disposition[1], Disposition::Dispatched);
}

#[test]
fn a_long_job_that_would_delay_the_head_is_refused() {
    let (mut inv, running, arena, candidates) = hole_scenario(1_000, 500);
    let mut disposition = vec![Disposition::Pending; candidates.len()];
    let mut decisions = Vec::new();
    let mut scratch = BackfillScratch::new();

    let outcome = easy_backfill(
        &candidates,
        0,
        &arena,
        &mut inv,
        &running,
        VirtualTime::ZERO,
        PackBudget::default(),
        &mut disposition,
        &mut decisions,
        &mut scratch,
    );

    assert_eq!(outcome.backfilled, 0);
    assert_eq!(disposition[1], Disposition::Pending);
    assert_eq!(
        inv.free(0),
        Some(Resources::new(1_000, 0, 0)),
        "rolled back"
    );
}

#[test]
fn a_job_too_large_for_current_free_capacity_is_refused_however_short() {
    let (mut inv, running, arena, candidates) = hole_scenario(2_000, 1);
    let mut disposition = vec![Disposition::Pending; candidates.len()];
    let mut decisions = Vec::new();
    let mut scratch = BackfillScratch::new();

    let outcome = easy_backfill(
        &candidates,
        0,
        &arena,
        &mut inv,
        &running,
        VirtualTime::ZERO,
        PackBudget::default(),
        &mut disposition,
        &mut decisions,
        &mut scratch,
    );

    assert_eq!(
        outcome.backfilled, 0,
        "fitting now is a necessary condition"
    );
}
