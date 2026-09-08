//! EASY backfill (spec §6 step 6).
//!
//! Only the head job — the highest-priority job that does not currently fit —
//! receives a reservation. Lower-priority jobs may run ahead of it only when
//! they provably do not push that reservation later. Conservative backfill,
//! which reserves for every blocked job, is explicitly out of scope.

use crate::{
    Arena, DispatchDecision, Disposition, Job, JobId, PackBudget, ReadyEntry, ResourceRequest,
    Resources, SlotIndex, SlotInventory, VirtualTime,
};

/// A job currently occupying capacity, with the time it must be gone by.
///
/// `ends_at` is the job's *walltime deadline*, not a prediction. Backfill
/// reasons about worst-case occupancy, which is why spec §5.1 makes
/// `walltime_limit` mandatory.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RunningJob {
    pub job: JobId,
    pub slot: SlotIndex,
    pub resources: Resources,
    pub ends_at: VirtualTime,
}

/// Reusable buffers so a backfill pass allocates nothing in steady state.
#[derive(Clone, Debug, Default)]
pub struct BackfillScratch {
    completions: Vec<(VirtualTime, Resources)>,
    running_with_candidate: Vec<RunningJob>,
}

impl BackfillScratch {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            completions: Vec::new(),
            running_with_candidate: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.completions.clear();
        self.running_with_candidate.clear();
    }
}

/// The earliest time `request` can start, assuming every running job occupies
/// its slot until its walltime deadline.
///
/// Returns `now` if it fits already, and `None` if no slot's declared capacity
/// could ever hold it even when completely empty.
///
/// Complexity is O(slots × running). M8 may index running jobs by slot; M1
/// prioritizes a formulation that is obviously correct.
#[must_use]
pub fn reservation_time(
    request: &ResourceRequest,
    inventory: &SlotInventory,
    running: &[RunningJob],
    now: VirtualTime,
    scratch: &mut BackfillScratch,
) -> Option<VirtualTime> {
    let mut earliest: Option<VirtualTime> = None;

    for slot_index in 0..inventory.len() {
        let slot = inventory.slot(slot_index)?;
        if !request.fits_within(&slot.capacity) {
            continue; // This slot can never hold it, however empty it gets.
        }
        if request.fits_within(&slot.free()) {
            return Some(now);
        }

        scratch.completions.clear();
        scratch.completions.extend(
            running
                .iter()
                .filter(|r| r.slot == slot_index)
                .map(|r| (r.ends_at, r.resources)),
        );
        scratch
            .completions
            .sort_unstable_by_key(|(ends_at, _)| *ends_at);

        let mut free = slot.free();
        for (ends_at, resources) in &scratch.completions {
            free = free.saturating_add(resources);
            if request.fits_within(&free) {
                earliest = Some(match earliest {
                    Some(current) => current.min(*ends_at),
                    None => *ends_at,
                });
                break;
            }
        }
    }

    earliest
}

/// What one backfill pass did.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct BackfillOutcome {
    pub considered: usize,
    pub backfilled: usize,
    /// The head job's reservation, or `None` if it can never be satisfied.
    pub reservation: Option<VirtualTime>,
}

/// Runs lower-priority jobs into the gap ahead of the head job's reservation.
///
/// A candidate is admitted when, and only when:
///
/// ```text
/// fits_in_current_free_capacity(candidate)
///   AND ( now + candidate.walltime_limit <= reservation
///         OR the head job's recomputed reservation is no later than before )
/// ```
///
/// Fitting now is necessary; the disjunction is the two ways a candidate can be
/// proven harmless. The second branch is evaluated by tentatively allocating the
/// candidate, recomputing the head's reservation with the candidate counted as
/// running, and rolling back if the reservation slipped. That makes spec §9.1's
/// invariant — backfill never delays the head job — true by construction rather
/// than by argument.
///
/// # Panics
/// If `disposition` is shorter than `candidates`, or `head_index` is out of range.
#[allow(clippy::too_many_arguments)] // Every parameter is caller-owned state a
// pure, allocation-free core must borrow.
pub fn easy_backfill(
    candidates: &[ReadyEntry],
    head_index: usize,
    jobs: &Arena<Job>,
    inventory: &mut SlotInventory,
    running: &[RunningJob],
    now: VirtualTime,
    budget: PackBudget,
    disposition: &mut [Disposition],
    decisions: &mut Vec<DispatchDecision>,
    scratch: &mut BackfillScratch,
) -> BackfillOutcome {
    assert!(
        disposition.len() >= candidates.len(),
        "disposition buffer too short"
    );
    assert!(head_index < candidates.len(), "head_index out of range");

    let mut outcome = BackfillOutcome::default();

    let Some(head_job) = jobs.get(candidates[head_index].job) else {
        return outcome;
    };
    let head_request = head_job.request;

    let reservation = reservation_time(&head_request, inventory, running, now, scratch);
    outcome.reservation = reservation;
    let Some(reservation) = reservation else {
        // The head job can never run on this inventory. Leave it for the
        // eligibility stage to reject in M2 rather than silently backfilling
        // around it forever.
        return outcome;
    };

    for index in (head_index + 1)..candidates.len() {
        if outcome.backfilled >= budget.max_decisions || outcome.considered >= budget.max_scanned {
            break;
        }

        let entry = candidates[index];
        let Some(job) = jobs.get(entry.job) else {
            disposition[index] = Disposition::Dropped;
            continue;
        };
        outcome.considered += 1;

        // Necessary condition: it has to fit right now.
        let Some(slot) = inventory.first_fit(&job.request) else {
            continue;
        };

        let ends_at = now.saturating_add(job.walltime_limit);

        // First branch: it finishes before the head is due to start.
        let harmless = if ends_at <= reservation {
            true
        } else {
            // Second branch: tentatively run it and see whether the head slips.
            inventory
                .try_allocate(slot, &job.request)
                .expect("first_fit just reported this slot has room");

            scratch.running_with_candidate.clear();
            scratch.running_with_candidate.extend_from_slice(running);
            scratch.running_with_candidate.push(RunningJob {
                job: entry.job,
                slot,
                resources: job.request,
                ends_at,
            });

            // `running_with_candidate` and `completions` are distinct buffers,
            // so this reborrow of `scratch` is sound; take the slice first.
            let simulated = core::mem::take(&mut scratch.running_with_candidate);
            let recomputed = reservation_time(&head_request, inventory, &simulated, now, scratch);
            scratch.running_with_candidate = simulated;

            let ok = matches!(recomputed, Some(at) if at <= reservation);
            if !ok {
                inventory
                    .release(slot, &job.request)
                    .expect("releasing what we just allocated");
            }
            ok
        };

        if !harmless {
            continue;
        }

        if ends_at <= reservation {
            // The first branch did not allocate yet.
            inventory
                .try_allocate(slot, &job.request)
                .expect("first_fit just reported this slot has room");
        }

        decisions.push(DispatchDecision {
            job: entry.job,
            slot,
        });
        disposition[index] = Disposition::Dispatched;
        outcome.backfilled += 1;
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AccountId, Arena, Job, JobState, PackBudget, PriorityClass, ReadyEntry, ResourceRequest,
        Resources, SlotInventory, VirtualDuration, VirtualTime,
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

    #[test]
    fn reservation_is_when_enough_running_jobs_have_finished() {
        // One slot of 4000 millis, fully occupied by two jobs ending at 10s and 30s.
        let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
        inv.try_allocate(0, &Resources::new(2_000, 0, 0)).unwrap();
        inv.try_allocate(0, &Resources::new(2_000, 0, 0)).unwrap();

        let running = [
            RunningJob {
                job: crate::JobId::from_bits(0),
                slot: 0,
                resources: Resources::new(2_000, 0, 0),
                ends_at: VirtualTime::from_nanos(10 * SECOND),
            },
            RunningJob {
                job: crate::JobId::from_bits(1),
                slot: 0,
                resources: Resources::new(2_000, 0, 0),
                ends_at: VirtualTime::from_nanos(30 * SECOND),
            },
        ];

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

        let running = [
            RunningJob {
                job: crate::JobId::from_bits(0),
                slot: 0,
                resources: Resources::new(4_000, 0, 0),
                ends_at: VirtualTime::from_nanos(50 * SECOND),
            },
            RunningJob {
                job: crate::JobId::from_bits(1),
                slot: 1,
                resources: Resources::new(4_000, 0, 0),
                ends_at: VirtualTime::from_nanos(20 * SECOND),
            },
        ];

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

    #[test]
    fn a_short_job_backfills_into_the_hole() {
        // Slot: 4000. Running job holds 3000 until 100s. Head needs 4000, so it
        // waits until 100s. A 1000-milli job with a 10s walltime finishes well
        // before the reservation, so it must be admitted.
        let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
        inv.try_allocate(0, &Resources::new(3_000, 0, 0)).unwrap();

        let running = [RunningJob {
            job: crate::JobId::from_bits(99),
            slot: 0,
            resources: Resources::new(3_000, 0, 0),
            ends_at: VirtualTime::from_nanos(100 * SECOND),
        }];

        let mut arena = Arena::new();
        let head = arena.insert(ready_job(4_000, 500));
        let filler = arena.insert(ready_job(1_000, 10));
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
        // Same shape, but the filler runs for 500s — past the head's 100s
        // reservation — and would still be holding capacity when the head is
        // due to start.
        let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
        inv.try_allocate(0, &Resources::new(3_000, 0, 0)).unwrap();

        let running = [RunningJob {
            job: crate::JobId::from_bits(99),
            slot: 0,
            resources: Resources::new(3_000, 0, 0),
            ends_at: VirtualTime::from_nanos(100 * SECOND),
        }];

        let mut arena = Arena::new();
        let head = arena.insert(ready_job(4_000, 500));
        let hog = arena.insert(ready_job(1_000, 500));
        let candidates = vec![
            ReadyEntry {
                class: PriorityClass::Normal,
                job: head,
                score: 100,
            },
            ReadyEntry {
                class: PriorityClass::Normal,
                job: hog,
                score: 1,
            },
        ];

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
        let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
        inv.try_allocate(0, &Resources::new(3_000, 0, 0)).unwrap();

        let running = [RunningJob {
            job: crate::JobId::from_bits(99),
            slot: 0,
            resources: Resources::new(3_000, 0, 0),
            ends_at: VirtualTime::from_nanos(100 * SECOND),
        }];

        let mut arena = Arena::new();
        let head = arena.insert(ready_job(4_000, 500));
        let too_big = arena.insert(ready_job(2_000, 1));
        let candidates = vec![
            ReadyEntry {
                class: PriorityClass::Normal,
                job: head,
                score: 100,
            },
            ReadyEntry {
                class: PriorityClass::Normal,
                job: too_big,
                score: 1,
            },
        ];

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
}
