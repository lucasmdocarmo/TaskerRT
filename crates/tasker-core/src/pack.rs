//! Greedy resource-aware packing (spec §6 step 5).

use crate::{Arena, Job, JobId, ReadyEntry, SlotIndex, SlotInventory};

/// One job placed on one slot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DispatchDecision {
    pub job: JobId,
    pub slot: SlotIndex,
}

/// What the scheduler decided about a candidate this cycle.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Disposition {
    /// Not scheduled; return it to the ready set.
    #[default]
    Pending,
    /// Placed on a slot; it is now running.
    Dispatched,
    /// The handle was stale. Discard it — do not return it to the ready set.
    Dropped,
}

/// Per-cycle work limits (spec §6: "work per cycle is bounded at every stage").
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PackBudget {
    pub max_decisions: usize,
    pub max_scanned: usize,
}

impl Default for PackBudget {
    fn default() -> Self {
        Self {
            max_decisions: usize::MAX,
            max_scanned: usize::MAX,
        }
    }
}

/// What one packing pass did.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct PackOutcome {
    pub scanned: usize,
    pub dispatched: usize,
    /// Index into `candidates` of the first job that did not fit — the *head
    /// job*, the one backfill reserves for. `None` when everything fit or the
    /// budget ran out first.
    pub head: Option<usize>,
    pub budget_exhausted: bool,
}

/// Walks `candidates` in priority order, allocating each job that fits.
///
/// Stops at the first job that does not fit and reports it as the head. This is
/// deliberate: continuing past it would starve it, which is the whole reason
/// EASY backfill exists as a separate, reservation-aware pass.
///
/// `disposition` must be at least `candidates.len()` long; the caller reuses it
/// across cycles so this allocates nothing.
///
/// # Panics
/// If `disposition` is shorter than `candidates`.
pub fn pack(
    candidates: &[ReadyEntry],
    jobs: &Arena<Job>,
    inventory: &mut SlotInventory,
    budget: PackBudget,
    disposition: &mut [Disposition],
    decisions: &mut Vec<DispatchDecision>,
) -> PackOutcome {
    assert!(
        disposition.len() >= candidates.len(),
        "disposition buffer is shorter than the candidate list"
    );

    let mut outcome = PackOutcome::default();

    for (index, entry) in candidates.iter().enumerate() {
        if outcome.dispatched >= budget.max_decisions || outcome.scanned >= budget.max_scanned {
            outcome.budget_exhausted = true;
            return outcome;
        }

        let Some(job) = jobs.get(entry.job) else {
            // The job was cancelled or completed after being queued. Drop the
            // stale entry rather than resurrecting it.
            disposition[index] = Disposition::Dropped;
            continue;
        };

        outcome.scanned += 1;

        let Some(slot) = inventory.first_fit(&job.request) else {
            outcome.head = Some(index);
            return outcome;
        };
        inventory
            .try_allocate(slot, &job.request)
            .expect("first_fit just reported this slot has room");
        decisions.push(DispatchDecision {
            job: entry.job,
            slot,
        });
        disposition[index] = Disposition::Dispatched;
        outcome.dispatched += 1;
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AccountId, Arena, Job, JobState, PriorityClass, ReadyEntry, ResourceRequest, Resources,
        SlotInventory, VirtualDuration, VirtualTime,
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
}
