//! Greedy resource-aware packing.

use crate::cluster::{SlotIndex, SlotInventory};
use crate::domain::{Arena, Job, JobId};
use crate::policy::ReadyEntry;

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
    /// Placed on a slot; now running.
    Dispatched,
    /// Stale handle; discard it.
    Dropped,
}

/// Per-cycle work limits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PackBudget {
    pub max_decisions: usize,
    pub max_scanned: usize,
}

impl Default for PackBudget {
    /// Unlimited.
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
    /// Index into `candidates` of the first job that did not fit (the head job).
    pub head: Option<usize>,
    pub budget_exhausted: bool,
}

/// Walks `candidates` in order, allocating each job that fits, and stops at the
/// first that does not — the head job, which backfill then reserves for.
/// Panics if `disposition` is shorter than `candidates`.
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

    // `enumerate` gives each candidate its index so `disposition` can be written by position.
    for (index, entry) in candidates.iter().enumerate() {
        if outcome.dispatched >= budget.max_decisions || outcome.scanned >= budget.max_scanned {
            outcome.budget_exhausted = true;
            return outcome;
        }

        // A stale handle means the job was cancelled or finished after queueing.
        let Some(job) = jobs.get(entry.job) else {
            disposition[index] = Disposition::Dropped;
            continue;
        };

        outcome.scanned += 1;

        // No slot has room: this is the head job. Stop rather than starve it.
        let Some(slot) = inventory.first_fit(&job.request) else {
            outcome.head = Some(index);
            return outcome;
        };
        // `expect` documents why this cannot fail: `first_fit` just checked.
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
