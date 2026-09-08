//! EASY backfill: only the head job gets a reservation; lower-priority jobs
//! may run ahead of it only if they provably do not push that reservation later.

use crate::cluster::{SlotIndex, SlotInventory};
use crate::domain::{Arena, Job, JobId, ResourceRequest, Resources, VirtualTime};
use crate::policy::{DispatchDecision, Disposition, PackBudget, ReadyEntry};

/// A job occupying capacity until its walltime deadline (`ends_at`).
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
    /// Empty buffers.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            completions: Vec::new(),
            running_with_candidate: Vec::new(),
        }
    }

    /// Empties both buffers, keeping their allocations.
    pub fn clear(&mut self) {
        self.completions.clear();
        self.running_with_candidate.clear();
    }
}

/// Earliest time `request` can start if every running job holds its slot until
/// its deadline. `Some(now)` if it fits already; `None` if no slot could ever hold it.
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
        // `continue` skips to the next slot; this one can never hold the request.
        if !request.fits_within(&slot.capacity) {
            continue;
        }
        if request.fits_within(&slot.free()) {
            return Some(now);
        }

        // Collect this slot's completions, earliest first.
        scratch.completions.clear();
        scratch.completions.extend(
            running
                .iter()
                .filter(|r| r.slot == slot_index)
                .map(|r| (r.ends_at, r.resources)),
        );
        // `sort_unstable_by_key` is faster than `sort_by_key` and order among equals is irrelevant.
        scratch
            .completions
            .sort_unstable_by_key(|(ends_at, _)| *ends_at);

        // Replay completions, accumulating freed capacity until the request fits.
        let mut free = slot.free();
        for (ends_at, resources) in &scratch.completions {
            free = free.saturating_add(resources);
            if request.fits_within(&free) {
                // Keep the minimum across slots; `match` handles the first-seen case.
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
/// A candidate is admitted iff it fits now AND (it finishes before the reservation
/// OR tentatively running it leaves the head's reservation no later than before).
#[allow(clippy::too_many_arguments)] // all caller-owned state a pure function must borrow
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
    // No reservation means the head can never run here; do not backfill around it forever.
    let Some(reservation) = reservation else {
        return outcome;
    };

    // Only jobs *after* the head are backfill candidates.
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

        // Necessary condition: it must fit right now.
        let Some(slot) = inventory.first_fit(&job.request) else {
            continue;
        };

        let ends_at = now.saturating_add(job.walltime_limit);

        // An `if` expression yields a value; both branches produce a `bool`.
        let harmless = if ends_at <= reservation {
            // Branch 1: it finishes before the head is due to start.
            true
        } else {
            // Branch 2: allocate tentatively, recompute the head's reservation, roll back if it slipped.
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

            // `mem::take` moves the Vec out (leaving an empty one) so `scratch` can be
            // reborrowed mutably by `reservation_time` without a borrow conflict.
            let simulated = core::mem::take(&mut scratch.running_with_candidate);
            let recomputed = reservation_time(&head_request, inventory, &simulated, now, scratch);
            scratch.running_with_candidate = simulated;

            // `matches!` with a guard: true only if `Some` and not later than before.
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

        // Branch 1 has not allocated yet; branch 2 already did.
        if ends_at <= reservation {
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
