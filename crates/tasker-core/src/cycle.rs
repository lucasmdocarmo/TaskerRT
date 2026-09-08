//! The scheduling cycle (spec §6).
//!
//! M1 implements steps 3 through 7: priority, order, pack, backfill, emit.
//! Step 1 (ingest drain) arrives with the lock-free ring in M3 and step 2
//! (dependency eligibility) with the DAG in M2.
//!
//! Every stage is bounded. A cycle that cannot finish its scan within budget
//! stops and resumes on the next tick — spec §6 prefers a predictable tail to
//! a globally optimal single decision.

use crate::{
    Arena, BackfillScratch, DispatchDecision, Disposition, Job, JobId, JobState, OrderKey,
    PackBudget, PriorityClass, PriorityConfig, ReadyEntry, ReadySet, RunningJob, SlotInventory,
    VirtualTime, easy_backfill, pack, score,
};

/// Everything a cycle needs that is not per-job state.
#[derive(Clone, Copy, Debug)]
pub struct CycleConfig {
    pub priority: PriorityConfig,
    pub budget: PackBudget,
    /// Upper bound on jobs examined per cycle. This is the ordering stage's
    /// share of the per-cycle work budget.
    pub max_candidates: usize,
}

/// What one cycle did.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct CycleOutcome {
    pub candidates: usize,
    pub dispatched: usize,
    pub backfilled: usize,
    /// The highest-priority job that did not fit, if any.
    pub head: Option<JobId>,
    pub reservation: Option<VirtualTime>,
    pub budget_exhausted: bool,
}

/// Owns the ready set and the per-cycle scratch buffers.
///
/// Allocation happens when the buffers first grow; steady-state cycles reuse
/// them, which is what keeps the hot path allocation-free (spec §7.2).
#[derive(Clone, Debug, Default)]
pub struct Scheduler {
    ready: ReadySet,
    candidates: Vec<ReadyEntry>,
    disposition: Vec<Disposition>,
    backfill_scratch: BackfillScratch,
}

impl Scheduler {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Preallocates for `slots` arena indices.
    #[must_use]
    pub fn with_slots(slots: usize) -> Self {
        let mut scheduler = Self::new();
        scheduler.ready.reserve_slots(slots);
        scheduler.candidates.reserve(slots);
        scheduler.disposition.reserve(slots);
        scheduler
    }

    /// Number of jobs waiting in the ready set.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.ready.len()
    }

    /// Adds a `Ready` job to the ready set at its current score.
    pub fn admit(&mut self, id: JobId, job: &Job, now: VirtualTime, config: &CycleConfig) {
        self.ready
            .insert(job.priority_class, id, score(job, now, &config.priority));
    }

    /// Removes a job from the ready set, e.g. on cancellation.
    pub fn withdraw(&mut self, class: PriorityClass, id: JobId) -> bool {
        self.ready.remove(class, id)
    }

    /// Runs one scheduling cycle, appending decisions to `decisions`.
    pub fn run_cycle(
        &mut self,
        jobs: &mut Arena<Job>,
        inventory: &mut SlotInventory,
        running: &[RunningJob],
        now: VirtualTime,
        config: &CycleConfig,
        decisions: &mut Vec<DispatchDecision>,
    ) -> CycleOutcome {
        let mut outcome = CycleOutcome::default();

        // Stage 4 — order. Take a bounded slice of the ready set.
        self.ready
            .drain_ordered_into(&mut self.candidates, config.max_candidates);
        if self.candidates.is_empty() {
            return outcome;
        }

        // Stage 3 — priority. Rescore against the current clock: a job's age has
        // advanced since it was queued, and the stored score is stale.
        for entry in &mut self.candidates {
            if let Some(job) = jobs.get(entry.job) {
                entry.score = score(job, now, &config.priority);
            }
        }
        // Rescoring can reorder within a class, so re-sort the slice. Class
        // still dominates: sort by (class, key) descending.
        self.candidates.sort_unstable_by(|a, b| {
            b.class
                .cmp(&a.class)
                .then_with(|| OrderKey::new(b.score, b.job).cmp(&OrderKey::new(a.score, a.job)))
        });

        outcome.candidates = self.candidates.len();
        self.disposition.clear();
        self.disposition
            .resize(self.candidates.len(), Disposition::Pending);

        // Stage 5 — pack.
        let pack_outcome = pack(
            &self.candidates,
            jobs,
            inventory,
            config.budget,
            &mut self.disposition,
            decisions,
        );
        outcome.dispatched = pack_outcome.dispatched;
        outcome.budget_exhausted = pack_outcome.budget_exhausted;

        // Stage 6 — EASY backfill, only when a head job exists.
        if let Some(head_index) = pack_outcome.head {
            outcome.head = Some(self.candidates[head_index].job);
            let backfill_outcome = easy_backfill(
                &self.candidates,
                head_index,
                jobs,
                inventory,
                running,
                now,
                config.budget,
                &mut self.disposition,
                decisions,
                &mut self.backfill_scratch,
            );
            outcome.backfilled = backfill_outcome.backfilled;
            outcome.reservation = backfill_outcome.reservation;
        }

        // Stage 7 — emit. Apply state transitions and return the rest.
        for (index, entry) in self.candidates.iter().enumerate() {
            match self.disposition[index] {
                Disposition::Dispatched => {
                    if let Some(job) = jobs.get_mut(entry.job) {
                        job.try_transition(JobState::Running)
                            .expect("a dispatched job was Ready");
                    }
                }
                Disposition::Pending => self.ready.reinsert(*entry),
                Disposition::Dropped => {}
            }
        }

        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AccountId, Arena, Job, JobState, PriorityWeights, ResourceRequest, Resources,
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

    fn ready_job(cpu: u32, class: PriorityClass, walltime_secs: u64) -> Job {
        let mut job = Job::new(
            AccountId::new(0),
            class,
            VirtualTime::ZERO,
            ResourceRequest::new(cpu, 0, 0),
            VirtualDuration::from_secs(walltime_secs),
        );
        job.try_transition(JobState::Ready).unwrap();
        job
    }

    #[test]
    fn a_cycle_dispatches_what_fits_and_marks_jobs_running() {
        let cfg = config();
        let mut jobs = Arena::new();
        let mut inventory = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
        let mut scheduler = Scheduler::with_slots(16);

        let a = jobs.insert(ready_job(1_000, PriorityClass::Normal, 60));
        let b = jobs.insert(ready_job(1_000, PriorityClass::Normal, 60));
        scheduler.admit(a, jobs.get(a).unwrap(), VirtualTime::ZERO, &cfg);
        scheduler.admit(b, jobs.get(b).unwrap(), VirtualTime::ZERO, &cfg);
        assert_eq!(scheduler.pending(), 2);

        let mut decisions = Vec::new();
        let outcome = scheduler.run_cycle(
            &mut jobs,
            &mut inventory,
            &[],
            VirtualTime::ZERO,
            &cfg,
            &mut decisions,
        );

        assert_eq!(outcome.dispatched, 2);
        assert_eq!(decisions.len(), 2);
        assert_eq!(scheduler.pending(), 0);
        assert_eq!(jobs.get(a).unwrap().state, JobState::Running);
        assert_eq!(jobs.get(b).unwrap().state, JobState::Running);
    }

    #[test]
    fn a_job_that_does_not_fit_stays_pending_and_is_reported_as_head() {
        let cfg = config();
        let mut jobs = Arena::new();
        let mut inventory = SlotInventory::from_uniform(1, Resources::new(1_000, 0, 0));
        let mut scheduler = Scheduler::with_slots(16);

        let big = jobs.insert(ready_job(4_000, PriorityClass::Urgent, 60));
        scheduler.admit(big, jobs.get(big).unwrap(), VirtualTime::ZERO, &cfg);

        let mut decisions = Vec::new();
        let outcome = scheduler.run_cycle(
            &mut jobs,
            &mut inventory,
            &[],
            VirtualTime::ZERO,
            &cfg,
            &mut decisions,
        );

        assert_eq!(outcome.dispatched, 0);
        assert_eq!(outcome.head, Some(big));
        assert_eq!(
            scheduler.pending(),
            1,
            "the head job returns to the ready set"
        );
        assert_eq!(jobs.get(big).unwrap().state, JobState::Ready);
    }

    #[test]
    fn class_order_is_respected_across_cycles() {
        let cfg = config();
        let mut jobs = Arena::new();
        let mut inventory = SlotInventory::from_uniform(1, Resources::new(1_000, 0, 0));
        let mut scheduler = Scheduler::with_slots(16);

        let low = jobs.insert(ready_job(1_000, PriorityClass::Low, 60));
        let urgent = jobs.insert(ready_job(1_000, PriorityClass::Urgent, 60));
        scheduler.admit(low, jobs.get(low).unwrap(), VirtualTime::ZERO, &cfg);
        scheduler.admit(urgent, jobs.get(urgent).unwrap(), VirtualTime::ZERO, &cfg);

        let mut decisions = Vec::new();
        scheduler.run_cycle(
            &mut jobs,
            &mut inventory,
            &[],
            VirtualTime::ZERO,
            &cfg,
            &mut decisions,
        );

        assert_eq!(decisions.len(), 1);
        assert_eq!(
            decisions[0].job, urgent,
            "Urgent outranks Low regardless of score"
        );
        assert_eq!(scheduler.pending(), 1);
    }

    #[test]
    fn a_cancelled_job_is_dropped_from_the_ready_set() {
        let cfg = config();
        let mut jobs = Arena::new();
        let mut inventory = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
        let mut scheduler = Scheduler::with_slots(16);

        let doomed = jobs.insert(ready_job(1_000, PriorityClass::Normal, 60));
        scheduler.admit(doomed, jobs.get(doomed).unwrap(), VirtualTime::ZERO, &cfg);
        jobs.remove(doomed);

        let mut decisions = Vec::new();
        let outcome = scheduler.run_cycle(
            &mut jobs,
            &mut inventory,
            &[],
            VirtualTime::ZERO,
            &cfg,
            &mut decisions,
        );

        assert_eq!(outcome.dispatched, 0);
        assert_eq!(scheduler.pending(), 0, "the stale entry is not reinserted");
    }

    #[test]
    fn the_candidate_limit_bounds_the_cycle() {
        let mut cfg = config();
        cfg.max_candidates = 2;
        let mut jobs = Arena::new();
        let mut inventory = SlotInventory::from_uniform(1, Resources::new(400_000, 0, 0));
        let mut scheduler = Scheduler::with_slots(64);

        for _ in 0..10 {
            let id = jobs.insert(ready_job(100, PriorityClass::Normal, 60));
            scheduler.admit(id, jobs.get(id).unwrap(), VirtualTime::ZERO, &cfg);
        }

        let mut decisions = Vec::new();
        let outcome = scheduler.run_cycle(
            &mut jobs,
            &mut inventory,
            &[],
            VirtualTime::ZERO,
            &cfg,
            &mut decisions,
        );

        assert_eq!(outcome.candidates, 2);
        assert_eq!(outcome.dispatched, 2);
        assert_eq!(scheduler.pending(), 8, "the rest wait for the next tick");
    }
}
