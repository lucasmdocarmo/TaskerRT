//! The scheduling cycle: rescore → order → pack → backfill → emit, plus the
//! job lifecycle calls that feed it. Every stage is bounded so a single tick
//! has predictable cost.

use crate::cluster::SlotInventory;
use crate::domain::{
    AccountId, Arena, Job, JobId, JobState, PriorityClass, TransitionError, VirtualTime,
};
use crate::policy::{
    AccountError, BackfillScratch, DependencyError, DependencyTracker, DispatchDecision,
    Disposition, FairShare, FairShareConfig, Ledger, MAX_ACCOUNTS, OrderKey, PackBudget,
    PreemptConfig, PriorityConfig, Readiness, ReadyEntry, ReadySet, RunningJob, easy_backfill,
    pack, score,
};

/// Everything a cycle needs that is not per-job state.
#[derive(Clone, Copy, Debug)]
pub struct CycleConfig {
    pub priority: PriorityConfig,
    pub fairshare: FairShareConfig,
    pub preempt: PreemptConfig,
    pub budget: PackBudget,
    /// Upper bound on jobs examined per cycle.
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

/// A lifecycle call could not be applied.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("no live job with handle {0:?}")]
    UnknownJob(JobId),
    #[error("job {job:?} is {state:?}, not Submitted")]
    NotSubmitted { job: JobId, state: JobState },
    // `#[from]` generates `From<DependencyError>`, which is what lets `?` convert it.
    #[error(transparent)]
    Dependency(#[from] DependencyError),
    #[error(transparent)]
    Account(#[from] AccountError),
    #[error(transparent)]
    Transition(#[from] TransitionError),
}

/// Owns the ready set, the dependency tracker, and the scratch buffers reused
/// across cycles.
#[derive(Clone, Debug, Default)]
pub struct Scheduler {
    ready: ReadySet,
    deps: DependencyTracker,
    fairshare: FairShare,
    candidates: Vec<ReadyEntry>,
    disposition: Vec<Disposition>,
    backfill_scratch: BackfillScratch,
}

impl Scheduler {
    /// An empty scheduler.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty scheduler preallocated for `slots` arena indices.
    #[must_use]
    pub fn with_slots(slots: usize) -> Self {
        let mut scheduler = Self::new();
        scheduler.ready.reserve_slots(slots);
        scheduler.deps.reserve_slots(slots);
        scheduler.candidates.reserve(slots);
        scheduler.disposition.reserve(slots);
        scheduler
    }

    /// Jobs waiting in the ready set.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.ready.len()
    }

    /// Removes a job from the ready set without touching its state. Prefer
    /// `cancel`, which also cascades.
    pub fn withdraw(&mut self, class: PriorityClass, id: JobId) -> bool {
        self.ready.remove(class, id)
    }

    /// Sets an account's fair-share weight (default 1), creating its ledger if needed.
    ///
    /// # Errors
    /// `Account` when the id is not below `MAX_ACCOUNTS`.
    pub fn set_shares(&mut self, account: AccountId, shares: u32) -> Result<(), LifecycleError> {
        self.fairshare.set_shares(account, shares)?;
        Ok(())
    }

    /// Per-account ledgers and factors, as of the last cycle.
    #[must_use]
    pub const fn fairshare(&self) -> &FairShare {
        &self.fairshare
    }

    /// Validates a submit for the job that `id` (from `Arena::next_id`) will
    /// become, touching nothing. Lets a caller insert only what it will accept.
    ///
    /// # Errors
    /// `Account` or `Dependency`, exactly as `submit` would fail.
    pub fn check_submit(
        &self,
        id: JobId,
        job: &Job,
        jobs: &Arena<Job>,
    ) -> Result<Readiness, LifecycleError> {
        if job.account.get() >= MAX_ACCOUNTS {
            return Err(AccountError(job.account).into());
        }
        Ok(self.deps.classify(id, job, jobs)?)
    }

    /// Replays a dispatch: `Ready` to `Running` without a cycle, charging the
    /// account from `now`.
    ///
    /// # Errors
    /// `UnknownJob`, or `Transition` if `id` was not `Ready`.
    pub fn restore_running(
        &mut self,
        id: JobId,
        jobs: &mut Arena<Job>,
        now: VirtualTime,
        config: &CycleConfig,
    ) -> Result<(), LifecycleError> {
        let job = jobs.get_mut(id).ok_or(LifecycleError::UnknownJob(id))?;
        // Removing is a no-op when absent; the transition below is the real check.
        self.ready.remove(job.priority_class, id);
        job.try_transition(JobState::Running)?;
        self.fairshare
            .on_dispatch(job.account, job.request.cpu_millis, now, &config.fairshare);
        Ok(())
    }

    /// Marks a running job as being evicted. Its cpu stays charged and its
    /// capacity stays allocated until the worker reports that it stopped.
    ///
    /// # Errors
    /// `UnknownJob`, or `Transition` if `id` was not `Running`.
    pub fn preempt(&mut self, id: JobId, jobs: &mut Arena<Job>) -> Result<(), LifecycleError> {
        let job = jobs.get_mut(id).ok_or(LifecycleError::UnknownJob(id))?;
        job.try_transition(JobState::Preempted)?;
        Ok(())
    }

    /// The worker confirmed the eviction: release the account, count it, requeue.
    ///
    /// # Errors
    /// `UnknownJob`, or `Transition` if `id` was not `Preempted`.
    pub fn on_preempted(
        &mut self,
        id: JobId,
        jobs: &mut Arena<Job>,
        now: VirtualTime,
        config: &CycleConfig,
    ) -> Result<(), LifecycleError> {
        let job = jobs.get_mut(id).ok_or(LifecycleError::UnknownJob(id))?;
        if job.state != JobState::Preempted {
            return Err(TransitionError {
                from: job.state,
                to: JobState::Ready,
            }
            .into());
        }
        job.preemptions = job.preemptions.saturating_add(1);
        self.requeue(id, jobs, now, config)
    }

    /// Replaces every fair-share ledger with a snapshot's.
    ///
    /// # Errors
    /// `Account` when there are more ledgers than `MAX_ACCOUNTS`.
    pub fn restore_ledgers(&mut self, ledgers: Vec<Ledger>) -> Result<(), LifecycleError> {
        self.fairshare.restore(ledgers)?;
        Ok(())
    }

    /// Classifies a `Submitted` job as `Ready`, `Blocked`, or `Cancelled` from
    /// its dependencies, applies the transition, and enqueues it if `Ready`.
    ///
    /// # Errors
    /// `UnknownJob`, `NotSubmitted`, or `Dependency` when a dep does not resolve.
    pub fn submit(
        &mut self,
        id: JobId,
        jobs: &mut Arena<Job>,
        now: VirtualTime,
        config: &CycleConfig,
    ) -> Result<JobState, LifecycleError> {
        let job = jobs.get(id).ok_or(LifecycleError::UnknownJob(id))?;
        if job.state != JobState::Submitted {
            return Err(LifecycleError::NotSubmitted {
                job: id,
                state: job.state,
            });
        }
        // Grow the ledger table here, never in a cycle: this is the one allocation site.
        self.fairshare.ensure(job.account)?;
        // `?` converts `DependencyError` into `LifecycleError` via the `#[from]` impl.
        let next = match self.deps.register(id, job, jobs)? {
            Readiness::Ready => JobState::Ready,
            Readiness::Blocked => JobState::Blocked,
            Readiness::Cancelled => JobState::Cancelled,
        };
        // The shared borrow `job` ended at its last use (NLL), so a mutable
        // borrow of the same arena is allowed here.
        let job = jobs.get_mut(id).expect("checked live above");
        job.try_transition(next)?;
        if next == JobState::Ready {
            self.ready.insert(
                job.priority_class,
                id,
                score(
                    job,
                    now,
                    &config.priority,
                    self.fairshare.factor(job.account),
                ),
            );
        }
        Ok(next)
    }

    /// Marks `id` `Completed` and promotes every dependent whose last
    /// dependency this was. `promoted` is cleared, then filled.
    ///
    /// # Errors
    /// `UnknownJob`, or `Transition` if `id` was not `Running`.
    pub fn on_completed(
        &mut self,
        id: JobId,
        jobs: &mut Arena<Job>,
        now: VirtualTime,
        config: &CycleConfig,
        promoted: &mut Vec<JobId>,
    ) -> Result<(), LifecycleError> {
        let job = jobs.get_mut(id).ok_or(LifecycleError::UnknownJob(id))?;
        job.try_transition(JobState::Completed)?;
        self.fairshare
            .on_release(job.account, job.request.cpu_millis, now, &config.fairshare);
        promoted.clear();
        self.deps.on_completed(id, promoted);
        for &dependent in promoted.iter() {
            let Some(job) = jobs.get_mut(dependent) else {
                continue;
            };
            // Only a Blocked job can move to Ready; anything else is left alone.
            if job.try_transition(JobState::Ready).is_ok() {
                self.ready.insert(
                    job.priority_class,
                    dependent,
                    score(
                        job,
                        now,
                        &config.priority,
                        self.fairshare.factor(job.account),
                    ),
                );
            }
        }
        Ok(())
    }

    /// Marks `id` `Failed` and cancels every transitive dependent.
    /// `cancelled` is cleared, then filled. The caller releases slot capacity.
    ///
    /// # Errors
    /// `UnknownJob`, or `Transition` if `id` was not `Running`.
    pub fn on_failed(
        &mut self,
        id: JobId,
        jobs: &mut Arena<Job>,
        now: VirtualTime,
        config: &CycleConfig,
        cancelled: &mut Vec<JobId>,
    ) -> Result<(), LifecycleError> {
        let job = jobs.get_mut(id).ok_or(LifecycleError::UnknownJob(id))?;
        job.try_transition(JobState::Failed)?;
        self.fairshare
            .on_release(job.account, job.request.cpu_millis, now, &config.fairshare);
        cancelled.clear();
        self.deps.on_terminated(id, cancelled);
        self.apply_cascade(jobs, cancelled);
        Ok(())
    }

    /// Cancels `id` from `Submitted`, `Blocked`, `Ready`, or `Running`, and
    /// cascades. `cancelled` holds the dependents, not `id` itself. If `id` was
    /// `Running`, the caller releases its slot capacity.
    ///
    /// # Errors
    /// `UnknownJob`, or `Transition` if `id` is already terminal.
    pub fn cancel(
        &mut self,
        id: JobId,
        jobs: &mut Arena<Job>,
        now: VirtualTime,
        config: &CycleConfig,
        cancelled: &mut Vec<JobId>,
    ) -> Result<(), LifecycleError> {
        let job = jobs.get_mut(id).ok_or(LifecycleError::UnknownJob(id))?;
        if job.state == JobState::Ready {
            self.ready.remove(job.priority_class, id);
        }
        let was_running = matches!(job.state, JobState::Running | JobState::Preempted);
        job.try_transition(JobState::Cancelled)?;
        if was_running {
            self.fairshare
                .on_release(job.account, job.request.cpu_millis, now, &config.fairshare);
        }
        cancelled.clear();
        self.deps.on_terminated(id, cancelled);
        self.apply_cascade(jobs, cancelled);
        Ok(())
    }

    /// Returns a `Running` job to the ready set through `Preempted` — the edge
    /// for a job evicted through no fault of its own, e.g. its worker died.
    /// The caller releases slot capacity.
    ///
    /// # Errors
    /// `UnknownJob`, or `Transition` if `id` was not `Running`.
    pub fn requeue(
        &mut self,
        id: JobId,
        jobs: &mut Arena<Job>,
        now: VirtualTime,
        config: &CycleConfig,
    ) -> Result<(), LifecycleError> {
        let job = jobs.get_mut(id).ok_or(LifecycleError::UnknownJob(id))?;
        // A job already mid-eviction is Preempted; a lost worker's job is Running.
        if job.state == JobState::Running {
            job.try_transition(JobState::Preempted)?;
        }
        job.try_transition(JobState::Ready)?;
        // Either way the job held its cpu until now.
        self.fairshare
            .on_release(job.account, job.request.cpu_millis, now, &config.fairshare);
        self.ready.insert(
            job.priority_class,
            id,
            score(
                job,
                now,
                &config.priority,
                self.fairshare.factor(job.account),
            ),
        );
        Ok(())
    }

    /// Transitions each cascaded job to `Cancelled`, pulling it from the ready
    /// set first if it was there.
    fn apply_cascade(&mut self, jobs: &mut Arena<Job>, cancelled: &[JobId]) {
        for &dependent in cancelled {
            let Some(job) = jobs.get_mut(dependent) else {
                continue;
            };
            if job.state == JobState::Ready {
                self.ready.remove(job.priority_class, dependent);
            }
            // A dependent of a terminated job is Blocked or Ready, never
            // terminal; `let _ =` discards the impossible Err rather than panic.
            let _ = job.try_transition(JobState::Cancelled);
        }
    }

    /// Runs one cycle, appending dispatch decisions to `decisions`.
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

        // Ledgers and factors first, so both the early return and rescoring see current values.
        self.fairshare.refresh(now, &config.fairshare);

        // Order: take a bounded slice of the ready set into the scratch buffer.
        self.ready
            .drain_ordered_into(&mut self.candidates, config.max_candidates);
        if self.candidates.is_empty() {
            return outcome;
        }

        // Rescore: stored scores are stale because every job has aged since admission.
        // `&mut self.candidates` yields `&mut ReadyEntry` so `entry.score` can be written.
        for entry in &mut self.candidates {
            if let Some(job) = jobs.get(entry.job) {
                entry.score = score(
                    job,
                    now,
                    &config.priority,
                    self.fairshare.factor(job.account),
                );
            }
        }
        // Re-sort descending: class first, then the total order key.
        // `b.cmp(&a)` (not `a.cmp(&b)`) is what makes the sort descending.
        self.candidates.sort_unstable_by(|a, b| {
            b.class
                .cmp(&a.class)
                // `then_with` is evaluated only when the classes tie.
                .then_with(|| OrderKey::new(b.score, b.job).cmp(&OrderKey::new(a.score, a.job)))
        });

        outcome.candidates = self.candidates.len();
        self.disposition.clear();
        self.disposition
            .resize(self.candidates.len(), Disposition::Pending);

        // Pack. Borrowing `&self.candidates` and `&mut self.disposition` together is
        // allowed because they are distinct fields.
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

        // Backfill, only when packing stopped at a head job.
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

        // Emit: apply state changes and return the untouched entries.
        for (index, entry) in self.candidates.iter().enumerate() {
            match self.disposition[index] {
                Disposition::Dispatched => {
                    if let Some(job) = jobs.get_mut(entry.job) {
                        job.try_transition(JobState::Running)
                            .expect("a dispatched job was Ready");
                        self.fairshare.on_dispatch(
                            job.account,
                            job.request.cpu_millis,
                            now,
                            &config.fairshare,
                        );
                    }
                }
                // `*entry` copies the `ReadyEntry` out of the reference.
                Disposition::Pending => self.ready.reinsert(*entry),
                // A dropped entry simply leaves the system.
                Disposition::Dropped => {}
            }
        }

        outcome
    }
}
