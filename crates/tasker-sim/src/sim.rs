//! The simulation driver: advance the clock one tick, apply due events, run a
//! cycle, schedule the fates of whatever was dispatched, record everything.

use std::mem;

use tasker_core::{
    Arena, CycleConfig, DispatchDecision, Eviction, Job, JobId, JobState, LifecycleError,
    RunningJob, Scheduler, SlotInventory, VirtualDuration, VirtualTime, plan_preemption,
};

use crate::invariants::{self, Violation};
use crate::{Action, Event, EventQueue, Outcome, Trace, TraceRecord, VirtualClock};

/// The simulation could not continue.
#[derive(Debug, thiserror::Error)]
pub enum SimError {
    #[error("submit index {index} refers to a job that has not been submitted yet")]
    ForwardDependency { index: usize },
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Invariant(#[from] Violation),
}

/// What one tick did.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct StepReport {
    pub at: VirtualTime,
    pub events_applied: usize,
    pub dispatched: usize,
    pub backfilled: usize,
    pub preempted: usize,
}

/// A complete simulated system. Public fields are read by the invariant
/// checker and by tests; nothing outside this crate should mutate them.
#[derive(Debug)]
pub struct Simulation {
    clock: VirtualClock,
    queue: EventQueue,
    pub jobs: Arena<Job>,
    pub inventory: SlotInventory,
    pub running: Vec<RunningJob>,
    pub scheduler: Scheduler,
    config: CycleConfig,
    tick: VirtualDuration,
    /// Submit index → handle. Lets scenarios name jobs before they exist.
    handles: Vec<JobId>,
    /// Scripted fate, keyed by `JobId::index`.
    outcomes: Vec<Option<Outcome>>,
    trace: Trace,
    decisions: Vec<DispatchDecision>,
    /// Dispatch count per job index; completion events carry the run they belong to.
    runs: Vec<u32>,
    evictions: Vec<Eviction>,
    promoted: Vec<JobId>,
    cascade: Vec<JobId>,
    /// Run `invariants::check` after every event and every cycle.
    pub check_invariants: bool,
}

impl Simulation {
    /// A simulation of `inventory`, ticking every `tick`.
    #[must_use]
    pub fn new(inventory: SlotInventory, config: CycleConfig, tick: VirtualDuration) -> Self {
        Self {
            clock: VirtualClock::new(),
            queue: EventQueue::new(),
            jobs: Arena::new(),
            inventory,
            running: Vec::new(),
            scheduler: Scheduler::new(),
            config,
            tick,
            handles: Vec::new(),
            outcomes: Vec::new(),
            trace: Trace::new(),
            decisions: Vec::new(),
            runs: Vec::new(),
            evictions: Vec::new(),
            promoted: Vec::new(),
            cascade: Vec::new(),
            check_invariants: false,
        }
    }

    /// The current virtual instant.
    #[must_use]
    pub const fn now(&self) -> VirtualTime {
        self.clock.now()
    }

    /// Queues `event` for `at`.
    pub fn schedule(&mut self, at: VirtualTime, event: Event) {
        self.queue.push(at, event);
    }

    /// Everything recorded so far.
    #[must_use]
    pub const fn trace(&self) -> &Trace {
        &self.trace
    }

    /// The handle of the `n`th submitted job, once it exists.
    #[must_use]
    pub fn handle(&self, submit_index: usize) -> Option<JobId> {
        self.handles.get(submit_index).copied()
    }

    /// Events not yet delivered.
    #[must_use]
    pub fn pending_events(&self) -> usize {
        self.queue.len()
    }

    /// True when nothing is queued, ready, or running.
    #[must_use]
    pub fn is_quiescent(&self) -> bool {
        self.queue.is_empty() && self.scheduler.pending() == 0 && self.running.is_empty()
    }

    /// Advances one tick: deliver due events, run one cycle, schedule fates.
    ///
    /// # Errors
    /// A scenario error, a rejected lifecycle call, or an invariant violation.
    pub fn step(&mut self) -> Result<StepReport, SimError> {
        let at = self.clock.now().saturating_add(self.tick);
        self.clock.advance_to(at);
        let mut report = StepReport {
            at,
            ..StepReport::default()
        };

        while let Some((_, event)) = self.queue.pop_due(at) {
            self.apply(event)?;
            report.events_applied += 1;
            if self.check_invariants {
                invariants::check(self)?;
            }
        }

        self.decisions.clear();
        // Every argument is a distinct field of `self`, so the borrows are disjoint.
        let outcome = self.scheduler.run_cycle(
            &mut self.jobs,
            &mut self.inventory,
            &self.running,
            at,
            &self.config,
            &mut self.decisions,
        );
        report.dispatched = outcome.dispatched;
        report.backfilled = outcome.backfilled;
        self.record(
            at,
            None,
            Action::Cycle {
                dispatched: outcome.dispatched,
                backfilled: outcome.backfilled,
            },
        );

        // `mem::take` moves the Vec out so `self` can be borrowed mutably in the loop.
        let decisions = mem::take(&mut self.decisions);
        for decision in &decisions {
            self.on_dispatched(*decision, at);
        }
        self.decisions = decisions;

        // Preemption: only when the cycle left a head job unplaced.
        if let Some(head) = outcome.head {
            let mut evictions = mem::take(&mut self.evictions);
            evictions.clear();
            if let Some(job) = self.jobs.get(head) {
                plan_preemption(
                    job,
                    &self.running,
                    &self.jobs,
                    &self.inventory,
                    at,
                    &self.config.preempt,
                    &mut evictions,
                );
            }
            for e in &evictions {
                if self.scheduler.preempt(e.job, &mut self.jobs).is_ok() {
                    let run = self.runs.get(e.job.index() as usize).copied().unwrap_or(0);
                    self.record(at, Some(e.job), Action::Preempted);
                    // The worker gets the grace period; then the eviction is confirmed.
                    self.queue.push(
                        at.saturating_add(self.config.preempt.grace),
                        Event::Preempted { job: e.job, run },
                    );
                    report.preempted += 1;
                }
            }
            self.evictions = evictions;
        }

        if self.check_invariants {
            invariants::check(self)?;
        }
        Ok(report)
    }

    /// Steps until the clock reaches `end`.
    ///
    /// # Errors
    /// As `step`.
    pub fn run_until(&mut self, end: VirtualTime) -> Result<(), SimError> {
        while self.clock.now() < end {
            self.step()?;
        }
        Ok(())
    }

    /// Steps until quiescent or `max_ticks` elapse. `Ok(true)` if quiescent.
    ///
    /// # Errors
    /// As `step`.
    pub fn run_to_quiescence(&mut self, max_ticks: usize) -> Result<bool, SimError> {
        for _ in 0..max_ticks {
            if self.is_quiescent() {
                return Ok(true);
            }
            self.step()?;
        }
        Ok(self.is_quiescent())
    }

    fn record(&mut self, at: VirtualTime, job: Option<JobId>, action: Action) {
        self.trace.push(TraceRecord { at, job, action });
    }

    /// Returns a running job's capacity to its slot and drops it from the set.
    /// True when `run` is the job's latest dispatch and it still occupies a slot.
    fn current_run(&self, id: JobId, run: u32) -> bool {
        let on_slot = self
            .jobs
            .get(id)
            .is_some_and(|j| matches!(j.state, JobState::Running | JobState::Preempted));
        on_slot && self.runs.get(id.index() as usize).copied() == Some(run)
    }

    fn release(&mut self, id: JobId) {
        if let Some(pos) = self.running.iter().position(|r| r.job == id) {
            // `swap_remove` is O(1); order in `running` is not significant.
            let r = self.running.swap_remove(pos);
            self.inventory
                .release(r.slot, &r.resources)
                .expect("slot came from a dispatch decision");
        }
    }

    fn on_dispatched(&mut self, d: DispatchDecision, at: VirtualTime) {
        let Some(job) = self.jobs.get(d.job) else {
            return;
        };
        let walltime = job.walltime_limit;
        let resources = job.request;
        self.running.push(RunningJob {
            job: d.job,
            slot: d.slot,
            resources,
            ends_at: at.saturating_add(walltime),
        });
        self.record(at, Some(d.job), Action::Dispatched { slot: d.slot });

        // `.copied().flatten()` turns `Option<&Option<Outcome>>` into `Option<Outcome>`.
        let outcome = self
            .outcomes
            .get(d.job.index() as usize)
            .copied()
            .flatten()
            .unwrap_or(Outcome::Completes { after: walltime });
        let index = d.job.index() as usize;
        if self.runs.len() <= index {
            self.runs.resize(index + 1, 0);
        }
        self.runs[index] += 1;
        let run = self.runs[index];
        let job = d.job;
        let (event, after) = match outcome {
            Outcome::Completes { after } if after <= walltime => {
                (Event::Complete { job, run }, after)
            }
            // Past the walltime the worker kills it: a failure at the limit.
            Outcome::Completes { .. } => (Event::Fail { job, run }, walltime),
            Outcome::Fails { after } => (Event::Fail { job, run }, after.min(walltime)),
        };
        self.queue.push(at.saturating_add(after), event);
    }

    fn apply(&mut self, event: Event) -> Result<(), SimError> {
        let now = self.clock.now();
        match event {
            Event::Submit {
                mut job,
                deps,
                outcome,
            } => {
                job.deps.clear();
                for index in deps {
                    let Some(&handle) = self.handles.get(index) else {
                        return Err(SimError::ForwardDependency { index });
                    };
                    job.deps.push(handle);
                }
                let id = self.jobs.insert(job);
                self.handles.push(id);
                let slot = id.index() as usize;
                if self.outcomes.len() <= slot {
                    self.outcomes.resize(slot + 1, None);
                }
                self.outcomes[slot] = Some(outcome);
                let state = self
                    .scheduler
                    .submit(id, &mut self.jobs, now, &self.config)?;
                self.record(now, Some(id), Action::Submitted { state });
            }
            Event::Complete { job: id, run } => {
                // Cancelled, or a run that was preempted: the fate no longer applies.
                if !self.current_run(id, run) {
                    return Ok(());
                }
                self.release(id);
                let mut promoted = mem::take(&mut self.promoted);
                self.scheduler.on_completed(
                    id,
                    &mut self.jobs,
                    now,
                    &self.config,
                    &mut promoted,
                )?;
                self.record(now, Some(id), Action::Completed);
                for p in &promoted {
                    self.record(now, Some(*p), Action::Promoted);
                }
                self.promoted = promoted;
            }
            Event::Fail { job: id, run } => {
                if !self.current_run(id, run) {
                    return Ok(());
                }
                self.release(id);
                let mut cascade = mem::take(&mut self.cascade);
                self.scheduler
                    .on_failed(id, &mut self.jobs, now, &self.config, &mut cascade)?;
                self.record(now, Some(id), Action::Failed);
                for c in &cascade {
                    self.record(now, Some(*c), Action::Cancelled);
                }
                self.cascade = cascade;
            }
            Event::Preempted { job: id, run } => {
                // Finished inside its grace, or cancelled: nothing to confirm.
                let evicting = self
                    .jobs
                    .get(id)
                    .is_some_and(|j| j.state == JobState::Preempted);
                if !evicting || !self.current_run(id, run) {
                    return Ok(());
                }
                self.release(id);
                self.scheduler
                    .on_preempted(id, &mut self.jobs, now, &self.config)?;
                self.record(now, Some(id), Action::Requeued);
            }
            Event::Cancel { submit_index } => {
                let Some(&id) = self.handles.get(submit_index) else {
                    return Err(SimError::ForwardDependency {
                        index: submit_index,
                    });
                };
                let Some(state) = self.jobs.get(id).map(|j| j.state) else {
                    return Ok(());
                };
                if state.is_terminal() {
                    return Ok(()); // finished before the cancel arrived
                }
                if state == JobState::Running {
                    self.release(id);
                }
                let mut cascade = mem::take(&mut self.cascade);
                self.scheduler
                    .cancel(id, &mut self.jobs, now, &self.config, &mut cascade)?;
                self.record(now, Some(id), Action::Cancelled);
                for c in &cascade {
                    self.record(now, Some(*c), Action::Cancelled);
                }
                self.cascade = cascade;
            }
        }
        Ok(())
    }
}
