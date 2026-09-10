//! The scheduler thread. Owns every piece of scheduling state; nothing else
//! touches the arena. Sync, never awaits, allocates only on first growth.

use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossbeam_utils::sync::Parker;
use tasker_core::{
    AccountId, Arena, ArenaRestoreError, CycleConfig, CycleOutcome, DispatchDecision, FACTOR_SCALE,
    Job, JobId, JobState, LifecycleError, Resources, RunningJob, Scheduler, SlotIndex,
    SlotInventory, SlotState, USAGE_PER_CORE_SECOND, VirtualDuration, VirtualTime,
};
use tasker_wal::{Record, Recovered, WalError};

use crate::{
    AccountSummary, Command, DaemonConfig, Demand, Dispatch, Inbox, Journal, Metrics, NodeSummary,
    Outbox, Query, QueueSummary, Reply, clock,
};

/// What a `Submit` reply carries.
type SubmitResult = Result<(JobId, JobState), LifecycleError>;
/// What a `Cancel` reply carries.
type CancelResult = Result<Vec<JobId>, LifecycleError>;

/// The log could not be turned back into an engine.
#[derive(Debug, thiserror::Error)]
pub enum RecoverError {
    #[error(transparent)]
    Wal(#[from] WalError),
    #[error(transparent)]
    Arena(#[from] ArenaRestoreError),
    #[error("replaying the log: {0}")]
    Lifecycle(#[from] LifecycleError),
    #[error("log expected job {expected:?} but the arena would issue {found:?}")]
    IdMismatch { expected: JobId, found: JobId },
}

/// All scheduling state, owned by one thread.
pub struct Engine {
    cycle: CycleConfig,
    drain_budget: usize,
    jobs: Arena<Job>,
    inventory: SlotInventory,
    scheduler: Scheduler,
    running: Vec<RunningJob>,
    /// Worker name per slot; `None` when the slot is drained.
    names: Vec<Option<String>>,
    decisions: Vec<DispatchDecision>,
    promoted: Vec<JobId>,
    cascade: Vec<JobId>,
    evicted: Vec<JobId>,
    /// Dispatches the outbox could not take; flushed first on the next tick.
    overflow: Vec<Dispatch>,
    cycles: u64,
    /// Recomputed every tick; the scaler RPC reads this, never the arena.
    demand: Demand,
    metrics: Arc<Metrics>,
    worker_cpu: u64,
    min_workers: u32,
    max_workers: u32,
    /// When the per-account gauges were last published.
    account_metrics_at: VirtualTime,
    /// The write-ahead log's engine end; `None` runs in memory only.
    journal: Option<Journal>,
    /// (terminal time, id) in order, for retention.
    terminal: VecDeque<(VirtualTime, JobId)>,
    retain: VirtualDuration,
    /// Jobs that were Running when the log ended, waiting for their worker to reconnect.
    reconciling: Vec<JobId>,
    /// Armed on the first tick after recovery; unclaimed jobs are requeued when it passes.
    reconcile_until: Option<VirtualTime>,
    reconcile_window: VirtualDuration,
    /// True while the daemon is shutting down: a closing worker stream then means
    /// the daemon is leaving, not the worker, and its jobs must stay Running.
    quiescing: bool,
}

impl Engine {
    /// An in-memory engine: nothing is logged.
    #[must_use]
    pub fn new(config: &DaemonConfig, metrics: Arc<Metrics>) -> Self {
        Self::build(config, metrics, None)
    }

    /// An engine that records every transition into `journal`.
    #[must_use]
    pub fn with_journal(config: &DaemonConfig, metrics: Arc<Metrics>, journal: Journal) -> Self {
        Self::build(config, metrics, Some(journal))
    }

    fn build(config: &DaemonConfig, metrics: Arc<Metrics>, journal: Option<Journal>) -> Self {
        let mut engine = Self {
            cycle: config.cycle,
            drain_budget: config.drain_budget,
            jobs: Arena::with_capacity(config.cycle.max_candidates),
            inventory: SlotInventory::from_capacities(&[]),
            scheduler: Scheduler::with_slots(config.cycle.max_candidates),
            running: Vec::with_capacity(1_024),
            names: Vec::new(),
            decisions: Vec::with_capacity(config.cycle.max_candidates),
            promoted: Vec::with_capacity(64),
            cascade: Vec::with_capacity(64),
            evicted: Vec::with_capacity(64),
            overflow: Vec::new(),
            cycles: 0,
            demand: Demand::default(),
            metrics,
            worker_cpu: u64::from(config.worker_cpu_millis.max(1)),
            min_workers: config.min_workers,
            max_workers: config.max_workers.max(config.min_workers),
            account_metrics_at: VirtualTime::ZERO,
            journal,
            terminal: VecDeque::new(),
            retain: VirtualDuration::from_nanos(
                u64::try_from(config.retain.as_nanos()).unwrap_or(u64::MAX),
            ),
            reconciling: Vec::new(),
            reconcile_until: None,
            quiescing: false,
            // Twice the heartbeat timeout: enough for a reconnect with backoff.
            reconcile_window: VirtualDuration::from_nanos(
                u64::try_from(config.heartbeat_timeout.as_nanos())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(2),
            ),
        };
        for &(account, shares) in &config.shares {
            if let Err(e) = engine.scheduler.set_shares(AccountId::new(account), shares) {
                tracing::warn!(account, error = %e, "ignoring shares for invalid account");
            }
        }
        engine
    }

    /// The thread body: park until the tick or a wake-up, drain, schedule.
    pub fn run(
        mut self,
        inbox: &Inbox,
        outbox: &Outbox,
        parker: &Parker,
        tick: Duration,
        stop: &AtomicBool,
        quiesce: &AtomicBool,
    ) {
        // Relaxed is enough: nothing else is published through these flags.
        while !stop.load(Ordering::Relaxed) {
            // Returns early on `unpark` (a push) or after `tick` elapses.
            parker.park_timeout(tick);
            self.quiescing = quiesce.load(Ordering::Relaxed);
            let now = clock::now();
            self.drain(inbox, now, outbox);
            self.tick(now, outbox);
        }
        // A blocking hand-off: the last acks must not die with the thread.
        if let Some(journal) = self.journal.as_mut() {
            journal.flush();
        }
        tracing::info!(cycles = self.cycles, "scheduler thread stopping");
    }

    /// Applies up to `drain_budget` commands. Returns how many.
    pub fn drain(&mut self, inbox: &Inbox, now: VirtualTime, outbox: &Outbox) -> usize {
        let mut applied = 0;
        while applied < self.drain_budget {
            let Some(command) = inbox.pop() else { break };
            self.handle(command, now, outbox);
            applied += 1;
        }
        applied
    }

    /// Applies one command.
    pub fn handle(&mut self, command: Command, now: VirtualTime, outbox: &Outbox) {
        match command {
            Command::Submit { job, reply } => self.on_submit(job, reply, now),
            Command::Cancel { id, reply } => self.on_cancel(id, reply, now, outbox),
            Command::Completed { id } => {
                // `release` returning `None` means the job was not running: a
                // completion for a cancelled or already-finished job is dropped.
                if self.release(id).is_some() || self.unhold(id) {
                    let mut promoted = mem::take(&mut self.promoted);
                    match self.scheduler.on_completed(
                        id,
                        &mut self.jobs,
                        now,
                        &self.cycle,
                        &mut promoted,
                    ) {
                        Ok(()) => {
                            self.record(&Record::Completed { id, at: now });
                            self.note_terminal(id, now);
                        }
                        Err(e) => tracing::warn!(?id, error = %e, "completion rejected"),
                    }
                    self.promoted = promoted;
                }
            }
            Command::Failed { id } => {
                if self.release(id).is_some() || self.unhold(id) {
                    let mut cascade = mem::take(&mut self.cascade);
                    match self.scheduler.on_failed(
                        id,
                        &mut self.jobs,
                        now,
                        &self.cycle,
                        &mut cascade,
                    ) {
                        Ok(()) => {
                            self.record(&Record::Failed { id, at: now });
                            self.note_terminal(id, now);
                            for &cascaded in &cascade {
                                self.note_terminal(cascaded, now);
                            }
                        }
                        Err(e) => tracing::warn!(?id, error = %e, "failure rejected"),
                    }
                    self.cascade = cascade;
                }
            }
            Command::WorkerJoined {
                name,
                capacity,
                in_flight,
                reply,
            } => {
                // Reuse a slot only if it is vacant *and* zeroed: a draining worker
                // still owns its zeroed slot until its stream closes.
                let vacant = self.names.iter().enumerate().find_map(|(i, n)| {
                    let slot = SlotIndex::try_from(i).ok()?;
                    let zero = self.inventory.slot(slot)?.capacity == Resources::ZERO;
                    (n.is_none() && zero).then_some(slot)
                });
                let slot = match vacant {
                    Some(slot) => {
                        let _ = self.inventory.set_capacity(slot, capacity);
                        slot
                    }
                    None => self.inventory.push_slot(capacity),
                };
                let index = slot as usize;
                if self.names.len() <= index {
                    self.names.resize(index + 1, None);
                }
                self.names[index] = Some(name);
                for id in in_flight {
                    self.claim(id, slot, now, outbox);
                }
                reply.send(slot).ok();
            }
            Command::WorkerLeft { slot } => self.evict_slot(slot, now),
            Command::WorkerDraining { slot } => {
                // No new work; whatever is running stays and completes normally.
                let _ = self.inventory.set_capacity(slot, Resources::ZERO);
                tracing::info!(slot, "worker draining");
            }
            Command::Query(query) => match query {
                Query::Status { id, reply } => {
                    reply.send(self.jobs.get(id).map(|j| j.state)).ok();
                }
                Query::Queue { reply } => {
                    reply.send(self.queue_summary()).ok();
                }
                Query::Nodes { reply } => {
                    reply.send(self.nodes()).ok();
                }
                Query::Demand { reply } => {
                    reply.send(self.demand).ok();
                }
                Query::Accounts { reply } => {
                    reply.send(self.accounts()).ok();
                }
            },
        }
    }

    /// Validates, inserts, logs, and schedules a new job. The reply waits for
    /// durability when a journal is present.
    fn on_submit(&mut self, job: Job, reply: Reply<SubmitResult>, now: VirtualTime) {
        let id = self.jobs.next_id();
        // Validate before inserting: only accepted jobs enter the arena, so
        // replaying the log reproduces every id exactly.
        if let Err(e) = self.scheduler.check_submit(id, &job, &self.jobs) {
            reply.send(Err(e)).ok();
            return;
        }
        let inserted = self.jobs.insert(job);
        debug_assert_eq!(inserted, id);
        if let (Some(journal), Some(job)) = (self.journal.as_mut(), self.jobs.get(id)) {
            journal.record_submitted(id, job);
        }
        let result = self
            .scheduler
            .submit(id, &mut self.jobs, now, &self.cycle)
            .map(|state| (id, state));
        if let Ok((_, state)) = result
            && state.is_terminal()
        {
            self.note_terminal(id, now);
        }
        // The client may have gone away; that is not our problem.
        match self.journal.as_mut() {
            Some(journal) => journal.defer(Box::new(move || {
                reply.send(result).ok();
            })),
            None => {
                reply.send(result).ok();
            }
        }
    }

    /// Cancels `id` and its dependents, logging the root; dependents are implied.
    fn on_cancel(
        &mut self,
        id: JobId,
        reply: Reply<CancelResult>,
        now: VirtualTime,
        outbox: &Outbox,
    ) {
        let slot = self.release(id);
        // A held job being cancelled must not be adopted later.
        self.unhold(id);
        let mut cascade = mem::take(&mut self.cascade);
        let result = self
            .scheduler
            .cancel(id, &mut self.jobs, now, &self.cycle, &mut cascade)
            .map(|()| cascade.clone());
        if result.is_ok() {
            self.record(&Record::Cancelled { id, at: now });
            self.note_terminal(id, now);
            for &cascaded in &cascade {
                self.note_terminal(cascaded, now);
            }
        }
        self.cascade = cascade;
        if let (Ok(_), Some(slot)) = (&result, slot) {
            // Kills are not held back: stopping work early is safe either way.
            self.push_dispatch(Dispatch::Kill { slot, id }, outbox);
        }
        match self.journal.as_mut() {
            Some(journal) => journal.defer(Box::new(move || {
                reply.send(result).ok();
            })),
            None => {
                reply.send(result).ok();
            }
        }
    }

    /// One scheduling cycle. Flushes overflow first so ordering is preserved.
    pub fn tick(&mut self, now: VirtualTime, outbox: &Outbox) -> CycleOutcome {
        let started = clock::now();
        while let Some(dispatch) = self.overflow.pop() {
            if let Err(back) = outbox.push(dispatch) {
                self.overflow.push(back);
                break;
            }
        }

        self.decisions.clear();
        let outcome = self.scheduler.run_cycle(
            &mut self.jobs,
            &mut self.inventory,
            &self.running,
            now,
            &self.cycle,
            &mut self.decisions,
        );
        let decisions = mem::take(&mut self.decisions);
        for decision in &decisions {
            self.on_dispatched(*decision, now, outbox);
        }
        self.decisions = decisions;
        self.cycles += 1;
        self.refresh_demand();
        // Label lookups allocate, so account gauges refresh once a second, not every tick.
        if now.saturating_sub_time(self.account_metrics_at) >= VirtualDuration::from_secs(1) {
            self.publish_account_metrics();
            self.account_metrics_at = now;
        }
        self.forget_expired(now);
        self.expire_reconcile(now);
        if self.journal.as_ref().is_some_and(Journal::wants_snapshot) {
            let snapshot = self.encode_snapshot(now);
            if let Some(journal) = self.journal.as_mut() {
                journal.attach_snapshot(snapshot);
            }
        }
        if let Some(journal) = self.journal.as_mut() {
            journal.commit();
        }
        // Histogram observe is an atomic add plus a bucket search; ~50 ns.
        let elapsed = clock::now().saturating_sub_time(started).as_nanos();
        #[allow(clippy::cast_precision_loss)] // nanoseconds → seconds; precision irrelevant here
        self.metrics.cycle_seconds.observe(elapsed as f64 / 1e9);
        outcome
    }

    /// One pass over the arena: what the pool would need to run everything
    /// ready and running at once, clamped to the configured range.
    fn refresh_demand(&mut self) {
        let mut cpu_ready = 0_u64;
        let mut cpu_running = 0_u64;
        let mut blocked = 0_i64;
        for (_, job) in self.jobs.iter() {
            match job.state {
                JobState::Ready => cpu_ready += u64::from(job.request.cpu_millis),
                JobState::Running => cpu_running += u64::from(job.request.cpu_millis),
                JobState::Blocked => blocked += 1,
                _ => {}
            }
        }
        // `div_ceil`: a partial worker's worth of demand still needs a whole worker.
        let needed = (cpu_ready + cpu_running).div_ceil(self.worker_cpu);
        let desired = u32::try_from(needed)
            .unwrap_or(u32::MAX)
            .clamp(self.min_workers, self.max_workers);
        let attached =
            u32::try_from(self.names.iter().filter(|n| n.is_some()).count()).unwrap_or(u32::MAX);
        self.demand = Demand {
            desired,
            attached,
            cpu_ready,
            cpu_running,
        };

        let m = &self.metrics;
        m.ready
            .set(i64::try_from(self.scheduler.pending()).unwrap_or(i64::MAX));
        m.blocked.set(blocked);
        m.running
            .set(i64::try_from(self.running.len()).unwrap_or(i64::MAX));
        m.workers_attached.set(i64::from(attached));
        m.workers_desired.set(i64::from(desired));
    }

    /// Counts by state. O(jobs); a query, not a hot path.
    #[must_use]
    pub fn queue_summary(&self) -> QueueSummary {
        let mut blocked = 0;
        for (_, job) in self.jobs.iter() {
            if job.state == JobState::Blocked {
                blocked += 1;
            }
        }
        QueueSummary {
            ready: self.scheduler.pending(),
            blocked,
            running: self.running.len(),
            cycles: self.cycles,
        }
    }

    /// One entry per slot, attached or drained.
    #[must_use]
    pub fn nodes(&self) -> Vec<NodeSummary> {
        (0..self.inventory.len())
            .filter_map(|slot| {
                let s = self.inventory.slot(slot)?;
                let name = self.names.get(slot as usize).cloned().flatten();
                Some(NodeSummary {
                    slot,
                    attached: name.is_some(),
                    name: name.unwrap_or_default(),
                    capacity: s.capacity,
                    allocated: s.allocated,
                })
            })
            .collect()
    }

    /// One row per account seen so far.
    #[must_use]
    pub fn accounts(&self) -> Vec<AccountSummary> {
        let fs = self.scheduler.fairshare();
        fs.ledgers()
            .iter()
            .zip(fs.factors())
            .enumerate()
            .map(|(i, (ledger, factor))| AccountSummary {
                account: u32::try_from(i).unwrap_or(u32::MAX),
                shares: ledger.shares,
                usage: ledger.usage,
                running_cpu: ledger.running_cpu,
                fairshare: *factor,
            })
            .collect()
    }

    #[allow(clippy::cast_precision_loss)] // gauges are for humans; f64 is plenty
    fn publish_account_metrics(&self) {
        let fs = self.scheduler.fairshare();
        for (i, (ledger, factor)) in fs.ledgers().iter().zip(fs.factors()).enumerate() {
            let label = i.to_string();
            self.metrics
                .account_usage
                .with_label_values(&[&label])
                .set(ledger.usage as f64 / USAGE_PER_CORE_SECOND as f64);
            self.metrics
                .account_fairshare
                .with_label_values(&[&label])
                .set(*factor as f64 / FACTOR_SCALE as f64);
        }
    }

    /// Rebuilds an engine from what `Store::recover` found, then queues a fresh
    /// snapshot so the first commit compacts the log.
    ///
    /// # Errors
    /// `RecoverError` when the files are corrupt or the log contradicts itself.
    pub fn recover(
        config: &DaemonConfig,
        metrics: Arc<Metrics>,
        recovered: Recovered,
        journal: Journal,
    ) -> Result<Self, RecoverError> {
        let mut engine = Self::with_journal(config, metrics, journal);
        let mut latest = VirtualTime::ZERO;
        if let Some(snapshot) = recovered.snapshot {
            latest = engine.restore_snapshot(snapshot)?;
        }
        for record in recovered.records {
            engine.apply(record, &mut latest)?;
        }
        // M6a: the old process took every worker with it. Running work goes back in the queue.
        let running: Vec<JobId> = engine
            .jobs
            .iter()
            .filter(|(_, j)| j.state == JobState::Running)
            .map(|(id, _)| id)
            .collect();
        // Their workers may still be running them: hold, and let the first tick arm the window.
        if !running.is_empty() {
            tracing::info!(held = running.len(), "running jobs await their workers");
        }
        engine.reconciling = running;
        clock::set_origin(latest.saturating_add(VirtualDuration::from_nanos(1)));
        let snapshot = engine.encode_snapshot(latest);
        if let Some(journal) = engine.journal.as_mut() {
            journal.attach_snapshot(snapshot);
        }
        tracing::info!(jobs = engine.jobs.len(), torn = ?recovered.torn_at, "recovered from the write-ahead log");
        Ok(engine)
    }

    /// Loads a snapshot: the arena as it was, non-terminal jobs re-classified
    /// through `submit`, running jobs restored, ledgers last. Returns `taken_at`.
    fn restore_snapshot(
        &mut self,
        snapshot: tasker_wal::Snapshot,
    ) -> Result<VirtualTime, RecoverError> {
        let at = snapshot.taken_at;
        let mut slots = snapshot.slots;
        // Non-terminal jobs go back to Submitted so `submit` classifies them again.
        for slot in &mut slots {
            if let SlotState::Occupied { value, .. } = slot
                && !value.state.is_terminal()
            {
                value.state = JobState::Submitted;
            }
        }
        self.jobs = Arena::from_parts(slots, snapshot.free_head)?;
        let ids: Vec<(JobId, JobState)> = self.jobs.iter().map(|(id, j)| (id, j.state)).collect();
        for (id, state) in ids {
            if state.is_terminal() {
                self.note_terminal(id, at);
            } else {
                let state = self.scheduler.submit(id, &mut self.jobs, at, &self.cycle)?;
                if state.is_terminal() {
                    self.note_terminal(id, at);
                }
            }
        }
        for id in snapshot.running {
            self.scheduler
                .restore_running(id, &mut self.jobs, at, &self.cycle)?;
        }
        self.scheduler.restore_ledgers(snapshot.ledgers)?;
        Ok(at)
    }

    /// Replays one record through the same API the live engine uses.
    fn apply(&mut self, record: Record, latest: &mut VirtualTime) -> Result<(), RecoverError> {
        match record {
            Record::Submitted { id, job } => {
                let next = self.jobs.next_id();
                if next != id {
                    return Err(RecoverError::IdMismatch {
                        expected: id,
                        found: next,
                    });
                }
                let at = job.submit_time;
                self.jobs.insert(job);
                let state = self.scheduler.submit(id, &mut self.jobs, at, &self.cycle)?;
                if state.is_terminal() {
                    self.note_terminal(id, at);
                }
                *latest = (*latest).max(at);
            }
            Record::Dispatched { id, at } => {
                self.scheduler
                    .restore_running(id, &mut self.jobs, at, &self.cycle)?;
                *latest = (*latest).max(at);
            }
            Record::Completed { id, at } => {
                let mut promoted = mem::take(&mut self.promoted);
                let result =
                    self.scheduler
                        .on_completed(id, &mut self.jobs, at, &self.cycle, &mut promoted);
                self.promoted = promoted;
                result?;
                self.note_terminal(id, at);
                *latest = (*latest).max(at);
            }
            Record::Failed { id, at } => {
                let mut cascade = mem::take(&mut self.cascade);
                let result =
                    self.scheduler
                        .on_failed(id, &mut self.jobs, at, &self.cycle, &mut cascade);
                self.note_terminal(id, at);
                for &c in &cascade {
                    self.note_terminal(c, at);
                }
                self.cascade = cascade;
                result?;
                *latest = (*latest).max(at);
            }
            Record::Cancelled { id, at } => {
                let mut cascade = mem::take(&mut self.cascade);
                let result =
                    self.scheduler
                        .cancel(id, &mut self.jobs, at, &self.cycle, &mut cascade);
                self.note_terminal(id, at);
                for &c in &cascade {
                    self.note_terminal(c, at);
                }
                self.cascade = cascade;
                result?;
                *latest = (*latest).max(at);
            }
            Record::Requeued { id, at } => {
                self.scheduler
                    .requeue(id, &mut self.jobs, at, &self.cycle)?;
                *latest = (*latest).max(at);
            }
            Record::Forgotten { id } => {
                self.jobs.remove(id);
            }
        }
        Ok(())
    }

    /// A reconnecting worker says it still runs `id`. Held after recovery: adopt
    /// it onto this slot. Still bound to an older stream of the same worker: move
    /// the binding. Anything else is stale, and the worker is told to stop it.
    fn claim(&mut self, id: JobId, slot: SlotIndex, now: VirtualTime, outbox: &Outbox) {
        if self.unhold(id) {
            self.adopt_running(id, slot, now, outbox);
        } else if let Some(pos) = self.running.iter().position(|r| r.job == id) {
            let (old, resources) = (self.running[pos].slot, self.running[pos].resources);
            if let Err(e) = self.inventory.release(old, &resources) {
                tracing::error!(slot = old, error = %e, "release failed");
            }
            match self.inventory.try_allocate(slot, &resources) {
                Ok(()) => {
                    self.running[pos].slot = slot;
                    tracing::info!(
                        ?id,
                        from = old,
                        to = slot,
                        "job followed its worker to a new stream"
                    );
                }
                Err(e) => {
                    tracing::warn!(?id, slot, error = %e, "new slot cannot hold the job; requeueing");
                    self.running.swap_remove(pos);
                    self.requeue_logged(id, now);
                    self.push_dispatch(Dispatch::Kill { slot, id }, outbox);
                }
            }
        } else {
            self.push_dispatch(Dispatch::Kill { slot, id }, outbox);
        }
    }

    /// Binds a held Running job to `slot` without a dispatch.
    fn adopt_running(&mut self, id: JobId, slot: SlotIndex, now: VirtualTime, outbox: &Outbox) {
        let Some(job) = self.jobs.get(id) else {
            return;
        };
        if job.state != JobState::Running {
            return;
        }
        let (request, ends_at) = (job.request, now.saturating_add(job.walltime_limit));
        match self.inventory.try_allocate(slot, &request) {
            Ok(()) => {
                self.running.push(RunningJob {
                    job: id,
                    slot,
                    resources: request,
                    ends_at,
                });
                tracing::info!(?id, slot, "reconciled: the worker kept the job running");
            }
            Err(e) => {
                tracing::warn!(?id, slot, error = %e, "worker cannot hold its job; requeueing");
                self.requeue_logged(id, now);
                self.push_dispatch(Dispatch::Kill { slot, id }, outbox);
            }
        }
    }

    /// Requeues and logs, warning instead of failing on an illegal transition.
    fn requeue_logged(&mut self, id: JobId, now: VirtualTime) {
        match self.scheduler.requeue(id, &mut self.jobs, now, &self.cycle) {
            Ok(()) => self.record(&Record::Requeued { id, at: now }),
            Err(e) => tracing::warn!(?id, error = %e, "requeue failed"),
        }
    }

    /// Drops `id` from the reconcile list. True if it was there.
    fn unhold(&mut self, id: JobId) -> bool {
        match self.reconciling.iter().position(|r| *r == id) {
            Some(pos) => {
                self.reconciling.swap_remove(pos);
                true
            }
            None => false,
        }
    }

    /// Arms the reconcile deadline on the first tick after recovery and, once it
    /// passes, requeues whatever no worker claimed.
    fn expire_reconcile(&mut self, now: VirtualTime) {
        if self.reconciling.is_empty() {
            self.reconcile_until = None;
            return;
        }
        let window = self.reconcile_window;
        let until = *self
            .reconcile_until
            .get_or_insert_with(|| now.saturating_add(window));
        if now < until {
            return;
        }
        let unclaimed = mem::take(&mut self.reconciling);
        let count = unclaimed.len();
        for id in unclaimed {
            if self
                .jobs
                .get(id)
                .is_some_and(|j| j.state == JobState::Running)
            {
                self.requeue_logged(id, now);
            }
        }
        tracing::warn!(requeued = count, "reconcile window closed without a claim");
        self.reconcile_until = None;
    }

    fn record(&mut self, record: &Record) {
        if let Some(journal) = self.journal.as_mut() {
            journal.record(record);
        }
    }

    /// Remembers when `id` turned terminal so retention can forget it later.
    fn note_terminal(&mut self, id: JobId, at: VirtualTime) {
        self.terminal.push_back((at, id));
    }

    /// Forgets terminal jobs older than the retention window. The slot returns
    /// to the arena's free list and the removal is logged, so replay agrees.
    fn forget_expired(&mut self, now: VirtualTime) {
        while let Some(&(at, id)) = self.terminal.front() {
            if now.saturating_sub_time(at) < self.retain {
                break;
            }
            self.terminal.pop_front();
            if self.jobs.remove(id).is_some() {
                self.record(&Record::Forgotten { id });
            }
        }
    }

    /// The whole durable state, encoded for the store.
    fn encode_snapshot(&self, now: VirtualTime) -> Vec<u8> {
        // From job state, not slot bindings: a job held for reconciliation is
        // Running without a slot and must come back Running.
        let running: Vec<JobId> = self
            .jobs
            .iter()
            .filter(|(_, j)| j.state == JobState::Running)
            .map(|(id, _)| id)
            .collect();
        let mut buf = Vec::new();
        tasker_wal::encode_snapshot(
            &mut buf,
            now,
            self.jobs.slots(),
            self.jobs.free_head(),
            &running,
            self.scheduler.fairshare().ledgers(),
        );
        buf
    }

    /// Drops `id` from the running set and returns its capacity. `None` if it
    /// was not running.
    fn release(&mut self, id: JobId) -> Option<SlotIndex> {
        let pos = self.running.iter().position(|r| r.job == id)?;
        let r = self.running.swap_remove(pos);
        if let Err(e) = self.inventory.release(r.slot, &r.resources) {
            tracing::error!(slot = r.slot, error = %e, "release failed");
        }
        Some(r.slot)
    }

    fn on_dispatched(&mut self, d: DispatchDecision, now: VirtualTime, outbox: &Outbox) {
        let Some(job) = self.jobs.get(d.job) else {
            return;
        };
        let walltime = job.walltime_limit;
        // Refcount bump, not a copy: the payload stays shared with the arena.
        let payload = job.payload.clone();
        self.running.push(RunningJob {
            job: d.job,
            slot: d.slot,
            resources: job.request,
            ends_at: now.saturating_add(walltime),
        });
        let assign = Dispatch::Assign {
            slot: d.slot,
            id: d.job,
            payload,
            walltime,
        };
        match self.journal.as_mut() {
            Some(journal) => {
                // Write-ahead: the worker sees this only after the record is on disk.
                journal.record(&Record::Dispatched { id: d.job, at: now });
                journal.hold(assign);
            }
            None => self.push_dispatch(assign, outbox),
        }
    }

    fn push_dispatch(&mut self, dispatch: Dispatch, outbox: &Outbox) {
        if let Err(back) = outbox.push(dispatch) {
            self.overflow.push(back);
        }
    }

    /// A worker is gone: requeue its jobs, zero its slot.
    fn evict_slot(&mut self, slot: SlotIndex, now: VirtualTime) {
        self.evicted.clear();
        self.evicted.extend(
            self.running
                .iter()
                .filter(|r| r.slot == slot)
                .map(|r| r.job),
        );
        // `retain` keeps the elements the closure accepts; no allocation.
        self.running.retain(|r| r.slot != slot);
        let evicted = mem::take(&mut self.evicted);
        if self.quiescing {
            // The daemon is leaving, not the worker. Its jobs stay Running in the
            // log, to be reclaimed when the worker reconnects to the next incarnation.
            tracing::info!(
                slot,
                kept = evicted.len(),
                "worker stream closed during shutdown"
            );
        } else {
            for id in &evicted {
                self.requeue_logged(*id, now);
            }
        }
        let requeued = evicted.len();
        self.evicted = evicted;
        let _ = self.inventory.clear_allocation(slot);
        let _ = self.inventory.set_capacity(slot, Resources::ZERO);
        if let Some(name) = self.names.get_mut(slot as usize) {
            *name = None;
        }
        tracing::info!(slot, requeued, "worker left");
    }
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("jobs", &self.jobs.len())
            .field("running", &self.running.len())
            .field("pending", &self.scheduler.pending())
            .field("cycles", &self.cycles)
            .finish_non_exhaustive()
    }
}
