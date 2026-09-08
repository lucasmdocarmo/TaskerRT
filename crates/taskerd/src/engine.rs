//! The scheduler thread. Owns every piece of scheduling state; nothing else
//! touches the arena. Sync, never awaits, allocates only on first growth.

use std::fmt;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossbeam_utils::sync::Parker;
use tasker_core::{
    Arena, CycleConfig, CycleOutcome, DispatchDecision, Job, JobId, JobState, Resources,
    RunningJob, Scheduler, SlotIndex, SlotInventory, VirtualTime,
};

use crate::{
    Command, DaemonConfig, Demand, Dispatch, Inbox, Metrics, NodeSummary, Outbox, Query,
    QueueSummary, clock,
};

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
}

impl Engine {
    #[must_use]
    pub fn new(config: &DaemonConfig, metrics: Arc<Metrics>) -> Self {
        Self {
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
        }
    }

    /// The thread body: park until the tick or a wake-up, drain, schedule.
    pub fn run(
        mut self,
        inbox: &Inbox,
        outbox: &Outbox,
        parker: &Parker,
        tick: Duration,
        stop: &AtomicBool,
    ) {
        // Relaxed is enough: nothing else is published through this flag.
        while !stop.load(Ordering::Relaxed) {
            // Returns early on `unpark` (a push) or after `tick` elapses.
            parker.park_timeout(tick);
            let now = clock::now();
            self.drain(inbox, now, outbox);
            self.tick(now, outbox);
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
            Command::Submit { job, reply } => {
                let id = self.jobs.insert(job);
                let result = match self.scheduler.submit(id, &mut self.jobs, now, &self.cycle) {
                    Ok(state) => Ok((id, state)),
                    Err(e) => {
                        // A rejected job never existed as far as clients are concerned.
                        self.jobs.remove(id);
                        Err(e)
                    }
                };
                // The client may have gone away; that is not our problem.
                reply.send(result).ok();
            }
            Command::Cancel { id, reply } => {
                let slot = self.release(id);
                let mut cascade = mem::take(&mut self.cascade);
                let result = self
                    .scheduler
                    .cancel(id, &mut self.jobs, &mut cascade)
                    .map(|()| cascade.clone());
                self.cascade = cascade;
                if let (Ok(_), Some(slot)) = (&result, slot) {
                    self.push_dispatch(Dispatch::Kill { slot, id }, outbox);
                }
                reply.send(result).ok();
            }
            Command::Completed { id } => {
                // `release` returning `None` means the job was not running: a
                // completion for a cancelled or already-finished job is dropped.
                if self.release(id).is_some() {
                    let mut promoted = mem::take(&mut self.promoted);
                    if let Err(e) = self.scheduler.on_completed(
                        id,
                        &mut self.jobs,
                        now,
                        &self.cycle,
                        &mut promoted,
                    ) {
                        tracing::warn!(?id, error = %e, "completion rejected");
                    }
                    self.promoted = promoted;
                }
            }
            Command::Failed { id } => {
                if self.release(id).is_some() {
                    let mut cascade = mem::take(&mut self.cascade);
                    if let Err(e) = self.scheduler.on_failed(id, &mut self.jobs, &mut cascade) {
                        tracing::warn!(?id, error = %e, "failure rejected");
                    }
                    self.cascade = cascade;
                }
            }
            Command::WorkerJoined {
                name,
                capacity,
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
            },
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
        self.push_dispatch(
            Dispatch::Assign {
                slot: d.slot,
                id: d.job,
                payload,
                walltime,
            },
            outbox,
        );
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
        for id in &evicted {
            if let Err(e) = self
                .scheduler
                .requeue(*id, &mut self.jobs, now, &self.cycle)
            {
                tracing::warn!(?id, error = %e, "requeue after worker loss failed");
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
