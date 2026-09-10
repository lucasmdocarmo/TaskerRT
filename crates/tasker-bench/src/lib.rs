//! Synthetic workload generation for the M1 benchmarks.
//!
//! The generator is deterministic: the same seed always produces the same
//! cluster, queue, and running set. Benchmarks that are not reproducible cannot
//! support the before/after claims spec §7.1 requires.

use tasker_core::{
    AccountId, Arena, CycleConfig, FairShareConfig, Job, JobId, JobState, PackBudget,
    PriorityClass, PriorityConfig, PriorityWeights, ResourceRequest, Resources, RunningJob,
    SlotInventory, VirtualDuration, VirtualTime,
};
use tasker_sim::SplitMix64;

/// A generated cluster plus its queued and running work.
#[derive(Debug)]
pub struct Workload {
    pub jobs: Arena<Job>,
    pub inventory: SlotInventory,
    pub running: Vec<RunningJob>,
    /// Queued job ids, in creation order.
    pub admitted: Vec<JobId>,
}

const SLOT_CPU: u32 = 8_000;
const SLOT_MEM: u64 = 32 << 30;
const SECOND: u64 = 1_000_000_000;

/// The spec §7.1 fixture: `pending` queued jobs, `running` occupied jobs, and
/// `slots` uniform worker slots.
#[must_use]
pub fn synthetic(pending: usize, running: usize, slots: u32, seed: u64) -> Workload {
    let mut rng = SplitMix64::new(seed);
    let mut jobs = Arena::with_capacity(pending + running);
    let mut inventory = SlotInventory::from_uniform(slots, Resources::new(SLOT_CPU, SLOT_MEM, 4));
    let mut running_jobs = Vec::with_capacity(running);

    for _ in 0..running {
        let cpu = u32::try_from(rng.range(500, 4_000)).expect("range fits u32");
        let mem = rng.range(1 << 28, 4 << 30);
        let resources = Resources::new(cpu, mem, 0);
        let slot = u32::try_from(rng.range(0, u64::from(slots))).expect("range fits u32");
        if inventory.try_allocate(slot, &resources).is_err() {
            continue; // Slot full; the workload is simply a little lighter.
        }
        let walltime = rng.range(30, 3_600);
        let mut job = Job::new(
            AccountId::new(u32::try_from(rng.range(0, 16)).expect("range fits u32")),
            PriorityClass::ALL[usize::try_from(rng.range(0, 4)).expect("range fits usize")],
            VirtualTime::ZERO,
            ResourceRequest::new(cpu, mem, 0),
            VirtualDuration::from_secs(walltime),
        );
        job.try_transition(JobState::Ready)
            .expect("Submitted -> Ready");
        job.try_transition(JobState::Running)
            .expect("Ready -> Running");
        let id = jobs.insert(job);
        running_jobs.push(RunningJob {
            job: id,
            slot,
            resources,
            ends_at: VirtualTime::from_nanos(walltime * SECOND),
        });
    }

    let mut admitted = Vec::with_capacity(pending);
    for _ in 0..pending {
        let cpu = u32::try_from(rng.range(250, u64::from(SLOT_CPU))).expect("range fits u32");
        let mem = rng.range(1 << 27, 8 << 30);
        let job = Job::new(
            AccountId::new(u32::try_from(rng.range(0, 16)).expect("range fits u32")),
            PriorityClass::ALL[usize::try_from(rng.range(0, 4)).expect("range fits usize")],
            VirtualTime::from_nanos(rng.range(0, 3_600 * SECOND)),
            ResourceRequest::new(cpu, mem, 0),
            VirtualDuration::from_secs(rng.range(30, 7_200)),
        );
        // Left `Submitted`: `Scheduler::submit` classifies it.
        admitted.push(jobs.insert(job));
    }

    Workload {
        jobs,
        inventory,
        running: running_jobs,
        admitted,
    }
}

/// The configuration the benchmarks measure against.
#[must_use]
pub fn bench_config() -> CycleConfig {
    CycleConfig {
        priority: PriorityConfig::new(
            PriorityWeights {
                age: 1_000,
                qos: 1_000,
                fairshare: 1_000,
                size: 100,
            },
            VirtualDuration::from_secs(3_600),
            ResourceRequest::new(SLOT_CPU, SLOT_MEM, 4),
        ),
        fairshare: FairShareConfig::default(),
        budget: PackBudget::default(),
        max_candidates: 10_000,
    }
}
