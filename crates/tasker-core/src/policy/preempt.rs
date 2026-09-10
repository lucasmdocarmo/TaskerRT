//! Preemption: when a head job cannot be placed, evict strictly lower-class
//! running jobs on one slot so it fits. Runs only when a cycle ended with a head.

use crate::cluster::{SlotIndex, SlotInventory};
use crate::domain::{
    Arena, Job, JobId, JobState, PriorityClass, Resources, VirtualDuration, VirtualTime,
};
use crate::policy::RunningJob;

/// Who may evict whom, and how long a victim gets to stop.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PreemptConfig {
    /// Heads of this class or higher may evict; `None` disables preemption.
    pub min_class: Option<PriorityClass>,
    /// A job evicted this many times becomes immune.
    pub max_preemptions: u8,
    /// How long a victim's worker gets to stop it cooperatively.
    pub grace: VirtualDuration,
}

impl Default for PreemptConfig {
    fn default() -> Self {
        Self {
            min_class: None,
            max_preemptions: 3,
            grace: VirtualDuration::from_secs(5),
        }
    }
}

/// One running job to evict from one slot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Eviction {
    pub job: JobId,
    pub slot: SlotIndex,
}

/// A candidate victim: sorted by class, then youngest first.
#[derive(Clone, Copy)]
struct Victim {
    class: PriorityClass,
    started: VirtualTime,
    job: JobId,
    resources: Resources,
}

/// Chooses victims for `head`, appends them to `out`, and returns their slot.
/// `None` when preemption is off, the head is below `min_class`, or no slot can
/// be freed enough. Lowest class first, then the youngest (least work lost);
/// among slots, the fewest victims, then the least lost runtime.
pub fn plan_preemption(
    head: &Job,
    running: &[RunningJob],
    jobs: &Arena<Job>,
    inventory: &SlotInventory,
    now: VirtualTime,
    config: &PreemptConfig,
    out: &mut Vec<Eviction>,
) -> Option<SlotIndex> {
    let min = config.min_class?;
    if head.priority_class < min {
        return None;
    }
    // One eviction at a time: victims in their grace are no longer Running, so
    // planning again would pick a second set for the same head.
    let in_flight = running.iter().any(|r| {
        jobs.get(r.job)
            .is_some_and(|j| j.state == JobState::Preempted)
    });
    if in_flight {
        return None;
    }
    // (slot, victims, lost runtime in nanoseconds) of the best plan so far.
    let mut best: Option<(SlotIndex, usize, u64)> = None;
    let mut chosen: Vec<Victim> = Vec::new();
    let mut candidates: Vec<Victim> = Vec::new();
    for slot in 0..inventory.len() {
        let Some(entry) = inventory.slot(slot) else {
            continue;
        };
        if !head.request.fits_within(&entry.capacity) {
            continue;
        }
        candidates.clear();
        for r in running.iter().filter(|r| r.slot == slot) {
            let Some(job) = jobs.get(r.job) else {
                continue;
            };
            let eligible = job.state == JobState::Running
                && job.priority_class < head.priority_class
                && job.preemptions < config.max_preemptions;
            if !eligible {
                continue;
            }
            // The running set stores the deadline; the start is one subtraction away.
            let started = VirtualTime::from_nanos(
                r.ends_at
                    .as_nanos()
                    .saturating_sub(job.walltime_limit.as_nanos()),
            );
            candidates.push(Victim {
                class: job.priority_class,
                started,
                job: r.job,
                resources: r.resources,
            });
        }
        // `sort_by` is stable, so equal keys keep dispatch order.
        candidates.sort_by(|a, b| {
            a.class
                .cmp(&b.class)
                .then_with(|| b.started.cmp(&a.started))
        });
        let mut freed = entry.free();
        let mut lost = 0_u64;
        let mut count = 0_usize;
        for v in &candidates {
            if head.request.fits_within(&freed) {
                break;
            }
            freed = freed.saturating_add(&v.resources);
            lost = lost.saturating_add(now.saturating_sub_time(v.started).as_nanos());
            count += 1;
        }
        if count == 0 || !head.request.fits_within(&freed) {
            continue;
        }
        let better = match best {
            None => true,
            Some((_, n, l)) => count < n || (count == n && lost < l),
        };
        if better {
            best = Some((slot, count, lost));
            chosen.clear();
            chosen.extend(candidates.iter().take(count));
        }
    }
    let (slot, _, _) = best?;
    out.extend(chosen.iter().map(|v| Eviction { job: v.job, slot }));
    Some(slot)
}
