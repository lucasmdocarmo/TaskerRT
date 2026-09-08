//! Seeded scenario generation. Same seed and parameters → same event list.

use smallvec::{SmallVec, smallvec};
use tasker_core::{AccountId, Job, PriorityClass, ResourceRequest, VirtualDuration, VirtualTime};

use crate::{Event, Outcome, SplitMix64};

/// The dependency structure to generate. Every edge points to a lower index.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shape {
    Independent,
    /// Job `i` depends on job `i - 1`.
    Chain,
    /// Job `i` depends on job `(i - 1) / fan_out`.
    Tree {
        fan_out: usize,
    },
    /// Job `i` depends on every job in the previous layer of `width`.
    Layered {
        width: usize,
    },
}

/// Knobs for `generate`.
#[derive(Clone, Copy, Debug)]
pub struct ScenarioParams {
    pub jobs: usize,
    pub shape: Shape,
    /// Arrivals are spread evenly over this span, in index order.
    pub arrival_span: VirtualDuration,
    pub cpu_range: (u32, u32),
    pub walltime_secs: (u64, u64),
    /// Per-job probability of a scripted failure, out of 1000.
    pub fail_permille: u32,
    /// Per-job probability of a scripted cancel, out of 1000.
    pub cancel_permille: u32,
}

impl Default for ScenarioParams {
    fn default() -> Self {
        Self {
            jobs: 32,
            shape: Shape::Layered { width: 4 },
            arrival_span: VirtualDuration::from_secs(60),
            cpu_range: (250, 4_000),
            walltime_secs: (5, 120),
            fail_permille: 50,
            cancel_permille: 20,
        }
    }
}

fn deps_for(index: usize, shape: Shape) -> SmallVec<[usize; 4]> {
    // The first job has nothing to depend on under any shape.
    if index == 0 {
        return smallvec![];
    }
    match shape {
        Shape::Independent => smallvec![],
        Shape::Chain => smallvec![index - 1],
        Shape::Tree { fan_out } => smallvec![(index - 1) / fan_out.max(1)],
        Shape::Layered { width } => {
            let width = width.max(1);
            let layer = index / width;
            if layer == 0 {
                smallvec![]
            } else {
                // `collect` into a SmallVec spills to the heap past 4 — fine at generation time.
                ((layer - 1) * width..layer * width).collect()
            }
        }
    }
}

/// Builds the event list for one scenario. Arrivals are non-decreasing in
/// index, so a dependency is always submitted no later than its dependent.
#[must_use]
pub fn generate(seed: u64, p: &ScenarioParams) -> Vec<(VirtualTime, Event)> {
    let mut rng = SplitMix64::new(seed);
    let mut events = Vec::with_capacity(p.jobs * 2);
    let jobs = u64::try_from(p.jobs.max(1)).expect("job count fits u64");

    for index in 0..p.jobs {
        let i = u64::try_from(index).expect("index fits u64");
        let at = VirtualTime::from_nanos(p.arrival_span.as_nanos() * i / jobs);

        let cpu = u32::try_from(rng.range(u64::from(p.cpu_range.0), u64::from(p.cpu_range.1)))
            .expect("range fits u32");
        let walltime = rng.range(p.walltime_secs.0, p.walltime_secs.1 + 1);
        let class_index = usize::try_from(rng.range(0, 4)).expect("fits usize");
        let account = u32::try_from(rng.range(0, 8)).expect("fits u32");

        let job = Job::new(
            AccountId::new(account),
            PriorityClass::ALL[class_index],
            at,
            ResourceRequest::new(cpu, 0, 0),
            VirtualDuration::from_secs(walltime),
        );

        let outcome = if rng.chance(p.fail_permille) {
            Outcome::Fails {
                after: VirtualDuration::from_secs(rng.range(1, walltime + 1)),
            }
        } else {
            // Up to 25% past the walltime, so the kill-at-limit path is exercised too.
            let after = rng.range(1, walltime + walltime / 4 + 2);
            Outcome::Completes {
                after: VirtualDuration::from_secs(after),
            }
        };

        events.push((
            at,
            Event::Submit {
                job,
                deps: deps_for(index, p.shape),
                outcome,
            },
        ));

        if rng.chance(p.cancel_permille) {
            let delay = VirtualDuration::from_secs(rng.range(1, walltime + 10));
            events.push((
                at.saturating_add(delay),
                Event::Cancel {
                    submit_index: index,
                },
            ));
        }
    }

    events
}
