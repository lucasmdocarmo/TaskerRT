//! Properties that must hold between any two events. Checked after every
//! event when `Simulation::check_invariants` is set.

use tasker_core::{JobId, JobState, SlotIndex};

use crate::Simulation;

/// A property the scheduler promised, and broke.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum Violation {
    #[error("{job:?} is Running but its dependency {dependency:?} is not Completed")]
    RunningWithUnsatisfiedDependency { job: JobId, dependency: JobId },
    #[error("slot {slot} is over capacity")]
    OverCapacity { slot: SlotIndex },
    #[error("{job:?} is Blocked although every dependency is terminal")]
    StuckBlocked { job: JobId },
    #[error("running set has {running_set} entries but {running_jobs} jobs are Running")]
    RunningSetMismatch {
        running_set: usize,
        running_jobs: usize,
    },
    #[error("ready set has {ready_set} entries but {ready_jobs} jobs are Ready")]
    ReadySetMismatch { ready_set: usize, ready_jobs: usize },
}

/// Checks all five invariants. O(jobs + deps); intended for tests, not benchmarks.
///
/// # Errors
/// The first violation found.
pub fn check(sim: &Simulation) -> Result<(), Violation> {
    let mut running_jobs = 0;
    let mut ready_jobs = 0;

    for (id, job) in sim.jobs.iter() {
        match job.state {
            JobState::Running => {
                running_jobs += 1;
                for dep in &job.deps {
                    let done = sim
                        .jobs
                        .get(*dep)
                        .is_some_and(|d| d.state == JobState::Completed);
                    if !done {
                        return Err(Violation::RunningWithUnsatisfiedDependency {
                            job: id,
                            dependency: *dep,
                        });
                    }
                }
            }
            JobState::Ready => ready_jobs += 1,
            JobState::Blocked => {
                // Missing deps count as terminal: they can never complete now.
                let all_terminal = job
                    .deps
                    .iter()
                    .all(|dep| sim.jobs.get(*dep).is_none_or(|d| d.state.is_terminal()));
                if all_terminal {
                    return Err(Violation::StuckBlocked { job: id });
                }
            }
            _ => {}
        }
    }

    for slot in 0..sim.inventory.len() {
        let s = sim.inventory.slot(slot).expect("index below len");
        if !s.allocated.fits_within(&s.capacity) {
            return Err(Violation::OverCapacity { slot });
        }
    }

    let running_set = sim.running.len();
    let consistent = sim.running.iter().all(|r| {
        sim.jobs
            .get(r.job)
            .is_some_and(|j| j.state == JobState::Running)
    });
    if running_set != running_jobs || !consistent {
        return Err(Violation::RunningSetMismatch {
            running_set,
            running_jobs,
        });
    }

    let ready_set = sim.scheduler.pending();
    if ready_set != ready_jobs {
        return Err(Violation::ReadySetMismatch {
            ready_set,
            ready_jobs,
        });
    }

    Ok(())
}
