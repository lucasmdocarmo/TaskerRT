//! Dependency eligibility: a reverse index so completing a job wakes its
//! dependents in O(dependents) rather than a scan of every blocked job.

use smallvec::SmallVec;

use crate::domain::{Arena, Job, JobId, JobState};

/// Sentinel: no live job occupies this index.
const NO_OWNER: JobId = JobId::from_bits(u64::MAX);

/// What `register` decided for a freshly submitted job.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Readiness {
    Ready,
    Blocked,
    /// A dependency is already `Failed` or `Cancelled`; the job can never run.
    Cancelled,
}

/// A dependency handle did not resolve to a live job.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
#[error("job {job:?} depends on {dependency:?}, which does not exist")]
pub struct DependencyError {
    pub job: JobId,
    pub dependency: JobId,
}

/// Side tables keyed by `JobId::index()`. `owner` guards every access so a
/// stale handle sharing an index with a live job is never mistaken for it.
#[derive(Clone, Debug, Default)]
pub struct DependencyTracker {
    owner: Vec<JobId>,
    unsatisfied: Vec<u32>,
    dependents: Vec<SmallVec<[JobId; 4]>>,
    /// Work stack for the cascade; reused so cascading allocates nothing.
    stack: Vec<JobId>,
}

impl DependencyTracker {
    /// An empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty tracker preallocated for `slots` arena indices.
    #[must_use]
    pub fn with_slots(slots: usize) -> Self {
        let mut t = Self::new();
        t.reserve_slots(slots);
        t
    }

    /// Grows every table to cover `slots` indices. Idempotent.
    pub fn reserve_slots(&mut self, slots: usize) {
        if self.owner.len() < slots {
            self.owner.resize(slots, NO_OWNER);
            self.unsatisfied.resize(slots, 0);
            // `resize_with` builds each new element from the closure; `SmallVec::new` allocates nothing.
            self.dependents.resize_with(slots, SmallVec::new);
        }
    }

    /// True when `id` is the live occupant of its index.
    #[must_use]
    pub fn is_tracked(&self, id: JobId) -> bool {
        self.owner.get(id.index() as usize) == Some(&id)
    }

    /// Unsatisfied-dependency count, or `None` if `id` is not tracked.
    #[must_use]
    pub fn unsatisfied(&self, id: JobId) -> Option<u32> {
        // `then` (lazy) rather than `then_some`: the index is only safe once tracked.
        self.is_tracked(id)
            .then(|| self.unsatisfied[id.index() as usize])
    }

    fn clear_index(&mut self, index: usize) {
        self.owner[index] = NO_OWNER;
        self.unsatisfied[index] = 0;
        self.dependents[index].clear();
    }

    /// Pass 1 of `register`: validates and classifies without mutating, so a
    /// caller can reject a job before it enters the arena.
    ///
    /// # Errors
    /// `DependencyError` for the first dependency that does not resolve.
    pub fn classify(
        &self,
        id: JobId,
        job: &Job,
        jobs: &Arena<Job>,
    ) -> Result<Readiness, DependencyError> {
        let mut pending = 0_u32;
        for dep in &job.deps {
            // A job could name the id it is about to receive: a cycle of length one.
            if *dep == id {
                return Err(DependencyError {
                    job: id,
                    dependency: *dep,
                });
            }
            let Some(target) = jobs.get(*dep) else {
                return Err(DependencyError {
                    job: id,
                    dependency: *dep,
                });
            };
            match target.state {
                JobState::Completed => {}
                JobState::Failed | JobState::Cancelled => return Ok(Readiness::Cancelled),
                _ => pending += 1,
            }
        }
        Ok(if pending == 0 {
            Readiness::Ready
        } else {
            Readiness::Blocked
        })
    }

    /// Records `job`'s dependencies and classifies it. Every dependency must be
    /// a live job other than itself. Leaves no reverse edges behind when the
    /// result is `Cancelled`.
    ///
    /// # Errors
    /// `DependencyError` for the first dependency that does not resolve.
    pub fn register(
        &mut self,
        id: JobId,
        job: &Job,
        jobs: &Arena<Job>,
    ) -> Result<Readiness, DependencyError> {
        let readiness = self.classify(id, job, jobs)?;
        // A doomed job leaves no reverse edges behind.
        if matches!(readiness, Readiness::Cancelled) {
            return Ok(readiness);
        }

        // Pass 2: commit.
        self.reserve_slots(id.index() as usize + 1);
        let index = id.index() as usize;
        self.clear_index(index);
        self.owner[index] = id;
        let mut pending = 0_u32;
        for dep in &job.deps {
            // Only live, non-terminal deps get a reverse edge; completed ones never fire.
            let is_pending = jobs.get(*dep).is_some_and(|t| {
                !matches!(
                    t.state,
                    JobState::Completed | JobState::Failed | JobState::Cancelled
                )
            });
            if !is_pending {
                continue;
            }
            pending += 1;
            self.reserve_slots(dep.index() as usize + 1);
            let dep_index = dep.index() as usize;
            // A dependency that was never registered (e.g. seeded as already running)
            // is adopted here so its completion still fires.
            if self.owner[dep_index] != *dep {
                self.clear_index(dep_index);
                self.owner[dep_index] = *dep;
            }
            self.dependents[dep_index].push(id);
        }

        self.unsatisfied[index] = pending;
        Ok(readiness)
    }

    /// Decrements each dependent's count; those reaching zero are appended to
    /// `promoted`. Retires `id` afterwards.
    pub fn on_completed(&mut self, id: JobId, promoted: &mut Vec<JobId>) {
        let index = id.index() as usize;
        if !self.is_tracked(id) {
            return;
        }
        // Disjoint-field borrow: `dependents` is read while `owner` and
        // `unsatisfied` are written. The borrow checker permits this because
        // they are separate fields accessed directly, not through `self` methods.
        for dependent in &self.dependents[index] {
            let d = dependent.index() as usize;
            if self.owner[d] != *dependent {
                continue; // retired or stale
            }
            if self.unsatisfied[d] > 0 {
                self.unsatisfied[d] -= 1;
                if self.unsatisfied[d] == 0 {
                    promoted.push(*dependent);
                }
            }
        }
        self.clear_index(index);
    }

    /// Appends every transitive dependent of `id` to `cascade`, each at most
    /// once, and retires all of them plus `id`. Iterative; no recursion.
    pub fn on_terminated(&mut self, id: JobId, cascade: &mut Vec<JobId>) {
        self.stack.clear();
        self.stack.push(id);
        // `while let` pops until the stack is empty.
        while let Some(current) = self.stack.pop() {
            let index = current.index() as usize;
            if self.owner.get(index) != Some(&current) {
                continue; // already visited (retired) or stale
            }
            if current != id {
                cascade.push(current);
            }
            for dependent in &self.dependents[index] {
                self.stack.push(*dependent);
            }
            self.clear_index(index); // marks visited: a second path to this job is skipped
        }
    }

    /// Forgets `id`. Safe to call for untracked handles.
    pub fn retire(&mut self, id: JobId) {
        if self.is_tracked(id) {
            self.clear_index(id.index() as usize);
        }
    }
}
