//! The job record and its state machine (spec §5.1, §5.2).

use smallvec::SmallVec;

use crate::{JobId, ResourceRequest, VirtualDuration, VirtualTime};

/// Identifies the account a job is charged to, for M5 fair-share.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct AccountId(u32);

impl AccountId {
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Quality-of-service tier. Ordering is significant: the ready set keeps one
/// bucket per class and drains them highest-first.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[repr(u8)]
pub enum PriorityClass {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Urgent = 3,
}

impl PriorityClass {
    /// Number of classes. Sizes the ready set's bucket array.
    pub const COUNT: usize = 4;

    /// Every class, lowest first.
    pub const ALL: [Self; Self::COUNT] = [Self::Low, Self::Normal, Self::High, Self::Urgent];

    /// Dense index in `0..COUNT`.
    #[must_use]
    pub const fn ordinal(self) -> u8 {
        self as u8
    }
}

/// Where a job is in its lifecycle (spec §5.2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum JobState {
    Submitted,
    Blocked,
    Ready,
    Running,
    Completed,
    Failed,
    Preempted,
    Cancelled,
}

impl JobState {
    /// Every state, for exhaustive tests.
    pub const ALL: [Self; 8] = [
        Self::Submitted,
        Self::Blocked,
        Self::Ready,
        Self::Running,
        Self::Completed,
        Self::Failed,
        Self::Preempted,
        Self::Cancelled,
    ];

    /// True when no transition out of this state exists.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// Encodes the state diagram in spec §5.2. This is the single source of
    /// truth for legality; nothing else may assign `Job::state`.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Submitted,
                Self::Blocked | Self::Ready | Self::Cancelled
            ) | (Self::Blocked, Self::Ready | Self::Cancelled)
                | (Self::Ready, Self::Running | Self::Cancelled)
                | (
                    Self::Running,
                    Self::Completed | Self::Failed | Self::Preempted | Self::Cancelled
                )
                | (Self::Preempted, Self::Ready)
        )
    }
}

/// An illegal state transition was attempted.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
#[error("illegal job state transition from {from:?} to {to:?}")]
pub struct TransitionError {
    pub from: JobState,
    pub to: JobState,
}

/// Inline capacity for dependency lists. Four covers the overwhelming majority
/// of DAG fan-in without touching the allocator (spec §7.2).
pub type Deps = SmallVec<[JobId; 4]>;

/// A unit of work (spec §5.1).
#[derive(Clone, Debug)]
pub struct Job {
    pub account: AccountId,
    pub priority_class: PriorityClass,
    pub submit_time: VirtualTime,
    pub request: ResourceRequest,
    /// Required, never optional: backfill cannot compute a reservation without
    /// an upper bound on occupancy (spec §5.1).
    pub walltime_limit: VirtualDuration,
    /// Present from M1 but unread until M2 adds DAG eligibility.
    pub deps: Deps,
    /// Opaque bytes handed to the worker in M3.
    pub payload: Vec<u8>,
    pub state: JobState,
}

impl Job {
    /// A job in [`JobState::Submitted`] with no dependencies and no payload.
    #[must_use]
    pub fn new(
        account: AccountId,
        priority_class: PriorityClass,
        submit_time: VirtualTime,
        request: ResourceRequest,
        walltime_limit: VirtualDuration,
    ) -> Self {
        Self {
            account,
            priority_class,
            submit_time,
            request,
            walltime_limit,
            deps: Deps::new(),
            payload: Vec::new(),
            state: JobState::Submitted,
        }
    }

    /// Moves to `next` if the transition is legal, leaving the job untouched
    /// otherwise.
    ///
    /// # Errors
    /// [`TransitionError`] when the transition is not in spec §5.2.
    pub fn try_transition(&mut self, next: JobState) -> Result<(), TransitionError> {
        if self.state.can_transition_to(next) {
            self.state = next;
            Ok(())
        } else {
            Err(TransitionError {
                from: self.state,
                to: next,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ResourceRequest, VirtualDuration, VirtualTime};

    fn a_job() -> Job {
        Job::new(
            AccountId::new(1),
            PriorityClass::Normal,
            VirtualTime::from_nanos(0),
            ResourceRequest::new(1_000, 1 << 30, 0),
            VirtualDuration::from_secs(60),
        )
    }

    #[test]
    fn a_new_job_starts_submitted() {
        assert_eq!(a_job().state, JobState::Submitted);
    }

    #[test]
    fn submitted_may_go_blocked_ready_or_cancelled() {
        assert!(JobState::Submitted.can_transition_to(JobState::Blocked));
        assert!(JobState::Submitted.can_transition_to(JobState::Ready));
        assert!(JobState::Submitted.can_transition_to(JobState::Cancelled));
        assert!(!JobState::Submitted.can_transition_to(JobState::Running));
    }

    #[test]
    fn preempted_requeues_to_ready_only() {
        assert!(JobState::Preempted.can_transition_to(JobState::Ready));
        assert!(!JobState::Preempted.can_transition_to(JobState::Running));
        assert!(!JobState::Preempted.can_transition_to(JobState::Completed));
    }

    #[test]
    fn terminal_states_have_no_successors() {
        for terminal in [JobState::Completed, JobState::Failed, JobState::Cancelled] {
            assert!(terminal.is_terminal());
            for next in JobState::ALL {
                assert!(
                    !terminal.can_transition_to(next),
                    "{terminal:?} must not reach {next:?}"
                );
            }
        }
    }

    #[test]
    fn cancellation_is_reachable_from_blocked_ready_and_running() {
        for from in [JobState::Blocked, JobState::Ready, JobState::Running] {
            assert!(from.can_transition_to(JobState::Cancelled));
        }
    }

    #[test]
    fn try_transition_rejects_illegal_moves() {
        let mut job = a_job();
        assert!(job.try_transition(JobState::Ready).is_ok());
        assert_eq!(job.state, JobState::Ready);
        let err = job.try_transition(JobState::Completed).unwrap_err();
        assert_eq!(
            err,
            TransitionError {
                from: JobState::Ready,
                to: JobState::Completed
            }
        );
        assert_eq!(
            job.state,
            JobState::Ready,
            "a rejected transition must not mutate"
        );
    }

    #[test]
    fn priority_classes_are_ordered_low_to_urgent() {
        assert!(PriorityClass::Low < PriorityClass::Normal);
        assert!(PriorityClass::Normal < PriorityClass::High);
        assert!(PriorityClass::High < PriorityClass::Urgent);
        assert_eq!(PriorityClass::COUNT, 4);
        assert_eq!(PriorityClass::Urgent.ordinal(), 3);
    }
}
