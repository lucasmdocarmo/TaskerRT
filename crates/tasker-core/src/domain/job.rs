//! The job record and its state machine.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::domain::{JobId, ResourceRequest, VirtualDuration, VirtualTime};

/// The account a job is charged to. Unused until fair-share (M5).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct AccountId(u32);

impl AccountId {
    /// Wraps a raw account number.
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// The raw account number.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Quality-of-service tier. Higher variants outrank lower ones outright.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[repr(u8)] // fixes the discriminant size so `as u8` is a stable, cheap index
pub enum PriorityClass {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Urgent = 3,
}

impl PriorityClass {
    /// Number of classes; sizes the ready set's bucket array.
    pub const COUNT: usize = 4;

    /// Every class, lowest first.
    pub const ALL: [Self; Self::COUNT] = [Self::Low, Self::Normal, Self::High, Self::Urgent];

    /// Dense index in `0..COUNT`.
    #[must_use]
    pub const fn ordinal(self) -> u8 {
        // `as u8` reads the `#[repr(u8)]` discriminant directly.
        self as u8
    }
}

/// Where a job is in its lifecycle.
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

    /// True when no transition leaves this state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        // `matches!` is a boolean pattern test: true if `self` fits any listed variant.
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// True when `self -> next` is a legal transition. The whole state diagram
    /// lives in this one pattern.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        // Matching on a tuple lets one pattern name both the source and the target.
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
#[error("illegal job state transition from {from:?} to {to:?}")] // `thiserror` derives Display from this
pub struct TransitionError {
    pub from: JobState,
    pub to: JobState,
}

/// Dependency list with four ids stored inline before spilling to the heap.
pub type Deps = SmallVec<[JobId; 4]>;

/// A unit of work.
#[derive(Clone, Debug)]
pub struct Job {
    pub account: AccountId,
    pub priority_class: PriorityClass,
    pub submit_time: VirtualTime,
    pub request: ResourceRequest,
    /// Required: backfill needs an upper bound on how long a job can occupy capacity.
    pub walltime_limit: VirtualDuration,
    /// Unread until DAG eligibility (M2).
    pub deps: Deps,
    /// Opaque bytes for the worker. Cloning is a refcount bump.
    pub payload: Bytes,
    pub state: JobState,
}

impl Job {
    /// A `Submitted` job with no dependencies and no payload.
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
            // `SmallVec::new()` allocates nothing; the first four ids live inline.
            deps: Deps::new(),
            payload: Bytes::new(),
            state: JobState::Submitted,
        }
    }

    /// Moves to `next` if legal; otherwise returns the error and changes nothing.
    ///
    /// # Errors
    /// `TransitionError` when the state diagram has no such edge.
    pub fn try_transition(&mut self, next: JobState) -> Result<(), TransitionError> {
        if self.state.can_transition_to(next) {
            self.state = next;
            // `Ok(())` is the unit value wrapped in the success variant.
            Ok(())
        } else {
            Err(TransitionError {
                from: self.state,
                to: next,
            })
        }
    }
}
