//! An append-only record of everything a simulation did. Two runs of the
//! same seed must produce equal traces; that equality is the determinism test.

use tasker_core::{JobId, JobState, SlotIndex, VirtualTime};

/// One thing that happened.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Action {
    Submitted {
        state: JobState,
    },
    Dispatched {
        slot: SlotIndex,
    },
    Completed,
    Failed,
    Cancelled,
    Promoted,
    /// Chosen as a victim; its worker has the grace period to stop it.
    Preempted,
    /// The eviction was confirmed and the job is Ready again.
    Requeued,
    Cycle {
        dispatched: usize,
        backfilled: usize,
    },
}

/// When and to whom. `job` is `None` for cycle-level records.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TraceRecord {
    pub at: VirtualTime,
    pub job: Option<JobId>,
    pub action: Action,
}

/// The full record, in order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trace {
    records: Vec<TraceRecord>,
}

impl Trace {
    /// An empty trace.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    /// Appends one record.
    pub fn push(&mut self, record: TraceRecord) {
        self.records.push(record);
    }

    /// Number of records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// True when nothing has been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Every record, in order.
    #[must_use]
    pub fn records(&self) -> &[TraceRecord] {
        &self.records
    }
}
