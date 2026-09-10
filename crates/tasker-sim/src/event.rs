//! Scripted events and the time-ordered queue that delivers them.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use smallvec::SmallVec;
use tasker_core::{Job, JobId, VirtualTime};

use crate::Outcome;

/// Something that happens to the simulated system.
///
/// `Submit` names dependencies by *submit index* (the 0-based position of an
/// earlier `Submit`), because a `JobId` does not exist until the job is inserted.
#[derive(Debug)]
pub enum Event {
    Submit {
        job: Job,
        deps: SmallVec<[usize; 4]>,
        outcome: Outcome,
    },
    Cancel {
        submit_index: usize,
    },
    /// Scheduled when a job is dispatched; `run` says which dispatch, so a
    /// completion left over from a run that was preempted is ignored.
    Complete {
        job: JobId,
        run: u32,
    },
    /// Scheduled when a job is dispatched; see `Complete`.
    Fail {
        job: JobId,
        run: u32,
    },
    /// Scheduled at eviction time, one grace period out.
    Preempted {
        job: JobId,
        run: u32,
    },
}

/// Heap entry. Ordered by `(at, seq)` only; the event itself is payload.
#[derive(Debug)]
struct Entry {
    at: VirtualTime,
    seq: u64,
    event: Event,
}

// Manual impls because `Event` (via `Job`) is not `Eq`/`Ord`, and ordering
// must ignore it anyway.
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Tuple comparison is lexicographic: time first, then insertion order.
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

/// A min-heap of events keyed by time, with insertion order as the tiebreak
/// so delivery is fully deterministic.
#[derive(Debug, Default)]
pub struct EventQueue {
    // `BinaryHeap` is a max-heap; `Reverse` flips it into a min-heap.
    heap: BinaryHeap<Reverse<Entry>>,
    next_seq: u64,
}

impl EventQueue {
    /// An empty queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pending events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// True when nothing is pending.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Schedules `event` at `at`.
    pub fn push(&mut self, at: VirtualTime, event: Event) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(Reverse(Entry { at, seq, event }));
    }

    /// Time of the earliest pending event.
    #[must_use]
    pub fn next_at(&self) -> Option<VirtualTime> {
        // `.0` unwraps the `Reverse`.
        self.heap.peek().map(|entry| entry.0.at)
    }

    /// Removes and returns the earliest event if it is due at or before `until`.
    pub fn pop_due(&mut self, until: VirtualTime) -> Option<(VirtualTime, Event)> {
        // `is_none_or`: an empty heap has nothing due.
        if self.next_at().is_none_or(|at| at > until) {
            return None;
        }
        let Reverse(entry) = self.heap.pop()?;
        Some((entry.at, entry.event))
    }
}
