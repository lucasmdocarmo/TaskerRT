//! The ready set: one indexed binary heap per priority class.

use crate::domain::{JobId, PriorityClass};
use crate::policy::{OrderKey, Score};

/// Sentinel meaning "this job is not in the heap".
const ABSENT: u32 = u32::MAX;

/// A job's position in the ready set.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReadyEntry {
    pub class: PriorityClass,
    pub job: JobId,
    pub score: Score,
}

/// Max-heap over `OrderKey` with O(log n) update and removal by `JobId`.
/// Scores and ids live in parallel arrays; membership is a dense table
/// indexed by `JobId::index`.
#[derive(Clone, Debug, Default)]
pub struct PriorityHeap {
    scores: Vec<Score>,
    ids: Vec<JobId>,
    positions: Vec<u32>,
}

impl PriorityHeap {
    /// An empty heap.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            scores: Vec::new(),
            ids: Vec::new(),
            positions: Vec::new(),
        }
    }

    /// An empty heap preallocated for `slots` arena indices.
    #[must_use]
    pub fn with_slots(slots: usize) -> Self {
        let mut heap = Self::new();
        heap.reserve_slots(slots);
        heap
    }

    /// Grows the position table to cover `slots` arena indices. Idempotent.
    pub fn reserve_slots(&mut self, slots: usize) {
        if self.positions.len() < slots {
            // `resize` extends with copies of the given value.
            self.positions.resize(slots, ABSENT);
        }
        // `reserve` takes *additional* capacity, hence the subtraction.
        self.scores
            .reserve(slots.saturating_sub(self.scores.capacity()));
        self.ids.reserve(slots.saturating_sub(self.ids.capacity()));
    }

    /// Number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Empties the heap, keeping its allocations.
    pub fn clear(&mut self) {
        for id in &self.ids {
            self.positions[id.index() as usize] = ABSENT;
        }
        self.scores.clear();
        self.ids.clear();
    }

    /// True when `job` is in the heap.
    #[must_use]
    pub fn contains(&self, job: JobId) -> bool {
        self.slot_of(job).is_some()
    }

    /// Heap position of `job`, verifying the stored id matches (guards stale handles).
    fn slot_of(&self, job: JobId) -> Option<usize> {
        // `*` copies the `u32` out of the `&u32` that `get` returns.
        let position = *self.positions.get(job.index() as usize)?;
        if position == ABSENT {
            return None;
        }
        let position = position as usize;
        // `then_some` turns a bool into `Some(x)` / `None`.
        (self.ids.get(position) == Some(&job)).then_some(position)
    }

    /// The comparison key at a heap position.
    fn key_at(&self, position: usize) -> OrderKey {
        OrderKey::new(self.scores[position], self.ids[position])
    }

    /// Swaps two heap positions and fixes both entries in the position table.
    fn swap(&mut self, a: usize, b: usize) {
        self.scores.swap(a, b);
        self.ids.swap(a, b);
        self.positions[self.ids[a].index() as usize] =
            u32::try_from(a).expect("heap length fits u32");
        self.positions[self.ids[b].index() as usize] =
            u32::try_from(b).expect("heap length fits u32");
    }

    /// Moves an entry toward the root while it outranks its parent.
    fn sift_up(&mut self, mut position: usize) {
        while position > 0 {
            // In an array heap the parent of `i` is `(i - 1) / 2`.
            let parent = (position - 1) / 2;
            if self.key_at(position) > self.key_at(parent) {
                self.swap(position, parent);
                position = parent;
            } else {
                break;
            }
        }
    }

    /// Moves an entry toward the leaves while a child outranks it.
    fn sift_down(&mut self, mut position: usize) {
        let len = self.ids.len();
        // `loop` runs until an explicit `break`.
        loop {
            let left = position * 2 + 1;
            let right = left + 1;
            let mut largest = position;
            if left < len && self.key_at(left) > self.key_at(largest) {
                largest = left;
            }
            if right < len && self.key_at(right) > self.key_at(largest) {
                largest = right;
            }
            if largest == position {
                break;
            }
            self.swap(position, largest);
            position = largest;
        }
    }

    /// Inserts `job` at `score`, or updates it if already present.
    /// Precondition: at most one `JobId` per arena index at a time.
    pub fn push(&mut self, job: JobId, score: Score) {
        if self.update(job, score) {
            return;
        }
        self.reserve_slots(job.index() as usize + 1);
        let position = self.ids.len();
        self.scores.push(score);
        self.ids.push(job);
        self.positions[job.index() as usize] =
            u32::try_from(position).expect("heap length fits u32");
        self.sift_up(position);
    }

    /// The top entry without removing it.
    #[must_use]
    pub fn peek_max(&self) -> Option<(JobId, Score)> {
        // `first()?` returns `None` on an empty heap before touching `scores[0]`.
        Some((*self.ids.first()?, self.scores[0]))
    }

    /// Removes and returns the top entry.
    pub fn pop_max(&mut self) -> Option<(JobId, Score)> {
        let job = *self.ids.first()?;
        let score = self.scores[0];
        self.remove_at(0);
        Some((job, score))
    }

    /// Sets `job`'s score, restoring heap order. False if absent.
    pub fn update(&mut self, job: JobId, score: Score) -> bool {
        let Some(position) = self.slot_of(job) else {
            return false;
        };
        let previous = self.scores[position];
        self.scores[position] = score;
        // Only one direction can be violated, depending on which way the score moved.
        if score > previous {
            self.sift_up(position);
        } else if score < previous {
            self.sift_down(position);
        }
        true
    }

    /// Removes `job`. False if absent.
    pub fn remove(&mut self, job: JobId) -> bool {
        let Some(position) = self.slot_of(job) else {
            return false;
        };
        self.remove_at(position);
        true
    }

    /// Removes the entry at a heap position by swapping in the last element.
    fn remove_at(&mut self, position: usize) {
        let last = self.ids.len() - 1;
        self.positions[self.ids[position].index() as usize] = ABSENT;
        if position == last {
            self.scores.pop();
            self.ids.pop();
            return;
        }
        self.scores.swap(position, last);
        self.ids.swap(position, last);
        self.scores.pop();
        self.ids.pop();
        self.positions[self.ids[position].index() as usize] =
            u32::try_from(position).expect("heap length fits u32");
        // The moved element may need to go either way; only one call will move it.
        self.sift_down(position);
        self.sift_up(position);
    }
}

/// One `PriorityHeap` per class, drained highest class first. Class strictly
/// dominates score.
#[derive(Clone, Debug, Default)]
pub struct ReadySet {
    buckets: [PriorityHeap; PriorityClass::COUNT],
}

impl ReadySet {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty set preallocated for `slots` arena indices.
    #[must_use]
    pub fn with_slots(slots: usize) -> Self {
        let mut set = Self::new();
        set.reserve_slots(slots);
        set
    }

    /// Preallocates every bucket.
    pub fn reserve_slots(&mut self, slots: usize) {
        for bucket in &mut self.buckets {
            bucket.reserve_slots(slots);
        }
    }

    /// Adds or rescores a job in its class bucket.
    pub fn insert(&mut self, class: PriorityClass, job: JobId, score: Score) {
        self.buckets[class.ordinal() as usize].push(job, score);
    }

    /// Removes a job from its class bucket. False if absent.
    pub fn remove(&mut self, class: PriorityClass, job: JobId) -> bool {
        self.buckets[class.ordinal() as usize].remove(job)
    }

    /// Total entries across all classes.
    #[must_use]
    pub fn len(&self) -> usize {
        // `map` then `sum` folds the per-bucket lengths into one number.
        self.buckets.iter().map(PriorityHeap::len).sum()
    }

    /// True when every bucket is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(PriorityHeap::is_empty)
    }

    /// Empties every bucket, keeping allocations.
    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            bucket.clear();
        }
    }

    /// The next entry to consider, without removing it.
    #[must_use]
    pub fn peek_next(&self) -> Option<ReadyEntry> {
        // `rev()` walks the classes highest first.
        for class in PriorityClass::ALL.iter().rev() {
            if let Some((job, score)) = self.buckets[class.ordinal() as usize].peek_max() {
                return Some(ReadyEntry {
                    class: *class,
                    job,
                    score,
                });
            }
        }
        None
    }

    /// Removes and returns the next entry.
    pub fn pop_next(&mut self) -> Option<ReadyEntry> {
        for class in PriorityClass::ALL.iter().rev() {
            if let Some((job, score)) = self.buckets[class.ordinal() as usize].pop_max() {
                return Some(ReadyEntry {
                    class: *class,
                    job,
                    score,
                });
            }
        }
        None
    }

    /// Moves up to `limit` entries, in priority order, into `out` (cleared first).
    pub fn drain_ordered_into(&mut self, out: &mut Vec<ReadyEntry>, limit: usize) {
        out.clear();
        for _ in 0..limit {
            match self.pop_next() {
                Some(entry) => out.push(entry),
                None => break,
            }
        }
    }

    /// Returns a drained entry to its bucket.
    pub fn reinsert(&mut self, entry: ReadyEntry) {
        self.insert(entry.class, entry.job, entry.score);
    }
}
