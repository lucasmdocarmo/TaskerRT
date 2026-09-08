//! The ready set: one indexed binary heap per priority class (spec §6 step 4).
//!
//! Two structural choices come from spec §7.2. The heap stores scores and job
//! ids in parallel arrays rather than one array of pairs, so the comparison
//! pass touches half as many cache lines. And membership is tracked in a dense
//! `Vec` indexed by [`JobId::index`] rather than a hash map, so lookup is one
//! indexed load with no hashing and no probe.

use crate::{JobId, OrderKey, PriorityClass, Score};

/// Sentinel for "this job is not in the heap".
const ABSENT: u32 = u32::MAX;

/// A job's position in the ready set.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReadyEntry {
    pub class: PriorityClass,
    pub job: JobId,
    pub score: Score,
}

/// A max-heap over [`OrderKey`] supporting O(log n) update and removal by
/// [`JobId`].
#[derive(Clone, Debug, Default)]
pub struct PriorityHeap {
    scores: Vec<Score>,
    ids: Vec<JobId>,
    /// `positions[job.index()]` is the heap slot holding that job, or [`ABSENT`].
    positions: Vec<u32>,
}

impl PriorityHeap {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            scores: Vec::new(),
            ids: Vec::new(),
            positions: Vec::new(),
        }
    }

    /// Preallocates for `slots` distinct arena indices, so steady-state
    /// operation never allocates.
    #[must_use]
    pub fn with_slots(slots: usize) -> Self {
        let mut heap = Self::new();
        heap.reserve_slots(slots);
        heap
    }

    /// Grows the position table to cover `slots` arena indices. Idempotent and
    /// safe to call as the arena grows.
    pub fn reserve_slots(&mut self, slots: usize) {
        if self.positions.len() < slots {
            self.positions.resize(slots, ABSENT);
        }
        self.scores
            .reserve(slots.saturating_sub(self.scores.capacity()));
        self.ids.reserve(slots.saturating_sub(self.ids.capacity()));
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Empties the heap, keeping every allocation for reuse.
    pub fn clear(&mut self) {
        for id in &self.ids {
            self.positions[id.index() as usize] = ABSENT;
        }
        self.scores.clear();
        self.ids.clear();
    }

    #[must_use]
    pub fn contains(&self, job: JobId) -> bool {
        self.slot_of(job).is_some()
    }

    fn slot_of(&self, job: JobId) -> Option<usize> {
        let position = *self.positions.get(job.index() as usize)?;
        if position == ABSENT {
            return None;
        }
        let position = position as usize;
        // A stale handle can share an index with a live job, so confirm identity.
        (self.ids.get(position) == Some(&job)).then_some(position)
    }

    // Not `const`: indexing a `Vec` is not a const operation.
    fn key_at(&self, position: usize) -> OrderKey {
        OrderKey::new(self.scores[position], self.ids[position])
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.scores.swap(a, b);
        self.ids.swap(a, b);
        self.positions[self.ids[a].index() as usize] =
            u32::try_from(a).expect("heap length fits u32");
        self.positions[self.ids[b].index() as usize] =
            u32::try_from(b).expect("heap length fits u32");
    }

    fn sift_up(&mut self, mut position: usize) {
        while position > 0 {
            let parent = (position - 1) / 2;
            if self.key_at(position) > self.key_at(parent) {
                self.swap(position, parent);
                position = parent;
            } else {
                break;
            }
        }
    }

    fn sift_down(&mut self, mut position: usize) {
        let len = self.ids.len();
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

    /// Inserts `job` at `score`. If already present, this is equivalent to
    /// [`PriorityHeap::update`].
    ///
    /// # Preconditions
    /// At most one [`JobId`] per arena index may be in the heap at a time.
    /// `positions` is keyed by index alone, so two handles sharing an index but
    /// differing in generation would collide. The arena guarantees one live job
    /// per index, and only live jobs are ever admitted, so this holds.
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

    /// The highest-priority entry, without removing it.
    #[must_use]
    pub fn peek_max(&self) -> Option<(JobId, Score)> {
        Some((*self.ids.first()?, self.scores[0]))
    }

    /// Removes and returns the highest-priority entry.
    pub fn pop_max(&mut self) -> Option<(JobId, Score)> {
        let job = *self.ids.first()?;
        let score = self.scores[0];
        self.remove_at(0);
        Some((job, score))
    }

    /// Sets `job`'s score, restoring the heap order. Returns false when `job`
    /// is not present.
    pub fn update(&mut self, job: JobId, score: Score) -> bool {
        let Some(position) = self.slot_of(job) else {
            return false;
        };
        let previous = self.scores[position];
        self.scores[position] = score;
        if score > previous {
            self.sift_up(position);
        } else if score < previous {
            self.sift_down(position);
        }
        true
    }

    /// Removes `job`. Returns false when it is not present.
    pub fn remove(&mut self, job: JobId) -> bool {
        let Some(position) = self.slot_of(job) else {
            return false;
        };
        self.remove_at(position);
        true
    }

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
        // The moved element may belong either way from here.
        self.sift_down(position);
        self.sift_up(position);
    }
}

/// The ready set: one [`PriorityHeap`] per [`PriorityClass`], drained
/// highest-class first.
///
/// Class strictly dominates score. An `Urgent` job outranks a `Low` job of any
/// score, which is what makes the QoS tier a guarantee rather than a nudge.
#[derive(Clone, Debug, Default)]
pub struct ReadySet {
    buckets: [PriorityHeap; PriorityClass::COUNT],
}

impl ReadySet {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_slots(slots: usize) -> Self {
        let mut set = Self::new();
        set.reserve_slots(slots);
        set
    }

    /// Preallocates every bucket for `slots` arena indices.
    pub fn reserve_slots(&mut self, slots: usize) {
        for bucket in &mut self.buckets {
            bucket.reserve_slots(slots);
        }
    }

    pub fn insert(&mut self, class: PriorityClass, job: JobId, score: Score) {
        self.buckets[class.ordinal() as usize].push(job, score);
    }

    pub fn remove(&mut self, class: PriorityClass, job: JobId) -> bool {
        self.buckets[class.ordinal() as usize].remove(job)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.buckets.iter().map(PriorityHeap::len).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(PriorityHeap::is_empty)
    }

    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            bucket.clear();
        }
    }

    /// The next entry to consider, without removing it.
    #[must_use]
    pub fn peek_next(&self) -> Option<ReadyEntry> {
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

    /// Moves up to `limit` entries, in priority order, into `out`.
    ///
    /// `out` is cleared first and is expected to be a scratch buffer the caller
    /// reuses across cycles, so a steady-state cycle allocates nothing. This is
    /// how spec §6's per-cycle work bound is applied to the ordering stage.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobId;

    fn id(n: u32) -> JobId {
        JobId::from_bits(u64::from(n))
    }

    #[test]
    fn pop_max_returns_the_highest_score_first() {
        let mut heap = PriorityHeap::with_slots(8);
        heap.push(id(0), 10);
        heap.push(id(1), 30);
        heap.push(id(2), 20);
        assert_eq!(heap.pop_max(), Some((id(1), 30)));
        assert_eq!(heap.pop_max(), Some((id(2), 20)));
        assert_eq!(heap.pop_max(), Some((id(0), 10)));
        assert_eq!(heap.pop_max(), None);
    }

    #[test]
    fn equal_scores_break_toward_the_lower_job_id() {
        let mut heap = PriorityHeap::with_slots(8);
        heap.push(id(5), 100);
        heap.push(id(2), 100);
        assert_eq!(heap.pop_max(), Some((id(2), 100)));
        assert_eq!(heap.pop_max(), Some((id(5), 100)));
    }

    #[test]
    fn update_reorders_in_place() {
        let mut heap = PriorityHeap::with_slots(8);
        heap.push(id(0), 10);
        heap.push(id(1), 20);
        assert!(heap.update(id(0), 99));
        assert_eq!(heap.peek_max(), Some((id(0), 99)));
        assert!(heap.update(id(0), 1));
        assert_eq!(heap.peek_max(), Some((id(1), 20)));
        assert_eq!(heap.len(), 2, "update never changes membership");
    }

    #[test]
    fn update_of_an_absent_job_is_false() {
        let mut heap = PriorityHeap::with_slots(8);
        assert!(!heap.update(id(3), 5));
    }

    #[test]
    fn remove_takes_an_interior_element() {
        let mut heap = PriorityHeap::with_slots(8);
        for (n, score) in [(0, 10), (1, 20), (2, 30), (3, 40)] {
            heap.push(id(n), score);
        }
        assert!(heap.remove(id(1)));
        assert!(!heap.contains(id(1)));
        assert_eq!(heap.len(), 3);
        let drained: Vec<_> = std::iter::from_fn(|| heap.pop_max()).collect();
        assert_eq!(drained, vec![(id(3), 40), (id(2), 30), (id(0), 10)]);
    }

    #[test]
    fn removing_an_absent_job_is_false() {
        let mut heap = PriorityHeap::with_slots(8);
        heap.push(id(0), 1);
        assert!(!heap.remove(id(7)));
    }

    #[test]
    fn class_dominates_score() {
        let mut set = ReadySet::with_slots(8);
        set.insert(PriorityClass::Low, id(0), 999_999);
        set.insert(PriorityClass::Urgent, id(1), 1);
        let first = set.pop_next().unwrap();
        assert_eq!(first.class, PriorityClass::Urgent);
        assert_eq!(first.job, id(1));
        assert_eq!(set.pop_next().unwrap().job, id(0));
    }

    #[test]
    fn peek_does_not_consume() {
        let mut set = ReadySet::with_slots(8);
        set.insert(PriorityClass::Normal, id(0), 5);
        assert_eq!(set.peek_next().map(|e| e.job), Some(id(0)));
        assert_eq!(set.len(), 1);
        assert_eq!(set.pop_next().map(|e| e.job), Some(id(0)));
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn drain_ordered_respects_the_limit_and_leaves_the_rest() {
        let mut set = ReadySet::with_slots(8);
        for n in 0..5_u32 {
            set.insert(PriorityClass::Normal, id(n), u64::from(n));
        }
        let mut out = Vec::new();
        set.drain_ordered_into(&mut out, 3);
        assert_eq!(
            out.iter().map(|e| e.job).collect::<Vec<_>>(),
            vec![id(4), id(3), id(2)]
        );
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn reinsert_restores_an_entry() {
        let mut set = ReadySet::with_slots(8);
        set.insert(PriorityClass::High, id(1), 7);
        let entry = set.pop_next().unwrap();
        assert!(set.is_empty());
        set.reinsert(entry);
        assert_eq!(set.pop_next(), Some(entry));
    }
}
