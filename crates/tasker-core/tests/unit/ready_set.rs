use tasker_core::{JobId, PriorityClass, PriorityHeap, ReadySet};

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
