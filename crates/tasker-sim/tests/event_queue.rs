use tasker_core::{JobId, VirtualDuration, VirtualTime};
use tasker_sim::{Event, EventQueue, VirtualClock};

fn t(secs: u64) -> VirtualTime {
    VirtualTime::from_nanos(secs * 1_000_000_000)
}

#[test]
fn events_pop_in_time_order_regardless_of_push_order() {
    let mut q = EventQueue::new();
    q.push(t(30), Event::Complete(JobId::from_bits(3)));
    q.push(t(10), Event::Complete(JobId::from_bits(1)));
    q.push(t(20), Event::Complete(JobId::from_bits(2)));

    let mut order = Vec::new();
    while let Some((at, Event::Complete(id))) = q.pop_due(t(100)) {
        order.push((at, id.to_bits()));
    }
    assert_eq!(order, vec![(t(10), 1), (t(20), 2), (t(30), 3)]);
}

#[test]
fn same_instant_events_pop_in_insertion_order() {
    let mut q = EventQueue::new();
    for n in 0..5_u64 {
        q.push(t(7), Event::Complete(JobId::from_bits(n)));
    }
    let mut order = Vec::new();
    while let Some((_, Event::Complete(id))) = q.pop_due(t(7)) {
        order.push(id.to_bits());
    }
    assert_eq!(order, vec![0, 1, 2, 3, 4], "sequence number breaks the tie");
}

#[test]
fn pop_due_leaves_future_events_in_place() {
    let mut q = EventQueue::new();
    q.push(t(5), Event::Complete(JobId::from_bits(0)));
    q.push(t(15), Event::Complete(JobId::from_bits(1)));
    assert!(q.pop_due(t(10)).is_some());
    assert!(q.pop_due(t(10)).is_none(), "t=15 is not yet due");
    assert_eq!(q.len(), 1);
    assert_eq!(q.next_at(), Some(t(15)));
}

#[test]
fn clock_is_monotonic() {
    let mut c = VirtualClock::new();
    c.advance_to(t(5));
    c.advance_by(VirtualDuration::from_secs(2));
    assert_eq!(c.now(), t(7));
}

#[test]
#[should_panic(expected = "moved backwards")]
fn clock_refuses_to_go_backwards() {
    let mut c = VirtualClock::new();
    c.advance_to(t(5));
    c.advance_to(t(4));
}
