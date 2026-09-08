use tasker_core::{VirtualDuration, VirtualTime};

#[test]
fn add_duration_advances_time() {
    let t = VirtualTime::from_nanos(100).saturating_add(VirtualDuration::from_nanos(50));
    assert_eq!(t.as_nanos(), 150);
}

#[test]
fn subtracting_a_later_time_saturates_to_zero() {
    let earlier = VirtualTime::from_nanos(10);
    let later = VirtualTime::from_nanos(90);
    assert_eq!(earlier.saturating_sub_time(later), VirtualDuration::ZERO);
    assert_eq!(later.saturating_sub_time(earlier).as_nanos(), 80);
}

#[test]
fn add_saturates_at_max() {
    let t = VirtualTime::from_nanos(u64::MAX).saturating_add(VirtualDuration::from_nanos(1));
    assert_eq!(t.as_nanos(), u64::MAX);
}

#[test]
fn from_secs_converts() {
    assert_eq!(VirtualDuration::from_secs(2).as_nanos(), 2_000_000_000);
}
