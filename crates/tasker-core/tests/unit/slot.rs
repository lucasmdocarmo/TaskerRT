use tasker_core::{CapacityError, Resources, SlotInventory};

fn inventory() -> SlotInventory {
    SlotInventory::from_uniform(2, Resources::new(4_000, 8 << 30, 1))
}

#[test]
fn a_fresh_slot_is_entirely_free() {
    let inv = inventory();
    assert_eq!(inv.len(), 2);
    assert_eq!(inv.free(0), Some(Resources::new(4_000, 8 << 30, 1)));
}

#[test]
fn allocation_reduces_free_capacity() {
    let mut inv = inventory();
    inv.try_allocate(0, &Resources::new(1_000, 1 << 30, 0))
        .unwrap();
    assert_eq!(inv.free(0), Some(Resources::new(3_000, 7 << 30, 1)));
    assert_eq!(
        inv.free(1),
        Some(Resources::new(4_000, 8 << 30, 1)),
        "other slot untouched"
    );
}

#[test]
fn allocation_beyond_capacity_is_rejected_and_leaves_state_unchanged() {
    let mut inv = inventory();
    let before = inv.free(0);
    let err = inv
        .try_allocate(0, &Resources::new(9_000, 0, 0))
        .unwrap_err();
    assert_eq!(err, CapacityError::WouldExceed { slot: 0 });
    assert_eq!(inv.free(0), before, "a rejected allocation must not mutate");
}

#[test]
fn unknown_slot_is_an_error() {
    let mut inv = inventory();
    assert_eq!(
        inv.try_allocate(9, &Resources::ZERO).unwrap_err(),
        CapacityError::NoSuchSlot { slot: 9 }
    );
}

#[test]
fn release_returns_capacity() {
    let mut inv = inventory();
    let req = Resources::new(1_000, 1 << 30, 1);
    inv.try_allocate(0, &req).unwrap();
    inv.release(0, &req).unwrap();
    assert_eq!(inv.free(0), Some(Resources::new(4_000, 8 << 30, 1)));
}

#[test]
fn first_fit_skips_full_slots() {
    let mut inv = inventory();
    inv.try_allocate(0, &Resources::new(4_000, 0, 0)).unwrap();
    assert_eq!(inv.first_fit(&Resources::new(2_000, 0, 0)), Some(1));
}

#[test]
fn can_ever_fit_uses_declared_capacity_not_free() {
    let mut inv = inventory();
    inv.try_allocate(0, &Resources::new(4_000, 0, 0)).unwrap();
    inv.try_allocate(1, &Resources::new(4_000, 0, 0)).unwrap();
    assert!(
        inv.can_ever_fit(&Resources::new(4_000, 0, 0)),
        "fits an empty slot"
    );
    assert!(
        !inv.can_ever_fit(&Resources::new(4_001, 0, 0)),
        "fits no slot ever"
    );
    assert_eq!(
        inv.first_fit(&Resources::new(4_000, 0, 0)),
        None,
        "but not right now"
    );
}

#[test]
fn heterogeneous_inventory_reports_per_slot_capacity() {
    let inv =
        SlotInventory::from_capacities(&[Resources::new(1_000, 0, 0), Resources::new(8_000, 0, 2)]);
    assert!(inv.can_ever_fit(&Resources::new(8_000, 0, 2)));
    assert!(!inv.can_ever_fit(&Resources::new(9_000, 0, 0)));
}

#[test]
fn total_free_sums_every_slot() {
    let inv = inventory();
    assert_eq!(inv.total_free(), Resources::new(8_000, 16 << 30, 2));
}

#[test]
fn add_slot_appends_then_reuses_a_zeroed_slot() {
    let mut inv = SlotInventory::from_uniform(1, Resources::new(1_000, 0, 0));
    let second = inv.add_slot(Resources::new(2_000, 0, 0));
    assert_eq!(second, 1);
    assert_eq!(inv.len(), 2);

    inv.set_capacity(0, Resources::ZERO).unwrap();
    assert_eq!(
        inv.first_fit(&Resources::new(1, 0, 0)),
        Some(1),
        "slot 0 is drained"
    );

    let reused = inv.add_slot(Resources::new(3_000, 0, 0));
    assert_eq!(reused, 0, "lowest zero-capacity slot is reused");
    assert_eq!(inv.len(), 2, "no growth");
    assert!(inv.can_ever_fit(&Resources::new(3_000, 0, 0)));
}

#[test]
fn clear_allocation_frees_a_slot_without_touching_its_capacity() {
    let mut inv = SlotInventory::from_uniform(1, Resources::new(4_000, 0, 0));
    inv.try_allocate(0, &Resources::new(3_000, 0, 0)).unwrap();
    inv.clear_allocation(0).unwrap();
    assert_eq!(inv.free(0), Some(Resources::new(4_000, 0, 0)));
    assert_eq!(
        inv.clear_allocation(9).unwrap_err(),
        CapacityError::NoSuchSlot { slot: 9 }
    );
}
