use tasker_core::{Arena, ArenaRestoreError, JobId, SlotState};

#[test]
fn insert_then_get_returns_value() {
    let mut arena = Arena::new();
    let id = arena.insert(42_u32);
    assert_eq!(arena.get(id), Some(&42));
    assert_eq!(arena.len(), 1);
}

#[test]
fn remove_invalidates_the_handle() {
    let mut arena = Arena::new();
    let id = arena.insert(42_u32);
    assert_eq!(arena.remove(id), Some(42));
    assert_eq!(arena.get(id), None);
    assert!(!arena.contains(id));
    assert_eq!(arena.len(), 0);
}

#[test]
fn reused_slot_yields_a_different_id() {
    let mut arena = Arena::new();
    let first = arena.insert(1_u32);
    arena.remove(first);
    let second = arena.insert(2_u32);

    assert_eq!(first.index(), second.index(), "slot should be reused");
    assert_ne!(first, second, "generation must differ");
    assert_eq!(first.generation() + 1, second.generation());
    assert_eq!(arena.get(first), None, "stale handle must not resolve");
    assert_eq!(arena.get(second), Some(&2));
}

#[test]
fn double_remove_is_none() {
    let mut arena = Arena::new();
    let id = arena.insert(7_u32);
    assert_eq!(arena.remove(id), Some(7));
    assert_eq!(arena.remove(id), None);
}

#[test]
fn id_bit_packing_roundtrips() {
    // Layout contract: index in the low 32 bits, generation in the high 32.
    // `+` rather than `|`: identical here (the shifted word has zero low bits)
    // and keeps the two numbers readable as decimals.
    let id = JobId::from_bits((678_u64 << 32) + 12_345);
    assert_eq!(id.index(), 12_345);
    assert_eq!(id.generation(), 678);
    assert_eq!(JobId::from_bits(id.to_bits()), id);
}

#[test]
fn iter_yields_only_live_entries() {
    let mut arena = Arena::new();
    let a = arena.insert(1_u32);
    let b = arena.insert(2_u32);
    arena.remove(a);
    let live: Vec<_> = arena.iter().map(|(id, v)| (id, *v)).collect();
    assert_eq!(live, vec![(b, 2)]);
}

#[test]
fn next_id_predicts_insert_through_reuse() {
    let mut arena = Arena::new();
    assert_eq!(arena.next_id(), arena.insert("a"));
    let b = arena.insert("b");
    assert_eq!(arena.next_id(), arena.insert("c"));
    arena.remove(b);
    // The freed slot is reused at the bumped generation.
    let predicted = arena.next_id();
    assert_eq!(predicted.index(), b.index());
    assert_eq!(predicted.generation(), b.generation() + 1);
    assert_eq!(predicted, arena.insert("d"));
}

#[test]
fn from_parts_reproduces_the_free_list_exactly() {
    let mut arena = Arena::new();
    let ids: Vec<_> = (0..5).map(|i| arena.insert(i)).collect();
    arena.remove(ids[1]);
    arena.remove(ids[3]);
    let slots: Vec<SlotState<i32>> = arena
        .slots()
        .map(|s| match s {
            SlotState::Occupied { generation, value } => SlotState::Occupied {
                generation,
                value: *value,
            },
            SlotState::Vacant {
                generation,
                next_free,
            } => SlotState::Vacant {
                generation,
                next_free,
            },
        })
        .collect();
    let mut restored = Arena::from_parts(slots, arena.free_head()).unwrap();
    assert_eq!(restored.len(), arena.len());
    // Both arenas hand out the same ids in the same order from here on.
    for _ in 0..3 {
        assert_eq!(restored.insert(9), arena.insert(9));
    }
}

#[test]
fn from_parts_rejects_a_broken_free_list() {
    let slots = vec![SlotState::<u8>::Vacant {
        generation: 0,
        next_free: Some(7),
    }];
    assert!(matches!(
        Arena::from_parts(slots, Some(0)),
        Err(ArenaRestoreError::BadFreeLink(7))
    ));
    let slots = vec![SlotState::<u8>::Vacant {
        generation: 0,
        next_free: None,
    }];
    assert!(matches!(
        Arena::from_parts(slots, None),
        Err(ArenaRestoreError::FreeListIncomplete { .. })
    ));
}
