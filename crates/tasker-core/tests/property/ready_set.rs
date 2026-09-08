//! Property: the indexed heap always drains in the total order from Task 5.

use proptest::prelude::*;
use std::collections::HashMap;
use tasker_core::{JobId, OrderKey, PriorityHeap};

#[derive(Debug, Clone)]
enum Op {
    Push { index: u32, score: u64 },
    Update { index: u32, score: u64 },
    Remove { index: u32 },
    PopMax,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0_u32..64, 0_u64..1_000).prop_map(|(index, score)| Op::Push { index, score }),
        (0_u32..64, 0_u64..1_000).prop_map(|(index, score)| Op::Update { index, score }),
        (0_u32..64).prop_map(|index| Op::Remove { index }),
        Just(Op::PopMax),
    ]
}

proptest! {
    #[test]
    fn heap_matches_a_naive_model(ops in prop::collection::vec(op_strategy(), 0..400)) {
        let mut heap = PriorityHeap::with_slots(64);
        let mut model: HashMap<u64, u64> = HashMap::new();

        for op in ops {
            match op {
                Op::Push { index, score } => {
                    let id = JobId::from_bits(u64::from(index));
                    heap.push(id, score);
                    model.insert(id.to_bits(), score);
                }
                Op::Update { index, score } => {
                    let id = JobId::from_bits(u64::from(index));
                    let present = heap.update(id, score);
                    prop_assert_eq!(present, model.contains_key(&id.to_bits()));
                    if present {
                        model.insert(id.to_bits(), score);
                    }
                }
                Op::Remove { index } => {
                    let id = JobId::from_bits(u64::from(index));
                    let present = heap.remove(id);
                    prop_assert_eq!(present, model.remove(&id.to_bits()).is_some());
                }
                Op::PopMax => {
                    let got = heap.pop_max();
                    let want = model
                        .iter()
                        .map(|(bits, score)| OrderKey::new(*score, JobId::from_bits(*bits)))
                        .max();
                    if let Some(key) = want {
                        prop_assert_eq!(got, Some((key.job(), key.score())));
                        model.remove(&key.job().to_bits());
                    } else {
                        prop_assert_eq!(got, None);
                    }
                }
            }
            prop_assert_eq!(heap.len(), model.len());
        }

        // Draining must produce a non-increasing key sequence.
        let mut previous: Option<OrderKey> = None;
        while let Some((job, score)) = heap.pop_max() {
            let key = OrderKey::new(score, job);
            if let Some(previous) = previous {
                prop_assert!(previous >= key, "heap drained out of order");
            }
            previous = Some(key);
        }
    }
}
