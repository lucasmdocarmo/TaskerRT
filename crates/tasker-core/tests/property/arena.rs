//! Property: a handle never resolves to a value it did not name.

use proptest::prelude::*;
use tasker_core::{Arena, JobId};

#[derive(Debug, Clone)]
enum Op {
    Insert(u32),
    RemoveNth(usize),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        any::<u32>().prop_map(Op::Insert),
        any::<usize>().prop_map(Op::RemoveNth),
    ]
}

proptest! {
    #[test]
    fn stale_handles_never_resolve(ops in prop::collection::vec(op_strategy(), 0..200)) {
        let mut arena: Arena<u32> = Arena::new();
        let mut live: Vec<(JobId, u32)> = Vec::new();
        let mut dead: Vec<JobId> = Vec::new();

        for op in ops {
            match op {
                Op::Insert(value) => {
                    let id = arena.insert(value);
                    live.push((id, value));
                }
                Op::RemoveNth(n) => {
                    if !live.is_empty() {
                        let (id, value) = live.remove(n % live.len());
                        prop_assert_eq!(arena.remove(id), Some(value));
                        dead.push(id);
                    }
                }
            }
        }

        for id in &dead {
            prop_assert!(arena.get(*id).is_none(), "a removed handle resolved");
        }
        for (id, value) in &live {
            prop_assert_eq!(arena.get(*id), Some(value), "a live handle went missing");
        }
        prop_assert_eq!(arena.len(), live.len());
    }
}
