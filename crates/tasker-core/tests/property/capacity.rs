//! Property: allocated capacity never exceeds slot capacity (spec §9.1).

use proptest::prelude::*;
use tasker_core::{Resources, SlotInventory};

#[derive(Debug, Clone)]
enum Op {
    Allocate {
        slot: u32,
        cpu: u32,
        mem: u64,
        gpus: u8,
    },
    Release {
        slot: u32,
        cpu: u32,
        mem: u64,
        gpus: u8,
    },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0_u32..6, 0_u32..6_000, 0_u64..(12 << 30), 0_u8..4).prop_map(|(slot, cpu, mem, gpus)| {
            Op::Allocate {
                slot,
                cpu,
                mem,
                gpus,
            }
        }),
        (0_u32..6, 0_u32..6_000, 0_u64..(12 << 30), 0_u8..4).prop_map(|(slot, cpu, mem, gpus)| {
            Op::Release {
                slot,
                cpu,
                mem,
                gpus,
            }
        }),
    ]
}

proptest! {
    #[test]
    fn allocation_never_exceeds_capacity(ops in prop::collection::vec(op_strategy(), 0..300)) {
        let capacity = Resources::new(4_000, 8 << 30, 2);
        let mut inv = SlotInventory::from_uniform(4, capacity);

        for op in ops {
            match op {
                Op::Allocate { slot, cpu, mem, gpus } => {
                    let _ = inv.try_allocate(slot, &Resources::new(cpu, mem, gpus));
                }
                Op::Release { slot, cpu, mem, gpus } => {
                    let _ = inv.release(slot, &Resources::new(cpu, mem, gpus));
                }
            }

            for index in 0..inv.len() {
                let s = inv.slot(index).expect("index below len");
                prop_assert!(
                    s.allocated.fits_within(&s.capacity),
                    "slot {index} over capacity: {:?} > {:?}",
                    s.allocated,
                    s.capacity
                );
            }
        }
    }
}
