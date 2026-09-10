//! Size guards for hot-path types. Rust reorders fields by alignment, so the
//! expected sizes below are (sum of fields) rounded up to the largest alignment.

use tasker_core::{DispatchDecision, JobId, Ledger, OrderKey, ReadyEntry, RunningJob};

#[test]
fn job_id_is_one_machine_word() {
    assert_eq!(std::mem::size_of::<JobId>(), 8);
}

#[test]
fn ready_entry_is_24_bytes() {
    // JobId 8 + Score 8 + PriorityClass 1 = 17, padded to the 8-byte alignment.
    assert_eq!(std::mem::size_of::<ReadyEntry>(), 24);
}

#[test]
fn running_job_is_40_bytes() {
    // JobId 8 + Resources 16 + VirtualTime 8 + SlotIndex 4 = 36, padded to 40.
    assert_eq!(std::mem::size_of::<RunningJob>(), 40);
}

#[test]
fn dispatch_decision_is_16_bytes() {
    // JobId 8 + SlotIndex 4 = 12, padded to 16.
    assert_eq!(std::mem::size_of::<DispatchDecision>(), 16);
}

#[test]
fn order_key_is_16_bytes() {
    // Score 8 + Reverse<u64> 8. `Reverse` is a transparent wrapper.
    assert_eq!(std::mem::size_of::<OrderKey>(), 16);
}

#[test]
fn ledger_is_40_bytes() {
    // usage 8 + running_cpu 8 + two VirtualTime 16 + shares 4 = 36, padded to 40.
    assert_eq!(std::mem::size_of::<Ledger>(), 40);
}
