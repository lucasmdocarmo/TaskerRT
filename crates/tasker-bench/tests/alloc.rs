//! Steady-state scheduling cycles must not allocate. A counting allocator
//! turns that design rule into a test.

// The one place `unsafe` is unavoidable: implementing `GlobalAlloc`.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use tasker_bench::{bench_config, synthetic};
use tasker_core::{DispatchDecision, Scheduler, VirtualTime};

struct Counting;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    // `const` init: no lazy allocation on first access, which matters inside an allocator.
    static TRACKING: Cell<bool> = const { Cell::new(false) };
}

// SAFETY: every call forwards to `System`, which upholds the `GlobalAlloc`
// contract. The counter is a side effect with no bearing on the returned
// pointer's validity, alignment, or provenance.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // `try_with` rather than `with`: during thread teardown the TLS slot
        // may already be destroyed, and an allocator must never panic.
        if TRACKING.try_with(Cell::get).unwrap_or(false) {
            // Relaxed: the count is read on the same thread after the fact; no ordering needed.
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: forwarding the caller's layout unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` was returned by `System.alloc` with this `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[test]
fn run_cycle_allocates_nothing_in_steady_state() {
    let config = bench_config();
    let mut workload = synthetic(2_000, 200, 64, 0xA110_C0DE);
    let mut scheduler = Scheduler::with_slots(workload.jobs.capacity_slots());
    for id in &workload.admitted {
        scheduler
            .submit(*id, &mut workload.jobs, VirtualTime::ZERO, &config)
            .expect("fresh Submitted job");
    }
    let mut decisions: Vec<DispatchDecision> = Vec::with_capacity(2_000);
    let now = VirtualTime::from_nanos(3_600_000_000_000);

    // Warm-up: scratch buffers grow to steady-state capacity on this cycle.
    scheduler.run_cycle(
        &mut workload.jobs,
        &mut workload.inventory,
        &workload.running,
        now,
        &config,
        &mut decisions,
    );

    TRACKING.with(|t| t.set(true));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    for _ in 0..10 {
        decisions.clear();
        scheduler.run_cycle(
            &mut workload.jobs,
            &mut workload.inventory,
            &workload.running,
            now,
            &config,
            &mut decisions,
        );
    }
    TRACKING.with(|t| t.set(false));

    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    assert_eq!(
        allocations, 0,
        "run_cycle allocated {allocations} times across 10 cycles"
    );
}
