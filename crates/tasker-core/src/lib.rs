//! TaskerRT scheduling core: pure, synchronous, no I/O, no wall clock.
//!
//! Layers, innermost first. Each may depend only on the ones before it:
//! `domain` → `cluster` → `policy` → `engine`.

pub mod cluster;
pub mod domain;
pub mod engine;
pub mod policy;

// Re-exports keep the public paths flat: `tasker_core::Job`, not
// `tasker_core::domain::job::Job`.
pub use cluster::{CapacityError, SlotIndex, SlotInventory, WorkerSlot};
pub use domain::{
    AccountId, Arena, ArenaRestoreError, Capacity, Deps, Job, JobId, JobState, PriorityClass,
    ResourceRequest, Resources, SlotState, TransitionError, VirtualDuration, VirtualTime,
};
pub use engine::{CycleConfig, CycleOutcome, LifecycleError, Scheduler};
pub use policy::{
    AccountError, BackfillOutcome, BackfillScratch, DECAY_ONE, DECAY_STEPS, DependencyError,
    DependencyTracker, DispatchDecision, Disposition, Eviction, FACTOR_SCALE, FairShare,
    FairShareConfig, Ledger, MAX_ACCOUNTS, OrderKey, PackBudget, PackOutcome, PreemptConfig,
    PriorityConfig, PriorityHeap, PriorityWeights, Readiness, ReadyEntry, ReadySet, RunningJob,
    Score, USAGE_PER_CORE_SECOND, Usage, decay_factor, easy_backfill, pack, plan_preemption,
    reservation_time, score,
};
