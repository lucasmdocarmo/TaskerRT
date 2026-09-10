//! Layer 3 — the scheduling algorithms. Depends on `domain` and `cluster`.
//! Every function here is pure: it reads and mutates what it is handed.

pub mod backfill;
pub mod eligibility;
pub mod fairshare;
pub mod pack;
pub mod preempt;
pub mod priority;
pub mod ready_set;

pub use backfill::{BackfillOutcome, BackfillScratch, RunningJob, easy_backfill, reservation_time};
pub use eligibility::{DependencyError, DependencyTracker, Readiness};
pub use fairshare::{
    AccountError, DECAY_ONE, DECAY_STEPS, FairShare, FairShareConfig, Ledger, MAX_ACCOUNTS,
    USAGE_PER_CORE_SECOND, Usage, decay_factor,
};
pub use pack::{DispatchDecision, Disposition, PackBudget, PackOutcome, pack};
pub use preempt::{Eviction, PreemptConfig, plan_preemption};
pub use priority::{FACTOR_SCALE, OrderKey, PriorityConfig, PriorityWeights, Score, score};
pub use ready_set::{PriorityHeap, ReadyEntry, ReadySet};
