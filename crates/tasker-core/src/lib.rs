//! TaskerRT scheduling core: pure, synchronous scheduling policy.
//!
//! This crate contains no async runtime, performs no I/O, and never reads the
//! wall clock. Time enters through [`VirtualTime`] parameters.

pub mod arena;
pub mod backfill;
pub mod cycle;
pub mod job;
pub mod pack;
pub mod priority;
pub mod ready_set;
pub mod resource;
pub mod slot;
pub mod time;

pub use arena::{Arena, JobId};
pub use backfill::{BackfillOutcome, BackfillScratch, RunningJob, easy_backfill, reservation_time};
pub use cycle::{CycleConfig, CycleOutcome, Scheduler};
pub use job::{AccountId, Job, JobState, PriorityClass, TransitionError};
pub use pack::{DispatchDecision, Disposition, PackBudget, PackOutcome, pack};
pub use priority::{FACTOR_SCALE, OrderKey, PriorityConfig, PriorityWeights, Score, score};
pub use ready_set::{PriorityHeap, ReadyEntry, ReadySet};
pub use resource::{Capacity, ResourceRequest, Resources};
pub use slot::{CapacityError, SlotIndex, SlotInventory, WorkerSlot};
pub use time::{VirtualDuration, VirtualTime};
