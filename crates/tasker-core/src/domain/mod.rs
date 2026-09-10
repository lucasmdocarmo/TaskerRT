//! Layer 1 — entities and value objects. Depends on nothing else in the crate.

pub mod arena;
pub mod job;
pub mod resource;
pub mod time;

pub use arena::{Arena, ArenaRestoreError, JobId, SlotState};
pub use job::{AccountId, Deps, Job, JobState, PriorityClass, TransitionError};
pub use resource::{Capacity, ResourceRequest, Resources};
pub use time::{VirtualDuration, VirtualTime};
