//! Layer 4 — orchestration. The only layer that knows the order of the cycle.

pub mod cycle;

pub use cycle::{CycleConfig, CycleOutcome, LifecycleError, Scheduler};
