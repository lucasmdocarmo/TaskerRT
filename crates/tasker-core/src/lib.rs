//! TaskerRT scheduling core: pure, synchronous scheduling policy.
//!
//! This crate contains no async runtime, performs no I/O, and never reads the
//! wall clock. Time enters through [`VirtualTime`] parameters.

pub mod arena;

pub use arena::{Arena, JobId};
