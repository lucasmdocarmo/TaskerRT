//! Property tests: randomized inputs checked against invariants.
//! Run with `cargo test -p tasker-core --test property`, or
//! `PROPTEST_CASES=20000 cargo test ...` for a deeper search.

mod arena;
mod backfill;
mod capacity;
mod eligibility;
mod priority;
mod ready_set;
