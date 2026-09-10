//! Unit tests, one module per source file. They use only the public API.
//! Run with `cargo test -p tasker-core --test unit`.

mod arena;
mod backfill;
mod cycle;
mod eligibility;
mod fairshare;
mod job;
mod pack;
mod priority;
mod ready_set;
mod resource;
mod slot;
mod time;
