//! A TaskerRT worker: one bidirectional stream to the daemon, one task per
//! `Assign`, each under a walltime timeout with panic capture.

pub mod executor;
pub mod worker;

pub use executor::{
    CommandExecutor, SleepExecutor, TaskError, TaskExecutor, command_payload,
    parse_command_payload, sleep_payload,
};
pub use worker::{Worker, WorkerConfig, WorkerError};
