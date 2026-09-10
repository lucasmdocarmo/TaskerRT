//! The TaskerRT control plane.
//!
//! One bounded lock-free inbox feeds a scheduler running on its own OS thread.
//! Dispatch decisions leave through an outbox to a tokio task that pushes them
//! onto attached workers' gRPC streams. Three tonic services share one port;
//! Prometheus metrics are served on another.

pub mod clock;
pub mod command;
pub mod config;
pub mod control;
pub mod daemon;
pub mod dispatcher;
pub mod engine;
pub mod inbox;
pub mod journal;
pub mod metrics;
pub mod outbox;
pub mod registry;
pub mod scaler;
pub mod worker_api;

pub use command::{
    AccountSummary, Command, Demand, Dispatch, NodeSummary, Query, QueueSummary, Reply,
};
pub use config::DaemonConfig;
pub use daemon::{Daemon, DaemonError};
pub use engine::{Engine, RecoverError};
pub use inbox::Inbox;
pub use journal::{Ack, COMMIT_QUEUE_DEPTH, Commit, Journal};
pub use metrics::Metrics;
pub use outbox::Outbox;
pub use registry::Registry;
