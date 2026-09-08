//! Messages into and out of the scheduler thread.

use std::fmt;

use bytes::Bytes;
use tasker_core::{
    Capacity, Job, JobId, JobState, LifecycleError, Resources, SlotIndex, VirtualDuration,
};
use tokio::sync::oneshot;

/// A one-shot reply channel. `send` is synchronous and never blocks, which is
/// what lets the scheduler thread answer without touching the runtime.
pub type Reply<T> = oneshot::Sender<T>;

/// Something for the scheduler thread to do.
pub enum Command {
    Submit {
        job: Job,
        reply: Reply<Result<(JobId, JobState), LifecycleError>>,
    },
    Cancel {
        id: JobId,
        reply: Reply<Result<Vec<JobId>, LifecycleError>>,
    },
    Completed {
        id: JobId,
    },
    Failed {
        id: JobId,
    },
    WorkerJoined {
        name: String,
        capacity: Capacity,
        reply: Reply<SlotIndex>,
    },
    WorkerLeft {
        slot: SlotIndex,
    },
    /// The worker got SIGTERM: stop dispatching to it, keep what it is running.
    WorkerDraining {
        slot: SlotIndex,
    },
    Query(Query),
}

/// A read-only question, answered from the scheduler thread's own state.
pub enum Query {
    Status {
        id: JobId,
        reply: Reply<Option<JobState>>,
    },
    Queue {
        reply: Reply<QueueSummary>,
    },
    Nodes {
        reply: Reply<Vec<NodeSummary>>,
    },
    Demand {
        reply: Reply<Demand>,
    },
}

/// Counts for the `Queue` RPC.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct QueueSummary {
    pub ready: usize,
    pub blocked: usize,
    pub running: usize,
    pub cycles: u64,
}

/// One slot's view for the `Nodes` RPC.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NodeSummary {
    pub slot: SlotIndex,
    pub name: String,
    pub capacity: Capacity,
    pub allocated: Resources,
    pub attached: bool,
}

/// What the scheduler thinks the worker pool should be.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Demand {
    /// Workers needed: `ceil((cpu_ready + cpu_running) / worker_cpu)`, clamped.
    pub desired: u32,
    pub attached: u32,
    pub cpu_ready: u64,
    pub cpu_running: u64,
}

/// Something for the dispatcher to send to a worker.
#[derive(Clone, Debug)]
pub enum Dispatch {
    Assign {
        slot: SlotIndex,
        id: JobId,
        payload: Bytes,
        walltime: VirtualDuration,
    },
    Kill {
        slot: SlotIndex,
        id: JobId,
    },
}

// Reply channels are not `Debug`; name the variant only.
impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Submit { .. } => "Submit",
            Self::Cancel { .. } => "Cancel",
            Self::Completed { .. } => "Completed",
            Self::Failed { .. } => "Failed",
            Self::WorkerJoined { .. } => "WorkerJoined",
            Self::WorkerLeft { .. } => "WorkerLeft",
            Self::WorkerDraining { .. } => "WorkerDraining",
            Self::Query(_) => "Query",
        };
        f.write_str(name)
    }
}

impl fmt::Debug for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Status { .. } => "Status",
            Self::Queue { .. } => "Queue",
            Self::Nodes { .. } => "Nodes",
            Self::Demand { .. } => "Demand",
        };
        f.write_str(name)
    }
}
