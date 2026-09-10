//! Daemon configuration with production-shaped defaults.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tasker_core::{
    CycleConfig, FairShareConfig, PackBudget, PriorityConfig, PriorityWeights, ResourceRequest,
    VirtualDuration,
};
use tasker_wal::SyncPolicy;

/// Everything the daemon needs that is not runtime state.
#[derive(Clone, Debug)]
pub struct DaemonConfig {
    /// One address for both the control and worker services.
    pub listen: SocketAddr,
    /// Scheduler tick; the loop also wakes on demand.
    pub tick: Duration,
    pub inbox_capacity: usize,
    pub outbox_capacity: usize,
    /// Commands drained per cycle before scheduling runs.
    pub drain_budget: usize,
    /// A worker silent for this long is treated as gone.
    pub heartbeat_timeout: Duration,
    /// Prometheus `/metrics`. Port 0 by default so tests never collide.
    pub metrics_listen: SocketAddr,
    /// Capacity of one worker, for the demand calculation. Matches the pod spec.
    pub worker_cpu_millis: u32,
    pub min_workers: u32,
    pub max_workers: u32,
    /// Fair-share weights per account id; unlisted accounts weigh 1.
    pub shares: Vec<(u32, u32)>,
    /// Durability root. `None` runs in memory only, as before M6.
    pub data_dir: Option<PathBuf>,
    pub wal_sync: SyncPolicy,
    /// Snapshot and start a fresh log once the log passes this many bytes.
    pub wal_rotate_bytes: u64,
    /// Terminal jobs are forgotten this long after they finish.
    pub retain: Duration,
    pub cycle: CycleConfig,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:7070".parse().expect("literal address"),
            tick: Duration::from_millis(5),
            inbox_capacity: 65_536,
            outbox_capacity: 65_536,
            drain_budget: 4_096,
            heartbeat_timeout: Duration::from_secs(10),
            metrics_listen: "127.0.0.1:0".parse().expect("literal address"),
            worker_cpu_millis: 4_000,
            min_workers: 1,
            max_workers: 8,
            shares: Vec::new(),
            data_dir: None,
            wal_sync: SyncPolicy::Data,
            wal_rotate_bytes: 64 << 20,
            retain: Duration::from_mins(5),
            cycle: CycleConfig {
                priority: PriorityConfig::new(
                    PriorityWeights::default(),
                    VirtualDuration::from_secs(3_600),
                    // The size factor normalizes against this; a large reference keeps it small.
                    ResourceRequest::new(u32::MAX, u64::MAX, u8::MAX),
                ),
                fairshare: FairShareConfig::new(VirtualDuration::from_secs(3_600)),
                budget: PackBudget::default(),
                max_candidates: 10_000,
            },
        }
    }
}
