//! Prometheus metrics: the handful that show whether scaling is doing its job.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Router, routing::get};
use prometheus::{
    Encoder, GaugeVec, Histogram, HistogramOpts, IntGauge, Opts, Registry, TextEncoder,
};
use tokio::net::TcpListener;

/// Registry plus typed handles. Updated by the scheduler thread; atomic ops only.
#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    pub ready: IntGauge,
    pub blocked: IntGauge,
    pub running: IntGauge,
    pub workers_attached: IntGauge,
    pub workers_desired: IntGauge,
    /// Decayed usage per account, in core-seconds.
    pub account_usage: GaugeVec,
    /// Fair-share factor per account, 0..=1.
    pub account_fairshare: GaugeVec,
    pub cycle_seconds: Histogram,
}

impl Metrics {
    /// Builds and registers every metric.
    ///
    /// # Panics
    /// Only if two metrics share a name, which is a programming error.
    #[must_use]
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();
        let gauge = |name: &str, help: &str| {
            let g = IntGauge::new(name, help).expect("valid metric name");
            registry
                .register(Box::new(g.clone()))
                .expect("unique metric name");
            g
        };
        let gauge_vec = |name: &str, help: &str| {
            let g = GaugeVec::new(Opts::new(name, help), &["account"]).expect("valid metric name");
            registry
                .register(Box::new(g.clone()))
                .expect("unique metric name");
            g
        };
        let cycle_seconds = Histogram::with_opts(
            HistogramOpts::new("tasker_cycle_seconds", "Scheduling cycle duration")
                // Buckets from 10 µs to ~40 ms: the range a cycle can plausibly take.
                .buckets(prometheus::exponential_buckets(1e-5, 2.0, 12).expect("valid buckets")),
        )
        .expect("valid histogram");
        registry
            .register(Box::new(cycle_seconds.clone()))
            .expect("unique metric name");
        Arc::new(Self {
            ready: gauge("tasker_jobs_ready", "Jobs in the ready set"),
            blocked: gauge("tasker_jobs_blocked", "Jobs blocked on dependencies"),
            running: gauge("tasker_jobs_running", "Jobs running on workers"),
            workers_attached: gauge("tasker_workers_attached", "Attached workers"),
            workers_desired: gauge("tasker_workers_desired", "Workers the scheduler wants"),
            account_usage: gauge_vec(
                "tasker_account_usage_core_seconds",
                "Decayed usage per account, in core-seconds",
            ),
            account_fairshare: gauge_vec(
                "tasker_account_fairshare",
                "Fair-share factor per account, 0..=1",
            ),
            cycle_seconds,
            registry,
        })
    }

    /// The exposition-format text.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .expect("encode to Vec");
        String::from_utf8(buf).expect("text format is UTF-8")
    }
}

/// Serves `GET /metrics` until the listener is dropped.
///
/// # Errors
/// I/O failure on the listener.
pub async fn serve(listener: TcpListener, metrics: Arc<Metrics>) -> std::io::Result<()> {
    let app = Router::new().route("/metrics", get(move || async move { metrics.encode() }));
    axum::serve(listener, app).await
}

/// Binds `addr` for `serve`.
///
/// # Errors
/// Bind failure.
pub async fn bind(addr: SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(addr).await
}
