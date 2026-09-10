//! `taskerd` — the control plane binary. A few lines of wiring; the logic is in the library.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use tasker_core::{CycleConfig, FairShareConfig, PreemptConfig, PriorityClass, VirtualDuration};
use tasker_wal::SyncPolicy;
use taskerd::{Daemon, DaemonConfig};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "taskerd", about = "TaskerRT control plane")]
struct Args {
    /// Address for the control, worker, and KEDA scaler gRPC services.
    #[arg(long, default_value = "127.0.0.1:7070")]
    listen: SocketAddr,
    /// Prometheus `/metrics`.
    #[arg(long, default_value = "127.0.0.1:9090")]
    metrics_listen: SocketAddr,
    /// Millicores per worker, for the KEDA demand figure.
    #[arg(long, default_value_t = 4_000)]
    worker_cpu_millis: u32,
    #[arg(long, default_value_t = 1)]
    min_workers: u32,
    #[arg(long, default_value_t = 8)]
    max_workers: u32,
    /// Fair-share usage half-life, in seconds. 0 remembers only running work.
    #[arg(long, default_value_t = 3_600)]
    half_life_secs: u64,
    /// Fair-share weights as `account=shares`, comma separated, e.g. `0=3,1=1`.
    #[arg(long)]
    shares: Option<String>,
    /// Durability root. Omit to run in memory only.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// How each WAL batch is synced: full, data, or none.
    #[arg(long, default_value = "data", value_parser = parse_sync)]
    wal_sync: SyncPolicy,
    /// Snapshot and rotate once the log passes this many bytes.
    #[arg(long, default_value_t = 64 << 20)]
    wal_rotate_bytes: u64,
    /// Forget terminal jobs this many seconds after they finish.
    #[arg(long, default_value_t = 300)]
    retain_secs: u64,
    /// Lowest class that may evict lower-class running jobs: off, low, normal, high, urgent.
    #[arg(long, default_value = "urgent", value_parser = parse_class)]
    preempt_min_class: Option<PriorityClass>,
    /// Evictions after which a job becomes immune.
    #[arg(long, default_value_t = 3)]
    preempt_max: u8,
    /// Seconds an evicted task gets to stop before it is killed.
    #[arg(long, default_value_t = 5)]
    preempt_grace_secs: u64,
}

fn parse_class(s: &str) -> Result<Option<PriorityClass>, String> {
    Ok(match s {
        "off" => None,
        "low" => Some(PriorityClass::Low),
        "normal" => Some(PriorityClass::Normal),
        "high" => Some(PriorityClass::High),
        "urgent" => Some(PriorityClass::Urgent),
        other => Err(format!(
            "unknown class {other:?}: off|low|normal|high|urgent"
        ))?,
    })
}

fn parse_sync(s: &str) -> Result<SyncPolicy, String> {
    match s {
        "full" => Ok(SyncPolicy::Full),
        "data" => Ok(SyncPolicy::Data),
        "none" => Ok(SyncPolicy::None),
        other => Err(format!("unknown sync policy {other:?}: full|data|none")),
    }
}

/// Parses `0=3,1=1`. Empty input is no overrides.
fn parse_shares(s: &str) -> anyhow::Result<Vec<(u32, u32)>> {
    s.split(',')
        .map(str::trim)
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (account, shares) = pair
                .split_once('=')
                .with_context(|| format!("expected account=shares, got {pair:?}"))?;
            Ok((account.trim().parse()?, shares.trim().parse()?))
        })
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // RUST_LOG overrides; otherwise info.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    let defaults = DaemonConfig::default();
    let config = DaemonConfig {
        listen: args.listen,
        metrics_listen: args.metrics_listen,
        worker_cpu_millis: args.worker_cpu_millis,
        min_workers: args.min_workers,
        max_workers: args.max_workers,
        shares: args
            .shares
            .as_deref()
            .map(parse_shares)
            .transpose()?
            .unwrap_or_default(),
        data_dir: args.data_dir,
        wal_sync: args.wal_sync,
        wal_rotate_bytes: args.wal_rotate_bytes,
        retain: Duration::from_secs(args.retain_secs),
        cycle: CycleConfig {
            fairshare: FairShareConfig::new(VirtualDuration::from_secs(args.half_life_secs)),
            preempt: PreemptConfig {
                min_class: args.preempt_min_class,
                max_preemptions: args.preempt_max,
                grace: VirtualDuration::from_secs(args.preempt_grace_secs),
            },
            ..defaults.cycle
        },
        ..defaults
    };
    let listener = TcpListener::bind(config.listen).await?;
    Daemon::new(config)
        .serve(listener, async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
