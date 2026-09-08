//! `taskerd` — the control plane binary. A few lines of wiring; the logic is in the library.

use std::net::SocketAddr;

use clap::Parser;
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
    let config = DaemonConfig {
        listen: args.listen,
        metrics_listen: args.metrics_listen,
        worker_cpu_millis: args.worker_cpu_millis,
        min_workers: args.min_workers,
        max_workers: args.max_workers,
        ..DaemonConfig::default()
    };
    let listener = TcpListener::bind(config.listen).await?;
    Daemon::new(config)
        .serve(listener, async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
