//! `tasker-worker` — attaches to a daemon and runs tasks with the chosen executor.

use std::time::Duration;

use clap::Parser;
use tasker_core::Resources;
use tasker_worker::{CommandExecutor, SleepExecutor, Worker, WorkerConfig};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "tasker-worker", about = "TaskerRT worker")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:7070")]
    daemon: String,
    #[arg(long, default_value = "worker")]
    name: String,
    #[arg(long, default_value_t = 4_000)]
    cpu_millis: u32,
    #[arg(long, default_value_t = 8 << 30)]
    mem_bytes: u64,
    #[arg(long, default_value_t = 0)]
    gpus: u8,
    /// Which executor runs tasks: `sleep` (payload = LE u64 nanos) or `command` (NUL-separated argv).
    #[arg(long, default_value = "sleep")]
    executor: String,
    /// Seconds to wait for in-flight tasks after SIGTERM.
    #[arg(long, default_value_t = 50)]
    drain_timeout: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    let config = WorkerConfig {
        daemon: args.daemon,
        name: args.name,
        capacity: Resources::new(args.cpu_millis, args.mem_bytes, args.gpus),
        heartbeat: Duration::from_secs(2),
        drain_timeout: Duration::from_secs(args.drain_timeout),
    };
    // Static dispatch: two monomorphized workers, one chosen at startup.
    match args.executor.as_str() {
        "sleep" => Worker::new(config, SleepExecutor).run().await?,
        "command" => Worker::new(config, CommandExecutor).run().await?,
        other => anyhow::bail!("unknown executor {other:?}; expected sleep or command"),
    }
    Ok(())
}
