//! `tasker` — submit, status, cancel, queue, nodes.

use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use tasker_proto::v1;
use tasker_proto::v1::control_api_client::ControlApiClient;
use tasker_worker::{command_payload, sleep_payload};

#[derive(Parser, Debug)]
#[command(name = "tasker", about = "TaskerRT client")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:7070", global = true)]
    daemon: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Submit a job. Exactly one of --sleep or --cmd.
    Submit {
        #[arg(long, default_value_t = 1_000)]
        cpu_millis: u32,
        #[arg(long, default_value_t = 0)]
        mem_bytes: u64,
        #[arg(long, default_value_t = 0)]
        gpus: u32,
        /// Walltime limit in seconds.
        #[arg(long, default_value_t = 60)]
        walltime: u64,
        #[arg(long, default_value = "normal")]
        class: String,
        #[arg(long, default_value_t = 0)]
        account: u32,
        /// A job id this one depends on; repeatable.
        #[arg(long = "dep")]
        deps: Vec<u64>,
        /// Sleep payload, in milliseconds.
        #[arg(long, conflicts_with = "cmd")]
        sleep: Option<u64>,
        /// Command payload: program and arguments.
        #[arg(long, num_args = 1.., conflicts_with = "sleep")]
        cmd: Option<Vec<String>>,
    },
    /// Show a job's state.
    Status { job_id: u64 },
    /// Cancel a job and everything that depends on it.
    Cancel { job_id: u64 },
    /// Queue counts.
    Queue,
    /// Attached workers.
    Nodes,
}

fn class(s: &str) -> anyhow::Result<v1::PriorityClass> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "low" => v1::PriorityClass::Low,
        "normal" => v1::PriorityClass::Normal,
        "high" => v1::PriorityClass::High,
        "urgent" => v1::PriorityClass::Urgent,
        other => anyhow::bail!("unknown class {other:?}: low|normal|high|urgent"),
    })
}

fn state_name(n: i32) -> String {
    v1::JobState::try_from(n).map_or_else(|_| format!("unknown({n})"), |s| format!("{s:?}"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut client = ControlApiClient::connect(args.daemon.clone())
        .await
        .with_context(|| format!("connecting to {}", args.daemon))?;

    match args.cmd {
        Cmd::Submit {
            cpu_millis,
            mem_bytes,
            gpus,
            walltime,
            class: class_name,
            account,
            deps,
            sleep,
            cmd,
        } => {
            let payload = match (sleep, cmd) {
                (Some(ms), None) => sleep_payload(Duration::from_millis(ms)),
                (None, Some(command_args)) => command_payload(&command_args),
                _ => anyhow::bail!("exactly one of --sleep or --cmd is required"),
            };
            let resp = client
                .submit(v1::SubmitRequest {
                    account,
                    priority_class: class(&class_name)? as i32,
                    request: Some(v1::Resources {
                        cpu_millis,
                        mem_bytes,
                        gpus,
                    }),
                    walltime_nanos: walltime.saturating_mul(1_000_000_000),
                    deps,
                    payload,
                })
                .await?
                .into_inner();
            println!("{} {}", resp.job_id, state_name(resp.state));
        }
        Cmd::Status { job_id } => {
            let resp = client
                .status(v1::StatusRequest { job_id })
                .await?
                .into_inner();
            println!("{} {}", resp.job_id, state_name(resp.state));
        }
        Cmd::Cancel { job_id } => {
            let resp = client
                .cancel(v1::CancelRequest { job_id })
                .await?
                .into_inner();
            println!("cancelled {job_id}; cascaded {}", resp.cascaded.len());
            for id in resp.cascaded {
                println!("  {id}");
            }
        }
        Cmd::Queue => {
            let q = client.queue(v1::QueueRequest {}).await?.into_inner();
            println!(
                "ready {}  blocked {}  running {}  cycles {}",
                q.ready, q.blocked, q.running, q.cycles
            );
        }
        Cmd::Nodes => {
            let nodes = client.nodes(v1::NodesRequest {}).await?.into_inner().nodes;
            println!(
                "{:<5} {:<12} {:<9} {:>10} {:>10}",
                "slot", "name", "attached", "cpu_free", "cpu_cap"
            );
            for n in nodes {
                let cap = n.capacity.unwrap_or_default();
                let used = n.allocated.unwrap_or_default();
                println!(
                    "{:<5} {:<12} {:<9} {:>10} {:>10}",
                    n.slot,
                    if n.name.is_empty() { "-" } else { &n.name },
                    n.attached,
                    cap.cpu_millis.saturating_sub(used.cpu_millis),
                    cap.cpu_millis
                );
            }
        }
    }
    Ok(())
}
