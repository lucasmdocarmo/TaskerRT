//! Open-loop load: submit at a fixed rate regardless of how the system keeps
//! up, and measure intended-send → task-start. Missing the intended send time
//! is counted as latency, not skipped — that is the coordinated-omission fix.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use hdrhistogram::Histogram;
use tasker_core::Resources;
use tasker_proto::v1;
use tasker_proto::v1::control_api_client::ControlApiClient;
use tasker_wal::SyncPolicy;
use tasker_worker::{SleepExecutor, TaskError, TaskExecutor, Worker, WorkerConfig, sleep_payload};
use taskerd::{Daemon, DaemonConfig, clock};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

fn parse_sync(s: &str) -> Result<SyncPolicy, String> {
    match s {
        "full" => Ok(SyncPolicy::Full),
        "data" => Ok(SyncPolicy::Data),
        "none" => Ok(SyncPolicy::None),
        other => Err(format!("unknown sync policy {other:?}: full|data|none")),
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "loadgen",
    about = "Open-loop submit→start latency for TaskerRT"
)]
struct Args {
    /// Submissions per second.
    #[arg(long, default_value_t = 2_000)]
    rate: u64,
    /// How long to drive load.
    #[arg(long, default_value_t = 10)]
    seconds: u64,
    #[arg(long, default_value_t = 4)]
    workers: u32,
    /// Capacity per worker, millicores.
    #[arg(long, default_value_t = 64_000)]
    worker_cpu: u32,
    /// Per-job request, millicores.
    #[arg(long, default_value_t = 100)]
    job_cpu: u32,
    /// Each task sleeps this long, milliseconds.
    #[arg(long, default_value_t = 10)]
    task_ms: u64,
    /// Scheduler tick, milliseconds.
    #[arg(long, default_value_t = 5)]
    tick_ms: u64,
    /// Durability root for the in-process daemon. Omit to run in memory only.
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,
    /// WAL sync policy when `--data-dir` is set: full, data, or none.
    #[arg(long, default_value = "data", value_parser = parse_sync)]
    wal_sync: SyncPolicy,
}

/// Wraps `SleepExecutor`: bytes 8..16 of the payload carry the intended send
/// time (daemon clock); the gap to now is the sample.
#[derive(Clone)]
struct LatencyExecutor {
    hist: Arc<Mutex<Histogram<u64>>>,
}

impl TaskExecutor for LatencyExecutor {
    async fn run(&self, payload: Bytes) -> Result<(), TaskError> {
        let started = clock::now().as_nanos();
        if let Some(stamp) = payload.get(8..16).and_then(|b| b.try_into().ok()) {
            let intended = u64::from_le_bytes(stamp);
            let micros = started.saturating_sub(intended) / 1_000;
            // Short critical section; the histogram write is ~100 ns.
            self.hist
                .lock()
                .expect("histogram poisoned")
                .record(micros.max(1))
                .ok();
        }
        SleepExecutor.run(payload).await
    }
}

fn stamped(sleep: Duration, intended_nanos: u64) -> Bytes {
    let mut v = sleep_payload(sleep).to_vec();
    v.extend_from_slice(&intended_nanos.to_le_bytes());
    Bytes::from(v)
}

type Client = ControlApiClient<tonic::transport::Channel>;

/// Polls `Nodes` until `n` workers report attached.
async fn wait_attached(client: &mut Client, n: usize) -> anyhow::Result<()> {
    loop {
        let nodes = client.nodes(v1::NodesRequest {}).await?.into_inner().nodes;
        if nodes.iter().filter(|x| x.attached).count() == n {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Polls `Queue` until nothing is ready or running.
async fn wait_drained(client: &mut Client) -> anyhow::Result<()> {
    loop {
        let q = client.queue(v1::QueueRequest {}).await?.into_inner();
        if q.ready == 0 && q.running == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Prints the distribution. Takes the lock here so no guard outlives the call.
fn print_report(hist: &Mutex<Histogram<u64>>, args: &Args, rejected: u64) {
    let h = hist.lock().expect("histogram poisoned");
    println!(
        "submit→start latency, open loop @ {} /s for {} s, {} workers, tick {} ms, task {} ms",
        args.rate, args.seconds, args.workers, args.tick_ms, args.task_ms
    );
    println!("samples {}  rejected {}", h.len(), rejected);
    for q in [50.0, 90.0, 99.0, 99.9, 99.99, 100.0] {
        println!("p{q:<6} {:>10} µs", h.value_at_quantile(q / 100.0));
    }
    println!("mean   {:>10.0} µs", h.mean());
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let hist = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(
        1, 60_000_000, 3,
    )?));

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let config = DaemonConfig {
        listen: addr,
        tick: Duration::from_millis(args.tick_ms),
        data_dir: args.data_dir.clone(),
        wal_sync: args.wal_sync,
        ..DaemonConfig::default()
    };
    let daemon = tokio::spawn(Daemon::new(config).serve(listener, async {
        let _ = stop_rx.await;
    }));

    let mut client = loop {
        if let Ok(c) = ControlApiClient::connect(format!("http://{addr}")).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let mut workers = Vec::new();
    for i in 0..args.workers {
        let cfg = WorkerConfig {
            daemon: format!("http://{addr}"),
            name: format!("lg-{i}"),
            capacity: Resources::new(args.worker_cpu, 0, 0),
            heartbeat: Duration::from_secs(1),
            drain_timeout: Duration::from_secs(5),
        };
        workers.push(tokio::spawn(
            Worker::new(
                cfg,
                LatencyExecutor {
                    hist: Arc::clone(&hist),
                },
            )
            .run(),
        ));
    }
    wait_attached(&mut client, args.workers as usize).await?;

    let period = Duration::from_nanos(1_000_000_000 / args.rate.max(1));
    let total = args.rate * args.seconds;
    let task = Duration::from_millis(args.task_ms);
    let start = clock::now().as_nanos();
    let mut interval = tokio::time::interval(period);
    // Burst: if we fall behind, fire the missed ticks back-to-back rather than
    // dropping them. The stamps below still carry the *intended* times.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let rejected = Arc::new(AtomicU64::new(0));

    for i in 0..total {
        interval.tick().await;
        let intended = start + i * u64::try_from(period.as_nanos()).unwrap_or(u64::MAX);
        let req = v1::SubmitRequest {
            account: 0,
            priority_class: v1::PriorityClass::Normal as i32,
            request: Some(v1::Resources {
                cpu_millis: args.job_cpu,
                mem_bytes: 0,
                gpus: 0,
            }),
            walltime_nanos: 60_000_000_000,
            deps: vec![],
            payload: stamped(task, intended),
        };
        // Fire and forget: the reply is not the latency we measure.
        let mut c = client.clone();
        let rejected = Arc::clone(&rejected);
        tokio::spawn(async move {
            if c.submit(req).await.is_err() {
                // Relaxed: a statistic, read once at the end.
                rejected.fetch_add(1, Ordering::Relaxed);
            }
        });
        if i % 1_000 == 0 {
            let q = client.queue(v1::QueueRequest {}).await?.into_inner();
            eprint!(
                "\rsubmitted {i:>8}  ready {:>6}  running {:>6}",
                q.ready, q.running
            );
        }
    }
    eprintln!();

    wait_drained(&mut client).await?;

    print_report(&hist, &args, rejected.load(Ordering::Relaxed));

    let _ = stop_tx.send(());
    daemon.await??;
    for w in workers {
        w.abort();
    }
    Ok(())
}
