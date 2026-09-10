//! The daemon and in-process workers talking over real gRPC on loopback.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tasker_core::{CycleConfig, PreemptConfig, PriorityClass, Resources, VirtualDuration};
use tasker_proto::v1;
use tasker_proto::v1::control_api_client::ControlApiClient;
use tasker_wal::SyncPolicy;
use tasker_worker::{
    SleepExecutor, Stop, TaskError, TaskExecutor, Worker, WorkerConfig, sleep_payload,
};
use taskerd::{Daemon, DaemonConfig, DaemonError};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

struct Rig {
    addr: SocketAddr,
    daemon: JoinHandle<Result<(), DaemonError>>,
    stop: Option<oneshot::Sender<()>>,
    client: ControlApiClient<Channel>,
}

async fn start() -> Rig {
    start_with(None, None).await
}

/// A fresh, empty directory under the OS temp root, unique per process and test.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("taskerrt-e2e-{}-{name}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Binds `addr` when given, so a restarted daemon can reuse its port.
async fn start_with(data_dir: Option<PathBuf>, addr: Option<SocketAddr>) -> Rig {
    // Logs go to the test's captured output; RUST_LOG selects the level.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init()
        .ok();
    let bind = addr.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
    let listener = TcpListener::bind(bind).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let config = DaemonConfig {
        listen: addr,
        tick: Duration::from_millis(2),
        heartbeat_timeout: Duration::from_millis(500),
        data_dir,
        wal_sync: SyncPolicy::None,
        // Urgent evicts; a short grace keeps the preemption test quick.
        cycle: CycleConfig {
            preempt: PreemptConfig {
                min_class: Some(PriorityClass::Urgent),
                max_preemptions: 3,
                grace: VirtualDuration::from_nanos(500_000_000),
            },
            ..DaemonConfig::default().cycle
        },
        ..DaemonConfig::default()
    };
    let daemon = tokio::spawn(Daemon::new(config).serve(listener, async {
        let _ = stop_rx.await;
    }));
    // The server may not be accepting yet; retry briefly.
    let client = loop {
        match ControlApiClient::connect(format!("http://{addr}")).await {
            Ok(c) => break c,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };
    Rig {
        addr,
        daemon,
        stop: Some(stop_tx),
        client,
    }
}

impl Rig {
    fn worker(&self, name: &str, cpu: u32) -> JoinHandle<Result<(), tasker_worker::WorkerError>> {
        self.worker_with(name, cpu, SleepExecutor)
    }

    fn worker_with<X: TaskExecutor>(
        &self,
        name: &str,
        cpu: u32,
        executor: X,
    ) -> JoinHandle<Result<(), tasker_worker::WorkerError>> {
        let config = WorkerConfig {
            daemon: format!("http://{}", self.addr),
            name: name.into(),
            capacity: Resources::new(cpu, 0, 0),
            heartbeat: Duration::from_millis(100),
            drain_timeout: Duration::from_secs(5),
        };
        tokio::spawn(Worker::new(config, executor).run())
    }

    async fn wait_attached(&mut self, n: usize) {
        let mut last = Vec::new();
        for _ in 0..500 {
            let nodes = self
                .client
                .nodes(v1::NodesRequest {})
                .await
                .unwrap()
                .into_inner()
                .nodes;
            if nodes.iter().filter(|x| x.attached).count() == n {
                return;
            }
            last = nodes
                .iter()
                .map(|x| (x.slot, x.name.clone(), x.attached))
                .collect();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("{n} workers never attached; last saw {last:?}");
    }

    async fn submit(&mut self, cpu: u32, sleep: Duration, deps: Vec<u64>) -> (u64, v1::JobState) {
        self.submit_class(cpu, sleep, deps, v1::PriorityClass::Normal)
            .await
    }

    async fn submit_class(
        &mut self,
        cpu: u32,
        sleep: Duration,
        deps: Vec<u64>,
        class: v1::PriorityClass,
    ) -> (u64, v1::JobState) {
        let resp = self
            .client
            .submit(v1::SubmitRequest {
                account: 1,
                priority_class: class as i32,
                request: Some(v1::Resources {
                    cpu_millis: cpu,
                    mem_bytes: 0,
                    gpus: 0,
                }),
                walltime_nanos: 60_000_000_000,
                deps,
                payload: sleep_payload(sleep),
            })
            .await
            .unwrap()
            .into_inner();
        (resp.job_id, v1::JobState::try_from(resp.state).unwrap())
    }

    async fn state(&mut self, id: u64) -> v1::JobState {
        let resp = self
            .client
            .status(v1::StatusRequest { job_id: id })
            .await
            .unwrap()
            .into_inner();
        v1::JobState::try_from(resp.state).unwrap()
    }

    async fn wait_state(&mut self, id: u64, want: v1::JobState, within: Duration) {
        // `timeout` wraps the whole poll loop; no clock reads needed here.
        let poll = async {
            loop {
                if self.state(id).await == want {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        assert!(
            tokio::time::timeout(within, poll).await.is_ok(),
            "job {id}: never reached {want:?} within {within:?}"
        );
    }

    async fn shutdown(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.daemon.await.unwrap().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_submitted_job_runs_to_completion_on_an_attached_worker() {
    let mut rig = start().await;
    let _w = rig.worker("w1", 4_000);
    rig.wait_attached(1).await;

    let (id, state) = rig.submit(1_000, Duration::from_millis(20), vec![]).await;
    assert_eq!(state, v1::JobState::Ready);
    rig.wait_state(id, v1::JobState::Completed, Duration::from_secs(3))
        .await;

    let q = rig
        .client
        .queue(v1::QueueRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(q.running, 0);
    assert!(q.cycles > 0);
    rig.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dependency_chain_runs_in_order() {
    let mut rig = start().await;
    let _w = rig.worker("w1", 4_000);
    rig.wait_attached(1).await;

    let (a, _) = rig.submit(1_000, Duration::from_millis(30), vec![]).await;
    let (b, sb) = rig.submit(1_000, Duration::from_millis(10), vec![a]).await;
    assert_eq!(sb, v1::JobState::Blocked);
    rig.wait_state(b, v1::JobState::Completed, Duration::from_secs(3))
        .await;
    assert_eq!(rig.state(a).await, v1::JobState::Completed);
    rig.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_kills_a_running_job() {
    let mut rig = start().await;
    let _w = rig.worker("w1", 4_000);
    rig.wait_attached(1).await;

    let (id, _) = rig.submit(1_000, Duration::from_secs(30), vec![]).await;
    rig.wait_state(id, v1::JobState::Running, Duration::from_secs(3))
        .await;
    rig.client
        .cancel(v1::CancelRequest { job_id: id })
        .await
        .unwrap();
    rig.wait_state(id, v1::JobState::Cancelled, Duration::from_secs(1))
        .await;
    let q = rig
        .client
        .queue(v1::QueueRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(q.running, 0);
    rig.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn losing_a_worker_requeues_its_job_onto_the_next_one() {
    let mut rig = start().await;
    let w1 = rig.worker("w1", 4_000);
    rig.wait_attached(1).await;

    let (id, _) = rig.submit(1_000, Duration::from_millis(400), vec![]).await;
    rig.wait_state(id, v1::JobState::Running, Duration::from_secs(3))
        .await;

    // Kill the worker task: its stream closes and the daemon sees WorkerLeft.
    w1.abort();
    rig.wait_state(id, v1::JobState::Ready, Duration::from_secs(2))
        .await;
    rig.wait_attached(0).await;

    let _w2 = rig.worker("w2", 4_000);
    rig.wait_attached(1).await;
    rig.wait_state(id, v1::JobState::Completed, Duration::from_secs(3))
        .await;
    rig.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_job_is_not_found_and_a_bad_submit_is_invalid() {
    let mut rig = start().await;
    let err = rig
        .client
        .status(v1::StatusRequest { job_id: 999 })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
    let err = rig
        .client
        .submit(v1::SubmitRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    rig.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accounts_report_usage_for_the_account_that_ran() {
    let mut rig = start().await;
    let _w = rig.worker("w1", 4_000);
    rig.wait_attached(1).await;
    let resp = rig
        .client
        .submit(v1::SubmitRequest {
            account: 3,
            priority_class: v1::PriorityClass::Normal as i32,
            request: Some(v1::Resources {
                cpu_millis: 2_000,
                mem_bytes: 0,
                gpus: 0,
            }),
            walltime_nanos: 10_000_000_000,
            deps: vec![],
            payload: sleep_payload(Duration::from_millis(150)),
        })
        .await
        .unwrap()
        .into_inner();
    rig.wait_state(resp.job_id, v1::JobState::Completed, Duration::from_secs(5))
        .await;

    let accounts = rig
        .client
        .accounts(v1::AccountsRequest {})
        .await
        .unwrap()
        .into_inner()
        .accounts;
    for a in &accounts {
        eprintln!("{a:?}");
    }
    assert_eq!(accounts.len(), 4, "ids 0..=3 exist once account 3 was seen");
    let ran = &accounts[3];
    // 150 ms on two cores is 300 000 millicore-milliseconds at minimum.
    assert!(ran.usage >= 150 * 2_000, "usage {}", ran.usage);
    assert_eq!(ran.running_cpu_millis, 0);
    assert!(ran.fairshare < 1_000_000);
    let idle = &accounts[2];
    assert_eq!(idle.usage, 0);
    assert_eq!(idle.fairshare, 1_000_000);
    rig.shutdown().await;
}

/// Counts executions, then sleeps like `SleepExecutor`.
#[derive(Clone)]
struct Counting(Arc<AtomicUsize>);

impl TaskExecutor for Counting {
    async fn run(&self, payload: Bytes, stop: Stop) -> Result<(), TaskError> {
        // Relaxed: the test reads the total only after every task has finished.
        self.0.fetch_add(1, Ordering::Relaxed);
        SleepExecutor.run(payload, stop).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_daemon_reclaims_work_its_worker_kept_running() {
    let dir = scratch("restart");
    let runs = Arc::new(AtomicUsize::new(0));
    let mut rig = start_with(Some(dir.clone()), None).await;
    let addr = rig.addr;
    let _w1 = rig.worker_with("w1", 4_000, Counting(Arc::clone(&runs)));
    rig.wait_attached(1).await;
    let (done, _) = rig.submit(1_000, Duration::from_millis(50), vec![]).await;
    rig.wait_state(done, v1::JobState::Completed, Duration::from_secs(3))
        .await;
    let (long, _) = rig.submit(1_000, Duration::from_secs(3), vec![]).await;
    rig.wait_state(long, v1::JobState::Running, Duration::from_secs(3))
        .await;
    let (after, state) = rig
        .submit(1_000, Duration::from_millis(50), vec![long])
        .await;
    assert_eq!(state, v1::JobState::Blocked);
    // The daemon goes away. w1 keeps running `long` and starts reconnecting.
    rig.shutdown().await;

    let mut rig = start_with(Some(dir), Some(addr)).await;
    rig.wait_attached(1).await;
    assert_eq!(rig.state(done).await, v1::JobState::Completed);
    assert_eq!(rig.state(long).await, v1::JobState::Running);
    rig.wait_state(long, v1::JobState::Completed, Duration::from_secs(6))
        .await;
    rig.wait_state(after, v1::JobState::Completed, Duration::from_secs(3))
        .await;
    // done, long, after: three executions. A rerun of `long` would make four.
    assert_eq!(runs.load(Ordering::Relaxed), 3, "long ran exactly once");
    rig.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_urgent_job_preempts_a_running_low_job_which_reruns() {
    let mut rig = start().await;
    let _w = rig.worker("w1", 4_000);
    rig.wait_attached(1).await;
    // A Low job takes the whole worker for three seconds.
    let (low, _) = rig
        .submit_class(
            4_000,
            Duration::from_secs(3),
            vec![],
            v1::PriorityClass::Low,
        )
        .await;
    rig.wait_state(low, v1::JobState::Running, Duration::from_secs(3))
        .await;
    // An Urgent job that needs the whole worker cannot wait; the Low one is evicted.
    let (urgent, _) = rig
        .submit_class(
            4_000,
            Duration::from_millis(300),
            vec![],
            v1::PriorityClass::Urgent,
        )
        .await;
    rig.wait_state(urgent, v1::JobState::Running, Duration::from_secs(3))
        .await;
    assert_ne!(
        rig.state(low).await,
        v1::JobState::Running,
        "the slot is the urgent job's now"
    );
    rig.wait_state(urgent, v1::JobState::Completed, Duration::from_secs(3))
        .await;
    // The victim reruns from the start and finishes.
    rig.wait_state(low, v1::JobState::Completed, Duration::from_secs(6))
        .await;
    rig.shutdown().await;
}
