//! The daemon and in-process workers talking over real gRPC on loopback.

use std::net::SocketAddr;
use std::time::Duration;

use tasker_core::Resources;
use tasker_proto::v1;
use tasker_proto::v1::control_api_client::ControlApiClient;
use tasker_worker::{SleepExecutor, Worker, WorkerConfig, sleep_payload};
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
    // Logs go to the test's captured output; RUST_LOG selects the level.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init()
        .ok();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let config = DaemonConfig {
        listen: addr,
        tick: Duration::from_millis(2),
        heartbeat_timeout: Duration::from_millis(500),
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
        let config = WorkerConfig {
            daemon: format!("http://{}", self.addr),
            name: name.into(),
            capacity: Resources::new(cpu, 0, 0),
            heartbeat: Duration::from_millis(100),
            drain_timeout: Duration::from_secs(5),
        };
        tokio::spawn(Worker::new(config, SleepExecutor).run())
    }

    async fn wait_attached(&mut self, n: usize) {
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
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("{n} workers never attached");
    }

    async fn submit(&mut self, cpu: u32, sleep: Duration, deps: Vec<u64>) -> (u64, v1::JobState) {
        let resp = self
            .client
            .submit(v1::SubmitRequest {
                account: 1,
                priority_class: v1::PriorityClass::Normal as i32,
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
