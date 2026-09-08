//! A worker *process* is killed with SIGKILL mid-task; the daemon requeues the job and
//! a second process finishes it.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tasker_proto::v1;
use tasker_proto::v1::control_api_client::ControlApiClient;
use tasker_worker::sleep_payload;
use taskerd::{Daemon, DaemonConfig};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

fn spawn_worker(addr: &str, name: &str) -> Child {
    // Cargo sets this for the current package's binaries at test-build time.
    Command::new(env!("CARGO_BIN_EXE_tasker-worker"))
        .args([
            "--daemon",
            &format!("http://{addr}"),
            "--name",
            name,
            "--cpu-millis",
            "4000",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn tasker-worker")
}

async fn state(c: &mut ControlApiClient<tonic::transport::Channel>, id: u64) -> v1::JobState {
    let r = c
        .status(v1::StatusRequest { job_id: id })
        .await
        .unwrap()
        .into_inner();
    v1::JobState::try_from(r.state).unwrap()
}

async fn wait_state(
    c: &mut ControlApiClient<tonic::transport::Channel>,
    id: u64,
    want: v1::JobState,
    within: Duration,
) {
    let poll = async {
        loop {
            if state(c, id).await == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    assert!(
        tokio::time::timeout(within, poll).await.is_ok(),
        "job {id} never reached {want:?}"
    );
}

async fn wait_attached(c: &mut ControlApiClient<tonic::transport::Channel>, n: usize) {
    let poll = async {
        loop {
            let nodes = c
                .nodes(v1::NodesRequest {})
                .await
                .unwrap()
                .into_inner()
                .nodes;
            if nodes.iter().filter(|x| x.attached).count() == n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(5), poll)
            .await
            .is_ok(),
        "{n} attached never seen"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sigkilled_worker_process_loses_nothing() {
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
    let mut client = loop {
        if let Ok(c) = ControlApiClient::connect(format!("http://{addr}")).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let mut w1 = spawn_worker(&addr.to_string(), "proc-w1");
    wait_attached(&mut client, 1).await;

    let id = client
        .submit(v1::SubmitRequest {
            account: 0,
            priority_class: v1::PriorityClass::Normal as i32,
            request: Some(v1::Resources {
                cpu_millis: 1_000,
                mem_bytes: 0,
                gpus: 0,
            }),
            walltime_nanos: 60_000_000_000,
            deps: vec![],
            payload: sleep_payload(Duration::from_millis(300)),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;
    wait_state(
        &mut client,
        id,
        v1::JobState::Running,
        Duration::from_secs(3),
    )
    .await;

    // SIGKILL: no goodbye. The kernel closes the socket; the daemon must notice.
    w1.kill().unwrap();
    w1.wait().unwrap();
    wait_state(&mut client, id, v1::JobState::Ready, Duration::from_secs(3)).await;
    wait_attached(&mut client, 0).await;

    let mut w2 = spawn_worker(&addr.to_string(), "proc-w2");
    wait_attached(&mut client, 1).await;
    wait_state(
        &mut client,
        id,
        v1::JobState::Completed,
        Duration::from_secs(3),
    )
    .await;

    w2.kill().unwrap();
    w2.wait().unwrap();
    let _ = stop_tx.send(());
    daemon.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sigtermed_worker_finishes_its_task_then_exits_cleanly() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let config = DaemonConfig {
        listen: addr,
        tick: Duration::from_millis(2),
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

    let mut w = spawn_worker(&addr.to_string(), "drain-w");
    wait_attached(&mut client, 1).await;
    let id = client
        .submit(v1::SubmitRequest {
            account: 0,
            priority_class: v1::PriorityClass::Normal as i32,
            request: Some(v1::Resources {
                cpu_millis: 1_000,
                mem_bytes: 0,
                gpus: 0,
            }),
            walltime_nanos: 60_000_000_000,
            deps: vec![],
            payload: sleep_payload(Duration::from_millis(300)),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;
    wait_state(
        &mut client,
        id,
        v1::JobState::Running,
        Duration::from_secs(3),
    )
    .await;

    // SIGTERM, not SIGKILL: the worker gets to finish.
    let status = Command::new("kill")
        .args(["-TERM", &w.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    wait_state(
        &mut client,
        id,
        v1::JobState::Completed,
        Duration::from_secs(3),
    )
    .await;

    let exit = tokio::time::timeout(
        Duration::from_secs(4),
        tokio::task::spawn_blocking(move || w.wait()),
    )
    .await
    .expect("worker exited within 4 s")
    .unwrap()
    .unwrap();
    assert!(exit.success(), "clean exit after drain, got {exit}");
    let _ = stop_tx.send(());
    daemon.await.unwrap().unwrap();
}
