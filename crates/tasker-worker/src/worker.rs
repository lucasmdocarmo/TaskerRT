//! The attach loop.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tasker_core::Resources;
use tasker_proto::convert::resources_to_proto;
use tasker_proto::v1;
use tasker_proto::v1::daemon_message::Body as Down;
use tasker_proto::v1::worker_api_client::WorkerApiClient;
use tasker_proto::v1::worker_message::Body as Up;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Sleep;
use tokio_stream::wrappers::ReceiverStream;

use crate::TaskExecutor;

/// How to reach the daemon and what to advertise.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// e.g. `http://127.0.0.1:7070`
    pub daemon: String,
    pub name: String,
    pub capacity: Resources,
    pub heartbeat: Duration,
    /// After SIGTERM, how long to wait for in-flight tasks before exiting anyway.
    pub drain_timeout: Duration,
}

/// The worker could not attach or lost the daemon.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error(transparent)]
    Rpc(#[from] tonic::Status),
    #[error("daemon closed the stream before Welcome")]
    NoWelcome,
    #[error("outbound channel closed")]
    Outbound,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// In-flight tasks, aborted together when the worker goes away. Without this
/// a detached worker's tasks would keep the request stream open until they
/// finished, and the daemon would learn of the loss only then.
#[derive(Debug, Default)]
struct TaskSet(HashMap<u64, JoinHandle<()>>);

impl Drop for TaskSet {
    fn drop(&mut self) {
        // `values()` borrows; `abort` needs only `&JoinHandle`.
        for handle in self.0.values() {
            handle.abort();
        }
    }
}

/// One attached worker.
#[derive(Debug)]
pub struct Worker<E> {
    config: WorkerConfig,
    executor: Arc<E>,
}

fn up(body: Up) -> v1::WorkerMessage {
    v1::WorkerMessage { body: Some(body) }
}

impl<E: TaskExecutor> Worker<E> {
    #[must_use]
    pub fn new(config: WorkerConfig, executor: E) -> Self {
        Self {
            config,
            executor: Arc::new(executor),
        }
    }

    /// Attaches and serves assignments until the daemon closes the stream or,
    /// after SIGTERM, until in-flight tasks finish (or the drain deadline).
    ///
    /// # Errors
    /// Connection or stream failure, or the SIGTERM handler could not be installed.
    pub async fn run(self) -> Result<(), WorkerError> {
        let mut client = WorkerApiClient::connect(self.config.daemon.clone()).await?;

        let (tx, rx) = mpsc::channel(64);
        tx.send(up(Up::Hello(v1::Hello {
            name: self.config.name.clone(),
            capacity: Some(resources_to_proto(self.config.capacity)),
        })))
        .await
        .map_err(|_| WorkerError::Outbound)?;

        let mut inbound = client.attach(ReceiverStream::new(rx)).await?.into_inner();
        let slot = match inbound.message().await? {
            Some(v1::DaemonMessage {
                body: Some(Down::Welcome(w)),
            }) => w.slot,
            _ => return Err(WorkerError::NoWelcome),
        };
        tracing::info!(slot, name = %self.config.name, "attached");

        let mut tasks = TaskSet::default();
        let mut heartbeat = tokio::time::interval(self.config.heartbeat);
        // SIGTERM is how Kubernetes asks a pod to stop; register before the loop.
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut draining = false;
        let mut drain_poll = tokio::time::interval(Duration::from_millis(100));
        // `None` until draining starts; boxed so it can be polled in `select!`.
        let mut deadline: Option<Pin<Box<Sleep>>> = None;

        loop {
            // `select!` races the futures; whichever completes first runs its arm.
            tokio::select! {
                msg = inbound.message() => match msg? {
                    Some(v1::DaemonMessage { body: Some(Down::Assign(assign)) }) => {
                        let job_id = assign.job_id;
                        let handle = tokio::spawn(run_task(Arc::clone(&self.executor), assign, tx.clone()));
                        tasks.0.insert(job_id, handle);
                    }
                    Some(v1::DaemonMessage { body: Some(Down::Kill(kill)) }) => {
                        if let Some(handle) = tasks.0.remove(&kill.job_id) {
                            handle.abort();
                        }
                    }
                    Some(_) => {}
                    None => break,
                },
                _ = heartbeat.tick() => {
                    tasks.0.retain(|_, h| !h.is_finished());
                    if tx.send(up(Up::Heartbeat(v1::Heartbeat {}))).await.is_err() {
                        break;
                    }
                }
                // `if !draining`: a second SIGTERM changes nothing.
                _ = sigterm.recv(), if !draining => {
                    draining = true;
                    deadline = Some(Box::pin(tokio::time::sleep(self.config.drain_timeout)));
                    tx.send(up(Up::Draining(v1::Draining {}))).await.ok();
                    tracing::info!(slot, in_flight = tasks.0.len(), "draining");
                }
                // Not even polled until SIGTERM has arrived.
                _ = drain_poll.tick(), if draining => {
                    tasks.0.retain(|_, h| !h.is_finished());
                    if tasks.0.is_empty() {
                        tracing::info!(slot, "drained; exiting");
                        break;
                    }
                }
                () = async {
                    match deadline.as_mut() {
                        Some(d) => d.await,
                        None => std::future::pending().await,
                    }
                } => {
                    tracing::warn!(slot, in_flight = tasks.0.len(), "drain deadline; exiting");
                    break;
                }
            }
        }
        tracing::info!(slot, "detached");
        Ok(())
    }
}

/// Runs one assignment and reports its result. Never panics itself: the
/// executor runs in an inner task so its panic surfaces as a `JoinError`.
async fn run_task<E: TaskExecutor>(
    executor: Arc<E>,
    assign: v1::Assign,
    tx: mpsc::Sender<v1::WorkerMessage>,
) {
    let job_id = assign.job_id;
    let walltime = Duration::from_nanos(assign.walltime_nanos);
    let inner = tokio::spawn(async move { executor.run(assign.payload).await });
    let abort = inner.abort_handle();

    let result = match tokio::time::timeout(walltime, inner).await {
        Ok(Ok(Ok(()))) => v1::TaskResult::Succeeded,
        Ok(Ok(Err(e))) => {
            tracing::warn!(job_id, error = %e, "task failed");
            v1::TaskResult::Failed
        }
        Ok(Err(join)) if join.is_panic() => {
            tracing::error!(job_id, "task panicked");
            v1::TaskResult::Panicked
        }
        // Aborted by a Kill: the daemon already moved on; nothing to report.
        Ok(Err(_cancelled)) => return,
        Err(_elapsed) => {
            abort.abort();
            tracing::warn!(job_id, "task exceeded its walltime");
            v1::TaskResult::TimedOut
        }
    };
    tx.send(up(Up::Finished(v1::TaskFinished {
        job_id,
        result: result as i32,
    })))
    .await
    .ok();
}
