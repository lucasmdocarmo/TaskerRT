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
use tokio::signal::unix::{Signal, SignalKind, signal};
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

/// First retry delay after a lost daemon; doubles up to `RECONNECT_MAX`.
const RECONNECT_MIN: Duration = Duration::from_millis(200);
const RECONNECT_MAX: Duration = Duration::from_secs(5);

/// Why one connection ended.
enum SessionEnd {
    /// The stream closed or failed; tasks keep running, the worker reconnects.
    Disconnected,
    /// A SIGTERM drain finished: nothing left to run or report.
    Drained,
}

/// Everything that outlives a single connection to the daemon.
struct Session {
    tasks: TaskSet,
    /// Task results go here first; the open stream, if any, forwards them.
    results_tx: mpsc::Sender<v1::WorkerMessage>,
    results_rx: mpsc::Receiver<v1::WorkerMessage>,
    sigterm: Signal,
    draining: bool,
    /// `None` until draining starts; boxed so it can be polled in `select!`.
    deadline: Option<Pin<Box<Sleep>>>,
}

impl Session {
    /// Sleeps `backoff` between attach attempts while still honouring SIGTERM.
    /// Returns true when the worker should exit instead of retrying.
    async fn wait_backoff(&mut self, backoff: Duration, drain_timeout: Duration) -> bool {
        tokio::select! {
            () = tokio::time::sleep(backoff) => false,
            _ = self.sigterm.recv(), if !self.draining => {
                self.draining = true;
                self.deadline = Some(Box::pin(tokio::time::sleep(drain_timeout)));
                // Nothing to report to and nothing running: there is no reason to stay.
                self.tasks.0.is_empty()
            }
            () = async {
                match self.deadline.as_mut() {
                    Some(d) => d.await,
                    None => std::future::pending().await,
                }
            } => {
                tracing::warn!(in_flight = self.tasks.0.len(), "drain deadline while disconnected; exiting");
                true
            }
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

    /// Serves assignments for the life of the process. A lost stream is not
    /// the end: in-flight tasks keep running and the worker reconnects with
    /// backoff, reporting what it still holds. Returns after a SIGTERM drain.
    ///
    /// # Errors
    /// Only if the SIGTERM handler cannot be installed; connection failures retry.
    pub async fn run(self) -> Result<(), WorkerError> {
        // Results wait here while no stream is open; 1024 is far beyond any backlog.
        let (results_tx, results_rx) = mpsc::channel(1_024);
        let mut session = Session {
            tasks: TaskSet::default(),
            results_tx,
            results_rx,
            sigterm: signal(SignalKind::terminate())?,
            draining: false,
            deadline: None,
        };
        let mut backoff = RECONNECT_MIN;
        loop {
            match self.attach_once(&mut session).await {
                Ok(SessionEnd::Drained) => return Ok(()),
                Ok(SessionEnd::Disconnected) => {
                    if session.draining && session.tasks.0.is_empty() {
                        tracing::info!("drained while disconnected; exiting");
                        return Ok(());
                    }
                    // We were attached, so the daemon is reachable in principle: start small.
                    backoff = RECONNECT_MIN;
                    tracing::warn!(
                        in_flight = session.tasks.0.len(),
                        "daemon stream closed; reconnecting"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, in_flight = session.tasks.0.len(), ?backoff, "attach failed; retrying");
                }
            }
            if session
                .wait_backoff(backoff, self.config.drain_timeout)
                .await
            {
                return Ok(());
            }
            backoff = (backoff * 2).min(RECONNECT_MAX);
        }
    }

    /// One connection: attach, serve until the stream ends or the drain finishes.
    async fn attach_once(&self, session: &mut Session) -> Result<SessionEnd, WorkerError> {
        let mut client = WorkerApiClient::connect(self.config.daemon.clone()).await?;
        let (tx, rx) = mpsc::channel(64);
        // Prune finished tasks first: only what is still running is in flight.
        session.tasks.0.retain(|_, h| !h.is_finished());
        let in_flight: Vec<u64> = session.tasks.0.keys().copied().collect();
        tx.send(up(Up::Hello(v1::Hello {
            name: self.config.name.clone(),
            capacity: Some(resources_to_proto(self.config.capacity)),
            in_flight,
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
        tracing::info!(slot, name = %self.config.name, in_flight = session.tasks.0.len(), "attached");
        if session.draining {
            tx.send(up(Up::Draining(v1::Draining {}))).await.ok();
        }

        let mut heartbeat = tokio::time::interval(self.config.heartbeat);
        let mut drain_poll = tokio::time::interval(Duration::from_millis(100));
        // Destructuring a `&mut Session` hands out disjoint `&mut` borrows, one per arm.
        let Session {
            tasks,
            results_tx,
            results_rx,
            sigterm,
            draining,
            deadline,
        } = session;

        loop {
            // `select!` races the futures; whichever completes first runs its arm.
            tokio::select! {
                msg = inbound.message() => match msg? {
                    Some(v1::DaemonMessage { body: Some(Down::Assign(assign)) }) => {
                        let job_id = assign.job_id;
                        // Results go to the session's channel, so they survive this stream.
                        let handle = tokio::spawn(run_task(Arc::clone(&self.executor), assign, results_tx.clone()));
                        tasks.0.insert(job_id, handle);
                    }
                    Some(v1::DaemonMessage { body: Some(Down::Kill(kill)) }) => {
                        if let Some(handle) = tasks.0.remove(&kill.job_id) {
                            handle.abort();
                        }
                    }
                    Some(_) => {}
                    None => return Ok(SessionEnd::Disconnected),
                },
                Some(report) = results_rx.recv() => {
                    if let Err(lost) = tx.send(report).await {
                        // The stream is gone; keep the report for the next connection.
                        if results_tx.try_send(lost.0).is_err() {
                            tracing::error!("result backlog full; a task report was dropped");
                        }
                        return Ok(SessionEnd::Disconnected);
                    }
                }
                _ = heartbeat.tick() => {
                    tasks.0.retain(|_, h| !h.is_finished());
                    if tx.send(up(Up::Heartbeat(v1::Heartbeat {}))).await.is_err() {
                        return Ok(SessionEnd::Disconnected);
                    }
                }
                // `if !*draining`: a second SIGTERM changes nothing.
                _ = sigterm.recv(), if !*draining => {
                    *draining = true;
                    *deadline = Some(Box::pin(tokio::time::sleep(self.config.drain_timeout)));
                    tx.send(up(Up::Draining(v1::Draining {}))).await.ok();
                    tracing::info!(slot, in_flight = tasks.0.len(), "draining");
                }
                // Not even polled until SIGTERM has arrived.
                _ = drain_poll.tick(), if *draining => {
                    tasks.0.retain(|_, h| !h.is_finished());
                    if tasks.0.is_empty() && results_rx.is_empty() {
                        tracing::info!(slot, "drained; exiting");
                        return Ok(SessionEnd::Drained);
                    }
                }
                () = async {
                    match deadline.as_mut() {
                        Some(d) => d.await,
                        None => std::future::pending().await,
                    }
                } => {
                    tracing::warn!(slot, in_flight = tasks.0.len(), "drain deadline; exiting");
                    return Ok(SessionEnd::Drained);
                }
            }
        }
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
