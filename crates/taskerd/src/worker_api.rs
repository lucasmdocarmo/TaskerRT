//! The `WorkerApi` service: one bidirectional stream per worker.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tasker_core::JobId;
use tasker_proto::convert::resources_from_proto;
use tasker_proto::v1;
use tasker_proto::v1::daemon_message::Body as Down;
use tasker_proto::v1::worker_message::Body as Up;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::{Command, Inbox, Registry};

/// Holds the inbox and registry.
#[derive(Debug)]
pub struct WorkerService {
    inbox: Arc<Inbox>,
    registry: Arc<Registry>,
    heartbeat_timeout: Duration,
}

impl WorkerService {
    #[must_use]
    pub fn new(inbox: Arc<Inbox>, registry: Arc<Registry>, heartbeat_timeout: Duration) -> Self {
        Self {
            inbox,
            registry,
            heartbeat_timeout,
        }
    }
}

#[tonic::async_trait]
impl v1::worker_api_server::WorkerApi for WorkerService {
    type AttachStream = Pin<Box<dyn Stream<Item = Result<v1::DaemonMessage, Status>> + Send>>;

    async fn attach(
        &self,
        request: Request<Streaming<v1::WorkerMessage>>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        let mut inbound = request.into_inner();

        // The first message must introduce the worker.
        let Some(v1::WorkerMessage {
            body: Some(Up::Hello(hello)),
        }) = inbound.message().await?
        else {
            return Err(Status::invalid_argument("first message must be Hello"));
        };
        let capacity = resources_from_proto(
            hello
                .capacity
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("Hello.capacity"))?,
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.inbox
            .push(Command::WorkerJoined {
                name: hello.name.clone(),
                capacity,
                reply: reply_tx,
            })
            .map_err(|_| Status::resource_exhausted("ingest ring is full"))?;
        let slot = reply_rx
            .await
            .map_err(|_| Status::unavailable("scheduler stopped"))?;

        let (tx, rx) = mpsc::channel(64);
        self.registry.insert(slot, tx.clone());
        tx.send(Ok(v1::DaemonMessage {
            body: Some(Down::Welcome(v1::Welcome { slot })),
        }))
        .await
        .map_err(|_| Status::internal("stream closed before Welcome"))?;
        tracing::info!(slot, name = %hello.name, "worker attached");

        // The reader loop outlives this handler; it owns the inbound half.
        let inbox = Arc::clone(&self.inbox);
        let registry = Arc::clone(&self.registry);
        let timeout = self.heartbeat_timeout;
        tokio::spawn(async move {
            loop {
                // A lapse, an error, or a clean end all mean the worker is gone.
                let Ok(Ok(Some(msg))) = tokio::time::timeout(timeout, inbound.message()).await
                else {
                    break;
                };
                match msg.body {
                    Some(Up::Finished(f)) => {
                        let id = JobId::from_bits(f.job_id);
                        let ok = f.result == v1::TaskResult::Succeeded as i32;
                        let command = if ok {
                            Command::Completed { id }
                        } else {
                            Command::Failed { id }
                        };
                        if inbox.push_with_retry(command, 1_000).await.is_err() {
                            tracing::error!("inbox saturated; task report dropped");
                        }
                    }
                    Some(Up::Draining(_)) => {
                        if inbox
                            .push_with_retry(Command::WorkerDraining { slot }, 1_000)
                            .await
                            .is_err()
                        {
                            tracing::error!(slot, "inbox saturated; drain notice dropped");
                        }
                    }
                    Some(Up::Heartbeat(_) | Up::Hello(_)) | None => {}
                }
            }
            registry.remove(slot);
            tracing::info!(slot, "worker detached");
            if inbox
                .push_with_retry(Command::WorkerLeft { slot }, 1_000)
                .await
                .is_err()
            {
                tracing::error!(slot, "inbox saturated; slot left undrained");
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
