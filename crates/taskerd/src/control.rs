//! The `ControlApi` service: every RPC is a command onto the inbox and an
//! awaited reply.

use std::sync::Arc;

use tasker_core::{JobId, LifecycleError};
use tasker_proto::convert::{job_from_submit, job_state_to_proto, resources_to_proto};
use tasker_proto::v1;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

use crate::{Command, Inbox, Query, clock};

/// Holds the inbox; nothing else.
#[derive(Debug)]
pub struct ControlService {
    inbox: Arc<Inbox>,
}

impl ControlService {
    #[must_use]
    pub fn new(inbox: Arc<Inbox>) -> Self {
        Self { inbox }
    }

    fn push(&self, command: Command) -> Result<(), Status> {
        self.inbox
            .push(command)
            .map_err(|_| Status::resource_exhausted("ingest ring is full"))
    }
}

fn lifecycle_status(e: LifecycleError) -> Status {
    match e {
        LifecycleError::UnknownJob(_) => Status::not_found(e.to_string()),
        LifecycleError::Dependency(_) | LifecycleError::Account(_) => {
            Status::invalid_argument(e.to_string())
        }
        LifecycleError::NotSubmitted { .. } | LifecycleError::Transition(_) => {
            Status::failed_precondition(e.to_string())
        }
    }
}

fn gone() -> Status {
    Status::unavailable("scheduler stopped")
}

#[tonic::async_trait]
impl v1::control_api_server::ControlApi for ControlService {
    async fn submit(
        &self,
        request: Request<v1::SubmitRequest>,
    ) -> Result<Response<v1::SubmitResponse>, Status> {
        let job = job_from_submit(request.get_ref(), clock::now())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let (tx, rx) = oneshot::channel();
        self.push(Command::Submit { job, reply: tx })?;
        let (id, state) = rx.await.map_err(|_| gone())?.map_err(lifecycle_status)?;
        Ok(Response::new(v1::SubmitResponse {
            job_id: id.to_bits(),
            state: job_state_to_proto(state) as i32,
        }))
    }

    async fn cancel(
        &self,
        request: Request<v1::CancelRequest>,
    ) -> Result<Response<v1::CancelResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        self.push(Command::Cancel {
            id: JobId::from_bits(request.get_ref().job_id),
            reply: tx,
        })?;
        let cascaded = rx.await.map_err(|_| gone())?.map_err(lifecycle_status)?;
        Ok(Response::new(v1::CancelResponse {
            cascaded: cascaded.into_iter().map(JobId::to_bits).collect(),
        }))
    }

    async fn status(
        &self,
        request: Request<v1::StatusRequest>,
    ) -> Result<Response<v1::StatusResponse>, Status> {
        let id = JobId::from_bits(request.get_ref().job_id);
        let (tx, rx) = oneshot::channel();
        self.push(Command::Query(Query::Status { id, reply: tx }))?;
        let state = rx
            .await
            .map_err(|_| gone())?
            .ok_or_else(|| Status::not_found("no such job"))?;
        Ok(Response::new(v1::StatusResponse {
            job_id: id.to_bits(),
            state: job_state_to_proto(state) as i32,
        }))
    }

    async fn queue(
        &self,
        _request: Request<v1::QueueRequest>,
    ) -> Result<Response<v1::QueueResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        self.push(Command::Query(Query::Queue { reply: tx }))?;
        let s = rx.await.map_err(|_| gone())?;
        Ok(Response::new(v1::QueueResponse {
            ready: u32::try_from(s.ready).unwrap_or(u32::MAX),
            blocked: u32::try_from(s.blocked).unwrap_or(u32::MAX),
            running: u32::try_from(s.running).unwrap_or(u32::MAX),
            cycles: s.cycles,
        }))
    }

    async fn nodes(
        &self,
        _request: Request<v1::NodesRequest>,
    ) -> Result<Response<v1::NodesResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        self.push(Command::Query(Query::Nodes { reply: tx }))?;
        let nodes = rx.await.map_err(|_| gone())?;
        Ok(Response::new(v1::NodesResponse {
            nodes: nodes
                .into_iter()
                .map(|n| v1::Node {
                    slot: n.slot,
                    name: n.name,
                    capacity: Some(resources_to_proto(n.capacity)),
                    allocated: Some(resources_to_proto(n.allocated)),
                    attached: n.attached,
                })
                .collect(),
        }))
    }

    async fn accounts(
        &self,
        _request: Request<v1::AccountsRequest>,
    ) -> Result<Response<v1::AccountsResponse>, Status> {
        let (tx, rx) = oneshot::channel();
        self.push(Command::Query(Query::Accounts { reply: tx }))?;
        let accounts = rx.await.map_err(|_| gone())?;
        Ok(Response::new(v1::AccountsResponse {
            accounts: accounts
                .into_iter()
                .map(|a| v1::Account {
                    account: a.account,
                    shares: a.shares,
                    usage: a.usage,
                    running_cpu_millis: a.running_cpu,
                    fairshare: a.fairshare,
                })
                .collect(),
        }))
    }
}
