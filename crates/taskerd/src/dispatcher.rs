//! Drains the outbox onto worker streams. A send that fails means the worker
//! is gone; the scheduler is told so it can requeue.

use std::sync::Arc;

use tasker_proto::v1;
use tasker_proto::v1::daemon_message::Body;
use tokio::sync::watch;

use crate::{Command, Dispatch, Inbox, Outbox, Registry};

/// The dispatcher task body.
pub async fn run(
    outbox: Arc<Outbox>,
    registry: Arc<Registry>,
    inbox: Arc<Inbox>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        while let Some(dispatch) = outbox.pop() {
            deliver(dispatch, &registry, &inbox).await;
        }
        // Drain first, then wait: `notify_one` keeps a permit, so a push that
        // landed between `pop` returning None and this await is not lost.
        tokio::select! {
            () = outbox.notified() => {}
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
        }
    }
}

async fn deliver(dispatch: Dispatch, registry: &Registry, inbox: &Inbox) {
    let (slot, body) = match dispatch {
        Dispatch::Assign {
            slot,
            id,
            payload,
            walltime,
        } => (
            slot,
            Body::Assign(v1::Assign {
                job_id: id.to_bits(),
                payload,
                walltime_nanos: walltime.as_nanos(),
            }),
        ),
        Dispatch::Kill { slot, id } => (
            slot,
            Body::Kill(v1::Kill {
                job_id: id.to_bits(),
            }),
        ),
    };
    let Some(tx) = registry.sender(slot) else {
        tracing::warn!(slot, "dispatch to a slot with no attached worker");
        if inbox
            .push_with_retry(Command::WorkerLeft { slot }, 1_000)
            .await
            .is_err()
        {
            tracing::error!(slot, "inbox saturated; slot left undrained");
        }
        return;
    };
    if tx
        .send(Ok(v1::DaemonMessage { body: Some(body) }))
        .await
        .is_err()
    {
        tracing::warn!(slot, "worker stream closed during dispatch");
        registry.remove(slot);
        if inbox
            .push_with_retry(Command::WorkerLeft { slot }, 1_000)
            .await
            .is_err()
        {
            tracing::error!(slot, "inbox saturated; slot left undrained");
        }
    }
}
