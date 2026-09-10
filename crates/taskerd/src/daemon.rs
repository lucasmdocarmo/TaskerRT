//! Wires the thread, the tasks, and the servers together.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_utils::sync::Parker;
use tasker_proto::externalscaler::external_scaler_server::ExternalScalerServer;
use tasker_proto::v1::control_api_server::ControlApiServer;
use tasker_proto::v1::worker_api_server::WorkerApiServer;
use tasker_wal::Store;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::control::ControlService;
use crate::scaler::ScalerService;
use crate::worker_api::WorkerService;
use crate::{
    COMMIT_QUEUE_DEPTH, DaemonConfig, Engine, Inbox, Journal, Metrics, Outbox, RecoverError,
    Registry, dispatcher, journal, metrics,
};

/// The daemon could not run to completion.
#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("scheduler thread panicked")]
    SchedulerPanic,
    #[error(transparent)]
    Wal(#[from] tasker_wal::WalError),
    #[error(transparent)]
    Recovery(#[from] RecoverError),
    #[error("WAL writer thread panicked")]
    WalPanic,
}

/// A configured, not-yet-running daemon.
#[derive(Debug)]
pub struct Daemon {
    config: DaemonConfig,
}

impl Daemon {
    #[must_use]
    pub fn new(config: DaemonConfig) -> Self {
        Self { config }
    }

    /// Serves on `listener` until `shutdown` resolves, then stops everything
    /// in order: servers, dispatcher, scheduler thread.
    ///
    /// # Errors
    /// Transport or I/O failure, or a panicked scheduler thread.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), DaemonError> {
        let parker = Parker::new();
        let inbox = Arc::new(Inbox::new(
            self.config.inbox_capacity,
            parker.unparker().clone(),
        ));
        let outbox = Arc::new(Outbox::new(self.config.outbox_capacity));
        let registry = Arc::new(Registry::new());
        let stop = Arc::new(AtomicBool::new(false));
        let quiesce = Arc::new(AtomicBool::new(false));

        let metrics = Metrics::new();
        let (engine, writer) = match self.config.data_dir.as_deref() {
            Some(dir) => {
                let recovered = Store::recover(dir)?;
                let store = Store::resume(dir, recovered.generation)?;
                let (tx, rx) = std::sync::mpsc::sync_channel(COMMIT_QUEUE_DEPTH);
                let journal = Journal::new(tx, self.config.wal_rotate_bytes);
                let engine =
                    Engine::recover(&self.config, Arc::clone(&metrics), recovered, journal)?;
                let writer = {
                    let outbox = Arc::clone(&outbox);
                    let policy = self.config.wal_sync;
                    std::thread::Builder::new()
                        .name("tasker-wal".into())
                        .spawn(move || journal::run_writer(store, policy, outbox, rx))?
                };
                (engine, Some(writer))
            }
            None => (Engine::new(&self.config, Arc::clone(&metrics)), None),
        };
        let thread = {
            let inbox = Arc::clone(&inbox);
            let outbox = Arc::clone(&outbox);
            let stop = Arc::clone(&stop);
            let quiesce = Arc::clone(&quiesce);
            let tick = self.config.tick;
            // A named OS thread: visible in profilers, never a tokio worker.
            // The closure owns the Arcs and the parker; `run` only borrows them.
            std::thread::Builder::new()
                .name("tasker-scheduler".into())
                .spawn(move || engine.run(&inbox, &outbox, &parker, tick, &stop, &quiesce))?
        };

        let (stop_tx, stop_rx) = watch::channel(false);
        let dispatcher = tokio::spawn(dispatcher::run(
            Arc::clone(&outbox),
            Arc::clone(&registry),
            Arc::clone(&inbox),
            stop_rx,
        ));

        let metrics_listener = metrics::bind(self.config.metrics_listen).await?;
        tracing::info!(metrics = %metrics_listener.local_addr()?, "metrics listening");
        let metrics_task = tokio::spawn(metrics::serve(metrics_listener, Arc::clone(&metrics)));

        let control = ControlApiServer::new(ControlService::new(Arc::clone(&inbox)));
        let workers = WorkerApiServer::new(WorkerService::new(
            Arc::clone(&inbox),
            Arc::clone(&registry),
            self.config.heartbeat_timeout,
        ));
        let scaler = ExternalScalerServer::new(ScalerService::new(Arc::clone(&inbox)));

        let addr = listener.local_addr()?;
        tracing::info!(%addr, "taskerd listening");
        // tonic's graceful shutdown waits for in-flight streams. Worker streams
        // are open for the daemon's lifetime, so close them first or it waits forever.
        let shutdown = {
            let registry = Arc::clone(&registry);
            let quiesce = Arc::clone(&quiesce);
            async move {
                shutdown.await;
                // Closing the streams makes every worker look lost. Tell the engine it is
                // the daemon leaving, so running jobs stay Running for the next incarnation.
                quiesce.store(true, Ordering::Release);
                registry.clear();
            }
        };
        Server::builder()
            .add_service(control)
            .add_service(workers)
            .add_service(scaler)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
            .await?;

        tracing::info!("shutting down");
        metrics_task.abort();
        stop_tx.send(true).ok();
        dispatcher.await.ok();
        // Release: the flag is the last write before the thread observes it and exits.
        stop.store(true, Ordering::Release);
        inbox.wake();
        thread.join().map_err(|_| DaemonError::SchedulerPanic)?;
        // The engine dropped its journal when `run` returned; the writer drains and exits.
        if let Some(writer) = writer {
            writer.join().map_err(|_| DaemonError::WalPanic)?;
        }
        Ok(())
    }
}
