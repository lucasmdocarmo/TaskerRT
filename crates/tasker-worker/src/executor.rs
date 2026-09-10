//! What a worker does with a payload.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::watch;

/// The task itself failed (as opposed to timing out or panicking).
#[derive(Debug, thiserror::Error)]
#[error("task failed: {0}")]
pub struct TaskError(pub String);

/// A request to stop early: the daemon evicted the job. Carries the grace the
/// worker will allow before killing the task outright.
#[derive(Clone, Debug)]
pub struct Stop(watch::Receiver<Option<Duration>>);

impl Stop {
    /// A signal that never fires, for callers with nothing to evict.
    #[must_use]
    pub fn never() -> Self {
        let (tx, rx) = watch::channel(None);
        // Dropping the sender makes `wait` pend forever, which is the point.
        drop(tx);
        Self(rx)
    }

    /// Resolves once a stop has been requested.
    pub async fn wait(&mut self) {
        // `wait_for` resolves when the predicate holds; an error means the sender is
        // gone, and a sender that can never ask is a stop that never comes.
        if self.0.wait_for(Option::is_some).await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    /// The grace granted, once a stop has been requested.
    #[must_use]
    pub fn grace(&self) -> Option<Duration> {
        *self.0.borrow()
    }
}

/// A stop signal and the handle that fires it.
#[must_use]
pub fn stop_channel() -> (watch::Sender<Option<Duration>>, Stop) {
    let (tx, rx) = watch::channel(None);
    (tx, Stop(rx))
}

/// Executes one task. Static dispatch: `Worker<E>` is monomorphized per executor.
pub trait TaskExecutor: Send + Sync + 'static {
    /// Runs the task to completion or failure. `stop` fires on eviction: return
    /// promptly with an error, or keep going and be killed at the grace deadline.
    /// Cancellation still arrives as a drop.
    fn run(&self, payload: Bytes, stop: Stop)
    -> impl Future<Output = Result<(), TaskError>> + Send;
}

/// Sleeps for the little-endian `u64` nanoseconds in the payload. The executor
/// for tests and load generation: deterministic, cancellable, no side effects.
#[derive(Clone, Copy, Debug, Default)]
pub struct SleepExecutor;

impl TaskExecutor for SleepExecutor {
    async fn run(&self, payload: Bytes, mut stop: Stop) -> Result<(), TaskError> {
        // `first_chunk::<8>` borrows exactly 8 bytes as an array, or `None` if short.
        let nanos = payload
            .first_chunk::<8>()
            .map_or(0, |b| u64::from_le_bytes(*b));
        tokio::select! {
            () = tokio::time::sleep(Duration::from_nanos(nanos)) => Ok(()),
            () = stop.wait() => Err(TaskError("stopped".into())),
        }
    }
}

/// Encodes a duration for `SleepExecutor`.
#[must_use]
pub fn sleep_payload(d: Duration) -> Bytes {
    let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
    Bytes::copy_from_slice(&nanos.to_le_bytes())
}

/// Runs the payload as a command: argv entries separated by NUL bytes, the
/// first being the program. Non-zero exit is a `TaskError`. The child is
/// killed when the future is dropped, so a `Kill` or worker death reaps it.
#[derive(Clone, Copy, Debug, Default)]
pub struct CommandExecutor;

impl TaskExecutor for CommandExecutor {
    // The one syscall here is `kill` on a child we spawned; no memory is involved.
    #[allow(unsafe_code)]
    async fn run(&self, payload: Bytes, mut stop: Stop) -> Result<(), TaskError> {
        let argv = parse_command_payload(&payload);
        let Some((program, rest)) = argv.split_first() else {
            return Err(TaskError("empty command payload".into()));
        };
        let mut child = tokio::process::Command::new(program)
            .args(rest)
            // Without this a dropped future would orphan the child.
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| TaskError(format!("spawn {program}: {e}")))?;
        let status = tokio::select! {
            status = child.wait() => status,
            () = stop.wait() => {
                // Cooperative first: SIGTERM and wait. The worker drops this future at
                // the grace deadline, and `kill_on_drop` finishes the job.
                if let Some(pid) = child.id().and_then(|p| libc::pid_t::try_from(p).ok()) {
                    // SAFETY: a plain syscall on a pid this process spawned and still owns.
                    unsafe {
                        libc::kill(pid, libc::SIGTERM);
                    }
                }
                child.wait().await.ok();
                return Err(TaskError(format!("{program}: stopped")));
            }
        };
        let status = status.map_err(|e| TaskError(format!("wait {program}: {e}")))?;
        if status.success() {
            Ok(())
        } else {
            Err(TaskError(format!("{program}: {status}")))
        }
    }
}

/// Encodes argv for `CommandExecutor`: NUL-separated, no trailing NUL.
#[must_use]
pub fn command_payload<S: AsRef<str>>(argv: &[S]) -> Bytes {
    let joined = argv
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join("\0");
    Bytes::from(joined.into_bytes())
}

/// Decodes a `command_payload`. An empty payload is an empty argv.
#[must_use]
pub fn parse_command_payload(payload: &[u8]) -> Vec<String> {
    if payload.is_empty() {
        return Vec::new();
    }
    payload
        .split(|b| *b == 0)
        .map(|part| String::from_utf8_lossy(part).into_owned())
        .collect()
}
