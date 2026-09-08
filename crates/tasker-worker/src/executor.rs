//! What a worker does with a payload.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;

/// The task itself failed (as opposed to timing out or panicking).
#[derive(Debug, thiserror::Error)]
#[error("task failed: {0}")]
pub struct TaskError(pub String);

/// Executes one task. Static dispatch: `Worker<E>` is monomorphized per executor.
pub trait TaskExecutor: Send + Sync + 'static {
    /// Runs the task to completion or failure. Cancellation arrives as a drop.
    fn run(&self, payload: Bytes) -> impl Future<Output = Result<(), TaskError>> + Send;
}

/// Sleeps for the little-endian `u64` nanoseconds in the payload. The executor
/// for tests and load generation: deterministic, cancellable, no side effects.
#[derive(Clone, Copy, Debug, Default)]
pub struct SleepExecutor;

impl TaskExecutor for SleepExecutor {
    async fn run(&self, payload: Bytes) -> Result<(), TaskError> {
        // `first_chunk::<8>` borrows exactly 8 bytes as an array, or `None` if short.
        let nanos = payload
            .first_chunk::<8>()
            .map_or(0, |b| u64::from_le_bytes(*b));
        tokio::time::sleep(Duration::from_nanos(nanos)).await;
        Ok(())
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
    async fn run(&self, payload: Bytes) -> Result<(), TaskError> {
        let argv = parse_command_payload(&payload);
        let Some((program, rest)) = argv.split_first() else {
            return Err(TaskError("empty command payload".into()));
        };
        let status = tokio::process::Command::new(program)
            .args(rest)
            // Without this a dropped future would orphan the child.
            .kill_on_drop(true)
            .status()
            .await
            .map_err(|e| TaskError(format!("spawn {program}: {e}")))?;
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
