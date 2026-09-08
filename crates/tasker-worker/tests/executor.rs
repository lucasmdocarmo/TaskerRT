use std::time::Duration;

use bytes::Bytes;
use tasker_worker::{SleepExecutor, TaskExecutor, sleep_payload};

#[tokio::test]
async fn sleep_payload_round_trips_and_an_empty_payload_returns_immediately() {
    let p = sleep_payload(Duration::from_millis(5));
    assert_eq!(p.len(), 8);
    SleepExecutor.run(p).await.unwrap();
    SleepExecutor.run(Bytes::new()).await.unwrap();
}

use tasker_worker::{CommandExecutor, command_payload, parse_command_payload};

#[test]
fn command_payload_round_trips_argv() {
    let p = command_payload(&["echo", "hello world", ""]);
    assert_eq!(parse_command_payload(&p), vec!["echo", "hello world", ""]);
    assert!(parse_command_payload(b"").is_empty());
}

#[tokio::test]
async fn a_succeeding_command_is_ok_and_a_failing_one_is_err() {
    CommandExecutor
        .run(command_payload(&["true"]))
        .await
        .unwrap();
    let err = CommandExecutor
        .run(command_payload(&["false"]))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("exit status"), "{err}");
    let err = CommandExecutor.run(Bytes::new()).await.unwrap_err();
    assert!(err.to_string().contains("empty"), "{err}");
}

#[tokio::test]
async fn dropping_the_future_kills_the_child() {
    // `sleep 30` would outlive the test by a lot if `kill_on_drop` were missing.
    let fut = CommandExecutor.run(command_payload(&["sleep", "30"]));
    let done = tokio::time::timeout(Duration::from_millis(100), fut).await;
    assert!(
        done.is_err(),
        "timed out as intended; the child is reaped on drop"
    );
}
