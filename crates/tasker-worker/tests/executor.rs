use std::time::Duration;

use bytes::Bytes;
use tasker_worker::{SleepExecutor, Stop, TaskExecutor, sleep_payload, stop_channel};

#[tokio::test]
async fn sleep_payload_round_trips_and_an_empty_payload_returns_immediately() {
    let p = sleep_payload(Duration::from_millis(5));
    assert_eq!(p.len(), 8);
    SleepExecutor.run(p, Stop::never()).await.unwrap();
    SleepExecutor
        .run(Bytes::new(), Stop::never())
        .await
        .unwrap();
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
        .run(command_payload(&["true"]), Stop::never())
        .await
        .unwrap();
    let err = CommandExecutor
        .run(command_payload(&["false"]), Stop::never())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("exit status"), "{err}");
    let err = CommandExecutor
        .run(Bytes::new(), Stop::never())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("empty"), "{err}");
}

#[tokio::test]
async fn dropping_the_future_kills_the_child() {
    // `sleep 30` would outlive the test by a lot if `kill_on_drop` were missing.
    let fut = CommandExecutor.run(command_payload(&["sleep", "30"]), Stop::never());
    let done = tokio::time::timeout(Duration::from_millis(100), fut).await;
    assert!(
        done.is_err(),
        "timed out as intended; the child is reaped on drop"
    );
}

#[tokio::test]
async fn a_stop_request_ends_a_command_promptly() {
    let (tx, stop) = stop_channel();
    let fut = CommandExecutor.run(command_payload(&["sleep", "30"]), stop);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(Some(Duration::from_secs(1))).ok();
    });
    // SIGTERM ends `sleep` at once; 500 ms is generous.
    let err = tokio::time::timeout(Duration::from_millis(500), fut)
        .await
        .expect("returned promptly after the stop")
        .unwrap_err();
    assert!(err.to_string().contains("stopped"), "{err}");
}

#[tokio::test]
async fn a_stopped_sleep_reports_stopped() {
    let (tx, stop) = stop_channel();
    let fut = SleepExecutor.run(sleep_payload(Duration::from_secs(30)), stop);
    tx.send(Some(Duration::from_secs(1))).ok();
    let err = tokio::time::timeout(Duration::from_millis(200), fut)
        .await
        .expect("returned at once")
        .unwrap_err();
    assert!(err.to_string().contains("stopped"), "{err}");
}
