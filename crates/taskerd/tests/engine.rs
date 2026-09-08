//! The engine driven by hand: no threads, no gRPC, a hand-fed inbox.

use std::time::Duration;

use bytes::Bytes;
use crossbeam_utils::sync::Parker;
use tasker_core::{
    AccountId, Job, JobId, JobState, PriorityClass, ResourceRequest, Resources, VirtualDuration,
    VirtualTime,
};
use taskerd::{Command, DaemonConfig, Dispatch, Engine, Inbox, Metrics, Outbox, Query};
use tokio::sync::oneshot;

fn config() -> DaemonConfig {
    DaemonConfig {
        tick: Duration::from_millis(1),
        inbox_capacity: 4,
        ..DaemonConfig::default()
    }
}

fn job(cpu: u32) -> Job {
    let mut j = Job::new(
        AccountId::new(0),
        PriorityClass::Normal,
        VirtualTime::ZERO,
        ResourceRequest::new(cpu, 0, 0),
        VirtualDuration::from_secs(60),
    );
    j.payload = Bytes::from_static(b"payload");
    j
}

struct Rig {
    engine: Engine,
    inbox: Inbox,
    outbox: Outbox,
    _parker: Parker,
}

fn rig() -> Rig {
    let cfg = config();
    let parker = Parker::new();
    Rig {
        engine: Engine::new(&cfg, Metrics::new()),
        inbox: Inbox::new(cfg.inbox_capacity, parker.unparker().clone()),
        outbox: Outbox::new(16),
        _parker: parker,
    }
}

fn join(r: &mut Rig, cpu: u32) -> u32 {
    let (tx, mut rx) = oneshot::channel();
    r.inbox
        .push(Command::WorkerJoined {
            name: "w".into(),
            capacity: Resources::new(cpu, 0, 0),
            reply: tx,
        })
        .unwrap();
    r.engine.drain(&r.inbox, VirtualTime::ZERO, &r.outbox);
    rx.try_recv().expect("replied synchronously during drain")
}

fn submit(r: &mut Rig, j: Job) -> (JobId, JobState) {
    let (tx, mut rx) = oneshot::channel();
    r.inbox.push(Command::Submit { job: j, reply: tx }).unwrap();
    r.engine.drain(&r.inbox, VirtualTime::ZERO, &r.outbox);
    rx.try_recv().unwrap().unwrap()
}

fn status(r: &mut Rig, id: JobId) -> Option<JobState> {
    let (tx, mut rx) = oneshot::channel();
    r.inbox
        .push(Command::Query(Query::Status { id, reply: tx }))
        .unwrap();
    r.engine.drain(&r.inbox, VirtualTime::ZERO, &r.outbox);
    rx.try_recv().unwrap()
}

#[test]
fn a_submit_is_dispatched_to_an_attached_worker_with_its_payload() {
    let mut r = rig();
    assert_eq!(join(&mut r, 4_000), 0);
    let (id, state) = submit(&mut r, job(1_000));
    assert_eq!(state, JobState::Ready);

    let outcome = r.engine.tick(VirtualTime::ZERO, &r.outbox);
    assert_eq!(outcome.dispatched, 1);
    match r.outbox.pop().expect("one dispatch") {
        Dispatch::Assign {
            slot,
            id: got,
            payload,
            walltime,
        } => {
            assert_eq!(slot, 0);
            assert_eq!(got, id);
            assert_eq!(&payload[..], b"payload");
            assert_eq!(walltime, VirtualDuration::from_secs(60));
        }
        Dispatch::Kill { .. } => panic!("expected Assign"),
    }
    assert_eq!(status(&mut r, id), Some(JobState::Running));
}

#[test]
fn completion_releases_capacity_and_a_second_job_runs() {
    let mut r = rig();
    join(&mut r, 1_000);
    let (a, _) = submit(&mut r, job(1_000));
    let (b, _) = submit(&mut r, job(1_000));
    r.engine.tick(VirtualTime::ZERO, &r.outbox);
    assert_eq!(
        status(&mut r, b),
        Some(JobState::Ready),
        "no room for b yet"
    );

    r.inbox.push(Command::Completed { id: a }).unwrap();
    r.engine.drain(&r.inbox, VirtualTime::ZERO, &r.outbox);
    assert_eq!(status(&mut r, a), Some(JobState::Completed));
    r.outbox.pop().expect("the tick dispatched one job");
    r.engine.tick(VirtualTime::ZERO, &r.outbox);
    assert_eq!(status(&mut r, b), Some(JobState::Running));
}

#[test]
fn a_worker_leaving_requeues_its_running_job_and_drains_the_slot() {
    let mut r = rig();
    join(&mut r, 4_000);
    let (id, _) = submit(&mut r, job(1_000));
    r.engine.tick(VirtualTime::ZERO, &r.outbox);
    assert_eq!(status(&mut r, id), Some(JobState::Running));

    r.inbox.push(Command::WorkerLeft { slot: 0 }).unwrap();
    r.engine.drain(&r.inbox, VirtualTime::ZERO, &r.outbox);
    assert_eq!(
        status(&mut r, id),
        Some(JobState::Ready),
        "requeued via Preempted"
    );

    let nodes = r.engine.nodes();
    assert_eq!(nodes.len(), 1);
    assert!(!nodes[0].attached);
    assert_eq!(nodes[0].capacity, Resources::ZERO);
    assert_eq!(nodes[0].allocated, Resources::ZERO);

    // Nothing to run it on: the next tick dispatches nothing.
    r.outbox.pop().expect("the tick dispatched one job");
    assert_eq!(r.engine.tick(VirtualTime::ZERO, &r.outbox).dispatched, 0);

    // A new worker reuses slot 0 and picks it up.
    assert_eq!(join(&mut r, 4_000), 0);
    assert_eq!(r.engine.tick(VirtualTime::ZERO, &r.outbox).dispatched, 1);
}

#[test]
fn cancelling_a_running_job_emits_a_kill() {
    let mut r = rig();
    join(&mut r, 4_000);
    let (id, _) = submit(&mut r, job(1_000));
    r.engine.tick(VirtualTime::ZERO, &r.outbox);
    r.outbox.pop().expect("the tick dispatched one job");

    let (tx, mut rx) = oneshot::channel();
    r.inbox.push(Command::Cancel { id, reply: tx }).unwrap();
    r.engine.drain(&r.inbox, VirtualTime::ZERO, &r.outbox);
    assert!(rx.try_recv().unwrap().unwrap().is_empty(), "no dependents");
    assert!(matches!(r.outbox.pop(), Some(Dispatch::Kill { slot: 0, id: got }) if got == id));
    assert_eq!(status(&mut r, id), Some(JobState::Cancelled));
    assert_eq!(r.engine.queue_summary().running, 0);
}

#[test]
fn a_full_inbox_rejects_the_push_and_hands_the_command_back() {
    let r = rig();
    for _ in 0..4 {
        r.inbox.push(Command::WorkerLeft { slot: 9 }).unwrap();
    }
    let back = r.inbox.push(Command::WorkerLeft { slot: 9 }).unwrap_err();
    assert!(matches!(back, Command::WorkerLeft { slot: 9 }));
    assert_eq!(r.inbox.len(), 4);
}

fn demand(r: &mut Rig) -> taskerd::Demand {
    let (tx, mut rx) = oneshot::channel();
    r.inbox
        .push(Command::Query(Query::Demand { reply: tx }))
        .unwrap();
    r.engine.drain(&r.inbox, VirtualTime::ZERO, &r.outbox);
    rx.try_recv().unwrap()
}

#[test]
fn demand_counts_ready_plus_running_in_whole_workers_and_draining_keeps_running_jobs() {
    let mut r = rig(); // worker_cpu_millis = 4_000 by default
    assert_eq!(join(&mut r, 4_000), 0);
    assert_eq!(join(&mut r, 4_000), 1);
    for _ in 0..3 {
        submit(&mut r, job(3_000));
    }
    r.engine.tick(VirtualTime::ZERO, &r.outbox);
    let d = demand(&mut r);
    // 3 × 3 000 = 9 000 millicores → ceil(9 000 / 4 000) = 3 workers wanted.
    assert_eq!(d.desired, 3, "{d:?}");
    assert_eq!(d.attached, 2);
    assert_eq!(d.cpu_ready + d.cpu_running, 9_000);

    // Slot 0 drains: its running job stays, nothing new lands there.
    assert_eq!(r.engine.nodes()[0].allocated.cpu_millis, 3_000);
    r.inbox.push(Command::WorkerDraining { slot: 0 }).unwrap();
    r.engine.drain(&r.inbox, VirtualTime::ZERO, &r.outbox);
    let n0 = r.engine.nodes()[0].clone();
    assert_eq!(n0.capacity, Resources::ZERO);
    assert_eq!(
        n0.allocated.cpu_millis, 3_000,
        "the running job is untouched"
    );
    assert!(n0.attached, "still attached while draining");

    // A new worker must not be handed the draining slot.
    assert_eq!(join(&mut r, 4_000), 2);
}
