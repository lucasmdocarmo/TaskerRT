//! Recovery without a network: an engine with a journal and a real writer
//! thread, then a second engine rebuilt from the directory the first wrote.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tasker_core::{
    AccountId, Job, JobId, JobState, PriorityClass, ResourceRequest, Resources, VirtualDuration,
    VirtualTime,
};
use tasker_wal::{Store, SyncPolicy};
use taskerd::{
    COMMIT_QUEUE_DEPTH, Command, DaemonConfig, Dispatch, Engine, Journal, Metrics, Outbox, Query,
    journal,
};
use tokio::sync::oneshot;

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("taskerrt-d-{}-{name}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(dir: &Path) -> DaemonConfig {
    DaemonConfig {
        data_dir: Some(dir.to_path_buf()),
        wal_sync: SyncPolicy::None,
        retain: Duration::from_hours(1),
        ..DaemonConfig::default()
    }
}

fn job(deps: Vec<JobId>) -> Job {
    let mut j = Job::new(
        AccountId::new(0),
        PriorityClass::Normal,
        VirtualTime::ZERO,
        ResourceRequest::new(1_000, 0, 0),
        VirtualDuration::from_secs(60),
    );
    j.deps.extend(deps);
    j.payload = Bytes::from_static(b"p");
    j
}

fn at(secs: u64) -> VirtualTime {
    VirtualTime::from_nanos(secs * 1_000_000_000)
}

/// An engine whose commits flow through a real writer thread into `dir`.
struct Rig {
    engine: Engine,
    outbox: Arc<Outbox>,
    writer: std::thread::JoinHandle<()>,
}

impl Rig {
    fn open(dir: &Path) -> Self {
        let cfg = config(dir);
        let recovered = Store::recover(dir).unwrap();
        let store = Store::resume(dir, recovered.generation).unwrap();
        let (tx, rx) = std::sync::mpsc::sync_channel(COMMIT_QUEUE_DEPTH);
        let journal = Journal::new(tx, cfg.wal_rotate_bytes);
        let engine = Engine::recover(&cfg, Metrics::new(), recovered, journal).unwrap();
        let outbox = Arc::new(Outbox::new(64));
        let writer = {
            let outbox = Arc::clone(&outbox);
            std::thread::spawn(move || journal::run_writer(store, SyncPolicy::None, outbox, rx))
        };
        Self {
            engine,
            outbox,
            writer,
        }
    }

    /// Submits, commits the tick, and waits for the durable acknowledgement.
    fn submit(&mut self, job: Job, now: VirtualTime) -> JobId {
        let (tx, rx) = oneshot::channel();
        self.engine
            .handle(Command::Submit { job, reply: tx }, now, &self.outbox);
        self.engine.tick(now, &self.outbox);
        rx.blocking_recv().unwrap().unwrap().0
    }

    fn state(&mut self, id: JobId) -> Option<JobState> {
        let (tx, rx) = oneshot::channel();
        self.engine.handle(
            Command::Query(Query::Status { id, reply: tx }),
            VirtualTime::ZERO,
            &self.outbox,
        );
        rx.blocking_recv().unwrap()
    }

    /// Attaches a 4-core worker that claims `in_flight`; returns its slot.
    fn worker(&mut self, in_flight: Vec<JobId>) -> u32 {
        let (tx, rx) = oneshot::channel();
        self.engine.handle(
            Command::WorkerJoined {
                name: "w".into(),
                capacity: Resources::new(4_000, 0, 0),
                in_flight,
                reply: tx,
            },
            VirtualTime::ZERO,
            &self.outbox,
        );
        rx.blocking_recv().unwrap()
    }

    /// Runs a cycle, then collects the assignments the writer released.
    fn dispatch(&mut self, now: VirtualTime) -> Vec<JobId> {
        self.engine.tick(now, &self.outbox);
        let mut ids = Vec::new();
        // The writer pushes after its sync; yield until it has.
        for _ in 0..1_000_000 {
            while let Some(d) = self.outbox.pop() {
                if let Dispatch::Assign { id, .. } = d {
                    ids.push(id);
                }
            }
            if !ids.is_empty() {
                break;
            }
            std::thread::yield_now();
        }
        ids
    }

    /// Dropping the engine drops the journal's sender; the writer then exits.
    fn close(self) {
        drop(self.engine);
        self.writer.join().unwrap();
    }
}

#[test]
#[allow(clippy::many_single_char_names)] // the scenario reads best as jobs a..f
fn a_recovered_engine_reproduces_ids_states_and_ledgers() {
    let dir = scratch("recovery");
    let mut rig = Rig::open(&dir);
    let _slot = rig.worker(vec![]);
    let a = rig.submit(job(vec![]), at(1));
    let b = rig.submit(job(vec![a]), at(2));
    let c = rig.submit(job(vec![]), at(3));
    let d = rig.submit(job(vec![c]), at(4));
    let e = rig.submit(job(vec![]), at(5));
    // a, c, e run on the 4-core worker. Complete a (promotes b), fail c (cancels d), cancel e.
    assert_eq!(rig.dispatch(at(6)).len(), 3);
    rig.engine
        .handle(Command::Completed { id: a }, at(10), &rig.outbox);
    rig.engine
        .handle(Command::Failed { id: c }, at(11), &rig.outbox);
    let (tx, rx) = oneshot::channel();
    rig.engine
        .handle(Command::Cancel { id: e, reply: tx }, at(12), &rig.outbox);
    rig.engine.tick(at(12), &rig.outbox);
    rx.blocking_recv().unwrap().unwrap();
    // b is Ready now; dispatch it so something is Running at the "crash".
    assert_eq!(rig.dispatch(at(13)), vec![b]);
    let before: Vec<_> = [a, b, c, d, e].iter().map(|id| rig.state(*id)).collect();
    assert_eq!(
        before,
        vec![
            Some(JobState::Completed),
            Some(JobState::Running),
            Some(JobState::Failed),
            Some(JobState::Cancelled),
            Some(JobState::Cancelled),
        ]
    );
    let usage_before = rig.engine.accounts()[0].usage;
    rig.close();

    let mut again = Rig::open(&dir);
    // Read before anything ticks, so both ledgers stand at their last event.
    // Work after the last record is not charged (b's final second), and decay
    // may differ by a step, so the bound is one core-second plus 0.2 percent.
    let ledger = again.engine.accounts()[0];
    // b is held as Running, not requeued, so its cpu is still charged.
    assert_eq!(ledger.running_cpu, 1_000);
    assert!(
        ledger.usage <= usage_before,
        "{} > {usage_before}",
        ledger.usage
    );
    assert!(
        usage_before - ledger.usage <= 1_000 * 1_000 + usage_before / 500,
        "recovered {} vs live {usage_before}",
        ledger.usage
    );
    let after: Vec<_> = [a, b, c, d, e].iter().map(|id| again.state(*id)).collect();
    // Running work is held for its worker, not requeued.
    assert_eq!(
        after,
        vec![
            Some(JobState::Completed),
            Some(JobState::Running),
            Some(JobState::Failed),
            Some(JobState::Cancelled),
            Some(JobState::Cancelled),
        ]
    );
    // The worker comes back still running b: the job binds to its new slot.
    let slot = again.worker(vec![b]);
    let node = again
        .engine
        .nodes()
        .into_iter()
        .find(|n| n.slot == slot)
        .unwrap();
    assert_eq!(
        node.allocated.cpu_millis, 1_000,
        "b holds capacity on the new slot"
    );
    again
        .engine
        .handle(Command::Completed { id: b }, at(15), &again.outbox);
    assert_eq!(again.state(b), Some(JobState::Completed));
    // Ids continue exactly where the first engine would have continued.
    let f = again.submit(job(vec![]), at(20));
    assert_eq!(f.index(), 5);
    again.close();

    // Third start: recovers from the snapshot the second start wrote, plus f's record.
    let mut third = Rig::open(&dir);
    let states: Vec<_> = [a, b, c, d, e, f]
        .iter()
        .map(|id| third.state(*id))
        .collect();
    assert_eq!(
        states,
        vec![
            Some(JobState::Completed),
            Some(JobState::Completed),
            Some(JobState::Failed),
            Some(JobState::Cancelled),
            Some(JobState::Cancelled),
            // f was dispatched to the worker attached in the second start: held again.
            Some(JobState::Running),
        ]
    );
    assert!(
        dir.join("snapshot-000002.bin").exists(),
        "every start compacts"
    );
    third.close();
}

#[test]
fn unclaimed_running_work_is_requeued_when_the_window_closes() {
    let dir = scratch("unclaimed");
    let mut rig = Rig::open(&dir);
    let _slot = rig.worker(vec![]);
    let x = rig.submit(job(vec![]), at(1));
    assert_eq!(rig.dispatch(at(2)), vec![x]);
    rig.close();

    let mut again = Rig::open(&dir);
    assert_eq!(again.state(x), Some(JobState::Running));
    // The first tick arms the window: twice the default 10 s heartbeat timeout.
    again.engine.tick(at(5), &again.outbox);
    assert_eq!(again.state(x), Some(JobState::Running));
    again.engine.tick(at(30), &again.outbox);
    assert_eq!(
        again.state(x),
        Some(JobState::Ready),
        "unclaimed after the window"
    );

    // A worker turning up late with the job still in hand is told to stop it.
    let _late = again.worker(vec![x]);
    let mut killed = None;
    while let Some(d) = again.outbox.pop() {
        if let Dispatch::Kill { id, .. } = d {
            killed = Some(id);
        }
    }
    assert_eq!(killed, Some(x));
    again.close();
}
