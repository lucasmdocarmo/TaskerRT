//! The write-ahead side of the engine. Records accumulate per tick; a writer
//! thread makes them durable and only then releases what waited on them.

use std::fmt;
use std::mem;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};

use tasker_core::{Job, JobId};
use tasker_wal::{Record, Store, SyncPolicy, WalError, encode_submitted};

use crate::{Dispatch, Outbox};

/// How many commits may wait for the writer. Shallow on purpose: while one
/// batch is being synced, later ticks merge into a single pending batch, so a
/// saturated disk makes batches larger instead of making the queue longer.
/// At depth 64 a 4 ms fsync put p50 submit latency at 64 × 4 ms.
pub const COMMIT_QUEUE_DEPTH: usize = 1;

/// Something to run once its records are durable: a reply, typically.
pub type Ack = Box<dyn FnOnce() + Send>;

/// One batch: framed records plus everything that must wait for them.
#[derive(Default)]
pub struct Commit {
    /// Frames recorded before the snapshot was taken; they belong to the log being retired.
    pub before: Vec<u8>,
    /// Frames recorded after the snapshot (or all frames when there is none).
    pub frames: Vec<u8>,
    pub acks: Vec<Ack>,
    pub dispatches: Vec<Dispatch>,
    /// An encoded snapshot; the store rotates onto it between `before` and `frames`.
    pub snapshot: Option<Vec<u8>>,
}

impl Commit {
    fn is_empty(&self) -> bool {
        self.before.is_empty()
            && self.frames.is_empty()
            && self.acks.is_empty()
            && self.dispatches.is_empty()
            && self.snapshot.is_none()
    }
}

// Closures are not `Debug`; describe the batch by its sizes.
impl fmt::Debug for Commit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Commit")
            .field("before", &self.before.len())
            .field("frames", &self.frames.len())
            .field("acks", &self.acks.len())
            .field("dispatches", &self.dispatches.len())
            .field("snapshot", &self.snapshot.is_some())
            .finish()
    }
}

/// The engine's end of the journal: builds one `Commit` per tick.
pub struct Journal {
    tx: SyncSender<Commit>,
    pending: Commit,
    rotate_bytes: u64,
    bytes_since_rotate: u64,
}

impl fmt::Debug for Journal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Journal")
            .field("pending", &self.pending)
            .field("bytes_since_rotate", &self.bytes_since_rotate)
            .finish_non_exhaustive()
    }
}

impl Journal {
    #[must_use]
    pub fn new(tx: SyncSender<Commit>, rotate_bytes: u64) -> Self {
        Self {
            tx,
            pending: Commit::default(),
            rotate_bytes,
            bytes_since_rotate: 0,
        }
    }

    fn grew(&mut self, before: usize) {
        self.bytes_since_rotate += (self.pending.frames.len() - before) as u64;
    }

    /// Frames a record into the pending batch.
    pub fn record(&mut self, record: &Record) {
        let before = self.pending.frames.len();
        record.encode(&mut self.pending.frames);
        self.grew(before);
    }

    /// Frames a `Submitted` record without cloning the job.
    pub fn record_submitted(&mut self, id: JobId, job: &Job) {
        let before = self.pending.frames.len();
        encode_submitted(&mut self.pending.frames, id, job);
        self.grew(before);
    }

    /// Runs `ack` once this batch is durable.
    pub fn defer(&mut self, ack: Ack) {
        self.pending.acks.push(ack);
    }

    /// Sends `dispatch` to the outbox once this batch is durable.
    pub fn hold(&mut self, dispatch: Dispatch) {
        self.pending.dispatches.push(dispatch);
    }

    /// True once the log has outgrown the rotation threshold and no snapshot is queued.
    #[must_use]
    pub fn wants_snapshot(&self) -> bool {
        self.pending.snapshot.is_none() && self.bytes_since_rotate >= self.rotate_bytes
    }

    /// Queues an encoded snapshot. Everything recorded so far predates it and
    /// moves to `before`; everything recorded afterwards goes to the new log.
    pub fn attach_snapshot(&mut self, bytes: Vec<u8>) {
        self.pending.before = mem::take(&mut self.pending.frames);
        self.pending.snapshot = Some(bytes);
        self.bytes_since_rotate = 0;
    }

    /// Hands the batch to the writer. Keeps it for next tick if the channel is full.
    pub fn commit(&mut self) -> bool {
        if self.pending.is_empty() {
            return true;
        }
        // `mem::take` leaves an empty `Commit` behind and moves the full one out.
        let batch = mem::take(&mut self.pending);
        match self.tx.try_send(batch) {
            Ok(()) => true,
            Err(TrySendError::Full(batch) | TrySendError::Disconnected(batch)) => {
                self.pending = batch;
                false
            }
        }
    }

    /// Blocking hand-off for shutdown, so the last acks are not lost.
    pub fn flush(&mut self) {
        if !self.pending.is_empty() {
            let batch = mem::take(&mut self.pending);
            self.tx.send(batch).ok();
        }
    }
}

/// Frames that predate the snapshot go to the retiring log, then the store
/// rotates, then the rest goes to the new log. Each half is synced before the
/// acks that depend on it can fire.
fn persist(store: &mut Store, policy: SyncPolicy, commit: &Commit) -> Result<(), WalError> {
    if !commit.before.is_empty() {
        store.append(&commit.before)?;
        store.sync(policy)?;
    }
    if let Some(snapshot) = &commit.snapshot {
        store.rotate(snapshot)?;
    }
    store.append(&commit.frames)?;
    store.sync(policy)?;
    Ok(())
}

/// The writer thread body: persist each batch, then release what waited on it.
/// Exits when the engine drops its `Journal`.
#[allow(clippy::needless_pass_by_value)] // the thread owns its handles for its whole life
pub fn run_writer(mut store: Store, policy: SyncPolicy, outbox: Arc<Outbox>, rx: Receiver<Commit>) {
    while let Ok(commit) = rx.recv() {
        if let Err(e) = persist(&mut store, policy, &commit) {
            // Nothing below may happen while the bytes are not on disk; clients time out and retry.
            tracing::error!(error = %e, "WAL write failed; dropping this batch's acks and dispatches");
            continue;
        }
        for ack in commit.acks {
            ack();
        }
        for mut dispatch in commit.dispatches {
            // The dispatcher drains continuously; a full ring clears within microseconds.
            while let Err(back) = outbox.push(dispatch) {
                dispatch = back;
                std::thread::yield_now();
            }
        }
    }
    tracing::info!(generation = store.generation(), "WAL writer stopping");
}
