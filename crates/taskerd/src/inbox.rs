//! The ingest ring: bounded, lock-free, many producers, one consumer.

use std::fmt;

use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;
use crossbeam_utils::sync::Unparker;

use crate::Command;

/// Bounded MPSC ring. `push` never blocks and never allocates; a full ring is
/// reported to the caller, who turns it into `RESOURCE_EXHAUSTED`.
pub struct Inbox {
    // `CachePadded` keeps the ring's head/tail off the cache line the scheduler's
    // own state lives on, so producers and consumer do not false-share.
    queue: CachePadded<ArrayQueue<Command>>,
    unparker: Unparker,
}

impl Inbox {
    /// A ring of `capacity` slots that wakes `unparker` on every push.
    #[must_use]
    pub fn new(capacity: usize, unparker: Unparker) -> Self {
        Self {
            queue: CachePadded::new(ArrayQueue::new(capacity)),
            unparker,
        }
    }

    /// Enqueues and wakes the scheduler. `Err` hands the command back when full.
    ///
    /// # Errors
    /// The command itself, when the ring is full.
    // Returning the command unboxed is deliberate: boxing would allocate on
    // the submit path, and the caller wants its value back, not a pointer.
    #[allow(clippy::result_large_err)]
    pub fn push(&self, command: Command) -> Result<(), Command> {
        self.queue.push(command)?;
        // `unpark` is cheap and idempotent: at most one token is stored.
        self.unparker.unpark();
        Ok(())
    }

    /// Dequeues the oldest command. Consumer side only.
    #[must_use]
    pub fn pop(&self) -> Option<Command> {
        self.queue.pop()
    }

    /// `push`, retried with a 1 ms pause up to `attempts` times. For messages
    /// that must not be lost to a transient submit storm (worker reports).
    /// Returns the command if every attempt found the ring full.
    ///
    /// # Errors
    /// The command itself, after `attempts` failures.
    #[allow(clippy::result_large_err)] // same reason as `push`
    pub async fn push_with_retry(
        &self,
        mut command: Command,
        attempts: u32,
    ) -> Result<(), Command> {
        for _ in 0..attempts {
            match self.push(command) {
                Ok(()) => return Ok(()),
                Err(back) => {
                    command = back;
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
        }
        Err(command)
    }

    /// Wakes the scheduler without enqueuing anything, e.g. to observe a stop flag.
    pub fn wake(&self) {
        self.unparker.unpark();
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }
}

impl fmt::Debug for Inbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inbox")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .finish()
    }
}
