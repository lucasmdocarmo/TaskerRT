//! Scheduler thread → dispatcher task, without the scheduler ever awaiting.

use std::fmt;

use crossbeam_queue::ArrayQueue;
use tokio::sync::Notify;

use crate::Dispatch;

/// Bounded ring plus a wake-up. The scheduler pushes and notifies; the
/// dispatcher drains and then sleeps on `notified`.
pub struct Outbox {
    queue: ArrayQueue<Dispatch>,
    notify: Notify,
}

impl Outbox {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
            notify: Notify::new(),
        }
    }

    /// Enqueues and wakes the dispatcher.
    ///
    /// # Errors
    /// The dispatch itself, when the ring is full.
    pub fn push(&self, dispatch: Dispatch) -> Result<(), Dispatch> {
        self.queue.push(dispatch)?;
        // `notify_one` stores a permit if nobody is waiting, so a wake-up is never lost.
        self.notify.notify_one();
        Ok(())
    }

    #[must_use]
    pub fn pop(&self) -> Option<Dispatch> {
        self.queue.pop()
    }

    /// Resolves after the next `push` (or immediately if one already happened).
    pub async fn notified(&self) {
        self.notify.notified().await;
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl fmt::Debug for Outbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outbox").field("len", &self.len()).finish()
    }
}
