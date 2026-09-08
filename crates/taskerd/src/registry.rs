//! Slot → outbound stream sender, on the tokio side.

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

use tasker_core::SlotIndex;
use tasker_proto::v1;
use tokio::sync::mpsc;
use tonic::Status;

/// The sending half of one worker's `Attach` response stream.
pub type Outbound = mpsc::Sender<Result<v1::DaemonMessage, Status>>;

/// Attached workers by slot.
#[derive(Default)]
pub struct Registry {
    // A std Mutex: the critical section is a HashMap lookup, never an await.
    inner: Mutex<HashMap<SlotIndex, Outbound>>,
}

impl Registry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, slot: SlotIndex, tx: Outbound) {
        self.inner
            .lock()
            .expect("registry poisoned")
            .insert(slot, tx);
    }

    pub fn remove(&self, slot: SlotIndex) {
        self.inner.lock().expect("registry poisoned").remove(&slot);
    }

    /// Drops every sender. Each worker's response stream ends, which is how
    /// a graceful shutdown persuades long-lived `Attach` calls to finish.
    pub fn clear(&self) {
        self.inner.lock().expect("registry poisoned").clear();
    }

    /// A clone of the sender, so the send happens outside the lock.
    #[must_use]
    pub fn sender(&self, slot: SlotIndex) -> Option<Outbound> {
        self.inner
            .lock()
            .expect("registry poisoned")
            .get(&slot)
            .cloned()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().expect("registry poisoned").len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry")
            .field("attached", &self.len())
            .finish()
    }
}
