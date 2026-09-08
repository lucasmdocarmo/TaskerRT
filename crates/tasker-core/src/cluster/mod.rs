//! Layer 2 — the capacity model. Depends on `domain` only.

pub mod slot;

pub use slot::{CapacityError, SlotIndex, SlotInventory, WorkerSlot};
