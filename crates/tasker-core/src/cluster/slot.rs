//! Virtual cluster capacity. Slot count and size are configuration, not
//! discovered from a real cluster, so benchmarks can simulate any fleet.

use crate::domain::{Capacity, ResourceRequest, Resources};

/// Dense index into a `SlotInventory`.
pub type SlotIndex = u32;

/// A capacity operation could not be applied.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum CapacityError {
    #[error("no slot with index {slot}")]
    NoSuchSlot { slot: SlotIndex },
    #[error("allocation would exceed the capacity of slot {slot}")]
    WouldExceed { slot: SlotIndex },
}

/// One worker's declared capacity and current allocation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct WorkerSlot {
    pub capacity: Capacity,
    pub allocated: Resources,
}

impl WorkerSlot {
    /// An empty slot of the given capacity.
    #[must_use]
    pub const fn new(capacity: Capacity) -> Self {
        Self {
            capacity,
            allocated: Resources::ZERO,
        }
    }

    /// Unallocated capacity.
    #[must_use]
    pub const fn free(&self) -> Resources {
        // Saturating so the result is always well-defined even if an invariant slipped.
        self.capacity.saturating_sub(&self.allocated)
    }
}

/// The scheduler's model of cluster capacity.
#[derive(Clone, Debug)]
pub struct SlotInventory {
    slots: Vec<WorkerSlot>,
    /// Per-dimension maximum over all slots; a fast pre-check for `can_ever_fit`.
    max_capacity: Capacity,
}

impl SlotInventory {
    /// `count` identical slots.
    #[must_use]
    pub fn from_uniform(count: u32, capacity: Capacity) -> Self {
        Self {
            // `vec![x; n]` clones `x` n times; `WorkerSlot` is `Copy`, so that is a memcpy.
            slots: vec![WorkerSlot::new(capacity); count as usize],
            max_capacity: capacity,
        }
    }

    /// One slot per entry, capacities as given.
    #[must_use]
    pub fn from_capacities(capacities: &[Capacity]) -> Self {
        // `fold` threads an accumulator through the iterator, here a running max.
        let max_capacity = capacities.iter().fold(Resources::ZERO, |acc, c| Resources {
            cpu_millis: acc.cpu_millis.max(c.cpu_millis),
            mem_bytes: acc.mem_bytes.max(c.mem_bytes),
            gpus: acc.gpus.max(c.gpus),
        });
        Self {
            // `copied()` turns `&Capacity` into `Capacity`; `map` wraps each in a slot.
            slots: capacities.iter().copied().map(WorkerSlot::new).collect(),
            max_capacity,
        }
    }

    /// Number of slots.
    #[must_use]
    pub fn len(&self) -> SlotIndex {
        SlotIndex::try_from(self.slots.len()).expect("slot count fits u32")
    }

    /// True when there are no slots.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Borrows one slot, or `None` if out of range.
    #[must_use]
    pub fn slot(&self, slot: SlotIndex) -> Option<&WorkerSlot> {
        self.slots.get(slot as usize)
    }

    /// Unallocated capacity of one slot, or `None` if out of range.
    #[must_use]
    pub fn free(&self, slot: SlotIndex) -> Option<Resources> {
        // `map` applies `free` inside the `Option` without unwrapping it.
        self.slots.get(slot as usize).map(WorkerSlot::free)
    }

    /// Lowest-indexed slot with room for `request` right now (first-fit).
    #[must_use]
    pub fn first_fit(&self, request: &ResourceRequest) -> Option<SlotIndex> {
        // `position` returns the index of the first element the closure accepts.
        self.slots
            .iter()
            .position(|s| request.fits_within(&s.free()))
            .map(|i| SlotIndex::try_from(i).expect("slot count fits u32"))
    }

    /// Charges `request` against `slot`. On error nothing changes.
    ///
    /// # Errors
    /// `NoSuchSlot` if out of range; `WouldExceed` if it does not fit.
    pub fn try_allocate(
        &mut self,
        slot: SlotIndex,
        request: &ResourceRequest,
    ) -> Result<(), CapacityError> {
        // `ok_or` converts `Option` to `Result`; `?` returns the error early.
        let entry = self
            .slots
            .get_mut(slot as usize)
            .ok_or(CapacityError::NoSuchSlot { slot })?;
        // Overflow and over-capacity are different failures; check both.
        let next = entry
            .allocated
            .checked_add(request)
            .ok_or(CapacityError::WouldExceed { slot })?;
        if !next.fits_within(&entry.capacity) {
            return Err(CapacityError::WouldExceed { slot });
        }
        entry.allocated = next;
        Ok(())
    }

    /// Returns `request` to `slot`.
    ///
    /// # Errors
    /// `NoSuchSlot` if out of range.
    pub fn release(
        &mut self,
        slot: SlotIndex,
        request: &ResourceRequest,
    ) -> Result<(), CapacityError> {
        let entry = self
            .slots
            .get_mut(slot as usize)
            .ok_or(CapacityError::NoSuchSlot { slot })?;
        entry.allocated = entry.allocated.saturating_sub(request);
        Ok(())
    }

    /// True when some slot's declared capacity could hold `request` if empty.
    #[must_use]
    pub fn can_ever_fit(&self, request: &ResourceRequest) -> bool {
        // Cheap per-dimension check first; it can only rule out, never rule in.
        if !request.fits_within(&self.max_capacity) {
            return false;
        }
        // `any` stops at the first slot that satisfies the closure.
        self.slots.iter().any(|s| request.fits_within(&s.capacity))
    }

    /// Componentwise sum of free capacity across all slots.
    #[must_use]
    pub fn total_free(&self) -> Resources {
        self.slots
            .iter()
            .fold(Resources::ZERO, |acc, s| acc.saturating_add(&s.free()))
    }

    /// Adds a slot of `capacity`, reusing the lowest zero-capacity slot if one
    /// exists so indices stay stable across worker churn.
    pub fn add_slot(&mut self, capacity: Capacity) -> SlotIndex {
        let index = if let Some(i) = self
            .slots
            .iter()
            .position(|s| s.capacity == Resources::ZERO)
        {
            self.slots[i] = WorkerSlot::new(capacity);
            i
        } else {
            self.slots.push(WorkerSlot::new(capacity));
            self.slots.len() - 1
        };
        self.max_capacity = Resources {
            cpu_millis: self.max_capacity.cpu_millis.max(capacity.cpu_millis),
            mem_bytes: self.max_capacity.mem_bytes.max(capacity.mem_bytes),
            gpus: self.max_capacity.gpus.max(capacity.gpus),
        };
        SlotIndex::try_from(index).expect("slot count fits u32")
    }

    /// Appends a slot, never reusing one. The engine decides reuse itself
    /// because it knows which zeroed slots still have a draining worker attached.
    pub fn push_slot(&mut self, capacity: Capacity) -> SlotIndex {
        self.slots.push(WorkerSlot::new(capacity));
        self.max_capacity = Resources {
            cpu_millis: self.max_capacity.cpu_millis.max(capacity.cpu_millis),
            mem_bytes: self.max_capacity.mem_bytes.max(capacity.mem_bytes),
            gpus: self.max_capacity.gpus.max(capacity.gpus),
        };
        SlotIndex::try_from(self.slots.len() - 1).expect("slot count fits u32")
    }

    /// Replaces a slot's declared capacity. Zero capacity drains it: nothing
    /// new fits, and `add_slot` may reuse it.
    ///
    /// # Errors
    /// `NoSuchSlot`.
    pub fn set_capacity(
        &mut self,
        slot: SlotIndex,
        capacity: Capacity,
    ) -> Result<(), CapacityError> {
        let entry = self
            .slots
            .get_mut(slot as usize)
            .ok_or(CapacityError::NoSuchSlot { slot })?;
        entry.capacity = capacity;
        // `max_capacity` only over-approximates, so it need not shrink here.
        Ok(())
    }

    /// Forgets every allocation on `slot`, keeping its capacity.
    ///
    /// # Errors
    /// `NoSuchSlot`.
    pub fn clear_allocation(&mut self, slot: SlotIndex) -> Result<(), CapacityError> {
        let entry = self
            .slots
            .get_mut(slot as usize)
            .ok_or(CapacityError::NoSuchSlot { slot })?;
        entry.allocated = Resources::ZERO;
        Ok(())
    }

    /// Drops every allocation, keeping the slots.
    pub fn reset(&mut self) {
        // `&mut self.slots` iterates mutable references so each slot can be written.
        for slot in &mut self.slots {
            slot.allocated = Resources::ZERO;
        }
    }
}
