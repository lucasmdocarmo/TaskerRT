//! Virtual cluster capacity (spec §5.3).
//!
//! Slot count and per-slot capacity are configuration, deliberately decoupled
//! from the real cluster. That is what lets the benchmark suite simulate 64
//! nodes on one laptop.

use crate::{Capacity, ResourceRequest, Resources};

/// Dense index into a [`SlotInventory`].
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
    #[must_use]
    pub const fn new(capacity: Capacity) -> Self {
        Self {
            capacity,
            allocated: Resources::ZERO,
        }
    }

    /// Unallocated capacity. Saturating subtraction keeps this total even if an
    /// invariant were ever violated; the property test in
    /// `tests/prop_capacity.rs` is what proves it never is.
    #[must_use]
    pub const fn free(&self) -> Resources {
        self.capacity.saturating_sub(&self.allocated)
    }
}

/// The scheduler's model of cluster capacity.
#[derive(Clone, Debug)]
pub struct SlotInventory {
    slots: Vec<WorkerSlot>,
    /// Componentwise maximum of every slot's capacity, cached so
    /// [`SlotInventory::can_ever_fit`] is O(1) on the eligibility path.
    /// Note this is a per-dimension maximum, so it over-approximates: a request
    /// passing this check may still fit no single slot, which
    /// [`SlotInventory::can_ever_fit`] then rules out exactly.
    max_capacity: Capacity,
}

impl SlotInventory {
    /// `count` identical slots.
    #[must_use]
    pub fn from_uniform(count: u32, capacity: Capacity) -> Self {
        Self {
            slots: vec![WorkerSlot::new(capacity); count as usize],
            max_capacity: capacity,
        }
    }

    /// One slot per entry, capacities as given.
    #[must_use]
    pub fn from_capacities(capacities: &[Capacity]) -> Self {
        let max_capacity = capacities.iter().fold(Resources::ZERO, |acc, c| Resources {
            cpu_millis: acc.cpu_millis.max(c.cpu_millis),
            mem_bytes: acc.mem_bytes.max(c.mem_bytes),
            gpus: acc.gpus.max(c.gpus),
        });
        Self {
            slots: capacities.iter().copied().map(WorkerSlot::new).collect(),
            max_capacity,
        }
    }

    #[must_use]
    pub fn len(&self) -> SlotIndex {
        SlotIndex::try_from(self.slots.len()).expect("slot count fits u32")
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    #[must_use]
    pub fn slot(&self, slot: SlotIndex) -> Option<&WorkerSlot> {
        self.slots.get(slot as usize)
    }

    /// Unallocated capacity of one slot.
    #[must_use]
    pub fn free(&self, slot: SlotIndex) -> Option<Resources> {
        self.slots.get(slot as usize).map(WorkerSlot::free)
    }

    /// Lowest-indexed slot with room for `request` right now.
    ///
    /// First-fit rather than best-fit: it is O(slots) with no scoring pass, and
    /// M8 can revisit the policy against a measured baseline.
    #[must_use]
    pub fn first_fit(&self, request: &ResourceRequest) -> Option<SlotIndex> {
        self.slots
            .iter()
            .position(|s| request.fits_within(&s.free()))
            .map(|i| SlotIndex::try_from(i).expect("slot count fits u32"))
    }

    /// Charges `request` against `slot`.
    ///
    /// # Errors
    /// [`CapacityError::NoSuchSlot`] if `slot` is out of range;
    /// [`CapacityError::WouldExceed`] if the allocation would not fit. On error
    /// the inventory is unchanged.
    pub fn try_allocate(
        &mut self,
        slot: SlotIndex,
        request: &ResourceRequest,
    ) -> Result<(), CapacityError> {
        let entry = self
            .slots
            .get_mut(slot as usize)
            .ok_or(CapacityError::NoSuchSlot { slot })?;
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
    /// [`CapacityError::NoSuchSlot`] if `slot` is out of range.
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

    /// True when some slot's declared capacity could ever hold `request`,
    /// regardless of current allocation. A request failing this can never run
    /// and is rejected at eligibility (spec §6 step 2).
    #[must_use]
    pub fn can_ever_fit(&self, request: &ResourceRequest) -> bool {
        if !request.fits_within(&self.max_capacity) {
            return false;
        }
        self.slots.iter().any(|s| request.fits_within(&s.capacity))
    }

    /// Componentwise sum of free capacity across all slots.
    #[must_use]
    pub fn total_free(&self) -> Resources {
        self.slots
            .iter()
            .fold(Resources::ZERO, |acc, s| acc.saturating_add(&s.free()))
    }

    /// Drops every allocation, keeping the slot inventory. Used by the
    /// benchmark harness between iterations.
    pub fn reset(&mut self) {
        for slot in &mut self.slots {
            slot.allocated = Resources::ZERO;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Resources;

    fn inventory() -> SlotInventory {
        SlotInventory::from_uniform(2, Resources::new(4_000, 8 << 30, 1))
    }

    #[test]
    fn a_fresh_slot_is_entirely_free() {
        let inv = inventory();
        assert_eq!(inv.len(), 2);
        assert_eq!(inv.free(0), Some(Resources::new(4_000, 8 << 30, 1)));
    }

    #[test]
    fn allocation_reduces_free_capacity() {
        let mut inv = inventory();
        inv.try_allocate(0, &Resources::new(1_000, 1 << 30, 0))
            .unwrap();
        assert_eq!(inv.free(0), Some(Resources::new(3_000, 7 << 30, 1)));
        assert_eq!(
            inv.free(1),
            Some(Resources::new(4_000, 8 << 30, 1)),
            "other slot untouched"
        );
    }

    #[test]
    fn allocation_beyond_capacity_is_rejected_and_leaves_state_unchanged() {
        let mut inv = inventory();
        let before = inv.free(0);
        let err = inv
            .try_allocate(0, &Resources::new(9_000, 0, 0))
            .unwrap_err();
        assert_eq!(err, CapacityError::WouldExceed { slot: 0 });
        assert_eq!(inv.free(0), before, "a rejected allocation must not mutate");
    }

    #[test]
    fn unknown_slot_is_an_error() {
        let mut inv = inventory();
        assert_eq!(
            inv.try_allocate(9, &Resources::ZERO).unwrap_err(),
            CapacityError::NoSuchSlot { slot: 9 }
        );
    }

    #[test]
    fn release_returns_capacity() {
        let mut inv = inventory();
        let req = Resources::new(1_000, 1 << 30, 1);
        inv.try_allocate(0, &req).unwrap();
        inv.release(0, &req).unwrap();
        assert_eq!(inv.free(0), Some(Resources::new(4_000, 8 << 30, 1)));
    }

    #[test]
    fn first_fit_skips_full_slots() {
        let mut inv = inventory();
        inv.try_allocate(0, &Resources::new(4_000, 0, 0)).unwrap();
        assert_eq!(inv.first_fit(&Resources::new(2_000, 0, 0)), Some(1));
    }

    #[test]
    fn can_ever_fit_uses_declared_capacity_not_free() {
        let mut inv = inventory();
        inv.try_allocate(0, &Resources::new(4_000, 0, 0)).unwrap();
        inv.try_allocate(1, &Resources::new(4_000, 0, 0)).unwrap();
        assert!(
            inv.can_ever_fit(&Resources::new(4_000, 0, 0)),
            "fits an empty slot"
        );
        assert!(
            !inv.can_ever_fit(&Resources::new(4_001, 0, 0)),
            "fits no slot ever"
        );
        assert_eq!(
            inv.first_fit(&Resources::new(4_000, 0, 0)),
            None,
            "but not right now"
        );
    }

    #[test]
    fn heterogeneous_inventory_reports_per_slot_capacity() {
        let inv = SlotInventory::from_capacities(&[
            Resources::new(1_000, 0, 0),
            Resources::new(8_000, 0, 2),
        ]);
        assert!(inv.can_ever_fit(&Resources::new(8_000, 0, 2)));
        assert!(!inv.can_ever_fit(&Resources::new(9_000, 0, 0)));
    }

    #[test]
    fn total_free_sums_every_slot() {
        let inv = inventory();
        assert_eq!(inv.total_free(), Resources::new(8_000, 16 << 30, 2));
    }
}
