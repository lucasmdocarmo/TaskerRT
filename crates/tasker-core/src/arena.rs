//! A generational arena: `Vec`-backed slot storage with ABA-safe handles.
//!
//! A handle carries the generation of the slot it was issued for. Reusing a
//! slot bumps that generation, so a handle from a previous occupant resolves to
//! `None` rather than silently addressing the new one.

/// A handle to a slot in an [`Arena`].
///
/// Packs a 32-bit slot index in the low bits and a 32-bit generation counter in
/// the high bits of a `u64`. The representation is stable and is what the M6
/// write-ahead log will serialize.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct JobId(u64);

impl JobId {
    const INDEX_BITS: u32 = 32;
    const INDEX_MASK: u64 = (1 << Self::INDEX_BITS) - 1;

    /// Builds a handle from its parts. Internal: only an [`Arena`] issues handles.
    #[must_use]
    pub(crate) const fn new(index: u32, generation: u32) -> Self {
        Self(((generation as u64) << Self::INDEX_BITS) | index as u64)
    }

    /// The slot index this handle refers to.
    #[must_use]
    // Truncation is the intent: the low 32 bits are the index by construction.
    #[allow(clippy::cast_possible_truncation)]
    pub const fn index(self) -> u32 {
        (self.0 & Self::INDEX_MASK) as u32
    }

    /// The generation this handle was issued at.
    #[must_use]
    // Truncation is the intent: the high 32 bits are the generation by construction.
    #[allow(clippy::cast_possible_truncation)]
    pub const fn generation(self) -> u32 {
        (self.0 >> Self::INDEX_BITS) as u32
    }

    /// The packed representation, for serialization.
    #[must_use]
    pub const fn to_bits(self) -> u64 {
        self.0
    }

    /// Rebuilds a handle from [`JobId::to_bits`].
    #[must_use]
    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }
}

#[derive(Debug)]
enum Slot<T> {
    Occupied {
        generation: u32,
        value: T,
    },
    Vacant {
        generation: u32,
        next_free: Option<u32>,
    },
}

/// Slot storage with generational handles.
///
/// Insertion reuses vacated slots through an intrusive free list, so handles
/// stay dense and lookups stay cache-friendly.
#[derive(Debug)]
pub struct Arena<T> {
    slots: Vec<Slot<T>>,
    free_head: Option<u32>,
    len: usize,
}

impl<T> Default for Arena<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Arena<T> {
    /// An empty arena that has not allocated.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            free_head: None,
            len: 0,
        }
    }

    /// An empty arena with room for `capacity` slots, allocated once.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            free_head: None,
            len: 0,
        }
    }

    /// Number of live entries.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// True when no entries are live.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Total slots ever allocated, live or vacant. Sizes side tables keyed by
    /// [`JobId::index`].
    #[must_use]
    pub fn capacity_slots(&self) -> usize {
        self.slots.len()
    }

    /// Stores `value` and returns its handle.
    ///
    /// # Panics
    /// If the arena would exceed `u32::MAX` slots.
    pub fn insert(&mut self, value: T) -> JobId {
        self.len += 1;
        if let Some(index) = self.free_head {
            let slot = &mut self.slots[index as usize];
            let Slot::Vacant {
                generation,
                next_free,
            } = *slot
            else {
                unreachable!("free list pointed at an occupied slot");
            };
            self.free_head = next_free;
            *slot = Slot::Occupied { generation, value };
            JobId::new(index, generation)
        } else {
            let index = u32::try_from(self.slots.len()).expect("arena exceeded u32::MAX slots");
            self.slots.push(Slot::Occupied {
                generation: 0,
                value,
            });
            JobId::new(index, 0)
        }
    }

    /// Borrows the value `id` refers to, or `None` if `id` is stale.
    #[must_use]
    pub fn get(&self, id: JobId) -> Option<&T> {
        match self.slots.get(id.index() as usize)? {
            Slot::Occupied { generation, value } if *generation == id.generation() => Some(value),
            _ => None,
        }
    }

    /// Mutably borrows the value `id` refers to, or `None` if `id` is stale.
    #[must_use]
    pub fn get_mut(&mut self, id: JobId) -> Option<&mut T> {
        match self.slots.get_mut(id.index() as usize)? {
            Slot::Occupied { generation, value } if *generation == id.generation() => Some(value),
            _ => None,
        }
    }

    /// True when `id` is live.
    #[must_use]
    pub fn contains(&self, id: JobId) -> bool {
        self.get(id).is_some()
    }

    /// Removes and returns the value `id` refers to, bumping the slot's
    /// generation so every outstanding handle to it goes stale.
    ///
    /// Generation wraps at `u32::MAX`; ABA safety therefore holds for the first
    /// 2^32 reuses of any single slot, which no realistic workload reaches.
    pub fn remove(&mut self, id: JobId) -> Option<T> {
        let index = id.index();
        let free_head = self.free_head;
        let slot = self.slots.get_mut(index as usize)?;
        let Slot::Occupied { generation, .. } = slot else {
            return None;
        };
        if *generation != id.generation() {
            return None;
        }
        let next_generation = generation.wrapping_add(1);
        let previous = std::mem::replace(
            slot,
            Slot::Vacant {
                generation: next_generation,
                next_free: free_head,
            },
        );
        self.free_head = Some(index);
        self.len -= 1;
        match previous {
            Slot::Occupied { value, .. } => Some(value),
            Slot::Vacant { .. } => unreachable!("checked occupied above"),
        }
    }

    /// Iterates live entries in slot order.
    pub fn iter(&self) -> impl Iterator<Item = (JobId, &T)> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| match slot {
                Slot::Occupied { generation, value } => {
                    let index = u32::try_from(index).expect("slot count fits u32");
                    Some((JobId::new(index, *generation), value))
                }
                Slot::Vacant { .. } => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_then_get_returns_value() {
        let mut arena = Arena::new();
        let id = arena.insert(42_u32);
        assert_eq!(arena.get(id), Some(&42));
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn remove_invalidates_the_handle() {
        let mut arena = Arena::new();
        let id = arena.insert(42_u32);
        assert_eq!(arena.remove(id), Some(42));
        assert_eq!(arena.get(id), None);
        assert!(!arena.contains(id));
        assert_eq!(arena.len(), 0);
    }

    #[test]
    fn reused_slot_yields_a_different_id() {
        let mut arena = Arena::new();
        let first = arena.insert(1_u32);
        arena.remove(first);
        let second = arena.insert(2_u32);

        assert_eq!(first.index(), second.index(), "slot should be reused");
        assert_ne!(first, second, "generation must differ");
        assert_eq!(first.generation() + 1, second.generation());
        assert_eq!(arena.get(first), None, "stale handle must not resolve");
        assert_eq!(arena.get(second), Some(&2));
    }

    #[test]
    fn double_remove_is_none() {
        let mut arena = Arena::new();
        let id = arena.insert(7_u32);
        assert_eq!(arena.remove(id), Some(7));
        assert_eq!(arena.remove(id), None);
    }

    #[test]
    fn id_bit_packing_roundtrips() {
        let id = JobId::from_bits(JobId::new(12_345, 678).to_bits());
        assert_eq!(id.index(), 12_345);
        assert_eq!(id.generation(), 678);
    }

    #[test]
    fn iter_yields_only_live_entries() {
        let mut arena = Arena::new();
        let a = arena.insert(1_u32);
        let b = arena.insert(2_u32);
        arena.remove(a);
        let live: Vec<_> = arena.iter().map(|(id, v)| (id, *v)).collect();
        assert_eq!(live, vec![(b, 2)]);
    }
}
