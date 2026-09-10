//! Generational arena: slot storage whose handles go stale when a slot is reused.

/// Handle to an arena slot: 32-bit index low, 32-bit generation high.
/// Stale after the slot is reused; `Arena::get` then returns `None`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct JobId(u64);

impl JobId {
    const INDEX_BITS: u32 = 32;
    const INDEX_MASK: u64 = (1 << Self::INDEX_BITS) - 1;

    /// Packs an index and generation. Crate-private: only `Arena` issues handles.
    #[must_use]
    pub(crate) const fn new(index: u32, generation: u32) -> Self {
        // `as u64` widens; `<<` moves the generation into the high word; `|` merges.
        Self(((generation as u64) << Self::INDEX_BITS) | index as u64)
    }

    /// The slot index.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // masked to 32 bits first
    pub const fn index(self) -> u32 {
        // `&` keeps only the low 32 bits, so the `as u32` cannot lose data.
        (self.0 & Self::INDEX_MASK) as u32
    }

    /// The generation this handle was issued at.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // only the high word remains
    pub const fn generation(self) -> u32 {
        // `>>` discards the low word.
        (self.0 >> Self::INDEX_BITS) as u32
    }

    /// The packed `u64`, for serialization.
    #[must_use]
    pub const fn to_bits(self) -> u64 {
        self.0
    }

    /// Rebuilds a handle from `to_bits`.
    #[must_use]
    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }
}

/// One slot: holding a value, or vacant and linked into the free list.
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

/// A slot as a snapshot sees it: enough to rebuild the arena exactly.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SlotState<T> {
    Occupied {
        generation: u32,
        value: T,
    },
    Vacant {
        generation: u32,
        next_free: Option<u32>,
    },
}

/// A snapshot does not describe a valid arena.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum ArenaRestoreError {
    #[error("free list points at slot {0}, which is out of range or occupied")]
    BadFreeLink(u32),
    #[error("free list reaches {reachable} of {vacant} vacant slots")]
    FreeListIncomplete { reachable: usize, vacant: usize },
}

/// `Vec`-backed storage that reuses freed slots through an intrusive free list.
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
    /// An empty arena. Allocates nothing until the first insert.
    #[must_use]
    pub const fn new() -> Self {
        // `Vec::new()` is `const` and does not touch the allocator.
        Self {
            slots: Vec::new(),
            free_head: None,
            len: 0,
        }
    }

    /// An empty arena with room for `capacity` slots, allocated once up front.
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

    /// True when nothing is live.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Slots ever allocated, live or vacant. Sizes side tables keyed by `JobId::index`.
    #[must_use]
    pub fn capacity_slots(&self) -> usize {
        self.slots.len()
    }

    /// The handle the next `insert` will return. Lets a caller validate and
    /// log a value before it enters the arena.
    #[must_use]
    pub fn next_id(&self) -> JobId {
        // The head of the free list is reused first; otherwise the arena grows.
        if let Some(index) = self.free_head {
            let Slot::Vacant { generation, .. } = &self.slots[index as usize] else {
                unreachable!("free list pointed at an occupied slot");
            };
            JobId::new(index, *generation)
        } else {
            let index = u32::try_from(self.slots.len()).expect("arena exceeded u32::MAX slots");
            JobId::new(index, 0)
        }
    }

    /// Head of the free list, for snapshots.
    #[must_use]
    pub const fn free_head(&self) -> Option<u32> {
        self.free_head
    }

    /// Every slot in index order, borrowed, for snapshots.
    pub fn slots(&self) -> impl ExactSizeIterator<Item = SlotState<&T>> {
        self.slots.iter().map(|slot| match slot {
            Slot::Occupied { generation, value } => SlotState::Occupied {
                generation: *generation,
                value,
            },
            Slot::Vacant {
                generation,
                next_free,
            } => SlotState::Vacant {
                generation: *generation,
                next_free: *next_free,
            },
        })
    }

    /// Rebuilds an arena exactly as `slots` and `free_head` describe it, so
    /// later inserts yield the ids the original would have.
    ///
    /// # Errors
    /// `ArenaRestoreError` when the free list is broken or misses a vacant slot.
    pub fn from_parts(
        slots: Vec<SlotState<T>>,
        free_head: Option<u32>,
    ) -> Result<Self, ArenaRestoreError> {
        let vacant = slots
            .iter()
            .filter(|s| matches!(s, SlotState::Vacant { .. }))
            .count();
        // Walk the free list once: every link must land on a vacant slot, and
        // the walk may not exceed the vacant count (that would be a cycle).
        let mut cursor = free_head;
        let mut reachable = 0_usize;
        while let Some(index) = cursor {
            match slots.get(index as usize) {
                Some(SlotState::Vacant { next_free, .. }) if reachable < vacant => {
                    cursor = *next_free;
                    reachable += 1;
                }
                _ => return Err(ArenaRestoreError::BadFreeLink(index)),
            }
        }
        if reachable != vacant {
            return Err(ArenaRestoreError::FreeListIncomplete { reachable, vacant });
        }
        let len = slots.len() - vacant;
        let slots = slots
            .into_iter()
            .map(|s| match s {
                SlotState::Occupied { generation, value } => Slot::Occupied { generation, value },
                SlotState::Vacant {
                    generation,
                    next_free,
                } => Slot::Vacant {
                    generation,
                    next_free,
                },
            })
            .collect();
        Ok(Self {
            slots,
            free_head,
            len,
        })
    }

    /// Stores `value` and returns its handle. Panics past `u32::MAX` slots.
    pub fn insert(&mut self, value: T) -> JobId {
        self.len += 1;
        // `if let` runs the block only when `free_head` is `Some`, binding `index`.
        if let Some(index) = self.free_head {
            // `&mut` borrows the slot so it can be overwritten in place.
            let slot = &mut self.slots[index as usize];
            // `let ... else` destructures or diverges; `unreachable!` marks the impossible.
            let Slot::Vacant {
                generation,
                next_free,
            } = *slot
            else {
                unreachable!("free list pointed at an occupied slot");
            };
            // Pop the free list: its head becomes whatever this slot pointed at.
            self.free_head = next_free;
            // `*slot = ...` writes through the mutable reference.
            *slot = Slot::Occupied { generation, value };
            JobId::new(index, generation)
        } else {
            // `try_from` fails loudly instead of wrapping if the count exceeds `u32`.
            let index = u32::try_from(self.slots.len()).expect("arena exceeded u32::MAX slots");
            self.slots.push(Slot::Occupied {
                generation: 0,
                value,
            });
            JobId::new(index, 0)
        }
    }

    /// Borrows the value behind `id`, or `None` if `id` is stale.
    #[must_use]
    pub fn get(&self, id: JobId) -> Option<&T> {
        // `?` on an `Option` returns `None` early when the index is out of range.
        match self.slots.get(id.index() as usize)? {
            // A match guard (`if ...`) adds a condition on top of the pattern.
            Slot::Occupied { generation, value } if *generation == id.generation() => Some(value),
            _ => None,
        }
    }

    /// Mutably borrows the value behind `id`, or `None` if stale.
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

    /// Removes and returns the value behind `id`, bumping the slot's generation so
    /// every copy of `id` goes stale. `None` if already stale.
    pub fn remove(&mut self, id: JobId) -> Option<T> {
        let index = id.index();
        // Copy `free_head` out first: while `slot` mutably borrows `self.slots`,
        // the borrow checker forbids reading any other field through `self`.
        let free_head = self.free_head;
        let slot = self.slots.get_mut(index as usize)?;
        let Slot::Occupied { generation, .. } = slot else {
            return None;
        };
        if *generation != id.generation() {
            return None;
        }
        // `wrapping_add` rolls over at `u32::MAX` instead of panicking.
        let next_generation = generation.wrapping_add(1);
        // `mem::replace` swaps in the new value and hands back the old one.
        let previous = std::mem::replace(
            slot,
            Slot::Vacant {
                generation: next_generation,
                next_free: free_head,
            },
        );
        // Push this slot onto the free list.
        self.free_head = Some(index);
        self.len -= 1;
        match previous {
            Slot::Occupied { value, .. } => Some(value),
            Slot::Vacant { .. } => unreachable!("checked occupied above"),
        }
    }

    /// Iterates live entries in slot order.
    pub fn iter(&self) -> impl Iterator<Item = (JobId, &T)> {
        // `enumerate` pairs each slot with its index; `filter_map` keeps only the `Some`s.
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
