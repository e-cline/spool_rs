//! A tiny generational slab.
//!
//! Keys carry a generation counter so that a stale key (for example, a wake for
//! a task that already finished and whose slot has been reused) is detected
//! and rejected instead of aliasing a new entry.

/// A key into a [`Slab`]: slot index plus the generation it was issued for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    index: u32,
    generation: u32,
}

impl Key {
    /// A key that never refers to a slab entry. Used as a sentinel.
    pub(crate) const SENTINEL: Key = Key {
        index: u32::MAX,
        generation: u32::MAX,
    };
}

enum Entry<T> {
    Occupied {
        generation: u32,
        value: T,
    },
    Vacant {
        generation: u32,
        next_free: Option<u32>,
    },
}

/// A generational slab with O(1) insert, lookup, and remove.
pub(crate) struct Slab<T> {
    entries: Vec<Entry<T>>,
    free_head: Option<u32>,
    len: usize,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Slab<T> {
    pub(crate) const fn new() -> Self {
        Slab {
            entries: Vec::new(),
            free_head: None,
            len: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The key that the next [`insert`](Self::insert) will return.
    pub(crate) fn next_key(&self) -> Key {
        match self.free_head {
            Some(index) => match &self.entries[index as usize] {
                Entry::Vacant { generation, .. } => Key {
                    index,
                    generation: *generation,
                },
                Entry::Occupied { .. } => unreachable!("free list points at an occupied slot"),
            },
            None => Key {
                index: u32::try_from(self.entries.len()).expect("slab overflow"),
                generation: 0,
            },
        }
    }

    pub(crate) fn insert(&mut self, value: T) -> Key {
        let key = self.next_key();
        assert!(key != Key::SENTINEL, "slab overflow");
        match self.free_head {
            Some(index) => {
                let slot = &mut self.entries[index as usize];
                let Entry::Vacant { next_free, .. } = *slot else {
                    unreachable!("free list points at an occupied slot")
                };
                self.free_head = next_free;
                *slot = Entry::Occupied {
                    generation: key.generation,
                    value,
                };
            }
            None => self.entries.push(Entry::Occupied {
                generation: 0,
                value,
            }),
        }
        self.len += 1;
        key
    }

    #[cfg(test)]
    pub(crate) fn get(&self, key: Key) -> Option<&T> {
        match self.entries.get(key.index as usize)? {
            Entry::Occupied { generation, value } if *generation == key.generation => Some(value),
            _ => None,
        }
    }

    pub(crate) fn get_mut(&mut self, key: Key) -> Option<&mut T> {
        match self.entries.get_mut(key.index as usize)? {
            Entry::Occupied { generation, value } if *generation == key.generation => Some(value),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, key: Key) -> bool {
        self.get(key).is_some()
    }

    pub(crate) fn remove(&mut self, key: Key) -> Option<T> {
        let slot = self.entries.get_mut(key.index as usize)?;
        match slot {
            Entry::Occupied { generation, .. } if *generation == key.generation => {
                // Bump the generation so stale keys for this slot stop matching.
                // On wrap-around we retire the slot rather than risk aliasing.
                let next_generation = generation.wrapping_add(1);
                let retired = next_generation == u32::MAX;
                let old = std::mem::replace(
                    slot,
                    Entry::Vacant {
                        generation: next_generation,
                        next_free: if retired { None } else { self.free_head },
                    },
                );
                if !retired {
                    self.free_head = Some(key.index);
                }
                self.len -= 1;
                match old {
                    Entry::Occupied { value, .. } => Some(value),
                    Entry::Vacant { .. } => unreachable!(),
                }
            }
            _ => None,
        }
    }

    /// Removes and returns every value, leaving the slab empty.
    ///
    /// Generations are preserved, so keys issued before the drain stay stale.
    pub(crate) fn drain(&mut self) -> Vec<T> {
        let keys: Vec<Key> = self.keys().collect();
        keys.into_iter().filter_map(|k| self.remove(k)).collect()
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = Key> + '_ {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| match entry {
                Entry::Occupied { generation, .. } => Some(Key {
                    index: index as u32,
                    generation: *generation,
                }),
                Entry::Vacant { .. } => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove() {
        let mut slab = Slab::new();
        let a = slab.insert("a");
        let b = slab.insert("b");
        assert_eq!(slab.len(), 2);
        assert_eq!(slab.get(a), Some(&"a"));
        assert_eq!(slab.get(b), Some(&"b"));
        assert_eq!(slab.remove(a), Some("a"));
        assert_eq!(slab.get(a), None);
        assert_eq!(slab.remove(a), None, "double remove is rejected");
        assert_eq!(slab.len(), 1);
    }

    #[test]
    fn stale_key_after_reuse_is_rejected() {
        let mut slab = Slab::new();
        let a = slab.insert(1);
        slab.remove(a);
        let b = slab.insert(2);
        assert_eq!(a.index, b.index, "slot is reused");
        assert_ne!(a, b, "but with a new generation");
        assert_eq!(slab.get(a), None);
        assert_eq!(slab.get_mut(a), None);
        assert!(!slab.contains(a));
        assert_eq!(slab.remove(a), None);
        assert_eq!(slab.get(b), Some(&2));
    }

    #[test]
    fn next_key_predicts_insert() {
        let mut slab = Slab::new();
        for i in 0..10 {
            let predicted = slab.next_key();
            assert_eq!(slab.insert(i), predicted);
        }
        let k = slab.keys().nth(3).unwrap();
        slab.remove(k);
        let predicted = slab.next_key();
        assert_eq!(slab.insert(99), predicted);
    }

    #[test]
    fn free_list_is_lifo_and_complete() {
        let mut slab = Slab::new();
        let keys: Vec<_> = (0..5).map(|i| slab.insert(i)).collect();
        for &k in &keys {
            slab.remove(k);
        }
        assert!(slab.is_empty());
        let reused: Vec<_> = (0..5).map(|i| slab.insert(i)).collect();
        // All five slots are reused; no new slots are allocated.
        assert_eq!(slab.entries.len(), 5);
        for k in reused {
            assert!(slab.contains(k));
        }
    }

    #[test]
    fn drain_empties_and_invalidates() {
        let mut slab = Slab::new();
        let keys: Vec<_> = (0..4).map(|i| slab.insert(i)).collect();
        let mut drained = slab.drain();
        drained.sort();
        assert_eq!(drained, vec![0, 1, 2, 3]);
        assert!(slab.is_empty());
        for k in keys {
            assert!(!slab.contains(k));
        }
    }

    #[test]
    fn generation_wraparound_retires_slot() {
        let mut slab = Slab::new();
        let k = slab.insert(());
        // Force the slot to the edge of its generation space.
        slab.entries[k.index as usize] = Entry::Occupied {
            generation: u32::MAX - 1,
            value: (),
        };
        let edge = Key {
            index: k.index,
            generation: u32::MAX - 1,
        };
        assert!(slab.remove(edge).is_some());
        // The slot is retired rather than recycled.
        let fresh = slab.insert(());
        assert_ne!(fresh.index, k.index);
    }

    #[test]
    fn sentinel_never_matches() {
        let mut slab = Slab::new();
        slab.insert(1);
        assert!(!slab.contains(Key::SENTINEL));
    }
}
