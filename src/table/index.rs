//! Table index
use crate::{
    key::{self, Ordering},
    Key,
};
use ahash::RandomState;
use indexmap::{IndexMap, IndexSet};
use std::hash::Hash;

/// key index to position in index map
pub type KeyRoots = IndexMap<usize, usize, RandomState>;

/// Index set of keys
pub type KeySet<K> = IndexSet<Key<K>, RandomState>;

/// Index of keys
#[derive(Debug, Clone)]
pub struct Index<K> {
    /// keys
    keys:  KeySet<K>,
    /// key and position in index of roots
    roots: KeyRoots,
    /// ordered index of keys
    index: Vec<usize>,
}

impl<K> Default for Index<K> {
    fn default() -> Self { Self::new() }
}

impl<K> Index<K> {
    /// creates a new empty index
    pub fn new() -> Self {
        Self {
            keys:  KeySet::default(),
            roots: KeyRoots::default(),
            index: Vec::default(),
        }
    }

    /// creates a new index with the given capacity
    pub fn with_capacity(n: usize) -> Self {
        Self {
            keys:  KeySet::with_capacity_and_hasher(n, RandomState::new()),
            roots: IndexMap::with_hasher(RandomState::new()),
            index: Vec::with_capacity(n),
        }
    }

    /// returns true if this index contains the given key
    pub fn contains(&self, key: &Key<K>) -> bool
    where
        K: Hash + Eq,
    {
        self.keys.contains(key)
    }

    /// return a reference to the keys in this index
    pub fn keys(&self) -> &KeySet<K> { &self.keys }

    /// return a slice containing ordered keys indexes
    /// if the index is not built the slice will be empty
    pub fn index(&self) -> &[usize] { &self.index }

    /// return an sorted indexmap over root keys index and position in the ordered index
    /// if the index is not built the slice will be empty
    pub fn roots(&self) -> &KeyRoots { &self.roots }

    #[cfg(feature = "rayon")]
    /// builds this index in parallel
    pub fn par_build(&mut self)
    where
        K: Ord + Send + Sync,
    {
        use rayon::slice::ParallelSliceMut;

        if self.index.is_empty() {
            self.index.extend(0..self.keys.len());
            self.index.par_sort_by(|x, y| self.keys[*x].key_cmp(&self.keys[*y]).into());
            self.index.iter().enumerate().for_each(|(i, kidx)| {
                if let Some((x, _)) = self.roots.last() {
                    if let Ordering::Less = &self.keys[*x].key_cmp(&self.keys[*kidx]) {
                        self.roots.insert(*kidx, i);
                    }
                } else {
                    self.roots.insert(*kidx, i);
                }
            });
        }
    }

    /// builds this index
    pub fn build(&mut self)
    where
        K: Ord,
    {
        if self.index.is_empty() {
            self.index.extend(0..self.keys.len());
            self.index.sort_by(|x, y| self.keys[*x].key_cmp(&self.keys[*y]).into());
            self.index.iter().enumerate().for_each(|(i, kidx)| {
                if let Some((x, _)) = self.roots.last() {
                    if let Ordering::Less = &self.keys[*x].key_cmp(&self.keys[*kidx]) {
                        self.roots.insert(*kidx, i);
                    }
                } else {
                    self.roots.insert(*kidx, i);
                }
            });
        }
    }

    /// number of keys
    pub fn len(&self) -> usize { self.keys.len() }

    /// number of indexed root keys
    pub fn len_roots(&self) -> usize { self.roots.len() }

    /// returns true if the keys are currently indexed
    pub fn is_indexed(&self) -> bool { !self.index.is_empty() }

    /// insert the given key in the this index
    ///
    /// if the inserted key when indexed goes in last position, the index is not invalidated
    pub fn insert(&mut self, key: Key<K>) -> (usize, bool)
    where
        K: Hash + Ord,
    {
        let (i, inserted) = self.keys.insert_full(key);
        if self.keys.len() == 1 {
            self.index = vec![0];
            self.roots = KeyRoots::from_iter([(0, 0)]);
        } else if inserted {
            if i == self.index.len()
                && &self.keys[i]
                    > self.keys.get_index(*self.index.last().unwrap()).unwrap()
            {
                self.index.push(i);
                if let key::Ordering::Less =
                    self.keys[*self.roots.last().unwrap().0].key_cmp(&self.keys[i])
                {
                    self.roots.insert(i, i);
                }
            } else {
                self.index.clear();
                self.roots.clear();
            }
        }
        (i, inserted)
    }

    /// try to keep an index valid after removing the given key from `self.keys` but not from the index
    fn try_maintain_index(&mut self, key: Key<K>, index: usize) -> (Key<K>, usize) {
        if index + 1 == self.index.len() && self.index[index] == index {
            self.index.pop();
            if index == *self.roots.last().unwrap().0 {
                self.roots.pop();
            }
        } else if index + 2 == self.index.len() && self.index[index] == index {
            self.index.swap_remove(index);
            let x = self.index.last_mut().unwrap();
            if *x == self.keys.len() {
                *x -= 1;
            }
            if self.roots.remove(&(index + 1)).is_some() {
                self.roots.insert(index, index);
            } else {
                self.roots.remove(&index);
            }
        } else {
            self.index.clear();
            self.roots.clear();
        }
        (key, index)
    }

    /// swap remove the given key
    /// if possible the internal index is maintained
    pub fn swap_remove(&mut self, key: &Key<K>) -> Option<(Key<K>, usize)>
    where
        K: Hash + Ord,
    {
        if let Some((x, y)) = self.keys.swap_remove_full(key) {
            Some(self.try_maintain_index(y, x))
        } else {
            None
        }
    }

    /// swap remove the given key index
    /// if possible the internal index is maintained
    pub fn swap_remove_index(&mut self, index: usize) -> Option<Key<K>>
    where
        K: Hash + Ord,
    {
        if let Some(x) = self.keys.swap_remove_index(index) {
            Some(self.try_maintain_index(x, index).0)
        } else {
            None
        }
    }

    /// Shrink the capacity of this index as much as possible.
    pub fn shrink_to_fit(&mut self)
    where
        K: Hash + Eq,
    {
        self.keys.shrink_to_fit();
        self.roots.shrink_to_fit();
        self.index.shrink_to_fit();
    }

    // TODO: investigate sorted networks for maintaining the index on insert or delete (ie: https://bertdobbelaere.github.io/sorting_networks.html)
}

impl<K: Hash + Ord> From<Key<K>> for Index<K> {
    fn from(value: Key<K>) -> Self {
        Self {
            keys:  KeySet::from_iter([value]),
            roots: KeyRoots::from_iter([(0, 0)]),
            index: vec![0],
        }
    }
}

impl<K: Hash + Ord> FromIterator<Key<K>> for Index<K> {
    fn from_iter<T: IntoIterator<Item = Key<K>>>(iter: T) -> Self {
        let keys = iter.into_iter();
        let x = match keys.size_hint() {
            (_, Some(x)) => x,
            (x, _) => x,
        };
        let mut new = Self {
            index: Vec::with_capacity(x),
            keys:  KeySet::with_capacity_and_hasher(x, RandomState::new()),
            roots: KeyRoots::default(),
        };
        for k in keys {
            new.insert(k);
        }
        new
    }
}

impl<K: Hash + Ord> From<Vec<Key<K>>> for Index<K> {
    fn from(value: Vec<Key<K>>) -> Self { value.into_iter().collect() }
}

impl<K: Hash + Ord, const N: usize> From<[Key<K>; N]> for Index<K> {
    fn from(value: [Key<K>; N]) -> Self { value.into_iter().collect() }
}

impl<K: Hash + Ord + Clone> From<&[Key<K>]> for Index<K> {
    fn from(value: &[Key<K>]) -> Self { value.into_iter().cloned().collect() }
}

impl<K: Hash + Ord> From<KeySet<K>> for Index<K> {
    fn from(keys: KeySet<K>) -> Self {
        let mut new = Self {
            index: Vec::with_capacity(keys.len()),
            keys,
            roots: KeyRoots::default(),
        };
        new.build();
        new
    }
}

impl<K: Hash + Ord> std::iter::Extend<Key<K>> for Index<K> {
    fn extend<T: IntoIterator<Item = Key<K>>>(&mut self, iter: T) {
        self.keys.extend(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_index() {
        let mut t = Index::with_capacity(5);
        let k1 = Key::from(["foo", "bar", "aa", "ff"]);
        let k2 = Key::from(["foo", "bar", "aa", "ff", "aa"]);
        let k3 = Key::from(["foo", "bar", "aa", "fff"]);
        let k4 = Key::from(["foo", "bar", "aa", "fff", "abc"]);
        let k5 = Key::from(["foo", "bar", "B"]);
        let k6 = Key::from(["foo", "bar", "b"]);
        t.insert(k5.clone());
        t.insert(k1.clone());
        t.insert(k2.clone());
        t.insert(k3.clone());
        t.insert(k4.clone());
        t.insert(k6.clone());
        assert_eq!(t.len_roots(), 4);
        t.swap_remove(&k4);
        assert_eq!(t.len_roots(), 4);
        t.swap_remove(&k3);
        assert_eq!(t.len_roots(), 3);
        t.swap_remove(&k6);
        assert_eq!(t.len_roots(), 2);
        t.swap_remove(&k5);
        assert!(!t.is_indexed());
        t.build();
        assert_eq!(t.len_roots(), 1);
    }
}
