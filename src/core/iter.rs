use super::{is_readable_at, is_writable_at};
use crate::{core::LayerAccess, key, Key};
use ahash::AHashSet;
use std::hash::Hash;

/// Core iterator
pub struct Iter<'a, K, C, I> {
    layers: Vec<I>,
    access: &'a [LayerAccess<K, C>],
    seen:   AHashSet<&'a Key<K>>,
}

impl<'a, K, C, I: DoubleEndedIterator> Iter<'a, K, C, I> {
    pub(in crate::core) fn new(
        x: impl Iterator<Item = I>, access: &'a [LayerAccess<K, C>],
    ) -> Self
    where
        Self: Iterator,
    {
        Self { layers: x.collect(), access, seen: AHashSet::new() }
    }
}

impl<'a, K, C, I, V> Iterator for Iter<'a, K, C, I>
where
    K: Hash + Ord + Send + Sync,
    I: Iterator<Item = (&'a Key<K>, V)>,
    C: key::MatchAtom<K>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let it = self.layers.last_mut()?;
        if let Some((k, v)) = it.next() {
            let i = self.layers.len() - 1;
            if is_readable_at(self.access, k, i).unwrap_or(true) && self.seen.insert(k) {
                Some((k, v))
            } else {
                self.next()
            }
        } else {
            self.layers.pop();
            self.next()
        }
    }
}

impl<'a, K, C, I, V> DoubleEndedIterator for Iter<'a, K, C, I>
where
    K: Hash + Ord + Send + Sync,
    I: DoubleEndedIterator<Item = (&'a Key<K>, V)>,
    C: key::MatchAtom<K>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let it = self.layers.last_mut()?;
        if let Some((k, v)) = it.next_back() {
            let i = self.layers.len() - 1;
            if is_readable_at(self.access, k, i).unwrap_or(true) && self.seen.insert(k) {
                Some((k, v))
            } else {
                self.next_back()
            }
        } else {
            self.layers.pop();
            self.next_back()
        }
    }
}

/// Core mutable iterator
pub struct IterMut<'a, K, C, I> {
    layers: Vec<I>,
    access: &'a [LayerAccess<K, C>],
    seen:   AHashSet<&'a Key<K>>,
}

impl<'a, K, C, I: DoubleEndedIterator> IterMut<'a, K, C, I> {
    pub(in crate::core) fn new(
        x: impl Iterator<Item = I>, access: &'a [LayerAccess<K, C>],
    ) -> Self
    where
        Self: Iterator,
    {
        Self { layers: x.collect(), access, seen: AHashSet::new() }
    }
}

impl<'a, K, C, I, V> Iterator for IterMut<'a, K, C, I>
where
    K: Hash + Ord + Send + Sync,
    I: Iterator<Item = (&'a Key<K>, V)>,
    C: key::MatchAtom<K>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let it = self.layers.last_mut()?;
        if let Some((k, v)) = it.next() {
            let i = self.layers.len() - 1;
            if is_writable_at(self.access, k, i).unwrap_or(true) && self.seen.insert(k) {
                Some((k, v))
            } else {
                self.next()
            }
        } else {
            self.layers.pop();
            self.next()
        }
    }
}

impl<'a, K, C, I, V> DoubleEndedIterator for IterMut<'a, K, C, I>
where
    K: Hash + Ord + Send + Sync,
    I: DoubleEndedIterator<Item = (&'a Key<K>, V)>,
    C: key::MatchAtom<K>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let it = self.layers.last_mut()?;
        if let Some((k, v)) = it.next_back() {
            let i = self.layers.len() - 1;
            if is_writable_at(self.access, k, i).unwrap_or(true) && self.seen.insert(k) {
                Some((k, v))
            } else {
                self.next_back()
            }
        } else {
            self.layers.pop();
            self.next_back()
        }
    }
}
