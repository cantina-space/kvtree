use crate::{
    key,
    query::QueryIter,
    table::index::{Index, KeySet},
    Key,
};
use std::collections::VecDeque;

/// Iterator of root keys => Iterator of children keys
pub struct RootsIter<'a, K> {
    keys: &'a KeySet<K>,
    items: <Vec<(usize, &'a [usize])> as IntoIterator>::IntoIter,
}

impl<'a, K> RootsIter<'a, K> {
    pub(in crate::table) fn new(index: &'a Index<K>) -> Self {
        let idx = index.index();
        assert!(!idx.is_empty());
        Self {
            keys: index.keys(),
            items: index
                .roots()
                .iter()
                .rev()
                .scan(idx.len(), |end, (kidx, iidx)| {
                    Some((
                        *kidx,
                        &idx[*iidx + if *iidx < *end { 1 } else { 0 }
                            ..std::mem::replace(end, *iidx)],
                    ))
                })
                .collect::<Vec<_>>()
                .into_iter(),
        }
    }
}

impl<'a, K> Iterator for RootsIter<'a, K> {
    type Item = (&'a Key<K>, Box<dyn QueryIter<Item = &'a Key<K>> + 'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next_back().map(|(i, x)| {
            (
                &self.keys[i],
                Box::new(x.iter().map(|x| &self.keys[*x]))
                    as Box<dyn QueryIter<Item = &'a Key<K>> + 'a>,
            )
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.items.len(), Some(self.items.len()))
    }
}

impl<'a, K> DoubleEndedIterator for RootsIter<'a, K> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.items.next().map(|(i, x)| {
            (
                &self.keys[i],
                Box::new(x.iter().map(|x| &self.keys[*x]))
                    as Box<dyn QueryIter<Item = &'a Key<K>> + 'a>,
            )
        })
    }
}

impl<'a, K> ExactSizeIterator for RootsIter<'a, K> {
    fn len(&self) -> usize {
        self.items.len()
    }
}

/// [Query::mask][crate::Query::mask] table iterator
pub struct MaskIter<'a, 'b, K, M> {
    keys: &'a KeySet<K>,
    mask: &'b M,
    items: VecDeque<<&'a [usize] as IntoIterator>::IntoIter>,
}

impl<'a, 'b, K, M: key::Match<K>> MaskIter<'a, 'b, K, M> {
    pub(crate) fn new(index: &'a Index<K>, mask: &'b M) -> Self {
        let mut items = VecDeque::new();
        let mut off = 0;
        let keys = index.keys();
        let mut idx = index.index();
        let mut prev = false;
        for (ki, ii) in index.roots() {
            match (mask.match_key(&keys.as_slice()[*ki]), prev) {
                (true, true) => {
                    let (h, t) = idx.split_at(*ii + 1 - off);
                    items.push_back(h.iter());
                    idx = t;
                }
                (true, false) => {
                    let (h, t) = idx[*ii - off..].split_at(1);
                    items.push_back(h.iter());
                    idx = t;
                }
                (false, true) => {
                    let (h, t) = idx.split_at(*ii - off);
                    items.push_back(h.iter());
                    idx = t;
                }
                (false, false) => {
                    idx = &idx[*ii + 1 - off..];
                }
            }
            prev = mask.match_children(&keys[*ki]);
            off = *ii + 1;
        }
        if prev {
            items.push_back(idx.iter());
        }
        Self { keys, mask, items }
    }
}

impl<'a, 'b, K, M: key::Match<K>> Iterator for MaskIter<'a, 'b, K, M> {
    type Item = (&'a Key<K>, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(x) = self.items.front_mut().and_then(|x| x.next()) {
            if self.mask.match_key(&self.keys[*x]) {
                Some((&self.keys[*x], *x))
            } else {
                self.next()
            }
        } else if !self.items.is_empty() {
            self.items.pop_front();
            self.next()
        } else {
            None
        }
    }
}

impl<'a, 'b, K, M: key::Match<K>> DoubleEndedIterator for MaskIter<'a, 'b, K, M> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(x) = self.items.back_mut().and_then(|x| x.next_back()) {
            if self.mask.match_key(&self.keys[*x]) {
                Some((&self.keys[*x], *x))
            } else {
                self.next_back()
            }
        } else if !self.items.is_empty() {
            self.items.pop_back();
            self.next()
        } else {
            None
        }
    }
}
