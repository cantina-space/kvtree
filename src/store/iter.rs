use crate::{key::Ordering, Key};
use std::collections::VecDeque;

/// [Store][crate::Store] ordered iterator  
pub struct OrderedIter<'a, I, K, V>(pub(in crate::store) VecDeque<((&'a Key<K>, V), I)>);

impl<'a, I, K, V> OrderedIter<'a, I, K, V> {}

impl<'a, I, K, V> Iterator for OrderedIter<'a, I, K, V>
where
    K: Ord,
    I: Iterator<Item = (&'a Key<K>, V)>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(f) = self.0.front_mut() {
            if let Some(x) = f.1.next() {
                Some(if let Ordering::Less | Ordering::Child = f.0 .0.key_cmp(x.0) {
                    let r = std::mem::replace(&mut f.0, x);
                    self.0
                        .make_contiguous()
                        .sort_by(|((x, _), _), ((y, _), _)| x.key_cmp(y).into());
                    r
                } else {
                    x
                })
            } else {
                self.0.pop_front().map(|(x, _)| x)
            }
        } else {
            None
        }
    }
}

impl<'a, I, K, V> DoubleEndedIterator for OrderedIter<'a, I, K, V>
where
    K: Ord,
    I: DoubleEndedIterator<Item = (&'a Key<K>, V)>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(f) = self.0.back_mut() {
            if let Some(x) = f.1.next_back() {
                Some(if let Ordering::Greater | Ordering::Parent = f.0 .0.key_cmp(x.0) {
                    let r = std::mem::replace(&mut f.0, x);
                    self.0
                        .make_contiguous()
                        .sort_by(|((x, _), _), ((y, _), _)| x.key_cmp(y).into());
                    r
                } else {
                    x
                })
            } else {
                self.0.pop_back().map(|(x, _)| x)
            }
        } else {
            None
        }
    }
}
