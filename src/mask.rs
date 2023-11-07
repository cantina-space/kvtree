//! [Key] pattern matching
use crate::key::{Key, Match, MatchAtom};
use std::{borrow::Borrow, ops::Index, slice::SliceIndex};

/// Mask element type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Atom<K, O = ()> {
    /// Match a single key element
    Any,
    /// Match 1 or more key element
    Many,
    /// Exact match a single key element
    Key(K),
    /// Custom mask atom
    Other(O),
}

impl<K, C> MatchAtom<K> for Atom<K, C>
where
    K: PartialEq + Send + Sync,
    C: MatchAtom<K>,
{
    fn match_atom(&self, atom: &K) -> bool {
        match (self, atom) {
            (Atom::Any, _) | (Atom::Many, _) => true,
            (Atom::Key(x), y) => x == y,
            (Atom::Other(x), y) => x.match_atom(y),
        }
    }
}

/// A mask can match 0 or more keys
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Mask<K, C = ()>(Box<[Atom<K, C>]>);

impl<K, C> Mask<K, C> {
    /// Creates a mask with a single element
    pub fn new(item: impl Into<Atom<K, C>>) -> Self {
        Self(Box::new([item.into()]))
    }

    /// number of elements in this mask
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// return true if this mask is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// reverse the order of elements in this mask
    pub fn reverse(&mut self) {
        self.0.reverse()
    }
}

impl<K, C, V> From<V> for Mask<K, C>
where
    V: Into<Box<[Atom<K, C>]>>,
{
    #[inline]
    fn from(value: V) -> Self {
        Mask(value.into())
    }
}

impl<K, C> From<Key<K>> for Mask<K, C> {
    #[inline]
    fn from(value: Key<K>) -> Self {
        Mask(value.into_iter().map(Atom::Key).collect())
    }
}

impl<K, C> FromIterator<Atom<K, C>> for Mask<K, C> {
    fn from_iter<T: IntoIterator<Item = Atom<K, C>>>(iter: T) -> Self {
        Mask(iter.into_iter().collect())
    }
}

impl<K, C> FromIterator<Mask<K, C>> for Mask<K, C> {
    fn from_iter<T: IntoIterator<Item = Mask<K, C>>>(iter: T) -> Self {
        Mask(iter.into_iter().flatten().collect())
    }
}

impl<K, C> AsRef<[Atom<K, C>]> for Mask<K, C> {
    #[inline]
    fn as_ref(&self) -> &[Atom<K, C>] {
        &self.0
    }
}

impl<K, C> Borrow<[Atom<K, C>]> for Mask<K, C> {
    #[inline]
    fn borrow(&self) -> &[Atom<K, C>] {
        self.0.borrow()
    }
}

impl<K, C, I> Index<I> for Mask<K, C>
where
    I: SliceIndex<[Atom<K, C>]>,
{
    type Output = <I as SliceIndex<[Atom<K, C>]>>::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        self.0.index(index)
    }
}

impl<'a, K, C> IntoIterator for &'a Mask<K, C> {
    type Item = <&'a [Atom<K, C>] as IntoIterator>::Item;
    type IntoIter = <&'a [Atom<K, C>] as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<K, C> IntoIterator for Mask<K, C> {
    type Item = <Vec<Atom<K, C>> as IntoIterator>::Item;
    type IntoIter = <Vec<Atom<K, C>> as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        Vec::from(self.0).into_iter()
    }
}

impl<K, C> Match<K> for Mask<K, C>
where
    K: PartialEq + Send + Sync,
    C: MatchAtom<K>,
{
    /// return true if this mask matches the key
    fn match_key(&self, key: &Key<K>) -> bool {
        #[inline]
        fn first_match<K, C>(pats: &[&Atom<K, C>], mut keys: &[K]) -> Option<usize>
        where
            K: PartialEq + Send + Sync,
            C: MatchAtom<K>,
        {
            if keys.len() < pats.len() {
                None
            } else {
                let mut r = 0;
                while pats.len() <= keys.len() {
                    if pats
                        .iter()
                        .zip(keys.iter().take(pats.len()))
                        .all(|(x, y)| x.match_atom(y))
                    {
                        return Some(r);
                    }
                    r += 1;
                    keys = &keys[1..];
                }
                None
            }
        }
        if self.0.is_empty() || key.is_empty() || key.len() < self.0.len() {
            return false;
        }
        // create groups of consecutive pattern that matches 1 element or 1+ elements
        let groups = self
            .0
            .iter()
            .fold(Vec::new(), |mut acc: Vec<Vec<&Atom<K, C>>>, x| {
                if let Some(ref mut last) = acc.last_mut() {
                    match (&last[0], x) {
                        (Atom::Many, r @ Atom::Many) => last.push(r),
                        (Atom::Many, r) => acc.push(vec![r]),
                        (_, r @ Atom::Many) => acc.push(vec![r]),
                        (_, r) => last.push(r),
                    }
                } else {
                    acc.push(vec![x]);
                }
                acc
            });
        let mut key_tmp = key.as_ref();
        let mut pat_tmp = groups.as_slice();
        //matching the pattern from the beginning until we find a MaskItem::Many
        for grp in groups.iter() {
            match grp[0] {
                Atom::Many => {
                    break;
                }
                _ => {
                    if !grp
                        .iter()
                        .zip(key_tmp.iter().take(grp.len()))
                        .all(|(x, y)| x.match_atom(y))
                    {
                        return false;
                    }
                    key_tmp = &key_tmp[grp.len()..];
                    pat_tmp = &pat_tmp[1..];
                }
            }
        }
        loop {
            // empty sequences
            if key_tmp.is_empty() && pat_tmp.is_empty() {
                return true;
            } else if key_tmp.is_empty() || pat_tmp.is_empty() {
                // one of keys or pats are not empty
                return false;
            }
            // we have more pattern than remaining in the key
            if key_tmp.len() < pat_tmp.len() {
                return false;
            }
            // last pattern
            if pat_tmp.len() == 1 {
                // if last pattern is Many we want to have at least the same number of elements remaining
                if let Atom::Many = *pat_tmp[0][0] {
                    return pat_tmp[0].len() <= key_tmp.len();
                }
                // we take the last n characters and we see if it matches the pattern
                if key_tmp.len() < pat_tmp[0].len() {
                    return false;
                }
                let (_, r) = key_tmp.split_at(key_tmp.len() - pat_tmp[0].len());
                return pat_tmp[0].iter().zip(r).all(|(x, y)| x.match_atom(y));
            } else if let Atom::Many = *pat_tmp[0][0] {
                //consume Many patterns and take the same number in key
                key_tmp = &key_tmp[pat_tmp[0].len()..];
                pat_tmp = &pat_tmp[1..];
            } else if let Some(idx) = first_match(pat_tmp[0].as_slice(), key_tmp) {
                // found the 1st occurrence of non Many pattern
                // we know that there is at least one Many remaining
                // so we just want to see if the pattern is present at least one time
                key_tmp = &key_tmp[idx..];
                pat_tmp = &pat_tmp[1..];
            } else {
                return false;
            }
        }
    }

    fn match_children(&self, key: &Key<K>) -> bool {
        if self.0.is_empty() || key.is_empty() {
            return false;
        }
        let nb = if let Some(i) = self.0.iter().position(|x| matches!(x, Atom::Many)) {
            self.0.len().min(key.len()).min(i)
        } else if self.0.len() >= key.len() {
            self.0.len().min(key.len())
        } else {
            return false;
        };
        self.0
            .iter()
            .take(nb)
            .zip(key.as_ref().iter().take(nb))
            .all(|(x, y)| x.match_atom(y))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_key() {
        assert!(
            Mask::<_, ()>::from([Atom::Key("foo"), Atom::Key("a"), Atom::Key("b")])
                .match_key(&Key::from(["foo", "a", "b"]))
        );
        assert!(
            !Mask::<_, ()>::from([Atom::Key("foo"), Atom::Key("a"), Atom::Key("b")])
                .match_key(&Key::from(["foo", "a", "b", "a", "b", "a", "b", "a", "b"]))
        );
        assert!(!Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Key("a"),
            Atom::Key("b"),
            Atom::Key("a"),
            Atom::Key("b"),
            Atom::Key("a"),
            Atom::Key("b"),
            Atom::Key("a"),
            Atom::Key("b")
        ])
        .match_key(&Key::from(["foo", "a", "b"])));

        assert!(
            Mask::<_, ()>::from([Atom::Key("foo"), Atom::Any]).match_key(&Key::from(["foo", "b"]))
        );
        assert!(!Mask::<_, ()>::from([Atom::Key("foo"), Atom::Any])
            .match_key(&Key::from(["foo", "b", "c"])));
        assert!(!Mask::<_, ()>::from([Atom::Key("foo"), Atom::Any]).match_key(&Key::new("foo")));
        assert!(
            Mask::<_, ()>::from([Atom::Key("foo"), Atom::Any, Atom::Any])
                .match_key(&Key::from(["foo", "b", "c"]))
        );
        assert!(
            !Mask::<_, ()>::from([Atom::Key("foo"), Atom::Any, Atom::Any])
                .match_key(&Key::from(["foo", "b"]))
        );
        assert!(
            !Mask::<_, ()>::from([Atom::Key("foo"), Atom::Any, Atom::Any])
                .match_key(&Key::from(["foo.b.d.e"]))
        );
        assert!(Mask::<_, ()>::from([Atom::Key("foo"), Atom::Many,])
            .match_key(&Key::from(["foo", "b"])));
        assert!(Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Key("a"),
            Atom::Key("b")
        ])
        .match_key(&Key::from(["foo", "a", "b", "a", "b", "a", "b", "a", "b"])));
        assert!(!Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Key("a"),
            Atom::Key("b")
        ])
        .match_key(&Key::from(["foo", "a", "b", "a", "b", "a", "b", "a"])));
        assert!(!Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Key("a"),
            Atom::Key("b")
        ])
        .match_key(&Key::from(["foo", "a", "b"])));
        assert!(!Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Key("a"),
            Atom::Key("b")
        ])
        .match_key(&Key::from(["foo", "b", "a", "c", "b"])));
        assert!(Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Key("a"),
            Atom::Key("b"),
            Atom::Many,
            Atom::Key("a"),
            Atom::Key("b")
        ])
        .match_key(&Key::from(["foo", "a", "b", "a", "b", "a", "b", "a", "b"])));
        assert!(!Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Key("a"),
            Atom::Key("b"),
            Atom::Many,
            Atom::Key("a"),
            Atom::Key("b")
        ])
        .match_key(&Key::from(["foo", "a", "b", "b", "a", "c", "a", "b"])));
        assert!(!Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Key("b"),
            Atom::Key("b")
        ])
        .match_key(&Key::from(["foo", "a", "b", "a", "b", "a", "b", "a", "a"])));

        assert!(!Mask::<_, ()>::from([Atom::Many, Atom::Many]).match_key(&Key::new("a")));
        assert!(Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Many,
            Atom::Many,
            Atom::Key("e")
        ])
        .match_key(&Key::from(["foo", "b", "b", "b", "e"])));
        assert!(!Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Many,
            Atom::Many,
            Atom::Key("e")
        ])
        .match_key(&Key::from(["foo", "b", "b", "b", "d"])));
        assert!(!Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Many,
            Atom::Many,
            Atom::Key("c")
        ])
        .match_key(&Key::from(["foo", "b", "b", "c"])));
        assert!(Mask::<_, ()>::from([Atom::<&str>::Many, Atom::Any])
            .match_key(&Key::from(["a", "foo"])));
        assert!(!Mask::<_, ()>::from([Atom::Many, Atom::Any]).match_key(&Key::new("a")));
        assert!(Mask::<_, ()>::from([Atom::<&str>::Many, Atom::Any])
            .match_key(&Key::from(["a", "foo", "bar"])));
        assert!(Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Key("a"),
            Atom::Key("b"),
            Atom::Many,
            Atom::Any,
            Atom::Key("b"),
            Atom::Many,
            Atom::Key("a"),
            Atom::Key("b"),
        ])
        .match_key(&Key::from([
            "foo", "a", "b", "a", "b", "a", "b", "a", "b", "a", "b", "a", "b"
        ])));
        assert!(Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Many,
            Atom::Many,
            Atom::Any
        ])
        .match_key(&Key::from(["foo", "b", "b", "b", "d"])));
        assert!(!Mask::<_, ()>::from([
            Atom::Key("foo"),
            Atom::Many,
            Atom::Many,
            Atom::Many,
            Atom::Any
        ])
        .match_key(&Key::from(["foo", "b", "b", "b"])));
        assert!(Mask::<_, ()>::from([Atom::<&str>::Any, Atom::Many])
            .match_key(&Key::from(["a", "foo"])));
        assert!(Mask::<_, ()>::from([Atom::<&str>::Any, Atom::Many])
            .match_key(&Key::from(["a", "foo", "bar"])));
        assert!(!Mask::<_, ()>::from([Atom::Many, Atom::Any]).match_key(&Key::new("foo")));
        assert!(!Mask::<_, ()>::from([Atom::Any, Atom::Many]).match_key(&Key::new("a")));
        assert!(
            Mask::<_, ()>::from([Atom::Key("foo"), Atom::Many, Atom::Key("a"), Atom::Any])
                .match_key(&Key::from(["foo", "b", "c", "a", "a", "d"]))
        );
        assert!(Mask::<_, ()>::from([Atom::Many, Atom::Key("a"),])
            .match_key(&Key::from(["foo", "b", "c", "a", "a", "d", "a"])));
        assert!(!Mask::<_, ()>::from([Atom::Many, Atom::Key("a"),])
            .match_key(&Key::from(["foo", "b", "c", "a", "a", "d", "a", "z"])));
    }

    #[test]
    fn match_child() {
        assert!(Mask::<_, ()>::from([Atom::<i32>::Many]).match_children(&Key::from([1, 2, 3])),);
        assert!(!Mask::<_, ()>::from([Atom::<i32>::Any]).match_children(&Key::from([1, 2, 3])),);

        assert!(
            Mask::<_, ()>::from([Atom::<i32>::Any, Atom::Any, Atom::Any, Atom::Any])
                .match_children(&Key::from([1, 2, 3])),
        )
    }
}
