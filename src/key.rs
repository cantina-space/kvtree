//! Key Module

use crate::mask::{Atom, Mask};
use std::{
    borrow::Borrow,
    cmp::Ordering as StdOrdering,
    fmt::Debug,
    ops::{
        Add, ControlFlow, Index, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo,
        RangeToInclusive,
    },
    slice::SliceIndex,
};

/// Key comparaison result
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum Ordering {
    /// An ordering where a compared key is equal to another.
    Equal,
    /// An ordering where a compared key is a parent to another.
    Parent,
    /// An ordering where a compared key is a child to another.
    Child,
    /// An ordering where a compared key share a common prefix and the divergent part is less than another.
    Less,
    /// An ordering where a compared key share a common prefix and the divergent part is greater than another.
    Greater,
}

impl AsRef<StdOrdering> for Ordering {
    fn as_ref(&self) -> &StdOrdering {
        match self {
            Ordering::Equal => &StdOrdering::Equal,
            Ordering::Parent => &StdOrdering::Less,
            Ordering::Child => &StdOrdering::Greater,
            Ordering::Less => &StdOrdering::Less,
            Ordering::Greater => &StdOrdering::Greater,
        }
    }
}

impl From<Ordering> for StdOrdering {
    fn from(value: Ordering) -> StdOrdering {
        match value {
            Ordering::Equal => StdOrdering::Equal,
            Ordering::Parent => StdOrdering::Less,
            Ordering::Child => StdOrdering::Greater,
            Ordering::Less => StdOrdering::Less,
            Ordering::Greater => StdOrdering::Greater,
        }
    }
}

/// Matches a single key part
pub trait MatchAtom<K>: Send + Sync {
    /// return true if the atom matches
    fn match_atom(&self, atom: &K) -> bool;
}

/// Trait that matches keys
pub trait Match<K>: Send + Sync {
    /// Returns true if self matches the given [Key]
    fn match_key(&self, key: &Key<K>) -> bool;

    /// Returns true if self can match a children of the given [Key]
    fn match_children(&self, key: &Key<K>) -> bool;
}

impl<K> MatchAtom<K> for () {
    fn match_atom(&self, _: &K) -> bool {
        true
    }
}

impl<K> Match<K> for () {
    fn match_key(&self, _: &Key<K>) -> bool {
        true
    }

    fn match_children(&self, _: &Key<K>) -> bool {
        true
    }
}

impl<K> Match<K> for Range<Key<K>>
where
    K: Ord + Send + Sync,
{
    fn match_key(&self, key: &Key<K>) -> bool {
        &self.start <= key && &self.end > key
    }

    fn match_children(&self, key: &Key<K>) -> bool {
        self.match_key(key)
    }
}

impl<K> Match<K> for RangeFrom<Key<K>>
where
    K: Ord + Send + Sync,
{
    fn match_key(&self, key: &Key<K>) -> bool {
        &self.start <= key
    }

    fn match_children(&self, key: &Key<K>) -> bool {
        self.match_key(key)
    }
}

impl<K> Match<K> for RangeFull {
    fn match_key(&self, _: &Key<K>) -> bool {
        true
    }

    fn match_children(&self, _: &Key<K>) -> bool {
        true
    }
}

impl<K> Match<K> for RangeInclusive<Key<K>>
where
    K: Ord + Send + Sync,
{
    fn match_key(&self, key: &Key<K>) -> bool {
        self.start() <= key && self.end() >= key
    }

    fn match_children(&self, key: &Key<K>) -> bool {
        self.match_key(key)
    }
}

impl<K> Match<K> for RangeTo<Key<K>>
where
    K: Ord + Send + Sync,
{
    fn match_key(&self, key: &Key<K>) -> bool {
        &self.end > key
    }

    fn match_children(&self, key: &Key<K>) -> bool {
        self.match_key(key)
    }
}

impl<K> Match<K> for RangeToInclusive<Key<K>>
where
    K: Ord + Send + Sync,
{
    fn match_key(&self, key: &Key<K>) -> bool {
        &self.end >= key
    }

    fn match_children(&self, key: &Key<K>) -> bool {
        self.match_key(key)
    }
}

/// Hierarchic key
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Key<K>(Box<[K]>);

impl<K> Key<K> {
    /// new key with 1 element
    #[inline]
    pub fn new(k: K) -> Self {
        Key([k].into())
    }

    /// return true if this key contains no elements
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// number if elements in this key
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// reverse the order of elements in this key
    #[inline]
    pub fn reverse(&mut self) {
        self.0.reverse()
    }
}

impl<K> Key<K>
where
    K: Ord,
{
    /// Compare 2 keys and return the [Ordering] relative to self
    ///
    /// Example:
    /// ```rust
    /// # use kvtree::{Key, key};
    /// let k1 = Key::new("a");
    /// let k2 = Key::from(["a", "b"]);
    ///
    /// assert_eq!(k1.key_cmp(&k2), key::Ordering::Parent);
    /// assert_eq!(k2.key_cmp(&k1), key::Ordering::Child);
    /// assert_eq!(k2.key_cmp(&["a", "b"].into()), key::Ordering::Equal);
    /// assert_eq!(k2.key_cmp(&["a", "a"].into()), key::Ordering::Greater);
    /// assert_eq!(k1.key_cmp(&["b"].into()), key::Ordering::Less);
    /// ```
    #[inline]
    pub fn key_cmp(&self, other: &Self) -> Ordering {
        let nb = self.0.len().min(other.0.len());
        match self
            .0
            .iter()
            .zip(other.0.iter())
            .take(nb)
            .enumerate()
            .try_fold(None, |x, (i, (l, r))| {
                if l == r {
                    ControlFlow::Continue(Some(i + 1))
                } else {
                    ControlFlow::Break(x)
                }
            }) {
            ControlFlow::Break(Some(i)) | ControlFlow::Continue(Some(i)) => {
                if i == self.len() && i == other.len() {
                    Ordering::Equal
                } else if i == self.len() {
                    Ordering::Parent
                } else if i == other.len() {
                    Ordering::Child
                } else {
                    match self[i].cmp(&other[i]) {
                        StdOrdering::Less => Ordering::Less,
                        StdOrdering::Greater => Ordering::Greater,
                        StdOrdering::Equal => unreachable!(),
                    }
                }
            }
            _ => match self.0.first().cmp(&other.0.first()) {
                StdOrdering::Less => Ordering::Less,
                StdOrdering::Equal => Ordering::Equal,
                StdOrdering::Greater => Ordering::Greater,
            },
        }
    }

    // TODO: see if it's usefull to implement is_parent_of, is_children_of, etc...
}

impl<K: Clone> Key<K> {
    /// creates a new key by cloning and concatenating other after self
    ///
    /// Example:
    /// ```rust
    /// # use kvtree::Key;
    /// let k = Key::new("abc");
    /// assert_eq!(k.join(&["def", "ghi"].into()), Key::from(["abc", "def", "ghi"]));
    /// ```
    pub fn join(&self, other: &Self) -> Self {
        Self(self.into_iter().chain(other).cloned().collect())
    }
}

impl<K: ToString> Key<K> {
    /// create a string that concatenates all members of a key and separate them with `sep`
    ///
    /// Example:
    /// ```rust
    /// # use kvtree::Key;
    /// assert_eq!(Key::new(1234).to_string("/"), "1234");
    /// assert_eq!(Key::<i32>::from([1234, 5678]).to_string("/"), "1234/5678");
    /// assert_eq!(Key::<&str>::from(["foo", "bar"]).to_string("."), "foo.bar");
    /// ```
    pub fn to_string(&self, sep: &str) -> String {
        let mut out = String::new();
        let len = self.len();
        if !self.is_empty() {
            for i in 0..(len - 1) {
                out.push_str(&self[i].to_string());
                out.push_str(sep);
            }
            out.push_str(&self[len - 1].to_string());
        }
        out
    }
}

impl<K> From<Vec<K>> for Key<K> {
    #[inline]
    fn from(value: Vec<K>) -> Self {
        Key(value.into_iter().collect())
    }
}

impl<K, const N: usize> From<[K; N]> for Key<K> {
    #[inline]
    fn from(value: [K; N]) -> Self {
        Key(value.into_iter().collect())
    }
}

impl<K> From<&[K]> for Key<K>
where
    K: Clone,
{
    #[inline]
    fn from(value: &[K]) -> Self {
        Key(value.iter().cloned().collect())
    }
}

impl<K> FromIterator<Key<K>> for Key<K> {
    fn from_iter<T: IntoIterator<Item = Key<K>>>(iter: T) -> Self {
        Key(iter.into_iter().flatten().collect())
    }
}

impl<K> FromIterator<K> for Key<K> {
    fn from_iter<T: IntoIterator<Item = K>>(iter: T) -> Self {
        Key(iter.into_iter().collect())
    }
}

impl<K> Add<K> for Key<K> {
    type Output = Key<K>;

    fn add(self, rhs: K) -> Self::Output {
        Key(Vec::from(self.0)
            .into_iter()
            .chain([rhs])
            .collect::<Box<[K]>>())
    }
}

impl<K> Add<Key<K>> for Key<K> {
    type Output = Key<K>;

    fn add(self, rhs: Key<K>) -> Self::Output {
        Key(Vec::from(self.0)
            .into_iter()
            .chain(Vec::from(rhs.0))
            .collect::<Box<[K]>>())
    }
}

impl<K, C> TryFrom<Mask<K, C>> for Key<K> {
    type Error = Mask<K, C>;

    fn try_from(value: Mask<K, C>) -> Result<Self, Self::Error> {
        let mut r = Vec::with_capacity(value.len());
        let mut it = value.into_iter();
        while let Some(x) = it.next() {
            match x {
                Atom::Key(x) => r.push(x),
                x @ (Atom::Any | Atom::Many | Atom::Other(_)) => {
                    return Err(r
                        .into_iter()
                        .map(Atom::Key)
                        .chain(std::iter::once(x))
                        .chain(it)
                        .collect())
                }
            }
        }
        Ok(r.into())
    }
}

impl<K> AsRef<[K]> for Key<K> {
    #[inline]
    fn as_ref(&self) -> &[K] {
        &self.0
    }
}

impl<K> Borrow<[K]> for Key<K> {
    #[inline]
    fn borrow(&self) -> &[K] {
        self.0.borrow()
    }
}

impl<K, I> Index<I> for Key<K>
where
    I: SliceIndex<[K]>,
{
    type Output = <I as SliceIndex<[K]>>::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        self.0.index(index)
    }
}

impl<'a, K> IntoIterator for &'a Key<K> {
    type Item = <&'a [K] as IntoIterator>::Item;
    type IntoIter = <&'a [K] as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<K> IntoIterator for Key<K> {
    type Item = <Vec<K> as IntoIterator>::Item;
    type IntoIter = <Vec<K> as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        Vec::from(self.0).into_iter()
    }
}

impl<K: Ord> PartialOrd for Key<K> {
    fn partial_cmp(&self, other: &Self) -> Option<StdOrdering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord> Ord for Key<K> {
    fn cmp(&self, other: &Self) -> StdOrdering {
        self.key_cmp(other).into()
    }
}

impl<K> Match<K> for Key<K>
where
    K: Ord + Send + Sync,
{
    fn match_key(&self, key: &Key<K>) -> bool {
        self == key
    }

    fn match_children(&self, key: &Key<K>) -> bool {
        matches!(self.key_cmp(key), Ordering::Child)
    }
}

#[cfg(test)]
mod tests {
    use super::Key;
    use crate::key::Ordering;

    #[test]
    fn test_key_cmp() {
        let k: Key<&'static str> = Key(Box::new([]));
        assert_eq!(k.key_cmp(&Key(Box::new([]))), Ordering::Equal);
        assert_eq!(k.key_cmp(&Key::from(["a"])), Ordering::Less);
        assert_eq!(Key::from(["a"]).key_cmp(&k), Ordering::Greater);
        assert_eq!(
            Key::from(["boo"]).key_cmp(&Key::from(["foo"])),
            Ordering::Less
        );
        assert_eq!(
            Key::from(["foo"]).key_cmp(&Key::from(["boo"])),
            Ordering::Greater
        );
        assert_eq!(Key::from(["a"]).key_cmp(&Key::from(["a"])), Ordering::Equal);
        assert_eq!(
            Key::from(["a"]).key_cmp(&Key::from(["a", "b"])),
            Ordering::Parent
        );
        assert_eq!(
            Key::from(["a", "b"]).key_cmp(&Key::from(["a"])),
            Ordering::Child
        );
        assert_eq!(
            Key::from(["a", "b"]).key_cmp(&Key::from(["a", "c"])),
            Ordering::Less
        );
        assert_eq!(
            Key::from(["a", "c"]).key_cmp(&Key::from(["a", "b"])),
            Ordering::Greater
        );
        assert_eq!(
            Key::from(["a", "c", "a"]).key_cmp(&Key::from(["a", "b", "a"])),
            Ordering::Greater
        );
        assert_eq!(
            Key::from(["a", "b", "a"]).key_cmp(&Key::from(["a", "c", "a"])),
            Ordering::Less
        );
        assert_eq!(
            Key::from(["with"]).key_cmp(&Key::from(["uuid", "foo"])),
            Ordering::Greater
        );
        assert_eq!(
            Key::from(["uuid"]).key_cmp(&Key::from(["with"])),
            Ordering::Less
        );
        assert_eq!(
            Key::from(["a", "b"]).key_cmp(&Key::from(["b"])),
            Ordering::Less
        )
    }
}
