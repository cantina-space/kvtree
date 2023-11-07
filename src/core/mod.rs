//! Stacked heterogenous storage module

mod iter;

pub use self::iter::*;
use crate::{
    key,
    query::QueryMaskIter,
    store::UpdateOpt,
    table::{Row, RowLike},
    Filter, Key, Mask, Query, Store,
};
use ahash::AHashSet;
use rayon::prelude::ParallelIterator;
use std::{
    cell::RefCell,
    hash::Hash,
    ops::ControlFlow,
    rc::Rc,
    sync::{Arc, Mutex},
};

/// Access mode: sets how a key/group of keys of a parent layer can be accessed
#[derive(Debug, Clone, Copy)]
pub enum Access {
    /// Copy on write
    Cow,
    /// Disables access to parents
    New,
    /// Read/write to the parent if possible
    Ref,
    /// Read only
    RO,
    /// deny access read & write
    Deny,
}

impl Access {
    /// give the most restrictive of the 2 access
    pub fn reduce(self, other: Access) -> Access {
        match (self, other) {
            (_, Access::Deny) => Access::Deny,
            (Access::Deny, _) => Access::Deny,
            (_, Access::RO) => Access::RO,
            (Access::RO, _) => Access::RO,
            (Access::New, _) => Access::New,
            (_, Access::New) => Access::New,
            (Access::Cow, _) => Access::Cow,
            (_, Access::Cow) => Access::Cow,
            (Access::Ref, Access::Ref) => Access::Ref,
        }
    }

    /// return true if this mode allows to read
    pub fn is_readable(&self) -> bool {
        !matches!(self, Access::Deny)
    }

    /// return true if this mode allows to write
    pub fn is_writable(&self) -> bool {
        !matches!(self, Access::RO | Access::Deny)
    }

    /// return true if this mode is a [Access::Ref]
    pub fn is_ref(&self) -> bool {
        !matches!(self, Access::Ref)
    }
}

/// Container of [Key]s, [Mask]s.
///
/// [Match][crate::Match] is implemented fot this container, the results depend on all inner items
#[derive(Debug, Clone)]
pub struct KeyMaskSet<K, C> {
    keys: AHashSet<Key<K>>,
    masks: AHashSet<Mask<K, C>>,
}

impl<K, C> Default for KeyMaskSet<K, C> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
            masks: Default::default(),
        }
    }
}

impl<K, C> KeyMaskSet<K, C>
where
    K: Hash + Eq,
{
    /// add a [Key] in this set
    pub fn add_key(&mut self, key: Key<K>)
    where
        K: Hash + Eq,
    {
        self.keys.insert(key);
    }

    /// remove a [Key] in this set
    pub fn remove_key(&mut self, key: &Key<K>) -> Option<Key<K>>
    where
        K: Hash + Eq,
    {
        self.keys.take(key)
    }
}

impl<K, C> KeyMaskSet<K, C>
where
    K: Hash + Eq,
    C: Hash + Eq + key::MatchAtom<K>,
{
    /// add a [Mask] in this set
    pub fn add_mask(&mut self, mask: Mask<K, C>) {
        self.masks.insert(mask);
    }

    /// remove a [Mask] in this set
    pub fn remove_mask(&mut self, mask: &Mask<K, C>) -> Option<Mask<K, C>> {
        self.masks.take(mask)
    }
}

impl<K, C> key::Match<K> for KeyMaskSet<K, C>
where
    K: Hash + Ord + Send + Sync,
    C: key::MatchAtom<K>,
{
    fn match_key(&self, key: &Key<K>) -> bool {
        self.keys.contains(key) || self.masks.iter().any(|x| x.match_key(key))
    }

    fn match_children(&self, key: &Key<K>) -> bool {
        self.keys.iter().any(|x| x.match_children(key))
            || self.masks.iter().any(|x| x.match_children(key))
    }
}

/// Key/Mask access
#[derive(Debug, Clone)]
pub struct LayerAccess<K, C> {
    cows: KeyMaskSet<K, C>,
    news: KeyMaskSet<K, C>,
    refs: KeyMaskSet<K, C>,
    ros: KeyMaskSet<K, C>,
    denys: KeyMaskSet<K, C>,
}

impl<K, C> Default for LayerAccess<K, C> {
    fn default() -> Self {
        Self {
            cows: Default::default(),
            news: Default::default(),
            refs: Default::default(),
            ros: Default::default(),
            denys: Default::default(),
        }
    }
}

impl<K, C> LayerAccess<K, C>
where
    K: Hash + Ord + Send + Sync,
    C: key::MatchAtom<K>,
{
    /// get the access mode associated to this key
    pub fn mode(&self, key: &Key<K>) -> Option<Access> {
        use key::Match;
        match (
            self.cows.match_key(key),
            self.news.match_key(key),
            self.refs.match_key(key),
            self.ros.match_key(key),
            self.denys.match_key(key),
        ) {
            (.., true) => Some(Access::Deny),
            (_, _, _, true, _) => Some(Access::RO),
            (_, true, ..) => Some(Access::New),
            (true, ..) => Some(Access::Cow),
            (_, _, true, ..) => Some(Access::Ref),
            (..) => None,
        }
    }
}

impl<K, C> LayerAccess<K, C>
where
    K: Eq + Hash,
{
    /// add a [Key] in this set
    pub fn add_key(&mut self, key: Key<K>, mode: Access) {
        match mode {
            Access::Cow => self.cows.add_key(key),
            Access::New => self.news.add_key(key),
            Access::Ref => self.refs.add_key(key),
            Access::RO => self.ros.add_key(key),
            Access::Deny => self.denys.add_key(key),
        };
    }

    /// remove a [Key] in this set
    pub fn remove_key(&mut self, key: &Key<K>, modes: &[Access]) -> Option<Key<K>> {
        let mut last = None;
        for mode in modes {
            last = match mode {
                Access::Cow => self.cows.remove_key(key),
                Access::New => self.news.remove_key(key),
                Access::Ref => self.refs.remove_key(key),
                Access::RO => self.ros.remove_key(key),
                Access::Deny => self.denys.remove_key(key),
            };
        }
        last
    }
}
impl<K, C> LayerAccess<K, C>
where
    K: Eq + Hash,
    C: Eq + Hash + key::MatchAtom<K>,
{
    /// add a [Mask] in this set
    pub fn add_mask(&mut self, mask: Mask<K, C>, mode: Access) {
        match mode {
            Access::Cow => self.cows.add_mask(mask),
            Access::New => self.news.add_mask(mask),
            Access::Ref => self.refs.add_mask(mask),
            Access::RO => self.ros.add_mask(mask),
            Access::Deny => self.denys.add_mask(mask),
        };
    }

    /// remove a [Mask] in this set
    pub fn remove_mask(&mut self, mask: &Mask<K, C>, modes: &[Access]) -> Option<Mask<K, C>> {
        let mut last = None;
        for mode in modes {
            last = match mode {
                Access::Cow => self.cows.remove_mask(mask),
                Access::New => self.news.remove_mask(mask),
                Access::Ref => self.refs.remove_mask(mask),
                Access::RO => self.ros.remove_mask(mask),
                Access::Deny => self.denys.remove_mask(mask),
            };
        }
        last
    }
}

pub(in crate::core) fn key_access_at<K, C>(
    access: &[LayerAccess<K, C>],
    key: &Key<K>,
    index: usize,
) -> Option<Access>
where
    K: Hash + Ord + Send + Sync,
    C: key::MatchAtom<K>,
{
    access[index..]
        .iter()
        .filter_map(|x| x.mode(key))
        .reduce(Access::reduce)
}

pub(in crate::core) fn is_readable_at<K, C>(
    access: &[LayerAccess<K, C>],
    key: &Key<K>,
    index: usize,
) -> Option<bool>
where
    K: Hash + Ord + Send + Sync,
    C: key::MatchAtom<K>,
{
    key_access_at(access, key, index).map(|x| x.is_readable())
}

pub(in crate::core) fn is_writable_at<K, C>(
    access: &[LayerAccess<K, C>],
    key: &Key<K>,
    index: usize,
) -> Option<bool>
where
    K: Hash + Ord + Send + Sync,
    C: key::MatchAtom<K>,
{
    key_access_at(access, key, index).map(|x| x.is_writable())
}

/// Core storage
///
/// A stack of [`Store`]s, llowing each store to read, modify or shadow the entries of its predecessor according to its
/// [`Access`] permissions.
#[derive(Debug)]
pub struct Core<K, C = ()> {
    stores: Vec<Store<K>>,
    access: Vec<LayerAccess<K, C>>,
}

impl<K: Hash + Ord, C> Core<K, C> {
    /// build an index for all [Store]s in this core
    pub fn build_index(&mut self) {
        self.stores.iter_mut().for_each(|x| x.build_index())
    }
}

impl<K: Hash + Ord + Send + Sync, C> Core<K, C> {
    #[cfg(feature = "rayon")]
    /// build an index for all tables in this store in parallel
    pub fn par_build_index(&mut self) {
        use rayon::prelude::IntoParallelRefMutIterator;

        self.stores.par_iter_mut().for_each(|x| x.par_build_index())
    }
}

impl<K, C> Core<K, C>
where
    K: Hash + Ord + Send + Sync,
    C: key::MatchAtom<K>,
{
    fn write_layer(&self, key: &Key<K>) -> Option<usize> {
        let sidx = self
            .stores
            .iter()
            .enumerate()
            .rev()
            .find_map(|(i, x)| if x.contains_key(key) { Some(i) } else { None })
            .unwrap_or(0);
        match self
            .access
            .iter()
            .skip(sidx)
            .enumerate()
            .rev()
            .try_fold(None, |acc, (i, x)| match x.mode(key) {
                Some(x) => match x {
                    Access::Cow | Access::New => ControlFlow::Break(Some(i + sidx)),
                    Access::Ref => ControlFlow::Continue(Some(i + sidx)),
                    Access::RO | Access::Deny => ControlFlow::Break(acc),
                },
                None => ControlFlow::Continue(Some(i + sidx)),
            }) {
            ControlFlow::Break(Some(i)) | ControlFlow::Continue(Some(i)) => Some(i),
            _ => None,
        }
    }

    fn read_layer(&self, key: &Key<K>) -> Option<usize> {
        let sidx = self
            .stores
            .iter()
            .enumerate()
            .rev()
            .find_map(|(i, x)| if x.contains_key(key) { Some(i) } else { None })
            .unwrap_or(0);
        match self
            .access
            .iter()
            .enumerate()
            .skip(sidx)
            .rev()
            .try_fold(None, |acc, (i, x)| match x.mode(key) {
                Some(x) => match x {
                    Access::Cow | Access::RO | Access::Ref => ControlFlow::Continue(Some(i)),
                    Access::New => ControlFlow::Break(Some(i)),
                    Access::Deny => ControlFlow::Break(acc),
                },
                None => ControlFlow::Continue(Some(i)),
            }) {
            ControlFlow::Break(Some(i)) | ControlFlow::Continue(Some(i)) => Some(i),
            _ => None,
        }
    }
}

impl<K, C> Default for Core<K, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, C> Core<K, C> {
    /// creates a new core
    pub fn new() -> Self {
        Self {
            stores: Default::default(),
            access: Default::default(),
        }
    }

    /// return true is all [Store]s in this core are indexed
    pub fn is_indexed(&self) -> bool {
        self.stores.iter().all(|x| x.is_indexed())
    }

    fn init_stack(&mut self) {
        if self.stores.is_empty() {
            self.stores.push(Store::default());
            self.access.push(LayerAccess::default());
        }
    }

    /// number of layers in this store
    pub fn len_layers(&self) -> usize {
        self.stores.len()
    }

    /// number of layers in this store
    pub fn push_layer(&mut self) {
        self.stores.push(Store::default());
        self.access.push(LayerAccess::default());
    }

    /// add a new layer with the given [Access]
    pub fn push_layer_access(&mut self, access: LayerAccess<K, C>) {
        self.access.push(access);
        self.stores.push(Store::default());
    }

    /// add a new layer with the given [Store]
    pub fn push_layer_store(&mut self, store: Store<K>) {
        self.stores.push(store);
        self.access.push(LayerAccess::default());
    }

    /// add a new layer with the given [Store] and [Access]
    pub fn push_layer_store_access(&mut self, store: Store<K>, access: LayerAccess<K, C>) {
        self.stores.push(store);
        self.access.push(access);
    }

    /// remove the current layer
    pub fn pop_layer(&mut self) -> bool {
        self.stores.pop();
        self.access.pop().is_some()
    }

    /// remove the current layer and return it's [Access]
    pub fn pop_layer_access(&mut self) -> Option<LayerAccess<K, C>> {
        self.stores.pop();
        self.access.pop()
    }

    /// remove the current layer and return it's [Store]
    pub fn pop_layer_store(&mut self) -> Option<Store<K>> {
        self.access.pop();
        self.stores.pop()
    }

    /// remove the current layer and return it's [Store] and [Access]
    pub fn pop_layer_store_access(&mut self) -> Option<(Store<K>, LayerAccess<K, C>)> {
        use tuplify::ValidateOpt;
        (self.stores.pop(), self.access.pop()).validate()
    }
}

impl<K, C> Core<K, C>
where
    K: Hash + Ord + Send + Sync,
    C: key::MatchAtom<K>,
{
    /// returns true if this core contains the given key
    pub fn contains_key(&self, key: &Key<K>) -> bool {
        self.contains_key_filter::<()>(key)
    }

    /// returns true if this core contains the given key if it matches the given [Filter]
    pub fn contains_key_filter<F: Filter>(&self, key: &Key<K>) -> bool {
        self.get_filter::<(), F>(key).is_some()
    }

    /// Get a key that matches the given [Query]
    pub fn get<'a, Q>(&'a self, key: &Key<K>) -> Option<Q::Item<'a>>
    where
        Q: Query,
    {
        self.get_filter::<Q, ()>(key)
    }

    /// Get a key that matches the given [Query] and [Filter]
    pub fn get_filter<'a, Q, F>(&'a self, key: &Key<K>) -> Option<Q::Item<'a>>
    where
        Q: Query,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        self.read_layer(key)
            .and_then(|i| self.stores[i].get_filter::<Q, F>(key))
    }

    /// Get a key that matches the given [Query] where the query can get mutable items
    pub fn get_mut<'a, Q>(&'a mut self, key: &Key<K>) -> Option<Q::Item<'a>>
    where
        Q: Query,
    {
        self.get_mut_filter::<Q, ()>(key)
    }

    /// Get a key that matches the given [Query] and [Filter] where the query can get mutable items
    pub fn get_mut_filter<'a, Q, F>(&'a mut self, key: &Key<K>) -> Option<Q::Item<'a>>
    where
        Q: Query,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        self.write_layer(key)
            .and_then(|i| self.stores[i].get_mut_filter::<Q, F>(key))
    }

    /// Get an iterator over key/values that matches the given [Query]
    pub fn iter<Q: Query + 'static>(&self) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)> {
        self.iter_filter::<Q, ()>()
    }

    /// Get an iterator over key/values that matches the given [Query] and [Filter]
    pub fn iter_filter<Q: Query + 'static, F: Filter>(
        &self,
    ) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)> {
        assert!(Q::READ_ONLY);
        let access = &self.access;
        let seen = Rc::new(RefCell::new(AHashSet::new()));
        self.stores.iter().enumerate().flat_map(move |(i, x)| {
            let seen = seen.clone();
            x.iter_filter::<Q, F>().filter_map(move |(x, y)| {
                if is_readable_at(access, x, i).unwrap_or(true) && seen.borrow_mut().insert(x) {
                    Some((x, y))
                } else {
                    None
                }
            })
        })
    }

    /// Get an ordered iterator over key/values that matches the given [Query]
    pub fn iter_indexed<Q>(&self) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
    {
        self.iter_indexed_filter::<Q, ()>()
    }

    /// Get an ordered iterator over key/values that matches the given [Query] and [Filter]
    pub fn iter_indexed_filter<Q, F>(&self) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        Iter::new(
            self.stores.iter().map(|x| x.iter_indexed_filter::<Q, F>()),
            &self.access,
        )
    }

    /// Get a parallel iterator over key/values that matches the given [Query]
    pub fn par_iter<Q>(
        &mut self,
    ) -> impl Iterator<Item = impl ParallelIterator<Item = (&Key<K>, Q::Item<'_>)>>
    where
        Q: Query + 'static,
        for<'a> Q::Item<'a>: Send + Sync,
    {
        self.par_iter_filter::<Q, ()>()
    }

    /// Get an iterator over key/values that matches the given [Query] and [Filter]
    pub fn par_iter_filter<Q, F>(
        &mut self,
    ) -> impl Iterator<Item = impl ParallelIterator<Item = (&Key<K>, Q::Item<'_>)>>
    where
        for<'a> Q::Item<'a>: Send + Sync,
        Q: Query + 'static,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        let access = &self.access;
        let seen = Arc::new(Mutex::new(AHashSet::new()));
        self.stores.iter().enumerate().map(move |(i, x)| {
            let seen = seen.clone();
            x.par_iter_filter::<Q, F>().filter_map(move |(x, y)| {
                if is_readable_at(access, x, i).unwrap_or(true) && seen.lock().unwrap().insert(x) {
                    Some((x, y))
                } else {
                    None
                }
            })
        })
    }

    /// Get an ordered iterator over key/values that matches the given [Match][key::Match<K>], [Query]
    pub fn mask<'a, 'b, Q>(
        &'a self,
        mask: &'b impl key::Match<K>,
    ) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'a>)> + 'a
    where
        'b: 'a,
        Q: Query + 'static,
    {
        self.mask_filter::<Q, ()>(mask)
    }

    /// Get an ordered iterator over key/values that matches the given [Match][key::Match<K>], [Query] and [Filter]
    pub fn mask_filter<'a, 'b, Q, F>(
        &'a self,
        mask: &'b impl key::Match<K>,
    ) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'a>)> + 'a
    where
        'b: 'a,
        Q: Query + 'static,
        F: Filter + 'a,
    {
        assert!(Q::READ_ONLY);
        Iter::new(
            self.stores.iter().map(|x| x.mask_filter::<Q, F>(mask)),
            &self.access,
        )
    }

    /// Get a mutable iterator over key/values that matches the given [Query]
    pub fn iter_mut<Q>(&mut self) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
    {
        self.iter_mut_filter::<Q, ()>()
    }

    /// Get a mutable iterator over key/values that matches the given [Query] and [Filter]
    pub fn iter_mut_filter<Q, F>(&mut self) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        let access = &self.access;
        let seen = Rc::new(RefCell::new(AHashSet::new()));
        self.stores.iter_mut().enumerate().flat_map(move |(i, x)| {
            let seen = seen.clone();
            x.iter_mut_filter::<Q, F>().filter_map(move |(x, y)| {
                if is_writable_at(access, x, i).unwrap_or(true) && seen.borrow_mut().insert(x) {
                    Some((x, y))
                } else {
                    None
                }
            })
        })
    }

    /// Get an ordered mutable iterator over key/values that matches the given [Query]
    pub fn iter_mut_indexed<Q: Query + 'static>(
        &mut self,
    ) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)> {
        self.iter_mut_indexed_filter::<Q, ()>()
    }

    /// Get an ordered mutable iterator over key/values that matches the given [Query] and [Filter]
    pub fn iter_mut_indexed_filter<Q: Query + 'static, F: Filter>(
        &mut self,
    ) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)> {
        assert!(Q::READ_ONLY);
        IterMut::new(
            self.stores
                .iter_mut()
                .map(|x| x.iter_mut_indexed_filter::<Q, F>()),
            &self.access,
        )
    }

    /// Get an iterator over key/values that matches the given [Query]
    pub fn par_iter_mut<Q>(
        &mut self,
    ) -> impl Iterator<Item = impl ParallelIterator<Item = (&Key<K>, Q::Item<'_>)>>
    where
        Q: Query + 'static,
        for<'a> Q::Item<'a>: Send + Sync,
    {
        self.par_iter_mut_filter::<Q, ()>()
    }

    /// for consistency each parallel iterator should be consumed in order
    pub fn par_iter_mut_filter<Q, F>(
        &mut self,
    ) -> impl Iterator<Item = impl ParallelIterator<Item = (&Key<K>, Q::Item<'_>)>>
    where
        for<'a> Q::Item<'a>: Send + Sync,
        Q: Query + 'static,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        let access = &self.access;
        let seen = Arc::new(Mutex::new(AHashSet::new()));
        self.stores.iter_mut().enumerate().map(move |(i, x)| {
            let seen = seen.clone();
            x.par_iter_mut_filter::<Q, F>().filter_map(move |(x, y)| {
                if is_writable_at(access, x, i).unwrap_or(true) && seen.lock().unwrap().insert(x) {
                    Some((x, y))
                } else {
                    None
                }
            })
        })
    }

    /// Get an ordered mutable iterator over key/values that matches the given [Match][key::Match<K>] and [Query]
    pub fn mask_mut<'a, 'b, Q>(
        &'a mut self,
        mask: &'b impl key::Match<K>,
    ) -> impl QueryMaskIter<Item = (&'a Key<K>, Q::Item<'a>)>
    where
        'b: 'a,
        Q: Query + 'static,
    {
        self.mask_mut_filter::<Q, ()>(mask)
    }

    /// Get an ordered mutable iterator over key/values that matches the given [Match][key::Match<K>], [Query] and [Filter]
    pub fn mask_mut_filter<'a, 'b, Q, F>(
        &'a mut self,
        mask: &'b impl key::Match<K>,
    ) -> impl QueryMaskIter<Item = (&'a Key<K>, Q::Item<'a>)>
    where
        'b: 'a,
        Q: Query + 'static,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        Iter::new(
            self.stores
                .iter_mut()
                .map(|x| x.mask_mut_filter::<Q, F>(mask)),
            &self.access,
        )
    }

    /// inserts the given key value if the key is not already present
    ///
    /// Returns the provided key value if write is not possible or if an entry with the same key exists
    pub fn insert<V: RowLike>(&mut self, key: Key<K>, value: V) -> Result<(), (Key<K>, V)> {
        self.init_stack();
        if let Some(i) = self.write_layer(&key) {
            self.stores[i].insert(key, value)
        } else {
            Err((key, value))
        }
    }

    /// inserts the given key value if the key is not already present
    ///
    /// Returns the provided key value if write is not possible or if an entry with the same key exists
    pub fn insert_row(&mut self, key: Key<K>, row: Row) -> Result<(), (Key<K>, Row)> {
        self.init_stack();
        if let Some(i) = self.write_layer(&key) {
            self.stores[i].insert_row(key, row)
        } else {
            Err((key, row))
        }
    }

    /// update the given key with value
    ///
    /// This update can be partial
    ///
    /// Returns the provided key value if write is not possible or if an entry with the same key exists
    pub fn update<V: RowLike>(
        &mut self,
        key: &Key<K>,
        value: V,
        opts: UpdateOpt,
    ) -> Result<Row, V> {
        if let Some(i) = self.write_layer(key) {
            self.stores[i].update(key, value, opts)
        } else {
            Err(value)
        }
    }

    /// update the given key with value
    ///
    /// This update can be partial
    ///
    /// Returns the provided key value if write is not possible or if an entry with the same key exists
    pub fn update_row(&mut self, key: &Key<K>, row: Row, opts: UpdateOpt) -> Result<Row, Row> {
        if let Some(i) = self.write_layer(key) {
            self.stores[i].update_row(key, row, opts)
        } else {
            Err(row)
        }
    }

    /// update or insert the given key with value
    ///
    /// if the key already exists this function behaves like [`Core::update`]
    pub fn upsert<V: RowLike>(
        &mut self,
        key: Key<K>,
        value: V,
        opts: UpdateOpt,
    ) -> Result<Option<Row>, (Key<K>, V)> {
        self.init_stack();
        if let Some(i) = self.write_layer(&key) {
            self.stores[i].upsert(key, value, opts)
        } else {
            Err((key, value))
        }
    }

    /// update or insert the given key with value
    ///
    /// if the key already exists this function behaves like [`Core::update_row`]
    pub fn upsert_row(
        &mut self,
        key: Key<K>,
        row: Row,
        opts: UpdateOpt,
    ) -> Result<Option<Row>, (Key<K>, Row)> {
        self.init_stack();
        if let Some(i) = self.write_layer(&key) {
            self.stores[i].upsert_row(key, row, opts)
        } else {
            Err((key, row))
        }
    }
}

impl<K, C> Core<K, C>
where
    K: Eq + Hash,
{
    /// set [Access] to a [Key]
    pub fn set_key_access(&mut self, key: Key<K>, mode: Access) {
        if let Some(access) = self.access.last_mut() {
            access.add_key(key, mode)
        } else {
            let mut access = LayerAccess::default();
            access.add_key(key, mode);
            self.access.push(access);
            if self.stores.is_empty() {
                self.stores.push(Store::default());
            }
        }
    }

    /// remove [Access] to a [Key]
    pub fn remove_key_access(&mut self, key: &Key<K>, modes: &[Access]) -> Option<Key<K>> {
        if let Some(access) = self.access.last_mut() {
            access.remove_key(key, modes)
        } else {
            None
        }
    }
}

impl<K, C> Core<K, C>
where
    K: Eq + Hash,
    C: Eq + Hash + key::MatchAtom<K>,
{
    /// set [Access] to a [Mask]
    pub fn set_mask_access(&mut self, mask: Mask<K, C>, mode: Access) {
        if let Some(access) = self.access.last_mut() {
            access.add_mask(mask, mode)
        } else {
            let mut access = LayerAccess::default();
            access.add_mask(mask, mode);
            self.access.push(access);
            if self.stores.is_empty() {
                self.stores.push(Store::default());
            }
        }
    }

    /// remove [Access] to a [Mask]
    pub fn remove_mask_access(
        &mut self,
        mask: &Mask<K, C>,
        modes: &[Access],
    ) -> Option<Mask<K, C>> {
        if let Some(access) = self.access.last_mut() {
            access.remove_mask(mask, modes)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {

    use crate::{
        core::{Access, LayerAccess},
        mask,
        store::UpdateOpt,
        Core, Key, Mask,
    };

    #[test]
    fn test_access_single_layer() {
        let mut c = Core::<_, ()>::default();
        let k1 = Key::from(["foo", "bar"]);
        let k2 = Key::from(["foo", "baz"]);
        c.insert(k2.clone(), (22i32,)).unwrap();
        c.set_key_access(k2.clone(), Access::Ref);
        assert_eq!(c.get::<&i32>(&k2), Some(&22));
        *c.get_mut::<&mut i32>(&k2).unwrap() += 1;
        assert_eq!(c.get::<&i32>(&k2), Some(&23));
        c.upsert(k1.clone(), (11i32,), UpdateOpt::Replace).unwrap();
        assert_eq!(c.get::<&i32>(&k1), Some(&11));
        *c.get_mut::<&mut i32>(&k1).unwrap() += 1;
        assert_eq!(c.get::<&i32>(&k1), Some(&12));
        c.set_key_access(k1.clone(), Access::RO);
        assert_eq!(c.get_mut::<&mut i32>(&k1), None);
        assert_eq!(c.get::<&i32>(&k1), Some(&12));
        c.set_key_access(k1.clone(), Access::Deny);
        assert_eq!(c.get::<&i32>(&k1), None);
    }

    #[test]
    fn test_access_multi_layer_simple() {
        let mut c = Core::<_, ()>::default();
        let k1 = Key::from(["foo", "bar"]);
        let k2 = Key::from(["foo", "baz"]);
        let k3 = Key::from(["foo", "boo"]);
        let k4 = Key::from(["foo", "zoo"]);
        c.insert(k1.clone(), (123,)).unwrap();
        c.insert(k2.clone(), (1234,)).unwrap();
        c.insert(k3.clone(), (12345,)).unwrap();
        c.insert(k4.clone(), (123456,)).unwrap();
        let mut access = LayerAccess::default();
        access.add_key(k1.clone(), Access::Cow);
        access.add_key(k2.clone(), Access::New);
        access.add_key(k3.clone(), Access::Ref);
        access.add_key(k4.clone(), Access::RO);
        assert_eq!(c.len_layers(), 1);
        c.push_layer_access(access);
        assert_eq!(c.len_layers(), 2);
        assert_eq!(c.get_mut::<&mut i32>(&k1), None);
        assert_eq!(c.get::<&i32>(&k1), Some(&123));
        let row = c.upsert(k1.clone(), (1123,), UpdateOpt::Update).unwrap();
        assert!(row.map_or(true, |x| x.is_empty()));

        assert_eq!(c.get::<&i32>(&k1), Some(&1123));
        let x = c.upsert(k2.clone(), (1123,), UpdateOpt::Update).unwrap();
        assert!(x.is_none());
        assert_eq!(c.get::<&i32>(&k2), Some(&1123));
        let row = c.upsert(k3.clone(), (112345,), UpdateOpt::Update).unwrap();
        assert_eq!(row.map(|x| x.len()), Some(1));
        let (x, (y,)) = c
            .upsert(k4.clone(), (12334567,), UpdateOpt::Update)
            .unwrap_err();
        assert_eq!(x, k4);
        assert_eq!(y, 12334567);
        assert_eq!(c.get::<&i32>(&k3), Some(&112345));
        assert_eq!(c.get::<&i32>(&k4), Some(&123456));
        let mut access = LayerAccess::default();
        access.add_mask(Mask::new(mask::Atom::Many), Access::Cow);
        c.push_layer_access(access);
        assert_eq!(c.len_layers(), 3);
        let row = c
            .upsert(k4.clone(), (11000, "yolo", 10.0f32), UpdateOpt::Update)
            .unwrap();
        assert!(row.map_or(true, |x| x.is_empty()));
        assert_eq!(c.get::<(&&str, &i32)>(&k4), Some((&"yolo", &11000)));
        assert!(c.get::<(&&str, &i32, &f32)>(&k4).is_some());
        c.pop_layer();
        assert_eq!(c.len_layers(), 2);
        c.pop_layer();
        assert_eq!(c.len_layers(), 1);
        assert_eq!(c.get::<&i32>(&k1), Some(&123));
        assert_eq!(c.get::<&i32>(&k2), Some(&1234));
        assert_eq!(c.get::<&i32>(&k3), Some(&112345));
        assert_eq!(c.get::<&i32>(&k4), Some(&123456));
    }

    #[test]
    fn test_select_option() {
        let mut c: Core<&str, ()> = Core::default();
        _ = c.upsert(Key::new("foo"), (vec![123],), UpdateOpt::Update);
        assert_eq!(
            c.get::<Option<&Vec<i32>>>(&Key::new("foo")),
            Some(Some(&vec![123]))
        );
        assert_eq!(
            c.get::<(Option<&Vec<i32>>, Option<&i32>)>(&Key::new("foo")),
            Some((Some(&vec![123]), None))
        );

        _ = c.upsert(Key::new("foo"), (123i32,), UpdateOpt::Replace);
        assert_eq!(c.get::<Option<&Vec<i32>>>(&Key::new("foo")), Some(None));
        assert_eq!(c.get::<Option<&i32>>(&Key::new("foo")), Some(Some(&123)));
        assert_eq!(
            c.get::<(Option<&Vec<i32>>, Option<&i32>)>(&Key::new("foo")),
            Some((None, Some(&123)))
        );
    }
}
