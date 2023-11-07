//! Heterogenous storage module

mod iter;

pub use self::iter::*;
#[cfg(feature = "rayon")]
use crate::query::ParallelQueryMaskIter;
use crate::{
    key::Ordering,
    query::{Filter, Query, QueryMaskIter},
    table::{Row, RowLike, Table},
    Key, Match,
};
use itertools::Itertools;
#[cfg(feature = "rayon")]
use rayon::prelude::*;

use std::hash::Hash;

/// Root keys with children
pub type RootTree<'a, K> = Vec<(&'a Key<K>, Box<dyn QueryMaskIter<Item = &'a Key<K>> + 'a>)>;

/// Heterogenous storage
///
/// Collection of [Table]s
///
/// Each key in the store is unique
#[derive(Debug)]
pub struct Store<K> {
    tables: Vec<Table<K>>,
}

impl<K> Default for Store<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K> Store<K> {
    /// creates a new empty store
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }

    /// Number of tables in the store
    pub fn len_tables(&self) -> usize {
        self.tables.len()
    }

    /// Returns true if this store is empty
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Number of elements in the store
    pub fn len(&self) -> usize {
        self.tables.iter().map(|x| x.len_rows()).sum()
    }

    /// Return true if all [Table]s in the store are indexed
    pub fn is_indexed(&self) -> bool {
        self.tables.iter().all(|x| x.is_indexed())
    }

    /// returns a table index that matches the given [RowLike]
    ///
    /// see also: [matches][Table::<K>::matches::<V>()]
    fn find_table_by<V: RowLike>(&self, partial: bool) -> Option<usize> {
        self.tables
            .iter()
            .enumerate()
            .find_map(|(i, x)| x.matches::<V>(partial).then_some(i))
    }

    fn find_table_by_row(&self, row: &Row, partial: bool) -> Option<usize> {
        self.tables
            .iter()
            .enumerate()
            .find_map(|(i, x)| x.match_row(row, partial).then_some(i))
    }

    /// Roots keys with children
    ///
    /// Root keys of each [Table] merged into a single tree
    ///
    /// Root keys are sorted, children keys are not
    pub fn roots(&self) -> RootTree<'_, K>
    where
        K: Ord,
    {
        self.tables
            .iter()
            .flat_map(move |x| x.roots())
            .sorted_by(|(x, _), (y, _)| x.key_cmp(y).into())
            .fold(vec![], |mut acc, (x, y)| {
                if let Some((k, it)) = acc.pop() {
                    match k.key_cmp(x) {
                        // keys should be unique & sorted
                        Ordering::Equal | Ordering::Greater => {
                            unreachable!()
                        }
                        Ordering::Parent => acc.push((
                            k,
                            Box::new(it.chain(std::iter::once(x)).chain(y))
                                as Box<dyn QueryMaskIter<Item = &Key<K>>>,
                        )),
                        Ordering::Child => {
                            acc.push((x, Box::new(it.chain(y).chain(std::iter::once(k)))))
                        }
                        Ordering::Less => acc.extend([(k, it), (x, Box::new(y))]),
                    }
                } else {
                    acc.push((x, Box::new(y)));
                }
                acc
            })
    }

    /// returns an iterator over the keys in this store
    pub fn keys(&self) -> impl QueryMaskIter<Item = &Key<K>> {
        self.tables.iter().flat_map(|x| x.keys())
    }

    /// returns an ordered iterator over the keys in this store
    pub fn keys_indexed(&self) -> impl QueryMaskIter<Item = &Key<K>>
    where
        K: Ord,
    {
        OrderedIter(
            self.tables
                .iter()
                .flat_map(|x| {
                    let mut x = x.keys().iter().map(|x| (x, ()));
                    x.next().map(|y| (y, x))
                })
                .collect(),
        )
        .map(|(x, _)| x)
    }

    /// return an iterator over keys/ values that matches the provided [Query]
    pub fn iter<Q>(&self) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
    {
        self.iter_filter::<Q, ()>()
    }

    /// return an iterator over keys/values that matches the provided [Query] and [Filter]
    pub fn iter_filter<Q, F>(&self) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        self.tables.iter().flat_map(|x| x.iter_filter::<Q, F>())
    }

    /// return an iterator over keys/ values that matches the provided [Match] and [Query]
    pub fn mask<'a, 'b, Q>(
        &'a self,
        mask: &'b impl Match<K>,
    ) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'a>)>
    where
        'b: 'a,
        Q: Query + 'static,
    {
        self.mask_filter::<Q, ()>(mask)
    }

    /// return an iterator over keys/ values that matches the provided [Match], [Query] and [Filter]
    pub fn mask_filter<'a, 'b, Q, F>(
        &'a self,
        mask: &'b impl Match<K>,
    ) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'a>)>
    where
        'b: 'a,
        Q: Query + 'static,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        self.tables.iter().flat_map(|x| x.mask_filter::<Q, F>(mask))
    }

    /// return an iterator over keys/values that matches the provided [Query] and [Filter]
    pub fn iter_mut<Q>(&mut self) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
    {
        self.iter_mut_filter::<Q, ()>()
    }

    /// return an iterator over keys/values that matches the provided [Query] and [Filter]
    pub fn iter_mut_filter<Q, F>(&mut self) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        self.tables
            .iter_mut()
            .flat_map(|x| x.iter_mut_filter::<Q, F>())
    }

    /// return an iterator over keys/ values that matches the provided [Match] and [Query]
    pub fn mask_mut<'a, 'b, Q>(
        &'a mut self,
        mask: &'b impl Match<K>,
    ) -> impl QueryMaskIter<Item = (&'a Key<K>, Q::Item<'a>)>
    where
        'b: 'a,
        Q: Query + 'static,
    {
        self.mask_mut_filter::<Q, ()>(mask)
    }

    /// return an iterator over keys/ values that matches the provided [Match], [Query] and [Filter]
    pub fn mask_mut_filter<'a, 'b, Q, F>(
        &'a mut self,
        mask: &'b impl Match<K>,
    ) -> impl QueryMaskIter<Item = (&'a Key<K>, Q::Item<'a>)>
    where
        'b: 'a,
        Q: Query + 'static,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        self.tables
            .iter_mut()
            .flat_map(|x| x.mask_mut_filter::<Q, F>(mask))
    }
}

impl<K: Eq + Hash> Store<K> {
    /// returns true if this store contains the given key
    pub fn contains_key(&self, key: &Key<K>) -> bool {
        self.tables.iter().any(|x| x.contains_key(key))
    }

    fn find_table_by_key(&self, key: &Key<K>) -> Option<usize> {
        self.tables
            .iter()
            .enumerate()
            .find_map(|(i, x)| x.contains_key(key).then_some(i))
    }

    /// Get a value that matches the given key and [Query]
    pub fn get<Q: Query>(&self, key: &Key<K>) -> Option<Q::Item<'_>> {
        self.get_filter::<Q, ()>(key)
    }

    /// Get a key that matches the given [Query] & [Filter]
    pub fn get_filter<Q: Query, F: Filter>(&self, key: &Key<K>) -> Option<Q::Item<'_>> {
        assert!(Q::READ_ONLY);
        self.tables.iter().find_map(|x| x.get_filter::<Q, F>(key))
    }

    /// Get a mutable value that matches `key` and the given [Query]
    pub fn get_mut<Q: Query>(&mut self, key: &Key<K>) -> Option<Q::Item<'_>> {
        self.get_mut_filter::<Q, ()>(key)
    }

    /// Get a mutable value that matches `key` and the given [Query] and [Filter]
    pub fn get_mut_filter<Q: Query, F: Filter>(&mut self, key: &Key<K>) -> Option<Q::Item<'_>> {
        assert!(!Q::READ_ONLY);
        self.tables
            .iter_mut()
            .find_map(|x| x.get_mut_filter::<Q, F>(key))
    }
}

/// Update behaviour
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOpt {
    /// Update existing row do not add new columns
    Patch,
    /// Update/extend existing row (add new columns if needed)
    Update,
    /// Replace existing row
    Replace,
}

impl<K: Hash + Ord> Store<K> {
    /// build an index for all tables in this store
    pub fn build_index(&mut self) {
        self.tables.iter_mut().for_each(|x| x.build_index())
    }

    /// inserts the given key value if the key is not already present
    ///
    /// Returns the provided key value if an entry with the same key exists
    pub fn insert<V: RowLike>(&mut self, key: Key<K>, value: V) -> Result<(), (Key<K>, V)> {
        if !self.contains_key(&key) {
            if let Some(x) = self.tables.iter_mut().find(|x| x.matches::<V>(false)) {
                x.insert(key, value).map_err(|_| unreachable!())
            } else {
                let mut x = Table::<K>::with_capacity::<V>(1);
                x.insert(key, value).map_err(|_| unreachable!()).unwrap();
                self.tables.push(x);
                Ok(())
            }
        } else {
            Err((key, value))
        }
    }

    /// inserts the given key value if the key is not already present
    ///
    /// Returns the provided key value if an entry with the same key exists
    pub fn insert_row(&mut self, key: Key<K>, row: Row) -> Result<(), (Key<K>, Row)> {
        if !self.contains_key(&key) {
            if let Some(x) = self.tables.iter_mut().find(|x| x.match_row(&row, false)) {
                x.insert_row(key, row).map_err(|_| unreachable!())
            } else {
                let mut x = Table::<K>::new();
                x.insert_row(key, row).map_err(|_| unreachable!()).unwrap();
                self.tables.push(x);
                Ok(())
            }
        } else {
            Err((key, row))
        }
    }

    /// update the given key with value
    ///
    /// This update can be partial
    ///
    /// returns the old values or the provided value if the key does not exists
    pub fn update<V: RowLike>(
        &mut self,
        key: &Key<K>,
        value: V,
        opts: UpdateOpt,
    ) -> Result<Row, V> {
        if let Some(x) = self.find_table_by_key(key) {
            match opts {
                UpdateOpt::Patch | UpdateOpt::Update if self.tables[x].matches::<V>(true) => self
                    .tables[x]
                    .update(key, value)
                    .map_err(|_| unreachable!()),
                UpdateOpt::Update => {
                    let (k, mut v) = self.tables[x].remove(key).unwrap();
                    let old = v.extend(value);
                    if let Some(x) = self.tables.iter_mut().find(|x| x.match_row(&v, false)) {
                        x.insert_row(k, v).map_err(|_| unreachable!()).unwrap();
                    } else {
                        self.tables.push((k, v).into())
                    }
                    Ok(old)
                }
                UpdateOpt::Replace => {
                    if self.tables[x].matches::<V>(false) {
                        self.tables[x].update(key, value)
                    } else {
                        let (k, v) = self.tables[x].remove(key).unwrap();
                        if let Some(x) = self.find_table_by::<V>(false) {
                            self.tables[x]
                                .insert(k, value)
                                .map_err(|_| unreachable!())
                                .unwrap();
                        } else {
                            let mut x = Table::<K>::with_capacity::<V>(1);
                            x.insert(k, value).map_err(|_| unreachable!()).unwrap();
                            self.tables.push(x);
                        }
                        Ok(v)
                    }
                }
                _ => Err(value),
            }
        } else {
            Err(value)
        }
    }

    /// update the given key with value
    ///
    /// This update can be partial
    ///
    /// returns the old values or the provided value if the key does not exists
    pub fn update_row(&mut self, key: &Key<K>, row: Row, opts: UpdateOpt) -> Result<Row, Row> {
        if let Some(x) = self.find_table_by_key(key) {
            match opts {
                UpdateOpt::Patch | UpdateOpt::Update if self.tables[x].match_row(&row, true) => {
                    self.tables[x]
                        .update_row(key, row)
                        .map_err(|_| unreachable!())
                }
                UpdateOpt::Update => {
                    let (k, mut v) = self.tables[x].remove(key).unwrap();
                    let old = v.extend_row(row);
                    if let Some(x) = self.tables.iter_mut().find(|x| x.match_row(&v, false)) {
                        x.insert_row(k, v).map_err(|_| unreachable!()).unwrap();
                    } else {
                        self.tables.push((k, v).into())
                    }
                    Ok(old)
                }
                UpdateOpt::Replace => {
                    if self.tables[x].match_row(&row, false) {
                        self.tables[x].update_row(key, row)
                    } else {
                        let (k, v) = self.tables[x].remove(key).unwrap();
                        if let Some(x) = self.find_table_by_row(&row, false) {
                            self.tables[x]
                                .insert_row(k, row)
                                .map_err(|_| unreachable!())
                                .unwrap();
                        } else {
                            let mut x = Table::<K>::new();
                            x.insert_row(k, row).map_err(|_| unreachable!()).unwrap();
                            self.tables.push(x);
                        }
                        Ok(v)
                    }
                }
                _ => Err(row),
            }
        } else {
            Err(row)
        }
    }

    /// update or insert the given key with value
    ///
    /// if the key already exists this function behaves like [Store::update]
    pub fn upsert<V: RowLike>(
        &mut self,
        key: Key<K>,
        value: V,
        opts: UpdateOpt,
    ) -> Result<Option<Row>, (Key<K>, V)> {
        if let Some(x) = self.find_table_by_key(&key) {
            match opts {
                UpdateOpt::Patch | UpdateOpt::Update if self.tables[x].matches::<V>(true) => self
                    .tables[x]
                    .update(&key, value)
                    .map(Some)
                    .map_err(|_| unreachable!()),
                UpdateOpt::Update => {
                    let (k, mut v) = self.tables[x].remove(&key).unwrap();
                    let old = v.extend(value);
                    if let Some(x) = self.tables.iter_mut().find(|x| x.match_row(&v, false)) {
                        x.insert_row(k, v).map_err(|_| unreachable!()).unwrap();
                    } else {
                        self.tables.push((k, v).into())
                    }
                    if old.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(old))
                    }
                }
                UpdateOpt::Replace => {
                    if self.tables[x].matches::<V>(false) {
                        self.tables[x]
                            .update(&key, value)
                            .map_err(|_| unreachable!())
                            .map(Some)
                    } else {
                        let (k, v) = self.tables[x].remove(&key).unwrap();
                        if let Some(x) = self.find_table_by::<V>(false) {
                            self.tables[x]
                                .insert(k, value)
                                .map_err(|_| unreachable!())
                                .unwrap();
                        } else {
                            let mut x = Table::<K>::with_capacity::<V>(1);
                            x.insert(k, value).map_err(|_| unreachable!()).unwrap();
                            self.tables.push(x);
                        }
                        Ok(Some(v))
                    }
                }
                _ => Err((key, value)),
            }
        } else if let Some(x) = self.find_table_by::<V>(false) {
            self.tables[x]
                .insert(key, value)
                .map_err(|_| unreachable!())
                .unwrap();
            Ok(None)
        } else {
            self.tables.push((key, value).into());
            Ok(None)
        }
    }

    /// update or insert the given key with value
    ///
    /// if the key already exists this function behaves like [`Store::update_row`]
    pub fn upsert_row(
        &mut self,
        key: Key<K>,
        row: Row,
        opts: UpdateOpt,
    ) -> Result<Option<Row>, (Key<K>, Row)> {
        if let Some(x) = self.find_table_by_key(&key) {
            match opts {
                UpdateOpt::Patch | UpdateOpt::Update if self.tables[x].match_row(&row, true) => {
                    self.tables[x]
                        .update_row(&key, row)
                        .map(Some)
                        .map_err(|_| unreachable!())
                }
                UpdateOpt::Update => {
                    let (k, mut v) = self.tables[x].remove(&key).unwrap();
                    let old = v.extend_row(row);
                    if let Some(x) = self.tables.iter_mut().find(|x| x.match_row(&v, false)) {
                        x.insert_row(k, v).map_err(|_| unreachable!()).unwrap();
                    } else {
                        self.tables.push((k, v).into())
                    }
                    if old.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(old))
                    }
                }
                UpdateOpt::Replace => {
                    if self.tables[x].match_row(&row, false) {
                        self.tables[x]
                            .update_row(&key, row)
                            .map_err(|_| unreachable!())
                            .map(Some)
                    } else {
                        let (k, v) = self.tables[x].remove(&key).unwrap();
                        if let Some(x) = self.find_table_by_row(&row, false) {
                            self.tables[x]
                                .insert_row(k, row)
                                .map_err(|_| unreachable!())
                                .unwrap();
                        } else {
                            let mut x = Table::<K>::new();
                            x.insert_row(k, row).map_err(|_| unreachable!()).unwrap();
                            self.tables.push(x);
                        }
                        Ok(Some(v))
                    }
                }
                _ => Err((key, row)),
            }
        } else if let Some(x) = self.find_table_by_row(&row, false) {
            self.tables[x]
                .insert_row(key, row)
                .map_err(|_| unreachable!())
                .unwrap();
            Ok(None)
        } else {
            self.tables.push((key, row).into());
            Ok(None)
        }
    }

    /// remove the given key from the store.
    pub fn remove(&mut self, key: &Key<K>) -> Option<(Key<K>, Row)> {
        Some(
            match self
                .tables
                .iter_mut()
                .enumerate()
                .find_map(|(x, y)| Some((x, y.remove(key)?)))?
            {
                (x, y) if self.tables[x].is_empty() => {
                    self.tables.remove(x);
                    y
                }
                (_, y) => y,
            },
        )
    }

    /// remove the given columns associated to `key`
    ///
    /// The entry associated to the key is removed if there is no columns lest
    pub fn remove_columns<V: RowLike>(&mut self, key: &Key<K>) -> (Option<Key<K>>, Option<V>) {
        if let Some((i, (x, y))) = self.tables.iter_mut().enumerate().find_map(|(x, y)| {
            if y.matches::<V>(true) {
                y.remove(key).map(|y| (x, y))
            } else {
                None
            }
        }) {
            if self.tables[i].is_empty() {
                self.tables.remove(i);
            }
            let (v, row) = V::try_from_row(y).unwrap();
            if !row.is_empty() {
                self.insert_row(x, row)
                    .map_err(|_| panic!("remove columns failed to reinsert remaining"))
                    .unwrap();
                (None, Some(v))
            } else {
                (Some(x), Some(v))
            }
        } else {
            (None, None)
        }
    }
}

impl<K: Ord> Store<K> {
    /// returns an ordered iterator of key/values that matches the given [Query]
    pub fn iter_indexed<Q: Query + 'static>(
        &self,
    ) -> impl QueryMaskIter<Item = (&'_ Key<K>, Q::Item<'_>)> {
        self.iter_indexed_filter::<Q, ()>()
    }

    /// returns an ordered iterator of key/values that matches the given [Query] and [Filter]
    pub fn iter_indexed_filter<Q, F>(&self) -> impl QueryMaskIter<Item = (&'_ Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        OrderedIter(
            self.tables
                .iter()
                .flat_map(|x| {
                    let mut x = x.iter_indexed_filter::<Q, F>();
                    x.next().map(|y| (y, x))
                })
                .sorted_by(|((x, _), _), ((y, _), _)| x.key_cmp(y).into())
                .collect(),
        )
    }

    /// returns an ordered iterator of key/ mutable values that matches the given [Query]
    pub fn iter_mut_indexed<Q>(&mut self) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
    {
        self.iter_mut_indexed_filter::<Q, ()>()
    }

    /// returns an ordered iterator of key/ mutable values that matches the given [Query] and [Filter]
    pub fn iter_mut_indexed_filter<Q, F>(
        &mut self,
    ) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        OrderedIter(
            self.tables
                .iter_mut()
                .flat_map(|x| {
                    let mut x = x.iter_mut_indexed_filter::<Q, F>();
                    x.next().map(|y| (y, x))
                })
                .sorted_by(|((x, _), _), ((y, _), _)| x.key_cmp(y).into())
                .collect(),
        )
    }
}

#[cfg(feature = "rayon")]
impl<K> Store<K> {
    /// returns a parallel iterator over the keys in this store
    pub fn par_keys(&self) -> impl ParallelQueryMaskIter<Item = &Key<K>>
    where
        K: Send + Sync,
    {
        self.tables.par_iter().flat_map(|x| x.keys().par_iter())
    }
}

#[cfg(feature = "rayon")]
impl<K> Store<K>
where
    K: Send + Sync,
{
    /// Get an iterator over key/values that matches the given [Query]
    pub fn par_iter<Q>(&self) -> impl ParallelQueryMaskIter<Item = (&'_ Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
        for<'a> Q::Item<'a>: Send + Sync,
    {
        self.par_iter_filter::<Q, ()>()
    }

    /// Get a parallel iterator over key/values that matches the given [Query] and filter
    pub fn par_iter_filter<Q, F>(
        &self,
    ) -> impl ParallelQueryMaskIter<Item = (&'_ Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
        for<'a> Q::Item<'a>: Send + Sync,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        self.tables
            .par_iter()
            .filter_map(|x| x.par_iter_filter::<Q, F>())
            .flatten()
    }

    /// Get a parallel iterator over key/ mutable values that matches the given [Query]
    pub fn par_iter_mut<Q>(
        &mut self,
    ) -> impl ParallelQueryMaskIter<Item = (&'_ Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
        for<'a> Q::Item<'a>: Send + Sync,
    {
        self.par_iter_mut_filter::<Q, ()>()
    }

    /// Get a parallel iterator over key/ mutable values that matches the given [Query] and [Filter]
    pub fn par_iter_mut_filter<Q, F>(
        &mut self,
    ) -> impl ParallelQueryMaskIter<Item = (&'_ Key<K>, Q::Item<'_>)>
    where
        Q: Query + 'static,
        for<'a> Q::Item<'a>: Send + Sync,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        self.tables
            .par_iter_mut()
            .filter_map(|x| x.par_iter_mut_filter::<Q, F>())
            .flatten()
    }
}

impl<K: Hash + Ord + Send + Sync> Store<K> {
    #[cfg(feature = "rayon")]
    /// build an index for all tables in this store in parallel
    pub fn par_build_index(&mut self) {
        self.tables.par_iter_mut().for_each(|x| x.par_build_index())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{mask::Atom, query::AnyOf, Mask};

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_deeply_nested() {
        let mut k: Key<&'static str> = Key::from(["a"]);
        let mut t = Store::new();
        const NB: usize = 2000;
        for x in 0..NB {
            t.insert(k.clone(), (x,)).unwrap();
            k = k + "abc"
        }
        assert_eq!(t.len(), 2000);
    }

    #[test]
    fn basic_test() {
        let mut c = Store::new();
        let k1 = Key::from(["foo", "baz"]);
        let k2 = Key::from(["foo", "bar"]);
        let k3 = Key::from(["foo", "baz", "blah"]);
        c.upsert(k1.clone(), (11i32,), UpdateOpt::Replace).unwrap();
        assert_eq!(c.get::<&i32>(&k1), Some(&11));
        *c.get_mut::<&mut i32>(&k1).unwrap() += 1;
        assert_eq!(c.get::<&i32>(&k1), Some(&12));
        c.upsert(k2.clone(), (22i32,), UpdateOpt::Replace).unwrap();
        assert_eq!(c.get::<&i32>(&k2), Some(&22));
        *c.get_mut::<&mut i32>(&k2).unwrap() += 1;
        assert_eq!(c.get::<&i32>(&k2), Some(&23));
        assert!(c.insert(k3.clone(), (10.0f32, 12i32)).is_ok());
        c.build_index();
        assert_eq!(
            c.iter_indexed::<()>().map(|x| x.0).collect::<Vec<_>>(),
            vec![&k2, &k1, &k3]
        );
        assert_eq!(
            c.iter_indexed::<()>()
                .rev()
                .map(|x| x.0)
                .collect::<Vec<_>>(),
            vec![&k3, &k1, &k2]
        );
    }

    #[test]
    fn iter_test() {
        let mut s = Store::new();
        let k1 = Key::from(["foo", "baz"]);
        let k2 = Key::from(["foo", "bar"]);
        let k3 = Key::from(["bla", "baz", "blah"]);
        let k4 = Key::from(["123", "baz"]);
        let k5 = Key::from(["foo", "bar2"]);
        let k6 = Key::from(["aa", "baz", "blah"]);
        let k7 = Key::from(["2foo", "baz"]);
        let k8 = Key::from(["bb", "bar"]);
        let k9 = Key::from(["foo", "baz", "blah"]);
        let k10 = Key::from(["xx", "baz"]);
        let k11 = Key::from(["cc", "bar"]);
        let k12 = Key::from(["hh", "h", "blah"]);
        s.insert(k1.clone(), (1i32,)).unwrap();
        s.insert(k2.clone(), (2i32, "s".to_string())).unwrap();
        s.insert(k3.clone(), (true,)).unwrap();
        s.insert(k4.clone(), (3u8, 18i32)).unwrap();
        s.insert(k5.clone(), ("hello".to_string(), 12i32)).unwrap();
        s.insert(k6.clone(), ("yolo".to_string(), 5u8)).unwrap();
        s.insert(k7.clone(), ('c', 12i16)).unwrap();
        s.insert(k8.clone(), (15i32, 10f32)).unwrap();
        s.insert(k9.clone(), (16i32, 10f32)).unwrap();
        s.insert(k10.clone(), (17i32, 10f32, "foo".to_string()))
            .unwrap();
        s.insert(k11.clone(), (false,)).unwrap();
        s.insert(k12.clone(), (true, "bar".to_string())).unwrap();
        s.build_index();
        assert_eq!(
            s.iter_mut_indexed::<&mut bool>().collect::<Vec<_>>(),
            vec![(&k3, &mut true), (&k11, &mut false), (&k12, &mut true)],
        );
        assert_eq!(
            s.iter_indexed::<&bool>().rev().collect::<Vec<_>>(),
            vec![(&k12, &true), (&k11, &false), (&k3, &true)],
        );
        assert_eq!(
            s.iter_indexed::<AnyOf<(&bool, &i32)>>().collect::<Vec<_>>(),
            vec![
                (&k4, (None, Some(&18))),
                (&k8, (None, Some(&15))),
                (&k3, (Some(&true), None)),
                (&k11, (Some(&false), None)),
                (&k2, (None, Some(&2))),
                (&k5, (None, Some(&12))),
                (&k1, (None, Some(&1))),
                (&k9, (None, Some(&16))),
                (&k12, (Some(&true), None)),
                (&k10, (None, Some(&17))),
            ],
        );
        assert_eq!(
            s.iter_mut_indexed::<&mut String>().collect::<Vec<_>>(),
            vec![
                (&k6, (&mut "yolo".to_string())),
                (&k2, (&mut "s".to_string())),
                (&k5, (&mut "hello".to_string())),
                (&k12, (&mut "bar".to_string())),
                (&k10, (&mut "foo".to_string())),
            ],
        );
    }

    #[test]
    fn test_many_mut() {
        let mut s = Store::<String>::new();
        s.insert(Key::new("a".to_string()), (true, 10, "".to_string()))
            .unwrap();
        s.insert(Key::new("b".to_string()), (false, 11, "i am 2".to_string()))
            .unwrap();
        s.insert(
            Key::from(["c".to_string(), "d".to_string()]),
            (false, 11, "i am 2".to_string()),
        )
        .unwrap();
        assert!(s.is_indexed());
        let mask = Mask::<_, ()>::new(Atom::Any);

        let it = s.mask_mut::<(&mut bool, &mut String)>(&mask);
        for (k, (b, s)) in it {
            *b = !*b;
            s.push_str(&format!("{k:?} => {b}"));
        }
        let it2 = s.mask_mut::<&mut i32>(&mask);
        for (i, itm) in it2.enumerate() {
            *itm.1 = (2 * *itm.1) + i as i32;
        }
        assert_eq!(
            s.get_mut::<(&mut String, &mut i32, &bool)>(&Key::new("a".into()))
                .unwrap(),
            (&mut "Key([\"a\"]) => false".to_string(), &mut 20, &false)
        );
        assert_eq!(
            s.get_mut::<(&mut String, &mut i32, &bool)>(&Key::new("b".into()))
                .unwrap(),
            (
                &mut "i am 2Key([\"b\"]) => true".to_string(),
                &mut 23,
                &true
            )
        );
    }

    #[test]
    fn test_root_keys() {
        let mut s = Store::new();
        let k1 = Key::new("a");
        let k2 = Key::from(["a", "b"]);
        let k3 = Key::from(["a", "c"]);
        let k4 = Key::from(["c", "a"]);
        s.insert(k1.clone(), (true, "key 1")).unwrap();
        s.insert(k2.clone(), (10, "key 2")).unwrap();
        s.insert(k3.clone(), (1u8, "key 3")).unwrap();
        s.insert(k4.clone(), (12.1, "key 4")).unwrap();
        s.build_index();
        assert_eq!(
            s.roots()
                .into_iter()
                .map(|(x, y)| (x, y.sorted().collect::<Vec<_>>()))
                .collect::<Vec<_>>(),
            vec![(&k1, vec![&k2, &k3]), (&k4, vec![])]
        );
    }

    #[test]
    fn test_root_keys2() {
        let mut s = Store::new();
        let k1 = Key::new("a");
        let k2 = Key::from(["b"]);
        let k3 = Key::from(["c"]);
        let k4 = Key::from(["b", "c"]);
        let k5 = Key::from(["b", "a"]);
        s.insert(k4.clone(), (12.1f32, "key 4")).unwrap();
        s.insert(k5.clone(), (12.1f32, "key 5")).unwrap();
        s.insert(k1.clone(), (true,)).unwrap();
        s.insert(k2.clone(), (true,)).unwrap();
        s.insert(k3.clone(), (true,)).unwrap();
        s.build_index();
        assert_eq!(
            s.roots()
                .into_iter()
                .map(|(x, y)| (x, y.sorted().collect::<Vec<_>>()))
                .collect::<Vec<_>>(),
            vec![(&k1, vec![]), (&k2, vec![&k5, &k4]), (&k3, vec![])]
        );
    }
}
