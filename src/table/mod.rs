//! Homogenous storage module
mod grid;
mod index;
mod iter;
mod view;

pub use self::{
    grid::{Grid, Row, RowLike, *},
    index::*,
    iter::*,
    view::*,
};
#[cfg(feature = "rayon")]
use crate::query::ParallelQueryIter;
use crate::{
    data::Data,
    key::Key,
    query::{Filter, Query, QueryIter, QueryMaskIter},
    Match,
};
use std::hash::Hash;

/// A [`Table`] stores hierarchical values of the same data type in a cache-friendly way.
///
/// For example, a [`Table`] containing 4 columns for types `A`, `B`, `C` and `D`:
///
/// |   key   | A | B | C | D |
/// |---------|---|---|---|---|
/// |   foo   | 1 | 2 | 3 | 4 |
/// | foo.bar | 5 | 6 | 7 | 8 |
/// |   ...   | - | - | - | - |
#[derive(Debug)]
pub struct Table<K> {
    index: Index<K>,
    grid: Grid,
}

impl<K> std::default::Default for Table<K> {
    fn default() -> Self {
        Self {
            index: Default::default(),
            grid: Default::default(),
        }
    }
}

impl<K> Table<K> {
    /// create a new empty table
    ///
    /// Table columns become fixed after the first insertion
    pub fn new() -> Self {
        Self::default()
    }

    /// create a new table with the given columns
    pub fn with<V: RowLike>() -> Self {
        Self::with_capacity::<V>(0)
    }

    /// create a new table with the given columns and reserver capacity for each columns
    pub fn with_capacity<V: RowLike>(n: usize) -> Self {
        Self {
            index: Index::with_capacity(n),
            grid: Grid::with_capacity::<V>(n),
        }
    }

    /// returns true if this table has a column of type `T`
    pub fn contains<T: Data>(&self) -> bool {
        self.grid.contains::<T>()
    }

    /// Number of columns in this table
    pub fn len_cols(&self) -> usize {
        self.grid.len_cols()
    }

    /// Number of rows in this table
    pub fn len_rows(&self) -> usize {
        self.grid.len_rows()
    }

    /// number of roots in this table
    pub fn len_roots(&self) -> usize {
        self.index.len_roots()
    }

    /// returns true if this table is indexed
    pub fn is_indexed(&self) -> bool {
        self.index.is_indexed()
    }

    /// return true if this table is empty
    pub fn is_empty(&self) -> bool {
        self.index.keys().is_empty()
    }

    /// returns true if the this table contains these columns
    ///
    /// if `partial` is true, this table should contains at least these columns.
    ///
    /// if `partial` is false this table should contain exactly these columns
    ///
    /// For tuples of [Data], match table panics if the same type is present more than once
    pub fn matches<V: RowLike>(&self, partial: bool) -> bool {
        V::match_grid(&self.grid, partial)
    }

    /// returns true if the this table contains these columns
    ///
    /// if `partial` is true, this table should contains at least these columns.
    ///
    /// if `partial` is false this table should contain exactly these columns
    ///
    /// For tuples of [Data], match table panics if the same type is present more than once
    pub fn match_row(&self, row: &Row, partial: bool) -> bool {
        row.match_grid(&self.grid, partial)
    }

    /// creates an immutable view of this table
    pub fn view_ref(&self) -> View<'_, K> {
        View::new_ref(&self.index, self.grid.grid_ref())
    }

    /// creates an mutable view of this table
    pub fn view_mut(&mut self) -> View<'_, K> {
        View::new_mut(&self.index, self.grid.grid_mut())
    }

    /// returns the root keys and children of this table
    ///
    /// panics in debug if the table is not indexed
    pub fn roots(&self) -> impl QueryIter<Item = (&Key<K>, impl QueryIter<Item = &Key<K>> + '_)> {
        if self.is_indexed() {
            Box::new(RootsIter::new(&self.index))
        } else {
            debug_assert!(false, "impossible to get root keys on an unindexed table");
            Box::new(std::iter::empty()) as Box<dyn QueryIter<Item = (&Key<K>, _)>>
        }
    }

    /// returns the keys in this table
    pub fn keys(&self) -> &KeySet<K> {
        &self.index.keys()
    }

    /// return ordered keys in this table
    pub fn keys_indexed(&self) -> impl QueryIter<Item = &Key<K>> {
        let idx = self.view_ref().index();
        idx.index().iter().map(|x| &idx.keys()[*x])
    }

    /// returns an iterator containing items in this table matching the given query
    pub fn iter<'a, Q>(&'a self) -> Box<dyn QueryIter<Item = (&'a Key<K>, Q::Item<'a>)> + 'a>
    where
        Q: Query + 'static,
    {
        self.iter_filter::<Q, ()>()
    }

    /// returns an iterator containing items in this table matching the given query and filter
    pub fn iter_filter<'a, Q, F>(
        &'a self,
    ) -> Box<dyn QueryIter<Item = (&'a Key<K>, Q::Item<'a>)> + 'a>
    where
        Q: Query + 'static,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        let view = self.view_ref();
        if F::match_view(&view) {
            Q::iter(view)
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// returns an iterator containing items ordered by key in this table matching the given query
    pub fn iter_indexed<'a, Q>(
        &'a self,
    ) -> Box<dyn QueryIter<Item = (&'a Key<K>, Q::Item<'a>)> + 'a>
    where
        Q: Query + 'static,
    {
        self.iter_indexed_filter::<Q, ()>()
    }

    /// returns an iterator containing items ordered by key in this table matching the given query and filter
    pub fn iter_indexed_filter<'a, Q, F>(
        &'a self,
    ) -> Box<dyn QueryIter<Item = (&'a Key<K>, Q::Item<'a>)> + 'a>
    where
        Q: Query + 'static,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        debug_assert!(self.is_indexed());
        let view = self.view_ref();
        if F::match_view(&view) {
            Q::iter_indexed(view)
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// returns an iterator ordered by key & filtered out by mask containing items in this table matching the given query  
    pub fn mask<'a, 'b, Q>(
        &'a self,
        mask: &'b impl Match<K>,
    ) -> impl QueryMaskIter<Item = (&'a Key<K>, Q::Item<'a>)>
    where
        'b: 'a,
        Q: Query + 'static,
    {
        self.mask_filter::<Q, ()>(mask)
    }

    /// returns an iterator ordered by key & filtered out by mask containing items in this table matching the given query and filter
    pub fn mask_filter<'a, 'b, Q, F>(
        &'a self,
        mask: &'b impl Match<K>,
    ) -> impl QueryMaskIter<Item = (&'a Key<K>, Q::Item<'a>)>
    where
        'b: 'a,
        Q: Query + 'static,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        let view = self.view_ref();
        F::match_view(&view)
            .then(|| Q::mask(view, mask))
            .into_iter()
            .flatten()
    }

    /// iterator over [Key] - mutable? [Query::Item] if this table contains the columns specified in the given [Query]
    pub fn iter_mut<'a, Q>(
        &'a mut self,
    ) -> Box<dyn QueryIter<Item = (&'a Key<K>, Q::Item<'a>)> + 'a>
    where
        Q: Query + 'static,
    {
        self.iter_mut_filter::<Q, ()>()
    }

    /// iterator over [Key] - mutable? [Query::Item] if this table contains the columns specified in the given [Query] and [Filter]
    pub fn iter_mut_filter<'a, Q, F>(
        &'a mut self,
    ) -> Box<dyn QueryIter<Item = (&'a Key<K>, Q::Item<'a>)> + 'a>
    where
        Q: Query + 'static,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        let view = self.view_mut();
        if F::match_view(&view) {
            Q::iter(view)
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// iterator over [Key] - mutable? [Query::Item] ordered by [Key] if this table contains the columns specified in the given [Query]
    pub fn iter_mut_indexed<Q>(&mut self) -> Box<dyn QueryIter<Item = (&Key<K>, Q::Item<'_>)> + '_>
    where
        Q: Query + 'static,
    {
        self.iter_mut_indexed_filter::<Q, ()>()
    }

    /// iterator over [Key] - mutable? [Query::Item] ordered by [Key] if this table contains the columns specified in the given [Query] and [Filter]
    pub fn iter_mut_indexed_filter<Q, F>(
        &mut self,
    ) -> Box<dyn QueryIter<Item = (&Key<K>, Q::Item<'_>)> + '_>
    where
        Q: Query + 'static,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        debug_assert!(self.is_indexed());
        let view = self.view_mut();
        if F::match_view(&view) {
            Q::iter_indexed(view)
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// iterator over [Key] - mutable? [Query::Item] ordered by [Key] matching the given mask (see: [Match] [Mask](crate::Mask)) if this table contains the columns specified in the given [Query] and [Filter]
    pub fn mask_mut<'a, 'b, Q>(
        &'a mut self,
        mask: &'b impl Match<K>,
    ) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'a>)>
    where
        'b: 'a,
        Q: Query + 'static,
    {
        self.mask_mut_filter::<Q, ()>(mask)
    }

    /// iterator over [Key] - mutable? [Query::Item] ordered by [Key] matching the given mask (see: [Match] [Mask][crate::Mask] if this table contains the columns specified in the given [Query]
    pub fn mask_mut_filter<'a, 'b, Q, F>(
        &'a mut self,
        mask: &'b impl Match<K>,
    ) -> impl QueryMaskIter<Item = (&Key<K>, Q::Item<'a>)>
    where
        'b: 'a,
        Q: Query + 'static,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        let view = self.view_mut();
        F::match_view(&view)
            .then(|| Q::mask(view, mask))
            .into_iter()
            .flatten()
    }
}

#[cfg(feature = "rayon")]
impl<K> Table<K> {
    /// if this table contains columns specified by the [Query], returns a parallel iterator of key - [Query::Item]
    pub fn par_iter<'a, Q>(
        &'a self,
    ) -> Option<impl ParallelQueryIter<Item = (&'a Key<K>, Q::Item<'a>)>>
    where
        K: Send + Sync,
        Q: Query + 'static,
        Q::Item<'a>: Send + Sync,
    {
        self.par_iter_filter::<Q, ()>()
    }

    /// if this table contains columns specified by the [Query] & [Filter], returns a parallel iterator of key - [Query::Item]
    pub fn par_iter_filter<'a, Q, F>(
        &'a self,
    ) -> Option<impl ParallelQueryIter<Item = (&'a Key<K>, Q::Item<'a>)>>
    where
        K: Send + Sync,
        Q: Query + 'static,
        Q::Item<'a>: Send + Sync,
        F: Filter,
    {
        assert!(Q::READ_ONLY);
        debug_assert!(self.is_indexed());
        let view = self.view_ref();
        if F::match_view(&view) {
            Q::par_iter(view)
        } else {
            None
        }
    }

    /// parallel iterator over [Key] - mutable? [Query::Item] if this table contains the columns specified in the given [Query]
    pub fn par_iter_mut<'a, Q>(
        &'a mut self,
    ) -> Option<impl ParallelQueryIter<Item = (&'a Key<K>, Q::Item<'a>)>>
    where
        K: Send + Sync,
        Q: Query + 'static,
        Q::Item<'a>: Send + Sync,
    {
        self.par_iter_mut_filter::<Q, ()>()
    }

    /// parallel iterator over [Key] - mutable? [Query::Item] if this table contains the columns specified in the given [Query] and [Filter]
    pub fn par_iter_mut_filter<'a, Q, F>(
        &'a mut self,
    ) -> Option<impl ParallelQueryIter<Item = (&'a Key<K>, Q::Item<'a>)>>
    where
        K: Send + Sync,
        Q: Query + 'static,
        Q::Item<'a>: Send + Sync,
        F: Filter,
    {
        assert!(!Q::READ_ONLY);
        debug_assert!(self.is_indexed());
        let view = self.view_mut();
        if F::match_view(&view) {
            Q::par_iter(view)
        } else {
            None
        }
    }
}

impl<K: Eq + Hash> Table<K> {
    /// returns true if this table contains the given key
    pub fn contains_key(&self, key: &Key<K>) -> bool {
        self.index.keys().contains(key)
    }

    /// get a single reference that matches the given [Query] and key
    pub fn get<Q: Query>(&self, key: &Key<K>) -> Option<Q::Item<'_>> {
        self.get_filter::<Q, ()>(key)
    }

    /// get a single reference that matches the given [Query], [Filter] and [Key]
    pub fn get_filter<Q: Query, F: Filter>(&self, key: &Key<K>) -> Option<Q::Item<'_>> {
        assert!(Q::READ_ONLY);
        let mut view = self.view_ref();
        if F::match_view(&view) {
            Q::get(&mut view, key)
        } else {
            None
        }
    }

    /// get a single mutable reference that matches the given [Query] and [Key]
    pub fn get_mut<Q: Query>(&mut self, key: &Key<K>) -> Option<Q::Item<'_>> {
        self.get_mut_filter::<Q, ()>(key)
    }

    /// get a single mutable reference that matches the given [Query], [Filter] and [Key]
    pub fn get_mut_filter<Q: Query, F: Filter>(&mut self, key: &Key<K>) -> Option<Q::Item<'_>> {
        assert!(!Q::READ_ONLY);
        let mut view = self.view_mut();
        if F::match_view(&view) {
            Q::get(&mut view, key)
        } else {
            None
        }
    }

    /// update the given key with a value
    ///
    /// This update can be partial
    ///
    /// returns the old values or the provided value if the key does not exists or cannot be inserted in this table
    pub fn update<V: RowLike>(&mut self, key: &Key<K>, value: V) -> Result<Row, V> {
        if let Some(index) = self.index.keys().get_index_of(key) {
            self.grid.update(index, value)
        } else {
            Err(value)
        }
    }

    /// update the given key with a row
    ///
    /// This update can be partial
    ///
    /// returns the old values or the provided value if the key does not exists or cannot be inserted in this table
    pub fn update_row(&mut self, key: &Key<K>, row: Row) -> Result<Row, Row> {
        if let Some(index) = self.index.keys().get_index_of(key) {
            self.grid.update_row(index, row)
        } else {
            Err(row)
        }
    }
}

impl<K: Hash + Ord> Table<K> {
    /// inserts the given key value if the key is not already present
    ///
    /// Returns the provided key value if the entry exists or cannot be inserted
    pub fn insert<V: RowLike>(&mut self, key: Key<K>, value: V) -> Result<(), (Key<K>, V)> {
        if !self.grid.has_cols() {
            self.index.insert(key);
            self.grid.insert(value).map_err(|_| unreachable!()).unwrap();
            Ok(())
        } else if !self.index.contains(&key) && V::match_grid(&self.grid, false) {
            self.grid.insert(value).map_err(|_| unreachable!())?;
            self.index.insert(key);
            Ok(())
        } else {
            Err((key, value))
        }
    }

    /// inserts the given key value if the key is not already present
    ///
    /// Returns the provided key value if the entry exists or cannot be inserted
    pub fn insert_row(&mut self, key: Key<K>, row: Row) -> Result<(), (Key<K>, Row)> {
        if !self.grid.has_cols() {
            self.index.insert(key);
            self.grid
                .insert_row(row)
                .map_err(|_| unreachable!())
                .unwrap();
            Ok(())
        } else if !self.index.keys().contains(&key) && row.match_grid(&self.grid, false) {
            self.grid.insert_row(row).map_err(|_| unreachable!())?;
            self.index.insert(key);
            Ok(())
        } else {
            Err((key, row))
        }
    }

    /// update or insert the given key/value
    ///
    /// returns the old values
    ///
    /// if the key is already present then a partial update can be done
    pub fn upsert<V: RowLike>(&mut self, key: Key<K>, value: V) -> Result<Row, (Key<K>, V)>
    where
        K: Clone,
    {
        match self.index.insert(key) {
            (index, true) => {
                if let Err(x) = self.grid.insert(value) {
                    Err((self.index.swap_remove_index(index).unwrap(), x))
                } else {
                    Ok(Row::default())
                }
            }
            (index, false) => self
                .grid
                .update(index, value)
                .map_err(|x| (self.index.keys().get_index(index).cloned().unwrap(), x)),
        }
    }

    /// update or insert the given key/value
    ///
    /// returns the old values
    ///
    /// if the key is already present then a partial update can be done
    pub fn upsert_row(&mut self, key: Key<K>, row: Row) -> Result<Row, (Key<K>, Row)>
    where
        K: Clone,
    {
        match self.index.insert(key) {
            (index, true) => {
                if let Err(x) = self.grid.insert_row(row) {
                    Err((self.index.swap_remove_index(index).unwrap(), x))
                } else {
                    Ok(Row::default())
                }
            }
            (index, false) => self
                .grid
                .update_row(index, row)
                .map_err(|x| (self.index.keys().get_index(index).cloned().unwrap(), x)),
        }
    }

    /// remove the given key and it's columns from this table
    ///
    /// return the old row if any
    pub fn remove(&mut self, key: &Key<K>) -> Option<(Key<K>, Row)> {
        self.index
            .swap_remove(key)
            .map(|(x, y)| (x, self.grid.swap_remove(y)))
    }

    /// Extend this table with the given entries
    pub fn extend<V: RowLike, I: IntoIterator<Item = (Key<K>, V)>>(
        &mut self,
        values: I,
    ) -> Result<(), I> {
        if V::match_grid(&self.grid, false) {
            for (k, v) in values {
                match self.index.insert(k) {
                    (_, true) => {
                        self.grid.insert(v).map_err(|_| unreachable!()).unwrap();
                    }
                    (index, false) => {
                        self.grid
                            .update(index, v)
                            .map_err(|_| unreachable!())
                            .unwrap();
                    }
                }
            }
            Ok(())
        } else {
            Err(values)
        }
    }
}

impl<K: Ord> Table<K> {
    /// build an index for this table
    pub fn build_index(&mut self) {
        self.index.build()
    }
}

impl<K: Ord + Send + Sync> Table<K> {
    #[cfg(feature = "rayon")]
    /// build an index for this table in parallel
    pub fn par_build_index(&mut self) {
        self.index.par_build()
    }
}

impl<K: Hash + Ord, T: RowLike> From<(Key<K>, T)> for Table<K> {
    fn from((k, v): (Key<K>, T)) -> Self {
        let mut new = Self {
            index: k.into(),
            grid: v.into_columns().into(),
        };
        new.build_index();
        new
    }
}

impl<K: Hash + Ord> From<(Key<K>, Row)> for Table<K> {
    fn from((k, v): (Key<K>, Row)) -> Self {
        Self {
            index: k.into(),
            grid: v.into(),
        }
    }
}

impl<K: Hash + Ord, V: RowLike> FromIterator<(Key<K>, V)> for Table<K> {
    fn from_iter<T: IntoIterator<Item = (Key<K>, V)>>(iter: T) -> Self {
        let (index, grid): (Index<K>, Grid) = iter.into_iter().unzip();
        assert_eq!(index.len(), grid.len_rows());
        let mut tbl = Self { index, grid };
        tbl.build_index();
        tbl
    }
}

impl<K: Hash + Ord, V: RowLike> From<Vec<(Key<K>, V)>> for Table<K> {
    fn from(value: Vec<(Key<K>, V)>) -> Self {
        value.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_deeply_nested_alloc() {
        let mut k: Key<&'static str> = Key::from(["a"]);
        const NB: usize = 2000;
        let mut t = Table::with_capacity::<(usize,)>(NB);
        for x in 0..NB {
            k = k + "abc";
            t.insert(k.clone(), (x,)).unwrap();
        }
        assert_eq!(t.len_rows(), NB);
        t.build_index();
        assert_eq!(t.len_roots(), 1);
        assert_eq!(t.get::<&usize>(&Key::from(["a", "abc"])), Some(&0));
        *t.get_mut::<&mut usize>(&Key::from(["a", "abc"])).unwrap() += 12;
        assert_eq!(t.get::<&usize>(&Key::from(["a", "abc"])), Some(&12));
        assert_eq!(
            t.get_mut::<(&mut usize, &usize)>(&Key::from(["a", "abc"])),
            None
        );
        assert_eq!(t.get::<&usize>(&k), Some(&(NB - 1)));
        t.insert(Key::from(["c"]), (1usize,)).unwrap();
        t.insert(Key::from(["b"]), (1usize,)).unwrap();
        t.build_index();
        assert_eq!(t.len_rows(), NB + 2);
        assert_eq!(t.len_roots(), 3);
        assert_eq!(t.len_roots(), 3);
    }

    #[test]
    fn test_roots() {
        let ks = [
            Key::new("a"),
            Key::from(["a", "b"]),
            Key::from(["a", "c"]),
            Key::new("z"),
            Key::from(["z", "b"]),
            Key::from(["z", "c"]),
        ];
        let mut t = Table::with_capacity::<(String, usize, bool)>(6);
        ks.iter().enumerate().for_each(|(i, x)| {
            t.insert(x.clone(), (format!("{x:?}"), i, i % 2 == 0))
                .unwrap();
        });
        assert_eq!(
            t.roots().map(|(x, y)| (x, y.collect())).collect::<Vec<_>>(),
            vec![
                (&ks[0], vec![&ks[1], &ks[2]]),
                (&ks[3], vec![&ks[4], &ks[5]])
            ]
        );
        assert_eq!(
            t.iter_indexed::<(&String, &usize, &bool)>()
                .map(|(x, (a, b, c))| (x, (a.clone(), *b, *c)))
                .collect::<Vec<_>>(),
            ks.iter()
                .enumerate()
                .map(|(i, x)| (x, (format!("{x:?}"), i, i % 2 == 0)))
                .collect::<Vec<_>>()
        )
    }
}
