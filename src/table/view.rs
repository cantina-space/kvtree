use super::{
    grid::{Mut, Ref},
    index::Index,
};
use crate::{Data, Key};
use ahash::AHashSet;
use std::{any::TypeId, hash::Hash};

#[derive(Debug)]
enum TableView<'a> {
    Ref(Ref<'a>),
    Mut { refs: Ref<'a>, muts: Mut<'a>, write: AHashSet<TypeId> },
}

impl<'a> TableView<'a> {
    /// number of columns
    pub fn len_cols(&self) -> usize {
        match self {
            TableView::Ref(x) => x.len_cols(),
            TableView::Mut { refs, muts, write } => {
                refs.len_cols() + muts.len_rows() + write.len()
            }
        }
    }

    /// returns true if there is columns
    pub fn has_cols(&self) -> bool {
        match self {
            TableView::Ref(x) => x.has_cols(),
            TableView::Mut { refs, muts, write } => {
                refs.has_cols() || muts.has_cols() || !write.is_empty()
            }
        }
    }

    /// returns true if the given type `T` is present in this view
    ///
    /// the data may be not be accessible for reading or writing see [Self::is_readable] and [Self::is_writable]
    pub fn contains<T: Data>(&self) -> bool {
        match self {
            TableView::Ref(x) => x.contains::<T>(),
            TableView::Mut { refs, muts, write } => {
                refs.contains::<T>() || muts.contains::<T>() || write.contains(&T::ID)
            }
        }
    }

    /// returns true if the given type id is present in this view
    ///
    /// the data may be not be accessible for reading or writing see [Self::is_readable] and [Self::is_writable]
    pub fn contains_type(&self, id: &TypeId) -> bool {
        match self {
            TableView::Ref(x) => x.contains_type(id),
            TableView::Mut { refs, muts, write } => {
                refs.contains_type(id) || muts.contains_type(id) || write.contains(id)
            }
        }
    }

    /// returns true if the type `T` can be read with [Self::read]
    pub fn is_readable<T: Data>(&self) -> bool {
        match self {
            TableView::Ref(x) => x.contains::<T>(),
            TableView::Mut { refs, muts, .. } => {
                refs.contains::<T>() || muts.contains::<T>()
            }
        }
    }

    /// returns true if the type `T` can be written with [Self::write]
    pub fn is_writable<T: Data>(&self) -> bool {
        if let TableView::Mut { muts, .. } = self {
            muts.contains::<T>()
        } else {
            false
        }
    }

    /// returns a slice corresponding to a column of `T`s
    ///
    /// None is returned if the type does not exists or the column has been fetched by [Self::write]
    pub fn read<T: Data>(&mut self) -> Option<&'a [T]> {
        match self {
            TableView::Ref(x) => x.read::<T>(),
            TableView::Mut { refs, muts, .. } => muts.read::<T>(refs),
        }
    }

    /// returns a slice corresponding to a column of mutable `T`s
    ///
    /// None is returned if the type does not exists or the column has been fetched by [Self::read] or [Self::write]
    pub fn write<T: Data>(&mut self) -> Option<&'a mut [T]> {
        if let TableView::Mut { muts, write, .. } = self {
            muts.write::<T>().map(|x| {
                write.insert(T::ID);
                x
            })
        } else {
            None
        }
    }
}

/// A view of a table
///
/// can get reference or mutable reference to columns and reference to keys
///
/// see also: [Table::view_ref](`crate::Table::<K>::view_ref`), [Table::view_mut](`crate::Table::<K>::view_mut`)
#[derive(Debug)]
pub struct View<'a, K> {
    index: &'a Index<K>,
    grid:  TableView<'a>,
}

impl<'a, K> View<'a, K> {
    pub(in crate::table) fn new_ref(index: &'a Index<K>, refs: Ref<'a>) -> Self {
        Self { index, grid: TableView::Ref(refs) }
    }

    pub(in crate::table) fn new_mut(index: &'a Index<K>, muts: Mut<'a>) -> Self {
        Self {
            index,
            grid: TableView::Mut { muts, refs: Ref::default(), write: AHashSet::new() },
        }
    }

    /// number of rows in the view
    pub fn len_rows(&self) -> usize { self.index.keys().len() }

    /// returns true if there is no rows in the view
    pub fn is_empty(&self) -> bool { self.index.keys().is_empty() }

    /// number of columns in the view
    pub fn len_cols(&self) -> usize { self.grid.len_cols() }

    /// returns true if this view has columns
    pub fn has_cols(&self) -> bool { self.grid.has_cols() }

    /// returns true if the given type `T` is present in this view
    ///
    /// the data may be not be accessible for reading or writing see [Self::is_readable] and [Self::is_writable]
    pub fn contains<T: Data>(&self) -> bool { self.grid.contains::<T>() }

    /// index of this view
    pub fn index(&self) -> &'a Index<K> { self.index }

    /// returns true if the given [TypeId] is present in this view
    ///
    /// the data may be not be accessible for reading or writing see [Self::is_readable] and [Self::is_writable]
    pub fn contains_type(&self, id: &TypeId) -> bool { self.grid.contains_type(id) }

    /// returns true if the type `T` can be read with [Self::read]
    pub fn is_readable<T: Data>(&self) -> bool { self.grid.is_readable::<T>() }

    /// returns true if the type `T` can be written with [Self::write]
    pub fn is_writable<T: Data>(&self) -> bool { self.grid.is_writable::<T>() }

    /// if a column of type `T` exits: make it read only and returns a slice of it
    ///
    /// successive access to read will return the same slice
    ///
    /// returns none if the type does not exists or the column as been taken by a [write](View::write)
    pub fn read<T: Data>(&mut self) -> Option<&'a [T]> { self.grid.read::<T>() }

    /// Try to take a column of type `T` from this view.
    ///
    /// if a column as already been taken by a [read](`View::read`) or a write [None] is returned
    pub fn write<T: Data>(&mut self) -> Option<&'a mut [T]> { self.grid.write::<T>() }
}

impl<'a, K> View<'a, K>
where
    Key<K>: Hash + Eq,
{
    /// returns true if this view contains the given key
    pub fn contains_key(&self, key: &Key<K>) -> bool { self.index.keys().contains(key) }

    /// returns the index of the given key if it exists in this view
    pub(crate) fn index_of(&self, key: &Key<K>) -> Option<usize> {
        self.index.keys().get_index_of(key)
    }
}
