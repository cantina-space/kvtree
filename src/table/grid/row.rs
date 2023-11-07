use crate::{
    data::Data,
    table::grid::{cell::Cell, column::Column, Grid},
};
use ahash::{AHashMap, AHashSet};
use std::{
    any::{Any, TypeId},
    fmt::Debug,
};
use tuplify::HList;

/// A row that can contain a dynamic number of columns as long as each column has a distinct type
#[derive(Debug, Default)]
pub struct Row(AHashMap<TypeId, Box<dyn Cell>>);

impl Row {
    /// creates a new empty row
    pub fn new<V: RowLike>(value: V) -> Row {
        Row(value.into_cells().into_iter().map(|x| ((*x).cell_type_id(), x)).collect())
    }

    /// number of columns in this row
    pub fn len(&self) -> usize { self.0.len() }

    /// return true if thios row has no columns
    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    /// return true if this row contains the given type
    pub fn contains<T: Data>(&self) -> bool { self.0.contains_key(&T::ID) }

    /// update or insert the given column, return the old value
    pub fn upsert<T: Data>(&mut self, value: T) -> Option<T> {
        self.0
            .insert(T::ID, Box::new(value))
            .map(|x| *x.into_any_box().downcast().unwrap())
    }

    /// update or insert the given column, return the old value
    pub fn upsert_cell(&mut self, value: Box<dyn Cell>) -> Option<Box<dyn Any>> {
        self.0
            .insert((*value).cell_type_id(), value)
            .map(|x| *x.into_any_box().downcast().unwrap())
    }

    /// remove a column of type `T` from this row
    pub fn remove<T: Data>(&mut self) -> Option<T> {
        self.0.remove(&T::ID).map(|x| *x.into_any_box().downcast().unwrap())
    }

    /// Extends this row and replace existing columns
    ///
    /// returns the replaces columns
    pub fn extend<V: RowLike>(&mut self, value: V) -> Row {
        let mut old = AHashMap::new();
        for cell in value.into_cells() {
            let tid = (*cell).cell_type_id();
            match self.0.entry(tid) {
                std::collections::hash_map::Entry::Occupied(mut x) => {
                    let v = std::mem::replace(x.get_mut(), cell);
                    old.insert(tid, v);
                }
                std::collections::hash_map::Entry::Vacant(x) => {
                    x.insert(cell);
                }
            }
        }
        Row(old)
    }

    /// Extends this row and replace existing columns
    ///
    /// returns the replaces columns
    pub fn extend_row(&mut self, row: Row) -> Row {
        let mut old = AHashMap::new();
        for (x, y) in row {
            match self.0.entry(x) {
                std::collections::hash_map::Entry::Occupied(mut z) => {
                    let v = std::mem::replace(z.get_mut(), y);
                    old.insert(x, v);
                }
                std::collections::hash_map::Entry::Vacant(x) => {
                    x.insert(y);
                }
            }
        }
        Row(old)
    }

    /// returns true if the this grid contains these columns
    ///
    /// if `partial` is true, this grid should contains at least these columns.
    ///
    /// if `partial` is false this grid should contain exactly these columns
    ///
    /// For tuples of [Data], match grid panics if the same type is present more than once
    pub fn match_grid(&self, grid: &Grid, partial: bool) -> bool {
        (partial || self.len() == grid.len_cols())
            && self.0.keys().all(|x| grid.contains_type(x))
    }
}

impl IntoIterator for Row {
    type IntoIter = <AHashMap<TypeId, Box<dyn Cell>> as IntoIterator>::IntoIter;
    type Item = <AHashMap<TypeId, Box<dyn Cell>> as IntoIterator>::Item;
    fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
}

impl<'a> IntoIterator for &'a Row {
    type IntoIter = <&'a AHashMap<TypeId, Box<dyn Cell>> as IntoIterator>::IntoIter;
    type Item = <&'a AHashMap<TypeId, Box<dyn Cell>> as IntoIterator>::Item;
    fn into_iter(self) -> Self::IntoIter { self.0.iter() }
}

impl<'a> IntoIterator for &'a mut Row {
    type IntoIter = <&'a mut AHashMap<TypeId, Box<dyn Cell>> as IntoIterator>::IntoIter;
    type Item = <&'a mut AHashMap<TypeId, Box<dyn Cell>> as IntoIterator>::Item;
    fn into_iter(self) -> Self::IntoIter { self.0.iter_mut() }
}

impl FromIterator<Box<dyn Cell>> for Row {
    fn from_iter<T: IntoIterator<Item = Box<dyn Cell>>>(iter: T) -> Self {
        Row(iter.into_iter().map(|x| ((*x).cell_type_id(), x)).collect())
    }
}

impl From<Vec<Box<dyn Cell>>> for Row {
    fn from(value: Vec<Box<dyn Cell>>) -> Self { value.into_iter().collect() }
}

/// Trait that transforms something into a list of [Cell]s or [Column]s
///
/// This trait is automatically implemented for tuples of [Data]
pub trait RowLike: TryFromRow {
    /// returns true if the given [Grid] contains these columns
    ///
    /// if `partial` is true, the [Grid] should contains at least these columns.
    ///
    /// if `partial` is false the [Grid] should contain exactly these columns
    ///
    /// For tuples of [Data], match table panics if the same type is present more than once
    fn match_grid(grid: &Grid, partial: bool) -> bool;

    /// create new empty columns correcponding to this type
    fn new_columns() -> Vec<Box<dyn Column>> { Self::new_columns_with_capatity(0) }

    /// Creates new empty columns with the specified capacity `size`
    fn new_columns_with_capatity(size: usize) -> Vec<Box<dyn Column>>;

    /// Transform self into a vec of columns
    fn into_columns(self) -> Vec<Box<dyn Column>>;

    /// Transform self into a vec of cells
    fn into_cells(self) -> Vec<Box<dyn Cell>>;
}

/// Trait that creates arbitrary data from a row
pub trait TryFromRow: Sized {
    /// create a new object from a row, returns the remaining row
    fn try_from_row(row: Row) -> Result<(Self, Row), Row>;
}

impl<T: Data> RowLike for (T,) {
    fn match_grid(grid: &Grid, partial: bool) -> bool {
        grid.contains::<T>() && (partial || grid.len_cols() == Self::LEN)
    }

    fn new_columns_with_capatity(size: usize) -> Vec<Box<dyn Column>> {
        vec![Vec::<T>::with_capacity(size).into()]
    }

    fn into_columns(self) -> Vec<Box<dyn Column>> { vec![vec![self.0].into()] }

    fn into_cells(self) -> Vec<Box<dyn Cell>> { vec![Box::new(self.0)] }
}

impl<T: Data> TryFromRow for (T,) {
    fn try_from_row(mut row: Row) -> Result<(Self, Row), Row> {
        if row.contains::<T>() {
            Ok(((row.remove::<T>().unwrap(),), row))
        } else {
            Err(row)
        }
    }
}

macro_rules! columns_tuple_impl {
    ($_head:ident) => {};
    ($head:ident $($tail:ident) *) => {
        columns_tuple_impl!($($tail)*);

        impl<$head: Data, $($tail: Data), *> RowLike for ($head, $($tail), *) {
            fn match_grid(grid: &Grid, partial: bool) -> bool {
                assert_eq!(AHashSet::from([$head::ID, $($tail::ID), *]).len(), Self::LEN);
                grid.contains::<$head>() $(&& grid.contains::<$tail>()) * && (partial || grid.len_cols() == Self::LEN)
            }

            fn new_columns_with_capatity(n: usize) -> Vec<Box<dyn Column>> {
                assert_eq!(AHashSet::from([$head::ID, $($tail::ID), *]).len(), Self::LEN);
                vec![Vec::<$head>::with_capacity(n).into(), $(Vec::<$tail>::with_capacity(n).into()), *]
            }

            #[allow(non_snake_case)]
            fn into_columns(self) -> Vec<Box<dyn Column>> {
                assert_eq!(AHashSet::from([$head::ID, $($tail::ID), *]).len(), Self::LEN);
                let ($head, $($tail), *) = self;
                vec![vec![$head].into(), $(vec![$tail].into()), *]
            }

            #[allow(non_snake_case)]
            fn into_cells(self) -> Vec<Box<dyn Cell>> {
                assert_eq!(AHashSet::from([$head::ID, $($tail::ID), *]).len(), Self::LEN);
                let ($head, $($tail), *) = self;
                vec![Box::new($head), $(Box::new($tail)), *]
            }
        }

        impl<$head: Data, $($tail: Data), *> TryFromRow for ($head, $($tail), *) {
            fn try_from_row(mut row: Row) -> Result<(Self, Row), Row> {
                if row.contains::<$head>() $(&& row.contains::<$tail>()) * {
                    Ok(((row.remove::<$head>().unwrap(), $(row.remove::<$tail>().unwrap()), *), row))
                } else {
                    Err(row)
                }
            }
        }
    };
}

columns_tuple_impl!(T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15 T16 T17 T18 T19 T20 T21 T22 T23 T24 T25 T26 T27 T28 T29 T30 T31 T32);
