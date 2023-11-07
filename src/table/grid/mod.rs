//! Grid storage module

mod cell;
mod column;
mod row;

pub use self::{cell::*, column::*, row::*};
use crate::data::Data;
use ahash::AHashMap;
use std::any::TypeId;

/// Array of struct storage
///
/// Each columns contains the same number of rows
///
/// For example, a [`Grid`] containing 4 columns for types `A`, `B`, `C` and `D`:
///
/// |   row index   | A | B | C | D |
/// |---------------|---|---|---|---|
/// |       0       | A | B | C | D |
/// |       1       | A | B | C | D |
/// |      ...      | - | - | - | - |
#[derive(Debug, Default)]
pub struct Grid {
    columns: AHashMap<TypeId, Box<dyn Column>>,
}

impl Grid {
    /// create a new empty grid
    pub fn new() -> Grid {
        Grid::default()
    }

    /// create a new grid with the given columns
    pub fn new_with<V: RowLike>() -> Grid {
        Self::with_capacity::<V>(0)
    }

    /// create a new grid with the given columns and reserver slots for each columns
    pub fn with_capacity<V: RowLike>(n: usize) -> Grid {
        Grid {
            columns: V::new_columns_with_capatity(n)
                .into_iter()
                .map(|x| (x.column_type(), x))
                .collect(),
        }
    }

    /// return true if this grid contains the given data
    pub fn contains<T: Data>(&self) -> bool {
        self.columns.contains_key(&T::ID)
    }

    /// return true if this grid contains the given type
    pub fn contains_type(&self, ty: &TypeId) -> bool {
        self.columns.contains_key(ty)
    }

    /// Number of columns in this grid
    pub fn len_cols(&self) -> usize {
        self.columns.len()
    }

    /// return true if this grid has columns
    pub fn has_cols(&self) -> bool {
        !self.columns.is_empty()
    }

    /// Number of rows in this grid
    pub fn len_rows(&self) -> usize {
        self.columns.values().next().map_or(0, |x| x.len())
    }

    /// returns true if the grid has no rows
    pub fn is_empty(&self) -> bool {
        self.columns.values().next().map_or(true, |x| x.is_empty())
    }

    /// insert the given column in this grid
    ///
    /// if this grid is empty, anything can be inserted
    ///
    /// returns the provided columns if the insertion is not possible
    pub fn insert<V: RowLike>(&mut self, columns: V) -> Result<(), V> {
        if self.columns.is_empty() {
            self.columns = columns
                .into_columns()
                .into_iter()
                .map(|x| (x.column_type(), x))
                .collect();
            Ok(())
        } else if V::match_grid(self, false) {
            for cell in columns.into_cells() {
                self.columns
                    .get_mut(&(*cell).cell_type_id())
                    .unwrap()
                    .push_cell(cell);
            }
            Ok(())
        } else {
            Err(columns)
        }
    }

    /// insert the given column in this grid
    ///
    /// if this grid is empty, anything can be inserted
    ///
    /// returns the provided row if insertion fails
    pub fn insert_row(&mut self, row: Row) -> Result<(), Row> {
        if self.columns.is_empty() {
            self.columns = row.into_iter().map(|(x, y)| (x, y.into_column())).collect();
            Ok(())
        } else if row.match_grid(self, false) {
            for (x, y) in row {
                self.columns.get_mut(&x).unwrap().push_cell(y);
            }
            Ok(())
        } else {
            Err(row)
        }
    }

    /// update the given row
    ///
    /// update can be partial (ie: contain less columns than this grid)
    ///
    /// if the provided columns cannot be inserted an error is returned
    pub fn update<V: RowLike>(&mut self, index: usize, columns: V) -> Result<Row, V> {
        if V::match_grid(self, true) {
            let mut row = Row::default();
            for cell in columns.into_cells() {
                row.upsert_cell(
                    self.columns
                        .get_mut(&(*cell).cell_type_id())
                        .unwrap()
                        .replace(index, cell),
                );
            }
            Ok(row)
        } else {
            Err(columns)
        }
    }

    /// update the given row
    ///
    /// update can be partial (ie: contain less columns than this grid)
    ///
    /// if the provided columns cannot be inserted an error is returned
    pub fn update_row(&mut self, index: usize, row: Row) -> Result<Row, Row> {
        if row.match_grid(self, true) {
            let mut old = Row::default();
            for (x, y) in row {
                old.upsert_cell(self.columns.get_mut(&x).unwrap().replace(index, y));
            }
            Ok(old)
        } else {
            Err(row)
        }
    }

    /// remove swap the given row from this grid
    ///
    /// panics if the index is out of bounds
    pub fn swap_remove(&mut self, index: usize) -> Row {
        self.columns
            .iter_mut()
            .map(|(_, x)| x.swap_remove(index))
            .collect()
    }

    /// insert the given column in this grid
    ///
    /// if this grid has no defined, columns anything can be insert
    ///
    /// panics if the `columns` do no match the existing columns
    pub fn extend<V: RowLike>(&mut self, columns: impl IntoIterator<Item = V>) {
        if self.columns.is_empty() {
            *self = columns.into_iter().collect();
        } else if V::match_grid(self, false) {
            for x in columns {
                for y in x.into_cells() {
                    self.columns
                        .get_mut(&(*y).cell_type_id())
                        .unwrap()
                        .push_cell(y);
                }
            }
        } else {
            panic!("columns <=> grid missmatch")
        }
    }

    /// create a new reference to this grid
    pub fn grid_ref(&self) -> Ref<'_> {
        Ref {
            columns: self.columns.iter().map(|(x, y)| (*x, y.as_dyn())).collect(),
        }
    }

    /// create a new mutable reference to this grid
    pub fn grid_mut(&mut self) -> Mut<'_> {
        Mut {
            columns: self
                .columns
                .iter_mut()
                .map(|(x, y)| (*x, y.as_dyn_mut()))
                .collect(),
        }
    }
}

impl<V: RowLike> FromIterator<V> for Grid {
    fn from_iter<T: IntoIterator<Item = V>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut columns: AHashMap<TypeId, Box<dyn Column>> =
            V::new_columns_with_capatity(match iter.size_hint() {
                (_, Some(x)) => x,
                (x, _) => x,
            })
            .into_iter()
            .map(|x| ((*x).column_type(), x))
            .collect();
        for x in iter {
            for y in x.into_cells() {
                columns.get_mut(&(*y).cell_type_id()).unwrap().push_cell(y);
            }
        }
        Grid { columns }
    }
}

impl<V: RowLike> std::iter::Extend<V> for Grid {
    fn extend<T: IntoIterator<Item = V>>(&mut self, iter: T) {
        Grid::extend(self, iter)
    }
}

impl From<Row> for Grid {
    fn from(value: Row) -> Self {
        Grid {
            columns: value
                .into_iter()
                .map(|(x, y)| (x, y.into_column()))
                .collect(),
        }
    }
}

impl FromIterator<Box<dyn Column>> for Grid {
    fn from_iter<T: IntoIterator<Item = Box<dyn Column>>>(iter: T) -> Self {
        Grid {
            columns: iter.into_iter().map(|x| (x.column_type(), x)).collect(),
        }
    }
}

impl From<Vec<Box<dyn Column>>> for Grid {
    fn from(value: Vec<Box<dyn Column>>) -> Self {
        value.into_iter().collect()
    }
}

impl FromIterator<Box<dyn Cell>> for Grid {
    fn from_iter<T: IntoIterator<Item = Box<dyn Cell>>>(iter: T) -> Self {
        Grid {
            columns: iter
                .into_iter()
                .map(|x| ((*x).cell_type_id(), x.into_column()))
                .collect(),
        }
    }
}

impl From<Vec<Box<dyn Cell>>> for Grid {
    fn from(value: Vec<Box<dyn Cell>>) -> Self {
        value.into_iter().collect()
    }
}

/// A wrapper over a grid reference
#[derive(Default, Debug, Clone)]
pub struct Ref<'a> {
    columns: AHashMap<TypeId, &'a dyn Column>,
}

impl<'a> Ref<'a> {
    /// number of rows
    pub fn len_rows(&self) -> usize {
        self.columns.values().next().map_or(0, |x| x.len())
    }

    /// returns true if there is at least 1 row
    pub fn has_rows(&self) -> bool {
        self.columns
            .values()
            .next()
            .map_or(false, |x| !x.is_empty())
    }

    /// number of column
    pub fn len_cols(&self) -> usize {
        self.columns.len()
    }

    /// returns true if there is at least 1 column
    pub fn has_cols(&self) -> bool {
        !self.columns.is_empty()
    }

    /// returns true if this if a column of type `T` exists
    pub fn contains<T: Data>(&self) -> bool {
        self.columns.contains_key(&T::ID)
    }

    /// returns true if a column of [TypeId] exiss
    pub fn contains_type(&self, id: &TypeId) -> bool {
        self.columns.contains_key(id)
    }

    /// Get a slice to a column
    pub fn read<T: Data>(&self) -> Option<&'a [T]> {
        self.columns
            .get(&T::ID)
            .map(|x| x.as_dyn().as_slice::<T>().unwrap())
    }
}

/// A wrapper over a mutable grid
#[derive(Default, Debug)]
pub struct Mut<'a> {
    columns: AHashMap<TypeId, &'a mut dyn Column>,
}

impl<'a> Mut<'a> {
    /// returns true if there is at least 1 row
    pub fn len_rows(&self) -> usize {
        self.columns.values().next().map_or(0, |x| x.len())
    }

    /// returns true if there is at least 1 row
    pub fn has_rows(&self) -> bool {
        self.columns
            .values()
            .next()
            .map_or(false, |x| !x.is_empty())
    }

    /// number of column
    pub fn len_cols(&self) -> usize {
        self.columns.len()
    }

    /// returns true if there is at least 1 column
    pub fn has_cols(&self) -> bool {
        !self.columns.is_empty()
    }

    /// returns true if this if a column of type `T` exists
    pub fn contains<T: Data>(&self) -> bool {
        self.columns.contains_key(&T::ID)
    }

    /// returns true if a column of [TypeId] exiss
    pub fn contains_type(&self, id: &TypeId) -> bool {
        self.columns.contains_key(id)
    }

    /// Get a slice to a column
    pub fn read<T: Data>(&mut self, refs: &mut Ref<'a>) -> Option<&'a [T]> {
        if let std::collections::hash_map::Entry::Vacant(e) = refs.columns.entry(T::ID) {
            e.insert(self.columns.remove(&T::ID)?);
        }
        refs.read::<T>()
    }

    /// Get a mutable slice to a column
    pub fn write<T: Data>(&mut self) -> Option<&'a mut [T]> {
        self.columns
            .remove(&T::ID)
            .map(|x| x.as_mut_slice::<T>().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extend() {
        let mut grid = Grid::new();
        grid.extend([
            (10, "a", 11.0),
            (11, "b", 12.0),
            (12, "c", 13.0),
            (13, "d", 14.0),
            (14, "e", 16.0),
        ]);
        assert_eq!(grid.len_cols(), 3);
        assert_eq!(grid.len_rows(), 5);
        grid.extend([(10, "f", 11.0), (11, "g", 12.0), (12, "h", 13.0)]);
        assert_eq!(grid.len_cols(), 3);
        assert_eq!(grid.len_rows(), 8);
    }
}
