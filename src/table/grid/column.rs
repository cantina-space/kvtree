use crate::{data::Data, table::grid::Cell};
use std::{
    any::{Any, TypeId},
    fmt::Debug,
};

mod seal {
    pub trait Seal {}
}

/// Column type wrapper
pub trait Column: Any + Send + Sync + Debug + seal::Seal {
    /// DataId of column elements
    fn column_type(&self) -> TypeId;

    /// number of elements
    fn len(&self) -> usize;

    /// return true if this column is empty
    fn is_empty(&self) -> bool;

    /// Inserts the given item at the end of this column
    /// panics if item is not of the same type as the items in this column
    fn push_cell(&mut self, item: Box<dyn Cell>);

    /// Inserts the given item at the end of this column
    ///
    /// panics if item is not of the same type as the items in this column
    fn extend_with(&mut self, item: Box<dyn Column>);

    /// Swap remove an element at the given index
    fn swap_remove(&mut self, index: usize) -> Box<dyn Cell>;

    /// replace the element at the given index
    ///
    /// panics if index is > to len or if the type of `value` do not match this column
    fn replace(&mut self, index: usize, value: Box<dyn Cell>) -> Box<dyn Cell>;

    /// Reference to [Any]
    fn as_any_ref(&self) -> &dyn Any;

    /// Mutable reference to [Any]
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// transforms itself into [Any]
    fn into_box(self: Box<Self>) -> Box<dyn Any>;

    /// get a reference of [Column] from self
    fn as_dyn(&self) -> &dyn Column;

    /// get a mutable reference of [Column] from self
    fn as_dyn_mut(&mut self) -> &mut dyn Column;
}

impl<T: Data> seal::Seal for Vec<T> {}

impl<T: Data> Column for Vec<T> {
    fn column_type(&self) -> TypeId { T::ID }

    fn len(&self) -> usize { Vec::<T>::len(self) }

    fn is_empty(&self) -> bool { Vec::<T>::is_empty(self) }

    fn push_cell(&mut self, item: Box<dyn Cell>) {
        self.push(*item.into_any_box().downcast().unwrap())
    }

    fn extend_with(&mut self, item: Box<dyn Column>) {
        self.extend(*item.into_box().downcast::<Vec<T>>().unwrap())
    }

    fn swap_remove(&mut self, index: usize) -> Box<dyn Cell> {
        Box::new(Vec::<T>::swap_remove(self, index))
    }

    fn as_any_ref(&self) -> &dyn Any { self }

    fn as_any_mut(&mut self) -> &mut dyn Any { self }

    fn into_box(self: Box<Self>) -> Box<dyn Any> { self }

    fn replace(&mut self, index: usize, value: Box<dyn Cell>) -> Box<dyn Cell> {
        Box::new(std::mem::replace(
            &mut self[index],
            *value.into_any_box().downcast().unwrap(),
        ))
    }

    fn as_dyn(&self) -> &dyn Column { self }

    fn as_dyn_mut(&mut self) -> &mut dyn Column { self }
}

impl dyn Column {
    /// Try to cast this column into a [Vec]
    pub fn as_mut_vec<T>(&mut self) -> Option<&mut Vec<T>>
    where
        T: Data,
    {
        self.as_any_mut().downcast_mut::<Vec<T>>()
    }

    /// Try to cast this column into a [`std::slice`]
    pub fn as_slice<T>(&self) -> Option<&[T]>
    where
        T: Data,
    {
        self.as_any_ref().downcast_ref::<Vec<T>>().map(|x| x.as_slice())
    }

    /// Try to cast this column into a mutable [`std::slice`]
    pub fn as_mut_slice<T>(&mut self) -> Option<&mut [T]>
    where
        T: Data,
    {
        self.as_any_mut().downcast_mut::<Vec<T>>().map(|x| x.as_mut_slice())
    }
}

impl<T: Data> From<Vec<T>> for Box<dyn Column> {
    fn from(value: Vec<T>) -> Box<dyn Column> { Box::new(value) }
}

impl<I: Data> FromIterator<I> for Box<dyn Column> {
    fn from_iter<T: IntoIterator<Item = I>>(iter: T) -> Self {
        Box::new(iter.into_iter().collect::<Vec<I>>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push() {
        let mut vcol: Box<dyn Column> = Box::new(vec![1, 2, 3, 4]);
        assert_eq!(vcol.len(), 4);
        assert_eq!(vcol.column_type(), i32::ID);
        vcol.push_cell(Box::new(11));
        assert_eq!(vcol.len(), 5);
        assert_eq!(vcol.as_slice::<i32>().unwrap(), &[1, 2, 3, 4, 11]);
    }

    #[test]
    fn test_extend() {
        let mut vcol: Box<dyn Column> = Box::new(vec![1, 2, 3, 4]);
        assert_eq!(vcol.column_type(), i32::ID);
        vcol.extend_with(Box::new(vec![11, 12, 13]));
        assert_eq!(vcol.len(), 7);
        assert_eq!(vcol.as_slice::<i32>().unwrap(), &[1, 2, 3, 4, 11, 12, 13]);
    }

    #[test]
    fn test_swap_remove() {
        let mut vcol: Box<dyn Column> = Box::new(vec!["a", "b", "c", "d"]);
        assert_eq!(vcol.len(), 4);
        assert_eq!(vcol.column_type(), <&'static str>::ID);
        assert_eq!(*vcol.swap_remove(2).into_any_box().downcast::<&str>().unwrap(), "c");
        assert_eq!(vcol.as_slice::<&str>().unwrap(), &["a", "b", "d"]);
        assert_eq!(*vcol.swap_remove(0).into_any_box().downcast::<&str>().unwrap(), "a");
        assert_eq!(vcol.as_slice::<&str>().unwrap(), &["d", "b"]);
    }

    #[test]
    fn test_replace() {
        let mut vcol: Box<dyn Column> = Box::new(vec!["a", "b", "c", "d"]);
        assert_eq!(vcol.column_type(), <&'static str>::ID);
        vcol.replace(0, Box::new("foo"));
        vcol.replace(2, Box::new("bar"));
        assert_eq!(vcol.as_slice::<&str>().unwrap(), &["foo", "b", "bar", "d"]);
    }

    #[test]
    #[should_panic]
    fn test_push_panic() {
        let mut vcol: Box<dyn Column> = Box::new(vec![1, 2, 3, 4]);
        vcol.push_cell(Box::new("abc"));
    }

    #[test]
    #[should_panic]
    fn test_extend_panic() {
        let mut vcol: Box<dyn Column> = Box::new(vec![1, 2, 3, 4]);
        vcol.extend_with(Box::new(vec!["abc", "def"]));
    }

    #[test]
    #[should_panic]
    fn test_swap_remove_panic() {
        let mut vcol: Box<dyn Column> = Box::new(vec![0]);
        vcol.swap_remove(1);
    }
}
