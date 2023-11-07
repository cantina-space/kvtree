use crate::{data::Data, table::grid::Column};
use std::{
    any::{Any, TypeId},
    fmt::Debug,
};

mod seal {
    pub trait Seal {}
}

/// single element of [crate::table::Row]s and [crate::table::Grid]s
pub trait Cell: Send + Sync + 'static + Debug + seal::Seal {
    /// type id of this Cell
    ///
    /// /!\ Similarly to [Any] this method can be called on containers
    ///
    /// ie: a [`Box<dyn Cell>`] should be transformed into a `&dyn Cell` if one want to get the correct [TypeId]
    fn cell_type_id(&self) -> TypeId;
    /// wraps the inner element in a box
    fn into_any_box(self: Box<Self>) -> Box<dyn Any>;
    /// create a new cell from with item
    fn into_cell(self) -> Box<dyn Cell>;
    /// create a new column with this item
    fn into_column(self: Box<Self>) -> Box<dyn Column>;
}

impl<T: Cell> seal::Seal for T {}

impl<T: Data> Cell for T {
    fn cell_type_id(&self) -> TypeId { T::ID }
    fn into_any_box(self: Box<Self>) -> Box<dyn Any> { self }
    fn into_cell(self) -> Box<dyn Cell> { Box::new(self) }
    fn into_column(self: Box<Self>) -> Box<dyn Column> { vec![*self].into() }
}
