//! Abstract data & data identifier

use std::{
    any::{Any, TypeId},
    fmt::Debug,
};

mod seal {
    pub trait Seal {}
}

/// An abstract datatype
///
/// Automatically implemented for all types that can be inserted in a [grid][crate::table::Grid], [table][crate::Table], [store][crate::Store] or [core][crate::Core]
pub trait Data: Any + Send + Sync + Debug + seal::Seal {
    /// Const type id of this datatype
    const ID: TypeId;
}

impl<T: Any + Send + Sync + Debug> Data for T {
    const ID: TypeId = TypeId::of::<T>();
}

impl<T: Data> seal::Seal for T {}
