#![warn(missing_docs)]
#![doc = include_str!("../readme.md")]
#![feature(const_type_id)]
#[cfg(feature = "core-storage")]
pub mod core;
pub mod query;
pub mod store;
pub mod table;

mod data;
pub mod key;
pub mod mask;

#[cfg(feature = "core-storage")]
pub use self::core::Core;
pub use self::{
    data::*,
    key::{Key, Match},
    mask::Mask,
    query::{Filter, Query},
    store::Store,
    table::Table,
};
