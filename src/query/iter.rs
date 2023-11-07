#[cfg(feature = "rayon")]
use rayon::prelude::{IndexedParallelIterator, ParallelIterator};

/// Wrapper over [DoubleEndedIterator] for queries
pub trait QueryMaskIter: DoubleEndedIterator {}

impl<T> QueryMaskIter for T where T: DoubleEndedIterator {}

/// Wrapper over [DoubleEndedIterator] + [ExactSizeIterator] for queries
pub trait QueryIter: QueryMaskIter + ExactSizeIterator {}

impl<T> QueryIter for T where T: QueryMaskIter + ExactSizeIterator {}

/// Wrapper over [ParallelIterator] for queries
#[cfg(feature = "rayon")]
pub trait ParallelQueryMaskIter: ParallelIterator {}

#[cfg(feature = "rayon")]
impl<T> ParallelQueryMaskIter for T where T: ParallelIterator {}

/// Wrapper over [ParallelIterator] + [IndexedParallelIterator] for queries
#[cfg(feature = "rayon")]
pub trait ParallelQueryIter: ParallelQueryMaskIter + IndexedParallelIterator {}

#[cfg(feature = "rayon")]
impl<T> ParallelQueryIter for T where T: ParallelQueryMaskIter + IndexedParallelIterator {}
