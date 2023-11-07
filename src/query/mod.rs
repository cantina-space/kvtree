//! Container queries & iterators
mod filter;
mod iter;

pub use self::{filter::*, iter::*};
use crate::{
    data::Data,
    key::Key,
    table::{MaskIter, View},
    Match,
};
#[cfg(feature = "rayon")]
use rayon::prelude::*;
use std::{cell::UnsafeCell, fmt::Debug, hash::Hash, marker::PhantomData};

/// Trait used to read/write data from containers
pub trait Query: Filter {
    /// Data type returned by this query
    type Item<'a>;
    /// Is this query read only
    const READ_ONLY: bool;

    /// Get a single element from a [View] corresponding to this query and the given key
    fn get<'a, K>(view: &mut View<'a, K>, key: &Key<K>) -> Option<Self::Item<'a>>
    where
        Key<K>: Eq + Hash;

    /// applies this query to the given view
    ///
    /// Items iteration order should be the same as view.keys()
    ///
    /// returns an iterator to the values and the remaining view
    fn items<'a, K>(
        view: View<'a, K>,
    ) -> (Option<impl QueryIter<Item = Self::Item<'a>>>, View<'a, K>);

    /// applies this query to the given view
    ///
    /// returns the remaining view and a function that returns a [Query::Item] for a given `row` index
    ///
    /// It's best to get the same index once
    ///
    /// to avoid UB: if this query is mutable, all references should be released before accessing the same index twice.
    fn by_index<'a, K>(
        view: View<'a, K>,
    ) -> (
        Option<impl (Fn(usize) -> Self::Item<'a>) + Send + Sync>,
        View<'a, K>,
    );

    /// Query parallel iterator over [Key]s and [Self::Item]
    #[cfg(feature = "rayon")]
    fn par_iter<'a, K>(
        view: View<'a, K>,
    ) -> Option<impl ParallelQueryIter<Item = (&'a Key<K>, Self::Item<'a>)>>
    where
        Self: 'a,
        K: Send + Sync,
        Self::Item<'a>: Send + Sync,
    {
        if let (Some(items), view) = Self::by_index(view) {
            Some(
                view.index()
                    .keys()
                    .par_iter()
                    .enumerate()
                    .map(move |(i, x)| (x, items(i))),
            )
        } else {
            None
        }
    }

    /// Query iterator over [Key]s and [Self::Item]
    fn iter<'a, K>(
        view: View<'a, K>,
    ) -> Box<dyn QueryIter<Item = (&'a Key<K>, Self::Item<'a>)> + 'a>
    where
        Self: 'a,
    {
        if let (Some(x), view) = Self::items(view) {
            Box::new(view.index().keys().iter().zip(x))
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Query iterator over [Key]s and [Self::Item] ordered by keys
    fn iter_indexed<'a, K>(
        view: View<'a, K>,
    ) -> Box<dyn QueryIter<Item = (&'a Key<K>, Self::Item<'a>)> + 'a>
    where
        Self: 'a,
    {
        if let (Some(x), view) = Self::by_index(view) {
            let ks = view.index().keys();
            Box::new(view.index().index().iter().map(move |i| (&ks[*i], x(*i))))
        } else {
            Box::new(std::iter::empty())
        }
    }
    /// Query iterator over [Key]s and [Self::Item] where keys are ordered and filtered by the given [mask](Match<K>)
    fn mask<'a, 'b, K>(
        view: View<'a, K>,
        mask: &'b impl Match<K>,
    ) -> impl QueryMaskIter<Item = (&'a Key<K>, Self::Item<'a>)>
    where
        Self: 'a,
        'b: 'a,
    {
        let index = view.index();
        let cols = match Self::by_index(view) {
            (Some(cols), _) => cols,
            (..) => return None.into_iter().flatten(),
        };
        Some(MaskIter::new(index, mask).map(move |(x, y)| (x, cols(y))))
            .into_iter()
            .flatten()
    }
}

fn any_value(_: usize) {}

impl Query for () {
    type Item<'a> = ();

    const READ_ONLY: bool = true;

    fn get<'a, K>(_: &mut View<'a, K>, _: &Key<K>) -> Option<Self::Item<'a>>
    where
        Key<K>: Eq + Hash,
    {
        Some(())
    }

    fn items<'a, K>(
        view: View<'a, K>,
    ) -> (Option<impl QueryIter<Item = Self::Item<'a>>>, View<'a, K>) {
        (Some(view.index().keys().iter().map(|_| ())), view)
    }

    fn by_index<'a, K>(
        view: View<'a, K>,
    ) -> (Option<impl (Fn(usize) -> Self::Item<'a>)>, View<'a, K>) {
        (Some(any_value), view)
    }
}

impl<T: Data> Query for &T {
    type Item<'a> = &'a T;

    const READ_ONLY: bool = true;

    fn get<'a, K>(view: &mut View<'a, K>, key: &Key<K>) -> Option<Self::Item<'a>>
    where
        Key<K>: Eq + Hash,
    {
        view.index_of(key).and_then(|x| Some(&view.read::<T>()?[x]))
    }

    fn items<'a, K>(
        mut view: View<'a, K>,
    ) -> (Option<impl QueryIter<Item = Self::Item<'a>>>, View<'a, K>) {
        (view.read::<T>().map(|x| x.iter()), view)
    }

    fn by_index<'a, K>(
        mut view: View<'a, K>,
    ) -> (Option<impl (Fn(usize) -> Self::Item<'a>)>, View<'a, K>) {
        (
            match view.read::<T>() {
                Some(rows) => Some(move |row_idx| &rows[row_idx]),
                None => None,
            },
            view,
        )
    }
}

impl<T: Data + Debug> Query for Option<&T> {
    type Item<'a> = Option<&'a T>;

    const READ_ONLY: bool = true;

    fn get<'a, K>(view: &mut View<'a, K>, key: &Key<K>) -> Option<Self::Item<'a>>
    where
        Key<K>: Eq + Hash,
    {
        view.index_of(key).map(|x| view.read::<T>().map(|y| &y[x]))
    }

    fn items<'a, K>(
        mut view: View<'a, K>,
    ) -> (Option<impl QueryIter<Item = Self::Item<'a>>>, View<'a, K>) {
        (
            view.read::<T>().map_or_else(
                || {
                    Some(Box::new((0..view.len_rows()).map(|_| None))
                        as Box<dyn QueryIter<Item = Self::Item<'_>>>)
                },
                |x| Some(Box::new(x.iter().map(Some)) as Box<dyn QueryIter<Item = Self::Item<'_>>>),
            ),
            view,
        )
    }

    fn by_index<'a, K>(
        mut view: View<'a, K>,
    ) -> (
        Option<impl (Fn(usize) -> Self::Item<'a>) + Send + Sync>,
        View<'a, K>,
    ) {
        (
            if let Some(rows) = view.read::<T>() {
                Some(Box::new(|row_idx| Some(&rows[row_idx]))
                    as Box<dyn (Fn(usize) -> Self::Item<'a>) + Send + Sync>)
            } else {
                Some(Box::new(|_| None))
            },
            view,
        )
    }
}

/// Wrapper over slice of [UnsafeCell]
///
/// Should only be used to access disjoint slice index
#[derive(Copy, Clone)]
struct UnsafeSlice<'a, T>(&'a [UnsafeCell<T>]);

unsafe impl<'a, T: Send + Sync> Send for UnsafeSlice<'a, T> {}
unsafe impl<'a, T: Send + Sync> Sync for UnsafeSlice<'a, T> {}

impl<'a, T> UnsafeSlice<'a, T> {
    fn new(slice: &'a mut [T]) -> Self {
        // SAFETY: `UnsafeCell` has the same layout as T
        Self(unsafe { &*(slice as *mut [T] as *const [UnsafeCell<T>]) })
    }

    /// get a mutable reference to the element at position `index`
    ///
    /// SAFETY: access to elements is safe for any unique index.
    unsafe fn get_mut(&self, index: usize) -> &'a mut T {
        &mut *self.0[index].get()
    }
}

#[inline(always)]
fn random_access_mut<'a, T: Send + Sync>(
    slice: &'a mut [T],
) -> impl (Fn(usize) -> &'a mut T) + Send + Sync {
    let slice = UnsafeSlice::new(slice);
    // Safety: access of elements is done by disjoint indices in a specific order
    #[inline(always)]
    move |idx| unsafe { slice.get_mut(idx) }
}

impl<T: Data> Query for &mut T {
    type Item<'a> = &'a mut T;

    const READ_ONLY: bool = false;

    fn get<'a, K>(view: &mut View<'a, K>, key: &Key<K>) -> Option<Self::Item<'a>>
    where
        Key<K>: Eq + Hash,
    {
        view.index_of(key)
            .and_then(|x| Some(&mut view.write::<T>()?[x]))
    }

    fn items<'a, K>(
        mut view: View<'a, K>,
    ) -> (Option<impl QueryIter<Item = Self::Item<'a>>>, View<'a, K>) {
        (view.write::<T>().map(|x| x.iter_mut()), view)
    }

    fn by_index<'a, K>(
        mut view: View<'a, K>,
    ) -> (Option<impl (Fn(usize) -> Self::Item<'a>)>, View<'a, K>) {
        (view.write::<T>().map(random_access_mut), view)
    }
}

fn none_opt<I>(_: usize) -> Option<I> {
    None
}

impl<T: Data> Query for Option<&mut T> {
    type Item<'a> = Option<&'a mut T>;

    const READ_ONLY: bool = false;

    fn get<'a, K>(view: &mut View<'a, K>, key: &Key<K>) -> Option<Self::Item<'a>>
    where
        Key<K>: Eq + Hash,
    {
        view.index_of(key)
            .map(|x| view.write::<T>().map(|y| &mut y[x]))
    }

    fn items<'a, K>(
        mut view: View<'a, K>,
    ) -> (Option<impl QueryIter<Item = Self::Item<'a>>>, View<'a, K>) {
        (
            view.write::<T>().map_or_else(
                || {
                    Some(Box::new((0..view.len_rows()).map(|_| None))
                        as Box<dyn QueryIter<Item = Self::Item<'_>>>)
                },
                |x| {
                    Some(Box::new(x.iter_mut().map(Some))
                        as Box<dyn QueryIter<Item = Self::Item<'_>>>)
                },
            ),
            view,
        )
    }

    fn by_index<'a, K>(
        mut view: View<'a, K>,
    ) -> (
        Option<impl (Fn(usize) -> Self::Item<'a>) + Send + Sync>,
        View<'a, K>,
    ) {
        (
            if let Some(rows) = view.write::<T>().map(random_access_mut) {
                Some(Box::new(move |row_idx| Some(rows(row_idx)))
                    as Box<dyn (Fn(usize) -> Self::Item<'a>) + Send + Sync>)
            } else {
                Some(Box::new(none_opt))
            },
            view,
        )
    }
}

impl<Q: Query + 'static> Query for (Q,) {
    type Item<'a> = (Q::Item<'a>,);

    const READ_ONLY: bool = Q::READ_ONLY;

    fn get<'a, K>(view: &mut View<'a, K>, key: &Key<K>) -> Option<Self::Item<'a>>
    where
        Key<K>: Eq + Hash,
    {
        Q::get(view, key).map(|x| (x,))
    }

    fn items<'a, K>(
        view: View<'a, K>,
    ) -> (Option<impl QueryIter<Item = Self::Item<'a>>>, View<'a, K>) {
        let (q, view) = Q::items(view);
        (q.map(|x| x.map(|x| (x,))), view)
    }

    fn by_index<'a, K>(
        view: View<'a, K>,
    ) -> (Option<impl Fn(usize) -> Self::Item<'a>>, View<'a, K>) {
        match Q::by_index(view) {
            (Some(x), view) => (Some(move |y| (x(y),)), view),
            (None, view) => (None, view),
        }
    }
}

/// Query that returns an item if at least one of the inner queries returns an item
pub struct AnyOf<Q>(PhantomData<Q>);

macro_rules! query_impl {
    ($_head:ident) => {};
    ($head:ident $($tail:ident) *) => {
        query_impl!($($tail) *);

        impl<$head: Query + 'static, $($tail: Query + 'static), *> Query for ($head, $($tail), *)
        {
            type Item<'a> = ($head::Item<'a>, $($tail::Item<'a>), *);

            const READ_ONLY: bool = $head::READ_ONLY $(&& $tail::READ_ONLY) *;

            fn get<'a, K>(view: &mut View<'a, K>, key: &Key<K>) -> Option<Self::Item<'a>>
            where
                Key<K>: Eq + Hash,
            {
                use tuplify::ValidateOpt;
                ($head::get(view, key), $($tail::get(view, key)), *).validate()
            }

            #[allow(non_snake_case)]
            fn items<'a, K>(
                view: View<'a, K>,
            ) -> (Option<impl QueryIter<Item = Self::Item<'a>>>, View<'a, K>) {
                use itertools::izip;
                let ($head, view) = match $head::items(view) {
                    (Some(x), y) => (x, y),
                    (None, x) => return (None, x)
                };
                $(let ($tail, view) = match $tail::items(view) {
                    (Some(x), y) => (x, y),
                    (None, x) => return (None, x)
                };) *
                (Some(izip!($head, $($tail), *)), view)
            }

            #[allow(non_snake_case)]
            fn by_index<'a, K>(
                view: View<'a, K>,
            ) -> (Option<impl (Fn(usize) -> Self::Item<'a>) + Send + Sync >, View<'a, K>) {
                let ($head, view) = match $head::by_index(view) {
                    (Some(x), y) => (x, y),
                    (None, x) => return (None, x)
                };
                $(let ($tail, view) = match $tail::by_index(view) {
                    (Some(x), y) => (x, y),
                    (None, x) => return (None, x)
                };) *
                (Some(move |index| ($head(index), $($tail(index)), *)), view)
            }
        }

        impl<$head: Filter + 'static, $($tail: Filter + 'static), *> Filter for AnyOf<($head, $($tail), *)> {
            fn match_view<K>(view: &View<K>) -> bool { $head::match_view(view) $(|| $tail::match_view(view)) * }
        }

        impl<$head: Query + 'static, $($tail: Query + 'static), *> Query for AnyOf<($head, $($tail), *)> {
            type Item<'a> = (Option<<$head as Query>::Item<'a>>, $(Option<<$tail as Query>::Item<'a>>), *);

            const READ_ONLY: bool = $head::READ_ONLY $(&& $tail::READ_ONLY) *;

            #[allow(non_snake_case)]
            fn get<'a, K>(view: &mut View<'a, K>, key: &Key<K>) -> Option<Self::Item<'a>>
            where
                Key<K>: Eq + Hash,
            {
                let ($head, $($tail), *) = ($head::get(view, key), $($tail::get(view, key)), *);
                if $head.is_some() $(|| $tail.is_some()) * {
                    Some(($head, $($tail), *))
                } else {
                    None
                }
            }

            #[allow(non_snake_case)]
            fn items<'a, K>(
                view: View<'a, K>,
            ) -> (Option<impl QueryIter<Item = Self::Item<'a>>>, View<'a, K>) {
                use itertools::izip;
                let mut any = true;
                let len_rows = view.len_rows();
                let ($head, view) = $head::items(view);
                let $head = if let Some(x) = $head {
                    any &= true;
                    Box::new(x.map(Some)) as Box<dyn QueryIter<Item = Option<<$head as Query>::Item<'_>>> + '_>
                } else {
                    Box::new((0..len_rows).map(|_| None))
                };
                $(
                    let ($tail, view) = $tail::items(view);
                    let $tail = if let Some(x) = $tail {
                        any &= true;
                        Box::new(x.map(Some)) as Box<dyn QueryIter<Item = Option<<$tail as Query>::Item<'_>>> + '_>
                    } else {
                        Box::new((0..len_rows).map(|_| None))
                    };
                ) *
                (if any { Some(izip!($head, $($tail), *)) } else { None }, view)
            }

            #[allow(non_snake_case)]
            fn by_index<'a, K>(
                view: View<'a, K>,
            ) -> (Option<impl (Fn(usize) -> Self::Item<'a>) + Send + Sync >, View<'a, K>) {
                let mut any = false;
                let ($head, view) = match $head::by_index(view) {
                    (None, x) => (Box::new(|_| None) as Box<dyn (Fn(usize) -> Option<$head::Item<'a>>) + Send + Sync >, x),
                    (Some(x), y) => ({ any |= true; Box::new(move |y| Some(x(y))) as Box<dyn (Fn(usize) -> Option<$head::Item<'a>>) + Send + Sync > }, y),
                };
                $(let ($tail, view) = match $tail::by_index(view) {
                    (None, x) => (Box::new(|_| None) as Box<dyn (Fn(usize) ->  Option<$tail::Item<'a>>) + Send + Sync >, x),
                    (Some(x), y) => ({ any |= true; Box::new(move |y| Some(x(y))) as Box<dyn (Fn(usize) ->  Option<$tail::Item<'a>>) + Send + Sync > }, y),
                };) *
                (if any { Some(move |index| ($head(index), $($tail(index)), *)) } else { None }, view)
            }
        }
    };
}

query_impl!(T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15 T16 T17 T18 T19 T20 T21 T22 T23 T24 T25 T26 T27 T28 T29 T30 T31 T32);
