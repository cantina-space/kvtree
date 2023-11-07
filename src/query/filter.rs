//! Data filtering
use crate::{data::Data, table::View};
use std::marker::PhantomData;

/// Trait to match against a [`View`]
pub trait Filter {
    /// returns true if the current filter matches the given [View]
    fn match_view<K>(view: &View<K>) -> bool;
}

impl<T: Data> Filter for &T {
    fn match_view<K>(view: &View<K>) -> bool { view.contains::<T>() }
}

impl<T: Data> Filter for Option<&T> {
    fn match_view<K>(_: &View<K>) -> bool { true }
}

impl<T: Data> Filter for &mut T {
    fn match_view<K>(view: &View<K>) -> bool { view.contains::<T>() }
}

impl<T: Data> Filter for Option<&mut T> {
    fn match_view<K>(_: &View<K>) -> bool { true }
}

/// Filters that matches if `T` is available
pub struct With<T>(PhantomData<T>);

impl<T: Data> Filter for With<T> {
    #[inline]
    fn match_view<K>(view: &View<K>) -> bool { view.contains::<T>() }
}

/// Filters that matches if `T` is not available
pub struct Without<T>(PhantomData<T>);

impl<T: Data> Filter for Without<T> {
    #[inline]
    fn match_view<K>(view: &View<K>) -> bool { !view.contains::<T>() }
}

/// Matches if all of the data is present
pub struct AllOf<T>(PhantomData<T>);

/// Matches if none of the data is present
pub struct NoneOf<T>(PhantomData<T>);

/// Inverts the filter result
pub struct Not<T>(PhantomData<T>);

/// Matches if at least one the inner filter matches
pub struct Or<T>(PhantomData<T>);

/// Matches if only one of inner filter matches (exclusive or)
pub struct OneOf<T>(PhantomData<T>);

/// always true [`Filter`]
impl Filter for () {
    #[inline]
    fn match_view<K>(_: &View<K>) -> bool { true }
}

macro_rules! filter_impl {
    () => {};
    ($head:ident $($tail:ident) *) => {
        filter_impl!($($tail) *);

        impl<$head: Filter, $($tail: Filter), *> Filter for ($head, $($tail), *) {
            #[inline]
            fn match_view<K>(view: &View<K>) -> bool { $head::match_view(view) $(&& $tail::match_view(view)) * }
        }

        impl<$head: Data, $($tail: Data), *> Filter for AllOf<($head, $($tail), *)> {
            #[inline]
            fn match_view<K>(view: &View<K>) -> bool {  With::<$head>::match_view(view) $(&& With::<$tail>::match_view(view)) * }
        }

        impl<$head: Data, $($tail: Data), *> Filter for NoneOf<($head, $($tail), *)> {
            #[inline]
            fn match_view<K>(view: &View<K>) -> bool {  Without::<$head>::match_view(view) $(&& Without::<$tail>::match_view(view)) * }
        }

        impl<$head: Filter, $($tail: Filter), *> Filter for Not<($head, $($tail), *)> {
            #[inline]
            fn match_view<K>(view: &View<K>) -> bool { !$head::match_view(view) $(&& !$tail::match_view(view)) * }
        }

        impl<$head: Filter, $($tail: Filter), *> Filter for Or<($head, $($tail), *)> {
            #[inline]
            fn match_view<K>(view: &View<K>) -> bool { $head::match_view(view) $(|| $tail::match_view(view)) * }
        }

        impl<$head: Filter, $($tail: Filter), *> Filter for OneOf<($head, $($tail), *)> {
            #[inline]
            fn match_view<K>(view: &View<K>) -> bool {  [$head::match_view(view) $(, $tail::match_view(view)) *].into_iter().filter(|x| *x).count() == 1 }
        }
    };
}

filter_impl!(T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12 T13 T14 T15 T16 T17 T18 T19 T20 T21 T22 T23 T24 T25 T26 T27 T28 T29 T30 T31 T32);
