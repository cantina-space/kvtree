# Kvtree

[![Crate link]][crates.io] [![Documentation]][docs.rs] ![License]

This library contains multiple in memory key/value containers with hierarchic keys and heterogenous containers so an entry attached to a key can have a variable number of values.

A query system allows to filter by key and/or associated data.

## Keys & Masks

Keys are represented as an array of elements (user defined) groupable by common prefix.

Masks can match one or more keys.

```rust
use kvtree::{Key, Mask, Match, mask::Atom, key::Ordering};

let parent = Key::new(1);
let mid = Key::from([1, 2]);
let child = Key::from([1, 2, 3]);

assert_eq!(parent.key_cmp(&child), Ordering::Parent);
assert_eq!(mid.key_cmp(&child), Ordering::Parent);
assert_eq!(child.key_cmp(&parent), Ordering::Child);

// matches any key that starts with `1` and has 1 or more parts after that
let mask = Mask::<i32, ()>::from([Atom::Key(1), Atom::Many]);

assert!(!mask.match_key(&parent));
assert!(mask.match_key(&child));
assert!(mask.match_key(&mid));
assert!(mask.match_children(&mid));
assert!(mask.match_children(&parent));
```

## Containers

[Store](#kvtree-store) or [Core](#kvtree-core) are the main containers in this library but building blocks are exposed for convenience.

### Grid

Grids contains homogenous data where each column in the grid is a unique type.

Example: a grid with A, B, C, D as column types.
```text
+---------------+---+---+---+---+
|   row index   | A | B | C | D |
|---------------|---|---|---|---|
|       0       | A | B | C | D |
|       1       | A | B | C | D |
|      ...      | - | - | - | - |
+---------------+---+---+---+---+
```

### Index

An index stores 0 or more keys and an index of keys by prefix, this allows to view the keys in an index as a tree.

Insertion and removals behaviour is the same as grids. This means that if both operations are done in tandem, keys indexes matches grid row index.

### Table

Built on top of a grid and an index, associates a key index (tree index) with a grid index.

example: a table with A, B, C, D as column types.

```text
+---------+---+---+---+---+
|   key   | A | B | C | D |
|---------|---|---|---|---|
|   foo   | A | B | C | D |
| foo.bar | A | B | C | D |
|   ...   | - | - | - | - |
+---------+---+---+---+---+
```

```rust
use kvtree::{Key, Table};

let k1 = Key::from(["foo", "bar"]);
let k2 = Key::from(["foo", "baz"]);

let mut table = Table::new();

table.insert(k1.clone(), ("hello", 123i32, 1u8)).unwrap();
table.insert(k2.clone(), ("world", 456i32, 2u8)).unwrap();

*table.get_mut::<&mut u8>(&k1).unwrap() *= 10;

assert_eq!(table.get::<&u8>(&Key::from(["foo", "bar"])), Some(&10));
assert_eq!(
    table.iter_indexed::<(&u8, &&str)>().collect::<Vec<_>>(),
    vec![(&k1, (&10, &"hello")), (&k2, (&2, &"world"))]
);
```

### <a name="kvtree-store"></a>Store

A Store is a collection of tables with unique keys. 

Tables in a store are automatically managed depending on data associated to keys.

```rust
use kvtree::{Key, Store};

let k1 = Key::from(["foo", "bar"]);
let k2 = Key::from(["foo", "baz"]);

let mut store = Store::new();

store.insert(k1.clone(), ("hello", 123i32, 1u8)).unwrap();
store.insert(k2.clone(), ("world", false)).unwrap();

assert_eq!(store.len_tables(), 2);

assert_eq!(
    store.iter::<(&u8, &&str)>().collect::<Vec<_>>(),
    vec![(&k1, (&1, &"hello"))]
);

assert_eq!(
    store.iter_indexed::<&&str>().collect::<Vec<_>>(),
    vec![(&k1, &"hello"), (&k2, &"world")]
);

store.remove(&k1);

assert_eq!(store.len_tables(), 1);
```

###  <a name="kvtree-core"></a>Core

This structure wraps a LIFO stack of stores and access control.

Access control modes are associated to keys and masks.

4 different access modes exist (in priority order):

- Deny: blocks read & write access on the current and previous layers
- Read only: on this layer on current and previous layers
- New: behaves like it's the first layer where the key is present (prevents reading and writing for previous layers, can reset previous modes)
- Copy on write: can read the value in previous layer, write on current
- Reference: allowed to read or write the data directly both in current or parent layer.

Only the access modes of the last layer of the stack can be modified.

Layers can be dynamically created/removed, allowing to fine tune access to keys.

Access modes are effective from a given layer up to the first store on the stack (from top to bottom).

```rust
#[cfg(feature = "core-storage")] {
use kvtree::{core::Access, store::UpdateOpt, Core, Key};

let mut core = Core::<_, ()>::new();
let k1 = Key::from(["foo", "bar"]);
let k2 = Key::from(["foo", "baz"]);
let k3 = Key::from(["foo", "boo"]);

core.insert(k1.clone(), ("abc", 123)).unwrap();
core.insert(k2.clone(), (22,)).unwrap();
core.insert(k3.clone(), (23,)).unwrap();

assert_eq!(core.get::<&i32>(&k1), Some(&123));

core.set_key_access(k1.clone(), Access::Deny);

assert_eq!(core.get::<&i32>(&k1), None);

core.push_layer();

core.set_key_access(k1.clone(), Access::New);
core.set_key_access(k2.clone(), Access::Cow);
core.set_key_access(k3.clone(), Access::Ref);

assert_eq!(core.get::<&i32>(&k2), Some(&22));
assert_eq!(core.get::<&i32>(&k1), None);

core.upsert(k1.clone(), ("new value", false), UpdateOpt::Replace).unwrap();

assert_eq!(core.get::<&bool>(&k1), Some(&false));

core.upsert(k2.clone(), ("new value", true), UpdateOpt::Replace).unwrap();

assert_eq!(core.get::<&i32>(&k2), None);
assert_eq!(core.get::<&bool>(&k2), Some(&true));

core.upsert(k3.clone(), ("updated", true), UpdateOpt::Update).unwrap();

assert_eq!(
    core.get::<(&bool, &&str, &i32)>(&k3),
    Some((&true, &"updated", &23))
);

core.pop_layer();

assert_eq!(
    core.get::<(&&str, &i32, &bool)>(&k3),
    Some((&"updated", &23, &true))
);
assert_eq!(core.get::<&i32>(&k1), None);
assert_eq!(core.get::<&i32>(&k2), Some(&22));
}
```

## Contribution

Found a problem or have a suggestion? Feel free to open an issue.

[Crate link]: https://img.shields.io/crates/v/kvtree.svg
[crates.io]: https://crates.io/crates/kvtree/
[Documentation]: https://docs.rs/kvtree/badge.svg
[docs.rs]: https://docs.rs/kvtree
[trait_doc]: https://docs.rs/kvtree/latest/kvtree/index.html#traits
[License]: https://img.shields.io/crates/l/kvtree.svg