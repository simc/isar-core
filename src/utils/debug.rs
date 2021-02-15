#![cfg(test)]

use crate::lmdb::cursor::Cursor;
use hashbrown::{HashMap, HashSet};
use std::hash::Hash;

#[macro_export]
macro_rules! map (
    ($($key:expr => $value:expr),+) => {
        {
            let mut m = ::hashbrown::HashMap::new();
            $(m.insert($key, $value);)+
            m
        }
    };
);

#[macro_export]
macro_rules! set (
    [$($val:expr),+] => {
        {
            let mut s = ::hashbrown::HashSet::new();
            $(s.insert($val);)+
            s
        }
    };
);

#[macro_export]
macro_rules! isar (
    (path: $path:ident, $isar:ident, $($col:ident => $schema:expr),+) => {
        let cols = vec![$($schema,)+];
        let schema = crate::schema::Schema::new(cols).unwrap();
        let $isar = crate::instance::IsarInstance::open($path, 10000000, schema).unwrap();
        $(
            let col = $schema;
            let $col = $isar.get_collection_by_name(&col.name).unwrap();
        )+
    };

    ($isar:ident, $($col:ident => $schema:expr),+) => {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().to_str().unwrap();
        isar!(path: path, $isar, $($col => $schema),+);
    };
);

#[macro_export]
macro_rules! col (
    ($($field:expr => $type:path),+) => {
        col!($($field => $type),+;);
    };

    ($($field:expr => $type:path),+; $($index:expr),*) => {
        col!(stringify!($($field)+), $($field => $type),+; $($index),*)
    };

    ($name:expr, $($field:expr => $type:path),+) => {
        col!($name, $($field => $type),+;);
    };

    ($name:expr, $($field:expr => $type:path),+; $($index:expr),*) => {
        {
            let mut properties = vec![];
            col!(add_property properties, true, $($field => $type,)+);
            let mut indexes = vec![];
            indexes.clear();
            $(
                let (fields, unique) = $index;
                let index = crate::schema::collection_schema::IndexSchema::new(fields, unique);
                indexes.push(index);
            )*
            crate::schema::collection_schema::CollectionSchema::new($name, properties, indexes)
        }
    };

    (add_property $vec:expr, $oid:expr, $field:expr => $type:path, $($fields:expr => $types:path,)*) => {
        let property = crate::schema::collection_schema::PropertySchema::new(stringify!($field), $type, $oid);
        $vec.push(property);
        col!(add_property $vec, false, $($fields => $types,)*);
    };

    (add_property $col:expr, $oid:expr,) => {};
);

#[macro_export]
macro_rules! ind (
    ($($index:expr),+) => {
        ind!($($index),+; false);
    };

    ($($index:expr),+; $unique:expr) => {
        ind!(str $($index, crate::index::IndexType::Value, None),+; $unique);
    };

    (str $($index:expr, $str_type:expr, $str_lc:expr),+) => {
        ind!(str $($index, $str_type, $str_lc),+; false);
    };

    (str $($index:expr, $str_type:expr, $str_lc:expr),+; $unique:expr) => {
        {
            let properties = vec![
                $(
                    crate::schema::collection_schema::IndexPropertySchema::new(stringify!($index), $str_type, $str_lc)
                ),+
            ];
            (properties, $unique)
        }
    };
);

pub fn ref_map<K: Eq + Hash, V>(map: &HashMap<K, V>) -> HashMap<&K, &V> {
    map.iter().map(|(k, v)| (k, v)).collect()
}

pub fn dump_db(cursor: &mut Cursor, prefix: Option<&[u8]>) -> HashSet<(Vec<u8>, Vec<u8>)> {
    let mut set = HashSet::new();

    cursor
        .iter_between(
            prefix.unwrap_or(&[]),
            prefix.unwrap_or(&[]),
            false,
            |_, k, v| {
                set.insert((k.to_vec(), v.to_vec()));
                Ok(true)
            },
        )
        .unwrap();

    set
}
