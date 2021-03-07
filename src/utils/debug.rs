#![cfg(test)]

use crate::lmdb::cursor::Cursor;
use crate::lmdb::{ByteKey, IntKey, MAX_ID, MIN_ID};
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
            $(
                let property = crate::schema::collection_schema::PropertySchema::new(stringify!($field), $type);
                properties.push(property);
            )+
            let mut indexes = vec![];
            indexes.clear();
            $(
                let (fields, unique, replace) = $index;
                let index = crate::schema::collection_schema::IndexSchema::new(fields, unique, replace);
                indexes.push(index);
            )*
            crate::schema::collection_schema::CollectionSchema::new($name, &properties[0].name.clone(), properties, indexes, vec![])
        }
    };
);

#[macro_export]
macro_rules! ind (
    ($($index:expr),+) => {
        ind!($($index),+; false, false);
    };

    ($($index:expr),+; $unique:expr, $replace:expr) => {
        ind!(str $($index, crate::schema::collection_schema::IndexType::Value, None),+; $unique, $replace);
    };

    (str $($index:expr, $str_type:expr, $str_lc:expr),+) => {
        ind!(str $($index, $str_type, $str_lc),+; false, false);
    };

    (str $($index:expr, $str_type:expr, $str_lc:expr),+; $unique:expr, $replace:expr) => {
        {
            let properties = vec![
                $(
                    crate::schema::collection_schema::IndexPropertySchema::new(stringify!($index), $str_type, $str_lc)
                ),+
            ];
            (properties, $unique, $replace)
        }
    };
);

pub fn dump_db(cursor: &mut Cursor, prefix: Option<&[u8]>) -> HashSet<(Vec<u8>, Vec<u8>)> {
    let mut set = HashSet::new();

    let mut upper = prefix.unwrap_or(&[]).to_vec();
    upper.extend_from_slice(&u64::MAX.to_le_bytes());
    cursor
        .iter_between(
            ByteKey::new(prefix.unwrap_or(&[])),
            ByteKey::new(&upper),
            false,
            true,
            |_, k, v| {
                set.insert((k.to_vec(), v.to_vec()));
                Ok(true)
            },
        )
        .unwrap();

    set
}

pub fn dump_db_oid(cursor: &mut Cursor, prefix: u16) -> HashSet<(Vec<u8>, Vec<u8>)> {
    let mut set = HashSet::new();

    cursor
        .iter_between(
            IntKey::new(prefix, MIN_ID),
            IntKey::new(prefix, MAX_ID),
            false,
            true,
            |_, k, v| {
                set.insert((k.to_vec(), v.to_vec()));
                Ok(true)
            },
        )
        .unwrap();

    set
}
