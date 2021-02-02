#![cfg(test)]

use crate::collection::IsarCollection;
use crate::lmdb::cursor::Cursor;
use crate::object::isar_object::IsarObject;
use crate::object::object_id::ObjectId;
use crate::txn::IsarTxn;
use hashbrown::{HashMap, HashSet};
use std::hash::Hash;
use std::mem;

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
        let mut schema = crate::schema::Schema::new();
        $(
            let col = $schema;
            schema.add_collection(col).unwrap();
        )+
        let $isar = crate::instance::IsarInstance::open($path, 10000000, schema).unwrap();
        $(
            let col = $schema;
            let $col = $isar.get_collection_by_name(&col.name).unwrap();
        )+
    };

    ($isar:ident, $($col:ident => $schema:expr),+) => {
        let temp = tempfile::tempdir().expect("DIR");
        let path = temp.path().to_str().expect("PATH");
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
        col!($name, crate::object::data_type::DataType::Long, $($field => $type),+; $($index),*);
    };

    ($name:expr, $oid_type:path, $($field:expr => $type:path),+; $($index:expr),*) => {
        {
            let mut collection = crate::schema::collection_schema::CollectionSchema::new($name, "id", $oid_type);
            $(collection.add_property(stringify!($field), $type).unwrap();)+
            $(
                let (fields, unique) = $index;
                collection.add_index(fields, unique).unwrap();
            )*
            collection
        }
    };
);

#[macro_export]
macro_rules! ind (
    ($($index:expr),+) => {
        ind!($($index),+; false);
    };

    ($($index:expr),+; $unique:expr) => {
        (&[$((stringify!($index), None, true)),+], $unique);
    };

    (str $($index:expr, $str_type:expr, $str_lc:expr),+) => {
        ind!(str $($index, $str_type, $str_lc),+; false);
    };

    (str $($index:expr, $str_type:expr, $str_lc:expr),+; $unique:expr) => {
        (&[$((stringify!($index), $str_type, $str_lc)),+], $unique);
    };
);

pub fn fill_db(
    col: &IsarCollection,
    txn: &mut IsarTxn,
    data: &[(Option<ObjectId>, Vec<u8>)],
) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut result = HashMap::new();
    for (oid, object) in data {
        let oid = col.put(txn, oid.clone(), IsarObject::new(object)).unwrap();
        result.insert(oid.as_bytes().to_vec(), object.to_vec());
    }
    result
}

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
