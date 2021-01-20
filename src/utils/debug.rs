#![cfg(test)]

use crate::collection::IsarCollection;
use crate::lmdb::cursor::Cursor;
use crate::object::object_builder::ObjectBuilderBytes;
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
            schema.add_collection(col).expect("HMM");
        )+
        let $isar = crate::instance::IsarInstance::create($path, 10000000, schema).expect("DIDNT GET");
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
    ($($field:expr => $type:ident),+) => {
        col!($($field => $type),+;);
    };

    ($($field:expr => $type:ident),+; $($index:expr),*) => {
        {
            let mut collection = crate::schema::collection_schema::CollectionSchema::new(stringify!($($field)+));
            $(collection.add_property(stringify!($field), crate::object::data_type::DataType::$type).unwrap();)+
            $(
                let (fields, unique, hash) = $index;
                collection.add_index(fields, unique, hash).unwrap();
            )*
            collection
        }
    };

    ($name:expr, $($field:expr => $type:ident),+) => {
        col!($name, $($field => $type),+;);
    };

    ($name:expr, $($field:expr => $type:ident),+; $($index:expr),*) => {
        {
            let mut collection = crate::schema::collection_schema::CollectionSchema::new($name);
            $(collection.add_property(stringify!($field), crate::object::data_type::DataType::$type).unwrap();)+
            $(
                let (fields, unique, hash) = $index;
                collection.add_index(fields, unique, hash).unwrap();
            )*
            collection
        }
    };
);

#[macro_export]
macro_rules! ind (
    ($($index:expr),+) => {
        ind!($($index),+; false, false);
    };

    ($($index:expr),+; $unique:expr) => {
        ind!($($index),+; $unique, false);
    };

    ($($index:expr),+; $unique:expr, $hash:expr) => {
        (&[$(stringify!($index)),+], $unique, $hash)
    };
);

pub fn fill_db(
    col: &IsarCollection,
    txn: &mut IsarTxn,
    data: &[(Option<ObjectId>, ObjectBuilderBytes)],
) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut result = HashMap::new();
    for (oid, object) in data {
        let oid = col.put(txn, *oid, object.as_ref()).unwrap();
        result.insert(oid.as_bytes().to_vec(), object.as_ref().to_vec());
    }
    result
}

pub fn ref_map<K: Eq + Hash, V>(map: &HashMap<K, V>) -> HashMap<&K, &V> {
    map.iter().map(|(k, v)| (k, v)).collect()
}

pub fn dump_db(cursor: &mut Cursor, prefix: Option<&[u8]>) -> HashSet<(Vec<u8>, Vec<u8>)> {
    let mut set = HashSet::new();

    cursor
        .iter_prefix(prefix.unwrap_or(&[]), |_, k, v| {
            set.insert((k.to_vec(), v.to_vec()));
            Ok(true)
        })
        .unwrap();

    set
}

#[repr(C, align(8))]
struct Align8([u8; 8]);

pub fn align(bytes: &[u8]) -> Vec<u8> {
    let n_units = (bytes.len() / mem::size_of::<Align8>()) + 1;

    let mut aligned: Vec<Align8> = Vec::with_capacity(n_units);

    let ptr = aligned.as_mut_ptr();
    let len_units = aligned.len();
    let cap_units = aligned.capacity();

    mem::forget(aligned);

    let mut vec = unsafe {
        Vec::from_raw_parts(
            ptr as *mut u8,
            len_units * mem::size_of::<Align8>(),
            cap_units * mem::size_of::<Align8>(),
        )
    };
    vec.extend_from_slice(bytes);
    vec
}

pub fn pad(data: &[u8], count: usize) -> Vec<u8> {
    let mut vec = data.to_vec();
    vec.extend((0..count).into_iter().map(|_| 0));
    vec
}

pub trait SlicePad {
    type Item;

    fn pad(&self, pre: usize, post: usize) -> Vec<Self::Item>;
}

impl SlicePad for [u8] {
    type Item = u8;

    fn pad(&self, pre: usize, post: usize) -> Vec<u8> {
        let mut vec: Vec<u8> = (0..pre).into_iter().map(|_| 0).collect();
        vec.extend_from_slice(&self);
        vec.extend((0..post).into_iter().map(|_| 0));
        vec
    }
}
