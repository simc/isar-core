#![allow(dead_code)]

use crate::TestObj;
use isar_core::collection::IsarCollection;
use isar_core::query::Query;
use isar_core::txn::IsarTxn;
use itertools::Itertools;

#[macro_export]
macro_rules! isar (
    ($isar:ident) => {
        isar!($isar,)
    };

    ($path:expr, $isar:ident) => {
        isar!($path, $isar,)
    };

    ($isar:ident, $($col:ident => $schema:expr),*) => {
        let mut dir = std::env::temp_dir();
        let r: u64 = rand::random();
        dir.push(&r.to_string());
        isar!(dir.to_str().unwrap(), $isar, $($col => $schema),*)
    };

    ($path:expr, $isar:ident,) => {
        let schema = isar_core::schema::Schema::new(vec![]).unwrap();
        let path = $path.to_string();
        std::fs::create_dir_all(&path).unwrap();
        let name = xxhash_rust::xxh3::xxh3_64(path.as_bytes()).to_string();
        let $isar = isar_core::instance::IsarInstance::open(&name, Some(&path), schema,false, None).unwrap();
    };

    ($path:expr, $isar:ident, $($col:ident => $schema:expr),+) => {
        let col_schemas = vec![$($schema.clone()),*];
        let schema = isar_core::schema::Schema::new(col_schemas).unwrap();
        let path = $path.to_string();
        std::fs::create_dir_all(&path).unwrap();
        let name = xxhash_rust::xxh3::xxh3_64(path.as_bytes()).to_string();
        let $isar = isar_core::instance::IsarInstance::open(&name, Some(&path),  schema,false, None).unwrap();
        isar!(col $isar, 0, $($col),+)
    };

    (col $isar:expr, $index:expr, $col:ident, $($cols:ident),+) => {
        let $col = $isar.collections.get($index).unwrap();
        isar!(col $isar, $index + 1, $($cols),+)
    };

    (col $isar:expr, $index:expr, $col:ident) => {
        let $col = $isar.collections.get($index).unwrap();
    };
);

#[macro_export]
macro_rules! txn (
    ($isar:expr, $txn:ident) => {
        let mut $txn = $isar.begin_txn(true, false).unwrap();
    };
);

#[macro_export]
macro_rules! put (
    ($col:expr, $txn:ident, $prop:ident, $($name:ident => $value:expr),+) => {
        $(
            let id = $col.auto_increment(&mut $txn).unwrap();
            let mut $name = $crate::common::test_obj::TestObj::default(id);
            $name.$prop = $value;
            $name.save(&mut $txn, $col);
        )+
    };

    (id: $col:expr, $txn:ident, $($name:ident => $value:expr),+) => {
        $(
            let $name = $crate::common::test_obj::TestObj::default($value);
            $name.save(&mut $txn, $col);
        )+
    };
);

#[macro_export]
macro_rules! verify (
    ($txn:ident) => {
        verify!($txn,)
    };

    ($txn:ident, $col:expr) => {
        verify!($txn, $col,)
    };

    ($txn:ident, $col:expr, $($obj:ident),*) => {
        verify!($txn, $col, $($obj),*;)
    };

    ($txn:ident, $col:expr, $($obj:ident),*; $($link:expr, $($source:expr => $target:expr),+);*) => {
        verify!(col $txn, col!($col, $($obj),*; $($link, $($source => $target),+);*))
    };

    ($txn:ident, $($col:expr);*) => {
        verify!(col $txn, $($col);*)
    };

    (col $txn:ident, $($col:expr);*) => {
        #[allow(unused_mut, clippy::vec_init_then_push)]
        let mut cols = vec![
            $($col,)*
        ];

        isar_core::verify::verify_isar(&mut $txn, cols);
    };
);

#[macro_export]
macro_rules! col (
    ($col:expr) => {
        col!($col,)
    };

    ($col:expr, $($obj:ident),*) => {
        col!($col, $($obj),*;)
    };

    ($col:expr, $($obj:ident),*; $($link:expr, $($source:expr => $target:expr),+);*) => {
        {
            #[allow(unused_mut)]
            let mut objects = vec![
                $(
                    isar_core::verify::ObjectEntry::new($obj.id, $obj.to_bytes($col)),
                )*
            ];


            #[allow(unused_mut)]
            let mut links = vec![
                $(
                    $(
                        isar_core::verify::LinkEntry::new($link, $source, $target),
                    )+
                )*
            ];


            ($col, objects, links)
        }
    };
);

pub fn assert_find<'a>(
    txn: &'a mut IsarTxn,
    col: &IsarCollection,
    query: Query,
    objects: &[&TestObj],
) {
    let result = query
        .find_all_vec(txn)
        .unwrap()
        .iter()
        .map(|(_, o)| TestObj::from_object(col, *o))
        .collect_vec();
    let borrowed = result.iter().collect_vec();
    assert_eq!(&borrowed, objects);
}
