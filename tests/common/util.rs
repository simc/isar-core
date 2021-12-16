use isar_core::query::Query;
use isar_core::txn::IsarTxn;
use itertools::Itertools;

use crate::common::test_obj::TestObj;

#[macro_export]
macro_rules! isar (
    ($isar:ident, $($col:ident, $schema:expr),+) => {
        let mut dir = std::env::temp_dir();
        let r: u64 = rand::random();
        dir.push(&r.to_string());
        let col_schemas = vec![$($schema),+];
        let schema = isar_core::schema:: Schema::new(col_schemas).unwrap();
        let $isar = isar_core::instance::IsarInstance::open(&r.to_string(), dir, 100000000, schema).unwrap();
        col!($isar, $($col),+)
    };
);

#[macro_export]
macro_rules! col (
    ($isar:expr, $($cols:ident),+) => {
        col!(index $isar, 0, $($cols),+)
    };

    (index $isar:expr, $index:expr, $col:ident, $($cols:ident),+) => {
        let $col = $isar.collections.get($index).unwrap();
        col!(index $isar, $index + 1, $($cols),+)
    };

    (index $isar:expr, $index:expr, $col:ident) => {
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
        )+;
    };

    (id $col:expr, $txn:ident, $($name:ident => $value:expr),+) => {
        $(
            let mut $name = $crate::common::test_obj::TestObj::default($value);
            $name.save(&mut $txn, $col);
        )+;
    };
);

#[macro_export]
macro_rules! verify (
    ($col:expr, $txn:ident) => {
        verify!($col, $txn,)
    };

    ($col:expr, $txn:ident, $($obj:ident),*) => {
        verify!($col, $txn, $($obj),*;)
    };

    ($col:expr, $txn:ident, $($obj:ident),*; $($link:expr, $($source:expr => $target:expr),+);*) => {
        let mut objects = vec![];
        $(
            objects.push(isar_core::verify::ObjectEntry::new($obj.id, $obj.to_bytes()));
        )*

        let mut links = vec![];
        $(
            $(
                links.push(isar_core::verify::LinkEntry::new($link, $source, $target));
            )+
        )*

        isar_core::verify::verify_isar(&mut $txn, vec![(&$col, objects, links)]);
    };
);

pub fn assert_find<'a>(txn: &'a mut IsarTxn, query: Query, objects: &[&TestObj]) {
    let result = query
        .find_all_vec(txn)
        .unwrap()
        .iter()
        .map(|(_, o)| TestObj::from(*o))
        .collect_vec();
    let borrowed = result.iter().collect_vec();
    assert_eq!(&borrowed, objects);
}
