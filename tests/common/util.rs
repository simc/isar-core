use isar_core::query::Query;
use isar_core::txn::IsarTxn;
use itertools::Itertools;

use crate::common::test_obj::TestObj;

#[macro_export]
macro_rules! isar (
    ($isar:ident, $col:ident, $schema:expr) => {
        let mut dir = std::env::temp_dir();
        let r: u64 = rand::random();
        dir.push(&r.to_string());
        let schema =isar_core::schema:: Schema::new(vec![$schema]).unwrap();
        let $isar = isar_core::instance::IsarInstance::open(&r.to_string(), dir, 100000000, schema).unwrap();
        let $col = $isar.get_collection(0).unwrap();
    };
);

#[macro_export]
macro_rules! txn (
    ($isar:expr, $txn:ident) => {
        let mut $txn = $isar.begin_txn(true, false).unwrap();
    };
);

#[macro_export]
macro_rules! put_objects (
    ($col:expr, $txn:ident, $prop:ident, $($name:ident, $value:expr),+) => {
        put_objects!(internal $col, $txn, 0, $prop, $($name, $value),+);
    };

    (internal $col:expr, $txn:ident, $index:expr, $prop:ident, $name:ident, $value:expr, $($other_name:ident, $other_value:expr),+) => {
        put_objects!(internal $col, $txn, $index, $prop, $name, $value);
        put_objects!(internal $col, $txn, $index + 1, $prop, $($other_name, $other_value),*);
    };

    (internal $col:expr, $txn:ident, $index:expr, $prop:ident, $name:ident, $value:expr) => {
        let mut $name = $crate::common::test_obj::TestObj::default($index);
        $name.$prop = $value;
        $name.save($col, &mut $txn);
    };
);

pub fn assert_find<'a>(txn: &'a mut IsarTxn, query: Query, objects: &[&TestObj]) {
    let result = query
        .find_all_vec(txn)
        .unwrap()
        .iter()
        .map(|o| TestObj::from(*o))
        .collect_vec();
    let borrowed = result.iter().collect_vec();
    assert_eq!(&borrowed, objects);
}
