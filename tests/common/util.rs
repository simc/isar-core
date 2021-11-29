use std::env::temp_dir;
use std::sync::Arc;

use isar_core::instance::IsarInstance;
use isar_core::schema::collection_schema::CollectionSchema;
use isar_core::schema::Schema;

pub fn open_isar(schema: CollectionSchema) -> Arc<IsarInstance> {
    let mut dir = temp_dir();
    let r: u64 = rand::random();
    dir.push(&r.to_string());
    let schema = Schema::new(vec![schema]).unwrap();
    IsarInstance::open(&r.to_string(), dir, 100000000, schema, None).unwrap()
}

#[macro_export]
macro_rules! txn (
    ($isar:expr, $txn:ident) => {
        let mut $txn = $isar.begin_txn(true, false).unwrap();
    };
);

#[macro_export]
macro_rules! col (
    ($isar:expr, $col:ident) => {
        let $col = $isar.get_collection(0).unwrap();
    };
);
