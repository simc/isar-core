use crate::dart::{dart_post_int, DartPort};
use crate::raw_object_set::RawObject;
use isar_core::collection::IsarCollection;
use isar_core::instance::IsarInstance;
use isar_core::query::query::Query;

#[no_mangle]
pub extern "C" fn isar_watch_collection(
    isar: &IsarInstance,
    collection: &IsarCollection,
    port: DartPort,
) {
    isar.watch_collection(
        collection,
        Box::new(move || {
            dart_post_int(port, 1);
        }),
    );
}

#[no_mangle]
pub extern "C" fn isar_watch_object(
    isar: &IsarInstance,
    collection: &IsarCollection,
    oid: &RawObject,
    port: DartPort,
) {
    isar.watch_object(
        collection,
        oid.get_object_id().unwrap(),
        Box::new(move |oid, val| {
            dart_post_int(port, 1);
        }),
    );
}

#[no_mangle]
pub extern "C" fn isar_watch_query(
    isar: &IsarInstance,
    collection: &IsarCollection,
    query: &Query,
    port: DartPort,
) {
    isar.watch_query(
        collection,
        query.clone(),
        Box::new(move |results| {
            dart_post_int(port, 1);
        }),
    );
}
