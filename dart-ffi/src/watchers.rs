use crate::dart::{dart_post_int, DartPort};
use crate::raw_object_set::RawObject;
use isar_core::collection::IsarCollection;
use isar_core::instance::IsarInstance;
use isar_core::query::query::Query;
use isar_core::watch::WatchHandle;

#[no_mangle]
pub extern "C" fn isar_watch_collection(
    isar: &IsarInstance,
    collection: &IsarCollection,
    port: DartPort,
) -> *mut WatchHandle {
    let handle = isar.watch_collection(
        collection,
        Box::new(move || {
            dart_post_int(port, 1);
        }),
    );
    Box::into_raw(Box::new(handle))
}

#[no_mangle]
pub extern "C" fn isar_watch_object(
    isar: &IsarInstance,
    collection: &IsarCollection,
    oid: &RawObject,
    port: DartPort,
) -> *mut WatchHandle {
    let handle = isar.watch_object(
        collection,
        oid.get_object_id().unwrap(),
        Box::new(move || {
            dart_post_int(port, 1);
        }),
    );
    Box::into_raw(Box::new(handle))
}

#[no_mangle]
pub extern "C" fn isar_watch_query(
    isar: &IsarInstance,
    collection: &IsarCollection,
    query: &Query,
    port: DartPort,
) -> *mut WatchHandle {
    let handle = isar.watch_query(
        collection,
        query.clone(),
        Box::new(move || {
            dart_post_int(port, 1);
        }),
    );
    Box::into_raw(Box::new(handle))
}

#[no_mangle]
pub unsafe extern "C" fn isar_stop_watching(handle: *mut WatchHandle) {
    Box::from_raw(handle).stop();
}
