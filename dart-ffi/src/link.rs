use crate::async_txn::IsarAsyncTxn;
use crate::raw_object_set::{RawObject, RawObjectSend, RawObjectSet, RawObjectSetSend};
use isar_core::collection::IsarCollection;
use isar_core::txn::IsarTxn;

#[no_mangle]
pub unsafe extern "C" fn isar_link(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    link_index: usize,
    oid: i64,
    target_oid: i64,
) -> i32 {
    isar_try! {
        collection.link(txn,link_index,oid,target_oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_all_async(
    collection: &'static IsarCollection,
    txn: &mut IsarAsyncTxn,
    link_index: usize,
    ids: *const i64,
    ids_length: u32,
) {
    let ids = std::slice::from_raw_parts(ids, ids_length as usize);
    txn.exec(move |txn| {
        for i in (0..ids_length as usize).step_by(2) {
            collection.link(txn, link_index, ids[i], ids[i + 1])?;
        }
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_unlink(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    link_index: usize,
    oid: i64,
    target_oid: i64,
) -> i32 {
    isar_try! {
        collection.unlink(txn,link_index,oid,target_oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_unlink_all_async(
    collection: &'static IsarCollection,
    txn: &mut IsarAsyncTxn,
    link_index: usize,
    ids: *const i64,
    ids_length: u32,
) {
    let ids = std::slice::from_raw_parts(ids, ids_length as usize);
    txn.exec(move |txn| {
        for i in (0..ids_length as usize).step_by(2) {
            collection.unlink(txn, link_index, ids[i], ids[i + 1])?;
        }
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_replace(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    link_index: usize,
    oid: i64,
    target_oid: i64,
) -> i32 {
    isar_try! {
        collection.unlink_all(txn, link_index, oid)?;
        collection.link(txn, link_index, oid, target_oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_replace_async(
    collection: &'static IsarCollection,
    txn: &mut IsarAsyncTxn,
    link_index: usize,
    oid: i64,
    target_oid: i64,
) {
    txn.exec(move |txn| {
        collection.unlink_all(txn, link_index, oid)?;
        collection.link(txn, link_index, oid, target_oid)?;
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_get_first(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    link_index: usize,
    oid: i64,
    object: &mut RawObject,
) -> i32 {
    isar_try! {
        collection.get_linked_objects(txn, link_index, oid, |o| {
            object.set_object(Some(o));
            false
        })?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_get_first_async(
    collection: &'static IsarCollection,
    txn: &mut IsarAsyncTxn,
    link_index: usize,
    oid: i64,
    object: &'static mut RawObject,
) {
    let object = RawObjectSend(object);
    txn.exec(move |txn| {
        collection.get_linked_objects(txn, link_index, oid, |o| {
            object.0.set_object(Some(o));
            false
        })?;
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_get_all(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    link_index: usize,
    oid: i64,
    result: &mut RawObjectSet,
) -> i32 {
    isar_try! {
        result.fill_from_link(collection, txn, link_index, oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_get_all_async(
    collection: &'static IsarCollection,
    txn: &mut IsarAsyncTxn,
    link_index: usize,
    oid: i64,
    result: &'static mut RawObjectSet,
) {
    let result = RawObjectSetSend(result);
    txn.exec(move |txn| result.0.fill_from_link(collection, txn, link_index, oid));
}
