use crate::async_txn::IsarAsyncTxn;
use crate::raw_object_set::{RawObjectSet, RawObjectSetSend};
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
pub unsafe extern "C" fn isar_unlink(
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
pub unsafe extern "C" fn isar_unlink_all_async(
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
pub unsafe extern "C" fn isar_link_get_objects(
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
pub unsafe extern "C" fn isar_link_get_objects_async(
    collection: &'static IsarCollection,
    txn: &mut IsarAsyncTxn,
    link_index: usize,
    oid: i64,
    result: &'static mut RawObjectSet,
) {
    let result = RawObjectSetSend(result);
    txn.exec(move |txn| result.0.fill_from_link(collection, txn, link_index, oid));
}
