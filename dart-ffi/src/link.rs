use crate::async_txn::IsarAsyncTxn;
use crate::raw_object_set::{RawObject, RawObjectSend, RawObjectSet, RawObjectSetSend};
use isar_core::collection::IsarCollection;
use isar_core::txn::IsarTxn;

#[no_mangle]
pub unsafe extern "C" fn isar_link(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    target_oid: i64,
) -> i32 {
    isar_try! {
        collection.link(txn, link_index, backlink, oid, target_oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_unlink(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    target_oid: i64,
) -> i32 {
    isar_try! {
        collection.unlink(txn, link_index, backlink, oid, target_oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_update_all_async(
    collection: &'static IsarCollection,
    txn: &mut IsarAsyncTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    ids: *const i64,
    link_count: u32,
    unlink_count: u32,
) {
    let ids = std::slice::from_raw_parts(ids, (link_count + unlink_count) as usize);
    txn.exec(move |txn| {
        for target_oid in ids.iter().take(link_count as usize) {
            collection.link(txn, link_index, backlink, oid, *target_oid)?;
        }
        for target_oid in ids
            .iter()
            .skip(link_count as usize)
            .take(unlink_count as usize)
        {
            collection.unlink(txn, link_index, backlink, oid, *target_oid)?;
        }
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_replace(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    target_oid: i64,
) -> i32 {
    isar_try! {
        collection.unlink_all(txn, link_index, backlink,oid)?;
        if target_oid != i64::MIN {
            collection.link(txn, link_index, backlink,oid, target_oid)?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_replace_async(
    collection: &'static IsarCollection,
    txn: &mut IsarAsyncTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    target_oid: i64,
) {
    txn.exec(move |txn| {
        collection.unlink_all(txn, link_index, backlink, oid)?;
        if target_oid != i64::MIN {
            collection.link(txn, link_index, backlink, oid, target_oid)?;
        }
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_get_first(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    object: &mut RawObject,
) -> i32 {
    isar_try! {
        collection.get_linked_objects(txn, link_index, backlink,oid, |o| {
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
    backlink: bool,
    oid: i64,
    object: &'static mut RawObject,
) {
    let object = RawObjectSend(object);
    txn.exec(move |txn| {
        collection.get_linked_objects(txn, link_index, backlink, oid, |o| {
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
    backlink: bool,
    oid: i64,
    result: &mut RawObjectSet,
) -> i32 {
    isar_try! {
        result.fill_from_link(collection, txn, link_index, backlink,oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_get_all_async(
    collection: &'static IsarCollection,
    txn: &mut IsarAsyncTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    result: &'static mut RawObjectSet,
) {
    let result = RawObjectSetSend(result);
    txn.exec(move |txn| {
        result
            .0
            .fill_from_link(collection, txn, link_index, backlink, oid)
    });
}
