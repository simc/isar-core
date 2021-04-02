use crate::raw_object_set::{RawObject, RawObjectSend, RawObjectSet, RawObjectSetSend};
use crate::txn::IsarDartTxn;
use isar_core::collection::IsarCollection;
use isar_core::error::Result;

#[no_mangle]
pub unsafe extern "C" fn isar_link(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    target_oid: i64,
) -> i32 {
    isar_try_txn!(txn, move |txn| -> Result<()> {
        collection.link(txn, link_index, backlink, oid, target_oid)?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_unlink(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    target_oid: i64,
) -> i32 {
    isar_try_txn!(txn, move |txn| -> Result<()> {
        collection.unlink(txn, link_index, backlink, oid, target_oid)?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_update_all(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    ids: *const i64,
    link_count: u32,
    unlink_count: u32,
) -> i32 {
    let ids = std::slice::from_raw_parts(ids, (link_count + unlink_count) as usize);
    isar_try_txn!(txn, move |txn| {
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
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_replace(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    target_oid: i64,
) -> i32 {
    isar_try_txn!(txn, move |txn| -> Result<()> {
        collection.unlink_all(txn, link_index, backlink, oid)?;
        if target_oid != i64::MIN {
            collection.link(txn, link_index, backlink, oid, target_oid)?;
        }
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_get_first(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    object: &'static mut RawObject,
) -> i32 {
    let object = RawObjectSend(object);
    isar_try_txn!(txn, move |txn| {
        collection.get_linked_objects(txn, link_index, backlink, oid, |o| {
            object.0.set_object(Some(o));
            false
        })?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_get_all(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: usize,
    backlink: bool,
    oid: i64,
    result: &'static mut RawObjectSet,
) -> i32 {
    let result = RawObjectSetSend(result);
    isar_try_txn!(txn, move |txn| {
        result
            .0
            .fill_from_link(collection, txn, link_index, backlink, oid)?;
        Ok(())
    })
}
