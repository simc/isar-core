use crate::raw_object_set::{RawObject, RawObjectSet};
use crate::txn::IsarDartTxn;
use isar_core::collection::IsarCollection;
use isar_core::error::Result;

#[no_mangle]
pub unsafe extern "C" fn isar_link(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: usize,
    backlink: bool,
    id: i64,
    target_id: i64,
) -> i64 {
    isar_try_txn!(txn, move |txn| -> Result<()> {
        collection.link(txn, link_index, backlink, id, target_id)?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_unlink(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: usize,
    backlink: bool,
    id: i64,
    target_id: i64,
) -> i64 {
    isar_try_txn!(txn, move |txn| -> Result<()> {
        collection.unlink(txn, link_index, backlink, id, target_id)?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_update_all(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: usize,
    backlink: bool,
    id: i64,
    ids: *const i64,
    link_count: u32,
    unlink_count: u32,
) -> i64 {
    let ids = std::slice::from_raw_parts(ids, (link_count + unlink_count) as usize);
    isar_try_txn!(txn, move |txn| {
        for target_id in ids.iter().take(link_count as usize) {
            collection.link(txn, link_index, backlink, id, *target_id)?;
        }
        for target_id in ids
            .iter()
            .skip(link_count as usize)
            .take(unlink_count as usize)
        {
            collection.unlink(txn, link_index, backlink, id, *target_id)?;
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
    id: i64,
    target_id: i64,
) -> i64 {
    isar_try_txn!(txn, move |txn| -> Result<()> {
        collection.unlink_all(txn, link_index, backlink, id)?;
        if target_id != i64::MIN {
            collection.link(txn, link_index, backlink, id, target_id)?;
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
    id: i64,
    object: &'static mut RawObject,
) -> i64 {
    isar_try_txn!(txn, move |txn| {
        object.set_object(None);
        collection.get_linked_objects(txn, link_index, backlink, id, |id, o| {
            object.set_id(id);
            object.set_object(Some(o));
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
    id: i64,
    result: &'static mut RawObjectSet,
) -> i64 {
    isar_try_txn!(txn, move |txn| {
        result.fill_from_link(collection, txn, link_index, backlink, id)?;
        Ok(())
    })
}
