use crate::txn::IsarDartTxn;
use isar_core::collection::IsarCollection;
use isar_core::error::Result;

#[no_mangle]
pub unsafe extern "C" fn isar_link(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: u32,
    id: i64,
    target_id: i64,
) -> i64 {
    isar_try_txn!(txn, move |txn| -> Result<()> {
        collection.link(txn, link_index as usize, id, target_id)?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_unlink(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: u32,
    id: i64,
    target_id: i64,
) -> i64 {
    isar_try_txn!(txn, move |txn| -> Result<()> {
        collection.unlink(txn, link_index as usize, id, target_id)?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_link_update_all(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    link_index: u32,
    id: i64,
    ids: *const i64,
    link_count: u32,
    unlink_count: u32,
    replace: bool,
) -> i64 {
    let ids = std::slice::from_raw_parts(ids, (link_count + unlink_count) as usize);
    isar_try_txn!(txn, move |txn| {
        if replace {
            collection.unlink_all(txn, link_index as usize, id)?;
        }
        for target_id in ids.iter().take(link_count as usize) {
            collection.link(txn, link_index as usize, id, *target_id)?;
        }
        for target_id in ids
            .iter()
            .skip(link_count as usize)
            .take(unlink_count as usize)
        {
            collection.unlink(txn, link_index as usize, id, *target_id)?;
        }
        Ok(())
    })
}
