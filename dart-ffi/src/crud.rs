use crate::async_txn::IsarAsyncTxn;
use crate::raw_object_set::{RawObject, RawObjectSend};
use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::txn::IsarTxn;

#[no_mangle]
pub unsafe extern "C" fn isar_get(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    object: &mut RawObject,
) -> i32 {
    isar_try! {
        let object_id = object.get_object_id().unwrap();
        let result = collection.get(txn, object_id)?;
        if let Some(result) = result {
            object.set_object(result);
        } else {
            object.clear();
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_get_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    object: &'static mut RawObject,
) {
    let object = RawObjectSend(object);
    let oid = object.0.get_object_id().unwrap();
    txn.exec(move |txn| -> Result<()> {
        let result = collection.get(txn, oid)?;
        if let Some(result) = result {
            object.0.set_object(result);
        } else {
            object.0.clear();
        }
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_put(
    collection: &mut IsarCollection,
    txn: &mut IsarTxn,
    object: &mut RawObject,
) -> i32 {
    isar_try! {
        let oid = object.get_object_id();
        let data = object.object_as_slice();
        let oid = collection.put(txn, oid, data)?;
        object.set_object_id(oid);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_put_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    object: &'static mut RawObject,
) {
    let object = RawObjectSend(object);
    txn.exec(move |txn| -> Result<()> {
        let oid = object.0.get_object_id();
        let data = object.0.object_as_slice();
        let oid = collection.put(txn, oid, data)?;
        object.0.set_object_id(oid);
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    object: &RawObject,
) -> i32 {
    isar_try! {
    let oid = object.get_object_id().unwrap();
        collection.delete(txn, oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    object: &RawObject,
) {
    let oid = object.get_object_id().unwrap();
    txn.exec(move |txn| {
        collection.delete(txn, oid)?;
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_all(collection: &IsarCollection, txn: &mut IsarTxn) -> i32 {
    isar_try! {
        collection.new_query_builder().build().delete_all(txn,collection)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_all_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
) {
    txn.exec(move |txn| {
        collection
            .new_query_builder()
            .build()
            .delete_all(txn, collection)?;
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_export_json(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    primitive_null: bool,
    json: *mut *mut u8,
    json_length: *mut u32,
) -> i32 {
    isar_try! {
        let exported_json = collection.export_json(txn, primitive_null)?;
        let bytes = serde_json::to_vec(&exported_json).unwrap();
        let mut bytes = bytes.into_boxed_slice();
        json_length.write(bytes.len() as u32);
        json.write(bytes.as_mut_ptr());
        std::mem::forget(bytes);
    }
}

struct JsonBytes(*mut *mut u8);
unsafe impl Send for JsonBytes {}

struct JsonLen(*mut u32);
unsafe impl Send for JsonLen {}

#[no_mangle]
pub unsafe extern "C" fn isar_export_json_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    primitive_null: bool,
    json_bytes: *mut *mut u8,
    json_length: *mut u32,
) {
    let json = JsonBytes(json_bytes);
    let json_length = JsonLen(json_length);
    txn.exec(move |txn| -> Result<()> {
        let exported_json = collection.export_json(txn, primitive_null)?;
        let bytes = serde_json::to_vec(&exported_json).unwrap();
        let mut bytes = bytes.into_boxed_slice();
        json_length.0.write(bytes.len() as u32);
        json.0.write(bytes.as_mut_ptr());
        std::mem::forget(bytes);
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_free_json(json_bytes: *mut u8, json_length: u32) {
    Vec::from_raw_parts(json_bytes, json_length as usize, json_length as usize);
}
