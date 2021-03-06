use crate::async_txn::IsarAsyncTxn;
use crate::raw_object_set::{RawObject, RawObjectSet, RawObjectSetSend};
use crate::UintSend;
use byteorder::{ByteOrder, LittleEndian};
use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::object::isar_object::IsarObject;
use isar_core::txn::IsarTxn;
use serde_json::Value;

#[no_mangle]
pub unsafe extern "C" fn isar_get(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    object: &mut RawObject,
) -> i32 {
    isar_try! {
        let oid = object.get_oid();
        let result = collection.get(txn, oid)?;
        object.set_object(result);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_get_all_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    objects: &'static mut RawObjectSet,
) {
    let objects = RawObjectSetSend(objects);
    txn.exec(move |txn| -> Result<()> {
        for object in objects.0.get_objects() {
            let oid = object.get_oid();
            let result = collection.get(txn, oid)?;
            object.set_object(result);
        }
        Ok(())
    });
}

fn update_auto_increment(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    bytes: &mut [u8],
) -> Result<i64> {
    let isar_object = IsarObject::from_bytes(bytes);
    let oid_property = collection.get_oid_property();
    if isar_object.is_null(oid_property) {
        let oid = collection.auto_increment(txn)?;
        LittleEndian::write_i64(&mut bytes[oid_property.offset..], oid);
        Ok(oid)
    } else {
        Ok(isar_object.read_long(oid_property))
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_put(
    collection: &mut IsarCollection,
    txn: &mut IsarTxn,
    object: &mut RawObject,
) -> i32 {
    isar_try! {
        let bytes = object.get_bytes();
        let auto_increment = update_auto_increment(collection, txn, bytes)?;
        collection.put(txn, IsarObject::from_bytes(bytes))?;
        object.set_oid(auto_increment);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_put_all_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    objects: &'static mut RawObjectSet,
) {
    let objects = RawObjectSetSend(objects);
    txn.exec(move |txn| -> Result<()> {
        for raw_obj in objects.0.get_objects() {
            let bytes = raw_obj.get_bytes();
            let auto_increment = update_auto_increment(collection, txn, bytes)?;
            collection.put(txn, IsarObject::from_bytes(bytes))?;
            raw_obj.set_oid(auto_increment)
        }
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    oid: i64,
    deleted: &mut bool,
) -> i32 {
    isar_try! {
        *deleted = collection.delete(txn, oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_all_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    oids: *const i64,
    oids_length: u32,
    count: &'static mut u32,
) {
    let oids = std::slice::from_raw_parts(oids, oids_length as usize);
    let count = UintSend(count);
    txn.exec(move |txn| {
        for oid in oids {
            if collection.delete(txn, *oid)? {
                *count.0 += 1;
            }
        }
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_clear(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    count: &mut u32,
) -> i32 {
    isar_try! {
        *count = collection.clear(txn)? as u32;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_clear_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    count: &'static mut u32,
) {
    let count = UintSend(count);
    txn.exec(move |txn| -> Result<()> {
        *count.0 = collection.clear(txn)? as u32;
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_json_import_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    json_bytes: *const u8,
    json_length: u32,
) {
    let bytes = std::slice::from_raw_parts(json_bytes, json_length as usize);
    let json: Value = serde_json::from_slice(bytes).unwrap();
    txn.exec(move |txn| -> Result<()> { collection.import_json(txn, json) });
}

struct JsonBytes(*mut *mut u8);
unsafe impl Send for JsonBytes {}

struct JsonLen(*mut u32);
unsafe impl Send for JsonLen {}

#[no_mangle]
pub unsafe extern "C" fn isar_json_export_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    primitive_null: bool,
    include_links: bool,
    json_bytes: *mut *mut u8,
    json_length: *mut u32,
) {
    let json = JsonBytes(json_bytes);
    let json_length = JsonLen(json_length);
    txn.exec(move |txn| -> Result<()> {
        let exported_json = collection.export_json(txn, primitive_null, true, include_links)?;
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
