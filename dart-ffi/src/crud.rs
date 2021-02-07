use crate::async_txn::IsarAsyncTxn;
use crate::raw_object_set::{RawObject, RawObjectSend, RawObjectSet, RawObjectSetSend};
use crate::{BoolSend, IntSend};
use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::object::object_id::ObjectId;
use isar_core::txn::IsarTxn;
use serde_json::Value;

#[no_mangle]
pub unsafe extern "C" fn isar_get(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    object: &mut RawObject,
) -> i32 {
    isar_try! {
        let object_id = object.get_object_id(collection).unwrap();
        let result = collection.get(txn, &object_id)?;
        object.set_object(result);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_get_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    object: &'static mut RawObject,
) {
    let object = RawObjectSend(object);
    let oid = object.0.get_object_id(collection).unwrap().to_owned();
    txn.exec(move |txn| -> Result<()> {
        let result = collection.get(txn, &oid)?;
        object.0.set_object(result);
        Ok(())
    });
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
            let oid = object.get_object_id(collection).unwrap();
            let result = collection.get(txn, &oid)?;
            object.set_object(result);
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
        let oid = object.get_object_id(collection);
        let oid_none = oid.is_none();
        let data = object.get_object();
        let new_oid = collection.put(txn, oid, data)?;
        if oid_none {
            object.set_object_id(&new_oid);
        }
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
        let oid = object.0.get_object_id(collection);
        let oid_none = oid.is_none();
        let data = object.0.get_object();
        let new_oid = collection.put(txn, oid, data)?;
        if oid_none {
            object.0.set_object_id(&new_oid);
        }
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_put_all_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    objects: &'static mut RawObjectSet,
) {
    let objects = RawObjectSetSend(objects);
    txn.exec(move |txn| -> Result<()> {
        let mut oids_none = vec![];
        let mut entries = vec![];
        for raw_obj in objects.0.get_objects() {
            let oid = raw_obj.get_object_id(collection);
            oids_none.push(oid.is_none());
            entries.push((oid, raw_obj.get_object()))
        }
        let oids = collection.put_all(txn, entries)?;
        let objects = objects.0.get_objects();
        for (i, oid_none) in oids_none.iter().enumerate() {
            if *oid_none {
                objects[i].set_object_id(oids.get(i).unwrap());
            }
        }
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    object: &RawObject,
    deleted: &mut bool,
) -> i32 {
    isar_try! {
    let oid = object.get_object_id(collection).unwrap();
        *deleted = collection.delete(txn, &oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    object: &RawObject,
    deleted: &'static mut bool,
) {
    let oid = object.get_object_id(collection).unwrap().to_owned();
    let deleted = BoolSend(deleted);
    txn.exec(move |txn| {
        *deleted.0 = collection.delete(txn, &oid)?;
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_all(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    objects: &RawObjectSet,
    count: &mut i64,
) -> i32 {
    let oids: Vec<ObjectId> = objects
        .get_objects()
        .iter()
        .map(|raw_obj| raw_obj.get_object_id(collection).unwrap())
        .collect();
    isar_try! {
        *count = collection.delete_all(txn, &oids)? as i64;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_all_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    objects: &RawObjectSet,
    count: &'static mut i64,
) {
    let oids: Vec<ObjectId> = objects
        .get_objects()
        .iter()
        .map(|raw_obj| raw_obj.get_object_id(collection).unwrap())
        .collect();
    let count = IntSend(count);
    txn.exec(move |txn| {
        *count.0 = collection.delete_all(txn, &oids)? as i64;
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_clear(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    count: &mut i64,
) -> i32 {
    isar_try! {
        *count = collection.clear(txn)? as i64;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_clear_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    count: &'static mut i64,
) {
    let count = IntSend(count);
    txn.exec(move |txn| -> Result<()> {
        *(count.0) = collection.clear(txn)? as i64;
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
    json_bytes: *mut *mut u8,
    json_length: *mut u32,
) {
    let json = JsonBytes(json_bytes);
    let json_length = JsonLen(json_length);
    txn.exec(move |txn| -> Result<()> {
        let exported_json = collection.export_json(txn, primitive_null, true)?;
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
