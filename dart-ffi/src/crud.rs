use crate::raw_object_set::{RawObject, RawObjectSet};
use crate::txn::IsarDartTxn;
use crate::{BoolSend, UintSend};
use byteorder::{ByteOrder, LittleEndian};
use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::index::index_key::IndexKey;
use isar_core::object::isar_object::IsarObject;
use isar_core::txn::IsarTxn;
use serde_json::Value;

#[no_mangle]
pub unsafe extern "C" fn isar_get(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    object: &'static mut RawObject,
    key: *mut IndexKey<'static>,
) -> i32 {
    let key = if !key.is_null() {
        Some(*Box::from_raw(key))
    } else {
        None
    };
    isar_try_txn!(txn, move |txn| {
        let result = if let Some(key) = key {
            collection.get_by_index(txn, &key)?
        } else {
            let oid = object.get_oid();
            collection.get(txn, oid)?
        };
        object.set_object(result);
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_get_all(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    objects: &'static mut RawObjectSet,
    keys: *const *mut IndexKey<'static>,
) -> i32 {
    let keys = if !keys.is_null() {
        let slice = std::slice::from_raw_parts(keys, objects.get_length());
        let keys: Vec<IndexKey<'static>> = slice.iter().map(|k| *Box::from_raw(*k)).collect();
        Some(keys)
    } else {
        None
    };
    isar_try_txn!(txn, move |txn| {
        if let Some(keys) = keys {
            for (object, key) in objects.get_objects().iter_mut().zip(keys) {
                let result = collection.get_by_index(txn, &key)?;
                object.set_object(result);
            }
        } else {
            for object in objects.get_objects() {
                let oid = object.get_oid();
                let result = collection.get(txn, oid)?;
                object.set_object(result);
            }
        };
        Ok(())
    })
}

fn update_auto_increment(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    bytes: &mut [u8],
) -> Result<i64> {
    let isar_object = IsarObject::from_bytes(bytes);
    if isar_object.is_null(IsarObject::ID_PROPERTY) {
        let oid = collection.auto_increment(txn)?;
        LittleEndian::write_i64(&mut bytes[IsarObject::ID_PROPERTY.offset..], oid);
        Ok(oid)
    } else {
        Ok(isar_object.read_id())
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_put(
    collection: &'static mut IsarCollection,
    txn: &mut IsarDartTxn,
    object: &'static mut RawObject,
) -> i32 {
    isar_try_txn!(txn, move |txn| {
        let bytes = object.get_bytes();
        let auto_increment = update_auto_increment(collection, txn, bytes)?;
        collection.put(txn, IsarObject::from_bytes(bytes))?;
        object.set_oid(auto_increment);
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_put_all(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    objects: &'static mut RawObjectSet,
) -> i32 {
    isar_try_txn!(txn, move |txn| {
        for raw_obj in objects.get_objects() {
            let bytes = raw_obj.get_bytes();
            let auto_increment = update_auto_increment(collection, txn, bytes)?;
            collection.put(txn, IsarObject::from_bytes(bytes))?;
            raw_obj.set_oid(auto_increment)
        }
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    oid: i64,
    key: *mut IndexKey<'static>,
    deleted: &'static mut bool,
) -> i32 {
    let deleted = BoolSend(deleted);
    let key = if !key.is_null() {
        Some(*Box::from_raw(key))
    } else {
        None
    };
    isar_try_txn!(txn, move |txn| {
        *deleted.0 = if let Some(key) = key {
            collection.delete_by_index(txn, &key)?
        } else {
            collection.delete(txn, oid)?
        };
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_all(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    oids: *const i64,
    keys: *const *mut IndexKey<'static>,
    oids_length: u32,
    count: &'static mut u32,
) -> i32 {
    let keys = if !keys.is_null() {
        let slice = std::slice::from_raw_parts(keys, oids_length as usize);
        let keys: Vec<IndexKey<'static>> = slice.iter().map(|k| *Box::from_raw(*k)).collect();
        Some(keys)
    } else {
        None
    };
    let oids = std::slice::from_raw_parts(oids, oids_length as usize);
    let count = UintSend(count);
    isar_try_txn!(txn, move |txn| {
        if let Some(keys) = keys {
            for key in keys {
                if collection.delete_by_index(txn, &key)? {
                    *count.0 += 1;
                }
            }
        } else {
            for oid in oids {
                if collection.delete(txn, *oid)? {
                    *count.0 += 1;
                }
            }
        }
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_clear(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    count: &'static mut u32,
) -> i32 {
    let count = UintSend(count);
    isar_try_txn!(txn, move |txn| {
        *count.0 = collection.clear(txn)? as u32;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_json_import(
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    json_bytes: *const u8,
    json_length: u32,
) -> i32 {
    let bytes = std::slice::from_raw_parts(json_bytes, json_length as usize);
    let json: Value = serde_json::from_slice(bytes).unwrap();
    isar_try_txn!(txn, move |txn| { collection.import_json(txn, json) })
}
