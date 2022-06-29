use crate::app_dir::get_app_dir;
use crate::dart::{dart_post_int, DartPort};
use crate::error::DartErrCode;
use crate::from_c_str;
use crate::txn::run_async;
use crate::txn::CIsarTxn;
use crate::CharsSend;
use isar_core::collection::IsarCollection;
use isar_core::error::{illegal_arg, Result};
use isar_core::instance::IsarInstance;
use isar_core::schema::Schema;
use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::Arc;

include!(concat!(env!("OUT_DIR"), "/version.rs"));

struct IsarInstanceSend(*mut *const IsarInstance);

unsafe impl Send for IsarInstanceSend {}

#[no_mangle]
pub unsafe extern "C" fn isar_version() -> i64 {
    ISAR_VERSION as i64
}

#[no_mangle]
pub unsafe extern "C" fn isar_instance_create(
    isar: *mut *const IsarInstance,
    name: *const c_char,
    path: *const c_char,
    relaxed_durability: bool,
    schema_json: *const c_char,
) -> i64 {
    let open = || -> Result<()> {
        let name = from_c_str(name).unwrap().unwrap();
        let path = from_c_str(path).unwrap().or_else(get_app_dir);
        let schema_json = from_c_str(schema_json).unwrap().unwrap();
        let schema = Schema::from_json(schema_json.as_bytes())?;

        let instance = IsarInstance::open(name, path, relaxed_durability, schema)?;
        isar.write(Arc::into_raw(instance));
        Ok(())
    };

    match open() {
        Ok(_) => 0,
        Err(e) => e.into_dart_err_code(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_instance_create_async(
    isar: *mut *const IsarInstance,
    name: *const c_char,
    path: *const c_char,
    relaxed_durability: bool,
    schema_json: *const c_char,
    port: DartPort,
) {
    let isar = IsarInstanceSend(isar);
    let name = CharsSend(name);
    let path = CharsSend(path);
    let schema_json = CharsSend(schema_json);
    run_async(move || {
        let isar = isar;
        let name = name;
        let path = path;
        let schema_json = schema_json;
        let result =
            isar_instance_create(isar.0, name.0, path.0, relaxed_durability, schema_json.0);
        dart_post_int(port, result);
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_instance_close(isar: *const IsarInstance) -> bool {
    let isar = Arc::from_raw(isar);
    isar.close()
}

#[no_mangle]
pub unsafe extern "C" fn isar_instance_close_and_delete(isar: *const IsarInstance) -> bool {
    let isar = Arc::from_raw(isar);
    isar.close_and_delete()
}

#[no_mangle]
pub unsafe extern "C" fn isar_instance_get_path(isar: &'static IsarInstance) -> *mut c_char {
    CString::new(isar.dir.as_str()).unwrap().into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn isar_instance_get_collection<'a>(
    isar: &'a IsarInstance,
    collection: *mut &'a IsarCollection,
    index: u32,
) -> i64 {
    isar_try! {
        let new_collection = isar.collections.get(index as usize);
        if let Some(new_collection) = new_collection {
            collection.write(new_collection);
        } else {
            illegal_arg("Collection index is invalid.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_instance_get_size(
    instance: &'static IsarInstance,
    txn: &mut CIsarTxn,
    include_indexes: bool,
    include_links: bool,
    size: &'static mut i64,
) -> i64 {
    isar_try_txn!(txn, move |txn| {
        *size = instance.get_size(txn, include_indexes, include_links)? as i64;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_get_offsets(collection: &IsarCollection, offsets: *mut u32) -> u32 {
    let properties = &collection.properties;
    let offsets = std::slice::from_raw_parts_mut(offsets, properties.len());
    for (i, p) in properties.iter().enumerate() {
        offsets[i] = p.offset as u32;
    }
    let property = properties.iter().max_by_key(|p| p.offset);
    property.map_or(2, |p| p.offset + p.data_type.get_static_size()) as u32
}
