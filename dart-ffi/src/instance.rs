use crate::dart::{dart_post_int, DartPort};
use crate::error::DartErrCode;
use crate::from_c_str;
use crate::txn::run_async;
use crate::CharsSend;
use isar_core::collection::IsarCollection;
use isar_core::error::{illegal_arg, Result};
use isar_core::instance::IsarInstance;
use isar_core::schema::Schema;
use std::os::raw::c_char;
use std::sync::Arc;

struct IsarInstanceSend(*mut *const IsarInstance);

unsafe impl Send for IsarInstanceSend {}

#[no_mangle]
pub unsafe extern "C" fn isar_create_instance(
    isar: *mut *const IsarInstance,
    name: *const c_char,
    path: *const c_char,
    relaxed_durability: bool,
    schema_json: *const c_char,
) -> i64 {
    let open = || -> Result<()> {
        let name = from_c_str(name).unwrap().unwrap();
        let path = from_c_str(path).unwrap().unwrap();
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
pub unsafe extern "C" fn isar_create_instance_async(
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
        let result =
            isar_create_instance(isar.0, name.0, path.0, relaxed_durability, schema_json.0);
        dart_post_int(port, result);
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_close_instance(
    isar: *const IsarInstance,
    delete_from_disk: bool,
) -> bool {
    let isar = Arc::from_raw(isar);
    if delete_from_disk {
        isar.close_and_delete()
    } else {
        isar.close()
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_get_collection<'a>(
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
pub unsafe extern "C" fn isar_get_property_offsets(collection: &IsarCollection, offsets: *mut u32) {
    let properties = &collection.properties;
    let offsets = std::slice::from_raw_parts_mut(offsets, properties.len());
    for (i, (_, p)) in properties.iter().enumerate() {
        offsets[i] = p.offset as u32;
    }
}
