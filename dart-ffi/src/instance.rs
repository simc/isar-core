use crate::dart::{dart_post_int, DartPort};
use crate::error::DartErrCode;
use crate::from_c_str;
use crate::txn::run_async;
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
    path: *const c_char,
    relaxed_durability: bool,
    schema_json: *const c_char,
    port: DartPort,
) {
    let isar = IsarInstanceSend(isar);
    let path = from_c_str(path).unwrap().unwrap();
    let schema_json = from_c_str(schema_json).unwrap().unwrap();

    fn open(path: &str, relaxed_durability: bool, schema_json: &str) -> Result<Arc<IsarInstance>> {
        let schema = Schema::from_json(schema_json.as_bytes())?;
        let instance = IsarInstance::open(path, relaxed_durability, schema)?;
        Ok(instance)
    }

    run_async(move || {
        let isar = isar;
        match open(path, relaxed_durability, schema_json) {
            Ok(instance) => {
                isar.0.write(instance.as_ref());
                dart_post_int(port, 0);
            }
            Err(e) => {
                dart_post_int(port, e.into_dart_err_code());
            }
        };
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_get_instance(isar: *mut *const IsarInstance, name: *const c_char) {
    let name = from_c_str(name).unwrap().unwrap();
    let instance = IsarInstance::get_instance(&name);
    if let Some(instance) = instance {
        isar.write(instance.as_ref());
    } else {
        isar.write(std::ptr::null());
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_close_instance(isar: *const IsarInstance) -> bool {
    let isar = Arc::from_raw(isar);
    isar.close()
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
