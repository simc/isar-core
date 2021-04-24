use crate::from_c_str;
use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::index::index_key::IndexKey;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn isar_key_create<'a>(
    collection: &'a IsarCollection,
    key: *mut *const IndexKey<'a>,
    index_index: i32,
) -> i32 {
    isar_try! {
        let index_key = collection.new_index_key(index_index as usize);
        if let Some(index_key) = index_key {
            let ptr = Box::into_raw(Box::new(index_key));
            key.write(ptr);
        } else {
            illegal_arg("Unknown index.")?;
        };
    }
}

#[no_mangle]
pub extern "C" fn isar_key_add_byte(key: &mut IndexKey, value: u8) {
    key.add_byte(value);
}

#[no_mangle]
pub extern "C" fn isar_key_add_int(key: &mut IndexKey, value: i32) {
    key.add_int(value);
}

#[no_mangle]
pub extern "C" fn isar_key_add_long(key: &mut IndexKey, value: i64) {
    key.add_long(value);
}

#[no_mangle]
pub extern "C" fn isar_key_add_float(key: &mut IndexKey, value: f32) {
    key.add_float(value);
}

#[no_mangle]
pub extern "C" fn isar_key_add_double(key: &mut IndexKey, value: f64) {
    key.add_double(value);
}

#[no_mangle]
pub unsafe extern "C" fn isar_key_add_string_value(
    key: &mut IndexKey,
    value: *const c_char,
    case_sensitive: bool,
) {
    let value = if !value.is_null() {
        Some(from_c_str(value).unwrap())
    } else {
        None
    };
    key.add_string_value(value, case_sensitive)
}

#[no_mangle]
pub unsafe extern "C" fn isar_key_add_string_hash(
    key: &mut IndexKey,
    value: *const c_char,
    case_sensitive: bool,
) {
    let value = if !value.is_null() {
        Some(from_c_str(value).unwrap())
    } else {
        None
    };
    key.add_string_hash(value, case_sensitive)
}

#[no_mangle]
pub unsafe extern "C" fn isar_key_add_string_word(
    key: &mut IndexKey,
    value: *const c_char,
    case_sensitive: bool,
) {
    let value = from_c_str(value).unwrap();
    key.add_string_word(value, case_sensitive)
}
