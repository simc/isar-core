use crate::{from_c_str, isar_try};
use core::slice;
use isar_core::index::StringIndexType;
use isar_core::object::data_type::DataType;
use isar_core::schema::collection_schema::CollectionSchema;
use isar_core::schema::Schema;
use std::ffi::CStr;
use std::os::raw::c_char;

#[no_mangle]
pub extern "C" fn isar_schema_create() -> *mut Schema {
    Box::into_raw(Box::new(Schema::new()))
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_create_collection(
    collection_schema: *mut *const CollectionSchema,
    name: *const c_char,
    oid_name: *const c_char,
    oid_type: u8,
) -> i32 {
    let oid_type = DataType::from_ordinal(oid_type).unwrap();
    isar_try! {
        let name_str = from_c_str(name)?;
        let oid_name_str = from_c_str(oid_name)?;
        let col = CollectionSchema::new(name_str,oid_name_str,oid_type);
        let col_ptr = Box::into_raw(Box::new(col));
        collection_schema.write(col_ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_add_collection(
    schema: &mut Schema,
    collection_schema: *mut CollectionSchema,
) -> i32 {
    isar_try! {
        let collection_schema = Box::from_raw(collection_schema);
        schema.add_collection(*collection_schema)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_add_property(
    collection_schema: &mut CollectionSchema,
    name: *const c_char,
    data_type: u8,
) -> i32 {
    let data_type = DataType::from_ordinal(data_type).unwrap();
    isar_try! {
        let name_str = from_c_str(name)?;
        collection_schema.add_property(&name_str, data_type)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_add_index(
    collection_schema: &mut CollectionSchema,
    property_names: *const *const c_char,
    string_types: *const u8,
    strings_case_sensitive: *const bool,
    properties_length: u32,
    unique: bool,
) -> i32 {
    let properties_length = properties_length as usize;
    let property_names = slice::from_raw_parts(property_names, properties_length);
    let string_types = slice::from_raw_parts(string_types, properties_length);
    let strings_case_sensitive = slice::from_raw_parts(strings_case_sensitive, properties_length);

    let properties: Vec<(&str, Option<StringIndexType>, bool)> = property_names
        .iter()
        .zip(string_types)
        .zip(strings_case_sensitive)
        .map(|((p_name, string_type), string_lower_case)| {
            let p_name = CStr::from_ptr(*p_name).to_str().unwrap();
            let string_type = StringIndexType::from_ordinal(*string_type);
            (p_name, string_type, *string_lower_case)
        })
        .collect();
    isar_try! {
        collection_schema.add_index(&properties, unique)?;
    }
}
