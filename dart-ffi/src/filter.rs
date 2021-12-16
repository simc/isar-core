use crate::from_c_str;
use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::object::data_type::DataType;
use isar_core::object::isar_object::IsarObject;
use isar_core::query::filter::*;
use std::os::raw::c_char;
use std::slice;

#[no_mangle]
pub unsafe extern "C" fn isar_filter_and_or(
    filter: *mut *const Filter,
    and: bool,
    conditions: *mut *mut Filter,
    length: u32,
) -> u8 {
    let filters = slice::from_raw_parts(conditions, length as usize)
        .iter()
        .map(|f| *Box::from_raw(*f))
        .collect();
    let and_or = if and {
        Filter::and(filters)
    } else {
        Filter::or(filters)
    };
    let ptr = Box::into_raw(Box::new(and_or));
    filter.write(ptr);
    0
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_not(filter: *mut *const Filter, condition: *mut Filter) -> u8 {
    let condition = *Box::from_raw(condition);
    let not = Filter::not(condition);
    let ptr = Box::into_raw(Box::new(not));
    filter.write(ptr);
    0
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_link(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    condition: *mut Filter,
    link_index: u32,
    backlink: bool,
) -> i32 {
    isar_try! {
        let condition = *Box::from_raw(condition);
        let query_filter = Filter::link(collection, link_index as usize, backlink, condition)?;
        let ptr = Box::into_raw(Box::new(query_filter));
        filter.write(ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_null(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    property_index: u32,
) -> i32 {
    let property = collection.properties.get(property_index as usize);
    isar_try! {
        if let Some(property) = property {
            let query_filter = Filter::null(*property);
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
           illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_byte(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    lower: u8,
    upper: u8,
    property_index: u32,
) -> i32 {
    let property = collection.properties.get(property_index as usize);
    isar_try! {
        if let Some(property) = property {
            let query_filter = Filter::byte(*property, lower, upper)?;
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_long(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    lower: i64,
    upper: i64,
    property_index: u32,
) -> i32 {
    let property = collection.properties.get(property_index as usize);
    isar_try! {
        if let Some(property) = property {
            let query_filter = if property.data_type == DataType::Int || property.data_type == DataType::IntList {
                let lower = lower.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                let upper = upper.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                Filter::int(*property, lower, upper)?
            } else {
                Filter::long(*property, lower, upper)?
            };
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_double(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    lower: f64,
    upper: f64,
    property_index: u32,
) -> i32 {
    let property = collection.properties.get(property_index as usize);
    isar_try! {
        if let Some(property) = property {
            let query_filter = if property.data_type == DataType::Float || property.data_type == DataType::FloatList {
                Filter::float(*property, lower as f32, upper as f32)?
            } else {
                Filter::double(*property, lower, upper)?
            };
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_string(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    lower: *const c_char,
    upper: *const c_char,
    case_sensitive: bool,
    property_index: u32,
) -> i32 {
    let property = collection.properties.get(property_index as usize);
    isar_try! {
        if let Some(property) = property {
            let lower = if !lower.is_null() {
                Some(from_c_str(lower)?)
            } else {
                None
            };
            let upper = if !upper.is_null() {
                Some(from_c_str(upper)?)
            } else {
                None
            };
            let query_filter = Filter::string(*property, lower, upper, case_sensitive)?;
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[macro_export]
macro_rules! filter_string_ffi {
    ($filter_name:ident, $function_name:ident) => {
        #[no_mangle]
        pub unsafe extern "C" fn $function_name(
            collection: &IsarCollection,
            filter: *mut *const Filter,
            value: *const c_char,
            case_sensitive: bool,
            property_index: u32,
        ) -> i32 {
            let property = collection.properties.get(property_index as usize);
            isar_try! {
                if let Some(property) = property {
                    let str = from_c_str(value)?;
                    let query_filter = isar_core::query::filter::Filter::$filter_name(*property, str, case_sensitive)?;
                    let ptr = Box::into_raw(Box::new(query_filter));
                    filter.write(ptr);
                } else {
                    illegal_arg("Property does not exist.")?;
                }
            }
        }
    }
}

filter_string_ffi!(string_starts_with, isar_filter_string_starts_with);
filter_string_ffi!(string_ends_with, isar_filter_string_ends_with);
filter_string_ffi!(string_matches, isar_filter_string_matches);
