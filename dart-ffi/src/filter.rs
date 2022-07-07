use crate::from_c_str;
use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::object::data_type::DataType;
use isar_core::object::isar_object::IsarObject;
use isar_core::query::filter::*;
use std::os::raw::c_char;
use std::slice;

#[no_mangle]
pub unsafe extern "C" fn isar_filter_static(filter: *mut *const Filter, value: bool) {
    let query_filter = Filter::stat(value);
    let ptr = Box::into_raw(Box::new(query_filter));
    filter.write(ptr);
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_and_or_xor(
    filter: *mut *const Filter,
    and: bool,
    exclusive: bool,
    conditions: *mut *mut Filter,
    length: u32,
) {
    let filters = slice::from_raw_parts(conditions, length as usize)
        .iter()
        .map(|f| *Box::from_raw(*f))
        .collect();
    let and_or = if and {
        Filter::and(filters)
    } else if exclusive {
        Filter::xor(filters)
    } else {
        Filter::or(filters)
    };
    let ptr = Box::into_raw(Box::new(and_or));
    filter.write(ptr);
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_not(filter: *mut *const Filter, condition: *mut Filter) {
    let condition = *Box::from_raw(condition);
    let not = Filter::not(condition);
    let ptr = Box::into_raw(Box::new(not));
    filter.write(ptr);
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_link(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    condition: *mut Filter,
    link_id: u32,
) -> i64 {
    isar_try! {
        let condition = *Box::from_raw(condition);
        let query_filter = Filter::link(collection, link_id as usize, condition)?;
        let ptr = Box::into_raw(Box::new(query_filter));
        filter.write(ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_null(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    property_id: u32,
    any_null: bool,
) -> i64 {
    let property = collection.properties.get(property_id as usize);
    isar_try! {
        if let Some(property) = property {
            let query_filter = if !property.data_type.is_scalar() && any_null {
                match property.data_type {
                    DataType::BoolList | DataType::ByteList => {
                        Filter::byte(property, IsarObject::NULL_BYTE, IsarObject::NULL_BYTE)?
                    },
                    DataType::IntList => {
                        Filter::int(property, IsarObject::NULL_INT, IsarObject::NULL_INT)?
                    },
                    DataType::FloatList => {
                        Filter::float(property, IsarObject::NULL_FLOAT, IsarObject::NULL_FLOAT)?
                    },
                    DataType::LongList => {
                        Filter::long(property, IsarObject::NULL_LONG, IsarObject::NULL_LONG)?
                    },
                    DataType::DoubleList => {
                        Filter::double(property, IsarObject::NULL_DOUBLE, IsarObject::NULL_DOUBLE)?
                    },
                    DataType::StringList => Filter::string(property, None, None, false)?,
                    _ => unreachable!()
                }
            } else {
                 Filter::null(property)
            };
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
           illegal_arg("Property does not exist.")?;
        }
    }
}

#[macro_export]
macro_rules! num_filter {
    ($filter:ident, $property:expr, $lower:ident, $include_lower:expr, $upper:ident, $include_upper:expr) => {{
        let lower = if !$include_lower {
            $lower.checked_add(1)
        } else {
            Some($lower)
        };
        let upper = if !$include_upper {
            $upper.checked_sub(1)
        } else {
            Some($upper)
        };

        if let (Some(lower), Some(upper)) = (lower, upper) {
            Filter::$filter($property, lower, upper)?
        } else {
            Filter::stat(false)
        }
    }};
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_id(
    filter: *mut *const Filter,
    lower: i64,
    include_lower: bool,
    upper: i64,
    include_upper: bool,
) -> i64 {
    isar_try! {
        let lower = if !include_lower {
            lower.checked_add(1)
        } else {
            Some(lower)
        };
        let upper = if !include_upper {
            upper.checked_sub(1)
        } else {
            Some(upper)
        };

        let query_filter = if let (Some(lower), Some(upper)) = (lower, upper) {
            Filter::id(lower, upper)?
        } else {
            Filter::stat(false)
        };
        let ptr = Box::into_raw(Box::new(query_filter));
        filter.write(ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_byte(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    lower: u8,
    include_lower: bool,
    upper: u8,
    include_upper: bool,
    property_id: u32,
) -> i64 {
    let property = collection.properties.get(property_id as usize);
    isar_try! {
        if let Some(property) = property {
            let query_filter = num_filter!(byte, property, lower, include_lower, upper, include_upper);
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
    include_lower: bool,
    upper: i64,
    include_upper: bool,
    property_id: u32,
) -> i64 {
    let property = collection.properties.get(property_id as usize);
    isar_try! {
        if let Some(property) = property {
            let query_filter = if property.data_type == DataType::Int || property.data_type == DataType::IntList {
                let lower = lower.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                let upper = upper.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                num_filter!(int, property, lower, include_lower, upper, include_upper)
            } else {
                num_filter!(long, property, lower, include_lower, upper, include_upper)
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
    property_id: u32,
) -> i64 {
    let property = collection.properties.get(property_id as usize);
    isar_try! {
        if let Some(property) = property {
            let query_filter = if upper.is_nan() {
                Filter::stat(false)
            } else if property.data_type == DataType::Float || property.data_type == DataType::FloatList {
                let lower = lower as f32;
                let upper = upper as f32;
                Filter::float(property, lower, upper)?
            } else {
                Filter::double(property, lower, upper)?
            };
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

unsafe fn get_lower_str(lower: Option<Vec<u8>>, include_lower: bool) -> Option<Vec<u8>> {
    if include_lower {
        lower
    } else if let Some(mut lower) = lower {
        if let Some(last) = lower.pop() {
            if last < 255 {
                lower.push(last + 1);
            } else {
                lower.push(255);
                lower.push(0);
            }
        } else {
            lower.push(0);
        }
        Some(lower)
    } else {
        Some(vec![])
    }
}

unsafe fn get_upper_str(upper: Option<Vec<u8>>, include_upper: bool) -> Option<Option<Vec<u8>>> {
    if include_upper {
        Some(upper)
    } else if let Some(mut upper) = upper {
        if upper.is_empty() {
            Some(None)
        } else {
            for i in (upper.len() - 1)..0 {
                if upper[i] > 0 {
                    upper[i] -= 1;
                    return Some(Some(upper));
                }
            }
            Some(Some(vec![]))
        }
    } else {
        // cannot exclude upper limit
        None
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_string(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    lower: *const c_char,
    include_lower: bool,
    upper: *const c_char,
    include_upper: bool,
    case_sensitive: bool,
    property_id: u32,
) -> i64 {
    let property = collection.properties.get(property_id as usize);
    isar_try! {
        if let Some(property) = property {
            let lower_bytes = Filter::string_to_bytes(from_c_str(lower)?, case_sensitive);
            let lower = get_lower_str(lower_bytes, include_lower);

            let upper_bytes = Filter::string_to_bytes(from_c_str(upper)?, case_sensitive);
            let upper = get_upper_str(upper_bytes, include_upper);

            let query_filter = if let Some(upper) = upper {
                Filter::byte_string(property, lower, upper, case_sensitive)?
            } else {
                Filter::stat(false)
            };
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
            property_id: u32,
        ) -> i64 {
            let property = collection.properties.get(property_id as usize);
            isar_try! {
                if let Some(property) = property {
                    let str = from_c_str(value)?.unwrap();
                    let query_filter = isar_core::query::filter::Filter::$filter_name(property, str, case_sensitive)?;
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
filter_string_ffi!(string_contains, isar_filter_string_contains);
filter_string_ffi!(string_matches, isar_filter_string_matches);
