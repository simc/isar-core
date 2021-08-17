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
        AndCond::filter(filters)
    } else {
        OrCond::filter(filters)
    };
    let ptr = Box::into_raw(Box::new(and_or));
    filter.write(ptr);
    0
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_not(filter: *mut *const Filter, condition: *mut Filter) -> u8 {
    let condition = *Box::from_raw(condition);
    let not = NotCond::filter(condition);
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
        let query_filter = LinkCond::filter(collection, link_index as usize, backlink, condition)?;
        let ptr = Box::into_raw(Box::new(query_filter));
        filter.write(ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_null_between(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    upper_unbounded: bool,
    property_index: u32,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    isar_try! {
        if let Some((_, property)) = property {
            let query_filter = match property.data_type {
                DataType::Byte => {
                    let upper = if upper_unbounded {
                        u8::MAX
                    } else {
                        IsarObject::NULL_BYTE
                    };
                    ByteBetweenCond::filter(*property, IsarObject::NULL_BYTE, upper)?
                },
                DataType::Int => {
                    let upper = if upper_unbounded {
                        i32::MAX
                    } else {
                        IsarObject::NULL_INT
                    };
                    IntBetweenCond::filter(*property, IsarObject::NULL_INT, upper)?
                },
                DataType::Float => {
                    let upper = if upper_unbounded {
                        f32::MAX
                    } else {
                        IsarObject::NULL_FLOAT
                    };
                    FloatBetweenCond::filter(*property, IsarObject::NULL_FLOAT, upper)?
                },
                DataType::Long => {
                    let upper = if upper_unbounded {
                        i64::MAX
                    } else {
                        IsarObject::NULL_LONG
                    };
                    LongBetweenCond::filter(*property, IsarObject::NULL_LONG, upper)?
                },
                DataType::Double => {
                    let upper = if upper_unbounded {
                        f64::MAX
                    } else {
                        IsarObject::NULL_DOUBLE
                    };
                    DoubleBetweenCond::filter(*property, IsarObject::NULL_DOUBLE, upper)?
                },
                DataType::String => StringBetweenCond::filter(*property, None, None, false)?,
                _ => unreachable!(),
            };
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
           illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_byte_between(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    lower: u8,
    upper: u8,
    property_index: u32,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    isar_try! {
        if let Some((_, property)) = property {
            let query_filter = ByteBetweenCond::filter(*property, lower, upper)?;
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_long_between(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    lower: i64,
    upper: i64,
    property_index: u32,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    isar_try! {
        if let Some((_, property)) = property {
            let query_filter = if property.data_type == DataType::Int {
                let lower = lower.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                let upper = upper.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                IntBetweenCond::filter(*property, lower, upper)?
            } else {
                LongBetweenCond::filter(*property, lower, upper)?
            };
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_double_between(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    lower: f64,
    upper: f64,
    property_index: u32,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    isar_try! {
        if let Some((_, property)) = property {
            let query_filter = if property.data_type == DataType::Float {
                FloatBetweenCond::filter(*property, lower as f32, upper as f32)?
            } else {
                DoubleBetweenCond::filter(*property, lower, upper)?
            };
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[macro_export]
macro_rules! filter_single_value_ffi {
    ($filter_name:ident, $function_name:ident, $type:ty) => {
        #[no_mangle]
        pub unsafe extern "C" fn $function_name(
            collection: &IsarCollection,
            filter: *mut *const Filter,
            value: $type,
            property_index: u32,
        ) -> i32 {
            let property = collection.get_properties().get(property_index as usize);
            isar_try! {
                if let Some((_, property)) = property {
                    let query_filter = isar_core::query::filter::$filter_name::filter(*property, value)?;
                    let ptr = Box::into_raw(Box::new(query_filter));
                    filter.write(ptr);
                } else {
                    illegal_arg("Property does not exist.")?;
                }
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_byte_list_contains(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    value: u8,
    property_index: u32,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    isar_try! {
        if let Some((_, property)) = property {
            let query_filter = ByteListContainsCond::filter(*property, value)?;
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_long_list_contains(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    value: i64,
    property_index: u32,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    isar_try! {
        if let Some((_, property)) = property {
            let query_filter = if property.data_type == DataType::Int {
                let value = value.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                IntListContainsCond::filter(*property, value)?
            } else {
                LongListContainsCond::filter(*property, value)?
            };
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_string_between(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    lower: *const c_char,
    upper: *const c_char,
    case_sensitive: bool,
    property_index: u32,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    isar_try! {
        if let Some((_, property)) = property {
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
            let query_filter = isar_core::query::filter::StringBetweenCond::filter(*property, lower, upper, case_sensitive)?;
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
            let property = collection.get_properties().get(property_index as usize);
            isar_try! {
                if let Some((_, property)) = property {
                    let str = if !value.is_null() {
                        Some(from_c_str(value)?)
                    } else {
                        None
                    };
                    let query_filter = isar_core::query::filter::$filter_name::filter(*property, str, case_sensitive)?;
                    let ptr = Box::into_raw(Box::new(query_filter));
                    filter.write(ptr);
                } else {
                    illegal_arg("Property does not exist.")?;
                }
            }
        }
    }
}

filter_string_ffi!(StringStartsWithCond, isar_filter_string_starts_with);
filter_string_ffi!(StringEndsWithCond, isar_filter_string_ends_with);
filter_string_ffi!(StringMatchesCond, isar_filter_string_matches);
filter_string_ffi!(StringListContainsCond, isar_filter_string_list_contains);
