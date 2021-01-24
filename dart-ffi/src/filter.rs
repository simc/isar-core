use float_next_after::NextAfter;
use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::query::filter::{And, Filter, IsNull, Or};
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
        And::filter(filters)
    } else {
        Or::filter(filters)
    };
    let ptr = Box::into_raw(Box::new(and_or));
    filter.write(ptr);
    0
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_is_null(
    collection: &IsarCollection,
    filter: *mut *const Filter,
    is_null: bool,
    property_index: u32,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    isar_try! {
        if let Some((_,property)) = property {
            let query_filter = IsNull::filter(*property, is_null);
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[macro_export]
macro_rules! filter_between_ffi {
    ($filter_name:ident, $function_name:ident, $next:ident, $prev:ident, $type:ty) => {
        #[no_mangle]
        pub unsafe extern "C" fn $function_name(
            collection: &IsarCollection,
            filter: *mut *const Filter,
            lower: $type,
            include_lower: bool,
            upper: $type,
            include_upper: bool,
            property_index: u32,
        ) -> i32 {
            let property = collection.get_properties().get(property_index as usize);
            let lower = if !include_lower {
                $next(lower)
            } else {
                Some(lower)
            };
            let upper = if !include_upper {
                $prev(upper)
            } else {
                Some(upper)
            };
            isar_try! {
                if let Some((_, property)) = property {
                    let query_filter = if let (Some(lower), Some(upper)) = (lower, upper) {
                        isar_core::query::filter::$filter_name::filter(*property, lower, upper)?
                    } else {
                        isar_core::query::filter::Never::filter()
                    };
                    let ptr = Box::into_raw(Box::new(query_filter));
                    filter.write(ptr);
                } else {
                    illegal_arg("Property does not exist.")?;
                }
            }
        }
    };
}

fn next_byte(value: u8) -> Option<u8> {
    value.checked_add(1)
}

fn prev_byte(value: u8) -> Option<u8> {
    value.checked_sub(1)
}

fn next_int(value: i32) -> Option<i32> {
    value.checked_add(1)
}

fn prev_int(value: i32) -> Option<i32> {
    value.checked_sub(1)
}

fn next_float(value: f32) -> Option<f32> {
    if value == f32::INFINITY {
        None
    } else if value == f32::NEG_INFINITY {
        Some(f32::MIN)
    } else if value.is_nan() {
        Some(f32::NEG_INFINITY)
    } else {
        Some(value.next_after(f32::INFINITY))
    }
}

fn prev_float(value: f32) -> Option<f32> {
    if value == f32::INFINITY {
        Some(f32::MIN)
    } else if value == f32::NEG_INFINITY || value.is_nan() {
        None
    } else {
        Some(value.next_after(f32::NEG_INFINITY))
    }
}

fn next_long(value: i64) -> Option<i64> {
    value.checked_add(1)
}

fn prev_long(value: i64) -> Option<i64> {
    value.checked_sub(1)
}

fn next_double(value: f64) -> Option<f64> {
    if value == f64::INFINITY {
        None
    } else if value == f64::NEG_INFINITY {
        Some(f64::MIN)
    } else if value.is_nan() {
        Some(f64::NEG_INFINITY)
    } else {
        Some(value.next_after(f64::INFINITY))
    }
}

fn prev_double(value: f64) -> Option<f64> {
    if value == f64::INFINITY {
        Some(f64::MIN)
    } else if value == f64::NEG_INFINITY || value.is_nan() {
        None
    } else {
        Some(value.next_after(f64::NEG_INFINITY))
    }
}

filter_between_ffi!(
    ByteBetween,
    isar_filter_byte_between,
    next_byte,
    prev_byte,
    u8
);
filter_between_ffi!(IntBetween, isar_filter_int_between, next_int, prev_int, i32);
filter_between_ffi!(
    FloatBetween,
    isar_filter_float_between,
    next_float,
    prev_float,
    f32
);
filter_between_ffi!(
    LongBetween,
    isar_filter_long_between,
    next_long,
    prev_long,
    i64
);
filter_between_ffi!(
    DoubleBetween,
    isar_filter_double_between,
    next_double,
    prev_double,
    f64
);

#[macro_export]
macro_rules! filter_not_equal_to_ffi {
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

filter_not_equal_to_ffi!(ByteNotEqual, isar_filter_byte_not_equal, u8);
filter_not_equal_to_ffi!(IntNotEqual, isar_filter_int_not_equal, i32);
filter_not_equal_to_ffi!(LongNotEqual, isar_filter_long_not_equal, i64);
