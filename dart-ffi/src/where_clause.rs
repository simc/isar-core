use crate::from_c_str;
use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::object::data_type::DataType;
use isar_core::object::isar_object::IsarObject;
use isar_core::query::index_where_clause::IndexWhereClause;
use isar_core::query::Sort;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn isar_wc_create(
    collection: &IsarCollection,
    wc: *mut *const IndexWhereClause,
    index_index: i32,
    skip_duplicates: bool,
    ascending: bool,
) -> i32 {
    let sort = if ascending {
        Sort::Ascending
    } else {
        Sort::Descending
    };
    isar_try! {
        let where_clause = collection.new_index_where_clause(index_index as usize, skip_duplicates, sort);
        if let Some(where_clause) = where_clause {
            let ptr = Box::into_raw(Box::new(where_clause));
            wc.write(ptr);
        } else {
            illegal_arg("Unknown index.")?;
        };
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_null(
    where_clause: &mut IndexWhereClause,
    lower_unbounded: bool,
    upper_unbounded: bool,
) -> i32 {
    let p = where_clause.get_next_property().copied();
    isar_try! {
        if let Some(p) = p {
            match p.property.data_type {
                DataType::Byte => {
                    let upper = if upper_unbounded {
                        u8::MAX
                    } else {
                        IsarObject::NULL_BYTE
                    };
                    where_clause.add_byte(IsarObject::NULL_BYTE, upper)?
                },
                DataType::Int => {
                    let upper = if upper_unbounded {
                        i32::MAX
                    } else {
                        IsarObject::NULL_INT
                    };
                    where_clause.add_int(IsarObject::NULL_INT, upper)?
                },
                DataType::Float => {
                    let upper = if upper_unbounded {
                        f32::MAX
                    } else {
                        IsarObject::NULL_FLOAT
                    };
                    where_clause.add_float(IsarObject::NULL_FLOAT, upper)?
                }
                DataType::Long => {
                    let upper = if upper_unbounded {
                        i64::MAX
                    } else {
                        IsarObject::NULL_LONG
                    };
                    where_clause.add_long(IsarObject::NULL_LONG, upper)?
                },
                DataType::Double => {
                    let upper = if upper_unbounded {
                        f64::MAX
                    } else {
                        IsarObject::NULL_DOUBLE
                    };
                    where_clause.add_double(IsarObject::NULL_DOUBLE, upper)?
                }
                DataType::String => where_clause.add_string(None, lower_unbounded, None, upper_unbounded)?,
                _ => unreachable!(),
            }
        } else {
           illegal_arg("Too many values for WhereClause")?;
        }
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_byte(
    where_clause: &mut IndexWhereClause,
    lower: u8,
    upper: u8,
) -> i32 {
    isar_try! {
        where_clause.add_byte(lower, upper)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_long(
    where_clause: &mut IndexWhereClause,
    lower: i64,
    upper: i64,
) -> i32 {
    let next = where_clause.get_next_property().copied();
    isar_try! {
        if let Some(next) = next {
            if next.property.data_type == DataType::Int {
                let lower = lower.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                let upper = upper.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                where_clause.add_int(lower, upper)?;
            } else {
                where_clause.add_long(lower, upper)?;
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_double(
    where_clause: &mut IndexWhereClause,
    lower: f64,
    upper: f64,
) -> i32 {
    let next = where_clause.get_next_property().copied();
    isar_try! {
        if let Some(next) = next {
            if next.property.data_type == DataType::Float {
                where_clause.add_float(lower as f32, upper as f32)?;
            } else {
                where_clause.add_double(lower, upper)?;
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_string(
    where_clause: &mut IndexWhereClause,
    lower: *const c_char,
    upper: *const c_char,
    lower_unbounded: bool,
    upper_unbounded: bool,
) -> i32 {
    let lower = if !lower.is_null() {
        Some(from_c_str(lower).unwrap())
    } else {
        None
    };
    let upper = if !upper.is_null() {
        Some(from_c_str(upper).unwrap())
    } else {
        None
    };
    isar_try! {
        where_clause.add_string(
            lower,
            lower_unbounded,
            upper,
            upper_unbounded,
        )?;
    }
}
