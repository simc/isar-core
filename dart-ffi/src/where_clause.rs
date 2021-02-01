use crate::from_c_str;
use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::query::where_clause::WhereClause;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn isar_wc_create(
    collection: &IsarCollection,
    wc: *mut *const WhereClause,
    primary: bool,
    index_index: u32,
    skip_duplicates: bool,
) -> i32 {
    isar_try! {
        let where_clause = if primary {
            Some(collection.new_primary_where_clause())
        } else {
            collection.new_secondary_where_clause(index_index as usize, skip_duplicates)
        };
        if let Some(where_clause) = where_clause {
            let ptr = Box::into_raw(Box::new(where_clause));
            wc.write(ptr);
        } else {
            illegal_arg("Unknown index.")?;
        };
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_byte(where_clause: &mut WhereClause, lower: u8, upper: u8) {
    where_clause.add_byte(lower, upper);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_int(where_clause: &mut WhereClause, lower: i32, upper: i32) {
    where_clause.add_int(lower, upper);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_float(where_clause: &mut WhereClause, lower: f32, upper: f32) {
    where_clause.add_float(lower, upper);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_long(where_clause: &mut WhereClause, lower: i64, upper: i64) {
    where_clause.add_long(lower, upper);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_double(where_clause: &mut WhereClause, lower: f64, upper: f64) {
    where_clause.add_double(lower, upper);
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_string_hash(
    where_clause: &mut WhereClause,
    value: *const c_char,
    case_sensitive: bool,
) {
    let str = if !value.is_null() {
        Some(from_c_str(value).unwrap())
    } else {
        None
    };
    where_clause.add_string_hash(str, case_sensitive);
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_string_value(
    where_clause: &mut WhereClause,
    lower: *const c_char,
    upper: *const c_char,
    case_sensitive: bool,
) {
    let lower_str = if !lower.is_null() {
        Some(from_c_str(lower).unwrap())
    } else {
        None
    };
    let upper_str = if !upper.is_null() {
        Some(from_c_str(upper).unwrap())
    } else {
        None
    };
    where_clause.add_string_value(lower_str, upper_str, case_sensitive);
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_string_word(
    where_clause: &mut WhereClause,
    lower: *const c_char,
    upper: *const c_char,
    case_sensitive: bool,
) {
    let lower_str = from_c_str(lower).unwrap();
    let upper_str = from_c_str(upper).unwrap();
    where_clause.add_string_word(lower_str, upper_str, case_sensitive);
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_oid_string(
    where_clause: &mut WhereClause,
    lower: *const c_char,
    upper: *const c_char,
) {
    let lower_str = from_c_str(lower).unwrap();
    let upper_str = from_c_str(upper).unwrap();
    where_clause.add_oid_string(lower_str, upper_str);
}
