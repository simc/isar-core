use crate::from_c_str;
use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::index::IndexType;
use isar_core::query::where_clause::WhereClause;
use isar_core::query::Sort;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn isar_wc_create(
    collection: &IsarCollection,
    wc: *mut *const WhereClause,
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
        let where_clause = if index_index < 0 {
            Some(collection.new_primary_where_clause(sort))
        } else {
            collection.new_secondary_where_clause(index_index as usize, skip_duplicates, sort)
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
pub unsafe extern "C" fn isar_wc_add_string(
    where_clause: &mut WhereClause,
    lower: *const c_char,
    upper: *const c_char,
    lower_unbounded: bool,
    upper_unbounded: bool,
    case_sensitive: bool,
    index_type: u8,
) {
    let index_type = IndexType::from_ordinal(index_type).unwrap();
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
    where_clause.add_string(
        lower,
        lower_unbounded,
        upper,
        upper_unbounded,
        case_sensitive,
        index_type,
    );
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
