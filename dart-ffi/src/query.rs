use super::raw_object_set::{RawObjectSet, RawObjectSetSend};
use crate::async_txn::IsarAsyncTxn;
use crate::UintSend;
use isar_core::collection::IsarCollection;
use isar_core::error::{illegal_arg, Result};
use isar_core::query::filter::Filter;
use isar_core::query::query_builder::QueryBuilder;
use isar_core::query::where_clause::WhereClause;
use isar_core::query::{Query, Sort};
use isar_core::txn::IsarTxn;

#[no_mangle]
pub extern "C" fn isar_qb_create(collection: &IsarCollection) -> *mut QueryBuilder {
    let builder = collection.new_query_builder();
    Box::into_raw(Box::new(builder))
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_add_primary_where_clause(
    collection: &IsarCollection,
    builder: &mut QueryBuilder,
    lower_oid: i64,
    upper_oid: i64,
    ascending: bool,
) -> i32 {
    let sort = if ascending {
        Sort::Ascending
    } else {
        Sort::Descending
    };
    isar_try! {
        let where_clause = collection.new_primary_where_clause(Some(lower_oid), Some(upper_oid), sort)?;
        builder.add_where_clause(where_clause,true,true)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_add_where_clause(
    builder: &mut QueryBuilder,
    where_clause: *mut WhereClause,
    include_lower: bool,
    include_upper: bool,
) -> i32 {
    let wc = *Box::from_raw(where_clause);
    isar_try! {
        builder.add_where_clause(wc, include_lower, include_upper)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_set_filter(builder: &mut QueryBuilder, filter: *mut Filter) {
    let filter = *Box::from_raw(filter);
    builder.set_filter(filter);
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_add_sort_by(
    collection: &IsarCollection,
    builder: &mut QueryBuilder,
    property_index: u32,
    asc: bool,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    let sort = if asc {
        Sort::Ascending
    } else {
        Sort::Descending
    };
    isar_try! {
        if let Some((_,property)) = property {
            builder.add_sort(*property, sort);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_add_distinct_by(
    collection: &IsarCollection,
    builder: &mut QueryBuilder,
    property_index: u32,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    isar_try! {
        if let Some((_,property)) = property {
            builder.add_distinct(*property);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_set_offset_limit(
    builder: &mut QueryBuilder,
    offset: i64,
    limit: i64,
) -> i32 {
    let offset = if offset > 0 {
        Some(offset as usize)
    } else {
        None
    };
    let limit = if limit > 0 {
        Some(limit as usize)
    } else {
        None
    };
    isar_try! {
        builder.set_offset_limit(offset, limit)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_build(builder: *mut QueryBuilder) -> *mut Query {
    let query = Box::from_raw(builder).build();
    Box::into_raw(Box::new(query))
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_free(query: *mut Query) {
    Box::from_raw(query);
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_find(
    query: &Query,
    txn: &mut IsarTxn<'static>,
    result: &mut RawObjectSet,
    limit: u32,
) -> i32 {
    isar_try! {
        result.fill_from_query(query, txn, limit as usize)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_find_async(
    query: &'static Query,
    txn: &IsarAsyncTxn,
    result: &'static mut RawObjectSet,
    limit: u32,
) {
    let result = RawObjectSetSend(result);
    txn.exec(move |txn| result.0.fill_from_query(query, txn, limit as usize));
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_count(query: &Query, txn: &mut IsarTxn, count: &mut u32) -> i32 {
    isar_try! {
        *count = query.count(txn)? as u32;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_count_async(
    query: &'static Query,
    txn: &IsarAsyncTxn,
    count: &'static mut u32,
) {
    let count = UintSend(count);
    txn.exec(move |txn| -> Result<()> {
        *(count.0) = query.count(txn)? as u32;
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_delete(
    query: &Query,
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    limit: u32,
    count: &mut u32,
) -> i32 {
    isar_try! {
        let mut deleted_count = 0;
        query.delete_while(txn, collection, |_| {
            deleted_count += 1;
            deleted_count <= limit
        })?;
        *count = deleted_count;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_delete_async(
    query: &'static Query,
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    limit: u32,
    count: &'static mut u32,
) {
    let count = UintSend(count);
    txn.exec(move |txn| -> Result<()> {
        let mut deleted_count = 0;
        query.delete_while(txn, collection, |_| {
            deleted_count += 1;
            deleted_count <= limit
        })?;
        *(count.0) = deleted_count;
        Ok(())
    });
}
