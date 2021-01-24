use super::raw_object_set::{RawObject, RawObjectSend, RawObjectSet, RawObjectSetSend};
use crate::async_txn::IsarAsyncTxn;
use crate::{BoolSend, IntSend};
use isar_core::collection::IsarCollection;
use isar_core::error::{illegal_arg, Result};
use isar_core::object::property::Property;
use isar_core::query::filter::Filter;
use isar_core::query::query::{Query, Sort};
use isar_core::query::query_builder::QueryBuilder;
use isar_core::query::where_clause::WhereClause;
use isar_core::txn::IsarTxn;

#[no_mangle]
pub extern "C" fn isar_qb_create(collection: &IsarCollection) -> *mut QueryBuilder {
    let builder = collection.new_query_builder();
    Box::into_raw(Box::new(builder))
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
pub unsafe extern "C" fn isar_add_sort_by(
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

pub unsafe extern "C" fn isar_set_distinct(
    collection: &IsarCollection,
    builder: &mut QueryBuilder,
    distinct_property_indices: *const u32,
    distinct_property_indices_length: u32,
) -> i32 {
    let properties: Option<Vec<Property>> = std::slice::from_raw_parts(
        distinct_property_indices,
        distinct_property_indices_length as usize,
    )
    .iter()
    .map(|index| {
        collection
            .get_properties()
            .get(*index as usize)
            .map(|(_, p)| *p)
    })
    .collect();
    isar_try! {
        if let Some(properties) = properties {
            builder.set_distinct(&properties);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

pub unsafe extern "C" fn isar_set_offset_limit(
    builder: &mut QueryBuilder,
    offset: u32,
    limit: u32,
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
pub unsafe extern "C" fn isar_q_find_first(
    query: &Query,
    txn: &mut IsarTxn<'static>,
    object: &mut RawObject,
) -> i32 {
    isar_try! {
        query.find_while(txn, |oid, obj| {
            object.set_object_id(*oid);
            object.set_object(obj);
            false
        })?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_find_first_async(
    query: &'static Query,
    txn: &IsarAsyncTxn,
    object: &'static mut RawObject,
) {
    let object = RawObjectSend(object);
    txn.exec(move |txn| {
        query.find_while(txn, |oid, obj| {
            object.0.set_object_id(*oid);
            object.0.set_object(obj);
            false
        })
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_find_all(
    query: &Query,
    txn: &mut IsarTxn<'static>,
    result: &mut RawObjectSet,
) -> i32 {
    isar_try! {
        result.fill_from_query(query, txn)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_find_all_async(
    query: &'static Query,
    txn: &IsarAsyncTxn,
    result: &'static mut RawObjectSet,
) {
    let result = RawObjectSetSend(result);
    txn.exec(move |txn| result.0.fill_from_query(query, txn));
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_count(query: &Query, txn: &mut IsarTxn, count: &mut i64) -> i32 {
    isar_try! {
        *count = query.count(txn)? as i64;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_count_async(
    query: &'static Query,
    txn: &IsarAsyncTxn,
    count: &'static mut i64,
) {
    let count = IntSend(count);
    txn.exec(move |txn| -> Result<()> {
        *(count.0) = query.count(txn)? as i64;
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_delete_first(
    query: &Query,
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    deleted: &mut bool,
) -> i32 {
    isar_try! {
        *deleted = false;
        query.delete_while(txn, collection, |_,_| {
            if !*deleted {
                *deleted = true;
                true
            } else {
                false
            }
        })?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_delete_first_async(
    query: &'static Query,
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    deleted: &'static mut bool,
) {
    *deleted = false;
    let deleted = BoolSend(deleted);
    txn.exec(move |txn| -> Result<()> {
        query.delete_while(txn, collection, |_, _| {
            if !*deleted.0 {
                *deleted.0 = true;
                true
            } else {
                false
            }
        })?;
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_delete_all(
    query: &Query,
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    count: &mut i64,
) -> i32 {
    isar_try! {
        *count = query.delete_while(txn, collection, |_,_| true)? as i64;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_delete_all_async(
    query: &'static Query,
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    count: &'static mut i64,
) {
    let count = IntSend(count);
    txn.exec(move |txn| -> Result<()> {
        *(count.0) = query.delete_while(txn, collection, |_, _| true)? as i64;
        Ok(())
    });
}
