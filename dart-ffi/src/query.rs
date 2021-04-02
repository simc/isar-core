use super::raw_object_set::{RawObjectSet, RawObjectSetSend};
use crate::txn::IsarDartTxn;
use crate::UintSend;
use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::query::filter::Filter;
use isar_core::query::index_where_clause::IndexWhereClause;
use isar_core::query::query_builder::QueryBuilder;
use isar_core::query::{Query, Sort};

#[no_mangle]
pub extern "C" fn isar_qb_create(collection: &IsarCollection) -> *mut QueryBuilder {
    let builder = collection.new_query_builder();
    Box::into_raw(Box::new(builder))
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_add_id_where_clause(
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
        let where_clause = collection.new_id_where_clause(Some(lower_oid), Some(upper_oid), sort)?;
        builder.add_id_where_clause(where_clause)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_add_index_where_clause(
    builder: &mut QueryBuilder,
    where_clause: *mut IndexWhereClause,
    include_lower: bool,
    include_upper: bool,
) -> i32 {
    let wc = *Box::from_raw(where_clause);
    isar_try! {
        builder.add_index_where_clause(wc, include_lower, include_upper)?;
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
    case_sensitive: bool,
) -> i32 {
    let property = collection.get_properties().get(property_index as usize);
    isar_try! {
        if let Some((_,property)) = property {
            builder.add_distinct(*property, case_sensitive);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_set_offset_limit(
    builder: &mut QueryBuilder,
    offset: u32,
    limit: u32,
) {
    builder.set_offset(offset as usize);
    builder.set_limit(limit as usize);
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
    query: &'static Query,
    txn: &mut IsarDartTxn,
    result: &'static mut RawObjectSet,
    limit: u32,
) -> i32 {
    let result = RawObjectSetSend(result);
    isar_try_txn!(txn, move |txn| {
        result.0.fill_from_query(query, txn, limit as usize)?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_delete(
    query: &'static Query,
    collection: &'static IsarCollection,
    txn: &mut IsarDartTxn,
    limit: u32,
    count: &'static mut u32,
) -> i32 {
    let limit = limit as usize;
    let count = UintSend(count);
    isar_try_txn!(txn, move |txn| {
        let mut oids_to_delete = vec![];
        query.find_while(txn, |object| {
            let oid = object.read_long(collection.get_oid_property());
            oids_to_delete.push(oid);
            oids_to_delete.len() <= limit
        })?;
        *count.0 = oids_to_delete.len() as u32;
        for oid in oids_to_delete {
            collection.delete(txn, oid)?;
        }
        Ok(())
    })
}
