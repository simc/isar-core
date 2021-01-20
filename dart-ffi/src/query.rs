use super::raw_object_set::RawObjectSet;
use crate::async_txn::IsarAsyncTxn;
use crate::raw_object_set::RawObjectSetSend;
use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::query::filter::Filter;
use isar_core::query::query::Query;
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
pub unsafe extern "C" fn isar_qb_build(builder: *mut QueryBuilder) -> *mut Query {
    let query = Box::from_raw(builder).build();
    Box::into_raw(Box::new(query))
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_free(query: *mut Query) {
    Box::from_raw(query);
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

struct IntSend(&'static mut i64);

unsafe impl Send for IntSend {}

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
