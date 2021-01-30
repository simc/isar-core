use crate::collection::IsarCollection;
use crate::error::Result;
use crate::object::data_type::DataType;
use crate::object::isar_object::{IsarObject, Property};
use crate::object::object_id::ObjectId;
use crate::query::filter::{Condition, Filter, Static};
use crate::query::where_clause::WhereClause;
use crate::query::where_executor::WhereExecutor;
use crate::txn::{Cursors, IsarTxn};
use hashbrown::HashSet;
use std::cmp::Ordering;
use std::hash::Hasher;
use wyhash::WyHash;

pub mod filter;
pub mod query_builder;
pub mod where_clause;
pub mod where_executor;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Sort {
    Ascending,
    Descending,
}

pub enum Case {
    Sensitive,
    Insensitive,
}

#[derive(Clone)]
pub struct Query {
    oid_type: DataType,
    where_clauses: Vec<WhereClause>,
    where_clauses_overlapping: bool,
    filter: Option<Filter>,
    sort: Vec<(Property, Sort)>,
    distinct: Option<Vec<Property>>,
    offset_limit: Option<(usize, usize)>,
}

impl<'txn> Query {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        oid_type: DataType,
        where_clauses: Vec<WhereClause>,
        filter: Option<Filter>,
        sort: Vec<(Property, Sort)>,
        distinct: Option<Vec<Property>>,
        offset_limit: Option<(usize, usize)>,
    ) -> Self {
        Query {
            oid_type,
            where_clauses,
            where_clauses_overlapping: true,
            filter,
            sort,
            distinct,
            offset_limit,
        }
    }

    pub(crate) fn execute_raw<F>(&self, cursors: &mut Cursors<'txn>, mut callback: F) -> Result<()>
    where
        F: FnMut(&mut Cursors<'txn>, ObjectId<'txn>, IsarObject<'txn>) -> Result<bool>,
    {
        let mut executor = WhereExecutor::new(
            self.oid_type,
            &self.where_clauses,
            self.where_clauses_overlapping,
        );

        let static_filter = Static::filter(true);
        let filter = self.filter.as_ref().unwrap_or(&static_filter);
        executor.execute(cursors, |cursors, oid, object| {
            if filter.evaluate(object) {
                callback(cursors, oid, object)
            } else {
                Ok(true)
            }
        })
    }

    fn execute_unsorted<F>(&self, cursors: &mut Cursors<'txn>, callback: F) -> Result<()>
    where
        F: FnMut(&mut Cursors<'txn>, ObjectId<'txn>, IsarObject<'txn>) -> Result<bool>,
    {
        if self.distinct.is_some() {
            let callback = self.add_distinct_unsorted(callback);
            let callback = self.add_offset_limit_unsorted(callback);
            self.execute_raw(cursors, callback)
        } else {
            let callback = self.add_offset_limit_unsorted(callback);
            self.execute_raw(cursors, callback)
        }
    }

    fn add_distinct_unsorted<F>(
        &self,
        mut callback: F,
    ) -> impl FnMut(&mut Cursors<'txn>, ObjectId<'txn>, IsarObject<'txn>) -> Result<bool>
    where
        F: FnMut(&mut Cursors<'txn>, ObjectId<'txn>, IsarObject<'txn>) -> Result<bool>,
    {
        let properties = self.distinct.as_ref().unwrap().clone();
        let mut hashes = HashSet::new();
        move |cursors, oid, object| {
            let mut hasher = WyHash::default();
            for property in &properties {
                object.hash_property(*property, &mut hasher);
            }
            let hash = hasher.finish();
            if hashes.insert(hash) {
                callback(cursors, oid, object)
            } else {
                Ok(true)
            }
        }
    }

    fn add_offset_limit_unsorted<F>(
        &self,
        mut callback: F,
    ) -> impl FnMut(&mut Cursors<'txn>, ObjectId<'txn>, IsarObject<'txn>) -> Result<bool>
    where
        F: FnMut(&mut Cursors<'txn>, ObjectId<'txn>, IsarObject<'txn>) -> Result<bool>,
    {
        let (offset, limit) = self.offset_limit.unwrap_or((0, usize::MAX));
        let mut count = 0;
        move |cursors, key, value| {
            let result = if count >= offset {
                callback(cursors, key, value)?
            } else {
                true
            };
            count += 1;
            let next = result && limit.saturating_add(offset) > count;
            Ok(next)
        }
    }

    fn execute_sorted(
        &self,
        cursors: &mut Cursors<'txn>,
    ) -> Result<Vec<(ObjectId<'txn>, IsarObject<'txn>)>> {
        let mut results = vec![];
        self.execute_raw(cursors, |_, key, val| {
            results.push((key, val));
            Ok(true)
        })?;

        results.sort_unstable_by(|(_, o1), (_, o2)| {
            for (p, sort) in &self.sort {
                let ord = o1.compare_property(o2, *p);
                if ord != Ordering::Equal {
                    return if *sort == Sort::Ascending {
                        ord
                    } else {
                        ord.reverse()
                    };
                }
            }
            Ordering::Equal
        });

        Ok(self.add_distinct_sorted(results))
    }

    fn add_distinct_sorted(
        &self,
        results: Vec<(ObjectId<'txn>, IsarObject<'txn>)>,
    ) -> Vec<(ObjectId<'txn>, IsarObject<'txn>)> {
        let properties = self.distinct.as_ref().unwrap().clone();
        let mut hashes = HashSet::new();
        results
            .into_iter()
            .filter(|(_, object)| {
                let mut hasher = WyHash::default();
                for property in &properties {
                    object.hash_property(*property, &mut hasher);
                }
                let hash = hasher.finish();
                hashes.insert(hash)
            })
            .collect()
    }

    fn add_offset_limit_sorted(
        &self,
        results: Vec<(ObjectId<'txn>, IsarObject<'txn>)>,
    ) -> impl IntoIterator<Item = (ObjectId<'txn>, IsarObject<'txn>)> {
        let (offset, limit) = self.offset_limit.unwrap_or((0, usize::MAX));
        results.into_iter().skip(offset).take(limit)
    }

    pub(crate) fn matches_wc_filter(&self, oid: &ObjectId, object: IsarObject) -> bool {
        let oid_bytes = oid.as_bytes();
        let wc_matches = self
            .where_clauses
            .iter()
            .any(|wc| wc.object_matches(oid_bytes, object));
        if !wc_matches {
            return false;
        }

        if let Some(filter) = &self.filter {
            filter.evaluate(object)
        } else {
            true
        }
    }

    pub(crate) fn find_all_internal<F>(
        &self,
        cursors: &mut Cursors<'txn>,
        skip_sorting: bool,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(&mut Cursors<'txn>, ObjectId<'txn>, IsarObject<'txn>) -> Result<bool>,
    {
        if self.sort.is_empty() || skip_sorting {
            self.execute_unsorted(cursors, callback)?;
        } else {
            let results = self.execute_sorted(cursors)?;
            let results_iter = self.add_offset_limit_sorted(results);
            for (oid, object) in results_iter {
                if !callback(cursors, oid, object)? {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn find_while<F>(&self, txn: &mut IsarTxn<'txn>, mut callback: F) -> Result<()>
    where
        F: FnMut(ObjectId<'txn>, IsarObject<'txn>) -> bool,
    {
        txn.read(|cursors| {
            self.find_all_internal(cursors, false, |_, oid, object| Ok(callback(oid, object)))
        })
    }

    pub fn delete_while<F>(
        &self,
        txn: &mut IsarTxn<'txn>,
        collection: &IsarCollection,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(&ObjectId<'txn>, IsarObject<'txn>) -> bool,
    {
        let skip_sorting = self.offset_limit.is_none() && self.distinct.is_none();
        let mut count = 0;
        txn.write(|cursors, change_set| {
            self.find_all_internal(cursors, skip_sorting, |cursors, oid, object| {
                if !callback(&oid, object) {
                    return Ok(false);
                }
                count += 1;
                collection.delete_object_internal(cursors, change_set, &oid, object, true)?;
                Ok(true)
            })
        })?;
        Ok(count)
    }

    pub fn find_all_vec(
        &self,
        txn: &mut IsarTxn<'txn>,
    ) -> Result<Vec<(ObjectId<'txn>, IsarObject<'txn>)>> {
        let mut results = vec![];
        self.find_while(txn, |oid, value| {
            results.push((oid, value));
            true
        })?;
        Ok(results)
    }

    pub fn count(&self, txn: &mut IsarTxn) -> Result<u32> {
        let mut counter = 0;
        self.find_while(txn, |_, _| {
            counter += 1;
            true
        })?;
        Ok(counter)
    }
}

/*#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::IsarInstance;
    use crate::object::object_id::ObjectId;
    use crate::{col, ind, isar, set};
    use std::sync::Arc;

    fn get_col(data: Vec<(i32, String)>) -> (Arc<IsarInstance>, Vec<ObjectId>) {
        isar!(isar, col => col!(field1 => Int, field2 => String; ind!(field1, field2; true), ind!(field2)));
        let mut txn = isar.begin_txn(true).unwrap();
        let mut ids = vec![];
        for (f1, f2) in data {
            let mut o = col.new_object_builder(None);
            o.write_int(f1);
            o.write_string(Some(&f2));
            let bytes = o.finish();
            ids.push(col.put(&mut txn, None, bytes.as_ref()).unwrap());
        }
        txn.commit().unwrap();
        (isar, ids)
    }

    fn keys(result: Vec<(&ObjectId, &[u8])>) -> Vec<ObjectId> {
        result.iter().map(|(k, _)| **k).collect()
    }

    #[test]
    fn test_no_where_clauses() {
        let (isar, ids) = get_col(vec![(1, "a".to_string()), (2, "b".to_string())]);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false).unwrap();

        let q = col.new_query_builder().build();
        let results = q.find_all_vec(&mut txn).unwrap();

        assert_eq!(keys(results), vec![ids[0], ids[1]]);
    }

    #[test]
    fn test_single_primary_where_clause() {}

    #[test]
    fn test_single_secondary_where_clause() {
        let (isar, ids) = get_col(vec![
            (1, "a".to_string()),
            (1, "b".to_string()),
            (1, "c".to_string()),
            (2, "d".to_string()),
            (2, "a".to_string()),
            (3, "b".to_string()),
        ]);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false).unwrap();

        let mut wc = col.new_secondary_where_clause(0, false).unwrap();
        wc.add_int(1, 1);

        let mut qb = col.new_query_builder();
        qb.add_where_clause(wc.clone(), true, true).unwrap();
        let q = qb.build();

        let results = q.find_all_vec(&mut txn).unwrap();
        assert_eq!(keys(results), vec![ids[0], ids[1], ids[2]]);

        wc.add_string_value(Some("b"), Some("x"));
        let mut qb = col.new_query_builder();
        qb.add_where_clause(wc, true, true).unwrap();
        let q = qb.build();

        let results = q.find_all_vec(&mut txn).unwrap();
        assert_eq!(keys(results), vec![ids[1], ids[2]]);
    }

    #[test]
    fn test_single_secondary_where_clause_dup() {
        let (isar, ids) = get_col(vec![
            (1, "aa".to_string()),
            (2, "ab".to_string()),
            (4, "bb".to_string()),
            (3, "ab".to_string()),
        ]);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false).unwrap();

        let mut wc = col.new_secondary_where_clause(1, false).unwrap();
        wc.add_string_value(Some("ab"), Some("xx"));

        let mut qb = col.new_query_builder();
        qb.add_where_clause(wc, true, true).unwrap();
        let q = qb.build();

        let results = q.find_all_vec(&mut txn).unwrap();
        assert_eq!(keys(results), vec![ids[1], ids[3], ids[2]]);

        let mut wc = col.new_secondary_where_clause(1, false).unwrap();
        wc.add_string_value(Some("ab"), Some("ab"));
        let mut qb = col.new_query_builder();
        qb.add_where_clause(wc, true, true).unwrap();
        let q = qb.build();

        let results = q.find_all_vec(&mut txn).unwrap();
        assert_eq!(keys(results), vec![ids[1], ids[3]]);
    }

    #[test]
    fn test_multiple_where_clauses() {
        let (isar, ids) = get_col(vec![
            (1, "aa".to_string()),
            (1, "ab".to_string()),
            (0, "ab".to_string()),
            (1, "bb".to_string()),
            (0, "bb".to_string()),
            (1, "bc".to_string()),
        ]);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false).unwrap();

        let mut primary_wc = col.new_primary_where_clause();
        primary_wc.add_oid(ids[5]);

        let mut secondary_wc = col.new_secondary_where_clause(0, false).unwrap();
        secondary_wc.add_int(0, 0);

        let mut secondary_dup_wc = col.new_secondary_where_clause(1, false).unwrap();
        secondary_dup_wc.add_string_value(None, Some("aa"));

        let mut qb = col.new_query_builder();
        qb.add_where_clause(primary_wc, true, true).unwrap();
        qb.add_where_clause(secondary_wc, true, true).unwrap();
        qb.add_where_clause(secondary_dup_wc, true, true).unwrap();
        let q = qb.build();

        let results = q.find_all_vec(&mut txn).unwrap();
        let set: HashSet<ObjectId> = keys(results).into_iter().collect();
        assert_eq!(set, set!(ids[0], ids[2], ids[4], ids[5]));
    }
}*/
