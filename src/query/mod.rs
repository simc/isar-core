use crate::collection::IsarCollection;
use crate::error::Result;
use crate::object::data_type::DataType;
use crate::object::isar_object::{IsarObject, Property};
use crate::object::object_id::ObjectId;
use crate::query::filter::{Condition, Filter, StaticCond};
use crate::query::where_clause::WhereClause;
use crate::query::where_executor::WhereExecutor;
use crate::txn::{Cursors, IsarTxn};
use hashbrown::HashSet;
use std::cmp::Ordering;
use std::hash::Hasher;
use wyhash::WyHash;

mod fast_wild_compare;
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
    distinct: Vec<Property>,
    offset_limit: Option<(usize, usize)>,
}

impl<'txn> Query {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        oid_type: DataType,
        where_clauses: Vec<WhereClause>,
        filter: Option<Filter>,
        sort: Vec<(Property, Sort)>,
        distinct: Vec<Property>,
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

        let static_filter = StaticCond::filter(true);
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
        if !self.distinct.is_empty() {
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
        let properties = self.distinct.clone();
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

        if !self.distinct.is_empty() {
            Ok(self.add_distinct_sorted(results))
        } else {
            Ok(results)
        }
    }

    fn add_distinct_sorted(
        &self,
        results: Vec<(ObjectId<'txn>, IsarObject<'txn>)>,
    ) -> Vec<(ObjectId<'txn>, IsarObject<'txn>)> {
        let properties = self.distinct.clone();
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
    ) -> Result<()>
    where
        F: FnMut(&ObjectId<'txn>, IsarObject<'txn>) -> bool,
    {
        let skip_sorting = self.offset_limit.is_none() && self.distinct.is_empty();
        txn.write(|cursors, change_set| {
            self.find_all_internal(cursors, skip_sorting, |cursors, oid, object| {
                if !callback(&oid, object) {
                    return Ok(false);
                }
                collection.delete_current_object_internal(cursors, change_set, &oid, object)?;
                Ok(true)
            })
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::IsarInstance;
    use crate::query::filter::{IntBetweenCond, NotCond, OrCond};
    use crate::{col, ind, isar, set};
    use std::sync::Arc;

    fn fill_int_col(data: Vec<i32>, unique: bool) -> Arc<IsarInstance> {
        isar!(isar, col => col!(oid => DataType::Int, field => DataType::Int; ind!(field; unique)));
        let mut txn = isar.begin_txn(true).unwrap();
        for (i, int) in data.iter().enumerate() {
            let mut o = col.new_object_builder(None);
            o.write_int(i as i32 + 1);
            o.write_int(*int);
            col.put(&mut txn, o.finish()).unwrap();
        }
        txn.commit().unwrap();
        isar
    }

    fn find(txn: &mut IsarTxn, query: Query) -> Vec<(i32, i32)> {
        query
            .find_all_vec(txn)
            .unwrap()
            .iter()
            .map(|(oid, obj)| {
                (
                    oid.get_int().unwrap(),
                    obj.read_int(Property {
                        offset: 6,
                        data_type: DataType::Int,
                    }),
                )
            })
            .collect()
    }

    #[test]
    fn test_no_where_clauses() -> Result<()> {
        let isar = fill_int_col(vec![1, 2, 3, 4], true);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false)?;

        let q = col.new_query_builder().build();
        assert_eq!(find(&mut txn, q), vec![(1, 1), (2, 2), (3, 3), (4, 4)]);

        Ok(())
    }

    #[test]
    fn test_single_primary_where_clause() -> Result<()> {
        let isar = fill_int_col(vec![1, 2, 3, 4, 5], true);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false)?;

        let mut wc = col.new_primary_where_clause();
        wc.add_int(2, 4);
        let mut qb = col.new_query_builder();
        qb.add_where_clause(wc, true, true)?;
        assert_eq!(find(&mut txn, qb.build()), vec![(2, 2), (3, 3), (4, 4)]);

        Ok(())
    }

    #[test]
    fn test_single_secondary_where_clause() -> Result<()> {
        let isar = fill_int_col(vec![1, 2, 3, 4], true);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false)?;

        let mut wc = col.new_secondary_where_clause(0, false).unwrap();
        wc.add_int(2, 3);
        let mut qb = col.new_query_builder();
        qb.add_where_clause(wc, true, true)?;
        assert_eq!(find(&mut txn, qb.build()), vec![(2, 2), (3, 3)]);

        Ok(())
    }

    #[test]
    fn test_single_secondary_where_clause_dup() -> Result<()> {
        let isar = fill_int_col(vec![1, 2, 2, 3, 3, 3, 4], false);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false)?;

        let mut wc = col.new_secondary_where_clause(0, false).unwrap();
        wc.add_int(2, 3);
        let mut qb = col.new_query_builder();
        qb.add_where_clause(wc, true, true)?;
        assert_eq!(
            find(&mut txn, qb.build()),
            vec![(2, 2), (3, 2), (4, 3), (5, 3), (6, 3)]
        );

        let mut wc = col.new_secondary_where_clause(0, true).unwrap();
        wc.add_int(2, 4);
        let mut qb = col.new_query_builder();
        qb.add_where_clause(wc, true, true)?;
        assert_eq!(find(&mut txn, qb.build()), vec![(2, 2), (4, 3), (7, 4)]);

        Ok(())
    }

    #[test]
    fn test_multiple_where_clauses() -> Result<()> {
        let isar = fill_int_col(vec![1, 2, 2, 3, 3, 3, 4], false);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false)?;

        let mut primary_wc = col.new_primary_where_clause();
        primary_wc.add_int(1, 1);

        let mut primary_wc2 = col.new_primary_where_clause();
        primary_wc2.add_int(5, 9);

        let mut secondary_dup_wc = col.new_secondary_where_clause(0, false).unwrap();
        secondary_dup_wc.add_int(3, 5);

        let mut qb = col.new_query_builder();
        qb.add_where_clause(primary_wc, true, true)?;
        qb.add_where_clause(primary_wc2, true, true)?;
        qb.add_where_clause(secondary_dup_wc, true, true)?;

        let results = find(&mut txn, qb.build());
        let results_set: HashSet<(i32, i32)> = results.into_iter().collect();
        assert_eq!(results_set, set![(1, 1), (4, 3), (5, 3), (6, 3), (7, 4)]);
        Ok(())
    }

    #[test]
    fn test_filter_unsorted() -> Result<()> {
        let isar = fill_int_col(vec![5, 4, 4, 3, 2, 2, 1], false);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false)?;

        let int_property = col.get_properties().get(1).unwrap().1;
        let mut qb = col.new_query_builder();
        qb.set_filter(OrCond::filter(vec![
            IntBetweenCond::filter(int_property, 2, 3)?,
            NotCond::filter(IntBetweenCond::filter(int_property, 0, 4)?),
        ]));

        assert_eq!(
            find(&mut txn, qb.build()),
            vec![(1, 5), (4, 3), (5, 2), (6, 2)]
        );

        Ok(())
    }

    #[test]
    fn test_filter_sorted() -> Result<()> {
        let isar = fill_int_col(vec![5, 4, 4, 3, 2, 2, 1], false);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false)?;

        let int_property = col.get_properties().get(1).unwrap().1;
        let mut qb = col.new_query_builder();
        qb.set_filter(OrCond::filter(vec![
            IntBetweenCond::filter(int_property, 2, 3)?,
            NotCond::filter(IntBetweenCond::filter(int_property, 0, 4)?),
        ]));
        qb.add_sort(int_property, Sort::Ascending);

        assert_eq!(
            find(&mut txn, qb.build()),
            vec![(5, 2), (6, 2), (4, 3), (1, 5)]
        );

        Ok(())
    }

    #[test]
    fn test_distinct_unsorted() -> Result<()> {
        let isar = fill_int_col(vec![5, 4, 4, 3, 2, 2, 1], false);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false)?;

        let int_property = col.get_properties().get(1).unwrap().1;
        let mut qb = col.new_query_builder();
        qb.add_distinct(int_property);

        assert_eq!(
            find(&mut txn, qb.build()),
            vec![(1, 5), (2, 4), (4, 3), (5, 2), (7, 1)]
        );

        Ok(())
    }

    #[test]
    fn test_distinct_sorted() -> Result<()> {
        let isar = fill_int_col(vec![5, 4, 4, 3, 2, 2, 1], false);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false)?;

        let int_property = col.get_properties().get(1).unwrap().1;
        let mut qb = col.new_query_builder();
        qb.add_distinct(int_property);
        qb.add_sort(int_property, Sort::Ascending);

        assert_eq!(
            find(&mut txn, qb.build()),
            vec![(7, 1), (5, 2), (4, 3), (2, 4), (1, 5)]
        );

        Ok(())
    }
}
