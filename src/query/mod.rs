use crate::error::Result;
use crate::object::isar_object::{IsarObject, Property};
use crate::query::filter::{Condition, Filter, FilterCursors, StaticCond};
use crate::query::where_clause::{IdWhereClause, IndexWhereClause};
use crate::query::where_executor::WhereExecutor;
use crate::txn::{Cursors, IsarTxn};
use hashbrown::HashSet;
use std::cmp::Ordering;
use std::hash::Hasher;
use wyhash::WyHash;

mod fast_wild_match;
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
    id_where_clauses: Vec<IdWhereClause>,
    index_where_clauses: Vec<IndexWhereClause>,
    where_clauses_overlapping: bool,
    filter: Option<Filter>,
    sort: Vec<(Property, Sort)>,
    distinct: Vec<Property>,
    offset_limit: Option<(usize, usize)>,
}

impl<'txn> Query {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id_where_clauses: Vec<IdWhereClause>,
        index_where_clauses: Vec<IndexWhereClause>,
        filter: Option<Filter>,
        sort: Vec<(Property, Sort)>,
        distinct: Vec<Property>,
        offset_limit: Option<(usize, usize)>,
    ) -> Self {
        Query {
            id_where_clauses,
            index_where_clauses,
            where_clauses_overlapping: true,
            filter,
            sort,
            distinct,
            offset_limit,
        }
    }

    pub(crate) fn execute_raw<F>(&self, cursors: &mut Cursors<'txn>, mut callback: F) -> Result<()>
    where
        F: FnMut(IsarObject<'txn>) -> Result<bool>,
    {
        let mut executor = WhereExecutor::new(
            &self.id_where_clauses,
            &self.index_where_clauses,
            self.where_clauses_overlapping,
        );

        let static_filter = StaticCond::filter(true);
        let filter = self.filter.as_ref().unwrap_or(&static_filter);
        executor.execute(cursors, |cursors, object| {
            let mut filter_cursors = FilterCursors::new(&mut cursors.primary2, &mut cursors.links);
            if filter.evaluate(object, Some(&mut filter_cursors))? {
                callback(object)
            } else {
                Ok(true)
            }
        })
    }

    pub(crate) fn get_linked_collections(&self) -> Option<HashSet<u16>> {
        if let Some(filter) = &self.filter {
            let mut col_ids = HashSet::<u16>::new();
            filter.get_linked_collections(&mut col_ids);
            Some(col_ids)
        } else {
            None
        }
    }

    fn execute_unsorted<F>(&self, cursors: &mut Cursors<'txn>, callback: F) -> Result<()>
    where
        F: FnMut(IsarObject<'txn>) -> Result<bool>,
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
    ) -> impl FnMut(IsarObject<'txn>) -> Result<bool>
    where
        F: FnMut(IsarObject<'txn>) -> Result<bool>,
    {
        let properties = self.distinct.clone();
        let mut hashes = HashSet::new();
        move |object| {
            let mut hasher = WyHash::default();
            for property in &properties {
                object.hash_property(*property, &mut hasher);
            }
            let hash = hasher.finish();
            if hashes.insert(hash) {
                callback(object)
            } else {
                Ok(true)
            }
        }
    }

    fn add_offset_limit_unsorted<F>(
        &self,
        mut callback: F,
    ) -> impl FnMut(IsarObject<'txn>) -> Result<bool>
    where
        F: FnMut(IsarObject<'txn>) -> Result<bool>,
    {
        let (offset, limit) = self.offset_limit.unwrap_or((0, usize::MAX));
        let mut count = 0;
        move |value| {
            let result = if count >= offset {
                callback(value)?
            } else {
                true
            };
            count += 1;
            let next = result && limit.saturating_add(offset) > count;
            Ok(next)
        }
    }

    fn execute_sorted(&self, cursors: &mut Cursors<'txn>) -> Result<Vec<IsarObject<'txn>>> {
        let mut results = vec![];
        self.execute_raw(cursors, |object| {
            results.push(object);
            Ok(true)
        })?;

        results.sort_unstable_by(|o1, o2| {
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

    fn add_distinct_sorted(&self, results: Vec<IsarObject<'txn>>) -> Vec<IsarObject<'txn>> {
        let properties = self.distinct.clone();
        let mut hashes = HashSet::new();
        results
            .into_iter()
            .filter(|object| {
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
        results: Vec<IsarObject<'txn>>,
    ) -> impl IntoIterator<Item = IsarObject<'txn>> {
        let (offset, limit) = self.offset_limit.unwrap_or((0, usize::MAX));
        results.into_iter().skip(offset).take(limit)
    }

    pub(crate) fn matches_wc_filter(&self, oid: i64, object: IsarObject) -> bool {
        let wc_matches_id = self.id_where_clauses.iter().any(|wc| wc.id_matches(oid));
        let wc_matches_index = self
            .index_where_clauses
            .iter()
            .any(|wc| wc.object_matches(object));
        if !wc_matches_id && !wc_matches_index {
            return false;
        }

        if let Some(filter) = &self.filter {
            filter.evaluate(object, None).unwrap_or(true)
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
        F: FnMut(IsarObject<'txn>) -> Result<bool>,
    {
        if self.sort.is_empty() || skip_sorting {
            self.execute_unsorted(cursors, callback)?;
        } else {
            let results = self.execute_sorted(cursors)?;
            let results_iter = self.add_offset_limit_sorted(results);
            for object in results_iter {
                if !callback(object)? {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn find_while<F>(&self, txn: &mut IsarTxn<'txn>, mut callback: F) -> Result<()>
    where
        F: FnMut(IsarObject<'txn>) -> bool,
    {
        txn.read(|cursors| self.find_all_internal(cursors, false, |object| Ok(callback(object))))
    }

    pub fn find_all_vec(&self, txn: &mut IsarTxn<'txn>) -> Result<Vec<IsarObject<'txn>>> {
        let mut results = vec![];
        self.find_while(txn, |object| {
            results.push(object);
            true
        })?;
        Ok(results)
    }

    pub fn count(&self, txn: &mut IsarTxn) -> Result<u32> {
        let mut counter = 0;
        self.find_while(txn, |_| {
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
    use crate::object::data_type::DataType;
    use crate::query::filter::{IntBetweenCond, NotCond, OrCond};
    use crate::{col, ind, isar, set};
    use std::sync::Arc;

    fn fill_int_col(data: Vec<i32>, unique: bool) -> Arc<IsarInstance> {
        isar!(isar, col => col!(oid => DataType::Long, field => DataType::Int; ind!(field; unique, false)));
        let mut txn = isar.begin_txn(true, false).unwrap();
        for (i, int) in data.iter().enumerate() {
            let mut o = col.new_object_builder(None);
            o.write_long(i as i64 + 1);
            o.write_int(*int);
            col.put(&mut txn, o.finish()).unwrap();
        }
        txn.commit().unwrap();
        isar
    }

    fn find(txn: &mut IsarTxn, query: Query) -> Vec<(i64, i32)> {
        query
            .find_all_vec(txn)
            .unwrap()
            .iter()
            .map(|obj| {
                (
                    obj.read_long(Property {
                        offset: 2,
                        data_type: DataType::Long,
                    }),
                    obj.read_int(Property {
                        offset: 10,
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
        let mut txn = isar.begin_txn(false, false)?;

        let q = col.new_query_builder().build();
        assert_eq!(find(&mut txn, q), vec![(1, 1), (2, 2), (3, 3), (4, 4)]);

        Ok(())
    }

    #[test]
    fn test_single_primary_where_clause() -> Result<()> {
        let isar = fill_int_col(vec![1, 2, 3, 4, 5], true);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false, false)?;

        let wc = col
            .new_id_where_clause(Some(2), Some(4), Sort::Ascending)
            .unwrap();
        let mut qb = col.new_query_builder();
        qb.add_id_where_clause(wc)?;
        assert_eq!(find(&mut txn, qb.build()), vec![(2, 2), (3, 3), (4, 4)]);

        Ok(())
    }

    #[test]
    fn test_single_secondary_where_clause() -> Result<()> {
        let isar = fill_int_col(vec![1, 2, 3, 4], true);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false, false)?;

        let mut wc = col
            .new_index_where_clause(0, false, Sort::Ascending)
            .unwrap();
        wc.add_int(2, 3);
        let mut qb = col.new_query_builder();
        qb.add_index_where_clause(wc, true, true)?;
        assert_eq!(find(&mut txn, qb.build()), vec![(2, 2), (3, 3)]);

        Ok(())
    }

    #[test]
    fn test_single_secondary_where_clause_dup() -> Result<()> {
        let isar = fill_int_col(vec![1, 2, 2, 3, 3, 3, 4], false);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false, false)?;

        let mut wc = col
            .new_index_where_clause(0, false, Sort::Ascending)
            .unwrap();
        wc.add_int(2, 3);
        let mut qb = col.new_query_builder();
        qb.add_index_where_clause(wc, true, true)?;
        assert_eq!(
            find(&mut txn, qb.build()),
            vec![(2, 2), (3, 2), (4, 3), (5, 3), (6, 3)]
        );

        let mut wc = col
            .new_index_where_clause(0, true, Sort::Ascending)
            .unwrap();
        wc.add_int(2, 4);
        let mut qb = col.new_query_builder();
        qb.add_index_where_clause(wc, true, true)?;
        assert_eq!(find(&mut txn, qb.build()), vec![(2, 2), (4, 3), (7, 4)]);

        Ok(())
    }

    #[test]
    fn test_multiple_where_clauses() -> Result<()> {
        let isar = fill_int_col(vec![1, 2, 2, 3, 3, 3, 4], false);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false, false)?;

        let primary_wc = col.new_id_where_clause(Some(1), Some(1), Sort::Ascending)?;
        let primary_wc2 = col.new_id_where_clause(Some(5), Some(9), Sort::Ascending)?;

        let mut secondary_dup_wc = col
            .new_index_where_clause(0, false, Sort::Ascending)
            .unwrap();
        secondary_dup_wc.add_int(3, 5);

        let mut qb = col.new_query_builder();
        qb.add_id_where_clause(primary_wc)?;
        qb.add_id_where_clause(primary_wc2)?;
        qb.add_index_where_clause(secondary_dup_wc, true, true)?;

        let results = find(&mut txn, qb.build());
        let results_set: HashSet<(i64, i32)> = results.into_iter().collect();
        assert_eq!(results_set, set![(1, 1), (4, 3), (5, 3), (6, 3), (7, 4)]);
        Ok(())
    }

    #[test]
    fn test_filter_unsorted() -> Result<()> {
        let isar = fill_int_col(vec![5, 4, 4, 3, 2, 2, 1], false);
        let col = isar.get_collection(0).unwrap();
        let mut txn = isar.begin_txn(false, false)?;

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
        let mut txn = isar.begin_txn(false, false)?;

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
        let mut txn = isar.begin_txn(false, false)?;

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
        let mut txn = isar.begin_txn(false, false)?;

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
