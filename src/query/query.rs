use crate::collection::IsarCollection;
use crate::error::Result;
use crate::lmdb::cursor::Cursor;
use crate::object::object_id::ObjectId;
use crate::object::property::Property;
use crate::query::filter::*;
use crate::query::where_clause::WhereClause;
use crate::query::where_executor::WhereExecutor;
use crate::txn::{Cursors, IsarTxn};
use hashbrown::HashSet;
use std::cmp::Ordering;
use std::hash::Hasher;
use wyhash::WyHash;

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
        where_clauses: Vec<WhereClause>,
        filter: Option<Filter>,
        sort: Vec<(Property, Sort)>,
        distinct: Option<Vec<Property>>,
        offset_limit: Option<(usize, usize)>,
    ) -> Self {
        Query {
            where_clauses,
            where_clauses_overlapping: true,
            filter,
            sort,
            distinct,
            offset_limit,
        }
    }

    fn execute_raw<F>(&self, cursors: &mut Cursors<'txn>, mut callback: F) -> Result<()>
    where
        F: FnMut(&mut Cursor<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>,
    {
        let mut executor = WhereExecutor::new(&self.where_clauses, self.where_clauses_overlapping);
        if let Some(filter) = &self.filter {
            executor.execute(cursors, |cursor, key, val| {
                if filter.evaluate(val) {
                    callback(cursor, key, val)
                } else {
                    Ok(true)
                }
            })
        } else {
            executor.execute(cursors, callback)
        }
    }

    fn execute_unsorted<F>(&self, cursors: &mut Cursors<'txn>, callback: F) -> Result<()>
    where
        F: FnMut(&mut Cursor<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>,
    {
        if self.distinct.is_some() {
            let callback = self.add_distinct_unsorted(callback);
            if self.offset_limit.is_some() {
                let callback = self.add_offset_limit_unsorted(callback);
                self.execute_raw(cursors, callback)
            } else {
                self.execute_raw(cursors, callback)
            }
        } else if self.offset_limit.is_some() {
            let callback = self.add_offset_limit_unsorted(callback);
            self.execute_raw(cursors, callback)
        } else {
            self.execute_raw(cursors, callback)
        }
    }

    fn add_distinct_unsorted<F>(
        &self,
        mut callback: F,
    ) -> impl FnMut(&mut Cursor<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>
    where
        F: FnMut(&mut Cursor<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>,
    {
        let properties = self.distinct.as_ref().unwrap().clone();
        let mut hashes = HashSet::new();
        move |cursor, key, val| {
            let mut hasher = WyHash::default();
            for property in &properties {
                property.hash_value(val, &mut hasher);
            }
            let hash = hasher.finish();
            if hashes.insert(hash) {
                callback(cursor, key, val)
            } else {
                Ok(true)
            }
        }
    }

    fn add_offset_limit_unsorted<F>(
        &self,
        mut callback: F,
    ) -> impl FnMut(&mut Cursor<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>
    where
        F: FnMut(&mut Cursor<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>,
    {
        let (offset, limit) = self.offset_limit.unwrap();
        let mut count = 0;
        move |cursor, key, value| {
            let result = if count >= offset {
                callback(cursor, key, value)?
            } else {
                true
            };
            count += 1;
            let cont = result && limit.saturating_add(offset) > count;
            Ok(cont)
        }
    }

    fn execute_sorted(&self, cursors: &mut Cursors<'txn>) -> Result<Vec<(&'txn [u8], &'txn [u8])>> {
        let mut results = vec![];
        self.execute_raw(cursors, |_, key, val| {
            results.push((key, val));
            Ok(true)
        })?;

        results.sort_unstable_by(|(_, o1), (_, o2)| {
            for (p, sort) in &self.sort {
                let ord = p.compare(o1, o2);
                if ord != Ordering::Equal {
                    if *sort == Sort::Ascending {
                        return ord;
                    } else {
                        return ord.reverse();
                    }
                }
            }
            Ordering::Equal
        });

        Ok(self.add_distinct_sorted(results))
    }

    fn add_distinct_sorted(
        &self,
        results: Vec<(&'txn [u8], &'txn [u8])>,
    ) -> Vec<(&'txn [u8], &'txn [u8])> {
        let properties = self.distinct.as_ref().unwrap().clone();
        let mut hashes = HashSet::new();
        results
            .into_iter()
            .filter(|(_, val)| {
                let mut hasher = WyHash::default();
                for property in &properties {
                    property.hash_value(val, &mut hasher);
                }
                let hash = hasher.finish();
                hashes.insert(hash)
            })
            .collect()
    }

    fn add_offset_limit_sorted<'a>(
        &self,
        mut results: &'a [(&'txn [u8], &'txn [u8])],
    ) -> &'a [(&'txn [u8], &'txn [u8])] {
        if let Some((offset, limit)) = self.offset_limit {
            if results.len() < offset {
                results = &results[offset..];
            } else {
                results = &[];
            }
            if results.len() >= limit {
                results = &results[0..limit];
            } else {
                results = &[];
            }
        }
        results
    }

    pub(crate) fn matches_wc_filter(&self, oid: ObjectId, object: &[u8]) -> bool {
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
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        if self.sort.is_empty() {
            self.execute_unsorted(cursors, |_, key, val| {
                let oid = ObjectId::from_bytes(key);
                Ok(callback(oid, val))
            })?;
        } else {
            let results = self.execute_sorted(cursors)?;
            let slice = self.add_offset_limit_sorted(&results);
            for (key, val) in slice {
                if !callback(ObjectId::from_bytes(key), val) {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn find_all<F>(&self, txn: &mut IsarTxn<'txn>, callback: F) -> Result<()>
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        txn.read(|cursors| self.find_all_internal(cursors, callback))
    }

    pub fn delete_all(&self, txn: &mut IsarTxn, collection: &IsarCollection) -> Result<usize> {
        let mut delete_cursors = txn.open_cursors()?;
        txn.write(|cursors, change_set| {
            let needs_sorting =
                self.sort.is_empty() || (self.offset_limit.is_none() && self.distinct.is_none());
            if !needs_sorting {
                let mut count = 0;
                self.execute_unsorted(cursors, |cursor, key, object| {
                    let oid = *ObjectId::from_bytes(key);
                    cursor.delete_current()?;
                    collection.delete_object_internal(
                        &mut delete_cursors,
                        change_set,
                        oid,
                        object,
                        false,
                    )?;
                    count += 1;
                    Ok(true)
                })?;
                Ok(count)
            } else {
                let results = self.execute_sorted(cursors)?;
                let slice = self.add_offset_limit_sorted(&results);
                for (key, object) in slice {
                    let oid = *ObjectId::from_bytes(key);
                    collection.delete_object_internal(cursors, change_set, oid, object, true)?;
                }
                Ok(slice.len())
            }
        })
    }

    pub fn find_all_vec(
        &self,
        txn: &mut IsarTxn<'txn>,
    ) -> Result<Vec<(&'txn ObjectId, &'txn [u8])>> {
        let mut results = vec![];
        txn.read(|cursors| {
            self.find_all_internal(cursors, |oid, value| {
                results.push((oid, value));
                true
            })
        })?;
        Ok(results)
    }

    pub fn count(&self, txn: &mut IsarTxn) -> Result<u32> {
        let mut counter = 0;
        txn.read(|cursors| {
            self.find_all_internal(cursors, |_, _| {
                counter += 1;
                true
            })
        })?;
        Ok(counter)
    }
}

#[cfg(test)]
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
}
