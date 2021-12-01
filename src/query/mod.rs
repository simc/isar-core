use std::cmp::Ordering;
use std::hash::Hasher;

use hashbrown::HashSet;
use serde_json::{json, Value};
use wyhash::WyHash;

use crate::collection::IsarCollection;
use crate::error::Result;
use crate::object::isar_object::{IsarObject, Property};
use crate::object::json_encode_decode::JsonEncodeDecode;
use crate::query::filter::Filter;
use crate::query::where_clause::WhereClause;
use crate::txn::{Cursors, IsarTxn};

mod fast_wild_match;
pub mod filter;
pub mod id_where_clause;
pub mod index_where_clause;
pub mod query_builder;
mod where_clause;

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
    distinct: Vec<(Property, bool)>,
    offset: usize,
    limit: usize,
}

impl<'txn> Query {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        where_clauses: Vec<WhereClause>,
        filter: Option<Filter>,
        sort: Vec<(Property, Sort)>,
        distinct: Vec<(Property, bool)>,
        offset: usize,
        limit: usize,
    ) -> Self {
        let where_clauses_overlapping = Self::check_where_clauses_overlapping(&where_clauses);
        Query {
            where_clauses,
            where_clauses_overlapping,
            filter,
            sort,
            distinct,
            offset,
            limit,
        }
    }

    fn check_where_clauses_overlapping(where_clauses: &[WhereClause]) -> bool {
        for (i, wc1) in where_clauses.iter().enumerate() {
            for wc2 in where_clauses.iter().skip(i + 1) {
                if wc1.is_overlapping(wc2) {
                    return true;
                }
            }
        }
        false
    }

    pub(crate) fn execute_raw<F>(&self, cursors: &mut Cursors<'txn>, mut callback: F) -> Result<()>
    where
        F: FnMut(IsarObject<'txn>) -> Result<bool>,
    {
        let mut result_ids = if self.where_clauses_overlapping {
            Some(HashSet::<i64>::new())
        } else {
            None
        };

        let static_filter = Filter::stat(true);
        let filter = self.filter.as_ref().unwrap_or(&static_filter);

        for where_clause in &self.where_clauses {
            let result =
                where_clause.iter(cursors, result_ids.as_mut(), |filter_cursors, object| {
                    if filter.evaluate(object, Some(filter_cursors))? {
                        callback(object)
                    } else {
                        Ok(true)
                    }
                })?;
            if !result {
                return Ok(());
            }
        }

        Ok(())
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
            for (property, case_sensitive) in &properties {
                object.hash_property(*property, *case_sensitive, &mut hasher);
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
        let offset = self.offset;
        let max_count = self.limit.saturating_add(offset);
        let mut count = 0;
        move |value| {
            count += 1;
            if count > max_count || (count > offset && !callback(value)?) {
                Ok(false)
            } else {
                Ok(true)
            }
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
                for (property, case_sensitive) in &properties {
                    object.hash_property(*property, *case_sensitive, &mut hasher);
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
        results.into_iter().skip(self.offset).take(self.limit)
    }

    pub(crate) fn matches_wc_filter(&self, id: i64, object: IsarObject) -> bool {
        let wc_matches = self.where_clauses.iter().any(|wc| wc.matches(id, object));
        if !wc_matches {
            return false;
        }

        if let Some(filter) = &self.filter {
            filter.evaluate(object, None).unwrap_or(true)
        } else {
            true
        }
    }

    pub(crate) fn find_while_internal<F>(
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
        txn.read(|cursors| self.find_while_internal(cursors, false, |object| Ok(callback(object))))
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

    pub fn export_json(
        &self,
        txn: &mut IsarTxn,
        collection: &IsarCollection,
        primitive_null: bool,
        byte_as_bool: bool,
    ) -> Result<Value> {
        let mut items = vec![];
        self.find_while(txn, |object| {
            let json = JsonEncodeDecode::encode(collection, object, primitive_null, byte_as_bool);
            items.push(json);
            true
        })?;
        Ok(json!(items))
    }
}
