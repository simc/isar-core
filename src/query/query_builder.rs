use crate::error::{illegal_arg, Result};
use crate::lmdb::{MAX_ID, MIN_ID};
use crate::object::isar_object::Property;
use crate::query::filter::Filter;
use crate::query::id_where_clause::IdWhereClause;
use crate::query::where_clause::WhereClause;
use crate::query::{Query, Sort};
use crate::{collection::IsarCollection, index::index_key::IndexKey};
use itertools::Itertools;

use super::index_where_clause::IndexWhereClause;

pub struct QueryBuilder<'a> {
    collection: &'a IsarCollection,
    where_clauses: Option<Vec<WhereClause>>,
    filter: Option<Filter>,
    sort: Vec<(Property, Sort)>,
    distinct: Vec<(Property, bool)>,
    offset: usize,
    limit: usize,
}

impl<'a> QueryBuilder<'a> {
    pub(crate) fn new(collection: &'a IsarCollection) -> QueryBuilder {
        QueryBuilder {
            collection,
            where_clauses: None,
            filter: None,
            sort: vec![],
            distinct: vec![],
            offset: 0,
            limit: usize::MAX,
        }
    }

    pub fn add_id_where_clause(&mut self, lower_id: i64, upper_id: i64, sort: Sort) -> Result<()> {
        if self.where_clauses.is_none() {
            self.where_clauses = Some(vec![]);
        }
        let wc = IdWhereClause::new(self.collection, lower_id, upper_id, sort);
        if !wc.is_empty() {
            self.where_clauses
                .as_mut()
                .unwrap()
                .push(WhereClause::Id(wc))
        }
        Ok(())
    }

    pub fn add_index_where_clause(
        &mut self,
        lower_key: IndexKey,
        include_lower: bool,
        upper_key: IndexKey,
        include_upper: bool,
        skip_duplicates: bool,
        sort: Sort,
    ) -> Result<()> {
        if lower_key.index.get_col_id() != self.collection.get_id() {
            return illegal_arg("Invalid IndexKey for this collection");
        }
        let mut wc = IndexWhereClause::new(lower_key, upper_key, skip_duplicates, sort)?;
        if self.where_clauses.is_none() {
            self.where_clauses = Some(vec![]);
        }
        if wc.try_exclude(include_lower, include_upper) && !wc.is_empty() {
            self.where_clauses
                .as_mut()
                .unwrap()
                .push(WhereClause::Index(wc));
        }
        Ok(())
    }

    pub fn set_filter(&mut self, filter: Filter) {
        self.filter = Some(filter);
    }

    pub fn add_sort(&mut self, property: Property, sort: Sort) {
        self.sort.push((property, sort))
    }

    pub fn add_distinct(&mut self, property: Property, case_sensitive: bool) {
        self.distinct.push((property, case_sensitive));
    }

    pub fn set_offset(&mut self, offset: usize) {
        self.offset = offset;
    }

    pub fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
    }

    pub fn build(mut self) -> Query {
        if self.where_clauses.is_none() {
            self.add_id_where_clause(MIN_ID, MAX_ID, Sort::Ascending)
                .unwrap();
        }
        let sort_unique = self.sort.into_iter().unique_by(|(p, _)| p.offset).collect();
        let distinct_unique = self
            .distinct
            .into_iter()
            .unique_by(|(p, _)| p.offset)
            .collect();
        Query::new(
            self.where_clauses.unwrap(),
            self.filter,
            sort_unique,
            distinct_unique,
            self.offset,
            self.limit,
        )
    }
}
