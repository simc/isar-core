use crate::collection::IsarCollection;
use crate::error::{illegal_arg, Result};
use crate::object::isar_object::Property;
use crate::query::filter::Filter;
use crate::query::where_clause::{IdWhereClause, IndexWhereClause};
use crate::query::{Query, Sort};
use itertools::Itertools;

pub struct QueryBuilder<'a> {
    collection: &'a IsarCollection,
    id_where_clauses: Option<Vec<IdWhereClause>>,
    index_where_clauses: Option<Vec<IndexWhereClause>>,
    filter: Option<Filter>,
    sort: Vec<(Property, Sort)>,
    distinct: Vec<Property>,
    offset_limit: Option<(usize, usize)>,
}

impl<'a> QueryBuilder<'a> {
    pub(crate) fn new(collection: &'a IsarCollection) -> QueryBuilder {
        QueryBuilder {
            collection,
            id_where_clauses: None,
            index_where_clauses: None,
            filter: None,
            sort: vec![],
            distinct: vec![],
            offset_limit: None,
        }
    }

    pub fn add_id_where_clause(&mut self, wc: IdWhereClause) -> Result<()> {
        if wc.get_prefix() != self.collection.get_id() {
            return illegal_arg("Wrong WhereClause for this collection.");
        }
        if self.id_where_clauses.is_none() {
            self.id_where_clauses = Some(vec![]);
        }
        if !wc.is_empty() {
            self.id_where_clauses.as_mut().unwrap().push(wc)
        }
        Ok(())
    }

    pub fn add_index_where_clause(
        &mut self,
        mut wc: IndexWhereClause,
        include_lower: bool,
        include_upper: bool,
    ) -> Result<()> {
        if !wc.is_from_collection(self.collection) {
            return illegal_arg("Wrong WhereClause for this collection.");
        }
        if self.index_where_clauses.is_none() {
            self.index_where_clauses = Some(vec![]);
        }
        if wc.try_exclude(include_lower, include_upper) && !wc.is_empty() {
            self.index_where_clauses.as_mut().unwrap().push(wc);
        }
        Ok(())
    }

    pub fn set_filter(&mut self, filter: Filter) {
        self.filter = Some(filter);
    }

    pub fn add_sort(&mut self, property: Property, sort: Sort) {
        self.sort.push((property, sort))
    }

    pub fn add_distinct(&mut self, property: Property) {
        self.distinct.push(property);
    }

    pub fn set_offset_limit(&mut self, offset: Option<usize>, limit: Option<usize>) -> Result<()> {
        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);

        if offset > limit {
            illegal_arg("Offset has to less or equal than limit.")
        } else {
            self.offset_limit = Some((offset, limit));
            Ok(())
        }
    }

    pub fn build(mut self) -> Query {
        if self.id_where_clauses.is_none() && self.index_where_clauses.is_none() {
            let default_wc = self
                .collection
                .new_id_where_clause(None, None, Sort::Ascending)
                .unwrap();
            self.add_id_where_clause(default_wc).unwrap();
        }
        let sort_unique = self.sort.into_iter().unique_by(|(p, _)| p.offset).collect();
        let distinct_unique = self.distinct.into_iter().unique_by(|p| p.offset).collect();
        Query::new(
            self.id_where_clauses.unwrap_or(vec![]),
            self.index_where_clauses.unwrap_or(vec![]),
            self.filter,
            sort_unique,
            distinct_unique,
            self.offset_limit,
        )
    }
}
