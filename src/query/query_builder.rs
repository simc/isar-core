use crate::collection::IsarCollection;
use crate::error::{illegal_arg, Result};
use crate::object::isar_object::Property;
use crate::query::filter::Filter;
use crate::query::where_clause::WhereClause;
use crate::query::{Query, Sort};
use itertools::Itertools;

pub struct QueryBuilder<'a> {
    collection: &'a IsarCollection,
    where_clauses: Vec<WhereClause>,
    filter: Option<Filter>,
    sort: Vec<(Property, Sort)>,
    distinct: Option<Vec<Property>>,
    offset_limit: Option<(usize, usize)>,
}

impl<'a> QueryBuilder<'a> {
    pub(crate) fn new(collection: &'a IsarCollection) -> QueryBuilder {
        QueryBuilder {
            collection,
            where_clauses: vec![],
            filter: None,
            sort: vec![],
            distinct: None,
            offset_limit: None,
        }
    }

    pub fn add_where_clause(
        &mut self,
        mut wc: WhereClause,
        include_lower: bool,
        include_upper: bool,
    ) -> Result<()> {
        if let Some(index) = &wc.index {
            if !self.collection.get_indexes().contains(index) {
                return illegal_arg("Wrong WhereClause for this collection.");
            }
        } else if self.collection.get_id() != wc.get_prefix() {
            return illegal_arg("Wrong WhereClause for this collection.");
        }
        if !wc.try_exclude(include_lower, include_upper) {
            wc = WhereClause::new_empty();
        }
        if self.where_clauses.is_empty() || !wc.is_empty() {
            self.where_clauses.push(wc);
        }
        Ok(())
    }

    pub fn set_filter(&mut self, filter: Filter) {
        self.filter = Some(filter);
    }

    pub fn add_sort(&mut self, property: Property, sort: Sort) {
        self.sort.push((property, sort))
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

    pub fn set_distinct(&mut self, properties: &[Property]) {
        self.distinct = Some(properties.iter().cloned().collect_vec());
    }

    pub fn build(mut self) -> Query {
        if self.where_clauses.is_empty() {
            self.where_clauses
                .push(self.collection.new_primary_where_clause())
        }
        Query::new(
            self.collection.get_oid_type(),
            self.where_clauses,
            self.filter,
            self.sort,
            self.distinct,
            self.offset_limit,
        )
    }
}
