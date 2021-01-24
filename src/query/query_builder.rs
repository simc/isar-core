use crate::collection::IsarCollection;
use crate::error::{illegal_arg, Result};
use crate::object::property::Property;
use crate::query::filter::Filter;
use crate::query::query::{Query, Sort};
use crate::query::where_clause::WhereClause;
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
        /*if let Some(index) = &wc.index {
            if !self.collection.get_indexes().contains(index) {
                return illegal_arg("Wrong WhereClause for this collection.");
            }
        } else if self.collection.get_id() != wc.get_prefix() {
            return illegal_arg("Wrong WhereClause for this collection.");
        }*/
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

    /*pub fn merge_where_clauses(mut where_clauses: Vec<WhereClause>) -> Vec<WhereClause> {
        where_clauses.sort_unstable_by(|a, b| a.lower_key.cmp(&b.lower_key));

        let mut merged = vec![];
        let mut i = 0;
        while i < where_clauses.len() {
            let a = where_clauses.get(i).unwrap();
            let mut new_upper_key = None;
            loop {
                if let Some(b) = where_clauses.get(i + 1) {
                    if b.lower_key <= a.upper_key {
                        new_upper_key = Some(max(&a.upper_key, &b.upper_key));
                        i += 1;
                        continue;
                    }
                }
                break;
            }
            if let Some(new_upper_key) = new_upper_key {
                merged.push(WhereClause {
                    lower_key: a.lower_key.clone(),
                    upper_key: new_upper_key.clone(),
                    index_type: a.index_type,
                });
                i += 2;
            } else {
                merged.push(a.deref().clone());
                i += 1;
            }
        }

        merged
    }*/

    pub fn build(mut self) -> Query {
        if self.where_clauses.is_empty() {
            self.where_clauses
                .push(self.collection.new_primary_where_clause())
        }
        Query::new(
            self.where_clauses,
            self.filter,
            self.sort,
            self.distinct,
            self.offset_limit,
        )
    }
}
