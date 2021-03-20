use crate::error::Result;
use crate::object::isar_object::IsarObject;
use crate::query::filter::FilterCursors;
use crate::query::id_where_clause::IdWhereClause;
use crate::query::index_where_clause::IndexWhereClause;
use crate::txn::Cursors;
use hashbrown::HashSet;

#[derive(Clone)]
pub(crate) enum WhereClause {
    Id(IdWhereClause),
    Index(IndexWhereClause),
}

impl WhereClause {
    pub fn matches(&self, id: i64, object: IsarObject) -> bool {
        match self {
            WhereClause::Id(wc) => wc.id_matches(id),
            WhereClause::Index(wc) => wc.object_matches(object),
        }
    }

    pub fn iter<'txn, 'a, F>(
        &self,
        cursors: &'a mut Cursors<'txn>,
        result_ids: Option<&mut HashSet<i64>>,
        mut callback: F,
    ) -> Result<bool>
    where
        F: FnMut(&mut FilterCursors<'txn, 'a>, IsarObject<'txn>) -> Result<bool>,
    {
        let mut filter_cursors = FilterCursors::new(&mut cursors.data2, &mut cursors.links);
        match self {
            WhereClause::Id(wc) => wc.iter(&mut cursors.data, result_ids, |_, _, o| {
                callback(&mut filter_cursors, o)
            }),
            WhereClause::Index(wc) => wc.iter(
                &mut cursors.data,
                &mut cursors.index,
                result_ids,
                |_, _, o| callback(&mut filter_cursors, o),
            ),
        }
    }
}
