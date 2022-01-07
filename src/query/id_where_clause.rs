use crate::cursor::IsarCursors;
use crate::error::Result;
use crate::id_key::IdKey;
use crate::mdbx::db::Db;
use crate::object::isar_object::IsarObject;
use crate::query::Sort;
use intmap::IntMap;

#[derive(Clone)]
pub(crate) struct IdWhereClause {
    db: Db,
    lower: i64,
    upper: i64,
    sort: Sort,
}

impl IdWhereClause {
    pub(crate) fn new(db: Db, lower: i64, upper: i64, sort: Sort) -> Self {
        IdWhereClause {
            db,
            lower,
            upper,
            sort,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.upper < self.lower
    }

    pub(crate) fn id_matches(&self, oid: i64) -> bool {
        self.lower <= oid && self.upper >= oid
    }

    pub(crate) fn iter<'txn, 'env, F>(
        &self,
        cursors: &IsarCursors<'txn, 'env>,
        mut result_ids: Option<&mut IntMap<()>>,
        mut callback: F,
    ) -> Result<bool>
    where
        F: FnMut(IdKey<'txn>, IsarObject<'txn>) -> Result<bool>,
    {
        let lower_key = IdKey::new(self.lower);
        let upper_key = IdKey::new(self.upper);
        let mut cursor = cursors.get_cursor(self.db)?;
        cursor.iter_between(
            lower_key.as_bytes(),
            upper_key.as_bytes(),
            false,
            false,
            self.sort == Sort::Ascending,
            |_, id_key, object| {
                let id_key = IdKey::from_bytes(id_key);
                if let Some(result_ids) = result_ids.as_deref_mut() {
                    if !result_ids.insert(id_key.get_unsigned_id(), ()) {
                        return Ok(true);
                    }
                }
                let object = IsarObject::from_bytes(object);
                callback(id_key, object)
            },
        )
    }

    pub(crate) fn is_overlapping(&self, other: &Self) -> bool {
        (self.lower <= other.lower && self.upper >= other.upper)
            || (other.lower <= self.lower && other.upper >= self.upper)
    }
}
