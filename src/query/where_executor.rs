use crate::error::{IsarError, Result};
use crate::lmdb::cursor::Cursor;
use crate::option;
use crate::query::where_clause::WhereClause;
use crate::txn::Cursors;
use hashbrown::HashSet;

pub(super) struct WhereExecutor<'a> {
    where_clauses: &'a [WhereClause],
    where_clauses_overlapping: bool,
}

impl<'a> WhereExecutor<'a> {
    pub fn new(where_clauses: &'a [WhereClause], where_clauses_overlapping: bool) -> Self {
        WhereExecutor {
            where_clauses,
            where_clauses_overlapping,
        }
    }

    pub fn execute<'txn, F>(&mut self, cursors: &mut Cursors<'txn>, mut callback: F) -> Result<()>
    where
        F: FnMut(&mut Cursor<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>,
    {
        let mut hash_set = HashSet::new();
        let mut result_ids = option!(self.where_clauses_overlapping, &mut hash_set);
        for where_clause in self.where_clauses {
            let result = if where_clause.is_primary() {
                self.execute_primary_where_clause(
                    where_clause,
                    cursors,
                    &mut result_ids,
                    &mut callback,
                )?
            } else {
                self.execute_secondary_where_clause(
                    where_clause,
                    cursors,
                    &mut result_ids,
                    &mut callback,
                )?
            };
            if !result {
                return Ok(());
            }
        }
        Ok(())
    }

    fn execute_primary_where_clause<'txn, F>(
        &mut self,
        where_clause: &WhereClause,
        cursors: &mut Cursors<'txn>,
        result_ids: &mut Option<&mut HashSet<&'txn [u8]>>,
        callback: &mut F,
    ) -> Result<bool>
    where
        F: FnMut(&mut Cursor<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>,
    {
        where_clause.iter(&mut cursors.primary, |cursor, key, val| {
            if let Some(result_ids) = result_ids {
                if !result_ids.insert(key) {
                    return Ok(true);
                }
            }
            callback(cursor, key, val)
        })
    }

    fn execute_secondary_where_clause<'txn, F>(
        &mut self,
        where_clause: &WhereClause,
        cursors: &mut Cursors<'txn>,
        result_ids: &mut Option<&mut HashSet<&'txn [u8]>>,
        callback: &mut F,
    ) -> Result<bool>
    where
        F: FnMut(&mut Cursor<'txn>, &'txn [u8], &'txn [u8]) -> Result<bool>,
    {
        let primary = &mut cursors.primary;
        let secondary = &mut cursors.secondary;
        let secondary_dup = &mut cursors.secondary_dup;
        let cursor = if where_clause.is_unique() {
            secondary
        } else {
            secondary_dup
        };
        where_clause.iter(cursor, |_, _, oid| {
            if let Some(result_ids) = result_ids {
                if !result_ids.insert(oid) {
                    return Ok(true);
                }
            }
            let entry = primary.move_to(oid)?;
            if let Some((_, val)) = entry {
                if !callback(primary, oid, val)? {
                    return Ok(false);
                }
            } else {
                return Err(IsarError::DbCorrupted {
                    source: None,
                    message: "Could not find object specified in index.".to_string(),
                });
            }
            Ok(true)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::IsarInstance;
    use crate::object::object_id::ObjectId;
    use crate::utils::debug::fill_db;
    use crate::*;
    use std::sync::Arc;

    fn execute_where_clauses(
        isar: &IsarInstance,
        wc: &[WhereClause],
        overlapping: bool,
    ) -> Vec<u32> {
        let mut txn = isar.begin_txn(false).unwrap();
        let mut executer = WhereExecutor::new(wc, overlapping);
        let mut entries = vec![];
        txn.read(|cursors| {
            executer.execute(cursors, |_, oid, _| {
                entries.push(ObjectId::from_bytes(oid).get_time());
                Ok(true)
            })
        })
        .unwrap();

        entries
    }

    fn get_test_db() -> Arc<IsarInstance> {
        isar!(isar, col => col!(f1 => Int, f2=> Int, f3 => String; ind!(f1, f3), ind!(f2; true)));
        let mut txn = isar.begin_txn(true).unwrap();

        let build_value = |field1: i32, field2: i32, field3: &str| {
            let mut builder = col.new_object_builder(None);
            builder.write_int(field1);
            builder.write_int(field2);
            builder.write_string(Some(field3));
            builder.finish()
        };

        let oid = |time: u32| Some(ObjectId::new(time, 0, 0));

        let data = vec![
            (oid(1), build_value(1, 1, "aaa")),
            (oid(2), build_value(1, 2, "abb")),
            (oid(3), build_value(2, 3, "abb")),
            (oid(4), build_value(2, 4, "bbb")),
            (oid(5), build_value(3, 5, "bbb")),
            (oid(6), build_value(3, 6, "bcc")),
        ];
        fill_db(col, &mut txn, &data);
        txn.commit().unwrap();

        isar
    }

    #[test]
    fn test_run_single_primary_where_clause() {
        let isar = get_test_db();
        let col = isar.get_collection(0).unwrap();

        let mut wc = col.new_primary_where_clause();
        wc.add_oid_time(4, u32::MAX);
        assert_eq!(execute_where_clauses(&isar, &[wc], false), vec![4, 5, 6]);

        let mut wc = col.new_primary_where_clause();
        wc.add_oid_time(4, 4);
        assert_eq!(execute_where_clauses(&isar, &[wc], false), vec![4]);

        let mut wc = col.new_secondary_where_clause(0, false).unwrap();
        wc.add_oid_time(u32::MAX, u32::MAX);
        assert_eq!(
            execute_where_clauses(&isar, &[wc], false),
            Vec::<u32>::new()
        );
    }

    #[test]
    fn test_run_single_secondary_where_clause() {
        let isar = get_test_db();
        let col = isar.get_collection(0).unwrap();

        let mut wc = col.new_secondary_where_clause(0, false).unwrap();
        wc.add_int(2, i32::MAX);
        assert_eq!(
            execute_where_clauses(&isar, &[wc.clone()], false),
            vec![3, 4, 5, 6]
        );

        let mut wc = col.new_secondary_where_clause(0, false).unwrap();
        wc.add_int(2, 2);
        assert_eq!(execute_where_clauses(&isar, &[wc], false), vec![3, 4]);

        let mut wc = col.new_secondary_where_clause(0, false).unwrap();
        wc.add_int(50, i32::MAX);
        assert_eq!(
            execute_where_clauses(&isar, &[wc], false),
            Vec::<u32>::new()
        );
    }

    #[test]
    fn test_run_single_secondary_where_clause_unique() {
        let isar = get_test_db();
        let col = isar.get_collection(0).unwrap();

        let mut wc = col.new_secondary_where_clause(1, false).unwrap();
        wc.add_int(4, i32::MAX);
        assert_eq!(
            execute_where_clauses(&isar, &[wc.clone()], false),
            vec![4, 5, 6]
        );

        let mut wc = col.new_secondary_where_clause(1, false).unwrap();
        wc.add_int(4, 5);
        assert_eq!(execute_where_clauses(&isar, &[wc], false), vec![4, 5]);

        let mut wc = col.new_secondary_where_clause(0, false).unwrap();
        wc.add_int(50, i32::MAX);
        assert_eq!(
            execute_where_clauses(&isar, &[wc], false),
            Vec::<u32>::new()
        );
    }

    #[test]
    fn test_run_single_secondary_compound_where_clause() {
        let isar = get_test_db();
        let col = isar.get_collection(0).unwrap();

        let mut wc = col.new_secondary_where_clause(0, false).unwrap();
        wc.add_int(2, i32::MAX);
        assert_eq!(
            execute_where_clauses(&isar, &[wc.clone()], false),
            vec![3, 4, 5, 6]
        );

        //wc.add_int(4, 5);
        //assert_eq!(execute_where_clauses(&isar, &[wc], false), vec![4, 5]);
    }

    #[test]
    fn test_run_non_overlapping_where_clauses() {
        let isar = get_test_db();
        let col = isar.get_collection(0).unwrap();

        let mut wc1 = col.new_secondary_where_clause(0, false).unwrap();
        wc1.add_int(1, 1);

        let mut wc2 = col.new_secondary_where_clause(0, false).unwrap();
        wc2.add_int(3, 3);
        assert_eq!(
            execute_where_clauses(&isar, &[wc1, wc2], false),
            vec![1, 2, 5, 6]
        );
    }

    #[test]
    fn test_run_overlapping_where_clauses() {
        let isar = get_test_db();
        let col = isar.get_collection(0).unwrap();

        let mut wc1 = col.new_secondary_where_clause(0, false).unwrap();
        wc1.add_int(2, i32::MAX);

        let mut wc2 = col.new_secondary_where_clause(0, false).unwrap();
        wc2.add_int(2, 3);

        let mut result = execute_where_clauses(&isar, &[wc1.clone(), wc2, wc1], true);
        result.sort_unstable();
        assert_eq!(result, vec![3, 4, 5, 6]);
    }
}
