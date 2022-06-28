use crate::cursor::IsarCursors;
use crate::error::{IsarError, Result};
use crate::id_key::IdKey;
use crate::mdbx::cursor::Cursor;
use crate::mdbx::db::Db;
use crate::mdbx::{debug_dump_db, Key};
use crate::object::isar_object::IsarObject;
use crate::txn::IsarTxn;
use std::collections::HashSet;

#[derive(Clone)]
pub(crate) struct IsarLink {
    pub(crate) name: String,
    db: Db,
    bl_db: Db,
    source_db: Db,
    target_db: Db,
}

impl IsarLink {
    pub fn new(name: String, db: Db, bl_db: Db, source_db: Db, target_db: Db) -> IsarLink {
        IsarLink {
            name,
            db,
            bl_db,
            source_db,
            target_db,
        }
    }

    pub fn get_target_col_runtime_id(&self) -> u64 {
        self.target_db.runtime_id()
    }

    pub fn iter_ids<F>(
        &self,
        cursors: &IsarCursors,
        id_key: &IdKey,
        mut callback: F,
    ) -> Result<bool>
    where
        F: FnMut(&mut Cursor, IdKey) -> Result<bool>,
    {
        let mut cursor = cursors.get_cursor(self.db)?;
        cursor.iter_dups(id_key.as_bytes(), |cursor, link_target_key| {
            callback(cursor, IdKey::from_bytes(link_target_key))
        })
    }

    pub fn iter<'txn, 'env, F>(
        &self,
        cursors: &IsarCursors<'txn, 'env>,
        id_key: &IdKey,
        mut callback: F,
    ) -> Result<bool>
    where
        F: FnMut(IdKey<'txn>, IsarObject<'txn>) -> Result<bool>,
    {
        let mut target_cursor = cursors.get_cursor(self.target_db)?;
        self.iter_ids(cursors, id_key, |_, link_target_key| {
            if let Some((id, object)) = target_cursor.move_to(link_target_key.as_bytes())? {
                callback(IdKey::from_bytes(id), IsarObject::from_bytes(object))
            } else {
                Err(IsarError::DbCorrupted {
                    message: "Target object does not exist".to_string(),
                })
            }
        })
    }

    pub fn create(
        &self,
        cursors: &IsarCursors,
        source_key: &IdKey,
        target_key: &IdKey,
    ) -> Result<bool> {
        let mut source_cursor = cursors.get_cursor(self.source_db)?;
        let mut target_cursor = cursors.get_cursor(self.target_db)?;
        let exists_source = source_cursor.move_to(source_key.as_bytes())?.is_some();
        let exists_target = target_cursor.move_to(target_key.as_bytes())?.is_some();
        if !exists_source || !exists_target {
            return Ok(false);
        }

        let mut link_cursor = cursors.get_cursor(self.db)?;
        link_cursor.put(source_key.as_bytes(), target_key.as_bytes())?;

        let mut backlink_cursor = cursors.get_cursor(self.bl_db)?;
        backlink_cursor.put(target_key.as_bytes(), source_key.as_bytes())?;
        Ok(true)
    }

    pub fn delete(
        &self,
        cursors: &IsarCursors,
        source_key: &IdKey,
        target_key: &IdKey,
    ) -> Result<bool> {
        let mut link_cursor = cursors.get_cursor(self.db)?;
        let exists = link_cursor
            .move_to_key_val(source_key.as_bytes(), target_key.as_bytes())?
            .is_some();

        if exists {
            let mut backlink_cursor = cursors.get_cursor(self.bl_db)?;
            let backlink_exists = backlink_cursor
                .move_to_key_val(target_key.as_bytes(), source_key.as_bytes())?
                .is_some();
            if backlink_exists {
                link_cursor.delete_current()?;
                backlink_cursor.delete_current()?;
                Ok(true)
            } else {
                Err(IsarError::DbCorrupted {
                    message: "Backlink does not exist".to_string(),
                })
            }
        } else {
            Ok(false)
        }
    }

    pub fn delete_all_for_object(&self, cursors: &IsarCursors, id_key: &IdKey) -> Result<()> {
        let mut backlink_cursor = cursors.get_cursor(self.bl_db)?;
        self.iter_ids(cursors, id_key, |cursor, link_target_key| {
            let exists = backlink_cursor
                .move_to_key_val(link_target_key.as_bytes(), id_key.as_bytes())?
                .is_some();
            if exists {
                cursor.delete_current()?;
                backlink_cursor.delete_current()?;
                Ok(true)
            } else {
                Err(IsarError::DbCorrupted {
                    message: "Backlink does not exist".to_string(),
                })
            }
        })?;
        Ok(())
    }

    pub fn get_size(&self, txn: &mut IsarTxn) -> Result<u64> {
        Ok(txn.db_stat(self.db)?.1)
    }

    pub fn clear(&self, txn: &mut IsarTxn) -> Result<()> {
        txn.clear_db(self.db)?;
        txn.clear_db(self.bl_db)
    }

    pub fn debug_dump(&self, cursors: &IsarCursors) -> HashSet<(Vec<u8>, Vec<u8>)> {
        let mut cursor = cursors.get_cursor(self.db).unwrap();
        debug_dump_db(&mut cursor)
    }

    pub fn debug_dump_bl(&self, cursors: &IsarCursors) -> HashSet<(Vec<u8>, Vec<u8>)> {
        let mut cursor = cursors.get_cursor(self.bl_db).unwrap();
        debug_dump_db(&mut cursor)
    }
}
