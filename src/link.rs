use crate::cursor::IsarCursors;
use crate::error::IsarError::DbCorrupted;
use crate::error::{IsarError, Result};
use crate::key::IdKey;
use crate::mdbx::cursor::Cursor;
use crate::mdbx::db::Db;
use crate::object::isar_object::IsarObject;
use crate::txn::IsarTxn;

#[derive(Copy, Clone)]
pub(crate) struct IsarLink {
    db: Db,
    source_db: Db,
    target_db: Db,
}

impl IsarLink {
    pub fn new(db: Db, source_db: Db, target_db: Db) -> IsarLink {
        IsarLink {
            db,
            source_db,
            target_db,
        }
    }

    pub fn get_target_col_runtime_id(&self) -> u64 {
        self.target_db.runtime_id()
    }

    pub fn iter_ids<'txn, F>(
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
        F: FnMut(IdKey, IsarObject<'txn>) -> Result<bool>,
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
        if source_cursor.move_to(source_key.as_bytes())?.is_none()
            || target_cursor.move_to(target_key.as_bytes())?.is_none()
        {
            return Ok(false);
        }

        let mut link_cursor = cursors.get_cursor(self.db)?;
        link_cursor.put(source_key.as_bytes(), target_key.as_bytes())?;
        link_cursor.put(target_key.as_bytes(), source_key.as_bytes())?;

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
            link_cursor.delete_current()?;

            let backlink_exists = link_cursor
                .move_to_key_val(target_key.as_bytes(), source_key.as_bytes())?
                .is_some();
            if backlink_exists {
                link_cursor.delete_current()?;
                Ok(true)
            } else {
                Err(DbCorrupted {
                    message: "Backlink does not exist".to_string(),
                })
            }
        } else {
            Ok(false)
        }
    }

    pub fn delete_all_for_object(&self, cursors: &IsarCursors, id_key: &IdKey) -> Result<()> {
        let mut target_oids = vec![];
        self.iter_ids(cursors, id_key, |cursor, link_target_key| {
            target_oids.push(link_target_key.get_id());
            cursor.delete_current()?;
            Ok(true)
        })?;

        let mut cursor = cursors.get_cursor(self.db)?;
        for target_id in target_oids {
            let target_id_key = IdKey::new(target_id);
            let exists = cursor
                .move_to_key_val(target_id_key.as_bytes(), id_key.as_bytes())?
                .is_some();
            if exists {
                cursor.delete_current()?;
            } else {
                return Err(DbCorrupted {
                    message: "Backlink does not exist".to_string(),
                });
            }
        }
        Ok(())
    }

    pub fn clear(&self, txn: &mut IsarTxn) -> Result<()> {
        txn.clear_db(self.db)
    }
}
