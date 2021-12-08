use crate::error::IsarError::DbCorrupted;
use crate::error::{IsarError, Result};
use crate::instance::IsarInstance;
use crate::lmdb::cursor::Cursor;
use crate::lmdb::{IntKey, Key};
use crate::object::isar_object::IsarObject;

use crate::instance::IsarInstance;
use crate::lmdb::db::Db;
#[cfg(test)]
use {
    crate::txn::IsarTxn, crate::utils::debug::dump_db_oid, hashbrown::HashMap, hashbrown::HashSet,
};

#[derive(Copy, Clone)]
pub(crate) struct Link {
    db: Db,
    source_db: Db,
    target_db: Db,
    backlink: bool,
}

impl IsarLink {
    pub fn new(id: u16, backlink_id: u16, col_id: u16, target_col_id: u16) -> IsarLink {
        IsarLink {
            id,
            col_id,
            backlink_id,
            target_col_id,
        }
    }

    pub fn get_target_col_id(&self) -> u16 {
        self.target_col_id
    }

    pub fn to_backlink(&self) -> IsarLink {
        IsarLink {
            id: self.backlink_id,
            backlink_id: self.id,
            col_id: self.target_col_id,
            target_col_id: self.col_id,
        }
    }

    pub fn link_key(&self, oid: i64) -> IntKey {
        IntKey::new(self.id, oid)
    }

    pub fn link_target(&self, target_oid: i64) -> IntKey {
        IntKey::new(self.target_col_id, target_oid)
    }

    pub fn bl_key(&self, target_oid: i64) -> IntKey {
        IntKey::new(self.backlink_id, target_oid)
    }

    pub fn bl_target(&self, oid: i64) -> IntKey {
        IntKey::new(self.col_id, oid)
    }

    pub fn iter_ids<'txn, F>(
        &self,
        links_cursor: &mut Cursor<'txn>,
        oid: i64,
        mut callback: F,
    ) -> Result<bool>
    where
        F: FnMut(&mut Cursor<'txn>, IntKey) -> Result<bool>,
    {
        let link_key = self.link_key(oid);
        links_cursor.iter_dups(link_key, |links_cursor, _, link_target_bytes| {
            callback(links_cursor, IntKey::from_bytes(link_target_bytes))
        })
    }

    pub fn iter<'txn, F>(
        &self,
        data_cursor: &mut Cursor<'txn>,
        links_cursor: &mut Cursor,
        oid: i64,
        mut callback: F,
    ) -> Result<bool>
    where
        F: FnMut(IsarObject<'txn>) -> Result<bool>,
    {
        self.iter_ids(links_cursor, oid, |_, link_target_key| {
            if let Some((_, object)) = data_cursor.move_to(link_target_key)? {
                callback(IsarObject::from_bytes(object))
            } else {
                Err(IsarError::DbCorrupted {
                    message: "Target object does not exist".to_string(),
                })
            }
        })
    }

    pub fn create(
        &self,
        data_cursor: &mut Cursor,
        links_cursor: &mut Cursor,
        oid: i64,
        target_oid: i64,
    ) -> Result<bool> {
        let id_key = IntKey::new(self.col_id, oid);
        let target_id_key = IntKey::new(self.target_col_id, target_oid);
        if data_cursor.move_to(id_key)?.is_none() || data_cursor.move_to(target_id_key)?.is_none() {
            return Ok(false);
        }

        let link_key = self.link_key(oid);
        let link_target = self.link_target(target_oid);
        links_cursor.put(link_key, link_target.as_bytes())?;

        self.create_backlink(links_cursor, oid, target_oid)?;

        Ok(true)
    }

    pub fn delete(&self, links_cursor: &mut Cursor, oid: i64, target_oid: i64) -> Result<bool> {
        let link_key = self.link_key(oid);
        let link_target = self.link_target(target_oid);
        let exists = links_cursor
            .move_to_key_val(link_key, link_target.as_bytes())?
            .is_some();

        if exists {
            links_cursor.delete_current()?;
            self.delete_backlink(links_cursor, oid, target_oid)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn delete_all_for_object(&self, links_cursor: &mut Cursor, oid: i64) -> Result<()> {
        let mut target_oids = vec![];
        self.iter_ids(links_cursor, oid, |links_cursor, link_target_key| {
            target_oids.push(link_target_key.get_id());
            links_cursor.delete_current()?;
            Ok(true)
        })?;

        for target_oid in target_oids {
            self.delete_backlink(links_cursor, oid, target_oid)?;
        }
        Ok(())
    }

    fn create_backlink(&self, links_cursor: &mut Cursor, oid: i64, target_oid: i64) -> Result<()> {
        let bl_key = self.bl_key(target_oid);
        let bl_target = self.bl_target(oid);
        links_cursor.put(bl_key, bl_target.as_bytes())
    }

    fn delete_backlink(&self, links_cursor: &mut Cursor, oid: i64, target_oid: i64) -> Result<()> {
        let bl_key = self.bl_key(target_oid);
        let bl_target = self.bl_target(oid);
        let backlink_exists = links_cursor
            .move_to_key_val(bl_key, bl_target.as_bytes())?
            .is_some();
        if backlink_exists {
            links_cursor.delete_current()?;
            Ok(())
        } else {
            Err(DbCorrupted {
                message: "Backlink does not exist".to_string(),
            })
        }
    }

    pub fn clear(&self, links_cursor: &mut Cursor) -> Result<()> {
        let min_link_key = self.link_key(IsarInstance::MIN_ID);
        let max_link_key = self.link_key(IsarInstance::MAX_ID);
        Self::clear_internal(links_cursor, min_link_key, max_link_key)?;
        let min_bl_key = self.bl_key(IsarInstance::MIN_ID);
        let max_bl_key = self.bl_key(IsarInstance::MAX_ID);
        Self::clear_internal(links_cursor, min_bl_key, max_bl_key)?;
        Ok(())
    }

    fn clear_internal(links_cursor: &mut Cursor, min_key: IntKey, max_key: IntKey) -> Result<()> {
        links_cursor.iter_between(min_key, max_key, false, true, |cursor, _, _| {
            cursor.delete_current()?;
            Ok(true)
        })?;
        Ok(())
    }
}
