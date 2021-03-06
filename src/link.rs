use crate::error::IsarError::DbCorrupted;
use crate::error::{IsarError, Result};
use crate::lmdb::cursor::Cursor;
use crate::lmdb::Key;
use crate::object::isar_object::IsarObject;
use crate::utils::{oid_from_bytes, oid_to_bytes, MAX_OID, MIN_OID};

#[derive(Copy, Clone)]
pub(crate) struct Link {
    id: u16,
    col_id: u16,
    backlink_id: u16,
    target_col_id: u16,
}

impl Link {
    pub fn new(id: u16, backlink_id: u16, col_id: u16, target_col_id: u16) -> Link {
        Link {
            id,
            col_id,
            backlink_id,
            target_col_id,
        }
    }

    pub fn get_target_col_id(&self) -> u16 {
        self.target_col_id
    }

    pub fn as_backlink(&self) -> Link {
        Link {
            id: self.backlink_id,
            col_id: self.target_col_id,
            backlink_id: self.id,
            target_col_id: self.col_id,
        }
    }

    fn get_link_bytes(&self, oid: i64) -> Result<[u8; 8]> {
        oid_to_bytes(oid, self.id)
    }

    fn get_link_target_bytes(&self, oid: i64) -> Result<[u8; 8]> {
        oid_to_bytes(oid, self.target_col_id)
    }

    fn get_bl_bytes(&self, oid: i64) -> Result<[u8; 8]> {
        oid_to_bytes(oid, self.backlink_id)
    }

    fn get_bl_target_bytes(&self, oid: i64) -> Result<[u8; 8]> {
        oid_to_bytes(oid, self.col_id)
    }

    pub(crate) fn iter_ids<'txn, F>(
        &self,
        links_cursor: &mut Cursor<'txn>,
        oid: i64,
        mut callback: F,
    ) -> Result<bool>
    where
        F: FnMut(&mut Cursor<'txn>, &[u8]) -> Result<bool>,
    {
        let link_bytes = self.get_link_bytes(oid)?;
        links_cursor.iter_dups(Key(&link_bytes), |links_cursor, _, link_target_bytes| {
            callback(links_cursor, link_target_bytes)
        })
    }

    pub fn iter<'txn, F>(
        &self,
        primary_cursor: &mut Cursor<'txn>,
        links_cursor: &mut Cursor,
        oid: i64,
        mut callback: F,
    ) -> Result<bool>
    where
        F: FnMut(IsarObject<'txn>) -> Result<bool>,
    {
        self.iter_ids(links_cursor, oid, |_, link_target_bytes| {
            if let Some((_, object)) = primary_cursor.move_to(Key(&link_target_bytes))? {
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
        primary_cursor: &mut Cursor,
        links_cursor: &mut Cursor,
        oid: i64,
        target_oid: i64,
    ) -> Result<bool> {
        let oid_bytes = oid_to_bytes(oid, self.col_id)?;
        let target_oid_bytes = oid_to_bytes(target_oid, self.target_col_id)?;
        if primary_cursor.move_to(Key(&oid_bytes))?.is_none()
            || primary_cursor.move_to(Key(&target_oid_bytes))?.is_none()
        {
            return Ok(false);
        }

        let link_bytes = self.get_link_bytes(oid)?;
        let link_target_bytes = self.get_link_target_bytes(target_oid)?;
        links_cursor.put(Key(&link_bytes), &link_target_bytes)?;

        self.create_backlink(links_cursor, oid, target_oid)?;

        Ok(true)
    }

    pub fn delete(&self, links_cursor: &mut Cursor, oid: i64, target_oid: i64) -> Result<bool> {
        let link_bytes = self.get_link_bytes(oid)?;
        let link_target_bytes = self.get_link_target_bytes(target_oid)?;
        let exists = links_cursor
            .move_to_key_val(Key(&link_bytes), &link_target_bytes)?
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
        self.iter_ids(links_cursor, oid, |links_cursor, link_target_bytes| {
            let (target_oid, _) = oid_from_bytes(link_target_bytes);
            target_oids.push(target_oid);
            links_cursor.delete_current()?;
            Ok(true)
        })?;

        for target_oid in target_oids {
            self.delete_backlink(links_cursor, oid, target_oid)?;
        }
        Ok(())
    }

    fn create_backlink(&self, links_cursor: &mut Cursor, oid: i64, target_oid: i64) -> Result<()> {
        if self.col_id == self.target_col_id {
            return Ok(());
        }
        let bl_bytes = self.get_bl_bytes(target_oid)?;
        let bl_target_bytes = self.get_bl_target_bytes(oid)?;
        links_cursor.put(Key(&bl_bytes), &bl_target_bytes)
    }

    fn delete_backlink(&self, links_cursor: &mut Cursor, oid: i64, target_oid: i64) -> Result<()> {
        if self.col_id == self.target_col_id {
            return Ok(());
        }
        let bl_bytes = self.get_bl_bytes(target_oid)?;
        let bl_target_bytes = self.get_bl_target_bytes(oid)?;
        let backlink_exists = links_cursor
            .move_to_key_val(Key(&bl_bytes), &bl_target_bytes)?
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
        let min_link_bytes = self.get_link_bytes(MIN_OID)?;
        let max_link_bytes = self.get_link_bytes(MAX_OID)?;
        links_cursor.iter_between(
            Key(&min_link_bytes),
            Key(&max_link_bytes),
            true,
            true,
            |cursor, _, _| {
                cursor.delete_current()?;
                Ok(true)
            },
        )?;
        if self.col_id == self.target_col_id {
            return Ok(());
        }
        let min_bl_bytes = self.get_bl_bytes(MIN_OID)?;
        let max_bl_bytes = self.get_bl_bytes(MAX_OID)?;
        links_cursor.iter_between(
            Key(&min_bl_bytes),
            Key(&max_bl_bytes),
            true,
            true,
            |cursor, _, _| {
                cursor.delete_current()?;
                Ok(true)
            },
        )?;
        Ok(())
    }
}
