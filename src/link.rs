use crate::error::IsarError::DbCorrupted;
use crate::error::{IsarError, Result};
use crate::lmdb::cursor::Cursor;
use crate::lmdb::Key;
use crate::object::isar_object::IsarObject;
use crate::txn::Cursors;
use crate::utils::{oid_from_bytes, oid_to_bytes};

#[derive(Copy, Clone)]
pub(crate) struct Link {
    id: u16,
    col_id: u16,
    backlink_id: u16,
    target_col_id: u16,
}

pub(crate) struct LinkCursors<'txn, 'a> {
    primary: &'a mut Cursor<'txn>,
    links: &'a mut Cursor<'txn>,
}

impl<'txn, 'a> LinkCursors<'txn, 'a> {
    pub fn new(primary: &'a mut Cursor<'txn>, links: &'a mut Cursor<'txn>) -> Self {
        LinkCursors { primary, links }
    }
}

impl Link {
    pub fn new(id: u16, col_id: u16, backlink_id: u16, target_col_id: u16) -> Link {
        Link {
            id,
            col_id,
            backlink_id,
            target_col_id,
        }
    }

    pub fn iter<'txn, 'a, F>(
        &self,
        cursors: &mut LinkCursors<'txn, 'a>,
        oid: i64,
        mut callback: F,
    ) -> Result<bool>
    where
        F: FnMut(IsarObject<'txn>) -> Result<bool>,
    {
        let link_oid_bytes = oid_to_bytes(oid, self.id)?;
        let links = &mut cursors.links;
        let primary = &mut cursors.primary;
        links.iter_dups(Key(&link_oid_bytes), |_, _, target_oid| {
            let (_, object) = primary
                .move_to(Key(&target_oid))?
                .ok_or(IsarError::DbCorrupted {
                    message: "Target object does not exist".to_string(),
                })?;
            callback(IsarObject::from_bytes(object))
        })
    }

    pub fn create(&self, cursors: &mut Cursors, oid: i64, target_oid: i64) -> Result<()> {
        let oid_bytes = oid_to_bytes(oid, self.id)?;
        let target_oid_bytes = oid_to_bytes(target_oid, 0)?;
        cursors.links.put(Key(&oid_bytes), &target_oid_bytes)?;

        let bl_oid_bytes = oid_to_bytes(oid, 0)?;
        let bl_target_oid_bytes = oid_to_bytes(oid, self.backlink_id)?;
        cursors
            .links
            .put(Key(&bl_target_oid_bytes), &bl_oid_bytes)?;
        Ok(())
    }

    pub fn delete(&self, cursors: &mut Cursors, oid: i64, target_oid: i64) -> Result<bool> {
        let oid_bytes = oid_to_bytes(oid, self.id)?;
        let target_oid_bytes = oid_to_bytes(target_oid, 0)?;
        cursors.links.put(Key(&oid_bytes), &target_oid_bytes)?;
        let exists = cursors
            .links
            .move_to_key_val(Key(&oid_bytes), &target_oid_bytes)?
            .is_some();
        if !exists {
            return Ok(false);
        }
        cursors.links.delete_current();

        let bl_oid_bytes = oid_to_bytes(oid, 0)?;
        let bl_target_oid_bytes = oid_to_bytes(oid, self.backlink_id)?;
        let exists = cursors
            .links
            .move_to_key_val(Key(&bl_target_oid_bytes), &bl_oid_bytes)?
            .is_some();
        if exists {
            cursors.links.delete_current()?;
            Ok(true)
        } else {
            Err(DbCorrupted {
                message: "Backlink does not exist".to_string(),
            })
        }
    }

    pub fn get_target_col_id(&self) -> u16 {
        self.target_col_id
    }

    pub fn delete_all_for_object(&self, cursors: &mut Cursors, oid: i64) -> Result<()> {
        let mut target_oids = vec![];
        let link_oid_bytes = oid_to_bytes(oid, self.id)?;
        cursors
            .links
            .iter_dups(Key(&link_oid_bytes), |cursor, _, target_oid| {
                let (oid, _) = oid_from_bytes(target_oid);
                target_oids.push(oid);
                cursor.delete_current()?;
                Ok(true)
            })?;

        let oid_bytes = oid_to_bytes(oid, self.col_id)?;
        for target_oid in target_oids {
            let target_oid_bytes = oid_to_bytes(target_oid, self.backlink_id)?;
            if cursors
                .links
                .move_to_key_val(Key(&target_oid_bytes), &oid_bytes)?
                .is_some()
            {
                cursors.links.delete_current()?;
            }
        }
        Ok(())
    }
}
