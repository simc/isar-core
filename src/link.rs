use crate::error::{IsarError, Result};
use crate::lmdb::Key;
use crate::object::isar_object::IsarObject;
use crate::txn::Cursors;
use crate::utils::{oid_from_bytes, oid_to_bytes};

#[derive(Clone)]
pub struct Link {
    id: u16,
    col_id: u16,
    backlink_id: u16,
    target_col_id: u16,
}

impl Link {
    pub(crate) fn new(id: u16, col_id: u16, backlink_id: u16, target_col_id: u16) -> Link {
        Link {
            id,
            col_id,
            backlink_id,
            target_col_id,
        }
    }

    pub(crate) fn iter<F>(&self, cursors: &mut Cursors, oid: i64, mut callback: F) -> Result<bool>
    where
        F: FnMut(IsarObject) -> Result<bool>,
    {
        let link_oid_bytes = oid_to_bytes(oid, self.id)?;
        let primary_cursor = &mut cursors.primary;
        let links_cursor = &mut cursors.links;
        links_cursor.iter_dups(Key(&link_oid_bytes), |_, _, target_oid| {
            let (_, object) =
                primary_cursor
                    .move_to(Key(&target_oid))?
                    .ok_or(IsarError::DbCorrupted {
                        message: "Target object does not exist".to_string(),
                    })?;
            callback(IsarObject::from_bytes(object))
        })
    }

    pub(crate) fn get_target_id(&self) -> u16 {
        self.target_col_id
    }

    pub(crate) fn delete_for_object(&self, cursors: &mut Cursors, oid: i64) -> Result<()> {
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
