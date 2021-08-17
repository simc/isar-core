use crate::error::IsarError::DbCorrupted;
use crate::error::{IsarError, Result};
use crate::lmdb::cursor::Cursor;
use crate::lmdb::{IntKey, Key};
use crate::object::isar_object::IsarObject;

use crate::instance::IsarInstance;
#[cfg(test)]
use {
    crate::txn::IsarTxn, crate::utils::debug::dump_db_oid, hashbrown::HashMap, hashbrown::HashSet,
};

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

    pub fn to_backlink(&self) -> Link {
        Link {
            id: self.backlink_id,
            backlink_id: self.id,
            col_id: self.target_col_id,
            target_col_id: self.col_id,
        }
    }

    fn link_key(&self, oid: i64) -> IntKey {
        IntKey::new(self.id, oid)
    }

    fn link_target_key(&self, oid: i64) -> IntKey {
        IntKey::new(self.target_col_id, oid)
    }

    fn bl_key(&self, oid: i64) -> IntKey {
        IntKey::new(self.backlink_id, oid)
    }

    fn bl_target_key(&self, oid: i64) -> IntKey {
        IntKey::new(self.col_id, oid)
    }

    pub(crate) fn iter_ids<'txn, F>(
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
        let link_target_key = self.link_target_key(target_oid);
        links_cursor.put(link_key, link_target_key.as_bytes())?;

        self.create_backlink(links_cursor, oid, target_oid)?;

        Ok(true)
    }

    pub fn delete(&self, links_cursor: &mut Cursor, oid: i64, target_oid: i64) -> Result<bool> {
        let link_key = self.link_key(oid);
        let link_target_key = self.link_target_key(target_oid);
        let exists = links_cursor
            .move_to_key_val(link_key, link_target_key.as_bytes())?
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
        let bl_target_key = self.bl_target_key(oid);
        links_cursor.put(bl_key, bl_target_key.as_bytes())
    }

    fn delete_backlink(&self, links_cursor: &mut Cursor, oid: i64, target_oid: i64) -> Result<()> {
        let bl_key = self.bl_key(target_oid);
        let bl_target_key = self.bl_target_key(oid);
        let backlink_exists = links_cursor
            .move_to_key_val(bl_key, bl_target_key.as_bytes())?
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

    #[cfg(test)]
    pub fn debug_dump(&self, txn: &mut IsarTxn) -> HashMap<i64, HashSet<i64>> {
        txn.read(|cursors| {
            let mut map: HashMap<i64, HashSet<i64>> = HashMap::new();
            let entries = dump_db_oid(&mut cursors.links, self.id);
            for (k, v) in entries {
                let key = IntKey::from_bytes(&k);
                let target_key = IntKey::from_bytes(&v);
                if let Some(items) = map.get_mut(&key.get_id()) {
                    items.insert(target_key.get_id());
                } else {
                    let mut set = HashSet::new();
                    set.insert(target_key.get_id());
                    map.insert(key.get_id(), set);
                }
            }
            Ok(map)
        })
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::IsarCollection;
    use crate::instance::IsarInstance;
    use crate::object::data_type::DataType;
    use crate::{col, isar, map, set};

    fn create_objects(isar: &IsarInstance, col: &IsarCollection) {
        let mut ob1 = col.new_object_builder(None);
        ob1.write_long(1);

        let mut ob2 = col.new_object_builder(None);
        ob2.write_long(2);

        let mut ob3 = col.new_object_builder(None);
        ob3.write_long(3);

        let mut txn = isar.begin_txn(true, true).unwrap();
        col.put(&mut txn, ob1.finish()).unwrap();
        col.put(&mut txn, ob2.finish()).unwrap();
        col.put(&mut txn, ob3.finish()).unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn test_create_aborts_if_object_not_existing() {
        isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

        create_objects(&isar, col2);

        let link = Link::new(0, 1, col1.get_id(), col2.get_id());
        let mut txn = isar.begin_txn(true, false).unwrap();

        let success = txn
            .write(|c, _| link.create(&mut c.data, &mut c.links, 555, 0))
            .unwrap();
        assert!(!success);
        assert!(link.debug_dump(&mut txn).is_empty());

        txn.abort();
        isar.close();
    }

    #[test]
    fn test_create_aborts_if_target_object_not_existing() {
        isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

        create_objects(&isar, col1);

        let link = Link::new(0, 1, col1.get_id(), col2.get_id());
        let mut txn = isar.begin_txn(true, false).unwrap();

        let success = txn
            .write(|c, _| link.create(&mut c.data, &mut c.links, 0, 555))
            .unwrap();
        assert!(!success);
        assert!(link.debug_dump(&mut txn).is_empty());

        txn.abort();
        isar.close();
    }

    #[test]
    fn test_create() {
        isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

        create_objects(&isar, col1);
        create_objects(&isar, col2);

        let link = Link::new(123, 456, col1.get_id(), col2.get_id());
        let mut txn = isar.begin_txn(true, false).unwrap();

        txn.write(|c, _| {
            assert!(link.create(&mut c.data, &mut c.links, 1, 1).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 1, 2).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());
            Ok(())
        })
        .unwrap();

        assert_eq!(
            link.debug_dump(&mut txn),
            map!(1 => set![1, 2], 2 => set![2])
        );

        assert_eq!(
            link.to_backlink().debug_dump(&mut txn),
            map!(1 => set![1], 2 => set![1, 2])
        );

        txn.abort();
        isar.close();
    }

    #[test]
    fn test_create_same_collection() {
        isar!(isar, col => col!());

        create_objects(&isar, col);

        let link = Link::new(123, 456, col.get_id(), col.get_id());
        let mut txn = isar.begin_txn(true, false).unwrap();

        txn.write(|c, _| {
            assert!(link.create(&mut c.data, &mut c.links, 1, 1).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 1, 2).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());
            Ok(())
        })
        .unwrap();

        assert_eq!(
            link.debug_dump(&mut txn),
            map!(1 => set![1, 2], 2 => set![2])
        );

        assert_eq!(
            link.to_backlink().debug_dump(&mut txn),
            map!(1 => set![1], 2 => set![1, 2])
        );

        txn.abort();
        isar.close();
    }

    #[test]
    fn test_delete() {
        isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

        create_objects(&isar, col1);
        create_objects(&isar, col2);

        let link = Link::new(123, 456, col1.get_id(), col2.get_id());
        let mut txn = isar.begin_txn(true, false).unwrap();

        txn.write(|c, _| {
            assert!(link.create(&mut c.data, &mut c.links, 1, 1).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 1, 2).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());

            assert!(link.delete(&mut c.links, 1, 2).unwrap());
            assert!(link.delete(&mut c.links, 2, 2).unwrap());
            assert!(!link.delete(&mut c.links, 2, 2).unwrap());
            assert!(!link.delete(&mut c.links, 3, 2).unwrap());
            Ok(())
        })
        .unwrap();

        assert_eq!(link.debug_dump(&mut txn), map!(1 => set![1]));

        assert_eq!(link.to_backlink().debug_dump(&mut txn), map!(1 => set![1]));

        txn.abort();
        isar.close();
    }

    #[test]
    fn test_delete_same_collection() {
        isar!(isar, col => col!());

        create_objects(&isar, col);

        let link = Link::new(123, 456, col.get_id(), col.get_id());
        let mut txn = isar.begin_txn(true, false).unwrap();

        txn.write(|c, _| {
            assert!(link.create(&mut c.data, &mut c.links, 1, 1).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 1, 2).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());

            assert!(link.delete(&mut c.links, 1, 2).unwrap());
            assert!(link.delete(&mut c.links, 2, 2).unwrap());
            assert!(!link.delete(&mut c.links, 2, 2).unwrap());
            assert!(!link.delete(&mut c.links, 3, 2).unwrap());
            Ok(())
        })
        .unwrap();

        assert_eq!(link.debug_dump(&mut txn), map!(1 => set![1]));

        assert_eq!(link.to_backlink().debug_dump(&mut txn), map!(1 => set![1]));

        txn.abort();
        isar.close();
    }

    #[test]
    fn test_delete_all_for_object() {
        isar!(isar, col1 => col!("col1"), col2 => col!("col2"));

        create_objects(&isar, col1);
        create_objects(&isar, col2);

        let link = Link::new(123, 456, col1.get_id(), col2.get_id());
        let mut txn = isar.begin_txn(true, false).unwrap();

        txn.write(|c, _| {
            assert!(link.create(&mut c.data, &mut c.links, 2, 3).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 3, 2).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 3, 1).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());

            link.delete_all_for_object(&mut c.links, 2).unwrap();
            Ok(())
        })
        .unwrap();

        assert_eq!(link.debug_dump(&mut txn), map!(3 => set![1, 2]));

        assert_eq!(
            link.to_backlink().debug_dump(&mut txn),
            map!(1 => set![3], 2 => set![3])
        );

        txn.abort();
        isar.close();
    }

    #[test]
    fn test_clear() {
        isar!(isar, col => col!());

        create_objects(&isar, col);

        let link = Link::new(123, 456, col.get_id(), col.get_id());
        let mut txn = isar.begin_txn(true, false).unwrap();

        txn.write(|c, _| {
            assert!(link.create(&mut c.data, &mut c.links, 2, 3).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 3, 2).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 3, 1).unwrap());
            assert!(link.create(&mut c.data, &mut c.links, 2, 2).unwrap());

            link.clear(&mut c.links).unwrap();
            Ok(())
        })
        .unwrap();

        assert!(link.debug_dump(&mut txn).is_empty());

        assert!(link.to_backlink().debug_dump(&mut txn).is_empty());

        txn.abort();
        isar.close();
    }
}
