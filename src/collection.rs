use crate::cursor::IsarCursors;
use crate::error::{illegal_arg, IsarError, Result};
use crate::index::index_key::IndexKey;
use crate::index::index_key_builder::IndexKeyBuilder;
use crate::index::IsarIndex;
use crate::link::IsarLink;
use crate::mdbx::db::Db;
use crate::mdbx::debug_dump_db;
use crate::object::id::BytesToId;
use crate::object::isar_object::IsarObject;
use crate::object::json_encode_decode::JsonEncodeDecode;
use crate::object::object_builder::ObjectBuilder;
use crate::object::property::Property;
use crate::query::query_builder::QueryBuilder;
use crate::txn::IsarTxn;
use crate::watch::change_set::ChangeSet;
use serde_json::Value;
use std::cell::Cell;
use std::collections::HashSet;
use std::ops::Deref;

pub struct IsarCollection {
    pub name: String,
    pub properties: Vec<Property>,

    pub(crate) instance_id: u64,
    pub(crate) db: Db,

    pub(crate) indexes: Vec<IsarIndex>,
    pub(crate) links: Vec<IsarLink>, // links from this collection
    backlinks: Vec<IsarLink>,        // links to this collection

    auto_increment: Cell<i64>,
}

unsafe impl Send for IsarCollection {}
unsafe impl Sync for IsarCollection {}

impl IsarCollection {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        db: Db,
        instance_id: u64,
        name: String,
        properties: Vec<Property>,
        indexes: Vec<IsarIndex>,
        links: Vec<IsarLink>,
        backlinks: Vec<IsarLink>,
    ) -> Self {
        IsarCollection {
            instance_id,
            db,
            name,
            properties,
            indexes,
            links,
            backlinks,
            auto_increment: Cell::new(0),
        }
    }

    pub fn new_object_builder(&self, buffer: Option<Vec<u8>>) -> ObjectBuilder {
        ObjectBuilder::new(&self.properties, buffer)
    }

    pub fn new_query_builder(&self) -> QueryBuilder {
        QueryBuilder::new(self)
    }

    pub(crate) fn init_auto_increment(&self, cursors: &IsarCursors) -> Result<()> {
        let mut cursor = cursors.get_cursor(self.db)?;
        if let Some((key, _)) = cursor.move_to_last()? {
            let id = key.deref().to_id();
            self.update_auto_increment(id);
        }
        Ok(())
    }

    pub(crate) fn update_auto_increment(&self, id: i64) {
        if id > self.auto_increment.get() {
            self.auto_increment.set(id);
        }
    }

    pub fn auto_increment(&self, _: &mut IsarTxn) -> Result<i64> {
        self.auto_increment_internal()
    }

    pub(crate) fn auto_increment_internal(&self) -> Result<i64> {
        let last = self.auto_increment.get();
        if last < i64::MAX {
            self.auto_increment.set(last + 1);
            Ok(last + 1)
        } else {
            Err(IsarError::AutoIncrementOverflow {})
        }
    }

    pub(crate) fn get_runtime_id(&self) -> u64 {
        self.db.runtime_id()
    }

    pub fn get<'txn>(&self, txn: &'txn mut IsarTxn, id: i64) -> Result<Option<IsarObject<'txn>>> {
        txn.read(self.instance_id, |cursors| {
            let mut cursor = cursors.get_cursor(self.db)?;
            let object = cursor
                .move_to(&id)?
                .map(|(_, v)| IsarObject::from_bytes(&v));
            Ok(object)
        })
    }

    pub(crate) fn get_index_by_id(&self, index_id: usize) -> Result<&IsarIndex> {
        self.indexes.get(index_id).ok_or(IsarError::UnknownIndex {})
    }

    pub fn get_by_index<'txn>(
        &self,
        txn: &'txn mut IsarTxn,
        index_id: usize,
        key: &IndexKey,
    ) -> Result<Option<(i64, IsarObject<'txn>)>> {
        let index = self.get_index_by_id(index_id)?;
        txn.read(self.instance_id, |cursors| {
            if let Some(id) = index.get_id(cursors, key)? {
                let mut cursor = cursors.get_cursor(self.db)?;
                let (_, bytes) = cursor.move_to(&id)?.ok_or(IsarError::DbCorrupted {
                    message: "Invalid index entry".to_string(),
                })?;
                let result = (id, IsarObject::from_bytes(&bytes));
                Ok(Some(result))
            } else {
                Ok(None)
            }
        })
    }

    pub fn put(&self, txn: &mut IsarTxn, id: Option<i64>, object: IsarObject) -> Result<i64> {
        txn.write(self.instance_id, |cursors, change_set| {
            self.put_internal(cursors, change_set, id, object)
        })
    }

    pub fn put_by_index(
        &self,
        txn: &mut IsarTxn,
        index_id: usize,
        object: IsarObject,
    ) -> Result<i64> {
        let index = self.get_index_by_id(index_id)?;
        if index.multi_entry {
            illegal_arg("Cannot put by a multi-entry index")?;
        }
        let key_builder = IndexKeyBuilder::new(&index.properties);
        txn.write(self.instance_id, |cursors, change_set| {
            let key = key_builder.create_primitive_key(object);
            let id = index.get_id(cursors, &key)?;
            let new_id = self.put_internal(cursors, change_set, id, object)?;
            Ok(new_id)
        })
    }

    fn put_internal(
        &self,
        cursors: &IsarCursors,
        mut change_set: Option<&mut ChangeSet>,
        id: Option<i64>,
        object: IsarObject,
    ) -> Result<i64> {
        let id = if let Some(id) = id {
            self.delete_internal(cursors, false, change_set.as_deref_mut(), id)?;
            self.update_auto_increment(id);
            id
        } else {
            self.auto_increment_internal()?
        };

        for index in &self.indexes {
            index.create_for_object(cursors, id, object, |id| {
                self.delete_internal(cursors, true, change_set.as_deref_mut(), id)?;
                Ok(())
            })?;
        }

        let mut cursor = cursors.get_cursor(self.db)?;
        cursor.put(&id, object.as_bytes())?;
        if let Some(change_set) = change_set {
            change_set.register_change(self.get_runtime_id(), Some(id), Some(object));
        }
        Ok(id)
    }

    pub fn delete(&self, txn: &mut IsarTxn, id: i64) -> Result<bool> {
        txn.write(self.instance_id, |cursors, change_set| {
            self.delete_internal(cursors, true, change_set, id)
        })
    }

    pub fn delete_by_index(
        &self,
        txn: &mut IsarTxn,
        index_id: usize,
        key: &IndexKey,
    ) -> Result<bool> {
        let index = self.get_index_by_id(index_id)?;
        txn.write(self.instance_id, |cursors, change_set| {
            if let Some(id) = index.get_id(cursors, key)? {
                self.delete_internal(cursors, true, change_set, id)?;
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    fn delete_internal(
        &self,
        cursors: &IsarCursors,
        delete_links: bool,
        change_set: Option<&mut ChangeSet>,
        id: i64,
    ) -> Result<bool> {
        let mut cursor = cursors.get_cursor(self.db)?;
        if let Some((_, object)) = cursor.move_to(&id)? {
            let object = IsarObject::from_bytes(&object);
            for index in &self.indexes {
                index.delete_for_object(cursors, id, object)?;
            }
            if delete_links {
                for link in &self.links {
                    link.delete_all_for_object(cursors, id)?;
                }
                for link in &self.backlinks {
                    link.delete_all_for_object(cursors, id)?;
                }
            }
            if let Some(change_set) = change_set {
                change_set.register_change(self.get_runtime_id(), Some(id), Some(object));
            }
            cursor.delete_current()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn get_link_backlink(&self, link_id: usize) -> Result<&IsarLink> {
        if link_id < self.links.len() {
            self.links.get(link_id)
        } else {
            self.backlinks.get(link_id - self.links.len())
        }
        .ok_or(IsarError::IllegalArg {
            message: "IsarLink does not exist".to_string(),
        })
    }

    pub fn link(&self, txn: &mut IsarTxn, link_id: usize, id: i64, target_id: i64) -> Result<bool> {
        let link = self.get_link_backlink(link_id)?;
        txn.write(self.instance_id, |cursors, change_set| {
            self.register_link_change(change_set, link);
            link.create(cursors, id, target_id)
        })
    }

    pub fn unlink(
        &self,
        txn: &mut IsarTxn,
        link_id: usize,
        id: i64,
        target_id: i64,
    ) -> Result<bool> {
        let link = self.get_link_backlink(link_id)?;
        txn.write(self.instance_id, |cursors, change_set| {
            self.register_link_change(change_set, link);
            link.delete(cursors, id, target_id)
        })
    }

    pub fn unlink_all(&self, txn: &mut IsarTxn, link_id: usize, id: i64) -> Result<()> {
        let link = self.get_link_backlink(link_id)?;
        txn.write(self.instance_id, |cursors, change_set| {
            self.register_link_change(change_set, link);
            link.delete_all_for_object(cursors, id)
        })
    }

    pub fn clear(&self, txn: &mut IsarTxn) -> Result<()> {
        for index in &self.indexes {
            index.clear(txn)?;
        }
        for link in &self.links {
            link.clear(txn)?;
        }
        for link in &self.backlinks {
            link.clear(txn)?;
        }
        txn.clear_db(self.db)?;
        txn.register_all_changed(self.get_runtime_id())?;
        self.auto_increment.set(0);
        Ok(())
    }

    pub fn count(&self, txn: &mut IsarTxn) -> Result<u64> {
        Ok(txn.db_stat(self.db)?.0)
    }

    pub fn get_size(
        &self,
        txn: &mut IsarTxn,
        include_indexes: bool,
        include_links: bool,
    ) -> Result<u64> {
        let mut size = txn.db_stat(self.db)?.1;

        if include_indexes {
            for index in &self.indexes {
                size += index.get_size(txn)?;
            }
        }

        if include_links {
            for link in &self.links {
                size += link.get_size(txn)?;
            }
        }

        Ok(size)
    }

    pub fn import_json(&self, txn: &mut IsarTxn, id_name: Option<&str>, json: Value) -> Result<()> {
        txn.write(self.instance_id, |cursors, mut change_set| {
            let array = json.as_array().ok_or(IsarError::InvalidJson {})?;
            let mut ob_result_cache = None;
            for value in array {
                let id = if let Some(id_name) = id_name {
                    if let Some(id) = value.get(id_name) {
                        let id = id.as_i64().ok_or(IsarError::InvalidJson {})?;
                        Some(id)
                    } else {
                        None
                    }
                } else {
                    None
                };
                let ob = JsonEncodeDecode::decode(&self.properties, value, ob_result_cache)?;
                let object = ob.finish();
                self.put_internal(cursors, change_set.as_deref_mut(), id, object)?;
                ob_result_cache = Some(ob.recycle());
            }
            Ok(())
        })
    }

    fn register_link_change(&self, change_set: Option<&mut ChangeSet>, link: &IsarLink) {
        if let Some(change_set) = change_set {
            change_set.register_change(self.get_runtime_id(), None, None);
            change_set.register_change(link.get_target_col_runtime_id(), None, None);
        }
    }

    pub(crate) fn fill_indexes(&self, indexes: &[usize], cursors: &IsarCursors) -> Result<()> {
        let mut cursor = cursors.get_cursor(self.db)?;
        cursor.iter_all(false, true, |cursor, id_bytes, object| {
            let id = id_bytes.to_id();
            let object = IsarObject::from_bytes(&object);
            for index_id in indexes {
                let index = self.indexes.get(*index_id).unwrap();
                index.create_for_object(cursors, id, object, |id| {
                    let deleted = self.delete_internal(cursors, true, None, id)?;
                    if deleted {
                        cursor.move_to_next()?;
                    }
                    Ok(())
                })?;
            }
            Ok(true)
        })?;
        Ok(())
    }

    pub(crate) fn debug_dump(&self, cursors: &IsarCursors) -> HashSet<(Vec<u8>, Vec<u8>)> {
        let mut cursor = cursors.get_cursor(self.db).unwrap();
        debug_dump_db(&mut cursor)
    }
}
