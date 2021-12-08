use crate::cursor::IsarCursors;
use crate::error::{illegal_arg, IsarError, Result};
use crate::index::index_key::IndexKey;
use crate::index::Index;
use crate::instance::IsarInstance;
use crate::link::Link;
use crate::lmdb::db::Db;
use crate::lmdb::{verify_id, ByteKey, IntKey};
use crate::object::isar_object::{IsarObject, Property};
use crate::object::json_encode_decode::JsonEncodeDecode;
use crate::object::object_builder::ObjectBuilder;
use crate::query::id_where_clause::IdWhereClause;
use crate::query::query_builder::QueryBuilder;
use crate::query::Sort;
use crate::txn::{Cursors, IsarTxn};
use crate::utils::debug::dump_db_oid;
use crate::watch::change_set::ChangeSet;
use hashbrown::HashMap;
use serde_json::Value;
use std::cell::Cell;

pub struct IsarCollection {
    pub name: String,
    pub properties: Vec<Property>,
    pub property_names: Vec<String>,
    db: Db,
    static_size: usize,
    indexes: Vec<Index>,
    links: Vec<(String, Link)>,
    backlinks: Vec<Link>,
    next_auto_increment: Cell<i64>,
}

unsafe impl Send for IsarCollection {}
unsafe impl Sync for IsarCollection {}

impl IsarCollection {
    pub(crate) fn new(
        db: Db,
        name: String,
        properties: Vec<Property>,
        property_names: Vec<String>,
        indexes: Vec<Index>,
        links: Vec<(String, Link)>,
        backlinks: Vec<Link>,
    ) -> Self {
        let static_size = ObjectBuilder::calculate_static_size(&properties);
        IsarCollection {
            db,
            name,
            properties,
            property_names,
            static_size,
            indexes,
            links,
            backlinks,
            next_auto_increment: Cell::new(IsarInstance::MIN_ID + 1),
        }
    }

    pub(crate) fn get_indexes(&self) -> &[Index] {
        &self.indexes
    }

    pub(crate) fn update_auto_increment(&self, id: i64) {
        if id >= self.next_auto_increment.get() {
            self.next_auto_increment.set(id + 1);
        }
    }

    fn get_links_and_backlinks(&self) -> impl Iterator<Item = &Link> {
        self.links
            .iter()
            .map(|(_, l)| l)
            .chain(self.backlinks.iter())
    }

    pub fn new_object_builder(&self, buffer: Option<Vec<u8>>) -> ObjectBuilder {
        ObjectBuilder::new_with_size(&self.properties, self.static_size, buffer)
    }

    pub fn new_query_builder(&self) -> QueryBuilder {
        QueryBuilder::new(self)
    }

    pub fn new_index_key(&self, index_index: usize) -> Option<IndexKey> {
        self.indexes.get(index_index).map(IndexKey::new)
    }

    pub(crate) fn verify_index_key(&self, key: &IndexKey) -> Result<()> {
        if key.index.col_id != self.id {
            return illegal_arg("Invalid IndexKey for this collection");
        }
        Ok(())
    }

    pub fn auto_increment(&self, _txn: &mut IsarTxn) -> Result<i64> {
        self.auto_increment_internal()
    }

    pub(crate) fn auto_increment_internal(&self) -> Result<i64> {
        let id = self.next_auto_increment.get();
        if id <= IsarInstance::MAX_ID {
            self.next_auto_increment.set(id + 1);
            Ok(id)
        } else {
            Err(IsarError::AutoIncrementOverflow {})
        }
    }

    pub fn get<'txn>(&self, txn: &'txn mut IsarTxn, id: i64) -> Result<Option<IsarObject<'txn>>> {
        verify_id(id)?;
        txn.read(|cursors| {
            let object = cursors
                .get_cursor(self.db)?
                .move_to(IntKey::new(self.id, id))?
                .map(|(_, v)| IsarObject::from_bytes(v));
            Ok(object)
        })
    }

    pub fn get_by_index<'txn>(
        &self,
        txn: &'txn mut IsarTxn,
        key: &IndexKey,
    ) -> Result<Option<IsarObject<'txn>>> {
        self.verify_index_key(key)?;
        txn.read(|cursors| {
            let index_result = cursors.index.move_to(ByteKey::new(&key.bytes))?;
            if let Some((_, key)) = index_result {
                let object = cursors
                    .get_cursor(self.db)?
                    .move_to(ByteKey::new(key))?
                    .map(|(_, v)| IsarObject::from_bytes(v));
                Ok(object)
            } else {
                Ok(None)
            }
        })
    }

    pub fn put(&self, txn: &mut IsarTxn, object: IsarObject) -> Result<()> {
        txn.write(|cursors, change_set| self.put_internal(cursors, change_set, object))
    }

    fn put_internal(
        &self,
        cursors: &IsarCursors,
        mut change_set: Option<&mut ChangeSet>,
        object: IsarObject,
    ) -> Result<()> {
        let id = object.read_id();
        verify_id(id)?;
        self.delete_internal(cursors, false, change_set.as_deref_mut(), id)?;
        self.update_auto_increment(id);

        /*if !self.object_info.verify_object(object) {
            return Err(IsarError::InvalidObject {});
        }*/

        for index in &self.indexes {
            index.create_for_object(cursors, id, object, |cursors, id| {
                self.delete_internal(cursors, true, change_set.as_deref_mut(), id)?;
                Ok(())
            })?;
        }

        cursors
            .data
            .put(IntKey::new(self.id, id), object.as_bytes())?;
        self.register_object_change(change_set, id, object);
        Ok(())
    }

    pub fn delete(&self, txn: &mut IsarTxn, id: i64) -> Result<bool> {
        txn.write(|cursors, change_set| self.delete_internal(cursors, true, change_set, id))
    }

    pub fn delete_by_index(&self, txn: &mut IsarTxn, key: &IndexKey) -> Result<bool> {
        self.verify_index_key(key)?;
        txn.write(|cursors, change_set| {
            let index_result = cursors.index.move_to(ByteKey::new(&key.bytes))?;
            if let Some((_, key)) = index_result {
                let id = IntKey::from_bytes(key).get_id();
                self.delete_internal(cursors, true, change_set, id)
            } else {
                Ok(false)
            }
        })
    }

    pub(crate) fn delete_internal(
        &self,
        cursors: &IsarCursors,
        delete_links: bool,
        change_set: Option<&mut ChangeSet>,
        id: i64,
    ) -> Result<bool> {
        if let Some((_, object)) = cursors.data.move_to(IntKey::new(self.id, id))? {
            let object = IsarObject::from_bytes(object);
            for index in &self.indexes {
                index.delete_for_object(cursors, id, object)?;
            }
            if delete_links {
                for link in self.get_links_and_backlinks() {
                    link.delete_all_for_object(&mut cursors.links, id)?;
                }
            }
            self.register_object_change(change_set, id, object);
            cursors.data.delete_current()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn get_link_backlink(&self, link_index: usize, backlink: bool) -> Result<Link> {
        self.links
            .get(link_index)
            .map(|(_, l)| if backlink { l.to_backlink() } else { *l })
            .ok_or(IsarError::IllegalArg {
                message: "Link does not exist".to_string(),
            })
    }

    pub fn link(
        &self,
        txn: &mut IsarTxn,
        link_index: usize,
        backlink: bool,
        id: i64,
        target_id: i64,
    ) -> Result<bool> {
        let link = self.get_link_backlink(link_index, backlink)?;
        txn.write(|cursors, change_set| {
            self.register_link_change(change_set, link);
            link.create(&mut cursors.data, &mut cursors.links, id, target_id)
        })
    }

    pub fn unlink(
        &self,
        txn: &mut IsarTxn,
        link_index: usize,
        backlink: bool,
        id: i64,
        target_id: i64,
    ) -> Result<bool> {
        let link = self.get_link_backlink(link_index, backlink)?;
        txn.write(|cursors, change_set| {
            self.register_link_change(change_set, link);
            link.delete(&mut cursors.links, id, target_id)
        })
    }

    pub fn unlink_all(
        &self,
        txn: &mut IsarTxn,
        link_index: usize,
        backlink: bool,
        id: i64,
    ) -> Result<()> {
        let link = self.get_link_backlink(link_index, backlink)?;
        txn.write(|cursors, change_set| {
            self.register_link_change(change_set, link);
            link.delete_all_for_object(&mut cursors.links, id)
        })
    }

    pub fn get_linked_objects<'txn, F>(
        &self,
        txn: &'txn mut IsarTxn,
        link_index: usize,
        backlink: bool,
        id: i64,
        mut callback: F,
    ) -> Result<bool>
    where
        F: FnMut(IsarObject<'txn>) -> bool,
    {
        let link = self.get_link_backlink(link_index, backlink)?;
        txn.read(|cursors| {
            link.iter(&mut cursors.data, &mut cursors.links, id, |object| {
                Ok(callback(object))
            })
        })
    }

    pub fn clear(&self, txn: &mut IsarTxn) -> Result<usize> {
        txn.write(|cursors, mut change_set| {
            let mut counter = 0;
            for index in &self.indexes {
                index.clear(cursors)?;
            }
            for link in self.get_links_and_backlinks() {
                link.clear(&mut cursors.links)?;
            }
            IdWhereClause::new(
                self,
                IsarInstance::MIN_ID,
                IsarInstance::MAX_ID,
                Sort::Ascending,
            )
            .iter(&mut cursors.data, None, |cursor, id, object| {
                self.register_object_change(change_set.as_deref_mut(), id.get_id(), object);
                cursor.delete_current()?;
                counter += 1;
                Ok(true)
            })?;
            Ok(counter)
        })
    }

    pub fn import_json(&self, txn: &mut IsarTxn, json: Value) -> Result<()> {
        txn.write(|cursors, mut change_set| {
            let array = json.as_array().ok_or(IsarError::InvalidJson {})?;
            let mut ob_result_cache = None;
            for value in array {
                let ob = JsonEncodeDecode::decode(self, value, ob_result_cache)?;
                let object = ob.finish();
                self.put_internal(cursors, change_set.as_deref_mut(), object)?;
                ob_result_cache = Some(ob.recycle());
            }
            Ok(())
        })
    }

    fn register_object_change(
        &self,
        change_set: Option<&mut ChangeSet>,
        id: i64,
        object: IsarObject,
    ) {
        if let Some(change_set) = change_set {
            change_set.register_change(self.id, Some(id), Some(object));
        }
    }

    fn register_link_change(&self, change_set: Option<&mut ChangeSet>, link: Link) {
        if let Some(change_set) = change_set {
            change_set.register_change(self.id, None, None);
            change_set.register_change(link.get_target_col_id(), None, None);
        }
    }

    pub fn debug_dump(&self, txn: &mut IsarTxn) -> HashMap<i64, Vec<u8>> {
        txn.read(|cursors| {
            let map = dump_db_oid(&mut cursors.data, self.id)
                .into_iter()
                .map(|(k, v)| (IntKey::from_bytes(&k).get_id(), v))
                .collect();
            Ok(map)
        })
        .unwrap()
    }

    pub(crate) fn debug_get_indexes(&self) -> &[Index] {
        &self.indexes
    }
}
