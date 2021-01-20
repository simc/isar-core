use crate::error::{IsarError, Result};
use crate::index::Index;
use crate::object::object_builder::{ObjectBuilder, ObjectBuilderBytes};
use crate::object::object_id::ObjectId;
use crate::object::object_id_generator::ObjectIdGenerator;
use crate::object::object_info::ObjectInfo;
use crate::object::property::Property;
use crate::query::query_builder::QueryBuilder;
use crate::query::where_clause::WhereClause;
use crate::txn::{Cursors, IsarTxn};
use crate::watch::change_set::ChangeSet;
use serde_json::{json, Value};
use std::hash::{Hash, Hasher};

use crate::object::json_encode_decode::JsonEncodeDecode;
#[cfg(test)]
use {crate::utils::debug::dump_db, hashbrown::HashMap};

pub struct IsarCollection {
    id: u16,
    name: String,
    object_info: ObjectInfo,
    indexes: Vec<Index>,
    oidg: ObjectIdGenerator,
}

impl IsarCollection {
    pub(crate) fn new(id: u16, name: String, object_info: ObjectInfo, indexes: Vec<Index>) -> Self {
        IsarCollection {
            id,
            name,
            object_info,
            indexes,
            oidg: ObjectIdGenerator::new(),
        }
    }

    pub(crate) fn get_id(&self) -> u16 {
        self.id
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_properties(&self) -> &[(String, Property)] {
        self.object_info.get_properties()
    }

    pub fn new_object_builder(&self, bytes: Option<ObjectBuilderBytes>) -> ObjectBuilder {
        ObjectBuilder::new(&self.object_info, bytes)
    }

    pub fn new_query_builder(&self) -> QueryBuilder {
        QueryBuilder::new(self)
    }

    pub fn new_primary_where_clause(&self) -> WhereClause {
        WhereClause::new_primary(self.id)
    }

    pub fn new_secondary_where_clause(
        &self,
        index_index: usize,
        skip_duplicates: bool,
    ) -> Option<WhereClause> {
        self.indexes
            .get(index_index)
            .map(|i| i.new_where_clause(skip_duplicates))
    }

    pub(crate) fn get_indexes(&self) -> &[Index] {
        &self.indexes
    }

    pub fn get<'txn>(
        &self,
        txn: &'txn mut IsarTxn,
        mut oid: ObjectId,
    ) -> Result<Option<&'txn [u8]>> {
        txn.read(|c| {
            oid.set_prefix(self.id);
            let oid_bytes = oid.as_bytes();
            let object = c.primary.move_to(&oid_bytes)?.map(|(_, v)| v);
            Ok(object)
        })
    }

    pub fn put(&self, txn: &mut IsarTxn, oid: Option<ObjectId>, object: &[u8]) -> Result<ObjectId> {
        txn.write(|cursors, change_set| self.put_internal(cursors, change_set, oid, object))
    }

    pub fn put_all(
        &self,
        txn: &mut IsarTxn,
        entries: &[(Option<ObjectId>, &[u8])],
    ) -> Result<Vec<ObjectId>> {
        txn.write(|cursors, change_set| {
            entries
                .iter()
                .map(|(oid, object)| self.put_internal(cursors, change_set, *oid, *object))
                .collect()
        })
    }

    fn put_internal(
        &self,
        cursors: &mut Cursors,
        change_set: &mut ChangeSet,
        oid: Option<ObjectId>,
        object: &[u8],
    ) -> Result<ObjectId> {
        let oid = if let Some(mut oid) = oid {
            oid.set_prefix(self.id);
            self.delete_internal(cursors, change_set, oid, false)?;
            oid
        } else {
            let mut oid = self.oidg.generate();
            oid.set_prefix(self.id);
            oid
        };

        if !self.object_info.verify_object(object) {
            return Err(IsarError::InvalidObject {});
        }

        let oid_bytes = oid.as_bytes();
        for index in &self.indexes {
            index.create_for_object(cursors, &oid_bytes, object)?;
        }

        cursors.primary.put(&oid_bytes, object)?;
        Ok(oid)
    }

    pub fn delete(&self, txn: &mut IsarTxn, mut oid: ObjectId) -> Result<bool> {
        oid.set_prefix(self.id);
        txn.write(|cursors, change_set| self.delete_internal(cursors, change_set, oid, true))
    }

    fn delete_internal(
        &self,
        cursors: &mut Cursors,
        change_set: &mut ChangeSet,
        oid: ObjectId,
        delete_object: bool,
    ) -> Result<bool> {
        let oid_bytes = oid.as_bytes();
        if let Some((_, existing_object)) = cursors.primary.move_to(&oid_bytes)? {
            self.delete_object_internal(cursors, change_set, oid, existing_object, delete_object)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn delete_object_internal(
        &self,
        cursors: &mut Cursors,
        change_set: &mut ChangeSet,
        oid: ObjectId,
        object: &[u8],
        delete_object: bool,
    ) -> Result<()> {
        let oid_bytes = oid.as_bytes();
        for index in &self.indexes {
            index.delete_for_object(cursors, oid_bytes, object)?;
        }
        change_set.register_change(oid, object);
        if delete_object {
            cursors.primary.delete_current()?;
        }
        Ok(())
    }

    pub fn delete_all(&self, txn: &mut IsarTxn) -> Result<usize> {
        txn.write(|cursors, change_set| self.delete_all_internal(cursors, Some(change_set)))
    }

    pub(crate) fn delete_all_internal(
        &self,
        cursors: &mut Cursors,
        mut change_set: Option<&mut ChangeSet>,
    ) -> Result<usize> {
        let prefix = self.get_id().to_le_bytes();
        let mut counter = 0;
        cursors
            .primary
            .iter_prefix(&prefix, false, |primary, oid, val| {
                if let Some(change_set) = &mut change_set {
                    change_set.register_change(*ObjectId::from_bytes(oid), val);
                }
                primary.delete_current()?;
                counter += 1;
                Ok(true)
            })?;
        for index in self.get_indexes() {
            index.clear(cursors)?;
        }
        Ok(counter)
    }

    pub fn import_json(&self, txn: &mut IsarTxn, json: Value) -> Result<()> {
        txn.write(|cursors, change_set| {
            let array = json.as_array().ok_or(IsarError::InvalidJson {})?;
            let json_encode_decode = JsonEncodeDecode::new(&self.object_info);
            let mut ob_result_cache = None;
            for value in array {
                let (oid, ob_result) = json_encode_decode.decode(value, ob_result_cache)?;
                self.put_internal(cursors, change_set, Some(oid), ob_result.as_ref())?;
                ob_result_cache = Some(ob_result);
            }
            Ok(())
        })
    }

    pub fn export_json(
        &self,
        txn: &mut IsarTxn,
        primitive_null: bool,
        byte_as_bool: bool,
    ) -> Result<Value> {
        txn.read(|cursors| {
            let json_encode_decode = JsonEncodeDecode::new(&self.object_info);
            let mut items = vec![];
            cursors
                .primary
                .iter_prefix(&self.id.to_le_bytes(), false, |_, oid_bytes, val| {
                    let oid = ObjectId::from_bytes(oid_bytes);
                    let entry = json_encode_decode.encode(*oid, val, primitive_null, byte_as_bool);
                    items.push(entry);
                    Ok(true)
                })?;
            Ok(json!(items))
        })
    }

    #[cfg(test)]
    pub fn debug_dump(&self, txn: &mut IsarTxn) -> HashMap<ObjectId, Vec<u8>> {
        txn.read(|cursors| {
            let map = dump_db(&mut cursors.primary, Some(&self.id.to_le_bytes()))
                .into_iter()
                .map(|(k, v)| (*ObjectId::from_bytes(&k), v))
                .collect();
            Ok(map)
        })
        .unwrap()
    }

    #[cfg(test)]
    pub fn debug_get_index(&self, index: usize) -> &Index {
        self.indexes.get(index).unwrap()
    }

    #[cfg(test)]
    pub(crate) fn debug_get_object_info(&self) -> &ObjectInfo {
        &self.object_info
    }
}

impl PartialEq for IsarCollection {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for IsarCollection {}

impl Hash for IsarCollection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u16(self.id)
    }
}

#[cfg(test)]
mod tests {
    use crate::{col, ind, isar, map, set};

    #[test]
    fn test_put_new() {
        isar!(isar, col => col!(field1 => Int));
        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1111111);
        let object1 = builder.finish();
        let oid1 = col.put(&mut txn, None, object1.as_ref()).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(123123123);
        let object2 = builder.finish();
        let oid2 = col.put(&mut txn, None, object2.as_ref()).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(123123123);
        let object3 = builder.finish();
        let oid3 = col.put(&mut txn, None, object3.as_ref()).unwrap();

        assert_eq!(
            col.debug_dump(&mut txn),
            map![
                oid1 => object1.as_ref().to_vec(),
                oid2 => object2.as_ref().to_vec(),
                oid3 => object3.as_ref().to_vec()
            ]
        );
    }

    #[test]
    fn test_put_existing() {
        isar!(isar, col => col!(field1 => Int));

        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1111111);
        let object1 = builder.finish();
        let oid1 = col.put(&mut txn, None, object1.as_ref()).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(123123123);
        let object2 = builder.finish();
        let oid2 = col.put(&mut txn, Some(oid1), object2.as_ref()).unwrap();
        assert_eq!(oid1, oid2);

        let new_oid = col.oidg.generate();
        let mut builder = col.new_object_builder(None);
        builder.write_int(55555555);
        let object3 = builder.finish();
        let oid3 = col.put(&mut txn, Some(new_oid), object3.as_ref()).unwrap();
        assert_eq!(new_oid, oid3);

        assert_eq!(
            col.debug_dump(&mut txn),
            map![
                oid1 => object2.as_ref().to_vec(),
                new_oid => object3.as_ref().to_vec()
            ]
        );
    }

    #[test]
    fn test_put_creates_index() {
        isar!(isar, col => col!(field1 => Int; ind!(field1)));

        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1234);
        let object = builder.finish();
        let oid = col.put(&mut txn, None, object.as_ref()).unwrap();

        let index = &col.indexes[0];
        assert_eq!(
            index.debug_dump(&mut txn),
            set![(index.debug_create_key(object.as_ref()), oid)]
        );
    }

    #[test]
    fn test_put_clears_old_index() {
        isar!(isar, col => col!(field1 => Int; ind!(field1)));

        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1234);
        let object = builder.finish();
        let oid = col.put(&mut txn, None, object.as_ref()).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(5678);
        let object2 = builder.finish();
        col.put(&mut txn, Some(oid), object2.as_ref()).unwrap();

        let index = &col.indexes[0];
        assert_eq!(
            index.debug_dump(&mut txn),
            set![(index.debug_create_key(object2.as_ref()), oid)]
        );
    }

    #[test]
    fn test_delete() {
        isar!(isar, col => col!(field1 => Int; ind!(field1)));

        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(12345);
        let object = builder.finish();
        let oid = col.put(&mut txn, None, object.as_ref()).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(54321);
        let object2 = builder.finish();
        let oid2 = col.put(&mut txn, None, object2.as_ref()).unwrap();

        col.delete(&mut txn, oid).unwrap();

        assert_eq!(
            col.debug_dump(&mut txn),
            map![oid2 => object2.as_ref().to_vec()],
        );

        let index = &col.indexes[0];
        assert_eq!(
            index.debug_dump(&mut txn),
            set![(index.debug_create_key(object2.as_ref()), oid2)],
        );
    }
}
