use crate::error::{illegal_arg, IsarError, Result};
use crate::index::Index;
use crate::object::data_type::DataType;
use crate::object::isar_object::{IsarObject, Property};
use crate::object::json_encode_decode::JsonEncodeDecode;
use crate::object::object_builder::ObjectBuilder;
use crate::object::object_id::ObjectId;
use crate::object::object_info::ObjectInfo;
use crate::query::query_builder::QueryBuilder;
use crate::query::where_clause::WhereClause;
use crate::txn::{Cursors, IsarTxn};
use crate::watch::change_set::ChangeSet;
use serde_json::{json, Value};
use std::cell::Cell;

#[cfg(test)]
use {crate::utils::debug::dump_db, hashbrown::HashMap};

pub struct IsarCollection {
    id: u16,
    name: String,
    object_info: ObjectInfo,
    indexes: Vec<Index>,
    oid_counter: Cell<i64>,
}

unsafe impl Send for IsarCollection {}
unsafe impl Sync for IsarCollection {}

impl IsarCollection {
    pub(crate) fn new(id: u16, name: String, object_info: ObjectInfo, indexes: Vec<Index>) -> Self {
        IsarCollection {
            id,
            name,
            object_info,
            indexes,
            oid_counter: Cell::new(0),
        }
    }

    pub(crate) fn get_id(&self) -> u16 {
        self.id
    }

    pub(crate) fn update_oid_counter(&self, counter: i64) {
        if counter > self.oid_counter.get() {
            self.oid_counter.set(counter);
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_oid_property(&self) -> Property {
        self.object_info.get_oid_property()
    }

    pub fn new_int_oid(&self, oid: i32) -> Result<ObjectId<'static>> {
        if self.object_info.get_oid_property().data_type == DataType::Int {
            Ok(ObjectId::from_int(self.id, oid))
        } else {
            illegal_arg("Wrong ObjectId type.")
        }
    }

    pub fn new_long_oid(&self, oid: i64) -> Result<ObjectId<'static>> {
        if self.object_info.get_oid_property().data_type == DataType::Long {
            Ok(ObjectId::from_long(self.id, oid))
        } else {
            illegal_arg("Wrong ObjectId type.")
        }
    }

    pub fn new_string_oid(&self, oid: &str) -> Result<ObjectId<'static>> {
        if self.object_info.get_oid_property().data_type == DataType::String {
            Ok(ObjectId::from_str(self.id, oid))
        } else {
            illegal_arg("Wrong ObjectId type.")
        }
    }

    pub fn get_properties(&self) -> &[(String, Property)] {
        self.object_info.get_properties()
    }

    pub fn new_object_builder(&self, buffer: Option<Vec<u8>>) -> ObjectBuilder {
        ObjectBuilder::new(&self.object_info, buffer)
    }

    pub fn new_query_builder(&self) -> QueryBuilder {
        QueryBuilder::new(self)
    }

    pub fn new_primary_where_clause(&self) -> WhereClause {
        WhereClause::new_primary(&self.id.to_be_bytes())
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

    pub fn auto_increment(&self, _txn: &mut IsarTxn) -> Result<i64> {
        if let Some(counter) = self.oid_counter.get().checked_add(1) {
            self.oid_counter.set(counter);
            match self.get_oid_property().data_type {
                DataType::Int => {
                    if counter <= i32::MAX as i64 {
                        Ok(counter)
                    } else {
                        Err(IsarError::AutoIncrementOverflow {})
                    }
                }
                DataType::Long => Ok(counter),
                DataType::String => illegal_arg("ObjectId cannot be generated"),
                _ => unreachable!(),
            }
        } else {
            Err(IsarError::AutoIncrementOverflow {})
        }
    }

    pub fn get<'txn>(
        &self,
        txn: &'txn mut IsarTxn,
        oid: &ObjectId,
    ) -> Result<Option<IsarObject<'txn>>> {
        if oid.get_col_id() != self.id {
            return Err(IsarError::InvalidObjectId {});
        }
        txn.read(|c| {
            let object = c
                .primary
                .move_to(oid.as_bytes())?
                .map(|(_, v)| IsarObject::new(v));
            Ok(object)
        })
    }

    pub fn put<'a>(&self, txn: &mut IsarTxn, object: IsarObject<'a>) -> Result<()> {
        txn.write(|cursors, change_set| self.put_internal(cursors, change_set, object))
    }

    fn put_internal<'a>(
        &self,
        cursors: &mut Cursors,
        change_set: &mut ChangeSet,
        object: IsarObject<'a>,
    ) -> Result<()> {
        let oid = object.read_oid(self).ok_or(IsarError::InvalidObjectId {})?;
        if !self.delete_internal(cursors, change_set, &oid)? {
            if oid.get_type() == DataType::Int {
                self.update_oid_counter(oid.get_int().unwrap() as i64)
            } else if oid.get_type() == DataType::Long {
                self.update_oid_counter(oid.get_long().unwrap())
            }
        }

        if !self.object_info.verify_object(object) {
            return Err(IsarError::InvalidObject {});
        }

        let oid_bytes = oid.as_bytes();
        for index in &self.indexes {
            index.create_for_object(cursors, &oid, object)?;
        }

        cursors.primary.put(&oid_bytes, object.as_bytes())?;
        change_set.register_change(self.id, &oid, object);
        Ok(())
    }

    pub fn delete(&self, txn: &mut IsarTxn, oid: &ObjectId) -> Result<bool> {
        txn.write(|cursors, change_set| self.delete_internal(cursors, change_set, oid))
    }

    fn delete_internal(
        &self,
        cursors: &mut Cursors,
        change_set: &mut ChangeSet,
        oid: &ObjectId,
    ) -> Result<bool> {
        if let Some((_, existing_object)) = cursors.primary.move_to(oid.as_bytes())? {
            let existing_object = IsarObject::new(existing_object);
            self.delete_current_object_internal(cursors, change_set, oid, existing_object)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn delete_current_object_internal(
        &self,
        cursors: &mut Cursors,
        change_set: &mut ChangeSet,
        oid: &ObjectId,
        object: IsarObject,
    ) -> Result<()> {
        for index in &self.indexes {
            index.delete_for_object(cursors, oid, object)?;
        }
        change_set.register_change(self.id, oid, object);
        cursors.primary.delete_current()?;
        Ok(())
    }

    pub fn clear(&self, txn: &mut IsarTxn) -> Result<usize> {
        let mut counter = 0;
        self.new_query_builder()
            .build()
            .delete_while(txn, self, |_, _| {
                counter += 1;
                true
            })?;
        Ok(counter)
    }

    pub fn import_json(&self, txn: &mut IsarTxn, json: Value) -> Result<()> {
        txn.write(|cursors, change_set| {
            let array = json.as_array().ok_or(IsarError::InvalidJson {})?;
            let mut ob_result_cache = None;
            for value in array {
                let ob = JsonEncodeDecode::decode(self, value, ob_result_cache)?;
                let object = ob.finish();
                if object.is_null(self.get_oid_property()) {
                    return Err(IsarError::InvalidJson {});
                }
                self.put_internal(cursors, change_set, object)?;
                ob_result_cache = Some(ob.recycle());
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
        let mut items = vec![];
        self.new_query_builder()
            .build()
            .find_while(txn, |_, object| {
                let entry = JsonEncodeDecode::encode(self, &object, primitive_null, byte_as_bool);
                items.push(entry);
                true
            })?;
        Ok(json!(items))
    }

    #[cfg(test)]
    pub fn debug_dump(&self, txn: &mut IsarTxn) -> HashMap<ObjectId, Vec<u8>> {
        txn.read(|cursors| {
            let map = dump_db(&mut cursors.primary, Some(&self.id.to_be_bytes()))
                .into_iter()
                .map(|(k, v)| {
                    (
                        ObjectId::from_bytes(self.object_info.get_oid_property().data_type, &k)
                            .to_owned(),
                        v,
                    )
                })
                .collect();
            Ok(map)
        })
        .unwrap()
    }

    #[cfg(test)]
    pub fn debug_get_index(&self, index: usize) -> &Index {
        self.indexes.get(index).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::object::data_type::DataType;
    use crate::object::object_id::ObjectId;
    use crate::query::filter::IntBetweenCond;
    use crate::{col, ind, isar, map, set};
    use crossbeam_channel::unbounded;

    #[test]
    fn test_get() {
        isar!(isar, col => col!(oid => DataType::Int, field2 => DataType::Int));
        let mut txn = isar.begin_txn(true).unwrap();

        let oid = col.new_int_oid(123).unwrap();
        let mut builder = col.new_object_builder(None);
        builder.write_oid(&oid);
        builder.write_int(555);
        let object = builder.finish();
        col.put(&mut txn, object).unwrap();

        assert_eq!(col.get(&mut txn, &oid).unwrap().unwrap(), object);

        let other_oid = ObjectId::from_int(col.id, 321);
        assert_eq!(col.get(&mut txn, &other_oid).unwrap(), None);
    }

    #[test]
    #[should_panic]
    fn test_get_fails_with_wrong_oid() {
        isar!(isar, col => col!(field1 => DataType::Int));

        let oid = ObjectId::from_int(1234, 12);
        let mut txn = isar.begin_txn(true).unwrap();
        col.get(&mut txn, &oid).unwrap();
    }

    #[test]
    fn test_put_new() {
        isar!(isar, col => col!(field1 => DataType::Int));
        let mut txn = isar.begin_txn(true).unwrap();
        assert_eq!(col.oid_counter.get(), 0);

        let mut builder = col.new_object_builder(None);
        builder.write_int(123);
        let object1 = builder.finish();
        col.put(&mut txn, object1).unwrap();
        assert_eq!(col.oid_counter.get(), 123);

        let mut builder = col.new_object_builder(None);
        builder.write_int(100);
        let object2 = builder.finish();
        col.put(&mut txn, object2).unwrap();
        assert_eq!(col.oid_counter.get(), 123);

        assert_eq!(
            col.debug_dump(&mut txn),
            map![
                col.new_int_oid(123).unwrap() => object1.as_bytes().to_vec(),
                col.new_int_oid(100).unwrap() => object2.as_bytes().to_vec()
            ]
        );
    }

    #[test]
    fn test_put_existing() {
        isar!(isar, col => col!(field1 => DataType::Int, field2 => DataType::Int));
        let mut txn = isar.begin_txn(true).unwrap();
        assert_eq!(col.oid_counter.get(), 0);

        let mut builder = col.new_object_builder(None);
        builder.write_int(123);
        builder.write_int(1);
        let object1 = builder.finish();
        col.put(&mut txn, object1).unwrap();
        assert_eq!(col.oid_counter.get(), 123);

        let mut builder = col.new_object_builder(None);
        builder.write_int(123);
        builder.write_int(2);
        let object2 = builder.finish();
        col.put(&mut txn, object2).unwrap();
        assert_eq!(col.oid_counter.get(), 123);

        let mut builder = col.new_object_builder(None);
        builder.write_int(333);
        builder.write_int(3);
        let object3 = builder.finish();
        col.put(&mut txn, object3).unwrap();
        assert_eq!(col.oid_counter.get(), 333);

        assert_eq!(
            col.debug_dump(&mut txn),
            map![
                col.new_int_oid(123).unwrap() => object2.as_bytes().to_vec(),
                col.new_int_oid(333).unwrap() => object3.as_bytes().to_vec()
            ]
        );
    }

    #[test]
    fn test_put_creates_index() {
        isar!(isar, col => col!(field1 => DataType::Int, field2 => DataType::Int; ind!(field2)));

        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        builder.write_int(1234);
        let object = builder.finish();
        col.put(&mut txn, object).unwrap();
        let oid = col.new_int_oid(1).unwrap();

        let index = &col.indexes[0];
        let key = index.debug_create_keys(object)[0].clone();
        assert_eq!(
            index.debug_dump(&mut txn),
            set![(key, oid.as_bytes().to_vec())]
        );
    }

    #[test]
    fn test_put_clears_old_index() {
        isar!(isar, col => col!(field1 => DataType::Int, field2 => DataType::Int; ind!(field2)));

        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        builder.write_int(1234);
        let object = builder.finish();
        col.put(&mut txn, object).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        builder.write_int(5678);
        let object2 = builder.finish();
        col.put(&mut txn, object2).unwrap();

        let oid = col.new_int_oid(1).unwrap();
        let index = &col.indexes[0];
        let key = index.debug_create_keys(object2)[0].clone();
        assert_eq!(
            index.debug_dump(&mut txn),
            set![(key, oid.as_bytes().to_vec())],
        );
    }

    #[test]
    fn test_put_calls_notifiers() {
        isar!(isar, col => col!(field1 => DataType::Int));
        let p = col.get_properties().first().unwrap().1;

        let mut qb1 = col.new_query_builder();
        qb1.set_filter(IntBetweenCond::filter(p, 1, 1).unwrap());
        let q1 = qb1.build();

        let mut qb2 = col.new_query_builder();
        qb2.set_filter(IntBetweenCond::filter(p, 2, 2).unwrap());
        let q2 = qb2.build();

        let (tx1, rx1) = unbounded();
        let handle1 = isar.watch_query(col, q1, Box::new(move || tx1.send(true).unwrap()));

        let (tx2, rx2) = unbounded();
        let handle2 = isar.watch_query(col, q2, Box::new(move || tx2.send(true).unwrap()));

        let mut txn = isar.begin_txn(true).unwrap();
        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        col.put(&mut txn, builder.finish()).unwrap();
        txn.commit().unwrap();

        assert_eq!(rx1.len(), 1);
        assert_eq!(rx2.len(), 0);
        assert!(rx1.try_recv().unwrap());

        let mut txn = isar.begin_txn(true).unwrap();
        let mut builder = col.new_object_builder(None);
        builder.write_int(2);
        col.put(&mut txn, builder.finish()).unwrap();
        txn.commit().unwrap();

        assert_eq!(rx1.len(), 0);
        assert_eq!(rx2.len(), 1);
        handle1.stop();
        handle2.stop();
    }

    #[test]
    fn test_delete() {
        isar!(isar, col => col!(oid => DataType::Int, field => DataType::Int; ind!(field)));

        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        builder.write_int(111);
        let object = builder.finish();
        col.put(&mut txn, object).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(2);
        builder.write_int(222);
        let object2 = builder.finish();
        col.put(&mut txn, object2).unwrap();

        let oid = col.new_int_oid(1).unwrap();
        let oid2 = col.new_int_oid(2).unwrap();
        col.delete(&mut txn, &oid).unwrap();

        assert_eq!(
            col.debug_dump(&mut txn),
            map![oid2.clone() => object2.as_bytes().to_vec()],
        );

        let index = &col.indexes[0];
        let key = index.debug_create_keys(object2)[0].clone();
        assert_eq!(
            index.debug_dump(&mut txn),
            set![(key, oid2.as_bytes().to_vec())],
        );
    }

    #[test]
    fn test_delete_calls_notifiers() {
        isar!(isar, col => col!(field1 => DataType::Int));

        let (tx, rx) = unbounded();
        let handle = isar.watch_collection(col, Box::new(move || tx.send(true).unwrap()));

        let mut txn = isar.begin_txn(true).unwrap();
        let mut builder = col.new_object_builder(None);
        builder.write_int(1234);
        col.put(&mut txn, builder.finish()).unwrap();
        txn.commit().unwrap();

        assert_eq!(rx.len(), 1);
        assert!(rx.try_recv().unwrap());
        handle.stop();
    }
}
