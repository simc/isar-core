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

    pub(crate) fn get_oid_type(&self) -> DataType {
        self.object_info.get_oid_type()
    }

    pub(crate) fn update_oid_counter(&self, counter: i64) {
        if counter > self.oid_counter.get() {
            self.oid_counter.set(counter);
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_int_oid(&self, oid: i32) -> ObjectId<'static> {
        ObjectId::from_int(self.id, oid)
    }

    pub fn get_long_oid(&self, oid: i64) -> ObjectId<'static> {
        ObjectId::from_long(self.id, oid)
    }

    pub fn get_string_oid(&self, oid: &str) -> ObjectId<'static> {
        ObjectId::from_str(self.id, oid)
    }

    fn verify_oid(&self, oid: &ObjectId) -> Result<()> {
        if oid.get_col_id() == self.id && oid.get_type() == self.object_info.get_oid_type() {
            Ok(())
        } else {
            Err(IsarError::InvalidObjectId {})
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

    fn generate_oid(&self) -> Result<ObjectId<'static>> {
        if let Some(counter) = self.oid_counter.get().checked_add(1) {
            self.oid_counter.set(counter);
            match self.object_info.get_oid_type() {
                DataType::Int => {
                    if counter <= i32::MAX as i64 {
                        Ok(self.get_int_oid(counter as i32))
                    } else {
                        Err(IsarError::AutoIncrementOverflow {})
                    }
                }
                DataType::Long => Ok(self.get_long_oid(counter)),
                DataType::String => illegal_arg("ObjectId must be provided"),
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
        self.verify_oid(oid)?;
        txn.read(|c| {
            let object = c
                .primary
                .move_to(oid.as_bytes())?
                .map(|(_, v)| IsarObject::new(v));
            Ok(object)
        })
    }

    pub fn put<'a>(
        &self,
        txn: &mut IsarTxn,
        oid: Option<ObjectId<'a>>,
        object: IsarObject,
    ) -> Result<ObjectId<'a>> {
        txn.write(|cursors, change_set| self.put_internal(cursors, change_set, oid, object))
    }

    pub fn put_all<'a>(
        &self,
        txn: &mut IsarTxn,
        entries: Vec<(Option<ObjectId<'a>>, IsarObject)>,
    ) -> Result<Vec<ObjectId<'a>>> {
        txn.write(|cursors, change_set| {
            entries
                .into_iter()
                .map(|(oid, object)| self.put_internal(cursors, change_set, oid, object))
                .collect()
        })
    }

    fn put_internal<'a>(
        &self,
        cursors: &mut Cursors,
        change_set: &mut ChangeSet,
        oid: Option<ObjectId<'a>>,
        object: IsarObject,
    ) -> Result<ObjectId<'a>> {
        let oid = if let Some(oid) = oid {
            self.verify_oid(&oid)?;
            if !self.delete_internal(cursors, change_set, &oid, false)? {
                if oid.get_type() == DataType::Int {
                    self.update_oid_counter(oid.get_int().unwrap() as i64)
                } else if oid.get_type() == DataType::Long {
                    self.update_oid_counter(oid.get_long().unwrap())
                }
            }
            oid
        } else {
            self.generate_oid()?
        };

        if !self.object_info.verify_object(object) {
            return Err(IsarError::InvalidObject {});
        }

        let oid_bytes = oid.as_bytes();
        for index in &self.indexes {
            index.create_for_object(cursors, &oid, object)?;
        }

        cursors.primary.put(&oid_bytes, object.as_bytes())?;
        change_set.register_change(self.id, &oid, object);
        Ok(oid)
    }

    pub fn delete(&self, txn: &mut IsarTxn, oid: &ObjectId) -> Result<bool> {
        txn.write(|cursors, change_set| self.delete_internal(cursors, change_set, oid, true))
    }

    fn delete_internal(
        &self,
        cursors: &mut Cursors,
        change_set: &mut ChangeSet,
        oid: &ObjectId,
        delete_object: bool,
    ) -> Result<bool> {
        if let Some((_, existing_object)) = cursors.primary.move_to(oid.as_bytes())? {
            let existing_object = IsarObject::new(existing_object);
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
        oid: &ObjectId,
        object: IsarObject,
        delete_object: bool,
    ) -> Result<()> {
        for index in &self.indexes {
            index.delete_for_object(cursors, oid, object)?;
        }
        change_set.register_change(self.id, oid, object);
        if delete_object {
            cursors.primary.delete_current()?;
        }
        Ok(())
    }

    pub fn delete_all(&self, txn: &mut IsarTxn) -> Result<usize> {
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
            let json_encode_decode = JsonEncodeDecode::new(self.id, &self.object_info);
            let mut ob_result_cache = None;
            for value in array {
                let (oid, ob) = json_encode_decode.decode(value, ob_result_cache)?;
                self.put_internal(cursors, change_set, oid, ob.finish())?;
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
        let json_encode_decode = JsonEncodeDecode::new(self.id, &self.object_info);
        let mut items = vec![];
        self.new_query_builder()
            .build()
            .find_while(txn, |oid, object| {
                let entry = json_encode_decode.encode(oid, object, primitive_null, byte_as_bool);
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
                        ObjectId::from_bytes(self.object_info.get_oid_type(), &k).to_owned(),
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
    use crate::query::filter::IntBetween;
    use crate::{col, ind, isar, map, set};
    use crossbeam_channel::unbounded;

    #[test]
    fn test_get() {
        isar!(isar, col => col!(field1 => DataType::Int));
        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1111111);
        let object = builder.finish();
        let oid = col.put(&mut txn, None, object).unwrap();

        assert_eq!(col.get(&mut txn, &oid).unwrap().unwrap(), object);

        let other_oid = ObjectId::from_long(col.id, 123);
        assert_eq!(col.get(&mut txn, &other_oid).unwrap(), None);
    }

    #[test]
    fn test_put_new() {
        isar!(isar, col => col!(field1 => DataType::Int));
        let mut txn = isar.begin_txn(true).unwrap();
        assert_eq!(col.oid_counter.get(), 0);

        let mut builder = col.new_object_builder(None);
        builder.write_int(1111111);
        let object1 = builder.finish();
        let oid1 = col.put(&mut txn, None, object1).unwrap();
        assert_eq!(col.oid_counter.get(), 1);

        let mut builder = col.new_object_builder(None);
        builder.write_int(123123123);
        let object2 = builder.finish();
        let oid2 = col.put(&mut txn, None, object2).unwrap();
        assert_eq!(col.oid_counter.get(), 2);

        assert_eq!(
            col.debug_dump(&mut txn),
            map![
                oid1 => object1.as_bytes().to_vec(),
                oid2 => object2.as_bytes().to_vec()
            ]
        );
    }

    #[test]
    fn test_put_existing() {
        isar!(isar, col => col!(field1 => DataType::Int));
        let mut txn = isar.begin_txn(true).unwrap();
        assert_eq!(col.oid_counter.get(), 0);

        let mut builder = col.new_object_builder(None);
        builder.write_int(1111111);
        let object1 = builder.finish();
        let oid1 = col.put(&mut txn, None, object1).unwrap();
        assert_eq!(col.oid_counter.get(), 1);

        let mut builder = col.new_object_builder(None);
        builder.write_int(123123123);
        let object2 = builder.finish();
        let oid2 = col.put(&mut txn, Some(oid1.clone()), object2).unwrap();
        assert_eq!(oid1, oid2);
        assert_eq!(col.oid_counter.get(), 1);

        let new_oid = ObjectId::from_long(col.id, 123);
        let mut builder = col.new_object_builder(None);
        builder.write_int(55555555);
        let object3 = builder.finish();
        let oid3 = col.put(&mut txn, Some(new_oid.clone()), object3).unwrap();
        assert_eq!(new_oid, oid3);
        assert_eq!(col.oid_counter.get(), 123);

        assert_eq!(
            col.debug_dump(&mut txn),
            map![
                oid1 => object2.as_bytes().to_vec(),
                new_oid => object3.as_bytes().to_vec()
            ]
        );
    }

    #[test]
    fn test_put_int_oid_overflow() {
        isar!(isar, col => col!("col", DataType::Int, field1 => DataType::Int;));

        let oid = ObjectId::from_int(col.id, i32::MAX);

        let mut txn = isar.begin_txn(true).unwrap();
        let mut builder = col.new_object_builder(None);
        builder.write_int(123);

        col.put(&mut txn, Some(oid), builder.finish()).unwrap();
        let result = col.put(&mut txn, None, builder.finish());
        assert!(result.is_err())
    }

    #[test]
    fn test_put_long_oid_overflow() {
        isar!(isar, col => col!(field1 => DataType::Int));

        let oid = ObjectId::from_long(col.id, i64::MAX);

        let mut txn = isar.begin_txn(true).unwrap();
        let mut builder = col.new_object_builder(None);
        builder.write_int(123);

        col.put(&mut txn, Some(oid), builder.finish()).unwrap();
        let result = col.put(&mut txn, None, builder.finish());
        assert!(result.is_err())
    }

    #[test]
    fn test_put_creates_index() {
        isar!(isar, col => col!(field1 => DataType::Int; ind!(field1)));

        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1234);
        let object = builder.finish();
        let oid = col.put(&mut txn, None, object).unwrap();

        let index = &col.indexes[0];
        let key = index.debug_create_keys(object)[0].clone();
        assert_eq!(
            index.debug_dump(&mut txn),
            set![(key, oid.as_bytes().to_vec())]
        );
    }

    #[test]
    fn test_put_clears_old_index() {
        isar!(isar, col => col!(field1 => DataType::Int; ind!(field1)));

        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(1234);
        let object = builder.finish();
        let oid = col.put(&mut txn, None, object).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(5678);
        let object2 = builder.finish();
        col.put(&mut txn, Some(oid.clone()), object2).unwrap();

        let index = &col.indexes[0];
        let key = index.debug_create_keys(object2)[0].clone();
        assert_eq!(
            index.debug_dump(&mut txn),
            set![(key, oid.as_bytes().to_vec())],
        );
    }

    #[test]
    #[should_panic]
    fn test_put_fails_with_wrong_oid() {
        isar!(isar, col => col!(field1 => DataType::Int; ind!(field1)));

        let oid = ObjectId::from_long(1234, 12);

        let mut builder = col.new_object_builder(None);
        builder.write_int(12345);
        let object = builder.finish();

        let mut txn = isar.begin_txn(true).unwrap();
        col.put(&mut txn, Some(oid), object).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_put_fails_with_wrong_oid_type() {
        isar!(isar, col => col!(field1 => DataType::Int; ind!(field1)));

        let oid = ObjectId::from_str(col.id, "hello");

        let mut builder = col.new_object_builder(None);
        builder.write_int(12345);
        let object = builder.finish();

        let mut txn = isar.begin_txn(true).unwrap();
        col.put(&mut txn, Some(oid), object).unwrap();
    }

    #[test]
    fn test_put_calls_notifiers() {
        isar!(isar, col => col!(field1 => DataType::Int; ind!(field1)));

        let mut qb1 = col.new_query_builder();
        qb1.set_filter(IntBetween::filter(col.get_properties().first().unwrap().1, 1, 1).unwrap());
        let q1 = qb1.build();

        let mut qb2 = col.new_query_builder();
        qb2.set_filter(IntBetween::filter(col.get_properties().first().unwrap().1, 2, 2).unwrap());
        let q2 = qb2.build();

        let (tx1, rx1) = unbounded();
        let handle1 = isar.watch_query(col, q1, Box::new(move || tx1.send(true).unwrap()));

        let (tx2, rx2) = unbounded();
        let handle2 = isar.watch_query(col, q2, Box::new(move || tx2.send(true).unwrap()));

        let mut txn = isar.begin_txn(true).unwrap();
        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        let oid = col
            .put(&mut txn, None, builder.finish())
            .unwrap()
            .to_owned();
        txn.commit().unwrap();

        assert_eq!(rx1.len(), 1);
        assert_eq!(rx2.len(), 0);
        assert!(rx1.try_recv().unwrap());

        let mut txn = isar.begin_txn(true).unwrap();
        let mut builder = col.new_object_builder(None);
        builder.write_int(2);
        col.put(&mut txn, Some(oid), builder.finish()).unwrap();
        txn.commit().unwrap();

        assert_eq!(rx1.len(), 1);
        assert_eq!(rx2.len(), 1);
        handle1.stop();
        handle2.stop();
    }

    #[test]
    fn test_delete() {
        isar!(isar, col => col!(field1 => DataType::Int; ind!(field1)));

        let mut txn = isar.begin_txn(true).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(12345);
        let object = builder.finish();
        let oid = col.put(&mut txn, None, object).unwrap();

        let mut builder = col.new_object_builder(None);
        builder.write_int(54321);
        let object2 = builder.finish();
        let oid2 = col.put(&mut txn, None, object2).unwrap();

        eprintln!("{:?}", col.debug_dump(&mut txn));

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
        isar!(isar, col => col!(field1 => DataType::Int; ind!(field1)));

        let (tx, rx) = unbounded();
        let handle = isar.watch_collection(col, Box::new(move || tx.send(true).unwrap()));

        let mut txn = isar.begin_txn(true).unwrap();
        let mut builder = col.new_object_builder(None);
        builder.write_int(1234);
        col.put(&mut txn, None, builder.finish()).unwrap();
        txn.commit().unwrap();

        assert_eq!(rx.len(), 1);
        assert!(rx.try_recv().unwrap());
        handle.stop();
    }
}
