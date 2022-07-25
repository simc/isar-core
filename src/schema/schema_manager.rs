use crate::collection::IsarCollection;
use crate::cursor::IsarCursors;
use crate::error::{IsarError, Result};
use crate::index::index_key::IndexKey;
use crate::legacy::isar_object_v1::{LegacyIsarObject, LegacyProperty};
use crate::link::IsarLink;
use crate::mdbx::cursor::{Cursor, UnboundCursor};
use crate::mdbx::db::Db;
use crate::mdbx::txn::Txn;
use crate::object::data_type::DataType;
use crate::object::id::BytesToId;
use crate::object::isar_object::IsarObject;
use crate::object::object_builder::ObjectBuilder;
use crate::object::property::Property;
use crate::schema::collection_schema::CollectionSchema;
use crate::schema::index_schema::IndexSchema;
use crate::schema::link_schema::LinkSchema;
use crate::schema::Schema;
use intmap::IntMap;
use itertools::Itertools;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Deref;
use xxhash_rust::xxh3::xxh3_64;

const ISAR_VERSION: u64 = 2;

static INFO_VERSION_KEY: Lazy<IndexKey> = Lazy::new(|| {
    let mut key = IndexKey::new();
    key.add_string(Some("version"), true);
    key
});

static INFO_SCHEMA_KEY: Lazy<IndexKey> = Lazy::new(|| {
    let mut key = IndexKey::new();
    key.add_string(Some("schema"), true);
    key
});

pub(crate) struct SchemaManger<'a> {
    instance_id: u64,
    txn: &'a Txn<'a>,
    info_cursor: Cursor<'a>,
    new_indexes: HashMap<String, Vec<usize>>,
}

impl<'a> SchemaManger<'a> {
    pub fn create(instance_id: u64, txn: &'a Txn<'a>) -> Result<Self> {
        let info_db = Db::open(txn, Some("_info"), false, false, false)?;
        let info_cursor = UnboundCursor::new();
        let mut manager = SchemaManger {
            instance_id,
            txn,
            info_cursor: info_cursor.bind(txn, info_db)?,
            new_indexes: HashMap::new(),
        };
        manager.check_isar_version()?;
        Ok(manager)
    }

    fn check_isar_version(&mut self) -> Result<()> {
        let version = self.info_cursor.move_to(INFO_VERSION_KEY.deref())?;
        if let Some((_, version)) = version {
            let version_num = u64::from_le_bytes(version.deref().try_into().unwrap());
            if version_num == 1 {
                self.migrate_v1()?;
            } else if version_num != ISAR_VERSION {
                return Err(IsarError::VersionError {});
            } else {
                return Ok(());
            }
        }

        let version_bytes = &ISAR_VERSION.to_le_bytes();
        self.info_cursor
            .put(INFO_VERSION_KEY.deref(), version_bytes)?;

        Ok(())
    }

    fn migrate_v1(&mut self) -> Result<()> {
        let mut schema = self.get_existing_schema()?;

        let cursors = IsarCursors::new(self.txn, vec![]);
        let mut buffer = Some(vec![]);
        for col in schema.collections.iter_mut() {
            for index in &col.indexes {
                let index_db = self.open_index_db(col, index)?;
                index_db.clear(self.txn)?;
            }
            col.indexes.clear();

            let props = col.get_properties();

            let mut offset = 2;
            let legacy_props = col
                .properties
                .iter()
                .map(|p| {
                    let property = LegacyProperty::new(p.data_type, offset);
                    offset += match p.data_type {
                        DataType::Byte => 1,
                        DataType::Int | DataType::Float => 4,
                        _ => 8,
                    };

                    property
                })
                .collect_vec();

            let db = self.open_collection_db(col)?;
            let mut db_cursor = cursors.get_cursor(db)?;
            db_cursor.iter_all(false, true, |cursor, id_bytes, obj| {
                // We need to copy the data here because it will become invalid during the write
                let id = id_bytes.to_id();
                let obj = obj.to_vec();

                let legacy_object = LegacyIsarObject::from_bytes(&obj);
                let mut new_object = ObjectBuilder::new(&props, buffer.take());
                for (prop, legacy_prop) in props.iter().zip(&legacy_props) {
                    match prop.data_type {
                        DataType::Bool => {
                            if legacy_object.is_null(*legacy_prop) {
                                new_object.write_bool(prop.offset, None);
                            } else {
                                new_object.write_bool(
                                    prop.offset,
                                    Some(legacy_object.read_bool(*legacy_prop)),
                                )
                            }
                        }
                        DataType::Byte => new_object
                            .write_byte(prop.offset, legacy_object.read_byte(*legacy_prop)),
                        DataType::Int => {
                            new_object.write_int(prop.offset, legacy_object.read_int(*legacy_prop))
                        }
                        DataType::Float => new_object
                            .write_float(prop.offset, legacy_object.read_float(*legacy_prop)),
                        DataType::Long => new_object
                            .write_long(prop.offset, legacy_object.read_long(*legacy_prop)),
                        DataType::Double => new_object
                            .write_double(prop.offset, legacy_object.read_double(*legacy_prop)),
                        DataType::String => new_object
                            .write_string(prop.offset, legacy_object.read_string(*legacy_prop)),
                        DataType::BoolList => {
                            let byte_list = legacy_object.read_byte_list(*legacy_prop);
                            let bool_list = byte_list.map(|bytes| {
                                bytes
                                    .into_iter()
                                    .map(|b| IsarObject::byte_to_bool(*b))
                                    .collect_vec()
                            });
                            new_object.write_bool_list(prop.offset, bool_list.as_deref())
                        }
                        DataType::ByteList => new_object.write_byte_list(
                            prop.offset,
                            legacy_object.read_byte_list(*legacy_prop),
                        ),
                        DataType::IntList => new_object.write_int_list(
                            prop.offset,
                            legacy_object.read_int_list(*legacy_prop).as_deref(),
                        ),
                        DataType::FloatList => new_object.write_float_list(
                            prop.offset,
                            legacy_object.read_float_list(*legacy_prop).as_deref(),
                        ),
                        DataType::LongList => new_object.write_long_list(
                            prop.offset,
                            legacy_object.read_long_list(*legacy_prop).as_deref(),
                        ),
                        DataType::DoubleList => new_object.write_double_list(
                            prop.offset,
                            legacy_object.read_double_list(*legacy_prop).as_deref(),
                        ),
                        DataType::StringList => new_object.write_string_list(
                            prop.offset,
                            legacy_object.read_string_list(*legacy_prop).as_deref(),
                        ),
                        _ => unreachable!(),
                    }
                }

                cursor.put(&id, new_object.finish().as_bytes())?;
                buffer.replace(new_object.recycle());
                Ok(true)
            })?;
        }

        self.save_schema(&schema)?;

        Ok(())
    }

    fn get_existing_schema(&mut self) -> Result<Schema> {
        let existing_schema_bytes = self.info_cursor.move_to(INFO_SCHEMA_KEY.deref())?;

        if let Some((_, existing_schema_bytes)) = existing_schema_bytes {
            serde_json::from_slice(&existing_schema_bytes).map_err(|e| IsarError::DbCorrupted {
                message: format!("Could not deserialize existing schema: {}", e),
            })
        } else {
            Schema::new(vec![])
        }
    }

    fn open_collection_db(&mut self, col: &CollectionSchema) -> Result<Db> {
        Db::open(self.txn, Some(&col.name), true, false, false)
    }

    fn open_index_db(&mut self, col: &CollectionSchema, index: &IndexSchema) -> Result<Db> {
        let db_name = format!("_i_{}_{}", col.name, index.name);
        Db::open(self.txn, Some(&db_name), false, !index.unique, false)
    }

    fn open_link_dbs(&mut self, col: &CollectionSchema, link: &LinkSchema) -> Result<(Db, Db)> {
        let link_db_name = format!("_l_{}_{}", col.name, link.name);
        let db = Db::open(self.txn, Some(&link_db_name), true, true, true)?;
        let backlink_db_name = format!("_b_{}_{}", col.name, link.name);
        let bl_db = Db::open(self.txn, Some(&backlink_db_name), true, true, true)?;
        Ok((db, bl_db))
    }

    fn delete_collection(&mut self, col: &CollectionSchema) -> Result<()> {
        let db = self.open_collection_db(col)?;
        db.drop(self.txn)?;
        for index in &col.indexes {
            self.delete_index(col, index)?;
        }
        for link in &col.links {
            self.delete_link(col, link)?;
        }
        Ok(())
    }

    fn delete_index(&mut self, col: &CollectionSchema, index: &IndexSchema) -> Result<()> {
        let db = self.open_index_db(col, index)?;
        db.drop(self.txn)
    }

    fn delete_link(&mut self, col: &CollectionSchema, link: &LinkSchema) -> Result<()> {
        let (db, bl_db) = self.open_link_dbs(col, link)?;
        db.drop(self.txn)?;
        bl_db.drop(self.txn)
    }

    pub fn perform_migration(&mut self, schema: &mut Schema) -> Result<()> {
        let existing_schema = self.get_existing_schema()?;

        let deleted_cols = get_added(&schema.collections, &existing_schema.collections);
        for col in deleted_cols {
            self.delete_collection(col)?;
        }

        for col in schema.collections.iter_mut() {
            let existing_col = existing_schema.get_collection(&col.name, col.embedded);
            if let Some(existing_col) = existing_col {
                col.merge_properties(existing_col)?;

                let added_indexes = get_added(&existing_col.indexes, &col.indexes)
                    .iter()
                    .map(|new_i| col.indexes.iter().position(|i| i == *new_i).unwrap())
                    .collect_vec();
                if !added_indexes.is_empty() {
                    self.new_indexes.insert(col.name.clone(), added_indexes);
                }

                let deleted_indexes = get_added(&col.indexes, &existing_col.indexes);
                for index in deleted_indexes {
                    self.delete_index(existing_col, index)?;
                }

                let deleted_links = get_added(&existing_col.links, &col.links);
                for link in deleted_links {
                    self.delete_link(existing_col, link)?;
                }
            }
        }

        self.save_schema(schema)?;

        Ok(())
    }

    fn save_schema(&mut self, schema: &Schema) -> Result<()> {
        let bytes = serde_json::to_vec(schema).map_err(|_| IsarError::SchemaError {
            message: "Could not serialize schema.".to_string(),
        })?;
        self.info_cursor.put(INFO_SCHEMA_KEY.deref(), &bytes)?;
        Ok(())
    }

    pub fn open_collections(&mut self, schema: &Schema) -> Result<Vec<IsarCollection>> {
        let cursors = IsarCursors::new(self.txn, vec![]);
        let mut cols = vec![];
        for col_schema in schema.collections.iter().filter(|c| !c.embedded) {
            let col = self.open_collection(schema, col_schema)?;
            col.init_auto_increment(&cursors)?;
            if let Some(new_indexes) = self.new_indexes.get(&col.name) {
                col.fill_indexes(new_indexes, &cursors)?;
            }
            cols.push(col);
        }
        Ok(cols)
    }

    fn get_embedded_properties(
        schema: &Schema,
        properties: &[Property],
        embedded_properties: &mut IntMap<Vec<Property>>,
    ) {
        for property in properties {
            if let Some(target_id) = property.target_id {
                if !embedded_properties.contains_key(target_id) {
                    let embedded_col_schema = schema
                        .collections
                        .iter()
                        .find(|c| xxh3_64(c.name.as_bytes()) == target_id)
                        .unwrap();
                    let properties = embedded_col_schema.get_properties();
                    embedded_properties.insert(target_id, properties.clone());
                    Self::get_embedded_properties(schema, &properties, embedded_properties)
                }
            }
        }
    }

    fn open_collection(
        &mut self,
        schema: &Schema,
        col_schema: &CollectionSchema,
    ) -> Result<IsarCollection> {
        let db = self.open_collection_db(col_schema)?;
        let properties = col_schema.get_properties();

        let mut embedded_properties = IntMap::new();
        Self::get_embedded_properties(schema, &properties, &mut embedded_properties);

        let mut indexes = vec![];
        for index_schema in &col_schema.indexes {
            let db = self.open_index_db(col_schema, index_schema)?;
            let index = index_schema.as_index(db, &properties);
            indexes.push(index);
        }

        let mut links = vec![];
        for link_schema in &col_schema.links {
            let (link_db, backlink_db) = self.open_link_dbs(col_schema, link_schema)?;
            let target_col_schema = schema
                .get_collection(&link_schema.target_col, false)
                .unwrap();
            let target_db = self.open_collection_db(target_col_schema)?;
            let link = IsarLink::new(
                &col_schema.name,
                &link_schema.name,
                false,
                link_db,
                backlink_db,
                db,
                target_db,
            );
            links.push(link);
        }

        let mut backlinks = vec![];
        for other_col_schema in &schema.collections {
            for link_schema in &other_col_schema.links {
                if link_schema.target_col == col_schema.name {
                    let other_col_db = self.open_collection_db(other_col_schema)?;
                    let (link_db, bl_db) = self.open_link_dbs(other_col_schema, link_schema)?;
                    let backlink = IsarLink::new(
                        &other_col_schema.name,
                        &link_schema.name,
                        true,
                        bl_db,
                        link_db,
                        db,
                        other_col_db,
                    );
                    backlinks.push(backlink);
                }
            }
        }

        Ok(IsarCollection::new(
            db,
            self.instance_id,
            &col_schema.name,
            properties,
            embedded_properties,
            indexes,
            links,
            backlinks,
        ))
    }
}

fn get_added<'a, E>(left: &'a [E], right: &'a [E]) -> Vec<&'a E>
where
    E: Eq,
{
    let mut added_items = vec![];
    for right_item in right {
        let has_left = left.iter().any(|i| i == right_item);
        if !has_left {
            added_items.push(right_item);
        }
    }
    added_items
}
