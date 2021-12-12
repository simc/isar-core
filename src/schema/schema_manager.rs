use crate::collection::IsarCollection;
use crate::error::{IsarError, Result};
use crate::link::IsarLink;
use crate::mdbx::cursor::{Cursor, UnboundCursor};
use crate::mdbx::db::Db;
use crate::mdbx::txn::Txn;
use crate::schema::collection_schema::CollectionSchema;
use crate::schema::index_schema::IndexSchema;
use crate::schema::link_schema::LinkSchema;
use crate::schema::Schema;
use std::convert::TryInto;
use std::hash::Hasher;
use xxhash_rust::xxh3::Xxh3;

const ISAR_VERSION: u64 = 1;
const INFO_VERSION_KEY: &[u8] = b"version";
const INFO_SCHEMA_KEY: &[u8] = b"schema";

pub(crate) struct SchemaManger<'a> {
    instance_id: u64,
    txn: &'a Txn<'a>,
    info_cursor: Cursor<'a>,
    hasher: Xxh3,
}

impl<'a> SchemaManger<'a> {
    pub fn create(instance_id: u64, txn: &'a Txn<'a>) -> Result<Self> {
        let info_db = Db::open(txn, "_info", false, false, false)?;
        let info_cursor = UnboundCursor::new();
        let mut manager = SchemaManger {
            instance_id,
            txn,
            info_cursor: info_cursor.bind(txn, info_db)?,
            hasher: Xxh3::new(),
        };
        manager.check_isar_version()?;
        Ok(manager)
    }

    fn check_isar_version(&mut self) -> Result<()> {
        let version = self.info_cursor.move_to(INFO_VERSION_KEY)?;
        if let Some((_, version)) = version {
            let version_num = u64::from_le_bytes(version.try_into().unwrap());
            if version_num != ISAR_VERSION {
                return Err(IsarError::VersionError {});
            }
        } else {
            let version_bytes = &ISAR_VERSION.to_le_bytes();
            self.info_cursor.put(INFO_VERSION_KEY, version_bytes)?;
        }
        Ok(())
    }

    fn get_existing_schema(&mut self) -> Result<Schema> {
        let existing_schema_bytes = self.info_cursor.move_to(INFO_SCHEMA_KEY)?;

        if let Some((_, existing_schema_bytes)) = existing_schema_bytes {
            serde_json::from_slice(existing_schema_bytes).map_err(|e| IsarError::DbCorrupted {
                message: format!("Could not deserialize existing schema: {}", e),
            })
        } else {
            Schema::new(vec![])
        }
    }

    fn get_db_name(
        &mut self,
        col: &CollectionSchema,
        index: Option<&IndexSchema>,
        link: Option<&LinkSchema>,
    ) -> String {
        self.hasher.reset();
        self.hasher.write(col.name.as_bytes());
        if let Some(index) = index {
            self.hasher.write(b"index");
            for p in &index.properties {
                self.hasher.write(p.name.as_bytes());
            }
        } else if let Some(link) = link {
            self.hasher.write(b"link");
            self.hasher.write(link.name.as_bytes());
        }
        let hash = self.hasher.finish();
        hash.to_string()
    }

    fn open_collection_db(&mut self, col: &CollectionSchema) -> Result<Db> {
        let db_name = self.get_db_name(col, None, None);
        Db::open(self.txn, &db_name, true, false, false)
    }

    fn open_index_db(&mut self, col: &CollectionSchema, index: &IndexSchema) -> Result<Db> {
        let db_name = self.get_db_name(col, Some(index), None);
        Db::open(self.txn, &db_name, false, !index.unique, false)
    }

    fn open_link_db(&mut self, col: &CollectionSchema, link: &LinkSchema) -> Result<Db> {
        let db_name = self.get_db_name(col, None, Some(link));
        Db::open(self.txn, &db_name, true, false, true)
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
        let db = self.open_link_db(col, link)?;
        db.drop(self.txn)
    }

    pub fn perform_migration(&mut self, schema: &mut Schema) -> Result<()> {
        let existing_schema = self.get_existing_schema()?;

        let deleted_cols = get_added(&schema.collections, &existing_schema.collections);
        for col in deleted_cols {
            self.delete_collection(col)?;
        }

        for col in schema.collections.iter_mut() {
            let existing_col = existing_schema.get_collection(&col.name);
            if let Some(existing_col) = existing_col {
                col.merge_properties(existing_col)?;

                let added_indexes = get_added(&existing_col.indexes, &col.indexes);
                for index in added_indexes {
                    let db = self.open_index_db(&col, index)?;
                    // todo create index
                    // don't close index dbi
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
        self.info_cursor.put(INFO_SCHEMA_KEY, &bytes)?;
        Ok(())
    }

    pub fn open_collections(&mut self, schema: &Schema) -> Result<Vec<IsarCollection>> {
        let mut cols = vec![];
        for col_schema in &schema.collections {
            cols.push(self.open_collection(schema, col_schema)?);
        }
        Ok(cols)
    }

    fn open_collection(
        &mut self,
        schema: &Schema,
        collection_schema: &CollectionSchema,
    ) -> Result<IsarCollection> {
        let db = self.open_collection_db(collection_schema)?;
        let (properties, property_names) = collection_schema.get_properties();

        let mut indexes = vec![];
        for index_schema in &collection_schema.indexes {
            let db = self.open_index_db(collection_schema, index_schema)?;
            let index = index_schema.as_index(db, &properties, &property_names);
            indexes.push(index);
        }

        let mut links = vec![];
        for link_schema in &collection_schema.links {
            let link_db = self.open_link_db(collection_schema, link_schema)?;
            let target_col_schema = schema.get_collection(&link_schema.target_col).unwrap();
            let target_db = self.open_collection_db(target_col_schema)?;
            let link = IsarLink::new(link_db, db, target_db);
            links.push((link_schema.name.clone(), link));
        }

        let mut backlinks = vec![];
        for other_col_schema in &schema.collections {
            if collection_schema.name != other_col_schema.name {
                for link_schema in &other_col_schema.links {
                    if link_schema.target_col == collection_schema.name {
                        let other_col_id = self.open_collection_db(other_col_schema)?;
                        let link_db = self.open_link_db(other_col_schema, link_schema)?;
                        let link = IsarLink::new(link_db, other_col_id, db);
                        backlinks.push(link);
                    }
                }
            }
        }

        Ok(IsarCollection::new(
            db,
            self.instance_id,
            collection_schema.name.clone(),
            properties,
            property_names,
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
