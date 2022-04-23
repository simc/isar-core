use crate::collection::IsarCollection;
use crate::cursor::IsarCursors;
use crate::error::{IsarError, Result};
use crate::link::IsarLink;
use crate::mdbx::cursor::{Cursor, UnboundCursor};
use crate::mdbx::db::Db;
use crate::mdbx::txn::Txn;
use crate::schema::collection_schema::CollectionSchema;
use crate::schema::index_schema::IndexSchema;
use crate::schema::link_schema::LinkSchema;
use crate::schema::Schema;
use itertools::Itertools;
use std::collections::HashMap;
use std::convert::TryInto;

const ISAR_VERSION: u64 = 1;
const INFO_VERSION_KEY: &[u8] = b"version";
const INFO_SCHEMA_KEY: &[u8] = b"schema";

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
            let existing_col = existing_schema.get_collection(&col.name);
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
        self.info_cursor.put(INFO_SCHEMA_KEY, &bytes)?;
        Ok(())
    }

    pub fn open_collections(&mut self, schema: &Schema) -> Result<Vec<IsarCollection>> {
        let cursors = IsarCursors::new(self.txn, vec![]);
        let mut cols = vec![];
        for col_schema in &schema.collections {
            let col = self.open_collection(schema, col_schema)?;
            col.init_auto_increment(&cursors)?;
            if let Some(new_indexes) = self.new_indexes.get(&col.name) {
                col.fill_indexes(new_indexes, &cursors)?;
            }
            cols.push(col);
        }
        Ok(cols)
    }

    fn open_collection(
        &mut self,
        schema: &Schema,
        col_schema: &CollectionSchema,
    ) -> Result<IsarCollection> {
        let db = self.open_collection_db(col_schema)?;
        let mut properties = col_schema.get_properties();
        properties.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut indexes = vec![];
        for index_schema in &col_schema.indexes {
            let db = self.open_index_db(col_schema, index_schema)?;
            let index = index_schema.as_index(db, &properties);
            indexes.push((index_schema.name.clone(), index));
        }
        indexes.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut links = vec![];
        for link_schema in &col_schema.links {
            let (link_db, backlink_db) = self.open_link_dbs(col_schema, link_schema)?;
            let target_col_schema = schema.get_collection(&link_schema.target_col).unwrap();
            let target_db = self.open_collection_db(target_col_schema)?;
            let link = IsarLink::new(link_db, backlink_db, db, target_db);
            links.push((link_schema.name.clone(), link));
        }
        // sort backlinks by name
        links.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut backlinks = vec![];
        for other_col_schema in &schema.collections {
            for link_schema in &other_col_schema.links {
                if link_schema.target_col == col_schema.name {
                    let other_col_db = self.open_collection_db(other_col_schema)?;
                    let (link_db, bl_db) = self.open_link_dbs(other_col_schema, link_schema)?;
                    let backlink = IsarLink::new(bl_db, link_db, db, other_col_db);
                    backlinks.push((&other_col_schema.name, &link_schema.name, backlink));
                }
            }
        }
        // sort backlinks by collection then by link name
        let backlinks = backlinks
            .into_iter()
            .sorted_by(|(col1, l1, _), (col2, l2, _)| col1.cmp(col2).then(l1.cmp(l2)))
            .map(|(_, _, link)| link)
            .collect_vec();

        Ok(IsarCollection::new(
            db,
            self.instance_id,
            col_schema.name.clone(),
            properties,
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
