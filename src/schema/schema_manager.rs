use crate::error::{IsarError, Result};
use crate::instance::IsarInstance;
use crate::lmdb::cursor::Cursor;
use crate::lmdb::{ByteKey, IntKey};
use crate::query::Sort;
use crate::schema::collection_migrator::CollectionMigrator;
use crate::schema::Schema;
use crate::txn::Cursors;
use crate::{collection::IsarCollection, query::id_where_clause::IdWhereClause};
use std::convert::TryInto;

const ISAR_VERSION: u64 = 1;
const INFO_VERSION_KEY: ByteKey = ByteKey::new(b"version");
const INFO_SCHEMA_KEY: ByteKey = ByteKey::new(b"schema");

pub(crate) struct SchemaManger<'env> {
    info_cursor: Cursor<'env>,
    cursors: Cursors<'env>,
    cursors2: Cursors<'env>,
}

impl<'env> SchemaManger<'env> {
    pub fn new(info_cursor: Cursor<'env>, cursors: Cursors<'env>, cursors2: Cursors<'env>) -> Self {
        SchemaManger {
            info_cursor,
            cursors,
            cursors2,
        }
    }

    pub fn check_isar_version(&mut self) -> Result<()> {
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

    pub fn get_collections(mut self, mut schema: Schema) -> Result<Vec<IsarCollection>> {
        let existing_schema_bytes = self.info_cursor.move_to(INFO_SCHEMA_KEY)?;

        let existing_collections = if let Some((_, existing_schema_bytes)) = existing_schema_bytes {
            let existing_schema = serde_json::from_slice(existing_schema_bytes).map_err(|e| {
                IsarError::DbCorrupted {
                    message: format!("Could not deserialize existing schema: {}", e),
                }
            })?;
            schema.update_with_existing_schema(Some(&existing_schema))?;
            existing_schema.build_collections()
        } else {
            schema.update_with_existing_schema(None)?;
            vec![]
        };

        self.save_schema(&schema)?;
        let collections = schema.build_collections();
        for collection in &collections {
            self.update_oid_counter(collection)?;
        }
        self.perform_migration(&collections, &existing_collections)?;

        Ok(collections)
    }

    fn update_oid_counter(&mut self, collection: &IsarCollection) -> Result<()> {
        let next_key = IntKey::new(collection.id + 1, IsarInstance::MIN_ID);
        let next_entry = self.cursors.data.move_to_gte(next_key)?;
        let greatest_qualifying_oid = if next_entry.is_some() {
            self.cursors.data.move_to_prev_key()?
        } else {
            self.cursors.data.move_to_last()?
        };

        if let Some((oid, _)) = greatest_qualifying_oid {
            let key = IntKey::from_bytes(oid);
            if key.get_prefix() == collection.id {
                collection.update_auto_increment(key.get_id());
            }
        }
        Ok(())
    }

    fn save_schema(&mut self, schema: &Schema) -> Result<()> {
        let bytes = serde_json::to_vec(schema).map_err(|_| IsarError::SchemaError {
            message: "Could not serialize schema.".to_string(),
        })?;
        self.info_cursor.put(INFO_SCHEMA_KEY, &bytes)?;
        Ok(())
    }

    fn perform_migration(
        &mut self,
        collections: &[IsarCollection],
        existing_collections: &[IsarCollection],
    ) -> Result<()> {
        let removed_collections = existing_collections
            .iter()
            .filter(|existing| !collections.iter().any(|c| existing.id == c.id));

        for col in removed_collections {
            for index in &col.indexes {
                index.clear(&mut self.cursors)?;
            }
            IdWhereClause::new(
                col,
                IsarInstance::MIN_ID,
                IsarInstance::MAX_ID,
                Sort::Ascending,
            )
            .iter(&mut self.cursors.data, None, |c, _, _| {
                c.delete_current()?;
                Ok(true)
            })?;
        }

        for col in collections {
            let existing = existing_collections
                .iter()
                .find(|existing| existing.id == col.id);

            if let Some(existing) = existing {
                let migrator = CollectionMigrator::create(col, existing);
                migrator.migrate(&mut self.cursors, &mut self.cursors2)?;
            }
        }

        Ok(())
    }
}
