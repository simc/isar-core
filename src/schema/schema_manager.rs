use crate::collection::IsarCollection;
use crate::error::{IsarError, Result};
use crate::lmdb::cursor::Cursor;
use crate::lmdb::Key;
use crate::query::Sort;
use crate::schema::collection_migrator::CollectionMigrator;
use crate::schema::Schema;
use crate::txn::Cursors;
use crate::utils::oid_from_bytes;
use std::convert::TryInto;

const ISAR_VERSION: u64 = 1;
const INFO_VERSION_KEY: Key = Key(b"version");
const INFO_SCHEMA_KEY: Key = Key(b"schema");

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
                    message: format!("Could not deserialize existing schema: {}", e).to_string(),
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
        let id = collection.get_id();
        let next_prefix = (id + 1).to_be_bytes();
        let next_entry = self.cursors.primary.move_to_gte(Key(&next_prefix))?;
        let greatest_qualifying_oid = if next_entry.is_some() {
            self.cursors.primary.move_to_prev_key()?
        } else {
            self.cursors.primary.move_to_last()?
        };

        if let Some((oid, _)) = greatest_qualifying_oid {
            let (oid, prefix) = oid_from_bytes(oid.0);
            if prefix == id {
                collection.update_oid_counter(oid);
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
            .filter(|existing| !collections.iter().any(|c| existing.get_id() == c.get_id()));

        for col in removed_collections {
            for index in col.get_indexes() {
                index.clear(&mut self.cursors)?;
            }
            col.new_primary_where_clause(None, None, Sort::Ascending)?
                .iter(&mut self.cursors, |c, _, _| {
                    c.primary.delete_current()?;
                    Ok(true)
                })?;
        }

        for col in collections {
            let existing = existing_collections
                .iter()
                .find(|existing| existing.get_id() == col.get_id());

            if let Some(existing) = existing {
                let migrator = CollectionMigrator::create(col, existing);
                migrator.migrate(&mut self.cursors, &mut self.cursors2)?;
            }
        }

        Ok(())
    }
}
