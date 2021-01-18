use crate::collection::IsarCollection;
use crate::error::{IsarError, Result};
use crate::lmdb::cursor::Cursor;
use crate::schema::collection_migrator::CollectionMigrator;
use crate::schema::Schema;
use crate::txn::Cursors;
use crate::watch::isar_watchers::IsarWatchers;
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Serializer};
use std::convert::TryInto;

const ISAR_VERSION: u64 = 1;
const INFO_VERSION_KEY: &[u8] = b"version";
const INFO_SCHEMA_KEY: &[u8] = b"schema";

pub(crate) struct SchemaManger<'env> {
    info_cursor: Cursor<'env>,
    cursors: Cursors<'env>,
    migration_cursors: Cursors<'env>,
}

impl<'env> SchemaManger<'env> {
    pub fn new(
        info_cursor: Cursor<'env>,
        cursors: Cursors<'env>,
        migration_cursors: Cursors<'env>,
    ) -> Self {
        SchemaManger {
            info_cursor,
            cursors,
            migration_cursors,
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
            let mut deser = Deserializer::from_slice(existing_schema_bytes);
            let existing_schema =
                Schema::deserialize(&mut deser).map_err(|e| IsarError::DbCorrupted {
                    source: Some(Box::new(e)),
                    message: "Could not deserialize existing schema.".to_string(),
                })?;
            schema.update_with_existing_schema(Some(&existing_schema));
            existing_schema.build_collections()
        } else {
            schema.update_with_existing_schema(None);
            vec![]
        };

        self.save_schema(&schema)?;
        let collections = schema.build_collections();
        self.perform_migration(&collections, &existing_collections)?;

        Ok(collections)
    }

    fn save_schema(&mut self, schema: &Schema) -> Result<()> {
        let mut bytes = vec![];
        let mut ser = Serializer::new(&mut bytes);
        schema
            .serialize(&mut ser)
            .map_err(|e| IsarError::MigrationError {
                source: Some(Box::new(e)),
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

        //let watchers = IsarWatchers::new();
        /*let mut change_set = ChangeSet::new(&watchers);
        for col in removed_collections {
            col.new_query_builder().build().delete_all_internal(
                &mut self.cursors,
                &mut self.migration_cursors,
                &mut change_set,
                col,
            )?;
        }*/

        for col in collections {
            let existing = existing_collections
                .iter()
                .find(|existing| existing.get_id() == col.get_id());

            if let Some(existing) = existing {
                let migrator = CollectionMigrator::create(col, existing);
                migrator.migrate(&mut self.cursors, &mut self.migration_cursors)?;
            }
        }

        Ok(())
    }
}
