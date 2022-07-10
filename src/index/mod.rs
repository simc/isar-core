use crate::cursor::IsarCursors;
use crate::error::{IsarError, Result};
use crate::index::index_key::IndexKey;
use crate::index::index_key_builder::IndexKeyBuilder;
use crate::mdbx::db::Db;
use crate::mdbx::debug_dump_db;
use crate::object::id::{BytesToId, IdToBytes};
use crate::object::isar_object::IsarObject;
use crate::object::property::Property;
use crate::schema::index_schema::IndexType;
use crate::txn::IsarTxn;
use std::collections::HashSet;

pub mod index_key;
pub(crate) mod index_key_builder;

#[derive(Clone, Eq, PartialEq)]
pub struct IndexProperty {
    pub property: Property,
    pub index_type: IndexType,
    pub case_sensitive: bool,
}

impl IndexProperty {
    pub(crate) fn new(property: Property, index_type: IndexType, case_sensitive: bool) -> Self {
        IndexProperty {
            property,
            index_type,
            case_sensitive,
        }
    }

    pub fn get_string_with_case(&self, object: IsarObject) -> Option<String> {
        object.read_string(self.property.offset).map(|str| {
            if self.case_sensitive {
                str.to_string()
            } else {
                str.to_lowercase()
            }
        })
    }

    fn is_multi_entry(&self) -> bool {
        self.property.data_type.get_element_type().is_some() && self.index_type != IndexType::Hash
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct IsarIndex {
    pub name: String,
    pub properties: Vec<IndexProperty>,
    pub unique: bool,
    pub replace: bool,
    pub multi_entry: bool,
    db: Db,
}

impl IsarIndex {
    pub const MAX_STRING_INDEX_SIZE: usize = 1024;

    pub fn new(
        name: String,
        db: Db,
        properties: Vec<IndexProperty>,
        unique: bool,
        replace: bool,
    ) -> Self {
        let multi_entry = properties.first().unwrap().is_multi_entry();
        IsarIndex {
            name,
            properties,
            unique,
            replace,
            multi_entry,
            db,
        }
    }

    pub fn create_for_object<F>(
        &self,
        cursors: &IsarCursors,
        id: i64,
        object: IsarObject,
        mut delete: F,
    ) -> Result<()>
    where
        F: FnMut(i64) -> Result<()>,
    {
        let mut cursor = cursors.get_cursor(self.db)?;
        let key_builder = IndexKeyBuilder::new(&self.properties);
        key_builder.create_keys(object, |key| {
            if self.unique {
                let existing = cursor.move_to(key)?;
                if let Some((_, existing_id_bytes)) = existing {
                    let existing_id = existing_id_bytes.to_id();
                    if self.replace && existing_id != id {
                        delete(existing_id)?;
                    } else {
                        return Err(IsarError::UniqueViolated {});
                    }
                }
            }
            cursor.put(key, &id.to_id_bytes())?;
            Ok(true)
        })?;

        Ok(())
    }

    pub fn delete_for_object(
        &self,
        cursors: &IsarCursors,
        id: i64,
        object: IsarObject,
    ) -> Result<()> {
        let mut cursor = cursors.get_cursor(self.db)?;
        let key_builder = IndexKeyBuilder::new(&self.properties);
        key_builder.create_keys(object, |key| {
            let entry = if self.unique {
                cursor.move_to(key)?
            } else {
                cursor.move_to_key_val(key, &id.to_id_bytes())?
            };
            if entry.is_some() {
                cursor.delete_current()?;
            }
            Ok(true)
        })?;
        Ok(())
    }

    pub fn iter_between<'txn, 'env>(
        &self,
        cursors: &IsarCursors<'txn, 'env>,
        lower_key: &IndexKey,
        upper_key: &IndexKey,
        skip_duplicates: bool,
        ascending: bool,
        mut callback: impl FnMut(i64) -> Result<bool>,
    ) -> Result<bool> {
        let mut cursor = cursors.get_cursor(self.db)?;
        cursor.iter_between(
            lower_key,
            upper_key,
            !self.unique,
            skip_duplicates,
            ascending,
            |_, _, id_bytes| callback(id_bytes.to_id()),
        )
    }

    pub fn get_id<'txn, 'env>(
        &self,
        cursors: &IsarCursors<'txn, 'env>,
        key: &IndexKey,
    ) -> Result<Option<i64>> {
        let mut result = None;
        self.iter_between(cursors, key, key, false, true, |id| {
            result = Some(id);
            Ok(false)
        })?;
        Ok(result)
    }

    pub fn get_size(&self, txn: &mut IsarTxn) -> Result<u64> {
        Ok(txn.db_stat(self.db)?.1)
    }

    pub fn clear(&self, txn: &mut IsarTxn) -> Result<()> {
        txn.clear_db(self.db)
    }

    pub fn debug_dump(&self, cursors: &IsarCursors) -> HashSet<(Vec<u8>, Vec<u8>)> {
        let mut cursor = cursors.get_cursor(self.db).unwrap();
        debug_dump_db(&mut cursor)
    }
}
