use crate::cursor::IsarCursors;
use crate::error::Result;
use crate::key::{IdKey, IndexKey};
use crate::mdbx::db::Db;
use crate::mdbx::debug_dump_db;
use crate::object::data_type::DataType;
use crate::object::isar_object::{IsarObject, Property};
use crate::schema::index_schema::IndexType;
use crate::txn::IsarTxn;
use std::collections::HashSet;

#[derive(Copy, Clone, Eq, PartialEq)]
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
        object.read_string(self.property).map(|str| {
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
    pub properties: Vec<IndexProperty>,
    pub unique: bool,
    pub multi_entry: bool,
    db: Db,
}

impl IsarIndex {
    pub const MAX_STRING_INDEX_SIZE: usize = 1024;

    pub fn new(db: Db, properties: Vec<IndexProperty>, unique: bool) -> Self {
        let multi_entry = properties.first().unwrap().is_multi_entry();
        IsarIndex {
            properties,
            unique,
            multi_entry,
            db,
        }
    }

    pub fn create_for_object<F>(
        &self,
        cursors: &IsarCursors,
        id_key: &IdKey,
        object: IsarObject,
        mut on_conflict: F,
    ) -> Result<()>
    where
        F: FnMut(&IdKey) -> Result<bool>,
    {
        let mut cursor = cursors.get_cursor(self.db)?;
        self.create_keys(object, |key| {
            if self.unique {
                let existing = cursor.move_to(key.as_bytes())?;
                if let Some((_, existing_key)) = existing {
                    on_conflict(&IdKey::from_bytes(existing_key))?;
                }
            }
            cursor.put(key.as_bytes(), id_key.as_bytes())?;
            Ok(true)
        })?;
        Ok(())
    }

    pub fn delete_for_object(
        &self,
        cursors: &IsarCursors,
        id_key: &IdKey,
        object: IsarObject,
    ) -> Result<()> {
        let mut cursor = cursors.get_cursor(self.db)?;
        self.create_keys(object, |key| {
            let entry = if self.unique {
                cursor.move_to(key.as_bytes())?
            } else {
                cursor.move_to_key_val(key.as_bytes(), id_key.as_bytes())?
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
        mut callback: impl FnMut(IdKey<'txn>) -> Result<bool>,
    ) -> Result<bool> {
        let mut cursor = cursors.get_cursor(self.db)?;
        cursor.iter_between(
            lower_key.as_bytes(),
            upper_key.as_bytes(),
            !self.unique,
            skip_duplicates,
            ascending,
            |_, _, id_key| callback(IdKey::from_bytes(id_key)),
        )
    }

    pub fn get_id<'txn, 'env>(
        &self,
        cursors: &IsarCursors<'txn, 'env>,
        key: &IndexKey,
    ) -> Result<Option<IdKey<'txn>>> {
        let mut result = None;
        self.iter_between(cursors, key, key, false, true, |id_key| {
            result = Some(id_key);
            Ok(false)
        })?;
        Ok(result)
    }

    pub fn clear(&self, txn: &mut IsarTxn) -> Result<()> {
        txn.clear_db(self.db)
    }

    pub fn create_keys(
        &self,
        object: IsarObject,
        mut callback: impl FnMut(&IndexKey) -> Result<bool>,
    ) -> Result<bool> {
        let first = self.properties.first().unwrap();
        if first.property.data_type.get_element_type().is_none()
            || first.index_type == IndexType::Hash
        {
            let key = Self::create_primitive_key(&self.properties, object);
            callback(&key)?;
            Ok(true)
        } else {
            Self::create_list_keys(first, object, &mut callback)
        }
    }

    fn create_primitive_key(properties: &[IndexProperty], object: IsarObject) -> IndexKey {
        let mut key = IndexKey::new();
        for index_property in properties {
            let property = index_property.property;

            if index_property.index_type == IndexType::Hash {
                let hash = object.hash_property(property, index_property.case_sensitive, 0);
                key.add_hash(hash);
            } else {
                match property.data_type {
                    DataType::Byte => key.add_byte(object.read_byte(property)),
                    DataType::Int => key.add_int(object.read_int(property)),
                    DataType::Float => key.add_float(object.read_float(property)),
                    DataType::Long => key.add_long(object.read_long(property)),
                    DataType::Double => key.add_double(object.read_double(property)),
                    DataType::String => {
                        key.add_string(object.read_string(property), index_property.case_sensitive)
                    }
                    _ => unreachable!(),
                }
            }
        }
        key
    }

    fn create_list_keys(
        index_property: &IndexProperty,
        object: IsarObject,
        mut callback: impl FnMut(&IndexKey) -> Result<bool>,
    ) -> Result<bool> {
        let mut key = IndexKey::new();
        let property = index_property.property;
        if object.is_null(property) {
            return Ok(true);
        }
        match property.data_type {
            DataType::ByteList => {
                for value in object.read_byte_list(property).unwrap() {
                    key.truncate(0);
                    key.add_byte(*value);
                    if !callback(&key)? {
                        return Ok(false);
                    }
                }
            }
            DataType::IntList => {
                for value in object.read_int_list(property).unwrap() {
                    key.truncate(0);
                    key.add_int(value);
                    if !callback(&key)? {
                        return Ok(false);
                    }
                }
            }
            DataType::LongList => {
                for value in object.read_long_list(property).unwrap() {
                    key.truncate(0);
                    key.add_long(value);
                    if !callback(&key)? {
                        return Ok(false);
                    }
                }
            }
            DataType::FloatList => {
                for value in object.read_float_list(property).unwrap() {
                    key.truncate(0);
                    key.add_float(value);
                    if !callback(&key)? {
                        return Ok(false);
                    }
                }
            }
            DataType::DoubleList => {
                for value in object.read_double_list(property).unwrap() {
                    key.truncate(0);
                    key.add_double(value);
                    if !callback(&key)? {
                        return Ok(false);
                    }
                }
            }
            DataType::StringList => {
                if index_property.index_type == IndexType::HashElements {
                    for value in object.read_string_list(property).unwrap() {
                        key.truncate(0);
                        let hash = IsarObject::hash_string(value, index_property.case_sensitive, 0);
                        key.add_hash(hash);
                        if !callback(&key)? {
                            return Ok(false);
                        }
                    }
                } else {
                    for value in object.read_string_list(property).unwrap() {
                        key.truncate(0);
                        key.add_string(value, index_property.case_sensitive);
                        if !callback(&key)? {
                            return Ok(false);
                        }
                    }
                }
            }
            _ => unreachable!(),
        }
        Ok(true)
    }

    pub fn debug_dump(&self, cursors: &IsarCursors) -> HashSet<(Vec<u8>, Vec<u8>)> {
        let mut cursor = cursors.get_cursor(self.db).unwrap();
        debug_dump_db(&mut cursor, false)
    }
}
