use crate::cursor::IsarCursors;
use crate::error::{IsarError, Result};
use crate::key::{IdKey, IndexKey};
use crate::mdbx::db::Db;
use crate::object::data_type::DataType;
use crate::object::isar_object::{IsarObject, Property};
use crate::schema::index_schema::IndexType;
use crate::txn::IsarTxn;
use itertools::Itertools;
use unicode_segmentation::UnicodeSegmentation;

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct IndexProperty {
    pub property: Property,
    pub index_type: IndexType,
    pub case_sensitive: Option<bool>,
}

impl IndexProperty {
    pub(crate) fn new(
        property: Property,
        index_type: IndexType,
        case_sensitive: Option<bool>,
    ) -> Self {
        IndexProperty {
            property,
            index_type,
            case_sensitive,
        }
    }

    pub fn get_string_with_case(&self, object: IsarObject) -> Option<String> {
        object.read_string(self.property).map(|str| {
            if self.case_sensitive.unwrap() {
                str.to_string()
            } else {
                str.to_lowercase()
            }
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct IsarIndex {
    pub properties: Vec<IndexProperty>,
    pub unique: bool,
    pub replace: bool,
    db: Db,
}

impl IsarIndex {
    pub const MAX_STRING_INDEX_SIZE: usize = 1024;

    pub fn new(db: Db, properties: Vec<IndexProperty>, unique: bool, replace: bool) -> Self {
        IsarIndex {
            properties,
            unique,
            replace,
            db,
        }
    }

    pub fn create_for_object<F>(
        &self,
        cursors: &IsarCursors,
        id_key: &IdKey,
        object: IsarObject,
        mut delete_existing: F,
    ) -> Result<()>
    where
        F: FnMut(&IdKey) -> Result<()>,
    {
        let mut cursor = cursors.get_cursor(self.db)?;
        self.create_keys(object, |key| {
            if self.unique {
                let success = cursor.put_no_override(key.as_bytes(), id_key.as_bytes())?;
                if !success {
                    if self.replace {
                        delete_existing(id_key)?;
                        cursor.put(key.as_bytes(), id_key.as_bytes())?;
                    } else {
                        return Err(IsarError::UniqueViolated {});
                    }
                }
            } else {
                cursor.put(key.as_bytes(), id_key.as_bytes())?;
            }
            Ok(true)
        })
    }

    pub fn delete_for_object(
        &self,
        cursors: &IsarCursors,
        id_key: &IdKey,
        object: IsarObject,
    ) -> Result<()> {
        let mut cursor = cursors.get_cursor(self.db)?;
        self.create_keys(object, |key| {
            let entry = cursor.move_to_key_val(key.as_bytes(), id_key.as_bytes())?;
            if entry.is_some() {
                cursor.delete_current()?;
            }
            Ok(true)
        })
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
            skip_duplicates,
            ascending,
            |_, id_key| callback(IdKey::from_bytes(id_key)),
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
    ) -> Result<()> {
        let mut key = IndexKey::new();
        Self::fill_single_key(&mut key, &self.properties, object);

        let last_property = self.properties.last().unwrap();
        if last_property.index_type == IndexType::Words {
            let mut result = Ok(());
            Self::fill_word_keys(&mut key, *last_property, object, |bytes| {
                match callback(bytes) {
                    Ok(cont) => cont,
                    Err(err) => {
                        result = Err(err);
                        false
                    }
                }
            });
            result
        } else {
            callback(&key)?;
            Ok(())
        }
    }

    fn fill_single_key(key: &mut IndexKey, properties: &[IndexProperty], object: IsarObject) {
        for ip in properties {
            match ip.property.data_type {
                DataType::Byte => {
                    let value = object.read_byte(ip.property);
                    key.add_byte(value);
                }
                DataType::Int => {
                    let value = object.read_int(ip.property);
                    key.add_int(value);
                }
                DataType::Long => {
                    let value = object.read_long(ip.property);
                    key.add_long(value);
                }
                DataType::Float => {
                    let value = object.read_float(ip.property);
                    key.add_float(value);
                }
                DataType::Double => {
                    let value = object.read_double(ip.property);
                    key.add_double(value);
                }
                DataType::String => {
                    let value = object.read_string(ip.property);
                    match ip.index_type {
                        IndexType::Value => key.add_string_value(value, ip.case_sensitive.unwrap()),
                        IndexType::Hash => key.add_string_hash(value, ip.case_sensitive.unwrap()),
                        _ => {}
                    }
                }
                _ => unimplemented!(),
            }
        }
    }

    fn fill_word_keys(
        key: &mut IndexKey,
        property: IndexProperty,
        object: IsarObject,
        mut callback: impl FnMut(&IndexKey) -> bool,
    ) {
        let key_len = key.len();
        let value = property.get_string_with_case(object);
        if let Some(str) = value {
            for word in str.unicode_words().unique() {
                key.truncate(key_len);
                key.add_string_word(word, property.case_sensitive.unwrap());
                if !callback(key) {
                    break;
                }
            }
        }
    }
}
