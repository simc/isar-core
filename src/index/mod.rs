use crate::error::{IsarError, Result};
use crate::index::index_key::IndexKey;
use crate::lmdb::{ByteKey, IntKey, Key};
use crate::object::data_type::DataType;
use crate::object::isar_object::{IsarObject, Property};
use crate::query::index_where_clause::IndexWhereClause;
use crate::query::Sort;
use crate::schema::collection_schema::IndexType;
use crate::txn::Cursors;
use itertools::Itertools;
use unicode_segmentation::UnicodeSegmentation;

#[cfg(test)]
use {crate::txn::IsarTxn, crate::utils::debug::dump_db, hashbrown::HashSet};

pub mod index_key;

pub const MAX_STRING_INDEX_SIZE: usize = 1024;

/*

Null values are always considered the "smallest" element.

 */

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
pub(crate) struct Index {
    pub id: u16,
    col_id: u16,
    pub properties: Vec<IndexProperty>,
    pub unique: bool,
    pub replace: bool,
}

impl Index {
    pub fn new(
        id: u16,
        col_id: u16,
        properties: Vec<IndexProperty>,
        unique: bool,
        replace: bool,
    ) -> Self {
        Index {
            id,
            col_id,
            properties,
            unique,
            replace,
        }
    }

    pub fn get_prefix(&self) -> Vec<u8> {
        self.id.to_be_bytes().to_vec()
    }

    pub(crate) fn get_col_id(&self) -> u16 {
        self.col_id
    }

    pub fn multiple(&self) -> bool {
        self.properties.first().unwrap().index_type == IndexType::Words
    }

    pub fn create_for_object<F>(
        &self,
        cursors: &mut Cursors,
        oid: i64,
        object: IsarObject,
        mut delete_existing: F,
    ) -> Result<()>
    where
        F: FnMut(&mut Cursors, i64) -> Result<()>,
    {
        let id_key = IntKey::new(self.col_id, oid);
        self.create_keys(object, |key| {
            self.create_for_object_key(cursors, id_key, ByteKey::new(key), &mut delete_existing)?;
            Ok(true)
        })
    }

    fn create_for_object_key<F>(
        &self,
        cursors: &mut Cursors,
        id_key: IntKey,
        key: ByteKey,
        mut delete_existing: F,
    ) -> Result<()>
    where
        F: FnMut(&mut Cursors, i64) -> Result<()>,
    {
        if self.unique {
            let success = cursors.index.put_no_override(key, id_key.as_bytes())?;
            if !success {
                if self.replace {
                    delete_existing(cursors, id_key.get_id())?;
                } else {
                    return Err(IsarError::UniqueViolated {});
                }
            }
        } else {
            cursors.index.put(key, id_key.as_bytes())?;
        }
        Ok(())
    }

    pub fn delete_for_object(
        &self,
        cursors: &mut Cursors,
        oid: i64,
        object: IsarObject,
    ) -> Result<()> {
        let key = IntKey::new(self.col_id, oid);
        let oid_bytes = key.as_bytes();
        self.create_keys(object, |key| {
            let entry = cursors
                .index
                .move_to_key_val(ByteKey::new(key), &oid_bytes)?;
            if entry.is_some() {
                cursors.index.delete_current()?;
            }
            Ok(true)
        })
    }

    pub fn clear(&self, cursors: &mut Cursors) -> Result<()> {
        IndexWhereClause::new(
            IndexKey::new(self),
            IndexKey::new(self),
            false,
            Sort::Ascending,
        )?
        .iter_ids(&mut cursors.index, |cursor, _| {
            cursor.delete_current()?;
            Ok(true)
        })?;
        Ok(())
    }

    pub fn create_keys(
        &self,
        object: IsarObject,
        mut callback: impl FnMut(&[u8]) -> Result<bool>,
    ) -> Result<()> {
        if self.multiple() {
            self.create_multiple_keys(object, callback)
        } else {
            let bytes = self.create_single_key(object, vec![]);
            callback(&bytes)?;
            Ok(())
        }
    }

    fn create_single_key(&self, object: IsarObject, buffer: Vec<u8>) -> Vec<u8> {
        let mut key = IndexKey::with_buffer(self, buffer);
        for ip in &self.properties {
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
                        _ => unimplemented!(),
                    }
                }
                _ => unimplemented!(),
            }
        }
        key.bytes
    }

    fn create_multiple_keys(
        &self,
        object: IsarObject,
        mut callback: impl FnMut(&[u8]) -> Result<bool>,
    ) -> Result<()> {
        let ip = self.properties.first().unwrap();
        let value = ip.get_string_with_case(object);
        let mut result = Ok(());
        Self::create_word_keys(value.as_deref(), |word_key| match callback(word_key) {
            Ok(cont) => cont,
            Err(err) => {
                result = Err(err);
                false
            }
        });
        result
    }

    pub fn create_word_keys(value: Option<&str>, mut callback: impl FnMut(&[u8]) -> bool) {
        if let Some(str) = value {
            for word in str.unicode_words().unique() {
                if !callback(word.as_bytes()) {
                    break;
                }
            }
        }
    }

    #[cfg(test)]
    pub fn debug_dump(&self, txn: &mut IsarTxn) -> HashSet<(Vec<u8>, Vec<u8>)> {
        txn.read(|cursors| {
            let set = dump_db(&mut cursors.index, Some(&self.id.to_be_bytes()))
                .into_iter()
                .map(|(key, val)| (key.to_vec(), val.to_vec()))
                .collect();
            Ok(set)
        })
        .unwrap()
    }

    #[cfg(test)]
    pub fn debug_create_keys(&self, object: IsarObject) -> Vec<Vec<u8>> {
        let mut keys = vec![];
        self.create_keys(object, |key| {
            keys.push(key.to_vec());
            Ok(true)
        })
        .unwrap();
        keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::IsarCollection;
    use crate::instance::IsarInstance;
    use crate::object::data_type::DataType;
    use crate::{col, ind, isar};
    use float_next_after::NextAfter;

    fn check_index(isar: &IsarInstance, col: &IsarCollection, obj: IsarObject) {
        let mut txn = isar.begin_txn(true, false).unwrap();
        let oid = obj.read_long(col.get_oid_property());
        col.put(&mut txn, obj).unwrap();
        let index = col.debug_get_index(0);

        let set: HashSet<(Vec<u8>, Vec<u8>)> = index
            .debug_create_keys(obj)
            .into_iter()
            .map(|key| (key, IntKey::new(col.get_id(), oid).as_bytes().to_vec()))
            .collect();

        assert_eq!(index.debug_dump(&mut txn), set)
    }

    #[test]
    fn test_create_for_object_byte() {
        isar!(isar, col => col!(oid => DataType::Long, field => DataType::Byte; ind!(field)));
        let mut builder = col.new_object_builder(None);
        builder.write_long(1);
        builder.write_byte(123);
        check_index(&isar, col, builder.finish());
        isar.close();
    }

    #[test]
    fn test_create_for_object_int() {
        isar!(isar, col => col!(oid => DataType::Long, field => DataType::Int; ind!(field)));
        let mut builder = col.new_object_builder(None);
        builder.write_long(1);
        builder.write_int(123);
        check_index(&isar, col, builder.finish());
        isar.close();
    }

    #[test]
    fn test_create_for_object_float() {
        isar!(isar, col => col!(oid => DataType::Long, field => DataType::Float; ind!(field)));
        let mut builder = col.new_object_builder(None);
        builder.write_long(1);
        builder.write_float(123.321);
        check_index(&isar, col, builder.finish());
        isar.close();
    }

    #[test]
    fn test_create_for_object_long() {
        isar!(isar, col => col!(oid => DataType::Long, field => DataType::Long; ind!(field)));
        let mut builder = col.new_object_builder(None);
        builder.write_long(1);
        builder.write_long(123321);
        check_index(&isar, col, builder.finish());
        isar.close();
    }

    #[test]
    fn test_create_for_object_double() {
        isar!(isar, col => col!(oid => DataType::Long, field => DataType::Double; ind!(field)));
        let mut builder = col.new_object_builder(None);
        builder.write_long(1);
        builder.write_double(123123.321321);
        check_index(&isar, col, builder.finish());
        isar.close();
    }

    #[test]
    fn test_create_for_object_string() {
        fn test(str_type: IndexType, str_lc: bool) {
            isar!(isar, col => col!(oid => DataType::Long, field => DataType::String; ind!(str field, str_type, Some(str_lc))));
            let mut builder = col.new_object_builder(None);
            builder.write_long(1);
            builder.write_string(Some("Hello This Is A TEST Hello"));
            check_index(&isar, col, builder.finish());
            isar.close();
        }

        for str_type in &[IndexType::Value, IndexType::Hash, IndexType::Words] {
            test(*str_type, false);
            test(*str_type, true);
        }
    }

    #[test]
    fn test_create_for_object_unique() {}

    #[test]
    fn test_create_for_object_violate_unique() {
        isar!(isar, col => col!(oid => DataType::Long, field => DataType::Int; ind!(field; true, false)));
        let mut txn = isar.begin_txn(true, false).unwrap();

        let mut ob = col.new_object_builder(None);
        ob.write_long(1);
        ob.write_int(5);
        col.put(&mut txn, ob.finish()).unwrap();

        let mut ob = col.new_object_builder(None);
        ob.write_long(2);
        ob.write_int(5);
        let result = col.put(&mut txn, ob.finish());
        match result {
            Err(IsarError::UniqueViolated { .. }) => {}
            _ => panic!("wrong error"),
        };
        txn.abort();
        isar.close();
    }

    #[test]
    fn test_create_for_object_compound() {}

    #[test]
    fn test_delete_for_object() {}

    #[test]
    fn test_clear() {}

    #[test]
    fn test_create_key() {}

    #[test]
    fn test_create_int_key() {
        let pairs = vec![
            (i32::MIN, vec![0, 0, 0, 0]),
            (i32::MIN + 1, vec![0, 0, 0, 1]),
            (-1, vec![127, 255, 255, 255]),
            (0, vec![128, 0, 0, 0]),
            (1, vec![128, 0, 0, 1]),
            (i32::MAX - 1, vec![255, 255, 255, 254]),
            (i32::MAX, vec![255, 255, 255, 255]),
        ];
        for (val, bytes) in pairs {
            assert_eq!(Index::create_int_key(val), bytes);
        }
    }

    #[test]
    fn test_get_long_key() {
        let pairs = vec![
            (i64::MIN, vec![0, 0, 0, 0, 0, 0, 0, 0]),
            (i64::MIN + 1, vec![0, 0, 0, 0, 0, 0, 0, 1]),
            (-1, vec![127, 255, 255, 255, 255, 255, 255, 255]),
            (0, vec![128, 0, 0, 0, 0, 0, 0, 0]),
            (1, vec![128, 0, 0, 0, 0, 0, 0, 1]),
            (i64::MAX - 1, vec![255, 255, 255, 255, 255, 255, 255, 254]),
            (i64::MAX, vec![255, 255, 255, 255, 255, 255, 255, 255]),
        ];
        for (val, bytes) in pairs {
            assert_eq!(Index::create_long_key(val), bytes);
        }
    }

    #[test]
    fn test_get_float_key() {
        let pairs = vec![
            (f32::NAN, vec![0, 0, 0, 0]),
            (f32::NEG_INFINITY, vec![0, 127, 255, 255]),
            (f32::MIN, vec![0, 128, 0, 0]),
            (f32::MIN.next_after(f32::MAX), vec![0, 128, 0, 1]),
            ((-0.0).next_after(f32::MIN), vec![127, 255, 255, 254]),
            (-0.0, vec![127, 255, 255, 255]),
            (0.0, vec![128, 0, 0, 0]),
            (0.0.next_after(f32::MAX), vec![128, 0, 0, 1]),
            (f32::MAX.next_after(f32::MIN), vec![255, 127, 255, 254]),
            (f32::MAX, vec![255, 127, 255, 255]),
            (f32::INFINITY, vec![255, 128, 0, 0]),
        ];
        for (val, bytes) in pairs {
            assert_eq!(Index::create_float_key(val), bytes);
        }
    }

    #[test]
    fn test_get_double_key() {
        let pairs = vec![
            (f64::NAN, vec![0, 0, 0, 0, 0, 0, 0, 0]),
            (f64::NEG_INFINITY, vec![0, 15, 255, 255, 255, 255, 255, 255]),
            (f64::MIN, vec![0, 16, 0, 0, 0, 0, 0, 0]),
            (f64::MIN.next_after(f64::MAX), vec![0, 16, 0, 0, 0, 0, 0, 1]),
            (
                (-0.0).next_after(f64::MIN),
                vec![127, 255, 255, 255, 255, 255, 255, 254],
            ),
            (-0.0, vec![127, 255, 255, 255, 255, 255, 255, 255]),
            (0.0, vec![128, 0, 0, 0, 0, 0, 0, 0]),
            (0.0.next_after(f64::MAX), vec![128, 0, 0, 0, 0, 0, 0, 1]),
            (
                f64::MAX.next_after(f64::MIN),
                vec![255, 239, 255, 255, 255, 255, 255, 254],
            ),
            (f64::MAX, vec![255, 239, 255, 255, 255, 255, 255, 255]),
            (f64::INFINITY, vec![255, 240, 0, 0, 0, 0, 0, 0]),
        ];
        for (val, bytes) in pairs {
            assert_eq!(Index::create_double_key(val), bytes);
        }
    }

    #[test]
    fn test_get_byte_index_key() {
        assert_eq!(Index::create_byte_key(IsarObject::NULL_BYTE), vec![0]);
        assert_eq!(Index::create_byte_key(123), vec![123]);
        assert_eq!(Index::create_byte_key(255), vec![255]);
    }

    #[test]
    fn test_get_string_hash_key() {
        let long_str = (0..1700).map(|_| "a").collect::<String>();

        let pairs: Vec<(Option<&str>, Vec<u8>)> = vec![
            (None, vec![0, 0, 0, 0, 0, 0, 0, 0]),
            (Some(""), vec![183, 56, 242, 170, 183, 88, 42, 211]),
            (Some("hello"), vec![255, 175, 47, 252, 56, 169, 22, 4]),
            (
                Some("this is just a test"),
                vec![156, 13, 228, 133, 209, 47, 168, 125],
            ),
            (
                Some(&long_str[..]),
                vec![188, 104, 253, 203, 125, 112, 236, 55],
            ),
        ];
        for (str, hash) in pairs {
            assert_eq!(hash, Index::create_string_hash_key(str));
        }
    }

    #[test]
    fn test_get_string_value_key() {
        //let long_str = (0..1500).map(|_| "a").collect::<String>();

        let mut hello_bytes = vec![1];
        hello_bytes.extend_from_slice(b"hello");
        hello_bytes.push(0);
        let pairs: Vec<(Option<&str>, Vec<u8>)> = vec![
            (None, vec![0]),
            (Some(""), vec![1, 0]),
            (Some("hello"), hello_bytes),
        ];
        for (str, hash) in pairs {
            assert_eq!(hash, Index::create_string_value_key(str));
        }
    }

    #[test]
    fn test_get_string_word_keys() {
        let pairs: Vec<(Option<&str>, Vec<&str>)> = vec![
            (None, vec![]),
            (Some(""), vec![""]),
            (Some("hello"), vec!["hello"]),
            (
                Some("The quick brown fox brown can’t jump 32.3 feet right."),
                vec![
                    "The", "quick", "brown", "fox", "can’t", "jump", "32.3", "feet", "right",
                ],
            ),
        ];
        for (str, words) in pairs {
            let mut i = 0;
            Index::create_word_keys(str, |word| {
                assert_eq!(word, words[i].as_bytes());
                i += 1;
                true
            })
        }
    }
}
