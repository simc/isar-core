use crate::error::{IsarError, Result};
use crate::object::data_type::DataType;
use crate::object::isar_object::{IsarObject, Property};
use crate::object::object_id::ObjectId;
use crate::query::where_clause::WhereClause;
use crate::txn::Cursors;
use byteorder::{BigEndian, ByteOrder};
use enum_ordinalize::Ordinalize;
use itertools::Itertools;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::mem::transmute;
use unicode_segmentation::UnicodeSegmentation;
use wyhash::wyhash;

#[cfg(test)]
use {crate::txn::IsarTxn, crate::utils::debug::dump_db, hashbrown::HashSet};

pub const MAX_STRING_INDEX_SIZE: usize = 1500;

/*

Null values are always considered the "smallest" element.

 */

#[derive(Copy, Clone, Eq, PartialEq, Serialize_repr, Deserialize_repr, Debug, Ordinalize)]
#[repr(u8)]
pub enum StringIndexType {
    Value,
    Hash,
    Words,
}

#[derive(Clone, Eq, PartialEq)]
pub struct IndexProperty {
    property: Property,
    string_type: Option<StringIndexType>,
    string_case_sensitive: bool,
}

impl IndexProperty {
    pub(crate) fn new(
        property: Property,
        string_type: Option<StringIndexType>,
        string_case_sensitive: bool,
    ) -> Self {
        IndexProperty {
            property,
            string_type,
            string_case_sensitive,
        }
    }
    pub fn get_string_with_case(&self, object: IsarObject) -> Option<String> {
        object.read_string(self.property).map(|str| {
            if self.string_case_sensitive {
                str.to_string()
            } else {
                str.to_lowercase()
            }
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Index {
    id: u16,
    properties: Vec<IndexProperty>,
    unique: bool,
}

impl Index {
    pub(crate) fn new(id: u16, properties: Vec<IndexProperty>, unique: bool) -> Self {
        Index {
            id,
            properties,
            unique,
        }
    }

    pub fn new_where_clause(&self, skip_duplicates: bool) -> WhereClause {
        WhereClause::new_secondary(&self.get_prefix(), self.clone(), skip_duplicates)
    }

    pub(crate) fn get_id(&self) -> u16 {
        self.id
    }

    pub(crate) fn get_prefix(&self) -> Vec<u8> {
        self.id.to_be_bytes().to_vec()
    }

    pub(crate) fn is_unique(&self) -> bool {
        self.unique
    }

    pub(crate) fn multiple(&self) -> bool {
        self.properties.first().unwrap().string_type == Some(StringIndexType::Words)
    }

    pub(crate) fn create_for_object(
        &self,
        cursors: &mut Cursors,
        oid: &ObjectId,
        object: IsarObject,
    ) -> Result<()> {
        let oid_bytes = oid.as_bytes();
        self.create_keys(object, |key| {
            if self.unique {
                let success = cursors.secondary.put_no_override(key, oid_bytes)?;
                if !success {
                    return Err(IsarError::UniqueViolated {});
                }
            } else {
                cursors.secondary_dup.put(key, oid_bytes)?;
            }
            Ok(true)
        })
    }

    pub(crate) fn delete_for_object(
        &self,
        cursors: &mut Cursors,
        oid: &ObjectId,
        object: IsarObject,
    ) -> Result<()> {
        let oid_bytes = oid.as_bytes();
        self.create_keys(object, |key| {
            if self.unique {
                let entry = cursors.secondary.move_to(key)?;
                if entry.is_some() {
                    cursors.secondary.delete_current()?;
                }
            } else {
                let entry = cursors.secondary_dup.move_to_dup(key, oid_bytes)?;
                if entry.is_some() {
                    cursors.secondary_dup.delete_current()?;
                }
            }
            Ok(true)
        })
    }

    pub(crate) fn clear(&self, cursors: &mut Cursors) -> Result<()> {
        self.new_where_clause(false)
            .iter(cursors, |cursors, _, _| {
                if self.unique {
                    cursors.secondary.delete_current()?;
                } else {
                    cursors.secondary_dup.delete_current()?;
                };
                Ok(true)
            })?;
        Ok(())
    }

    pub(crate) fn create_keys(
        &self,
        object: IsarObject,
        mut callback: impl FnMut(&[u8]) -> Result<bool>,
    ) -> Result<()> {
        let mut bytes = self.get_prefix();
        if self.multiple() {
            self.create_multiple_keys(object, |key| {
                bytes.truncate(2);
                bytes.extend_from_slice(key);
                callback(&bytes)
            })
        } else {
            bytes.extend(self.create_single_key(object));
            callback(&bytes)?;
            Ok(())
        }
    }

    fn create_single_key(&self, object: IsarObject) -> Vec<u8> {
        self.properties
            .iter()
            .flat_map(|ip| match ip.property.data_type {
                DataType::Byte => {
                    let value = object.read_byte(ip.property);
                    Self::create_byte_key(value)
                }
                DataType::Int => {
                    let value = object.read_int(ip.property);
                    Self::create_int_key(value)
                }
                DataType::Long => {
                    let value = object.read_long(ip.property);
                    Self::create_long_key(value)
                }
                DataType::Float => {
                    let value = object.read_float(ip.property);
                    Self::create_float_key(value)
                }
                DataType::Double => {
                    let value = object.read_double(ip.property);
                    Self::create_double_key(value)
                }
                DataType::String => {
                    let value = ip.get_string_with_case(object);
                    match ip.string_type.as_ref().unwrap() {
                        StringIndexType::Value => Self::create_string_value_key(value.as_deref()),
                        StringIndexType::Hash => Self::create_string_hash_key(value.as_deref()),
                        _ => unimplemented!(),
                    }
                }
                _ => unimplemented!(),
            })
            .collect()
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

    pub fn create_int_key(value: i32) -> Vec<u8> {
        let unsigned = unsafe { transmute::<i32, u32>(value) };
        u32::to_be_bytes(unsigned ^ 1 << 31).to_vec()
    }

    pub fn get_int_from_key(bytes: &[u8]) -> i32 {
        let unsigned = BigEndian::read_u32(bytes);
        unsafe { transmute::<u32, i32>(unsigned ^ 1 << 31) }
    }

    pub fn create_long_key(value: i64) -> Vec<u8> {
        let unsigned = unsafe { transmute::<i64, u64>(value) };
        u64::to_be_bytes(unsigned ^ 1 << 63).to_vec()
    }

    pub fn get_long_from_key(bytes: &[u8]) -> i64 {
        let unsigned = BigEndian::read_u64(bytes);
        unsafe { transmute::<u64, i64>(unsigned ^ 1 << 63) }
    }

    pub fn create_float_key(value: f32) -> Vec<u8> {
        if !value.is_nan() {
            let bits = if value.is_sign_positive() {
                value.to_bits() + 2u32.pow(31)
            } else {
                !(-value).to_bits() - 2u32.pow(31)
            };
            u32::to_be_bytes(bits).to_vec()
        } else {
            vec![0; 4]
        }
    }

    pub fn create_double_key(value: f64) -> Vec<u8> {
        if !value.is_nan() {
            let bits = if value.is_sign_positive() {
                value.to_bits() + 2u64.pow(63)
            } else {
                !(-value).to_bits() - 2u64.pow(63)
            };
            u64::to_be_bytes(bits).to_vec()
        } else {
            vec![0; 8]
        }
    }

    pub fn create_byte_key(value: u8) -> Vec<u8> {
        vec![value]
    }

    pub fn create_string_hash_key(value: Option<&str>) -> Vec<u8> {
        let hash = if let Some(value) = value {
            wyhash(value.as_ref(), 0)
        } else {
            0
        };
        u64::to_be_bytes(hash).to_vec()
    }

    pub fn create_string_value_key(value: Option<&str>) -> Vec<u8> {
        if let Some(value) = value {
            let value = value.as_bytes();
            let mut bytes = vec![1];
            if value.len() >= MAX_STRING_INDEX_SIZE {
                bytes.extend_from_slice(&value[0..MAX_STRING_INDEX_SIZE]);
                bytes.push(0);
                let hash = wyhash(&bytes, 0);
                bytes.extend_from_slice(&u64::to_le_bytes(hash));
            } else {
                bytes.extend_from_slice(value);
                bytes.push(0);
            }
            bytes
        } else {
            vec![0]
        }
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
            let cursor = if self.unique {
                &mut cursors.secondary
            } else {
                &mut cursors.secondary_dup
            };
            let set = dump_db(cursor, Some(&self.id.to_be_bytes()))
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
        let mut txn = isar.begin_txn(true).unwrap();
        let oid = obj.read_oid(col).unwrap();
        col.put(&mut txn, obj).unwrap();
        let index = col.debug_get_index(0);

        let set: HashSet<(Vec<u8>, Vec<u8>)> = index
            .debug_create_keys(obj)
            .into_iter()
            .map(|key| (key, oid.as_bytes().to_vec()))
            .collect();

        assert_eq!(index.debug_dump(&mut txn), set)
    }

    #[test]
    fn test_create_for_object_byte() {
        isar!(isar, col => col!(oid => DataType::Int, field => DataType::Byte; ind!(field)));
        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        builder.write_byte(123);
        check_index(&isar, col, builder.finish());
    }

    #[test]
    fn test_create_for_object_int() {
        isar!(isar, col => col!(oid => DataType::Int, field => DataType::Int; ind!(field)));
        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        builder.write_int(123);
        check_index(&isar, col, builder.finish());
    }

    #[test]
    fn test_create_for_object_float() {
        isar!(isar, col => col!(oid => DataType::Int, field => DataType::Float; ind!(field)));
        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        builder.write_float(123.321);
        check_index(&isar, col, builder.finish());
    }

    #[test]
    fn test_create_for_object_long() {
        isar!(isar, col => col!(oid => DataType::Int, field => DataType::Long; ind!(field)));
        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        builder.write_long(123321);
        check_index(&isar, col, builder.finish());
    }

    #[test]
    fn test_create_for_object_double() {
        isar!(isar, col => col!(oid => DataType::Int, field => DataType::Double; ind!(field)));
        let mut builder = col.new_object_builder(None);
        builder.write_int(1);
        builder.write_double(123123.321321);
        check_index(&isar, col, builder.finish());
    }

    #[test]
    fn test_create_for_object_string() {
        fn test(str_type: StringIndexType, str_lc: bool) {
            isar!(isar, col => col!(oid => DataType::Int, field => DataType::String; ind!(str field, Some(str_type), str_lc)));
            let mut builder = col.new_object_builder(None);
            builder.write_int(1);
            builder.write_string(Some("Hello This Is A TEST Hello"));
            check_index(&isar, col, builder.finish());
        }

        for str_type in &[
            StringIndexType::Value,
            StringIndexType::Hash,
            StringIndexType::Words,
        ] {
            test(*str_type, false);
            test(*str_type, true);
        }
    }

    #[test]
    fn test_create_for_object_unique() {}

    #[test]
    fn test_create_for_object_violate_unique() {
        isar!(isar, col => col!(oid => DataType::Int, field => DataType::Int; ind!(field; true)));
        let mut txn = isar.begin_txn(true).unwrap();

        let mut ob = col.new_object_builder(None);
        ob.write_int(1);
        ob.write_int(5);
        col.put(&mut txn, ob.finish()).unwrap();

        let mut ob = col.new_object_builder(None);
        ob.write_int(2);
        ob.write_int(5);
        let result = col.put(&mut txn, ob.finish());
        match result {
            Err(IsarError::UniqueViolated { .. }) => {}
            _ => panic!("wrong error"),
        };
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
        let long_str = (0..1500).map(|_| "a").collect::<String>();

        let pairs: Vec<(&str, Vec<u8>)> = vec![
            ("hello", vec![196, 78, 229, 110, 148, 114, 106, 255]),
            (
                "this is just a test",
                vec![35, 152, 168, 2, 106, 235, 53, 50],
            ),
            (
                &long_str[..1499],
                vec![241, 58, 121, 152, 47, 193, 215, 217],
            ),
            (&long_str[..], vec![107, 96, 243, 122, 159, 148, 180, 244]),
        ];
        for (str, hash) in pairs {
            assert_eq!(hash, Index::create_string_hash_key(Some(str)));
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
