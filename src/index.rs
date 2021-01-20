use crate::error::{IsarError, Result};
use crate::object::data_type::DataType;
use crate::object::property::Property;
use crate::query::where_clause::WhereClause;
use crate::txn::Cursors;
use std::mem::transmute;
use wyhash::wyhash;

#[cfg(test)]
use {
    crate::object::object_id::ObjectId, crate::txn::IsarTxn, crate::utils::debug::dump_db,
    hashbrown::HashSet,
};

pub const MAX_STRING_INDEX_SIZE: usize = 1500;

/*

Null values are always considered the "smallest" element.

 */

#[derive(Clone, Eq, PartialEq)]
pub struct Index {
    id: u16,
    properties: Vec<Property>,
    unique: bool,
    hash_value: bool,
}

impl Index {
    pub(crate) fn new(id: u16, properties: Vec<Property>, unique: bool, hash_value: bool) -> Self {
        Index {
            id,
            properties,
            unique,
            hash_value,
        }
    }

    pub(crate) fn get_id(&self) -> u16 {
        self.id
    }

    pub(crate) fn get_prefix(&self) -> Vec<u8> {
        self.id.to_le_bytes().to_vec()
    }

    pub(crate) fn is_unique(&self) -> bool {
        self.unique
    }

    pub(crate) fn create_for_object(
        &self,
        cursors: &mut Cursors,
        oid: &[u8],
        object: &[u8],
    ) -> Result<()> {
        let index_key = self.create_key(object);
        if self.unique {
            let success = cursors.secondary.put_no_override(&index_key, oid)?;
            if success {
                Ok(())
            } else {
                Err(IsarError::UniqueViolated {})
            }
        } else {
            cursors.secondary_dup.put(&index_key, oid)
        }
    }

    pub(crate) fn delete_for_object(
        &self,
        cursors: &mut Cursors,
        oid: &[u8],
        object: &[u8],
    ) -> Result<()> {
        let index_key = self.create_key(object);
        if self.unique {
            cursors.secondary.move_to(&index_key)?;
            cursors.secondary.delete_current()
        } else {
            cursors.secondary_dup.move_to_dup(&index_key, oid)?;
            cursors.secondary_dup.delete_current()
        }
    }

    pub(crate) fn clear(&self, cursors: &mut Cursors) -> Result<()> {
        let cursor = if self.unique {
            &mut cursors.secondary
        } else {
            &mut cursors.secondary_dup
        };
        cursor.iter_prefix(&self.get_prefix(), false, |cursor, _, _| {
            cursor.delete_current()?;
            Ok(true)
        })?;
        Ok(())
    }

    pub fn new_where_clause(&self, skip_duplicates: bool) -> WhereClause {
        WhereClause::new_secondary(self.clone(), skip_duplicates)
    }

    pub(crate) fn create_key(&self, object: &[u8]) -> Vec<u8> {
        let mut bytes = self.get_prefix();
        let index_iter = self
            .properties
            .iter()
            .flat_map(|property| match property.data_type {
                DataType::Byte => {
                    let value = property.get_byte(object);
                    Self::get_byte_key(value)
                }
                DataType::Int => {
                    let value = property.get_int(object);
                    Self::get_int_key(value)
                }
                DataType::Long => {
                    let value = property.get_long(object);
                    Self::get_long_key(value)
                }
                DataType::Float => {
                    let value = property.get_float(object);
                    Self::get_float_key(value)
                }
                DataType::Double => {
                    let value = property.get_double(object);
                    Self::get_double_key(value)
                }
                DataType::String => {
                    let value = property.get_string(object);
                    if self.hash_value {
                        Self::get_string_hash_key(value)
                    } else {
                        Self::get_string_value_key(value)
                    }
                }
                _ => unimplemented!(),
            });
        bytes.extend(index_iter);
        bytes
    }

    pub fn get_int_key(value: i32) -> Vec<u8> {
        let unsigned = unsafe { transmute::<i32, u32>(value) };
        u32::to_be_bytes(unsigned ^ 1 << 31).to_vec()
    }

    pub fn get_long_key(value: i64) -> Vec<u8> {
        let unsigned = unsafe { transmute::<i64, u64>(value) };
        u64::to_be_bytes(unsigned ^ 1 << 63).to_vec()
    }

    pub fn get_float_key(value: f32) -> Vec<u8> {
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

    pub fn get_double_key(value: f64) -> Vec<u8> {
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

    pub fn get_byte_key(value: u8) -> Vec<u8> {
        vec![value]
    }

    pub fn get_string_hash_key(value: Option<&str>) -> Vec<u8> {
        let hash = if let Some(value) = value {
            wyhash(value.as_ref(), 0)
        } else {
            0
        };
        u64::to_be_bytes(hash).to_vec()
    }

    pub fn get_string_value_key(value: Option<&str>) -> Vec<u8> {
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

    #[cfg(test)]
    pub fn debug_dump(&self, txn: &mut IsarTxn) -> HashSet<(Vec<u8>, ObjectId)> {
        txn.read(|cursors| {
            let cursor = if self.unique {
                &mut cursors.secondary
            } else {
                &mut cursors.secondary_dup
            };
            let set = dump_db(cursor, Some(&self.id.to_le_bytes()))
                .into_iter()
                .map(|(key, val)| (key.to_vec(), *ObjectId::from_bytes(&val)))
                .collect();
            Ok(set)
        })
        .unwrap()
    }

    #[cfg(test)]
    pub fn debug_create_key(&self, object: &[u8]) -> Vec<u8> {
        self.create_key(object).to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{col, ind, isar, set};
    use float_next_after::NextAfter;

    #[test]
    fn test_create_for_object() {
        macro_rules! test_index (
            ($data_type:ident , $data:expr, $write:ident) => {
                isar!(isar, col => col!(field => $data_type; ind!(field)));
                let mut txn = isar.begin_txn(true).unwrap();

                let mut builder = col.new_object_builder(None);
                builder.$write($data);
                let obj = builder.finish();

                let oid = col.put(&mut txn, None, obj.as_ref()).unwrap();
                let index = col.debug_get_index(0);

                assert_eq!(
                    index.debug_dump(&mut txn),
                    set![(index.create_key(obj.as_ref()), oid)]
                )
            };
        );

        test_index!(Byte, 123, write_byte);
        test_index!(Int, 123456i32, write_int);
        test_index!(Float, 123.456f32, write_float);
        test_index!(Long, 123456i64, write_long);
        test_index!(Double, 123.456f64, write_double);
        test_index!(String, Some("hello"), write_string);
    }

    #[test]
    fn test_create_for_object_unique() {}

    #[test]
    fn test_create_for_violate_unique() {
        isar!(isar, col => col!(field => Int; ind!(field; true)));
        let mut txn = isar.begin_txn(true).unwrap();

        let mut o = col.new_object_builder(None);
        o.write_int(5);
        let bytes = o.finish();

        col.put(&mut txn, None, bytes.as_ref()).unwrap();

        let result = col.put(&mut txn, None, bytes.as_ref());
        match result {
            Err(IsarError::UniqueViolated { .. }) => {}
            _ => panic!("wrong error"),
        };
    }

    #[test]
    fn test_create_for_object_compound() {}

    #[test]
    fn test_create_for_object_string() {}

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
            assert_eq!(Index::get_int_key(val), bytes);
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
            assert_eq!(Index::get_long_key(val), bytes);
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
            assert_eq!(Index::get_float_key(val), bytes);
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
            assert_eq!(Index::get_double_key(val), bytes);
        }
    }

    #[test]
    fn test_get_byte_index_key() {
        assert_eq!(Index::get_byte_key(Property::NULL_BYTE), vec![0]);
        assert_eq!(Index::get_byte_key(123), vec![123]);
        assert_eq!(Index::get_byte_key(255), vec![255]);
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
            assert_eq!(hash, Index::get_string_hash_key(Some(str)));
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
            assert_eq!(hash, Index::get_string_value_key(str));
        }
    }
}
