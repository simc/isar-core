use crate::object::data_type::DataType;
use crate::object::object_builder::ObjectBuilder;
use byteorder::{ByteOrder, LittleEndian};
use num_traits::Float;
use std::cmp::Ordering;
use xxhash_rust::xxh3::xxh3_64_with_seed;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Property {
    pub data_type: DataType,
    pub offset: usize,
}

impl Property {
    pub const fn new(data_type: DataType, offset: usize) -> Self {
        Property { data_type, offset }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct IsarObject<'a> {
    bytes: &'a [u8],
    static_size: usize,
}

impl<'a> IsarObject<'a> {
    pub const NULL_BYTE: u8 = 0;
    pub const FALSE_BYTE: u8 = 1;
    pub const TRUE_BYTE: u8 = 2;
    pub const NULL_INT: i32 = i32::MIN;
    pub const NULL_LONG: i64 = i64::MIN;
    pub const NULL_FLOAT: f32 = f32::NAN;
    pub const NULL_DOUBLE: f64 = f64::NAN;

    pub fn from_bytes(bytes: &'a [u8]) -> Self {
        let static_size = LittleEndian::read_u16(bytes) as usize;
        IsarObject { bytes, static_size }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.bytes
    }

    #[inline]
    pub(crate) fn contains_offset(&self, offset: usize) -> bool {
        self.static_size > offset
    }

    #[inline]
    pub fn contains_property(&self, property: Property) -> bool {
        self.contains_offset(property.offset)
    }

    pub fn is_null(&self, property: Property) -> bool {
        match property.data_type {
            DataType::Byte => self.read_byte(property) == Self::NULL_BYTE,
            DataType::Int => self.read_int(property) == Self::NULL_INT,
            DataType::Long => self.read_long(property) == Self::NULL_LONG,
            DataType::Float => self.read_float(property).is_nan(),
            DataType::Double => self.read_double(property).is_nan(),
            _ => self.get_offset_length(property.offset, false).is_none(),
        }
    }

    pub fn read_byte(&self, property: Property) -> u8 {
        assert_eq!(property.data_type, DataType::Byte);
        if self.contains_property(property) {
            self.bytes[property.offset]
        } else {
            Self::NULL_BYTE
        }
    }

    pub fn read_bool(&self, property: Property) -> bool {
        self.read_byte(property) == Self::TRUE_BYTE
    }

    pub fn read_int(&self, property: Property) -> i32 {
        assert_eq!(property.data_type, DataType::Int);
        if self.contains_property(property) {
            LittleEndian::read_i32(&self.bytes[property.offset..])
        } else {
            Self::NULL_INT
        }
    }

    pub fn read_float(&self, property: Property) -> f32 {
        assert_eq!(property.data_type, DataType::Float);
        if self.contains_property(property) {
            LittleEndian::read_f32(&self.bytes[property.offset..])
        } else {
            Self::NULL_FLOAT
        }
    }

    pub fn read_long(&self, property: Property) -> i64 {
        assert_eq!(property.data_type, DataType::Long);
        if self.contains_property(property) {
            LittleEndian::read_i64(&self.bytes[property.offset..])
        } else {
            Self::NULL_LONG
        }
    }

    pub fn read_double(&self, property: Property) -> f64 {
        assert_eq!(property.data_type, DataType::Double);
        if self.contains_property(property) {
            LittleEndian::read_f64(&self.bytes[property.offset..])
        } else {
            Self::NULL_DOUBLE
        }
    }

    fn get_offset_length(&self, offset: usize, dynamic_offset: bool) -> Option<(usize, usize)> {
        if dynamic_offset || self.contains_offset(offset) {
            let list_offset = LittleEndian::read_u32(&self.bytes[offset..]) as usize;
            let length = LittleEndian::read_u32(&self.bytes[offset + 4..]);
            if list_offset != 0 {
                return Some((list_offset as usize, length as usize));
            }
        }
        None
    }

    fn read_string_at(&self, offset: usize, dynamic_offset: bool) -> Option<&'a str> {
        let (offset, length) = self.get_offset_length(offset, dynamic_offset)?;
        let str = unsafe { std::str::from_utf8_unchecked(&self.bytes[offset..offset + length]) };
        Some(str)
    }

    pub fn read_string(&'a self, property: Property) -> Option<&'a str> {
        assert_eq!(property.data_type, DataType::String);
        self.read_string_at(property.offset, false)
    }

    pub fn read_byte_list(&self, property: Property) -> Option<&'a [u8]> {
        assert_eq!(property.data_type, DataType::ByteList);
        let (offset, length) = self.get_offset_length(property.offset, false)?;
        Some(&self.bytes[offset..offset + length])
    }

    pub fn read_int_list(&self, property: Property) -> Option<Vec<i32>> {
        assert_eq!(property.data_type, DataType::IntList);
        let (offset, length) = self.get_offset_length(property.offset, false)?;
        let list = (offset..offset + length * 4)
            .step_by(4)
            .into_iter()
            .map(|offset| LittleEndian::read_i32(&self.bytes[offset..]))
            .collect();
        Some(list)
    }

    pub fn read_float_list(&self, property: Property) -> Option<Vec<f32>> {
        assert_eq!(property.data_type, DataType::FloatList);
        let (offset, length) = self.get_offset_length(property.offset, false)?;
        let list = (offset..offset + length * 4)
            .step_by(4)
            .into_iter()
            .map(|offset| LittleEndian::read_f32(&self.bytes[offset..]))
            .collect();
        Some(list)
    }

    pub fn read_long_list(&self, property: Property) -> Option<Vec<i64>> {
        assert_eq!(property.data_type, DataType::LongList);
        let (offset, length) = self.get_offset_length(property.offset, false)?;
        let list = (offset..offset + length * 8)
            .step_by(8)
            .into_iter()
            .map(|offset| LittleEndian::read_i64(&self.bytes[offset..]))
            .collect();
        Some(list)
    }

    pub fn read_double_list(&self, property: Property) -> Option<Vec<f64>> {
        assert_eq!(property.data_type, DataType::DoubleList);
        let (offset, length) = self.get_offset_length(property.offset, false)?;
        let list = (offset..offset + length * 8)
            .step_by(8)
            .into_iter()
            .map(|offset| LittleEndian::read_f64(&self.bytes[offset..]))
            .collect();
        Some(list)
    }

    pub fn read_string_list(&self, property: Property) -> Option<Vec<Option<&'a str>>> {
        assert_eq!(property.data_type, DataType::StringList);
        let (offset, length) = self.get_offset_length(property.offset, false)?;
        let list = (offset..offset + length * 8)
            .step_by(8)
            .into_iter()
            .map(|offset| self.read_string_at(offset, true))
            .collect();
        Some(list)
    }

    pub fn hash_property(&self, property: Property, case_sensitive: bool, seed: u64) -> u64 {
        match property.data_type {
            DataType::Byte => xxh3_64_with_seed(&[self.read_byte(property)], seed),
            DataType::Int => xxh3_64_with_seed(&self.read_int(property).to_le_bytes(), seed),
            DataType::Float => xxh3_64_with_seed(&self.read_float(property).to_le_bytes(), seed),
            DataType::Long => xxh3_64_with_seed(&self.read_long(property).to_le_bytes(), seed),
            DataType::Double => xxh3_64_with_seed(&self.read_double(property).to_le_bytes(), seed),
            DataType::String => Self::hash_string(self.read_string(property), case_sensitive, seed),
            _ => {
                if let Some((offset, length)) = self.get_offset_length(property.offset, false) {
                    match property.data_type {
                        DataType::ByteList => {
                            xxh3_64_with_seed(&self.bytes[offset..offset + length], seed)
                        }
                        DataType::IntList | DataType::FloatList => {
                            xxh3_64_with_seed(&self.bytes[offset..offset + length * 4], seed)
                        }
                        DataType::LongList | DataType::DoubleList => {
                            xxh3_64_with_seed(&self.bytes[offset..offset + length * 8], seed)
                        }
                        DataType::StringList => Self::hash_string_list(
                            self.read_string_list(property),
                            case_sensitive,
                            seed,
                        ),
                        _ => panic!(),
                    }
                } else {
                    seed
                }
            }
        }
    }

    pub fn hash_string(value: Option<&str>, case_sensitive: bool, seed: u64) -> u64 {
        if let Some(str) = value {
            if case_sensitive {
                xxh3_64_with_seed(str.as_bytes(), seed)
            } else {
                xxh3_64_with_seed(str.to_lowercase().as_bytes(), seed)
            }
        } else {
            seed
        }
    }

    pub fn hash_list<T>(value: Option<&[T]>, seed: u64) -> u64 {
        if let Some(list) = value {
            let bytes = ObjectBuilder::get_list_bytes(list);
            xxh3_64_with_seed(bytes, seed)
        } else {
            seed
        }
    }

    pub fn hash_string_list(
        value: Option<Vec<Option<&str>>>,
        case_sensitive: bool,
        seed: u64,
    ) -> u64 {
        if let Some(str) = value {
            let mut hash = seed;
            for value in str {
                hash = Self::hash_string(value, case_sensitive, hash);
            }
            hash
        } else {
            seed
        }
    }

    pub fn compare_property(&self, other: &IsarObject, property: Property) -> Ordering {
        fn compare_float<T: Float>(f1: T, f2: T) -> Ordering {
            if !f1.is_nan() {
                if !f2.is_nan() {
                    if f1 > f2 {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                } else {
                    Ordering::Greater
                }
            } else if !f2.is_nan() {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }
        match property.data_type {
            DataType::Byte => self.read_byte(property).cmp(&other.read_byte(property)),
            DataType::Int => self.read_int(property).cmp(&other.read_int(property)),
            DataType::Float => {
                let f1 = self.read_float(property);
                let f2 = other.read_float(property);
                compare_float(f1, f2)
            }
            DataType::Long => self.read_long(property).cmp(&other.read_long(property)),
            DataType::Double => {
                let f1 = self.read_double(property);
                let f2 = other.read_double(property);
                compare_float(f1, f2)
            }
            DataType::String => {
                let s1 = self.read_string(property);
                let s2 = other.read_string(property);
                if let Some(s1) = s1 {
                    if let Some(s2) = s2 {
                        s1.cmp(s2)
                    } else {
                        Ordering::Greater
                    }
                } else if s2.is_some() {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            }
            _ => Ordering::Equal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Property;
    use crate::object::data_type::DataType::*;
    use crate::object::isar_object::IsarObject;
    use crate::object::object_builder::ObjectBuilder;

    macro_rules! builder {
        ($builder:ident, $prop:ident, $type:ident) => {
            let $prop = Property::new($type, 2);
            let props = vec![$prop];
            let mut $builder = ObjectBuilder::new(&props, None);
        };
    }

    #[test]
    fn test_read_non_contained_property() {
        let data_types = vec![
            Byte, Int, Float, Long, Double, String, ByteList, IntList, FloatList, LongList,
            DoubleList, StringList,
        ];
        for data_type in data_types {
            builder!(_b, p, data_type);
            let empty = vec![0, 0];
            let object = IsarObject::from_bytes(&empty);
            assert!(object.is_null(p));
        }
    }

    #[test]
    fn test_read_byte() {
        builder!(b, p, Byte);
        b.write_null();
        assert_eq!(b.finish().read_byte(p), IsarObject::NULL_BYTE);
        assert!(b.finish().is_null(p));

        builder!(b, p, Byte);
        b.write_byte(123);
        assert_eq!(b.finish().read_byte(p), 123);
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_int() {
        builder!(b, p, Int);
        b.write_null();
        assert_eq!(b.finish().read_int(p), IsarObject::NULL_INT);
        assert!(b.finish().is_null(p));

        builder!(b, p, Int);
        b.write_int(123);
        assert_eq!(b.finish().read_int(p), 123);
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_float() {
        builder!(b, p, Float);
        b.write_null();
        assert!(b.finish().read_float(p).is_nan());
        assert!(b.finish().is_null(p));

        builder!(b, p, Float);
        b.write_float(123.123);
        assert!((b.finish().read_float(p) - 123.123).abs() < 0.000001);
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_long() {
        builder!(b, p, Long);
        b.write_null();
        assert_eq!(b.finish().read_long(p), IsarObject::NULL_LONG);
        assert!(b.finish().is_null(p));

        builder!(b, p, Long);
        b.write_long(123123123123123123);
        assert_eq!(b.finish().read_long(p), 123123123123123123);
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_double() {
        builder!(b, p, Double);
        b.write_null();
        assert!(b.finish().read_double(p).is_nan());
        assert!(b.finish().is_null(p));

        builder!(b, p, Double);
        b.write_double(123123.123123123);
        assert!((b.finish().read_double(p) - 123123.123123123).abs() < 0.00000001);
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_string() {
        builder!(b, p, String);
        b.write_null();
        assert_eq!(b.finish().read_string(p), None);
        assert!(b.finish().is_null(p));

        builder!(b, p, String);
        b.write_string(Some("hello"));
        assert_eq!(b.finish().read_string(p), Some("hello"));
        assert!(!b.finish().is_null(p));

        builder!(b, p, String);
        b.write_string(Some(""));
        assert_eq!(b.finish().read_string(p), Some(""));
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_byte_list() {
        builder!(b, p, ByteList);
        b.write_null();
        assert_eq!(b.finish().read_byte_list(p), None);
        assert!(b.finish().is_null(p));

        builder!(b, p, ByteList);
        b.write_byte_list(Some(&[1, 2, 3]));
        assert_eq!(b.finish().read_byte_list(p), Some(&[1, 2, 3][..]));
        assert!(!b.finish().is_null(p));

        builder!(b, p, ByteList);
        b.write_byte_list(Some(&[]));
        assert_eq!(b.finish().read_byte_list(p), Some(&[][..]));
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_int_list() {
        builder!(b, p, IntList);
        b.write_null();
        assert_eq!(b.finish().read_int_list(p), None);
        assert!(b.finish().is_null(p));

        builder!(b, p, IntList);
        b.write_int_list(Some(&[1, 2, 3]));
        assert_eq!(b.finish().read_int_list(p), Some(vec![1, 2, 3]));
        assert!(!b.finish().is_null(p));

        builder!(b, p, IntList);
        b.write_int_list(Some(&[]));
        assert_eq!(b.finish().read_int_list(p), Some(vec![]));
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_float_list() {
        builder!(b, p, FloatList);
        b.write_null();
        assert_eq!(b.finish().read_float_list(p), None);
        assert!(b.finish().is_null(p));

        builder!(b, p, FloatList);
        b.write_float_list(Some(&[1.1, 2.2, 3.3]));
        assert_eq!(b.finish().read_float_list(p), Some(vec![1.1, 2.2, 3.3]));
        assert!(!b.finish().is_null(p));

        builder!(b, p, FloatList);
        b.write_float_list(Some(&[]));
        assert_eq!(b.finish().read_float_list(p), Some(vec![]));
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_long_list() {
        builder!(b, p, LongList);
        b.write_null();
        assert_eq!(b.finish().read_long_list(p), None);
        assert!(b.finish().is_null(p));

        builder!(b, p, LongList);
        b.write_long_list(Some(&[1, 2, 3]));
        assert_eq!(b.finish().read_long_list(p), Some(vec![1, 2, 3]));
        assert!(!b.finish().is_null(p));

        builder!(b, p, LongList);
        b.write_long_list(Some(&[]));
        assert_eq!(b.finish().read_long_list(p), Some(vec![]));
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_double_list() {
        builder!(b, p, DoubleList);
        b.write_null();
        assert_eq!(b.finish().read_double_list(p), None);
        assert!(b.finish().is_null(p));

        builder!(b, p, DoubleList);
        b.write_double_list(Some(&[1.1, 2.2, 3.3]));
        assert_eq!(b.finish().read_double_list(p), Some(vec![1.1, 2.2, 3.3]));
        assert!(!b.finish().is_null(p));

        builder!(b, p, DoubleList);
        b.write_double_list(Some(&[]));
        assert_eq!(b.finish().read_double_list(p), Some(vec![]));
        assert!(!b.finish().is_null(p));
    }

    #[test]
    fn test_read_string_list() {
        builder!(b, p, StringList);
        b.write_null();
        assert_eq!(b.finish().read_string_list(p), None);
        assert!(b.finish().is_null(p));

        builder!(b, p, StringList);
        b.write_string_list(Some(&[Some("hello"), None, Some(""), Some("last")]));
        assert_eq!(
            b.finish().read_string_list(p),
            Some(vec![Some("hello"), None, Some(""), Some("last")])
        );
        assert!(!b.finish().is_null(p));

        builder!(b, p, StringList);
        b.write_string_list(Some(&[]));
        assert_eq!(b.finish().read_string_list(p), Some(vec![]));
        assert!(!b.finish().is_null(p));
    }
}
