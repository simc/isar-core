use crate::object::data_type::DataType;
use crate::object::object_builder::ObjectBuilder;
use byteorder::{ByteOrder, LittleEndian};
use num_traits::Float;
use std::{cmp::Ordering, str::from_utf8_unchecked};
use xxhash_rust::xxh3::xxh3_64_with_seed;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Property {
    pub name: String,
    pub data_type: DataType,
    pub offset: usize,
    pub target_col: Option<String>,
}

impl Property {
    pub const fn new(
        name: String,
        data_type: DataType,
        offset: usize,
        target_col: Option<String>,
    ) -> Self {
        Property {
            name,
            data_type,
            offset,
            target_col,
        }
    }

    pub const fn debug(data_type: DataType, offset: usize) -> Self {
        Property {
            name: String::new(),
            data_type,
            offset,
            target_col: None,
        }
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
    pub fn contains_property(&self, offset: usize) -> bool {
        self.contains_offset(offset)
    }

    pub fn is_null(&self, offset: usize, data_type: DataType) -> bool {
        match data_type {
            DataType::Byte => self.read_byte(offset) == Self::NULL_BYTE,
            DataType::Int => self.read_int(offset) == Self::NULL_INT,
            DataType::Long => self.read_long(offset) == Self::NULL_LONG,
            DataType::Float => self.read_float(offset).is_nan(),
            DataType::Double => self.read_double(offset).is_nan(),
            _ => self.get_offset_size(offset).is_none(),
        }
    }

    pub fn read_byte(&self, offset: usize) -> u8 {
        if self.contains_property(offset) {
            self.bytes[offset]
        } else {
            Self::NULL_BYTE
        }
    }

    pub fn read_bool(&self, offset: usize) -> bool {
        self.read_byte(offset) == Self::TRUE_BYTE
    }

    #[inline]
    fn read_offset(&self, offset: usize) -> usize {
        LittleEndian::read_u32(&self.bytes[offset..]) as usize
    }

    pub fn read_int(&self, offset: usize) -> i32 {
        if self.contains_property(offset) {
            LittleEndian::read_i32(&self.bytes[offset..])
        } else {
            Self::NULL_INT
        }
    }

    pub fn read_float(&self, offset: usize) -> f32 {
        if self.contains_property(offset) {
            LittleEndian::read_f32(&self.bytes[offset..])
        } else {
            Self::NULL_FLOAT
        }
    }

    pub fn read_long(&self, offset: usize) -> i64 {
        if self.contains_property(offset) {
            LittleEndian::read_i64(&self.bytes[offset..])
        } else {
            Self::NULL_LONG
        }
    }

    pub fn read_double(&self, offset: usize) -> f64 {
        if self.contains_property(offset) {
            LittleEndian::read_f64(&self.bytes[offset..])
        } else {
            Self::NULL_DOUBLE
        }
    }

    fn get_offset_size(&self, offset: usize) -> Option<(usize, usize)> {
        if self.contains_offset(offset) {
            let start_offset = self.read_offset(offset);
            if start_offset != 0 {
                let mut i = 1;
                while self.contains_offset(offset + i * 4) {
                    let end_offset = self.read_offset(offset + i * 4);
                    if end_offset != 0 {
                        return Some((start_offset, end_offset - start_offset));
                    }
                    i += 1;
                }
                return Some((start_offset, self.bytes.len() - start_offset));
            }
        }
        None
    }

    pub fn read_string(&'a self, offset: usize) -> Option<&'a str> {
        let bytes = self.read_byte_list(offset)?;
        let str = unsafe { from_utf8_unchecked(bytes) };
        Some(str)
    }

    pub fn read_object(&'a self, offset: usize) -> Option<IsarObject> {
        let bytes = self.read_byte_list(offset)?;
        Some(IsarObject::from_bytes(bytes))
    }

    pub fn read_byte_list(&self, offset: usize) -> Option<&'a [u8]> {
        let (offset, size) = self.get_offset_size(offset)?;
        Some(&self.bytes[offset..offset + size])
    }

    pub fn read_int_list(&self, offset: usize) -> Option<Vec<i32>> {
        let (offset, size) = self.get_offset_size(offset)?;
        let list = (offset..offset + size)
            .step_by(4)
            .into_iter()
            .map(|offset| LittleEndian::read_i32(&self.bytes[offset..]))
            .collect();
        Some(list)
    }

    pub fn read_float_list(&self, offset: usize) -> Option<Vec<f32>> {
        let (offset, size) = self.get_offset_size(offset)?;
        let list = (offset..offset + size)
            .step_by(4)
            .into_iter()
            .map(|offset| LittleEndian::read_f32(&self.bytes[offset..]))
            .collect();
        Some(list)
    }

    pub fn read_long_list(&self, offset: usize) -> Option<Vec<i64>> {
        let (offset, size) = self.get_offset_size(offset)?;
        let list = (offset..offset + size)
            .step_by(8)
            .into_iter()
            .map(|offset| LittleEndian::read_i64(&self.bytes[offset..]))
            .collect();
        Some(list)
    }

    pub fn read_double_list(&self, offset: usize) -> Option<Vec<f64>> {
        let (offset, size) = self.get_offset_size(offset)?;
        let list = (offset..offset + size)
            .step_by(8)
            .into_iter()
            .map(|offset| LittleEndian::read_f64(&self.bytes[offset..]))
            .collect();
        Some(list)
    }

    pub fn read_string_list(&self, offset: usize) -> Option<Vec<Option<&'a str>>> {
        self.read_dynamic_list(offset, |bytes| unsafe { from_utf8_unchecked(bytes) })
    }

    pub fn read_object_list(&self, offset: usize) -> Option<Vec<Option<IsarObject<'a>>>> {
        self.read_dynamic_list(offset, |bytes| IsarObject::from_bytes(bytes))
    }

    fn get_first_content_offset(&self, mut offset: usize, size: usize) -> (usize, usize) {
        let mut none_count = 0;
        while offset + 4 <= size {
            let content_offset = self.read_offset(offset);
            if content_offset != 0 {
                return (content_offset, none_count);
            }
            offset += 4;
            none_count += 1;
        }
        (0, none_count)
    }

    fn read_dynamic_list<T: Clone>(
        &self,
        offset: usize,
        transform: impl Fn(&'a [u8]) -> T,
    ) -> Option<Vec<Option<T>>> {
        let (offset, size) = self.get_offset_size(offset)?;
        if size == 0 {
            return Some(vec![]);
        }

        let (mut start_offset, none_count) = self.get_first_content_offset(offset, offset + size);
        if start_offset == 0 {
            return Some(vec![None; none_count]);
        }

        let length = (start_offset - offset) / 4;
        let mut list = vec![None; length];

        let mut unsaved_index = -1;
        for i in none_count..length {
            let end_offset = if i == length - 1 {
                offset + size
            } else {
                self.read_offset(offset + (i + 1) * 4)
            };

            if end_offset == 0 {
                list[i] = None;
                if unsaved_index == -1 {
                    unsaved_index = i as i32;
                }
            } else {
                let bytes = &self.bytes[start_offset..end_offset];
                let value = transform(bytes);
                if unsaved_index >= 0 {
                    list[unsaved_index as usize] = Some(value);
                    unsaved_index = -1;
                } else {
                    list[i] = Some(value);
                }
            }

            if end_offset != 0 {
                start_offset = end_offset;
            }
        }

        Some(list)
    }

    pub fn hash_property(&self, property: &Property, case_sensitive: bool, seed: u64) -> u64 {
        match property.data_type {
            DataType::Byte => xxh3_64_with_seed(&[self.read_byte(property.offset)], seed),
            DataType::Int => xxh3_64_with_seed(&self.read_int(property.offset).to_le_bytes(), seed),
            DataType::Float => {
                xxh3_64_with_seed(&self.read_float(property.offset).to_le_bytes(), seed)
            }
            DataType::Long => {
                xxh3_64_with_seed(&self.read_long(property.offset).to_le_bytes(), seed)
            }
            DataType::Double => {
                xxh3_64_with_seed(&self.read_double(property.offset).to_le_bytes(), seed)
            }
            DataType::String => {
                Self::hash_string(self.read_string(property.offset), case_sensitive, seed)
            }
            _ => {
                if let Some((offset, size)) = self.get_offset_size(property.offset) {
                    match property.data_type {
                        DataType::StringList => Self::hash_string_list(
                            self.read_string_list(property.offset),
                            case_sensitive,
                            seed,
                        ),
                        _ => xxh3_64_with_seed(&self.bytes[offset..offset + size], seed),
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

    pub fn compare_property(&self, other: &IsarObject, property: &Property) -> Ordering {
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
            DataType::Byte => self
                .read_byte(property.offset)
                .cmp(&other.read_byte(property.offset)),
            DataType::Int => self
                .read_int(property.offset)
                .cmp(&other.read_int(property.offset)),
            DataType::Float => {
                let f1 = self.read_float(property.offset);
                let f2 = other.read_float(property.offset);
                compare_float(f1, f2)
            }
            DataType::Long => self
                .read_long(property.offset)
                .cmp(&other.read_long(property.offset)),
            DataType::Double => {
                let f1 = self.read_double(property.offset);
                let f2 = other.read_double(property.offset);
                compare_float(f1, f2)
            }
            DataType::String => {
                let s1 = self.read_string(property.offset);
                let s2 = other.read_string(property.offset);
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
    use itertools::Itertools;

    use super::Property;
    use crate::object::data_type::DataType::*;
    use crate::object::isar_object::IsarObject;
    use crate::object::object_builder::ObjectBuilder;

    macro_rules! builder {
        ($builder:ident, $prop:ident, $type:ident) => {
            let $prop = Property::debug($type, 2);
            let props = vec![$prop.clone()];
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
            assert!(object.is_null(p.offset, p.data_type));
        }
    }

    #[test]
    fn test_read_byte() {
        builder!(b, p, Byte);
        b.write_null();
        assert_eq!(b.finish().read_byte(p.offset), IsarObject::NULL_BYTE);
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, Byte);
        b.write_byte(123);
        assert_eq!(b.finish().read_byte(p.offset), 123);
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_int() {
        builder!(b, p, Int);
        b.write_null();
        assert_eq!(b.finish().read_int(p.offset), IsarObject::NULL_INT);
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, Int);
        b.write_int(123);
        assert_eq!(b.finish().read_int(p.offset), 123);
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_float() {
        builder!(b, p, Float);
        b.write_null();
        assert!(b.finish().read_float(p.offset).is_nan());
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, Float);
        b.write_float(123.123);
        assert!((b.finish().read_float(p.offset) - 123.123).abs() < 0.000001);
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_long() {
        builder!(b, p, Long);
        b.write_null();
        assert_eq!(b.finish().read_long(p.offset), IsarObject::NULL_LONG);
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, Long);
        b.write_long(123123123123123123);
        assert_eq!(b.finish().read_long(p.offset), 123123123123123123);
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_double() {
        builder!(b, p, Double);
        b.write_null();
        assert!(b.finish().read_double(p.offset).is_nan());
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, Double);
        b.write_double(123123.123123123);
        assert!((b.finish().read_double(p.offset) - 123123.123123123).abs() < 0.00000001);
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_string() {
        builder!(b, p, String);
        b.write_null();
        assert_eq!(b.finish().read_string(p.offset), None);
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, String);
        b.write_string(Some("hello"));
        assert_eq!(b.finish().read_string(p.offset), Some("hello"));
        assert!(!b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, String);
        b.write_string(Some(""));
        assert_eq!(b.finish().read_string(p.offset), Some(""));
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_byte_list() {
        builder!(b, p, ByteList);
        b.write_null();
        assert_eq!(b.finish().read_byte_list(p.offset), None);
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, ByteList);
        b.write_byte_list(Some(&[1, 2, 3]));
        assert_eq!(b.finish().read_byte_list(p.offset), Some(&[1, 2, 3][..]));
        assert!(!b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, ByteList);
        b.write_byte_list(Some(&[]));
        assert_eq!(b.finish().read_byte_list(p.offset), Some(&[][..]));
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_int_list() {
        builder!(b, p, IntList);
        b.write_null();
        assert_eq!(b.finish().read_int_list(p.offset), None);
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, IntList);
        b.write_int_list(Some(&[1, 2, 3]));
        assert_eq!(b.finish().read_int_list(p.offset), Some(vec![1, 2, 3]));
        assert!(!b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, IntList);
        b.write_int_list(Some(&[]));
        assert_eq!(b.finish().read_int_list(p.offset), Some(vec![]));
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_float_list() {
        builder!(b, p, FloatList);
        b.write_null();
        assert_eq!(b.finish().read_float_list(p.offset), None);
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, FloatList);
        b.write_float_list(Some(&[1.1, 2.2, 3.3]));
        assert_eq!(
            b.finish().read_float_list(p.offset),
            Some(vec![1.1, 2.2, 3.3])
        );
        assert!(!b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, FloatList);
        b.write_float_list(Some(&[]));
        assert_eq!(b.finish().read_float_list(p.offset), Some(vec![]));
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_long_list() {
        builder!(b, p, LongList);
        b.write_null();
        assert_eq!(b.finish().read_long_list(p.offset), None);
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, LongList);
        b.write_long_list(Some(&[1, 2, 3]));
        assert_eq!(b.finish().read_long_list(p.offset), Some(vec![1, 2, 3]));
        assert!(!b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, LongList);
        b.write_long_list(Some(&[]));
        assert_eq!(b.finish().read_long_list(p.offset), Some(vec![]));
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_double_list() {
        builder!(b, p, DoubleList);
        b.write_null();
        assert_eq!(b.finish().read_double_list(p.offset), None);
        assert!(b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, DoubleList);
        b.write_double_list(Some(&[1.1, 2.2, 3.3]));
        assert_eq!(
            b.finish().read_double_list(p.offset),
            Some(vec![1.1, 2.2, 3.3])
        );
        assert!(!b.finish().is_null(p.offset, p.data_type));

        builder!(b, p, DoubleList);
        b.write_double_list(Some(&[]));
        assert_eq!(b.finish().read_double_list(p.offset), Some(vec![]));
        assert!(!b.finish().is_null(p.offset, p.data_type));
    }

    #[test]
    fn test_read_string_list() {
        builder!(b, p, StringList);
        b.write_null();
        assert_eq!(b.finish().read_string_list(p.offset), None);

        let cases = vec![
            vec![],
            vec![None],
            vec![None, None],
            vec![None, None, None],
            vec![Some("")],
            vec![Some(""), Some("")],
            vec![Some(""), Some(""), Some("")],
            vec![Some(""), None],
            vec![None, Some("")],
            vec![Some(""), None, None],
            vec![None, Some(""), None],
            vec![None, None, Some("")],
            vec![None, Some(""), Some("")],
            vec![Some(""), None, Some("")],
            vec![Some(""), Some(""), None],
            vec![Some("a")],
            vec![Some("a"), Some("ab")],
            vec![Some("a"), Some("ab"), Some("abc")],
            vec![None, Some("a")],
            vec![Some("a"), None],
            vec![None, Some("a")],
            vec![Some("a"), None, None],
            vec![None, Some("a"), None],
            vec![None, None, Some("a")],
            vec![None, Some("a"), Some("bbb")],
            vec![Some("a"), None, Some("bbb")],
            vec![Some("a"), Some("bbb"), None],
        ];

        for case1 in &cases {
            for case2 in &cases {
                for case3 in &cases {
                    let case = case1
                        .iter()
                        .chain(case2)
                        .chain(case3)
                        .cloned()
                        .collect_vec();
                    builder!(b, p, StringList);
                    b.write_string_list(Some(&case));
                    assert_eq!(b.finish().read_string_list(p.offset), Some(case));
                }
            }
        }
    }
}
