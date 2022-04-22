use crate::object::data_type::DataType;
use crate::object::isar_object::IsarObject;
use crate::object::isar_object::Property;
use std::slice::from_raw_parts;

pub struct ObjectBuilder<'a> {
    buffer: Vec<u8>,
    properties: &'a [Property],
    property_id: usize,
    dynamic_offset: usize,
}

impl<'a> ObjectBuilder<'a> {
    pub fn new(properties: &[Property], buffer: Option<Vec<u8>>) -> ObjectBuilder {
        assert_eq!(properties.first().unwrap().offset, 2);
        let last_property = properties.last().unwrap();
        let static_size = last_property.offset + last_property.data_type.get_static_size();
        let buffer = buffer.unwrap_or_else(|| Vec::with_capacity(static_size * 2));

        let mut ob = ObjectBuilder {
            buffer,
            properties,
            property_id: 0,
            dynamic_offset: static_size,
        };
        ob.write_at(0, &(static_size as u16).to_le_bytes());
        ob
    }

    fn next_property(&mut self, peek: bool) -> Property {
        let property = self.properties.get(self.property_id).unwrap();
        if !peek {
            self.property_id += 1;
        }

        *property
    }

    fn write_at(&mut self, offset: usize, bytes: &[u8]) {
        if offset + bytes.len() > self.buffer.len() {
            let required = offset + bytes.len();
            self.buffer.resize(required, 0);
        }
        self.buffer[offset..(offset + bytes.len())].clone_from_slice(bytes);
    }

    pub fn write_null(&mut self) {
        let property = self.next_property(true);
        match property.data_type {
            DataType::Byte => self.write_byte(IsarObject::NULL_BYTE),
            DataType::Int => self.write_int(IsarObject::NULL_INT),
            DataType::Float => self.write_float(IsarObject::NULL_FLOAT),
            DataType::Long => self.write_long(IsarObject::NULL_LONG),
            DataType::Double => self.write_double(IsarObject::NULL_DOUBLE),
            DataType::String => self.write_string(None),
            DataType::ByteList => self.write_byte_list(None),
            DataType::IntList => self.write_int_list(None),
            DataType::FloatList => self.write_float_list(None),
            DataType::LongList => self.write_long_list(None),
            DataType::DoubleList => self.write_double_list(None),
            DataType::StringList => self.write_string_list(None),
        }
    }

    pub fn write_byte(&mut self, value: u8) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::Byte);
        self.write_at(property.offset, &[value]);
    }

    pub fn write_bool(&mut self, value: bool) {
        let byte = if value {
            IsarObject::TRUE_BYTE
        } else {
            IsarObject::FALSE_BYTE
        };
        self.write_byte(byte);
    }

    pub fn write_int(&mut self, value: i32) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::Int);
        self.write_at(property.offset, &value.to_le_bytes());
    }

    pub fn write_float(&mut self, value: f32) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::Float);
        self.write_at(property.offset, &value.to_le_bytes());
    }

    pub fn write_long(&mut self, value: i64) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::Long);
        self.write_at(property.offset, &value.to_le_bytes());
    }

    pub fn write_double(&mut self, value: f64) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::Double);
        self.write_at(property.offset, &value.to_le_bytes());
    }

    pub fn write_string(&mut self, value: Option<&str>) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::String);
        self.write_list(property.offset, value.map(|s| s.as_ref()));
    }

    pub fn write_byte_list(&mut self, value: Option<&[u8]>) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::ByteList);
        self.write_list(property.offset, value);
    }

    pub fn write_int_list(&mut self, value: Option<&[i32]>) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::IntList);
        self.write_list(property.offset, value);
    }

    pub fn write_float_list(&mut self, value: Option<&[f32]>) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::FloatList);
        self.write_list(property.offset, value);
    }

    pub fn write_long_list(&mut self, value: Option<&[i64]>) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::LongList);
        self.write_list(property.offset, value);
    }

    pub fn write_double_list(&mut self, value: Option<&[f64]>) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::DoubleList);
        self.write_list(property.offset, value);
    }

    pub fn write_string_list(&mut self, value: Option<&[Option<&str>]>) {
        let property = self.next_property(false);
        assert_eq!(property.data_type, DataType::StringList);
        if let Some(value) = value {
            self.write_at(property.offset, &(self.dynamic_offset as u32).to_le_bytes());
            self.write_at(property.offset + 4, &(value.len() as u32).to_le_bytes());
            let mut offset_list_offset = self.dynamic_offset;
            self.dynamic_offset += value.len() * 8;
            for str in value {
                let bytes = str.map(|str| str.as_bytes());
                self.write_list(offset_list_offset, bytes);
                offset_list_offset += 8;
            }
        } else {
            self.write_at(property.offset, &0u64.to_le_bytes());
        }
    }

    fn write_list<T>(&mut self, offset: usize, list: Option<&[T]>) {
        if let Some(list) = list {
            self.write_at(offset, &(self.dynamic_offset as u32).to_le_bytes());
            self.write_at(offset + 4, &(list.len() as u32).to_le_bytes());
            let bytes = Self::get_list_bytes(list);
            self.write_at(self.dynamic_offset, bytes);
            self.dynamic_offset += bytes.len();
        } else {
            self.write_at(offset, &0u64.to_le_bytes());
        }
    }

    #[inline]
    pub(crate) fn get_list_bytes<T>(list: &[T]) -> &[u8] {
        let type_size = std::mem::size_of::<T>();
        let ptr = list.as_ptr() as *const T;
        unsafe { from_raw_parts::<u8>(ptr as *const u8, list.len() * type_size) }
    }

    pub fn finish(&self) -> IsarObject {
        assert_eq!(self.property_id, self.properties.len());
        IsarObject::from_bytes(&self.buffer)
    }

    pub fn recycle(self) -> Vec<u8> {
        let mut buffer = self.buffer;
        buffer.clear();
        buffer
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectBuilder;
    use crate::object::data_type::DataType::{self, *};
    use crate::object::isar_object::{IsarObject, Property};

    macro_rules! builder {
        ($var:ident, $type:ident) => {
            let props = vec![Property::new(Long, 2), Property::new($type, 10)];
            let mut $var = ObjectBuilder::new(&props, None);
            $var.write_long(1);
        };
    }

    #[test]
    pub fn test_write_null() {
        builder!(b, Byte);
        b.write_null();
        assert_eq!(b.finish().as_bytes(), &[11, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]);

        builder!(b, Int);
        b.write_null();
        let mut bytes = vec![14, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&IsarObject::NULL_INT.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);

        builder!(b, Float);
        b.write_null();
        let mut bytes = vec![14, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&IsarObject::NULL_FLOAT.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);

        builder!(b, Long);
        b.write_null();
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&IsarObject::NULL_LONG.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);

        builder!(b, Double);
        b.write_null();
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&IsarObject::NULL_DOUBLE.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);

        let list_types = vec![
            String, ByteList, IntList, FloatList, LongList, DoubleList, StringList,
        ];

        for list_type in list_types {
            builder!(b, list_type);
            b.write_null();
            let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
            bytes.extend_from_slice(&0u64.to_le_bytes());
            assert_eq!(b.finish().as_bytes(), &bytes);
        }
    }

    #[test]
    pub fn test_write_byte() {
        builder!(b, Byte);
        b.write_byte(0);
        assert_eq!(b.finish().as_bytes(), &[11, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]);

        builder!(b, Byte);
        b.write_byte(123);
        assert_eq!(b.finish().as_bytes(), &[11, 0, 1, 0, 0, 0, 0, 0, 0, 0, 123]);

        builder!(b, Byte);
        b.write_byte(255);
        assert_eq!(b.finish().as_bytes(), &[11, 0, 1, 0, 0, 0, 0, 0, 0, 0, 255]);
    }

    #[test]
    #[should_panic]
    pub fn test_write_byte_wrong_type() {
        builder!(b, String);
        b.write_byte(123);
    }

    #[test]
    pub fn test_write_bool() {
        builder!(b, Byte);
        b.write_bool(true);
        assert_eq!(
            b.finish().as_bytes(),
            &[11, 0, 1, 0, 0, 0, 0, 0, 0, 0, IsarObject::TRUE_BYTE]
        );

        builder!(b, Byte);
        b.write_bool(false);
        assert_eq!(
            b.finish().as_bytes(),
            &[11, 0, 1, 0, 0, 0, 0, 0, 0, 0, IsarObject::FALSE_BYTE]
        );
    }

    #[test]
    pub fn test_write_int() {
        builder!(b, Int);
        b.write_int(123);
        assert_eq!(
            b.finish().as_bytes(),
            &[14, 0, 1, 0, 0, 0, 0, 0, 0, 0, 123, 0, 0, 0]
        )
    }

    #[test]
    #[should_panic]
    pub fn test_write_int_wrong_type() {
        builder!(b, Long);
        b.write_int(123);
    }

    #[test]
    pub fn test_write_float() {
        builder!(b, Float);
        b.write_float(123.123);
        let mut bytes = vec![14, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&123.123f32.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);

        builder!(b, Float);
        b.write_float(f32::NAN);
        let mut bytes = vec![14, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&f32::NAN.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);
    }

    #[test]
    #[should_panic]
    pub fn test_write_float_wrong_type() {
        builder!(b, Double);
        b.write_float(123.123);
    }

    #[test]
    pub fn test_write_long() {
        builder!(b, Long);
        b.write_long(123123);
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&123123i64.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes)
    }

    #[test]
    #[should_panic]
    pub fn test_write_long_wrong_type() {
        builder!(b, Int);
        b.write_long(123123);
    }

    #[test]
    pub fn test_write_double() {
        builder!(b, Double);
        b.write_double(123.123);
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&123.123f64.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);

        builder!(b, Double);
        b.write_double(f64::NAN);
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&f64::NAN.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);
    }

    #[test]
    #[should_panic]
    pub fn test_write_double_wrong_type() {
        builder!(b, Float);
        b.write_double(123.0);
    }

    #[test]
    pub fn test_write_string() {
        builder!(b, String);
        b.write_string(Some("hello"));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&5u32.to_le_bytes());
        bytes.extend_from_slice(b"hello");
        assert_eq!(b.finish().as_bytes(), &bytes);
    }

    #[test]
    #[should_panic]
    pub fn test_write_string_wrong_type() {
        builder!(b, ByteList);
        b.write_string(Some("hello"));
    }

    #[test]
    pub fn test_write_multiple_static_types() {
        let props = vec![
            Property::new(DataType::Long, 2),
            Property::new(DataType::Byte, 10),
            Property::new(DataType::Int, 11),
            Property::new(DataType::Float, 15),
            Property::new(DataType::Long, 19),
            Property::new(DataType::Double, 27),
        ];
        let mut b = ObjectBuilder::new(&props, None);

        b.write_long(1);
        b.write_byte(u8::MAX);
        b.write_int(i32::MAX);
        b.write_float(std::f32::consts::E);
        b.write_long(i64::MIN);
        b.write_double(std::f64::consts::PI);

        let mut bytes = vec![35, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.push(u8::MAX);
        bytes.extend_from_slice(&i32::MAX.to_le_bytes());
        bytes.extend_from_slice(&std::f32::consts::E.to_le_bytes());
        bytes.extend_from_slice(&i64::MIN.to_le_bytes());
        bytes.extend_from_slice(&std::f64::consts::PI.to_le_bytes());

        assert_eq!(b.finish().as_bytes(), bytes);
    }

    #[test]
    pub fn test_write_byte_list() {
        builder!(b, ByteList);
        b.write_byte_list(Some(&[1, 2, 3]));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&[1, 2, 3]);
        assert_eq!(b.finish().as_bytes(), &bytes);

        builder!(b, ByteList);
        b.write_byte_list(Some(&[]));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);
    }

    #[test]
    #[should_panic]
    pub fn test_write_byte_list_wrong_type() {
        builder!(b, String);
        b.write_byte_list(Some(&[1, 2, 3]));
    }

    #[test]
    pub fn test_write_int_list() {
        builder!(b, IntList);
        b.write_int_list(Some(&[1, -10]));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&1i32.to_le_bytes());
        bytes.extend_from_slice(&(-10i32).to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);

        builder!(b, IntList);
        b.write_int_list(Some(&[]));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);
    }

    #[test]
    #[should_panic]
    pub fn test_write_int_list_wrong_type() {
        builder!(b, LongList);
        b.write_int_list(Some(&[1, 2, 3]));
    }

    #[test]
    pub fn test_write_float_list() {
        builder!(b, FloatList);
        b.write_float_list(Some(&[1.1, -10.10]));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&1.1f32.to_le_bytes());
        bytes.extend_from_slice(&(-10.10f32).to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);

        builder!(b, FloatList);
        b.write_float_list(Some(&[]));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);
    }

    #[test]
    #[should_panic]
    pub fn test_write_float_list_wrong_type() {
        builder!(b, Double);
        b.write_float(123.123);
    }

    #[test]
    pub fn test_write_long_list() {
        builder!(b, LongList);
        b.write_long_list(Some(&[1, -10]));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&1i64.to_le_bytes());
        bytes.extend_from_slice(&(-10i64).to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);

        builder!(b, LongList);
        b.write_long_list(Some(&[]));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);
    }

    #[test]
    #[should_panic]
    pub fn test_write_long_list_wrong_type() {
        builder!(b, IntList);
        b.write_long_list(Some(&[1, 2, 3]));
    }

    #[test]
    pub fn test_write_double_list() {
        builder!(b, DoubleList);
        b.write_double_list(Some(&[1.1, -10.10]));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&1.1f64.to_le_bytes());
        bytes.extend_from_slice(&(-10.10f64).to_le_bytes());
        assert_eq!(b.finish().as_bytes(), &bytes);
    }

    #[test]
    #[should_panic]
    pub fn test_write_double_list_wrong_type() {
        builder!(b, FloatList);
        b.write_double_list(Some(&[1.2]));
    }

    #[test]
    pub fn test_write_string_list() {
        builder!(b, StringList);
        b.write_string_list(Some(&[Some("abc"), None, Some("de")]));
        let mut bytes = vec![18, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        bytes.extend_from_slice(&18u32.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&42u32.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&45u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(b"abcde");
        assert_eq!(b.finish().as_bytes(), &bytes);
    }

    #[test]
    #[should_panic]
    pub fn test_write_string_list_wrong_type() {
        builder!(b, StringList);
        b.write_string(Some("hello"));
    }

    #[test]
    #[should_panic]
    pub fn test_finish_missing_properties() {
        builder!(b, Int);
        b.finish();
    }
}
