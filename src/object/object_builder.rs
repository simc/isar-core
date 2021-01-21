use crate::object::data_type::DataType;
use crate::object::object_id::ObjectId;
use crate::object::object_info::ObjectInfo;
use crate::object::property::Property;
use std::alloc::{AllocError, Allocator, Layout, System};
use std::ptr::NonNull;
use std::slice::from_raw_parts;

pub struct ObjectBuilder<'a> {
    buffer: Vec<u8, IsarObjectAllocator>,
    object_info: &'a ObjectInfo,
    property_index: usize,
    dynamic_offset: usize,
}

impl<'a> ObjectBuilder<'a> {
    pub(crate) fn new(
        object_info: &ObjectInfo,
        buffer: Option<Vec<u8, IsarObjectAllocator>>,
    ) -> ObjectBuilder {
        let buffer = buffer.unwrap_or_else(|| {
            Vec::with_capacity_in(object_info.get_static_size() * 2, IsarObjectAllocator {})
        });
        ObjectBuilder {
            buffer,
            object_info,
            property_index: 0,
            dynamic_offset: object_info.get_static_size(),
        }
    }

    fn get_next_property(&mut self, peek: bool) -> Property {
        let (_, property) = self
            .object_info
            .get_properties()
            .get(self.property_index)
            .unwrap();
        if !peek {
            self.property_index += 1;
        }

        *property
    }

    fn write_at(&mut self, offset: usize, bytes: &[u8]) {
        if offset + bytes.len() > self.buffer.len() {
            let required = offset + bytes.len();
            self.buffer.resize(required, 0);
        }
        self.buffer[offset..(offset + bytes.len())].clone_from_slice(&bytes[..]);
    }

    pub fn write_null(&mut self) {
        let property = self.get_next_property(true);
        match property.data_type {
            DataType::Byte => self.write_byte(Property::NULL_BYTE),
            DataType::Int => self.write_int(Property::NULL_INT),
            DataType::Float => self.write_float(Property::NULL_FLOAT),
            DataType::Long => self.write_long(Property::NULL_LONG),
            DataType::Double => self.write_double(Property::NULL_DOUBLE),
            DataType::String => self.write_string(None),
            DataType::ByteList => self.write_byte_list(None),
            DataType::IntList => self.write_int_list(None),
            DataType::FloatList => self.write_float_list(None),
            DataType::LongList => self.write_long_list(None),
            DataType::DoubleList => self.write_double_list(None),
            DataType::StringList => self.write_string_list(None),
        }
    }

    pub fn write_from(&mut self, property: Property, object: &[u8]) {
        match property.data_type {
            DataType::Byte => self.write_byte(property.get_byte(object)),
            DataType::Int => self.write_int(property.get_int(object)),
            DataType::Float => self.write_float(property.get_float(object)),
            DataType::Long => self.write_long(property.get_long(object)),
            DataType::Double => self.write_double(property.get_double(object)),
            DataType::String => self.write_string(property.get_string(object)),
            DataType::ByteList => self.write_byte_list(property.get_byte_list(object)),
            DataType::IntList => self.write_int_list(property.get_int_list(object)),
            DataType::FloatList => self.write_float_list(property.get_float_list(object)),
            DataType::LongList => self.write_long_list(property.get_long_list(object)),
            DataType::DoubleList => self.write_double_list(property.get_double_list(object)),
            DataType::StringList => {
                let list = property.get_string_list(object);
                if let Some(list) = list {
                    self.write_string_list(Some(list.as_slice()))
                } else {
                    self.write_string_list(None)
                }
            }
        }
    }

    pub fn write_byte(&mut self, value: u8) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::Byte);
        self.write_at(property.offset, &[value]);
    }

    pub fn write_bool(&mut self, value: bool) {
        let byte = if value {
            Property::TRUE_BYTE
        } else {
            Property::FALSE_BYTE
        };
        self.write_byte(byte);
    }

    pub fn write_int(&mut self, value: i32) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::Int);
        self.write_at(property.offset, &value.to_le_bytes());
    }

    pub fn write_float(&mut self, value: f32) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::Float);
        self.write_at(property.offset, &value.to_le_bytes());
    }

    pub fn write_long(&mut self, value: i64) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::Long);
        self.write_at(property.offset, &value.to_le_bytes());
    }

    pub fn write_double(&mut self, value: f64) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::Double);
        self.write_at(property.offset, &value.to_le_bytes());
    }

    pub fn write_string(&mut self, value: Option<&str>) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::String);
        self.write_list(property.offset, value.map(|s| s.as_ref()));
    }

    pub fn write_byte_list(&mut self, value: Option<&[u8]>) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::ByteList);
        self.write_list(property.offset, value);
    }

    pub fn write_int_list(&mut self, value: Option<&[i32]>) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::IntList);
        self.write_list(property.offset, value);
    }

    pub fn write_float_list(&mut self, value: Option<&[f32]>) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::FloatList);
        self.write_list(property.offset, value);
    }

    pub fn write_long_list(&mut self, value: Option<&[i64]>) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::LongList);
        self.write_list(property.offset, value);
    }

    pub fn write_double_list(&mut self, value: Option<&[f64]>) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::DoubleList);
        self.write_list(property.offset, value);
    }

    pub fn write_string_list(&mut self, value: Option<&[Option<&str>]>) {
        let property = self.get_next_property(false);
        assert_eq!(property.data_type, DataType::StringList);
        self.write_list::<u8>(property.offset, None);
    }

    pub fn finish(self) -> Vec<u8, IsarObjectAllocator> {
        let mut buffer = self.buffer;
        let end_padding = (8 - (buffer.len() + ObjectId::get_size()) % 8) % 8;
        buffer.resize(buffer.len() + end_padding, 0);

        buffer
    }

    fn write_list<T>(&mut self, offset: usize, list: Option<&[T]>) {
        if let Some(list) = list {
            self.write_at(offset, &(self.dynamic_offset as u32).to_le_bytes());
            self.write_at(offset + 4, &(list.len() as u32).to_le_bytes());
            let type_size = std::mem::size_of::<T>();
            let ptr = list.as_ptr() as *const T;
            let bytes = unsafe { from_raw_parts::<u8>(ptr as *const u8, list.len() * type_size) };
            self.write_at(self.dynamic_offset, bytes);
            self.dynamic_offset += bytes.len();
        } else {
            self.write_at(offset, &0u64.to_le_bytes());
        }
    }
}

pub struct IsarObjectAllocator {}

unsafe impl Allocator for IsarObjectAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let alloc_size = layout.size() + 8;
        let layout = Layout::from_size_align(alloc_size, layout.align()).unwrap();
        let ptr = System.allocate(layout)?.as_ptr() as *mut u8;
        let padding = 8 - (ptr as usize - ObjectId::get_size()) % 8;
        unsafe {
            let padded_ptr = ptr.add(padding);
            *padded_ptr.sub(1) = padding as u8;
            let new_ptr = core::ptr::slice_from_raw_parts_mut(padded_ptr, alloc_size - padding);
            Ok(NonNull::new(new_ptr).unwrap())
        }
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        let padding = *ptr.as_ptr().sub(1) as usize;
        let unpadded_ptr = ptr.as_ptr().sub(padding);
        let layout = Layout::from_size_align(layout.size() + padding, layout.align()).unwrap();
        System.deallocate(NonNull::new_unchecked(unpadded_ptr), layout);
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::debug::SlicePad;
    use crate::{col, isar};

    macro_rules! builder {
        ($var:ident, $oi:ident, $type:ident) => {
            isar!(isar, col => col!("int" => $type));
            let mut $var = col.new_object_builder(None);
            let $oi = col.debug_get_object_info();
        };
    }

    #[test]
    pub fn test_write_int() {
        builder!(b, oi, Int);
        b.write_int(123);
        let result = b.finish();
        oi.verify_object(result.as_ref());
        assert_eq!(result.as_ref(), 123i32.to_le_bytes().pad(2, 4))
    }

    #[test]
    #[should_panic]
    pub fn test_write_int_wrong_type() {
        builder!(b, _oi, Long);
        b.write_int(123);
    }

    #[test]
    pub fn test_write_float() {
        builder!(b, oi, Float);
        b.write_float(123.123);
        let result = b.finish();
        oi.verify_object(result.as_ref());
        assert_eq!(result.as_ref(), 123.123f32.to_le_bytes().pad(2, 4));

        builder!(b, oi, Float);
        b.write_float(f32::NAN);
        let result = b.finish();
        oi.verify_object(result.as_ref());
        assert_eq!(result.as_ref(), f32::NAN.to_le_bytes().pad(2, 4));
    }

    #[test]
    #[should_panic]
    pub fn test_write_float_wrong_type() {
        builder!(b, _oi, Double);
        b.write_float(123.123);
    }

    #[test]
    pub fn test_write_long() {
        builder!(b, oi, Long);
        b.write_long(123123);
        let result = b.finish();
        oi.verify_object(result.as_ref());
        assert_eq!(result.as_ref(), 123123i64.to_le_bytes().pad(2, 0))
    }

    #[test]
    #[should_panic]
    pub fn test_write_long_wrong_type() {
        builder!(b, _oi, Int);
        b.write_long(123123);
    }

    #[test]
    pub fn test_write_double() {
        builder!(b, oi, Double);
        b.write_double(123.123);
        let result = b.finish();
        oi.verify_object(result.as_ref());
        assert_eq!(result.as_ref(), 123.123f64.to_le_bytes().pad(2, 0));

        builder!(b, oi, Double);
        b.write_double(f64::NAN);
        let result = b.finish();
        oi.verify_object(result.as_ref());
        assert_eq!(result.as_ref(), f64::NAN.to_le_bytes().pad(2, 0));
    }

    #[test]
    #[should_panic]
    pub fn test_write_double_wrong_type() {
        builder!(b, _oi, Float);

        b.write_double(123.0);
    }

    #[test]
    pub fn test_write_byte() {
        builder!(b, oi, Byte);
        b.write_byte(0);
        let result = b.finish();
        oi.verify_object(&result);
        assert_eq!(&result, &[0, 0]);

        builder!(b, oi, Byte);
        b.write_byte(123);
        let result = b.finish();
        oi.verify_object(&result);
        assert_eq!(&result, &[123, 0]);

        builder!(b, oi, Byte);
        b.write_byte(255);
        let result = b.finish();
        oi.verify_object(&result);
        assert_eq!(&result, &[255, 0]);
    }

    #[test]
    #[should_panic]
    pub fn test_write_byte_wrong_type() {
        builder!(b, _oi, String);
        b.write_byte(123);
    }

    #[test]
    pub fn test_write_multiple_static_types() {
        /*builder!(
            b,
            Property::new( DataType::Int, 0),
            Property::new( DataType::Int, 8),
            Property::new( DataType::Double, 16),
            Property::new( DataType::Bool, 24),
            Property::new( DataType::Double, 25)
        );

        b.write_int(i64::MAX);
        b.write_long(i64::MIN);
        b.write_double(consts::PI);
        b.write_bool(None);
        b.write_float(consts::E);

        let mut bytes = i64::MAX.to_le_bytes().to_vec();
        bytes.extend_from_slice(&i64::MIN.to_le_bytes());
        bytes.extend_from_slice(&consts::PI.to_le_bytes());
        bytes.push(Property::NULL_BOOL);
        bytes.extend_from_slice(&consts::E.to_le_bytes());

        assert_eq!(b.to_bytes(), bytes);*/
    }
}
