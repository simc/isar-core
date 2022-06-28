use crate::error::{IsarError, Result};
use crate::object::data_type::DataType;
use crate::object::isar_object::IsarObject;
use crate::object::object_builder::ObjectBuilder;
use serde_json::{json, Map, Value};

use super::isar_object::Property;

pub(crate) struct JsonEncodeDecode {}

impl<'a> JsonEncodeDecode {
    pub fn encode(
        properties: &[Property],
        object: IsarObject,
        primitive_null: bool,
        byte_as_bool: bool,
    ) -> Map<String, Value> {
        let mut object_map = Map::new();

        for property in properties {
            let value = if primitive_null && object.is_null(property.offset, property.data_type) {
                Value::Null
            } else {
                match property.data_type {
                    DataType::Byte => {
                        if byte_as_bool {
                            json!(object.read_bool(property.offset))
                        } else {
                            json!(object.read_byte(property.offset))
                        }
                    }
                    DataType::Int => json!(object.read_int(property.offset)),
                    DataType::Float => json!(object.read_float(property.offset)),
                    DataType::Long => json!(object.read_long(property.offset)),
                    DataType::Double => json!(object.read_double(property.offset)),
                    DataType::String => json!(object.read_string(property.offset)),
                    DataType::Object => unimplemented!(),
                    DataType::ByteList => json!(object.read_byte_list(property.offset)),
                    DataType::IntList => json!(object.read_int_list(property.offset)),
                    DataType::FloatList => json!(object.read_float_list(property.offset)),
                    DataType::LongList => json!(object.read_long_list(property.offset)),
                    DataType::DoubleList => json!(object.read_double_list(property.offset)),
                    DataType::StringList => json!(object.read_string_list(property.offset)),
                    DataType::ObjectList => unimplemented!(),
                }
            };
            object_map.insert(property.name.clone(), value);
        }

        object_map
    }

    pub fn decode(
        properties: &'a [Property],
        json: &Value,
        buffer: Option<Vec<u8>>,
    ) -> Result<ObjectBuilder<'a>> {
        let mut ob = ObjectBuilder::new(properties, buffer);
        let object = json.as_object().ok_or(IsarError::InvalidJson {})?;

        for property in properties {
            if let Some(value) = object.get(&property.name) {
                match property.data_type {
                    DataType::Byte => ob.write_byte(Self::value_to_byte(value)?),
                    DataType::Int => ob.write_int(Self::value_to_int(value)?),
                    DataType::Float => ob.write_float(Self::value_to_float(value)?),
                    DataType::Long => ob.write_long(Self::value_to_long(value)?),
                    DataType::Double => ob.write_double(Self::value_to_double(value)?),
                    DataType::String => ob.write_string(Self::value_to_string(value)?),
                    DataType::Object => unimplemented!(),
                    DataType::ByteList => {
                        let list = Self::value_to_array(value, Self::value_to_byte)?;
                        ob.write_byte_list(list.as_deref());
                    }
                    DataType::IntList => {
                        let list = Self::value_to_array(value, Self::value_to_int)?;
                        ob.write_int_list(list.as_deref());
                    }
                    DataType::FloatList => {
                        let list = Self::value_to_array(value, Self::value_to_float)?;
                        ob.write_float_list(list.as_deref());
                    }
                    DataType::LongList => {
                        let list = Self::value_to_array(value, Self::value_to_long)?;
                        ob.write_long_list(list.as_deref());
                    }
                    DataType::DoubleList => {
                        let list = Self::value_to_array(value, Self::value_to_double)?;
                        ob.write_double_list(list.as_deref());
                    }
                    DataType::StringList => {
                        if value.is_null() {
                            ob.write_string_list(None);
                        } else if let Some(value) = value.as_array() {
                            let list: Result<Vec<Option<&str>>> =
                                value.iter().map(Self::value_to_string).collect();
                            ob.write_string_list(Some(list?.as_slice()));
                        } else {
                            return Err(IsarError::InvalidJson {});
                        }
                    }
                    DataType::ObjectList => unimplemented!(),
                }
            } else {
                ob.write_null();
            }
        }

        Ok(ob)
    }

    fn value_to_byte(value: &Value) -> Result<u8> {
        if value.is_null() {
            return Ok(IsarObject::NULL_BYTE);
        } else if let Some(value) = value.as_i64() {
            if value >= 0 && value <= u8::MAX as i64 {
                return Ok(value as u8);
            }
        } else if let Some(value) = value.as_bool() {
            let byte = if value {
                IsarObject::TRUE_BYTE
            } else {
                IsarObject::FALSE_BYTE
            };
            return Ok(byte);
        }
        Err(IsarError::InvalidJson {})
    }

    fn value_to_int(value: &Value) -> Result<i32> {
        if value.is_null() {
            return Ok(IsarObject::NULL_INT);
        } else if let Some(value) = value.as_i64() {
            if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
                return Ok(value as i32);
            }
        }
        Err(IsarError::InvalidJson {})
    }

    fn value_to_float(value: &Value) -> Result<f32> {
        if value.is_null() {
            return Ok(IsarObject::NULL_FLOAT);
        } else if let Some(value) = value.as_f64() {
            if value >= f32::MIN as f64 && value <= f32::MAX as f64 {
                return Ok(value as f32);
            }
        }
        Err(IsarError::InvalidJson {})
    }

    fn value_to_long(value: &Value) -> Result<i64> {
        if value.is_null() {
            Ok(IsarObject::NULL_LONG)
        } else if let Some(value) = value.as_i64() {
            Ok(value)
        } else {
            Err(IsarError::InvalidJson {})
        }
    }

    fn value_to_double(value: &Value) -> Result<f64> {
        if value.is_null() {
            Ok(IsarObject::NULL_DOUBLE)
        } else if let Some(value) = value.as_f64() {
            Ok(value)
        } else {
            Err(IsarError::InvalidJson {})
        }
    }

    fn value_to_string(value: &Value) -> Result<Option<&str>> {
        if value.is_null() {
            Ok(None)
        } else if let Some(value) = value.as_str() {
            Ok(Some(value))
        } else {
            Err(IsarError::InvalidJson {})
        }
    }

    fn value_to_array<T, F>(value: &Value, convert: F) -> Result<Option<Vec<T>>>
    where
        F: Fn(&Value) -> Result<T>,
    {
        if value.is_null() {
            Ok(None)
        } else if let Some(value) = value.as_array() {
            let array: Result<Vec<T>> = value.iter().map(convert).collect();
            Ok(Some(array?))
        } else {
            Err(IsarError::InvalidJson {})
        }
    }
}
