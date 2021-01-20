use crate::error::{IsarError, Result};
use crate::object::data_type::DataType;
use crate::object::object_builder::{ObjectBuilder, ObjectBuilderBytes};
use crate::object::object_id::ObjectId;
use crate::object::object_info::ObjectInfo;
use crate::object::property::Property;
use serde_json::{json, Map, Value};

pub(crate) struct JsonEncodeDecode<'a> {
    object_info: &'a ObjectInfo,
}

impl<'a> JsonEncodeDecode<'a> {
    pub fn new(object_info: &'a ObjectInfo) -> Self {
        JsonEncodeDecode { object_info }
    }

    pub fn encode(
        &self,
        oid: ObjectId,
        object: &[u8],
        primitive_null: bool,
        byte_as_bool: bool,
    ) -> Value {
        let mut object_map = Map::new();

        object_map.insert("id".to_string(), json!(oid.to_string()));

        for (property_name, property) in self.object_info.get_properties() {
            let value =
                if primitive_null && property.data_type.is_static() && property.is_null(object) {
                    Value::Null
                } else {
                    match property.data_type {
                        DataType::Byte => {
                            if byte_as_bool {
                                json!(property.get_bool(object))
                            } else {
                                json!(property.get_byte(object))
                            }
                        }
                        DataType::Int => json!(property.get_int(object)),
                        DataType::Float => json!(property.get_float(object)),
                        DataType::Long => json!(property.get_long(object)),
                        DataType::Double => json!(property.get_double(object)),
                        DataType::String => json!(property.get_string(object)),
                        DataType::ByteList => json!(property.get_byte_list(object)),
                        DataType::IntList => json!(property.get_int_list(object)),
                        DataType::FloatList => json!(property.get_float_list(object)),
                        DataType::LongList => json!(property.get_float_list(object)),
                        DataType::DoubleList => json!(property.get_double_list(object)),
                        DataType::StringList => json!(property.get_string_list(object)),
                    }
                };
            object_map.insert(property_name.clone(), value);
        }
        json!(object_map)
    }

    pub fn decode(
        &self,
        json: &Value,
        bytes: Option<ObjectBuilderBytes>,
    ) -> Result<(ObjectId, ObjectBuilderBytes)> {
        let mut ob = ObjectBuilder::new(self.object_info, bytes);

        let object = json.as_object().ok_or(IsarError::InvalidJson {})?;
        let oid_str = object
            .get("id")
            .map(|id_str| id_str.as_str())
            .ok_or(IsarError::InvalidJson {})?
            .ok_or(IsarError::InvalidJson {})?;
        let oid = oid_str.parse()?;

        for (name, property) in self.object_info.get_properties() {
            if let Some(value) = object.get(name) {
                match property.data_type {
                    DataType::Byte => ob.write_byte(Self::value_to_byte(value)?),
                    DataType::Int => ob.write_int(Self::value_to_int(value)?),
                    DataType::Float => ob.write_float(Self::value_to_float(value)?),
                    DataType::Long => ob.write_long(Self::value_to_long(value)?),
                    DataType::Double => ob.write_double(Self::value_to_double(value)?),
                    DataType::String => ob.write_string(Self::value_to_string(value)?),
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
                }
            } else {
                ob.write_null();
            }
        }

        Ok((oid, ob.finish()))
    }

    fn value_to_byte(value: &Value) -> Result<u8> {
        if value.is_null() {
            return Ok(Property::NULL_BYTE);
        } else if let Some(value) = value.as_i64() {
            if value >= 0 && value <= u8::MAX as i64 {
                return Ok(value as u8);
            }
        } else if let Some(value) = value.as_bool() {
            let byte = if value {
                Property::TRUE_BYTE
            } else {
                Property::FALSE_BYTE
            };
            return Ok(byte);
        }
        Err(IsarError::InvalidJson {})
    }

    fn value_to_int(value: &Value) -> Result<i32> {
        if value.is_null() {
            return Ok(Property::NULL_INT);
        } else if let Some(value) = value.as_i64() {
            if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
                return Ok(value as i32);
            }
        }
        Err(IsarError::InvalidJson {})
    }

    fn value_to_float(value: &Value) -> Result<f32> {
        if value.is_null() {
            return Ok(Property::NULL_FLOAT);
        } else if let Some(value) = value.as_f64() {
            if value >= f32::MIN as f64 && value <= f32::MAX as f64 {
                return Ok(value as f32);
            }
        }
        Err(IsarError::InvalidJson {})
    }

    fn value_to_long(value: &Value) -> Result<i64> {
        if value.is_null() {
            Ok(Property::NULL_LONG)
        } else if let Some(value) = value.as_i64() {
            Ok(value)
        } else {
            Err(IsarError::InvalidJson {})
        }
    }

    fn value_to_double(value: &Value) -> Result<f64> {
        if value.is_null() {
            Ok(Property::NULL_DOUBLE)
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
