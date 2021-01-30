use crate::error::{IsarError, Result};
use crate::object::data_type::DataType;
use crate::object::isar_object::IsarObject;
use crate::object::object_builder::ObjectBuilder;
use crate::object::object_id::ObjectId;
use crate::object::object_info::ObjectInfo;
use serde_json::{json, Map, Value};

pub(crate) struct JsonEncodeDecode<'a> {
    col_id: u16,
    object_info: &'a ObjectInfo,
}

impl<'a> JsonEncodeDecode<'a> {
    pub fn new(col_id: u16, object_info: &'a ObjectInfo) -> Self {
        JsonEncodeDecode {
            col_id,
            object_info,
        }
    }

    pub fn encode(
        &self,
        oid: ObjectId,
        object: IsarObject,
        primitive_null: bool,
        byte_as_bool: bool,
    ) -> Value {
        let mut object_map = Map::new();

        let oid_json = match oid.get_type() {
            DataType::String => json!(oid.get_string().unwrap()),
            DataType::Int => json!(oid.get_int().unwrap()),
            DataType::Long => json!(oid.get_long().unwrap()),
            _ => unreachable!(),
        };
        object_map.insert(self.object_info.get_oid_name().to_string(), oid_json);

        for (property_name, property) in self.object_info.get_properties() {
            let property = *property;
            let value =
                if primitive_null && property.data_type.is_static() && object.is_null(property) {
                    Value::Null
                } else {
                    match property.data_type {
                        DataType::Byte => {
                            if byte_as_bool {
                                json!(object.read_bool(property))
                            } else {
                                json!(object.read_byte(property))
                            }
                        }
                        DataType::Int => json!(object.read_int(property)),
                        DataType::Float => json!(object.read_float(property)),
                        DataType::Long => json!(object.read_long(property)),
                        DataType::Double => json!(object.read_double(property)),
                        DataType::String => json!(object.read_string(property)),
                        DataType::ByteList => json!(object.read_byte_list(property)),
                        DataType::IntList => json!(object.read_int_list(property)),
                        DataType::FloatList => json!(object.read_float_list(property)),
                        DataType::LongList => json!(object.read_float_list(property)),
                        DataType::DoubleList => json!(object.read_double_list(property)),
                        DataType::StringList => json!(object.read_string_list(property)),
                    }
                };
            object_map.insert(property_name.clone(), value);
        }
        json!(object_map)
    }

    pub fn decode(
        &self,
        json: &Value,
        buffer: Option<Vec<u8>>,
    ) -> Result<(Option<ObjectId>, ObjectBuilder)> {
        let mut ob = ObjectBuilder::new(self.object_info, buffer);
        let object = json.as_object().ok_or(IsarError::InvalidJson {})?;
        let oid = if let Some(oid_json) = object.get(self.object_info.get_oid_name()) {
            let oid = match (oid_json, self.object_info.get_oid_type()) {
                (Value::String(str), DataType::String) => ObjectId::from_str(self.col_id, str),
                (Value::Number(num), DataType::Int) => {
                    if let Some(num) = num.as_i64() {
                        if num <= i32::MAX as i64 {
                            ObjectId::from_int(self.col_id, num as i32)
                        } else {
                            return Err(IsarError::InvalidJson {});
                        }
                    } else {
                        return Err(IsarError::InvalidJson {});
                    }
                }
                (Value::Number(num), DataType::Long) => {
                    if let Some(num) = num.as_i64() {
                        ObjectId::from_long(self.col_id, num)
                    } else {
                        return Err(IsarError::InvalidJson {});
                    }
                }
                _ => return Err(IsarError::InvalidJson {}),
            };
            Some(oid)
        } else {
            None
        };

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

        Ok((oid, ob))
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
