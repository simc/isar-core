use enum_ordinalize::Ordinalize;
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(PartialEq, Eq, Clone, Copy, Serialize_repr, Deserialize_repr, Debug, Ordinalize)]
#[repr(u8)]
pub enum DataType {
    Byte = 0,
    Int = 1,
    Float = 2,
    Long = 3,
    Double = 4,
    String = 5,
    ByteList = 6,
    IntList = 7,
    FloatList = 8,
    LongList = 9,
    DoubleList = 10,
    StringList = 11,
}

impl DataType {
    pub fn is_static(&self) -> bool {
        matches!(
            &self,
            DataType::Int | DataType::Long | DataType::Float | DataType::Double | DataType::Byte
        )
    }

    pub fn is_dynamic(&self) -> bool {
        !self.is_static()
    }

    pub fn get_static_size(&self) -> usize {
        match *self {
            DataType::Byte => 1,
            DataType::Int | DataType::Float => 4,
            _ => 8,
        }
    }
}
