use super::data_type::DataType;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Property {
    pub name: String,
    pub data_type: DataType,
    pub offset: usize,
}

impl Property {
    pub const fn new(name: String, data_type: DataType, offset: usize) -> Self {
        Property {
            name,
            data_type,
            offset,
        }
    }

    pub const fn debug(data_type: DataType, offset: usize) -> Self {
        Property {
            name: String::new(),
            data_type,
            offset,
        }
    }
}
