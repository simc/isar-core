use xxhash_rust::xxh3::xxh3_64;

use super::data_type::DataType;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Property {
    pub name: String,
    pub id: u64,
    pub data_type: DataType,
    pub offset: usize,
    pub target_id: Option<u64>,
}

impl Property {
    pub fn new(name: &str, data_type: DataType, offset: usize, target_id: Option<&str>) -> Self {
        let id = xxh3_64(name.as_bytes());
        let target_id = target_id.map(|col| xxh3_64(col.as_bytes()));
        Property {
            name: name.to_string(),
            id,
            data_type,
            offset,
            target_id,
        }
    }

    pub const fn debug(data_type: DataType, offset: usize) -> Self {
        Property {
            name: String::new(),
            id: 0,
            data_type,
            offset,
            target_id: None,
        }
    }
}
