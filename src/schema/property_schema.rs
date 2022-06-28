use crate::object::{data_type::DataType, isar_object::Property};
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug, Eq, Hash)]
pub struct PropertySchema {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) data_type: DataType,
    #[serde(default)]
    #[serde(rename = "target")]
    pub(crate) target_col: Option<String>,
}

impl PropertySchema {
    pub fn new(name: &str, data_type: DataType, target_col: Option<String>) -> PropertySchema {
        PropertySchema {
            name: name.to_string(),
            data_type,
            target_col,
        }
    }

    pub(crate) fn as_property(&self, offset: usize) -> Property {
        Property::new(
            self.name.clone(),
            self.data_type,
            offset,
            self.target_col.clone(),
        )
    }
}
