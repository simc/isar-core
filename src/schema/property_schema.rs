use crate::object::{data_type::DataType, isar_object::Property};
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug, Eq, Hash)]
pub struct PropertySchema {
    pub(crate) name: Option<String>,
    #[serde(rename = "type")]
    pub(crate) data_type: DataType,
    #[serde(default)]
    #[serde(rename = "target")]
    pub(crate) target_col: Option<String>,
}

impl PropertySchema {
    pub fn new(
        name: Option<String>,
        data_type: DataType,
        target_col: Option<String>,
    ) -> PropertySchema {
        PropertySchema {
            name,
            data_type,
            target_col,
        }
    }

    pub(crate) fn as_property(&self, offset: usize) -> Option<Property> {
        if let Some(name) = &self.name {
            let p = Property::new(
                name.clone(),
                self.data_type,
                offset,
                self.target_col.clone(),
            );
            Some(p)
        } else {
            None
        }
    }
}
