use crate::object::data_type::DataType;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct PropertySchema {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) data_type: DataType,
    pub(crate) offset: Option<usize>,
}

impl PropertySchema {
    pub fn new(name: &str, data_type: DataType) -> PropertySchema {
        PropertySchema {
            name: name.to_string(),
            data_type,
            offset: None,
        }
    }

    pub(crate) fn update_with_existing_properties(
        &mut self,
        existing_properties: &[PropertySchema],
        existing_offset: usize,
    ) -> usize {
        let existing_property = existing_properties.iter().find(|i| i.name == self.name);
        if let Some(existing_property) = existing_property {
            self.offset = existing_property.offset;
        } else {
            self.offset = Some(existing_offset);
        }
        self.offset.unwrap() + self.data_type.get_static_size()
    }
}
