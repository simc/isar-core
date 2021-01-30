use crate::index::StringIndexType;
use crate::schema::property_schema::PropertySchema;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct IndexPropertySchema {
    pub(crate) property: PropertySchema,
    #[serde(rename = "stringType")]
    pub(crate) string_type: Option<StringIndexType>,
    #[serde(rename = "stringLowerCase")]
    pub(crate) string_lower_case: bool,
}

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct IndexSchema {
    pub(crate) id: Option<u16>,
    pub(crate) properties: Vec<IndexPropertySchema>,
    pub(crate) unique: bool,
}

impl IndexSchema {
    pub fn new(properties: Vec<IndexPropertySchema>, unique: bool) -> IndexSchema {
        IndexSchema {
            id: None,
            properties,
            unique,
        }
    }

    pub(crate) fn update_with_existing_indexes<F>(
        &mut self,
        existing_indexes: &[IndexSchema],
        get_id: &mut F,
    ) where
        F: FnMut() -> u16,
    {
        let existing_index = existing_indexes
            .iter()
            .find(|i| i.properties == self.properties && i.unique == self.unique);
        if let Some(existing_index) = existing_index {
            self.id = existing_index.id;
        } else {
            self.id = Some(get_id());
        }
    }
}
