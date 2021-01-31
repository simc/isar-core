use crate::index::StringIndexType;
use crate::schema::property_schema::PropertySchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexPropertySchema {
    pub(crate) property: PropertySchema,
    #[serde(rename = "stringType")]
    pub(crate) string_type: Option<StringIndexType>,
    #[serde(rename = "stringLowerCase")]
    pub(crate) string_lower_case: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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
        let existing_index = existing_indexes.iter().find(|i| {
            let index_properties_equal =
                i.properties
                    .iter()
                    .zip(self.properties.iter())
                    .all(|(p1, p2)| {
                        p1.property.name == p2.property.name
                            && p1.property.data_type == p2.property.data_type
                            && p1.string_type == p2.string_type
                            && p1.string_lower_case == p2.string_lower_case
                    });
            index_properties_equal && i.unique == self.unique
        });
        if let Some(existing_index) = existing_index {
            self.id = existing_index.id;
        } else {
            self.id = Some(get_id());
        }
    }
}
