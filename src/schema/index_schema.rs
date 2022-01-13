use crate::index::{IndexProperty, IsarIndex};
use crate::mdbx::db::Db;
use crate::object::isar_object::Property;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Copy, Clone, Eq, PartialEq, Debug)]
pub enum IndexType {
    Value,
    Hash,
    HashElements,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct IndexPropertySchema {
    pub(crate) name: Option<String>,
    #[serde(rename = "type")]
    pub(crate) index_type: IndexType,
    #[serde(rename = "caseSensitive")]
    pub(crate) case_sensitive: bool,
}

impl IndexPropertySchema {
    pub fn new(
        name: Option<&str>,
        index_type: IndexType,
        case_sensitive: bool,
    ) -> IndexPropertySchema {
        IndexPropertySchema {
            name: name.map(|name| name.to_string()),
            index_type,
            case_sensitive,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct IndexSchema {
    pub(crate) name: String,
    pub(crate) properties: Vec<IndexPropertySchema>,
    pub(crate) unique: bool,
}

impl IndexSchema {
    pub fn new(name: &str, properties: Vec<IndexPropertySchema>, unique: bool) -> IndexSchema {
        IndexSchema {
            name: name.to_string(),
            properties,
            unique,
        }
    }

    pub(crate) fn as_index(&self, db: Db, properties: &[(String, Property)]) -> IsarIndex {
        let index_properties = self
            .properties
            .iter()
            .map(|p| {
                let property = if let Some(ref name) = p.name {
                    Some(properties.iter().find(|(n, _)| n == name).unwrap().1)
                } else {
                    None
                };
                IndexProperty::new(property, p.index_type, p.case_sensitive)
            })
            .collect_vec();
        IsarIndex::new(db, index_properties, self.unique)
    }
}
