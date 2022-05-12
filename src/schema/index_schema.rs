use crate::index::{IndexProperty, IsarIndex};
use crate::mdbx::db::Db;
use crate::object::isar_object::Property;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Copy, Clone, Eq, PartialEq, Debug, Hash)]
pub enum IndexType {
    Value,
    Hash,
    HashElements,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct IndexPropertySchema {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) index_type: IndexType,
    #[serde(rename = "caseSensitive")]
    pub(crate) case_sensitive: bool,
}

impl IndexPropertySchema {
    pub fn new(name: &str, index_type: IndexType, case_sensitive: bool) -> IndexPropertySchema {
        IndexPropertySchema {
            name: name.to_string(),
            index_type,
            case_sensitive,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct IndexSchema {
    pub(crate) name: String,
    pub(crate) properties: Vec<IndexPropertySchema>,
    pub(crate) unique: bool,
    pub(crate) replace: bool,
}

impl IndexSchema {
    pub fn new(
        name: &str,
        properties: Vec<IndexPropertySchema>,
        unique: bool,
        replace: bool,
    ) -> IndexSchema {
        IndexSchema {
            name: name.to_string(),
            properties,
            unique,
            replace,
        }
    }

    pub(crate) fn as_index(&self, db: Db, properties: &[(String, Property)]) -> IsarIndex {
        let index_properties = self
            .properties
            .iter()
            .map(|p| {
                let (_, property) = properties.iter().find(|(n, _)| &p.name == n).unwrap();
                IndexProperty::new(*property, p.index_type, p.case_sensitive)
            })
            .collect_vec();
        IsarIndex::new(db, index_properties, self.unique, self.replace)
    }
}
