use crate::index::{IndexProperty, IsarIndex};
use crate::mdbx::db::Db;
use crate::object::isar_object::Property;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct IndexPropertySchema {
    pub(crate) name: String,
    pub(crate) hash: bool,
    #[serde(rename = "hashElements")]
    pub(crate) hash_elements: bool,
    #[serde(rename = "caseSensitive")]
    pub(crate) case_sensitive: bool,
}

impl IndexPropertySchema {
    pub fn new(
        name: &str,
        hash: bool,
        hash_elements: bool,
        case_sensitive: bool,
    ) -> IndexPropertySchema {
        IndexPropertySchema {
            name: name.to_string(),
            hash,
            hash_elements,
            case_sensitive,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, Hash)]
pub struct IndexSchema {
    pub(crate) properties: Vec<IndexPropertySchema>,
    pub(crate) unique: bool,
    pub(crate) replace: bool,
}

impl IndexSchema {
    pub fn new(properties: Vec<IndexPropertySchema>, unique: bool, replace: bool) -> IndexSchema {
        IndexSchema {
            properties,
            unique,
            replace,
        }
    }

    pub(crate) fn as_index(
        &self,
        db: Db,
        properties: &[Property],
        property_names: &[String],
    ) -> IsarIndex {
        let index_properties = self
            .properties
            .iter()
            .map(|p| {
                let property_index = property_names.iter().position(|n| &p.name == n).unwrap();
                let property = properties.get(property_index).unwrap();
                IndexProperty::new(*property, p.hash, p.hash_elements, p.case_sensitive)
            })
            .collect_vec();
        IsarIndex::new(db, index_properties, self.unique, self.replace)
    }
}

impl PartialEq<Self> for IndexSchema {
    fn eq(&self, other: &Self) -> bool {
        self.unique == other.unique && self.properties == other.properties
    }
}
