use crate::index::{IndexProperty, IsarIndex};
use crate::mdbx::db::Db;
use crate::object::isar_object::Property;
use enum_ordinalize::Ordinalize;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Copy, Clone, Eq, PartialEq, Serialize_repr, Deserialize_repr, Debug, Ordinalize)]
#[repr(u8)]
pub enum IndexType {
    Value,
    Hash,
    Words,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct IndexPropertySchema {
    #[serde(rename = "name")]
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) index_type: IndexType,
    #[serde(rename = "caseSensitive")]
    pub(crate) case_sensitive: Option<bool>,
}

impl IndexPropertySchema {
    pub fn new(
        name: &str,
        index_type: IndexType,
        case_sensitive: Option<bool>,
    ) -> IndexPropertySchema {
        IndexPropertySchema {
            name: name.to_string(),
            index_type,
            case_sensitive,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq)]
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
                IndexProperty::new(*property, p.index_type, p.case_sensitive)
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
