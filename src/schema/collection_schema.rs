use crate::collection::IsarCollection;
use crate::error::{schema_error, Result};
use crate::index::{Index, IndexProperty};
use crate::link::Link;
use crate::object::data_type::DataType;
use crate::object::isar_object::Property;
use crate::object::object_info::ObjectInfo;
use enum_ordinalize::Ordinalize;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

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
}

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
    #[serde(rename = "indexType")]
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexSchema {
    pub(crate) id: Option<u16>,
    pub(crate) properties: Vec<IndexPropertySchema>,
    pub(crate) unique: bool,
    pub(crate) replace: bool,
}

impl IndexSchema {
    pub fn new(properties: Vec<IndexPropertySchema>, unique: bool, replace: bool) -> IndexSchema {
        IndexSchema {
            id: None,
            properties,
            unique,
            replace,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LinkSchema {
    pub(crate) id: Option<u16>,
    #[serde(rename = "backlinkId")]
    pub(crate) backlink_id: Option<u16>,
    pub(crate) name: String,
    #[serde(rename = "collection")]
    pub(crate) target_col: String,
}

impl LinkSchema {
    pub fn new(name: &str, target_collection_name: &str) -> Self {
        LinkSchema {
            id: None,
            backlink_id: None,
            name: name.to_string(),
            target_col: target_collection_name.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CollectionSchema {
    pub(crate) id: Option<u16>,
    pub(crate) name: String,
    #[serde(rename = "idProperty")]
    pub(crate) id_property: String,
    pub(crate) properties: Vec<PropertySchema>,
    pub(crate) indexes: Vec<IndexSchema>,
    pub(crate) links: Vec<LinkSchema>,
}

impl CollectionSchema {
    pub fn new(
        name: &str,
        id_property: &str,
        properties: Vec<PropertySchema>,
        indexes: Vec<IndexSchema>,
        links: Vec<LinkSchema>,
    ) -> CollectionSchema {
        CollectionSchema {
            id: None,
            name: name.to_string(),
            id_property: id_property.to_string(),
            properties,
            indexes,
            links,
        }
    }

    pub(crate) fn verify(&mut self) -> Result<()> {
        if self.name.is_empty() {
            schema_error("Empty collection names are not allowed")?;
        }

        let properties_link_names = self
            .properties
            .iter()
            .map(|p| &p.name)
            .chain(self.links.iter().map(|l| &l.name));
        if properties_link_names.unique().count() < self.properties.len() + self.links.len() {
            schema_error("Duplicate property or link name")?;
        }

        let mut has_oid = false;
        for property in &mut self.properties {
            if property.name.is_empty() {
                schema_error("Empty property names are not allowed")?;
            }
            if property.name == self.id_property {
                if property.data_type != DataType::Long {
                    schema_error("Illegal ObjectId type")?;
                }
                has_oid = true;
            }
            property.offset = None
        }
        if !has_oid {
            schema_error("Unknown ObjectId property")?;
        }

        for index in &self.indexes {
            if index.properties.is_empty() {
                schema_error("At least one property needs to be added to a valid index")?;
            } else if index.properties.len() > 3 {
                schema_error("No more than three properties may be used as a composite index")?;
            }

            for (i, index_property) in index.properties.iter().enumerate() {
                let property = self
                    .properties
                    .iter()
                    .find(|p| p.name == index_property.name)
                    .cloned();
                if property.is_none() {
                    schema_error("Index property does not exist")?;
                }
                let property = property.unwrap();

                if property.data_type.is_dynamic() && property.data_type != DataType::String {
                    schema_error("Illegal index data type")?;
                }

                if property.data_type != DataType::String
                    && index_property.index_type != IndexType::Value
                {
                    schema_error("Non string indexes must use IndexType::Value")?;
                }
                if (property.data_type == DataType::String)
                    != index_property.case_sensitive.is_some()
                {
                    schema_error("Only String indexes must have case sensitivity.")?;
                }

                match index_property.index_type {
                    IndexType::Value | IndexType::Words => {
                        if i != index.properties.len() - 1 {
                            schema_error(
                                "Value and word string indexes must only be at the end of a composite index.",
                            )?;
                        }
                    }
                    _ => {}
                }
            }
        }

        for link in &self.links {
            if link.name.is_empty() {
                schema_error("Empty link names are not allowed")?;
            }
        }

        Ok(())
    }

    pub(super) fn get_isar_collection(&self, cols: &[CollectionSchema]) -> IsarCollection {
        let properties = self.get_properties();
        let indexes = self.get_indexes(&properties);
        let links = self.get_links(cols);
        let backlinks = self.get_backlinks(cols);

        let (_, id_property) = properties
            .iter()
            .find(|(name, _)| name == &self.id_property)
            .unwrap();

        let oi = ObjectInfo::new(*id_property, properties);
        IsarCollection::new(
            self.id.unwrap(),
            self.name.clone(),
            oi,
            indexes,
            links,
            backlinks,
        )
    }

    fn get_properties(&self) -> Vec<(String, Property)> {
        self.properties
            .iter()
            .map(|f| {
                let property = Property::new(f.data_type, f.offset.unwrap());
                (f.name.clone(), property)
            })
            .collect()
    }

    fn get_indexes(&self, properties: &[(String, Property)]) -> Vec<Index> {
        self.indexes
            .iter()
            .map(|index| {
                let properties = index
                    .properties
                    .iter()
                    .map(|ips| {
                        let (_, property) = properties
                            .iter()
                            .find(|(name, _)| name == &ips.name)
                            .unwrap();
                        IndexProperty::new(*property, ips.index_type, ips.case_sensitive)
                    })
                    .collect_vec();

                Index::new(
                    index.id.unwrap(),
                    self.id.unwrap(),
                    properties,
                    index.unique,
                    index.replace,
                )
            })
            .collect()
    }

    fn get_links(&self, cols: &[CollectionSchema]) -> Vec<Link> {
        self.links
            .iter()
            .map(|l| {
                let target_col_id = cols
                    .iter()
                    .find(|c| c.name == l.target_col)
                    .unwrap()
                    .id
                    .unwrap();
                Link::new(
                    l.id.unwrap(),
                    l.backlink_id.unwrap(),
                    self.id.unwrap(),
                    target_col_id,
                )
            })
            .collect()
    }

    fn get_backlinks(&self, cols: &[CollectionSchema]) -> Vec<Link> {
        cols.iter()
            .filter(|c| c.id != self.id)
            .flat_map(|c| {
                c.links
                    .iter()
                    .filter(|l| l.target_col == self.name)
                    .map(|l| {
                        Link::new(
                            l.backlink_id.unwrap(),
                            l.id.unwrap(),
                            c.id.unwrap(),
                            self.id.unwrap(),
                        )
                    })
                    .collect_vec()
            })
            .collect()
    }

    fn find_next_offset(properties: &[PropertySchema]) -> usize {
        properties
            .iter()
            .map(|p| p.offset.unwrap() + p.data_type.get_static_size())
            .max()
            .unwrap_or(2)
    }

    fn check_indexes_equal(
        index1: &IndexSchema,
        properties1: &[PropertySchema],
        index2: &IndexSchema,
        properties2: &[PropertySchema],
    ) -> bool {
        if index1.unique != index2.unique || index1.properties.len() != index2.properties.len() {
            return false;
        }
        for (ip1, ip2) in index1.properties.iter().zip(index2.properties.iter()) {
            if ip1 != ip2 {
                return false;
            }
            let p1 = properties1.iter().find(|p| p.name == ip1.name).unwrap();
            let p2 = properties2.iter().find(|p| p.name == ip2.name).unwrap();
            if p1 != p2 {
                return false;
            }
        }
        true
    }

    pub(super) fn update_with_existing_collection(
        &mut self,
        existing_col: Option<&CollectionSchema>,
        get_id: &mut impl FnMut() -> u16,
    ) -> Result<()> {
        if let Some(existing_col) = existing_col {
            self.id = existing_col.id;
            if existing_col.id_property != self.id_property {
                return schema_error("The id property must not change between versions.");
            }
        } else {
            self.id = Some(get_id());
        }

        let existing_properties: &[PropertySchema] = existing_col.map_or(&[], |e| &e.properties);
        let mut next_offset = Self::find_next_offset(existing_properties);
        for property in &mut self.properties {
            let existing_property = existing_properties.iter().find(|i| i.name == property.name);
            if let Some(existing_property) = existing_property {
                property.offset = existing_property.offset;
            } else {
                property.offset = Some(next_offset);
                next_offset += property.data_type.get_static_size();
            }
        }

        let existing_indexes: &[IndexSchema] = existing_col.map_or(&[], |e| &e.indexes);
        let properties = &self.properties;
        for index in &mut self.indexes {
            let existing_index = existing_indexes
                .iter()
                .find(|i| Self::check_indexes_equal(index, properties, i, existing_properties));
            if let Some(existing_index) = existing_index {
                index.id = existing_index.id;
            } else {
                index.id = Some(get_id());
            }
        }

        let existing_links: &[LinkSchema] = existing_col.map_or(&[], |e| &e.links);
        for link in &mut self.links {
            let existing_link = existing_links
                .iter()
                .find(|l| l.name == link.name && l.target_col == link.target_col);
            if let Some(existing_link) = existing_link {
                link.id = existing_link.id;
                link.backlink_id = existing_link.backlink_id;
            } else {
                link.id = Some(get_id());
                link.backlink_id = Some(get_id());
            }
        }

        Ok(())
    }
}

/*#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_property_empty_name() {
        let mut col = CollectionSchema::new("col");
        assert!(col.add_property("", DataType::Int).is_err())
    }

    #[test]
    fn test_add_property_duplicate_name() {
        let mut col = CollectionSchema::new("col");
        col.add_property("prop", DataType::Int).unwrap();
        assert!(col.add_property("prop", DataType::Int).is_err())
    }

    #[test]
    fn test_add_property_same_type_wrong_order() {
        let mut col = CollectionSchema::new("col");

        col.add_property("b", DataType::Int).unwrap();
        assert!(col.add_property("a", DataType::Int).is_err())
    }

    #[test]
    fn test_add_property_wrong_order() {
        let mut col = CollectionSchema::new("col");

        col.add_property("a", DataType::Long).unwrap();
        assert!(col.add_property("b", DataType::Int).is_err())
    }

    #[test]
    fn test_add_index_without_properties() {
        let mut col = CollectionSchema::new("col");

        assert!(col.add_index(&[], false, false).is_err())
    }

    #[test]
    fn test_add_index_with_non_existing_property() {
        let mut col = CollectionSchema::new("col");
        col.add_property("prop1", DataType::Int).unwrap();

        col.add_index(&["prop1"], false, false).unwrap();
        assert!(col.add_index(&["wrongprop"], false, false).is_err())
    }

    #[test]
    fn test_add_index_with_illegal_data_type() {
        let mut col = CollectionSchema::new("col");
        col.add_property("byte", DataType::Byte).unwrap();
        col.add_property("int", DataType::Int).unwrap();
        col.add_property("float", DataType::Float).unwrap();
        col.add_property("long", DataType::Long).unwrap();
        col.add_property("double", DataType::Double).unwrap();
        col.add_property("str", DataType::String).unwrap();
        col.add_property("byteList", DataType::ByteList).unwrap();
        col.add_property("intList", DataType::IntList).unwrap();

        col.add_index(&["byte"], false, None, false).unwrap();
        col.add_index(&["int"], false, None, false).unwrap();
        col.add_index(&["float"], false, None, false).unwrap();
        col.add_index(&["long"], false, None, false).unwrap();
        col.add_index(&["double"], false, None, false).unwrap();
        col.add_index(&["str"], false, Some(StringIndexType::Value), false)
            .unwrap();
        assert!(col.add_index(&["byteList"], false, false).is_err());
        assert!(col.add_index(&["intList"], false, false).is_err());
    }

    #[test]
    fn test_add_index_too_many_properties() {
        let mut col = CollectionSchema::new("col");
        col.add_property("prop1", DataType::Int).unwrap();
        col.add_property("prop2", DataType::Int).unwrap();
        col.add_property("prop3", DataType::Int).unwrap();
        col.add_property("prop4", DataType::Int).unwrap();

        assert!(col
            .add_index(&["prop1", "prop2", "prop3", "prop4"], false, false)
            .is_err())
    }

    #[test]
    fn test_add_duplicate_index() {
        let mut col = CollectionSchema::new("col");
        col.add_property("prop1", DataType::Int).unwrap();
        col.add_property("prop2", DataType::Int).unwrap();

        col.add_index(&["prop2"], false, false).unwrap();
        col.add_index(&["prop1", "prop2"], false, false).unwrap();
        assert!(col.add_index(&["prop1", "prop2"], false, false).is_err());
        assert!(col.add_index(&["prop1"], false, false).is_err());
    }

    #[test]
    fn test_add_composite_index_with_non_hashed_string_in_the_middle() {
        let mut col = CollectionSchema::new("col");
        col.add_property("int", DataType::Int).unwrap();
        col.add_property("str", DataType::String).unwrap();

        col.add_index(&["int", "str"], false, false).unwrap();
        assert!(col.add_index(&["str", "int"], false, false).is_err());
        col.add_index(&["str", "int"], false, true).unwrap();
    }

    #[test]
    fn test_properties_have_correct_offset() {
        fn get_offsets(mut schema: CollectionSchema) -> Vec<usize> {
            let mut get_id = || 1;
            schema.update_with_existing_collections(&[], &mut get_id);
            let col = schema.get_isar_collection();
            let mut offsets = vec![];
            for i in 0..schema.properties.len() {
                let (_, p) = col.get_properties().get(i).unwrap();
                offsets.push(p.offset);
            }
            offsets
        }

        let mut col = CollectionSchema::new("col");
        col.add_property("byte", DataType::Byte).unwrap();
        col.add_property("int", DataType::Int).unwrap();
        col.add_property("double", DataType::Double).unwrap();
        assert_eq!(get_offsets(col), vec![0, 2, 10]);

        let mut col = CollectionSchema::new("col");
        col.add_property("byte1", DataType::Byte).unwrap();
        col.add_property("byte2", DataType::Byte).unwrap();
        col.add_property("byte3", DataType::Byte).unwrap();
        col.add_property("str", DataType::String).unwrap();
        assert_eq!(get_offsets(col), vec![0, 1, 2, 10]);

        let mut col = CollectionSchema::new("col");
        col.add_property("byteList", DataType::ByteList).unwrap();
        col.add_property("intList", DataType::IntList).unwrap();
        col.add_property("doubleList", DataType::DoubleList)
            .unwrap();
        assert_eq!(get_offsets(col), vec![2, 10, 18]);
    }

    #[test]
    fn update_with_no_existing_collection() {
        let mut col = CollectionSchema::new("col");
        col.add_property("byte", DataType::Byte).unwrap();
        col.add_property("int", DataType::Int).unwrap();
        col.add_index(&["byte"], true, false).unwrap();
        col.add_index(&["int"], true, false).unwrap();

        let mut counter = 0;
        let mut get_id = || {
            counter += 1;
            counter
        };
        col.update_with_existing_collections(&[], &mut get_id);

        assert_eq!(col.id, Some(1));
        assert_eq!(col.indexes[0].id, Some(2));
        assert_eq!(col.indexes[1].id, Some(3));
    }

    #[test]
    fn update_with_existing_collection() {
        let mut counter = 0;
        let mut get_id = || {
            counter += 1;
            counter
        };

        let mut col1 = CollectionSchema::new("col");
        col1.add_property("byte", DataType::Byte).unwrap();
        col1.add_property("int", DataType::Int).unwrap();
        col1.add_index(&["byte"], true, false).unwrap();
        col1.add_index(&["int"], true, false).unwrap();

        col1.update_with_existing_collections(&[], &mut get_id);
        assert_eq!(col1.id, Some(1));
        assert_eq!(col1.indexes[0].id, Some(2));
        assert_eq!(col1.indexes[1].id, Some(3));

        let mut col2 = CollectionSchema::new("col");
        col2.add_property("byte", DataType::Byte).unwrap();
        col2.add_property("int", DataType::Int).unwrap();
        col2.add_index(&["byte"], true, false).unwrap();
        col2.add_index(&["int", "byte"], true, false).unwrap();

        col2.update_with_existing_collections(&[col1], &mut get_id);
        assert_eq!(col2.id, Some(1));
        assert_eq!(col2.indexes[0].id, Some(2));
        assert_eq!(col2.indexes[1].id, Some(4));

        let mut col3 = CollectionSchema::new("col3");
        col3.update_with_existing_collections(&[col2], &mut get_id);
        assert_eq!(col3.id, Some(5));
    }
}
*/
