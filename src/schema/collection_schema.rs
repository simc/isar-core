use crate::error::{schema_error, IsarError, Result};
use crate::object::data_type::DataType;
use crate::object::isar_object::Property;
use crate::schema::index_schema::{IndexSchema, IndexType};
use crate::schema::link_schema::LinkSchema;
use crate::schema::property_schema::PropertySchema;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Eq)]
pub struct CollectionSchema {
    pub(crate) name: String,
    pub(crate) properties: Vec<PropertySchema>,
    #[serde(default)]
    #[serde(skip_serializing)]
    pub(crate) hidden_properties: Vec<String>,
    pub(crate) indexes: Vec<IndexSchema>,
    pub(crate) links: Vec<LinkSchema>,
}

impl PartialEq for CollectionSchema {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl CollectionSchema {
    pub fn new(
        name: &str,
        mut properties: Vec<PropertySchema>,
        mut indexes: Vec<IndexSchema>,
        mut links: Vec<LinkSchema>,
    ) -> CollectionSchema {
        properties.sort_by(|a, b| a.name.cmp(&b.name));
        indexes.sort_by(|a, b| a.name.cmp(&b.name));
        links.sort_by(|a, b| a.name.cmp(&b.name));
        CollectionSchema {
            name: name.to_string(),
            properties,
            hidden_properties: vec![],
            indexes,
            links,
        }
    }

    fn verify_name(name: &str) -> Result<()> {
        if name.is_empty() {
            schema_error("Empty names are not allowed.")
        } else if name.starts_with('_') {
            schema_error("Names must not begin with an underscore.")
        } else {
            Ok(())
        }
    }

    pub(crate) fn verify(&mut self) -> Result<()> {
        Self::verify_name(&self.name)?;

        for property in &self.properties {
            Self::verify_name(&property.name)?;
        }

        for link in &self.links {
            Self::verify_name(&link.name)?;
        }

        let property_names = self.properties.iter().map(|p| p.name.as_str());
        if property_names.unique().count() != self.properties.len() {
            schema_error("Duplicate property name")?;
        }

        let index_names = self.indexes.iter().map(|i| i.name.as_str());
        if index_names.unique().count() != self.indexes.len() {
            schema_error("Duplicate index name")?;
        }

        let link_names = self.links.iter().map(|l| l.name.as_str());
        if link_names.unique().count() != self.links.len() {
            schema_error("Duplicate link name")?;
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
                    .find(|p| p.name == index_property.name);
                if property.is_none() {
                    schema_error("IsarIndex property does not exist")?;
                }
                let property = property.unwrap();

                if property.data_type == DataType::Float
                    || property.data_type == DataType::Double
                    || property.data_type == DataType::FloatList
                    || property.data_type == DataType::DoubleList
                {
                    if index_property.index_type == IndexType::Hash {
                        schema_error("Float values cannot be hashed.")?;
                    } else if i != index.properties.len() - 1 {
                        schema_error(
                            "Float indexes must only be at the end of a composite index.",
                        )?;
                    }
                }

                if property.data_type.get_element_type().is_some() {
                    if index.properties.len() > 1 && index_property.index_type != IndexType::Hash {
                        schema_error("Composite list indexes are not supported.")?;
                    }
                } else if property.data_type == DataType::String
                    && i != index.properties.len() - 1
                    && index_property.index_type != IndexType::Hash
                {
                    schema_error(
                        "Non-hashed string indexes must only be at the end of a composite index.",
                    )?;
                }

                if property.data_type != DataType::String
                    && property.data_type.get_element_type().is_none()
                    && index_property.index_type == IndexType::Hash
                {
                    schema_error("Only string and list indexes may be hashed")?;
                }
                if property.data_type != DataType::StringList
                    && index_property.index_type == IndexType::HashElements
                {
                    schema_error("Only string list indexes may be use hash elements")?;
                }
                if property.data_type != DataType::String
                    && property.data_type != DataType::StringList
                    && index_property.case_sensitive
                {
                    schema_error("Only String and StringList indexes may be case sensitive.")?;
                }
            }
        }

        for link in &self.links {
            Self::verify_name(&link.name)?;
        }

        Ok(())
    }

    pub(crate) fn merge_properties(&mut self, existing: &Self) -> Result<()> {
        let mut properties = existing.properties.clone();
        for property in &self.properties {
            let existing_property = existing.properties.iter().find(|p| p.name == property.name);
            if let Some(existing_property) = existing_property {
                if property.data_type != existing_property.data_type {
                    return Err(IsarError::SchemaError {
                        message: format!(
                            "Property \"{}\" already exists but has a different type",
                            property.name
                        ),
                    });
                }
            } else {
                properties.push(property.clone());
            }
        }
        for property in &existing.properties {
            if !self.properties.contains(property) {
                self.hidden_properties.push(property.name.clone())
            }
        }
        self.properties = properties;

        Ok(())
    }

    pub(crate) fn get_properties(&self) -> Vec<(String, Property)> {
        let mut properties = vec![];
        let mut offset = 2;
        for property_schema in &self.properties {
            if !self.hidden_properties.contains(&property_schema.name) {
                let property = Property::new(property_schema.data_type, offset);
                properties.push((property_schema.name.clone(), property));
            }
            offset += property_schema.data_type.get_static_size();
        }
        properties
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
