use crate::collection::IsarCollection;
use crate::error::{illegal_arg, Result};
use crate::index::Index;
use crate::object::data_type::DataType;
use crate::object::object_id::ObjectId;
use crate::object::object_info::ObjectInfo;
use crate::object::property::Property;
use crate::schema::index_schema::IndexSchema;
use crate::schema::property_schema::PropertySchema;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::cmp;
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CollectionSchema {
    pub(crate) id: Option<u16>,
    pub(crate) name: String,
    pub(crate) properties: Vec<PropertySchema>,
    pub(crate) indexes: Vec<IndexSchema>,
}

impl CollectionSchema {
    pub fn new(name: &str) -> CollectionSchema {
        CollectionSchema {
            id: None,
            name: name.to_string(),
            properties: vec![],
            indexes: vec![],
        }
    }

    pub fn add_property(&mut self, name: &str, data_type: DataType) -> Result<()> {
        if name.is_empty() {
            illegal_arg("Empty properties are not allowed")?;
        }

        if self.properties.iter().any(|f| f.name == name) {
            illegal_arg("Property already exists")?;
        }

        if let Some(previous) = self.properties.last() {
            match data_type.cmp(&previous.data_type) {
                Ordering::Equal => {
                    if name < &previous.name {
                        illegal_arg("Propertys with same type need to be ordered alphabetically")?;
                    }
                }
                Ordering::Less => illegal_arg("Propertys need to be ordered by type")?,
                Ordering::Greater => {}
            }
        }

        self.properties.push(PropertySchema {
            name: name.to_string(),
            data_type,
        });

        Ok(())
    }

    pub fn add_index(
        &mut self,
        property_names: &[&str],
        unique: bool,
        hash_value: bool,
    ) -> Result<()> {
        if property_names.is_empty() {
            illegal_arg("At least one property needs to be added to a valid index.")?;
        }

        if property_names.len() > 3 {
            illegal_arg("No more than three properties may be used as a composite index.")?;
        }

        let properties: Option<Vec<_>> = property_names
            .iter()
            .map(|index_property| {
                self.properties
                    .iter()
                    .find(|p| p.name == *index_property)
                    .cloned()
            })
            .collect();
        if properties.is_none() {
            illegal_arg("Index property does not exist.")?;
        }
        let properties = properties.unwrap();

        let duplicate = self.indexes.iter().any(|i| {
            let min_len = cmp::min(i.properties.len(), properties.len());
            i.properties[..min_len] == properties[..min_len]
        });
        if duplicate {
            illegal_arg("Index already exists.")?;
        }

        let illegal_data_type = properties
            .iter()
            .any(|p| p.data_type.is_dynamic() && p.data_type != DataType::String);
        if illegal_data_type {
            illegal_arg("Illegal index data type.")?;
        }

        let has_string_properties = properties.iter().any(|p| p.data_type == DataType::String);
        if !has_string_properties && hash_value {
            illegal_arg("Only string indexes can be hashed.")?;
        }

        if !hash_value {
            for (index, property) in properties.iter().enumerate() {
                if property.data_type == DataType::String && index < properties.len() - 1 {
                    illegal_arg(
                        "Non-hashed string indexes must only be at the end of a composite index.",
                    )?;
                }
            }
        }

        self.indexes
            .push(IndexSchema::new(properties, unique, hash_value));

        Ok(())
    }

    pub(super) fn get_isar_collection(&self) -> IsarCollection {
        let properties = self.get_properties();
        let indexes = self.get_indexes(&properties);
        let object_info = ObjectInfo::new(properties);
        IsarCollection::new(self.id.unwrap(), self.name.clone(), object_info, indexes)
    }

    fn get_properties(&self) -> Vec<(String, Property)> {
        let oid_offset = ObjectId::get_size();
        let mut offset = oid_offset;

        self.properties
            .iter()
            .map(|f| {
                let size = f.data_type.get_static_size();

                if offset % size != 0 {
                    offset += size - offset % size;
                }
                // padding to align data
                let property = Property::new(f.data_type, offset - oid_offset);
                offset += size;

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
                    .map(|property| {
                        let pos = self.properties.iter().position(|p| property == p).unwrap();
                        let (_, property) = properties.get(pos).unwrap();
                        property
                    })
                    .cloned()
                    .collect_vec();

                Index::new(
                    index.id.unwrap(),
                    properties,
                    index.unique,
                    index.hash_value,
                )
            })
            .collect()
    }

    pub(super) fn update_with_existing_collections(
        &mut self,
        existing_collections: &[CollectionSchema],
        get_id: &mut impl FnMut() -> u16,
    ) {
        let existing_collection = existing_collections.iter().find(|c| c.name == self.name);

        let id = existing_collection.map_or_else(|| get_id(), |e| e.id.unwrap());
        self.id = Some(id);

        let existing_indexes: &[IndexSchema] = existing_collection.map_or(&[], |e| &e.indexes);
        for index in &mut self.indexes {
            index.update_with_existing_indexes(existing_indexes, get_id);
        }
    }
}

#[cfg(test)]
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

        col.add_index(&["byte"], false, false).unwrap();
        col.add_index(&["int"], false, false).unwrap();
        col.add_index(&["float"], false, false).unwrap();
        col.add_index(&["long"], false, false).unwrap();
        col.add_index(&["double"], false, false).unwrap();
        col.add_index(&["str"], false, false).unwrap();
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
