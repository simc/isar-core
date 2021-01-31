use crate::collection::IsarCollection;
use crate::error::{illegal_arg, Result};
use crate::index::{Index, IndexProperty, StringIndexType};
use crate::object::data_type::DataType;
use crate::object::isar_object::Property;
use crate::object::object_info::ObjectInfo;
use crate::schema::index_schema::{IndexPropertySchema, IndexSchema};
use crate::schema::property_schema::PropertySchema;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CollectionSchema {
    pub(crate) id: Option<u16>,
    pub(crate) name: String,
    pub(crate) oid_name: String,
    pub(crate) oid_type: DataType,
    pub(crate) properties: Vec<PropertySchema>,
    pub(crate) indexes: Vec<IndexSchema>,
}

impl CollectionSchema {
    pub fn new(name: &str, oid_name: &str, oid_type: DataType) -> CollectionSchema {
        CollectionSchema {
            id: None,
            name: name.to_string(),
            oid_name: oid_name.to_string(),
            oid_type,
            properties: vec![],
            indexes: vec![],
        }
    }

    pub fn add_property(&mut self, name: &str, data_type: DataType) -> Result<()> {
        if name.is_empty() {
            illegal_arg("Empty property names are not allowed")?;
        }

        if self.properties.iter().any(|f| f.name == name) {
            illegal_arg("Property already exists")?;
        }

        self.properties.push(PropertySchema {
            name: name.to_string(),
            data_type,
            offset: None,
        });

        Ok(())
    }

    pub fn add_index(
        &mut self,
        properties: &[(&str, Option<StringIndexType>, bool)],
        unique: bool,
    ) -> Result<()> {
        if properties.is_empty() {
            illegal_arg("At least one property needs to be added to a valid index.")?;
        }

        if properties.len() > 3 {
            illegal_arg("No more than three properties may be used as a composite index.")?;
        }

        let properties: Result<Vec<_>> = properties
            .iter()
            .enumerate()
            .map(|(i, (property_name, string_type, string_lower_case))| {
                let property = self.properties
                    .iter()
                    .find(|p| p.name == *property_name)
                    .cloned();
                if property.is_none() {
                    illegal_arg("Index property does not exist.")?;
                }
                let property = property.unwrap();

                if property.data_type.is_dynamic() && property.data_type != DataType::String{
                    illegal_arg("Illegal index data type.")?;
                }

                if (property.data_type == DataType::String) != string_type.is_some() {
                    illegal_arg("String indexes must have a StringIndexType.")?;
                }

                match string_type {
                    Some(StringIndexType::Value) => {
                        if i != properties.len() -1 {
                            illegal_arg(
                                "Value string indexes must only be at the end of a composite index.",
                            )?;
                        }
                    }
                    Some(StringIndexType::Words) => {
                        if properties.len() > 1 {
                            illegal_arg("Word indexes require a single property")?;
                        }
                    }
                    _ => {}
                }

                Ok(IndexPropertySchema {
                    property,
                    string_type:string_type.clone(),
                    string_lower_case:*string_lower_case,
                })
            })
            .collect();
        let properties = properties?;

        let same_property = self.indexes.iter().any(|i| {
            i.properties.first().unwrap().property.name == properties.first().unwrap().property.name
        });
        if same_property {
            illegal_arg("Another index already exists for this property.")?;
        }

        self.indexes.push(IndexSchema::new(properties, unique));

        Ok(())
    }

    pub(super) fn get_isar_collection(&self) -> IsarCollection {
        let properties = self.get_properties();
        let indexes = self.get_indexes(&properties);
        let object_info = ObjectInfo::new(self.oid_name.clone(), self.oid_type, properties);
        IsarCollection::new(self.id.unwrap(), self.name.clone(), object_info, indexes)
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
                            .find(|(name, _)| name == &ips.property.name)
                            .unwrap();
                        IndexProperty::new(*property, ips.string_type, ips.string_lower_case)
                    })
                    .collect_vec();

                Index::new(index.id.unwrap(), properties, index.unique)
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

        let existing_properties: &[PropertySchema] =
            existing_collection.map_or(&[], |e| &e.properties);
        let mut existing_offset = existing_properties
            .iter()
            .map(|p| p.offset.unwrap() + p.data_type.get_static_size())
            .max()
            .unwrap_or(2);
        for property in &mut self.properties {
            let offset =
                property.update_with_existing_properties(existing_properties, existing_offset);
            if offset > existing_offset {
                existing_offset = offset;
            }
        }

        let existing_indexes: &[IndexSchema] = existing_collection.map_or(&[], |e| &e.indexes);
        for index in &mut self.indexes {
            index.update_with_existing_indexes(existing_indexes, get_id);
        }
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
