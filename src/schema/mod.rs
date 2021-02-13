mod collection_migrator;
pub mod collection_schema;
pub(crate) mod schema_manager;

use crate::collection::IsarCollection;
use crate::error::{schema_error, Result};
use crate::schema::collection_schema::CollectionSchema;
use hashbrown::HashSet;
use itertools::Itertools;
use rand::random;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Schema {
    collections: Vec<CollectionSchema>,
}

impl Schema {
    pub fn new(collections: Vec<CollectionSchema>) -> Result<Schema> {
        let mut schema = Schema { collections };
        schema.verify()?;
        Ok(schema)
    }

    pub fn from_json(json: &[u8]) -> Result<Schema> {
        if let Ok(mut schema) = serde_json::from_slice::<Schema>(json) {
            for col in &mut schema.collections {
                col.id = None;
                for index in &mut col.indexes {
                    index.id = None;
                }
            }
            schema.verify()?;
            Ok(schema)
        } else {
            schema_error("Could not deserialize schema JSON")
        }
    }

    fn verify(&mut self) -> Result<()> {
        if self.collections.iter().unique_by(|c| &c.name).count() != self.collections.len() {
            return schema_error("Duplicate collections");
        }
        for col in &mut self.collections {
            col.verify()?;
        }
        Ok(())
    }

    pub(crate) fn build_collections(self) -> Vec<IsarCollection> {
        self.collections
            .iter()
            .map(|c| c.get_isar_collection())
            .collect()
    }

    fn collect_ids(&self) -> HashSet<u16> {
        let mut ids = HashSet::<u16>::new();
        for collection in &self.collections {
            if let Some(id) = collection.id {
                assert!(ids.insert(id), "Schema contains duplicate id.");
            }
            for index in &collection.indexes {
                if let Some(id) = index.id {
                    assert!(ids.insert(id), "Schema contains duplicate id.");
                }
            }
        }
        ids
    }

    fn update_with_existing_schema_internal(
        &mut self,
        existing_schema: Option<&Schema>,
        mut random: impl FnMut() -> u16,
    ) -> Result<()> {
        let mut ids = if let Some(existing_schema) = existing_schema {
            existing_schema.collect_ids()
        } else {
            HashSet::new()
        };

        let mut find_id = || loop {
            let id = random();
            if ids.insert(id) {
                return id;
            }
        };

        let empty = vec![];
        let existing_collections = existing_schema.map_or(&empty, |c| &c.collections);
        for collection in &mut self.collections {
            let existing_collection = existing_collections
                .iter()
                .find(|c| c.name == collection.name);
            collection.update_with_existing_collections(existing_collection, &mut find_id)?;
        }
        Ok(())
    }

    pub fn update_with_existing_schema(&mut self, existing_schema: Option<&Schema>) -> Result<()> {
        self.update_with_existing_schema_internal(existing_schema, random)
    }
}

/*#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::data_type::DataType;

    #[test]
    fn test_add_collection() {
        let mut schema = Schema::new();

        let col1 = CollectionSchema::new("col");
        schema.add_collection(col1).unwrap();

        let col2 = CollectionSchema::new("other");
        schema.add_collection(col2).unwrap();

        let duplicate = CollectionSchema::new("col");
        assert!(schema.add_collection(duplicate).is_err());
    }

    #[test]
    fn test_update_with_existing_schema() -> Result<()> {
        let mut schema1 = Schema::new();
        let mut col = CollectionSchema::new("col");
        col.add_property("byteProperty", DataType::Byte)?;
        col.add_property("intProperty", DataType::Int)?;
        col.add_property("longProperty", DataType::Long)?;
        col.add_property("stringProperty", DataType::String)?;
        col.add_index(&["byteProperty"], false, false)?;
        col.add_index(&["intProperty", "byteProperty"], true, false)?;
        col.add_index(&["longProperty"], false, false)?;
        col.add_index(&["intProperty", "longProperty"], false, false)?;
        col.add_index(&["stringProperty"], false, true)?;
        schema1.add_collection(col)?;

        let mut counter = 0;
        let get_id = || {
            counter += 1;
            counter
        };
        schema1.update_with_existing_schema_internal(None, get_id);
        let col = &schema1.collections[0];
        assert_eq!(col.id, Some(1));
        assert_eq!(col.indexes[0].id, Some(2));
        assert_eq!(col.indexes[1].id, Some(3));
        assert_eq!(col.indexes[2].id, Some(4));
        assert_eq!(col.indexes[3].id, Some(5));
        assert_eq!(col.indexes[4].id, Some(6));

        let mut schema2 = Schema::new();
        let mut col = CollectionSchema::new("col");
        col.add_property("byteProperty", DataType::Byte)?;
        col.add_property("intProperty", DataType::Int)?;
        col.add_property("longProperty", DataType::Double)?; // changed type
        col.add_property("stringProperty", DataType::String)?;
        col.add_index(&["byteProperty"], false, false)?;
        col.add_index(&["intProperty", "byteProperty"], false, false)?; // changed unique
        col.add_index(&["longProperty"], false, false)?; // changed property type
        col.add_index(&["intProperty", "longProperty"], false, false)?; // changed property type-
        col.add_index(&["stringProperty"], false, false)?; // changed hash_value
        schema2.add_collection(col)?;

        let mut counter = 0;
        let get_id = || {
            counter += 1;
            counter
        };
        schema2.update_with_existing_schema_internal(Some(&schema1), get_id);
        let col = &schema2.collections[0];
        assert_eq!(col.id, Some(1));
        assert_eq!(col.indexes[0].id, Some(2));
        assert_eq!(col.indexes[1].id, Some(7));
        assert_eq!(col.indexes[2].id, Some(8));
        assert_eq!(col.indexes[3].id, Some(9));
        assert_eq!(col.indexes[4].id, Some(10));

        Ok(())
    }
}
*/
