use crate::collection::IsarCollection;
use crate::error::Result;
use crate::index::Index;
use crate::object::object_builder::ObjectBuilder;
use crate::object::property::Property;
use crate::txn::Cursors;

pub(crate) struct CollectionMigrator<'a> {
    retained_properties: Vec<Option<&'a Property>>,
    collection: &'a IsarCollection,
    object_migration_required: bool,
    removed_indexes: Vec<&'a Index>,
    added_indexes: Vec<&'a Index>,
}

impl<'a> CollectionMigrator<'a> {
    pub fn create(collection: &'a IsarCollection, existing_collection: &'a IsarCollection) -> Self {
        let properties = collection.get_properties();
        let existing_properties = existing_collection.get_properties();

        let mut retained_properties = vec![];
        for (name, property) in properties {
            let existing_property = existing_properties
                .iter()
                .find(|(e_name, e_property)| {
                    name == e_name && property.data_type == e_property.data_type
                })
                .map(|(_, p)| p);
            retained_properties.push(existing_property);
        }
        let object_migration_required = retained_properties.iter().any(|p| p.is_none());

        let added_indexes = Self::get_diff_indexes(collection, existing_collection);
        let removed_indexes = Self::get_diff_indexes(existing_collection, collection);

        CollectionMigrator {
            retained_properties,
            collection,
            object_migration_required,
            added_indexes,
            removed_indexes,
        }
    }

    fn get_diff_indexes<'c>(col1: &'c IsarCollection, col2: &IsarCollection) -> Vec<&'c Index> {
        let mut diff_indexes = vec![];
        for index_col1 in col1.get_indexes() {
            let col2_contains_index = col2
                .get_indexes()
                .iter()
                .any(|i| i.get_id() == index_col1.get_id());
            if !col2_contains_index {
                diff_indexes.push(index_col1);
            }
        }
        diff_indexes
    }

    pub fn migrate(self, cursors: &mut Cursors, migration_cursors: &mut Cursors) -> Result<()> {
        for removed_index in &self.removed_indexes {
            removed_index.clear(cursors)?;
        }

        let mut ob_bytes_cache = None;
        let collection_prefix = self.collection.get_id().to_le_bytes();
        if self.object_migration_required {
            cursors
                .primary
                .iter_prefix(&collection_prefix, false, |primary, key, object| {
                    let mut ob = self.collection.new_object_builder(ob_bytes_cache.take());
                    for property in &self.retained_properties {
                        Self::write_property_to_ob(&mut ob, *property, object);
                    }
                    let ob_bytes = ob.finish();
                    let new_object = ob_bytes.as_ref();
                    primary.put(key, new_object)?;

                    for index in &self.added_indexes {
                        index.create_for_object(migration_cursors, key, new_object)?;
                    }

                    ob_bytes_cache.replace(ob_bytes);
                    Ok(true)
                })?;
        } else if !self.added_indexes.is_empty() {
            cursors
                .primary
                .iter_prefix(&collection_prefix, false, |_, key, object| {
                    for index in &self.added_indexes {
                        index.create_for_object(migration_cursors, key, object)?;
                    }
                    Ok(true)
                })?;
        }

        Ok(())
    }

    fn write_property_to_ob(ob: &mut ObjectBuilder, property: Option<&Property>, object: &[u8]) {
        if let Some(p) = property {
            ob.write_from(*p, object)
        } else {
            ob.write_null();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_create_collection_migrator() {}
}
