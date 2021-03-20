use crate::collection::IsarCollection;
use crate::error::Result;
use crate::index::Index;
use crate::txn::Cursors;

pub(crate) struct CollectionMigrator<'a> {
    collection: &'a IsarCollection,
    removed_indexes: Vec<&'a Index>,
    added_indexes: Vec<&'a Index>,
}

impl<'a> CollectionMigrator<'a> {
    pub fn create(collection: &'a IsarCollection, existing_collection: &'a IsarCollection) -> Self {
        let added_indexes = Self::get_diff_indexes(collection, existing_collection);
        let removed_indexes = Self::get_diff_indexes(existing_collection, collection);

        CollectionMigrator {
            collection,
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

    pub fn migrate<'b>(self, cursors: &mut Cursors<'b>, cursors2: &mut Cursors<'b>) -> Result<()> {
        for removed_index in &self.removed_indexes {
            removed_index.clear(cursors)?;
        }

        if !self.added_indexes.is_empty() {
            self.collection
                .new_query_builder()
                .build()
                .find_all_internal(cursors, false, |object| {
                    let oid = object.read_long(self.collection.get_oid_property());
                    for index in &self.added_indexes {
                        index.create_for_object(cursors2, oid, object, |cursors, id| {
                            self.collection.delete_internal(cursors, None, id)?;
                            Ok(())
                        })?;
                    }
                    Ok(true)
                })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_create_collection_migrator() {}
}
