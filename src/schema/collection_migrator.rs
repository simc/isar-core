use crate::collection::IsarCollection;
use crate::error::Result;
use crate::index::IsarIndex;
use crate::txn::Cursors;

pub(crate) struct CollectionMigrator<'a> {
    collection: &'a IsarCollection,
    removed_indexes: Vec<&'a IsarIndex>,
    added_indexes: Vec<&'a IsarIndex>,
}

impl<'a> CollectionMigrator<'a> {
    pub fn create(collection: &'a IsarCollection, existing_collection: &'a IsarCollection) -> Self {
        let added_indexes = Self::get_added_indexes(existing_collection, collection);
        let removed_indexes = Self::get_added_indexes(collection, existing_collection);

        CollectionMigrator {
            collection,
            added_indexes,
            removed_indexes,
        }
    }

    fn get_added_indexes<'c>(old: &IsarCollection, new: &'c IsarCollection) -> Vec<&'c IsarIndex> {
        let mut added_indexes = vec![];
        for index in &new.indexes {
            let old_contains_index = old.indexes.iter().any(|i| i.id == index.id);
            if !old_contains_index {
                added_indexes.push(index);
            }
        }
        added_indexes
    }

    pub fn migrate<'b>(self, cursors: &mut Cursors<'b>, cursors2: &mut Cursors<'b>) -> Result<()> {
        for removed_index in &self.removed_indexes {
            removed_index.clear(cursors)?;
        }

        if !self.added_indexes.is_empty() {
            self.collection
                .new_query_builder()
                .build()
                .find_while_internal(cursors, false, |object| {
                    let oid = object.read_id();
                    for index in &self.added_indexes {
                        index.create_for_object(cursors2, oid, object, |cursors, oid| {
                            self.collection.delete_internal(cursors, true, None, oid)?;
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
