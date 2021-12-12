use std::collections::HashSet;

use crate::collection::IsarCollection;
use crate::key::IdKey;
use crate::mdbx::cursor::Cursor;
use crate::object::isar_object::IsarObject;
use crate::txn::IsarTxn;

pub fn verify_isar(
    txn: &mut IsarTxn,
    data: Vec<(&IsarCollection, Vec<Vec<u8>>, Vec<(&str, i64, i64)>)>,
) {
    /*let mut entries = HashSet::new();
    let mut index_entries = HashSet::new();
    let mut link_entries = HashSet::new();
    for (col, objects, linked_objects) in data {
        for bytes in objects {
            let object = IsarObject::from_bytes(&bytes);
            let id_key = IdKey::new(object.read_id());
            entries.insert((id_key.as_bytes().to_vec(), bytes.clone()));

            for index in &col.indexes {
                index
                    .create_keys(object, |key| {
                        index_entries.insert((key.as_bytes().to_vec(), id_key.as_bytes().to_vec()));
                        Ok(true)
                    })
                    .unwrap();
            }
        }

        for (name, source_id, target_id) in linked_objects {
            let (_, link) = col.links.iter().find(|(n, _)| n == name).unwrap();
            link_entries.insert((
                IdKey::new(source_id).as_bytes().to_vec(),
                IdKey::new(target_id).as_bytes().to_vec(),
            ));
            link_entries.insert((
                IdKey::new(target_id).as_bytes().to_vec(),
                IdKey::new(source_id).as_bytes().to_vec(),
            ));
        }
    }

    let (actual, actual_index, actual_link) = txn
        .read(0, |cursors| {
            let actual = dump_col(&mut cursors.data);
            let actual_index = dump_col(&mut cursors.index);
            let actual_link = dump_col(&mut cursors.links);
            Ok((actual, actual_index, actual_link))
        })
        .unwrap();

    assert_eq!(entries, actual);
    assert_eq!(index_entries, actual_index);
    assert_eq!(link_entries, actual_link);*/
}

fn dump_col(cursor: &mut Cursor) -> HashSet<(Vec<u8>, Vec<u8>)> {
    /*let mut entries = HashSet::new();
    cursor
        .iter_between(&[], &[255], false, false, |_, key, val| {
            entries.insert((key.to_vec(), val.to_vec()));
            Ok(true)
        })
        .unwrap();
    entries*/
    HashSet::new()
}
