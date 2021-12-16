use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::collection::IsarCollection;
use crate::key::IdKey;
use crate::mdbx::cursor::Cursor;
use crate::object::isar_object::IsarObject;
use crate::txn::IsarTxn;

#[derive(Clone)]
pub struct ObjectEntry {
    pub id: i64,
    pub bytes: Vec<u8>,
}

impl ObjectEntry {
    pub fn new(id: i64, bytes: Vec<u8>) -> Self {
        ObjectEntry { id, bytes }
    }
}

#[derive(Clone)]
pub struct LinkEntry {
    pub name: String,
    pub source_id: i64,
    pub target_id: i64,
}

impl LinkEntry {
    pub fn new(name: &str, source_id: i64, target_id: i64) -> Self {
        LinkEntry {
            name: name.to_string(),
            source_id,
            target_id,
        }
    }
}

pub fn verify_isar(
    txn: &mut IsarTxn,
    data: Vec<(&IsarCollection, Vec<ObjectEntry>, Vec<LinkEntry>)>,
) {
    for (col, objects, links) in data {
        let mut entries = HashSet::new();
        let mut index_entries = col.indexes.iter().map(|_| HashSet::new()).collect_vec();
        let mut link_entries: HashMap<String, HashSet<(Vec<u8>, Vec<u8>)>> = col
            .links
            .iter()
            .map(|(n, _)| (n.clone(), HashSet::new()))
            .collect();

        for entry in objects {
            let id_key = IdKey::new(entry.id);
            let inserted = entries.insert((id_key.as_bytes().to_vec(), entry.bytes.clone()));
            assert!(inserted);

            let object = IsarObject::from_bytes(&entry.bytes);
            for (i, index) in col.indexes.iter().enumerate() {
                index
                    .create_keys(object, |key| {
                        let entry = (key.as_bytes().to_vec(), id_key.as_bytes().to_vec());
                        let inserted = index_entries[i].insert(entry);
                        assert!(inserted);
                        Ok(true)
                    })
                    .unwrap();
            }
        }

        for entry in links {
            let inserted = link_entries.get_mut(&entry.name).unwrap().insert((
                IdKey::new(entry.source_id).as_bytes().to_vec(),
                IdKey::new(entry.target_id).as_bytes().to_vec(),
            ));
            assert!(inserted);
            let inserted = link_entries.get_mut(&entry.name).unwrap().insert((
                IdKey::new(entry.target_id).as_bytes().to_vec(),
                IdKey::new(entry.source_id).as_bytes().to_vec(),
            ));
            assert!(inserted);
        }

        txn.read(col.instance_id, |cur| {
            let mut primary_cursor = cur.get_cursor(col.db).unwrap();
            assert_eq!(dump_db(&mut primary_cursor, true), entries);

            for (i, index) in col.indexes.iter().enumerate() {
                let mut index_cursor = cur.get_cursor(index.db).unwrap();
                assert_eq!(dump_db(&mut index_cursor, false), index_entries[i]);
            }

            for (name, link) in &col.links {
                let mut link_cursor = cur.get_cursor(link.db).unwrap();
                assert_eq!(dump_db(&mut link_cursor, true), link_entries[name]);
            }

            Ok(())
        })
        .unwrap();
    }
}

fn dump_db(cursor: &mut Cursor, int_key: bool) -> HashSet<(Vec<u8>, Vec<u8>)> {
    let mut entries = HashSet::new();
    let lower = if int_key {
        vec![0, 0, 0, 0, 0, 0, 0, 0]
    } else {
        vec![]
    };
    let upper = vec![255, 255, 255, 255, 255, 255, 255, 255];
    cursor
        .iter_between(&lower, &upper, false, true, |key, val| {
            entries.insert((key.to_vec(), val.to_vec()));
            Ok(true)
        })
        .unwrap();
    entries
}
