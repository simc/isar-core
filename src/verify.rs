use crate::collection::IsarCollection;
use crate::index::index_key_builder::IndexKeyBuilder;
use crate::mdbx::Key;
use crate::object::id::IdToBytes;
use crate::object::isar_object::IsarObject;
use crate::txn::IsarTxn;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

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
    let cols = data.iter().map(|(col, _, _)| *col).collect_vec();
    verify_db_names(txn, &cols);

    for (col, objects, links) in data {
        let mut entries = HashSet::new();
        let mut index_entries = col.indexes.iter().map(|_| HashSet::new()).collect_vec();
        let mut link_entries: HashMap<String, HashSet<(Vec<u8>, Vec<u8>)>> = col
            .links
            .iter()
            .map(|link| (link.name.clone(), HashSet::new()))
            .collect();

        for entry in objects {
            let inserted = entries.insert((entry.id.to_id_bytes().to_vec(), entry.bytes.clone()));
            assert!(inserted);

            let object = IsarObject::from_bytes(&entry.bytes);
            for (i, index) in col.indexes.iter().enumerate() {
                let key_builder = IndexKeyBuilder::new(&index.properties);
                key_builder
                    .create_keys(object, |key| {
                        let entry = (key.as_bytes().to_vec(), entry.id.to_id_bytes().to_vec());
                        index_entries[i].insert(entry);
                        Ok(true)
                    })
                    .unwrap();
            }
        }

        for entry in links {
            let inserted = link_entries.get_mut(&entry.name).unwrap().insert((
                entry.source_id.to_id_bytes().to_vec(),
                entry.target_id.to_id_bytes().to_vec(),
            ));
            assert!(inserted);
        }

        txn.read(col.instance_id, |cur| {
            assert_eq!(col.debug_dump(cur), entries);

            for (i, index) in col.indexes.iter().enumerate() {
                assert_eq!(index.debug_dump(cur), index_entries[i]);
            }

            for link in &col.links {
                assert_eq!(link.debug_dump(cur), link_entries[&link.name]);

                let bl_entries: HashSet<(Vec<u8>, Vec<u8>)> = link_entries[&link.name]
                    .iter()
                    .map(|(source, target)| (target.clone(), source.clone()))
                    .collect();
                assert_eq!(link.debug_dump_bl(cur), bl_entries);
            }

            Ok(())
        })
        .unwrap();
    }
}

fn verify_db_names(txn: &mut IsarTxn, cols: &[&IsarCollection]) {
    let mut db_names = HashSet::new();
    db_names.insert("_info".to_string());
    for col in cols {
        db_names.insert(col.name.clone());
        for index in &col.indexes {
            db_names.insert(format!("_i_{}_{}", col.name, index.name));
        }

        for link in &col.links {
            db_names.insert(format!("_l_{}_{}", col.name, link.name));
            db_names.insert(format!("_b_{}_{}", col.name, link.name));
        }
    }

    let actual_db_names = HashSet::from_iter(txn.debug_db_names().unwrap().into_iter());
    assert_eq!(actual_db_names, db_names);
}
