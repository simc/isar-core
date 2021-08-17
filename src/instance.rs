use crate::collection::IsarCollection;
use crate::error::*;
use crate::lmdb::cursor::Cursor;
use crate::lmdb::db::Db;
use crate::lmdb::env::Env;
use crate::lmdb::txn::Txn;
use crate::query::Query;
use crate::schema::schema_manager::SchemaManger;
use crate::schema::Schema;
use crate::txn::{Cursors, IsarTxn};
use crate::watch::change_set::ChangeSet;
use crate::watch::isar_watchers::{IsarWatchers, WatcherModifier};
use crate::watch::watcher::WatcherCallback;
use crate::watch::WatchHandle;
use crossbeam_channel::{unbounded, Sender};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use once_cell::sync::Lazy;
use rand::random;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

static INSTANCES: Lazy<RwLock<HashMap<String, Arc<IsarInstance>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub struct IsarInstance {
    env: Env,
    dbs: DataDbs,
    name: String,
    collections: Vec<IsarCollection>,
    watchers: Mutex<IsarWatchers>,
    watcher_modifier_sender: Sender<WatcherModifier>,
}

impl IsarInstance {
    pub const ID_NAME: &'static str = "id";
    pub const MIN_ID: i64 = -(1 << 47);
    pub const MAX_ID: i64 = (1 << 47) - 1;
    pub const ENCRYPTION_KEY_LEN: usize = 32;

    pub fn open(
        name: &str,
        dir: PathBuf,
        max_size: usize,
        schema: Schema,
        encryption_key: Option<&[u8]>,
    ) -> Result<Arc<Self>> {
        let mut lock = INSTANCES.write().unwrap();
        match lock.entry(name.to_string()) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
                let new_instance =
                    Self::open_internal(e.key(), dir, max_size, schema, encryption_key)?;
                let instance_ref = e.insert(Arc::new(new_instance));
                Ok(instance_ref.clone())
            }
        }
    }

    fn open_internal(
        name: &str,
        mut dir: PathBuf,
        max_size: usize,
        schema: Schema,
        encryption_key: Option<&[u8]>,
    ) -> Result<Self> {
        if let Some(encryption_key) = encryption_key {
            if encryption_key.len() != IsarInstance::ENCRYPTION_KEY_LEN {
                return illegal_arg("Wrong Encryption key size.");
            }
        }

        dir.push(name);
        let path = dir.to_str().unwrap();
        let env = Env::create(path, 4, max_size, encryption_key)?;
        let dbs = IsarInstance::open_databases(&env)?;

        let txn = env.txn(true)?;
        let collections = {
            let info_cursor = dbs.open_info_cursor(&txn)?;
            let cursors = dbs.open_cursors(&txn)?;
            let cursors2 = dbs.open_cursors(&txn)?;

            let mut manager = SchemaManger::new(info_cursor, cursors, cursors2);
            manager.check_isar_version()?;
            manager.get_collections(schema)?
        };
        txn.commit()?;

        let (tx, rx) = unbounded();

        Ok(IsarInstance {
            env,
            dbs,
            name: name.to_string(),
            collections,
            watchers: Mutex::new(IsarWatchers::new(rx)),
            watcher_modifier_sender: tx,
        })
    }

    pub fn get_instance(name: &str) -> Option<Arc<Self>> {
        INSTANCES.read().unwrap().get(name).cloned()
    }

    fn open_databases(env: &Env) -> Result<DataDbs> {
        let txn = env.txn(true)?;
        let info = Db::open(&txn, "info", false, false, false)?;
        let data = Db::open(&txn, "data", true, false, false)?;
        let index = Db::open(&txn, "index", false, true, true)?;
        let links = Db::open(&txn, "links", true, true, true)?;
        txn.commit()?;
        Ok(DataDbs {
            info,
            data,
            index,
            links,
        })
    }

    pub(crate) fn open_cursors<'txn>(&self, txn: &'txn Txn<'txn>) -> Result<Cursors<'txn>> {
        self.dbs.open_cursors(txn)
    }

    pub fn begin_txn(&self, write: bool, silent: bool) -> Result<IsarTxn> {
        let change_set = if write && !silent {
            let mut watchers_lock = self.watchers.lock().unwrap();
            watchers_lock.sync();
            let change_set = ChangeSet::new(watchers_lock);
            Some(change_set)
        } else {
            None
        };

        let txn = self.env.txn(write)?;
        IsarTxn::new(self, txn, write, change_set)
    }

    pub fn get_collection(&self, collection_index: usize) -> Option<&IsarCollection> {
        self.collections.get(collection_index)
    }

    pub fn get_collection_by_name(&self, collection_name: &str) -> Option<&IsarCollection> {
        self.collections
            .iter()
            .find(|c| c.get_name() == collection_name)
    }

    fn new_watcher(&self, start: WatcherModifier, stop: WatcherModifier) -> WatchHandle {
        self.watcher_modifier_sender.try_send(start).unwrap();

        let sender = self.watcher_modifier_sender.clone();
        WatchHandle::new(Box::new(move || {
            let _ = sender.try_send(stop);
        }))
    }

    pub fn watch_collection(
        &self,
        collection: &IsarCollection,
        callback: WatcherCallback,
    ) -> WatchHandle {
        let watcher_id = random();
        let col_id = collection.get_id();
        self.new_watcher(
            Box::new(move |iw| {
                iw.get_col_watchers(col_id)
                    .add_watcher(watcher_id, callback);
            }),
            Box::new(move |iw| {
                iw.get_col_watchers(col_id).remove_watcher(watcher_id);
            }),
        )
    }

    pub fn watch_object(
        &self,
        collection: &IsarCollection,
        oid: i64,
        callback: WatcherCallback,
    ) -> WatchHandle {
        let watcher_id = random();
        let col_id = collection.get_id();
        self.new_watcher(
            Box::new(move |iw| {
                iw.get_col_watchers(col_id)
                    .add_object_watcher(watcher_id, oid, callback);
            }),
            Box::new(move |iw| {
                iw.get_col_watchers(col_id)
                    .remove_object_watcher(oid, watcher_id);
            }),
        )
    }

    pub fn watch_query(
        &self,
        collection: &IsarCollection,
        query: Query,
        callback: WatcherCallback,
    ) -> WatchHandle {
        let watcher_id = random();
        let col_id = collection.get_id();
        self.new_watcher(
            Box::new(move |iw| {
                iw.get_col_watchers(col_id)
                    .add_query_watcher(watcher_id, query, callback);
            }),
            Box::new(move |iw| {
                iw.get_col_watchers(col_id).remove_query_watcher(watcher_id);
            }),
        )
    }

    pub fn close(self: Arc<Self>) -> bool {
        if Arc::strong_count(&self) == 2 {
            INSTANCES.write().unwrap().remove(&self.name);
            Arc::downgrade(&self);
            true
        } else {
            false
        }
    }
}

struct DataDbs {
    pub info: Db,
    pub data: Db,
    pub index: Db,
    pub links: Db,
}

impl DataDbs {
    fn open_cursors<'txn>(&self, txn: &'txn Txn) -> Result<Cursors<'txn>> {
        Ok(Cursors {
            data: self.data.cursor(txn)?,
            data2: self.data.cursor(txn)?,
            index: self.index.cursor(txn)?,
            links: self.links.cursor(txn)?,
        })
    }

    fn open_info_cursor<'txn>(&self, txn: &'txn Txn) -> Result<Cursor<'txn>> {
        self.info.cursor(txn)
    }
}

#[cfg(test)]
mod tests {
    use crate::object::data_type::DataType;
    use crate::object::isar_object::IsarObject;
    use crate::{col, isar};
    use tempfile::tempdir;

    #[test]
    fn test_open_new_instance() {
        isar!(isar, col => col!());

        let mut ob = col.new_object_builder(None);
        ob.write_long(123);
        let o = ob.finish();

        let mut txn = isar.begin_txn(true, false).unwrap();
        col.put(&mut txn, o).unwrap();
        txn.commit().unwrap();
        let mut txn = isar.begin_txn(false, false).unwrap();
        assert_eq!(col.get(&mut txn, 123).unwrap().unwrap(), o);
        txn.abort();
        isar.close();
    }

    #[test]
    fn test_open_instance_added_collection() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        isar!(path: path, isar, col1 => col!("col1",));

        let mut ob = col1.new_object_builder(None);
        ob.write_long(123);
        let object = ob.finish();
        let object_bytes = object.as_bytes().to_vec();

        let mut txn = isar.begin_txn(true, false).unwrap();
        col1.put(&mut txn, object).unwrap();
        txn.commit().unwrap();

        assert!(isar.close());

        isar!(path: path, isar2, col1 => col!("col1"), col2 => col!("col2"));
        let mut txn = isar2.begin_txn(false, false).unwrap();
        let object = IsarObject::from_bytes(&object_bytes);
        assert_eq!(col1.get(&mut txn, 123).unwrap(), Some(object));
        assert_eq!(col2.new_query_builder().build().count(&mut txn).unwrap(), 0);
        txn.abort();
        isar2.close();
    }

    #[test]
    fn test_open_instance_removed_collection() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        isar!(path: path, isar, col1 => col!("col1"), _col2 => col!("col2"));
        let mut ob = col1.new_object_builder(None);
        ob.write_long(123);
        let o = ob.finish();
        let mut txn = isar.begin_txn(true, false).unwrap();
        //col1.put(&txn, None, o.as_ref()).unwrap();
        col1.put(&mut txn, o).unwrap();
        txn.commit().unwrap();
        isar.close();

        isar!(path: path, isar, _col2 => col!("col2"));
        isar.close();

        isar!(path: path, isar, col1 => col!("col1"), _col2 => col!("col2"));
        let mut txn = isar.begin_txn(false, false).unwrap();
        assert_eq!(col1.new_query_builder().build().count(&mut txn).unwrap(), 0);
        txn.abort();
        isar.close();
    }
}
