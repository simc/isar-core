use crate::collection::IsarCollection;
use crate::error::*;
use crate::mdbx::env::Env;
use crate::query::Query;
use crate::schema::schema_manager::SchemaManger;
use crate::schema::Schema;
use crate::txn::IsarTxn;
use crate::watch::change_set::ChangeSet;
use crate::watch::isar_watchers::{IsarWatchers, WatcherModifier};
use crate::watch::watcher::WatcherCallback;
use crate::watch::WatchHandle;
use crossbeam_channel::{unbounded, Sender};
use intmap::IntMap;
use once_cell::sync::Lazy;
use rand::random;
use std::fs::create_dir_all;
use std::sync::{Arc, Mutex, RwLock};
use xxhash_rust::xxh3::xxh3_64;

static INSTANCES: Lazy<RwLock<IntMap<Arc<IsarInstance>>>> =
    Lazy::new(|| RwLock::new(IntMap::new()));

pub struct IsarInstance {
    pub path: String,
    pub collections: Vec<IsarCollection>,
    pub(crate) instance_id: u64,

    env: Env,
    watchers: Mutex<IsarWatchers>,
    watcher_modifier_sender: Sender<WatcherModifier>,
}

impl IsarInstance {
    pub fn open(path: &str, relaxed_durability: bool, schema: Schema) -> Result<Arc<Self>> {
        let mut lock = INSTANCES.write().unwrap();
        let instance_id = xxh3_64(path.as_bytes());
        let instance = lock.get(instance_id);
        if let Some(instance) = instance {
            Ok(instance.clone())
        } else {
            let new_instance = Self::open_internal(path, instance_id, relaxed_durability, schema)?;
            let new_instance = Arc::new(new_instance);
            lock.insert(instance_id, new_instance.clone());
            Ok(new_instance)
        }
    }

    fn open_internal(
        path: &str,
        instance_id: u64,
        relaxed_durability: bool,
        mut schema: Schema,
    ) -> Result<Self> {
        let _ = create_dir_all(path);

        let db_count = schema.count_dbs() as u64 + 3;
        let env = Env::create(path, db_count, relaxed_durability)?;

        let txn = env.txn(true)?;
        let collections = {
            let mut manager = SchemaManger::create(instance_id, &txn)?;
            manager.perform_migration(&mut schema)?;
            manager.open_collections(&schema)?
        };
        txn.commit()?;

        let (tx, rx) = unbounded();

        Ok(IsarInstance {
            env,
            path: path.to_string(),
            collections,
            instance_id,
            watchers: Mutex::new(IsarWatchers::new(rx)),
            watcher_modifier_sender: tx,
        })
    }

    pub fn get_instance(name: &str) -> Option<Arc<Self>> {
        let instance_id = xxh3_64(name.as_bytes());
        INSTANCES.read().unwrap().get(instance_id).cloned()
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
        IsarTxn::new(self.instance_id, txn, write, change_set)
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
        let col_id = collection.get_runtime_id();
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
        let col_id = collection.get_runtime_id();
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
        let col_id = collection.get_runtime_id();
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
        // Check whether all other references are gone
        if Arc::strong_count(&self) == 2 {
            let mut lock = INSTANCES.write().unwrap();
            // Check again to make sure there are no new references
            if Arc::strong_count(&self) == 2 {
                lock.remove(self.instance_id);
                return true;
            }
        }
        false
    }
}
