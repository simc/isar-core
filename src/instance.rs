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
use std::fs::remove_file;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::{fs, mem};
use xxhash_rust::xxh3::xxh3_64;

static INSTANCES: Lazy<RwLock<IntMap<Arc<IsarInstance>>>> =
    Lazy::new(|| RwLock::new(IntMap::new()));

pub struct IsarInstance {
    pub name: String,
    pub dir: String,
    pub collections: Vec<IsarCollection>,
    pub(crate) instance_id: u64,
    pub(crate) schema_hash: u64,

    env: Env,
    watchers: Mutex<IsarWatchers>,
    watcher_modifier_sender: Sender<WatcherModifier>,
}

impl IsarInstance {
    pub fn open(
        name: &str,
        dir: Option<&str>,
        relaxed_durability: bool,
        schema: Schema,
    ) -> Result<Arc<Self>> {
        let mut lock = INSTANCES.write().unwrap();
        let instance_id = xxh3_64(name.as_bytes());
        if let Some(instance) = lock.get(instance_id) {
            if instance.schema_hash == schema.get_hash() {
                Ok(instance.clone())
            } else {
                Err(IsarError::SchemaMismatch {})
            }
        } else {
            if let Some(dir) = dir {
                let new_instance =
                    Self::open_internal(name, dir, instance_id, relaxed_durability, schema)?;
                let new_instance = Arc::new(new_instance);
                lock.insert(instance_id, new_instance.clone());
                Ok(new_instance)
            } else {
                Err(IsarError::IllegalArg {
                    message:
                        "There is no open instance. Please provide a valid directory to open one."
                            .to_string(),
                })
            }
        }
    }

    fn get_db_path(name: &str, dir: &str) -> String {
        let mut file_name = name.to_string();
        file_name.push_str(".isar");

        let mut path_buf = PathBuf::from(dir);
        path_buf.push(file_name);
        path_buf.as_path().to_str().unwrap().to_string()
    }

    fn move_old_database(name: &str, dir: &str, new_path: &str) {
        let mut old_path_buf = PathBuf::from(dir);
        old_path_buf.push(name);
        old_path_buf.push("mdbx.dat");
        let old_path = old_path_buf.as_path();

        let result = fs::rename(old_path, new_path);

        // Also try to migrate the previous default isar name
        if name == "default" && result.is_err() {
            Self::move_old_database("isar", dir, new_path)
        }
    }

    fn open_internal(
        name: &str,
        dir: &str,
        instance_id: u64,
        relaxed_durability: bool,
        mut schema: Schema,
    ) -> Result<Self> {
        let db_file = Self::get_db_path(name, dir);

        Self::move_old_database(name, dir, &db_file);

        let db_count = schema.count_dbs() as u64 + 3;
        let env = Env::create(&db_file, db_count, relaxed_durability)
            .map_err(|e| IsarError::EnvError { error: Box::new(e) })?;

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
            name: name.to_string(),
            dir: dir.to_string(),
            collections,
            instance_id,
            schema_hash: schema.get_hash(),
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

    fn close_internal(self: Arc<Self>, delete_from_disk: bool) -> bool {
        // Check whether all other references are gone
        if Arc::strong_count(&self) == 2 {
            let mut lock = INSTANCES.write().unwrap();
            // Check again to make sure there are no new references
            if Arc::strong_count(&self) == 2 {
                lock.remove(self.instance_id);

                if delete_from_disk {
                    let mut path = Self::get_db_path(&self.name, &self.dir);
                    mem::drop(self);
                    let _ = remove_file(&path);
                    path.push_str(".lock");
                    let _ = remove_file(&path);
                }
                return true;
            }
        }
        false
    }

    pub fn close(self: Arc<Self>) -> bool {
        self.close_internal(false)
    }

    pub fn close_and_delete(self: Arc<Self>) -> bool {
        self.close_internal(true)
    }
}
