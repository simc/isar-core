use crate::object::object_id::ObjectId;
use crate::query::query::Query;
use crate::watch::collection_watcher::CollectionWatcher;
use crate::watch::object_watcher::ObjectWatcher;
use crate::watch::query_watcher::QueryWatcher;
use crate::watch::{CollectionWatcherCallback, ObjectWatcherCallback, QueryWatcherCallback};
use hashbrown::HashMap;
use std::sync::Arc;

pub(crate) type WatcherModifier = Box<dyn FnOnce(&mut IsarWatchers) + Send + 'static>;

pub(crate) struct IsarWatchers {
    collection_watchers: HashMap<u16, IsarCollectionWatchers>,
}

impl IsarWatchers {
    pub fn new() -> Self {
        IsarWatchers {
            collection_watchers: HashMap::new(),
        }
    }

    pub(crate) fn get_col_watchers(&mut self, col_id: u16) -> &mut IsarCollectionWatchers {
        if !self.collection_watchers.contains_key(&col_id) {
            self.collection_watchers
                .insert(col_id, IsarCollectionWatchers::new());
        }
        self.collection_watchers.get_mut(&col_id).unwrap()
    }
}

pub struct IsarCollectionWatchers {
    pub(crate) watchers: Vec<Arc<CollectionWatcher>>,
    pub(crate) object_watchers: HashMap<ObjectId, Vec<Arc<ObjectWatcher>>>,
    pub(crate) query_watchers: Vec<Arc<QueryWatcher>>,
}

impl IsarCollectionWatchers {
    fn new() -> Self {
        IsarCollectionWatchers {
            watchers: Vec::new(),
            object_watchers: HashMap::new(),
            query_watchers: Vec::new(),
        }
    }

    pub fn add_watcher(&mut self, watcher_id: usize, callback: CollectionWatcherCallback) {
        let watcher = Arc::new(CollectionWatcher::new(watcher_id, callback));
        self.watchers.push(watcher);
    }

    pub fn remove_watcher(&mut self, watcher_id: usize) {
        let position = self
            .watchers
            .iter()
            .position(|w| w.id == watcher_id)
            .unwrap();
        self.watchers.remove(position);
    }

    pub fn add_object_watcher(
        &mut self,
        watcher_id: usize,
        oid: ObjectId,
        callback: ObjectWatcherCallback,
    ) {
        assert_ne!(oid.get_prefix(), 0);
        let watcher = Arc::new(ObjectWatcher::new(watcher_id, oid, callback));
        if let Some(object_watchers) = self.object_watchers.get_mut(&oid) {
            object_watchers.push(watcher);
        } else {
            self.object_watchers.insert(oid, vec![watcher]);
        }
    }

    pub fn remove_object_watcher(&mut self, oid: ObjectId, watcher_id: usize) {
        let watchers = self.object_watchers.get_mut(&oid).unwrap();
        let position = watchers.iter().position(|w| w.id == watcher_id).unwrap();
        watchers.remove(position);
    }

    pub fn add_query_watcher(
        &mut self,
        watcher_id: usize,
        query: Query,
        callback: QueryWatcherCallback,
    ) {
        let watcher = Arc::new(QueryWatcher::new(watcher_id, query, callback));
        self.query_watchers.push(watcher);
    }

    pub fn remove_query_watcher(&mut self, watcher_id: usize) {
        let position = self
            .query_watchers
            .iter()
            .position(|w| w.id == watcher_id)
            .unwrap();
        self.query_watchers.remove(position);
    }
}
