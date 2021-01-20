use crate::object::object_id::ObjectId;
use crate::query::query::Query;
use crate::watch::watcher::{Watcher, WatcherCallback};
use crossbeam_channel::Receiver;
use hashbrown::HashMap;
use itertools::Itertools;
use std::sync::Arc;

pub(crate) type WatcherModifier = Box<dyn FnOnce(&mut IsarWatchers) + Send + 'static>;

pub(crate) struct IsarWatchers {
    modifiers: Receiver<WatcherModifier>,
    collection_watchers: HashMap<u16, IsarCollectionWatchers>,
}

impl IsarWatchers {
    pub fn new(modifiers: Receiver<WatcherModifier>) -> Self {
        IsarWatchers {
            modifiers,
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

    pub(crate) fn sync(&mut self) {
        let modifiers = self.modifiers.try_iter().collect_vec();
        for modifier in modifiers {
            modifier(self)
        }
    }
}

pub struct IsarCollectionWatchers {
    pub(super) watchers: Vec<Arc<Watcher>>,
    pub(super) object_watchers: HashMap<ObjectId, Vec<Arc<Watcher>>>,
    pub(super) query_watchers: Vec<(Query, Arc<Watcher>)>,
}

impl IsarCollectionWatchers {
    fn new() -> Self {
        IsarCollectionWatchers {
            watchers: Vec::new(),
            object_watchers: HashMap::new(),
            query_watchers: Vec::new(),
        }
    }

    pub fn add_watcher(&mut self, watcher_id: usize, callback: WatcherCallback) {
        let watcher = Arc::new(Watcher::new(watcher_id, callback));
        self.watchers.push(watcher);
    }

    pub fn remove_watcher(&mut self, watcher_id: usize) {
        let position = self
            .watchers
            .iter()
            .position(|w| w.get_id() == watcher_id)
            .unwrap();
        self.watchers.remove(position);
    }

    pub fn add_object_watcher(
        &mut self,
        watcher_id: usize,
        oid: ObjectId,
        callback: WatcherCallback,
    ) {
        assert_ne!(oid.get_prefix(), 0);
        let watcher = Arc::new(Watcher::new(watcher_id, callback));
        if let Some(object_watchers) = self.object_watchers.get_mut(&oid) {
            object_watchers.push(watcher);
        } else {
            self.object_watchers.insert(oid, vec![watcher]);
        }
    }

    pub fn remove_object_watcher(&mut self, oid: ObjectId, watcher_id: usize) {
        let watchers = self.object_watchers.get_mut(&oid).unwrap();
        let position = watchers
            .iter()
            .position(|w| w.get_id() == watcher_id)
            .unwrap();
        watchers.remove(position);
    }

    pub fn add_query_watcher(
        &mut self,
        watcher_id: usize,
        query: Query,
        callback: WatcherCallback,
    ) {
        let watcher = Arc::new(Watcher::new(watcher_id, callback));
        self.query_watchers.push((query, watcher));
    }

    pub fn remove_query_watcher(&mut self, watcher_id: usize) {
        let position = self
            .query_watchers
            .iter()
            .position(|(_, w)| w.get_id() == watcher_id)
            .unwrap();
        self.query_watchers.remove(position);
    }
}
