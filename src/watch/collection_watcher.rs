use crate::txn::Cursors;
use crate::watch::{CollectionWatcherCallback, Watcher};

pub struct CollectionWatcher {
    pub id: usize,
    callback: CollectionWatcherCallback,
}

impl CollectionWatcher {
    pub fn new(id: usize, callback: CollectionWatcherCallback) -> Self {
        CollectionWatcher { id, callback }
    }
}

impl Watcher for CollectionWatcher {
    fn notify(&self, _cursors: &mut Cursors) {
        (*self.callback)()
    }
}
