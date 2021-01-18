use crate::object::object_id::ObjectId;
use crate::txn::Cursors;

pub(super) mod change_set;
mod collection_watcher;
pub(super) mod isar_watchers;
mod object_watcher;
mod query_watcher;

pub type CollectionWatcherCallback = Box<dyn Fn() + Send + Sync + 'static>;
pub type ObjectWatcherCallback = Box<dyn Fn(ObjectId, Option<&[u8]>) + Send + Sync + 'static>;
pub type QueryWatcherCallback = Box<dyn Fn(Vec<(&ObjectId, &[u8])>) + Send + Sync + 'static>;

pub struct WatchHandle {
    stop_callback: Option<Box<dyn FnOnce()>>,
}

impl WatchHandle {
    pub(crate) fn new(stop_callback: Box<dyn FnOnce()>) -> Self {
        WatchHandle {
            stop_callback: Some(stop_callback),
        }
    }

    pub fn stop(self) {}
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        let callback = self.stop_callback.take().unwrap();
        callback();
    }
}

trait Watcher {
    fn notify(&self, cursors: &mut Cursors);
}
