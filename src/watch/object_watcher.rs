use crate::object::object_id::ObjectId;
use crate::txn::Cursors;
use crate::watch::{ObjectWatcherCallback, Watcher};

pub struct ObjectWatcher {
    pub id: usize,
    oid: ObjectId,
    callback: ObjectWatcherCallback,
}

impl ObjectWatcher {
    pub fn new(id: usize, oid: ObjectId, callback: ObjectWatcherCallback) -> Self {
        ObjectWatcher { id, oid, callback }
    }
}

impl Watcher for ObjectWatcher {
    fn notify(&self, cursors: &mut Cursors) {
        let oid_bytes = self.oid.as_bytes();
        let result = cursors.primary.move_to(&oid_bytes);
        if let Ok(result) = result {
            let object = result.map(|(_, object)| object);
            (*self.callback)(self.oid, object)
        }
    }
}
