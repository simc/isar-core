use crate::query::query::Query;
use crate::txn::Cursors;
use crate::watch::{QueryWatcherCallback, Watcher};

pub struct QueryWatcher {
    pub id: usize,
    pub query: Query,
    callback: QueryWatcherCallback,
}

impl QueryWatcher {
    pub fn new(id: usize, query: Query, callback: QueryWatcherCallback) -> Self {
        QueryWatcher {
            id,
            query,
            callback,
        }
    }
}

impl Watcher for QueryWatcher {
    fn notify(&self, cursors: &mut Cursors) {
        let mut items = vec![];
        let result = self.query.find_all_internal(cursors, |oid, value| {
            items.push((oid, value));
            true
        });
        if result.is_ok() {
            (*self.callback)(items)
        }
    }
}
