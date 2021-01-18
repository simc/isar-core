use crate::object::object_id::ObjectId;
use crate::txn::IsarTxn;
use crate::watch::isar_watchers::IsarWatchers;
use crate::watch::Watcher;
use hashbrown::HashMap;
use std::sync::{Arc, MutexGuard};

pub(crate) struct ChangeSet<'a> {
    watchers: MutexGuard<'a, IsarWatchers>,
    changed_watchers: HashMap<usize, Arc<dyn Watcher>>,
}

impl<'a> ChangeSet<'a> {
    pub fn new(watchers: MutexGuard<'a, IsarWatchers>) -> Self {
        ChangeSet {
            watchers,
            changed_watchers: HashMap::new(),
        }
    }

    pub fn register_change(&mut self, oid: ObjectId, object: &[u8]) {
        let col_id = oid.get_prefix();
        let cw = self.watchers.get_col_watchers(col_id);
        for w in &cw.watchers {
            if self.changed_watchers.insert(w.id, w.clone()).is_some() {
                break;
            }
        }
        if let Some(object_watchers) = cw.object_watchers.get(&oid) {
            for w in object_watchers {
                if self.changed_watchers.insert(w.id, w.clone()).is_some() {
                    break;
                }
            }
        }
        for w in &cw.query_watchers {
            if !self.changed_watchers.contains_key(&w.id) && w.query.matches_wc_filter(oid, object)
            {
                self.changed_watchers.insert(w.id, w.clone());
            }
        }
    }

    #[allow(unused_must_use)]
    pub fn notify_watchers(self, txn: &mut IsarTxn) {
        txn.read(|cursors| {
            for watcher in self.changed_watchers.values() {
                watcher.notify(cursors);
            }
            Ok(())
        });
    }
}
