use crate::object::isar_object::IsarObject;
use crate::watch::isar_watchers::IsarWatchers;
use crate::watch::watcher::Watcher;
use hashbrown::HashMap;
use std::sync::{Arc, MutexGuard};

pub(crate) struct ChangeSet<'a> {
    watchers: MutexGuard<'a, IsarWatchers>,
    changed_watchers: HashMap<usize, Arc<Watcher>>,
}

impl<'a> ChangeSet<'a> {
    pub fn new(watchers: MutexGuard<'a, IsarWatchers>) -> Self {
        ChangeSet {
            watchers,
            changed_watchers: HashMap::new(),
        }
    }

    pub fn register_change(&mut self, col_id: u16, oid: Option<i64>, object: Option<IsarObject>) {
        let cw = self.watchers.get_col_watchers(col_id);
        for w in &cw.watchers {
            if self
                .changed_watchers
                .insert(w.get_id(), w.clone())
                .is_some()
            {
                break;
            }
        }

        if let Some(oid) = oid {
            if let Some(object_watchers) = cw.object_watchers.get(&oid) {
                for w in object_watchers {
                    if self
                        .changed_watchers
                        .insert(w.get_id(), w.clone())
                        .is_some()
                    {
                        break;
                    }
                }
            }

            if let Some(object) = object {
                for (q, w) in &cw.query_watchers {
                    if !self.changed_watchers.contains_key(&w.get_id())
                        && q.matches_wc_filter(oid, object)
                    {
                        self.changed_watchers.insert(w.get_id(), w.clone());
                    }
                }
            }
        }
    }

    pub fn notify_watchers(self) {
        for watcher in self.changed_watchers.values() {
            watcher.notify();
        }
    }
}
