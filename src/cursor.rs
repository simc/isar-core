use crate::error::Result;
use crate::lmdb::cursor::{Cursor, UnboundCursor};
use crate::lmdb::db::Db;
use crate::txn::IsarTxn;
use intmap::IntMap;
use std::cell::RefCell;
use std::ops::{Deref, DerefMut};

pub(crate) struct IsarCursors<'a> {
    txn: &'a IsarTxn<'a>,
    unbound_cursors: RefCell<Vec<UnboundCursor>>,
    cursors: RefCell<IntMap<Cursor<'a>>>,
}

impl<'a> IsarCursors<'a> {
    pub(crate) fn new(
        txn: &'a IsarTxn<'a>,
        unbound_cursors: Vec<UnboundCursor>,
    ) -> IsarCursors<'a> {
        IsarCursors {
            txn,
            unbound_cursors: RefCell::new(unbound_cursors),
            cursors: RefCell::new(IntMap::new()),
        }
    }

    pub(crate) fn get_cursor(&'a self, db: Db) -> Result<IsarCursor<'a>> {
        let cursor = if let Some(cursor) = self.cursors.borrow_mut().remove(db.get_id()) {
            cursor
        } else {
            let unbound = self
                .unbound_cursors
                .borrow_mut()
                .pop()
                .unwrap_or_else(|| UnboundCursor::new());
            self.txn.bind_cursor(unbound, db)?
        };

        Ok(IsarCursor {
            cursors: self,
            cursor: Some(cursor),
            dbid: db.get_id(),
        })
    }

    pub(crate) fn close(self) -> Vec<UnboundCursor> {
        let mut unbound_cursors = self.unbound_cursors.take();
        for (_, cursor) in self.cursors.borrow_mut().drain() {
            unbound_cursors.push(cursor.unbind())
        }
        unbound_cursors
    }
}

pub(crate) struct IsarCursor<'a> {
    cursors: &'a IsarCursors<'a>,
    cursor: Option<Cursor<'a>>,
    dbid: u64,
}

impl<'a> Deref for IsarCursor<'a> {
    type Target = Cursor<'a>;

    fn deref(&self) -> &Self::Target {
        self.cursor.as_ref().unwrap()
    }
}

impl<'a> DerefMut for IsarCursor<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.cursor.as_mut().unwrap()
    }
}

impl<'a> Drop for IsarCursor<'a> {
    fn drop(&mut self) {
        let cursor = self.cursor.take().unwrap();
        let cursors = &self.cursors.cursors;
        if !cursors.borrow().contains_key(self.dbid) {
            cursors.borrow_mut().insert(self.dbid, cursor);
        } else {
            self.cursors
                .unbound_cursors
                .borrow_mut()
                .push(cursor.unbind());
        }
    }
}
