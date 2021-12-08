use crate::cursor::IsarCursors;
use crate::error::{IsarError, Result};
use crate::lmdb::cursor::{Cursor, UnboundCursor};
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::watch::change_set::ChangeSet;

pub struct IsarTxn<'a> {
    txn: Option<Txn<'a>>,
    write: bool,
    change_set: Option<ChangeSet<'a>>,
    unbound_cursors: Option<Vec<UnboundCursor>>,
}

impl<'a> IsarTxn<'a> {
    pub(crate) fn new(
        txn: Txn<'a>,
        write: bool,
        change_set: Option<ChangeSet<'a>>,
    ) -> Result<Self> {
        Ok(IsarTxn {
            txn: Some(txn),
            write,
            change_set,
            unbound_cursors: Some(vec![]),
        })
    }

    pub(crate) fn bind_cursor(&'a self, unbound: UnboundCursor, db: Db) -> Result<Cursor<'a>> {
        unbound.bind(self.txn.as_ref().unwrap(), db)
    }

    pub fn is_active(&self) -> bool {
        self.unbound_cursors.is_some()
    }

    pub(crate) fn read<T, F>(&'a mut self, job: F) -> Result<T>
    where
        F: FnOnce(&IsarCursors<'a>) -> Result<T>,
    {
        if let Some(unbound_cursors) = self.unbound_cursors.take() {
            let cursors = IsarCursors::new(self, unbound_cursors);
            let result = job(&cursors);
            self.unbound_cursors.replace(cursors.close());
            result
        } else {
            Err(IsarError::TransactionClosed {})
        }
    }

    pub(crate) fn write<T, F>(&'a mut self, job: F) -> Result<T>
    where
        F: FnOnce(&IsarCursors<'a>, Option<&mut ChangeSet<'a>>) -> Result<T>,
    {
        if !self.write {
            return Err(IsarError::WriteTxnRequired {});
        }
        if let Some(unbound_cursors) = self.unbound_cursors.take() {
            let cursors = IsarCursors::new(self, unbound_cursors);
            let result = job(&cursors, self.change_set.as_mut());
            if result.is_ok() {
                self.unbound_cursors.replace(cursors.close());
            }
            result
        } else {
            Err(IsarError::TransactionClosed {})
        }
    }

    pub fn commit(mut self) -> Result<()> {
        if !self.is_active() {
            return Err(IsarError::TransactionClosed {});
        }

        if self.write {
            self.txn.take().unwrap().commit()?;
            if let Some(change_set) = self.change_set.take() {
                change_set.notify_watchers();
            }
        }
        Ok(())
    }

    pub fn abort(self) {}
}
