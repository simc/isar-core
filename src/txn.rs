use crate::cursor::IsarCursors;
use crate::error::{IsarError, Result};
use crate::mdbx::cursor::{Cursor, UnboundCursor};
use crate::mdbx::db::Db;
use crate::mdbx::txn::Txn;
use crate::watch::change_set::ChangeSet;
use std::cell::RefCell;

pub struct IsarTxn<'env> {
    instance_id: u64,
    txn: Option<Txn<'env>>,
    write: bool,
    change_set: RefCell<Option<ChangeSet<'env>>>,
    unbound_cursors: RefCell<Option<Vec<UnboundCursor>>>,
}

impl<'env> IsarTxn<'env> {
    pub(crate) fn new(
        instance_id: u64,
        txn: Txn<'env>,
        write: bool,
        change_set: Option<ChangeSet<'env>>,
    ) -> Result<Self> {
        Ok(IsarTxn {
            instance_id,
            txn: Some(txn),
            write,
            change_set: RefCell::new(change_set),
            unbound_cursors: RefCell::new(Some(vec![])),
        })
    }

    pub(crate) fn bind_cursor(&self, unbound: UnboundCursor, db: Db) -> Result<Cursor> {
        unbound.bind(self.txn.as_ref().unwrap(), db)
    }

    pub fn is_active(&self) -> bool {
        self.unbound_cursors.borrow().is_some()
    }

    fn verify_instance_id(&self, instance_id: u64) -> Result<()> {
        if self.instance_id != instance_id {
            Err(IsarError::InstanceMismatch {})
        } else {
            Ok(())
        }
    }

    pub(crate) fn read<'txn, T, F>(&'txn mut self, instance_id: u64, job: F) -> Result<T>
    where
        F: FnOnce(&IsarCursors<'txn, 'env>) -> Result<T>,
    {
        self.verify_instance_id(instance_id)?;
        if let Some(unbound_cursors) = self.unbound_cursors.take() {
            let cursors = IsarCursors::new(self, unbound_cursors);
            let result = job(&cursors);
            self.unbound_cursors.borrow_mut().replace(cursors.close());
            result
        } else {
            Err(IsarError::TransactionClosed {})
        }
    }

    pub(crate) fn write<'txn, T, F>(&'txn mut self, instance_id: u64, job: F) -> Result<T>
    where
        F: FnOnce(&IsarCursors<'txn, 'env>, Option<&mut ChangeSet<'txn>>) -> Result<T>,
    {
        self.verify_instance_id(instance_id)?;
        if !self.write {
            return Err(IsarError::WriteTxnRequired {});
        }
        if let Some(unbound_cursors) = self.unbound_cursors.take() {
            let cursors = IsarCursors::new(self, unbound_cursors);
            let result = job(&cursors, None); //self.change_set.as_mut());
            let unbounded_cursors = cursors.close();
            if result.is_ok() {
                self.unbound_cursors.borrow_mut().replace(unbounded_cursors);
            }
            result
        } else {
            Err(IsarError::TransactionClosed {})
        }
    }

    pub(crate) fn clear_db(&mut self, db: Db) -> Result<()> {
        if !self.write {
            return Err(IsarError::WriteTxnRequired {});
        }
        db.clear(self.txn.as_ref().unwrap())
    }

    pub(crate) fn register_all_changed(&mut self, col_id: u64) -> Result<()> {
        if !self.write {
            return Err(IsarError::WriteTxnRequired {});
        }
        if let Some(change_set) = self.change_set.borrow_mut().as_mut() {
            change_set.register_all(col_id)
        }
        Ok(())
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
