use crate::error::{IsarError, Result};
use crate::instance::IsarInstance;
use crate::lmdb::cursor::Cursor;
use crate::lmdb::txn::Txn;
use crate::watch::change_set::ChangeSet;

pub struct IsarTxn<'a> {
    isar: &'a IsarInstance,
    txn: Txn<'a>,
    write: bool,
    change_set: Option<ChangeSet<'a>>,
    cursors: Cursors<'a>,
}

pub(crate) struct Cursors<'a> {
    pub(crate) primary: Cursor<'a>,
    pub(crate) secondary: Cursor<'a>,
    pub(crate) secondary_dup: Cursor<'a>,
}

impl<'a> IsarTxn<'a> {
    pub(crate) fn new(
        isar: &'a IsarInstance,
        txn: Txn<'a>,
        cursors: Cursors<'a>,
        write: bool,
        change_set: Option<ChangeSet<'a>>,
    ) -> Self {
        assert_eq!(write, change_set.is_some());

        IsarTxn {
            isar,
            txn,
            cursors,
            write,
            change_set,
        }
    }

    pub(crate) fn read<T, F>(&mut self, job: F) -> Result<T>
    where
        F: FnOnce(&mut Cursors<'a>) -> Result<T>,
    {
        if self.write && self.change_set.is_none() {
            Err(IsarError::TransactionClosed {})
        } else {
            job(&mut self.cursors)
        }
    }

    pub(crate) fn write<T, F>(&mut self, job: F) -> Result<T>
    where
        F: FnOnce(&mut Cursors<'a>, &mut ChangeSet<'a>) -> Result<T>,
    {
        if !self.write {
            return Err(IsarError::WriteTxnRequired {});
        }
        let change_set = self.change_set.take();
        if let Some(mut change_set) = change_set {
            let result = job(&mut self.cursors, &mut change_set);
            if result.is_ok() {
                self.change_set.replace(change_set);
            }
            result
        } else {
            Err(IsarError::TransactionClosed {})
        }
    }

    pub(crate) fn open_cursors(&self) -> Result<Cursors<'a>> {
        self.isar.open_cursors(&self.txn)
    }

    pub fn commit(self) -> Result<()> {
        if self.write && self.change_set.is_none() {
            return Err(IsarError::TransactionClosed {});
        }

        if self.write {
            self.txn.commit()?;
            self.change_set.unwrap().notify_watchers();
        } else {
            self.txn.abort();
        }
        Ok(())
    }

    pub fn abort(self) {
        self.txn.abort();
    }
}
