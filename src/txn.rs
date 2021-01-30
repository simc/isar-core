use crate::error::{IsarError, Result};
use crate::instance::IsarInstance;
use crate::lmdb::cursor::Cursor;
use crate::lmdb::txn::Txn;
use crate::watch::change_set::ChangeSet;

pub struct IsarTxn<'a> {
    txn: Txn<'a>,
    write: bool,
    change_set: Option<ChangeSet<'a>>,
    cursors: Option<Cursors<'a>>,
}

#[derive(Clone)]
pub(crate) struct Cursors<'a> {
    pub(crate) primary: Cursor<'a>,
    pub(crate) secondary: Cursor<'a>,
    pub(crate) secondary_dup: Cursor<'a>,
}

impl<'a> IsarTxn<'a> {
    pub(crate) fn new(
        isar: &'a IsarInstance,
        txn: Txn<'a>,
        write: bool,
        change_set: Option<ChangeSet<'a>>,
    ) -> Result<Self> {
        assert_eq!(write, change_set.is_some());

        let cursors = isar.open_cursors(&txn)?;
        let cursors: Cursors<'static> = unsafe { std::mem::transmute(cursors) };

        Ok(IsarTxn {
            txn,
            write,
            change_set,
            cursors: Some(cursors),
        })
    }

    pub(crate) fn read<T, F>(&mut self, job: F) -> Result<T>
    where
        F: FnOnce(&mut Cursors<'a>) -> Result<T>,
    {
        if self.write && self.change_set.is_none() {
            Err(IsarError::TransactionClosed {})
        } else {
            job(self.cursors.as_mut().unwrap())
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
            let result = job(self.cursors.as_mut().unwrap(), &mut change_set);
            if result.is_ok() {
                self.change_set.replace(change_set);
            }
            result
        } else {
            Err(IsarError::TransactionClosed {})
        }
    }

    pub fn commit(mut self) -> Result<()> {
        if self.write && self.change_set.is_none() {
            return Err(IsarError::TransactionClosed {});
        }
        self.cursors.take(); // drop before txn

        if self.write {
            self.txn.commit()?;
            self.change_set.unwrap().notify_watchers();
        } else {
            self.txn.abort();
        }
        Ok(())
    }

    pub fn abort(mut self) {
        self.cursors.take(); // drop before txn
        self.txn.abort();
    }
}
