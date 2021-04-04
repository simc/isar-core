use crate::error::{IsarError, Result};
use crate::instance::IsarInstance;
use crate::lmdb::cursor::Cursor;
use crate::lmdb::txn::Txn;
use crate::watch::change_set::ChangeSet;

pub struct IsarTxn<'a> {
    txn: Option<Txn<'a>>,
    active: bool,
    write: bool,
    change_set: Option<ChangeSet<'a>>,
    cursors: Option<Cursors<'a>>,
}

#[derive(Clone)]
pub(crate) struct Cursors<'a> {
    pub(crate) data: Cursor<'a>,
    pub(crate) data2: Cursor<'a>,
    pub(crate) index: Cursor<'a>,
    pub(crate) links: Cursor<'a>,
}

impl<'a> IsarTxn<'a> {
    pub(crate) fn new(
        isar: &'a IsarInstance,
        txn: Txn<'a>,
        write: bool,

        change_set: Option<ChangeSet<'a>>,
    ) -> Result<Self> {
        let cursors = isar.open_cursors(&txn)?;
        let cursors: Cursors<'static> = unsafe { std::mem::transmute(cursors) };

        Ok(IsarTxn {
            txn: Some(txn),
            active: true,
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
        F: FnOnce(&mut Cursors<'a>, Option<&mut ChangeSet<'a>>) -> Result<T>,
    {
        if !self.write {
            return Err(IsarError::WriteTxnRequired {});
        }
        if self.active {
            self.active = false;
            let result = job(self.cursors.as_mut().unwrap(), self.change_set.as_mut());
            if result.is_ok() {
                self.active = true;
            }
            result
        } else {
            Err(IsarError::TransactionClosed {})
        }
    }

    pub fn commit(mut self) -> Result<()> {
        if !self.active {
            return Err(IsarError::TransactionClosed {});
        }

        if self.write {
            self.cursors.take(); // drop before txn
            self.txn.take().unwrap().commit()?;
            if let Some(change_set) = self.change_set.take() {
                change_set.notify_watchers();
            }
        }
        Ok(())
    }

    pub fn abort(self) {}
}

impl<'a> Drop for IsarTxn<'a> {
    fn drop(&mut self) {
        if self.cursors.is_some() {
            self.cursors.take(); // drop before txn
            self.txn.take().unwrap().abort();
        }
    }
}
