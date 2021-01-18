use crate::error::Result;
use crate::lmdb::error::lmdb_result;
use core::ptr;
use lmdb_sys as ffi;
use std::marker::PhantomData;

pub struct Txn<'env> {
    pub(crate) txn: *mut ffi::MDB_txn,
    pub(crate) write: bool,
    _marker: PhantomData<&'env ()>,
}

impl<'env> Txn<'env> {
    pub(crate) fn new(txn: *mut ffi::MDB_txn, write: bool) -> Self {
        Txn {
            txn,
            write,
            _marker: PhantomData::default(),
        }
    }

    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { lmdb_result(ffi::mdb_txn_commit(self.txn)) };
        self.txn = ptr::null_mut();
        result?;
        Ok(())
    }

    pub fn abort(self) {}
}

impl<'a> Drop for Txn<'a> {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            unsafe { ffi::mdb_txn_abort(self.txn) }
            self.txn = ptr::null_mut();
        }
    }
}
