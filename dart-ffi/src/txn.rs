use crate::async_txn::IsarAsyncTxn;
use crate::dart::DartPort;
use isar_core::instance::IsarInstance;
use isar_core::txn::IsarTxn;

#[no_mangle]
pub unsafe extern "C" fn isar_txn_begin<'env>(
    isar: &'env IsarInstance,
    txn: *mut *const IsarTxn<'env>,
    write: bool,
    silent: bool,
) -> i32 {
    isar_try! {
        let new_txn = isar.begin_txn(write,silent)?;
        let txn_ptr = Box::into_raw(Box::new(new_txn));
        txn.write(txn_ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_txn_begin_async(
    isar: &'static IsarInstance,
    txn: *mut *const IsarAsyncTxn,
    write: bool,
    silent: bool,
    port: DartPort,
) {
    let new_txn = IsarAsyncTxn::new(isar, write, silent, port);
    let txn_ptr = Box::into_raw(Box::new(new_txn));
    txn.write(txn_ptr);
}

#[no_mangle]
pub unsafe extern "C" fn isar_txn_commit(txn: *mut IsarTxn) -> i32 {
    isar_try! {
        let txn = Box::from_raw(txn);
        txn.commit()?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_txn_commit_async(txn: *mut IsarAsyncTxn) {
    let txn = Box::from_raw(txn);
    txn.commit();
}

#[no_mangle]
pub unsafe extern "C" fn isar_txn_abort(txn: *mut IsarTxn) {
    let txn = Box::from_raw(txn);
    txn.abort();
}

#[no_mangle]
pub unsafe extern "C" fn isar_txn_abort_async(txn: *mut IsarAsyncTxn) {
    let txn = Box::from_raw(txn);
    txn.abort();
}
