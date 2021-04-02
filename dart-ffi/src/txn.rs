use crate::dart::{dart_post_int, DartPort};
use crate::error::DartErrCode;
use isar_core::error::{IsarError, Result};
use isar_core::instance::IsarInstance;
use isar_core::txn::IsarTxn;
use once_cell::sync::Lazy;
use std::borrow::BorrowMut;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use threadpool::{Builder, ThreadPool};

static THREAD_POOL: Lazy<Mutex<ThreadPool>> = Lazy::new(|| Mutex::new(Builder::new().build()));

pub fn run_async<F: FnOnce() + Send + 'static>(job: F) {
    THREAD_POOL.lock().unwrap().execute(job);
}

type AsyncJob = (Box<dyn FnOnce() + Send + 'static>, bool);

#[no_mangle]
pub unsafe extern "C" fn isar_txn_begin(
    isar: &'static IsarInstance,
    txn: *mut *const IsarDartTxn,
    sync: bool,
    write: bool,
    silent: bool,
    port: DartPort,
) -> i32 {
    isar_try! {
        let new_txn = if sync {
            IsarDartTxn::begin_sync(isar, write, silent)?
        } else {
            IsarDartTxn::begin_async(isar, write, silent, port)
        };
        let txn_ptr = Box::into_raw(Box::new(new_txn));
        txn.write(txn_ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_txn_finish(txn: *mut IsarDartTxn, commit: bool) -> i32 {
    let txn = Box::from_raw(txn);
    isar_try! {
        txn.finish(commit)?;
    }
}

pub struct IsarTxnSend(IsarTxn<'static>);

unsafe impl Send for IsarTxnSend {}

pub enum IsarDartTxn {
    Sync {
        txn: Option<IsarTxn<'static>>,
    },
    Async {
        tx: Sender<AsyncJob>,
        port: DartPort,
        txn: Arc<Mutex<Option<IsarTxnSend>>>,
    },
}

impl IsarDartTxn {
    fn begin_sync(isar: &'static IsarInstance, write: bool, silent: bool) -> Result<IsarDartTxn> {
        let sync_txn = IsarDartTxn::Sync {
            txn: Some(isar.begin_txn(write, silent)?),
        };
        Ok(sync_txn)
    }

    fn begin_async(
        isar: &'static IsarInstance,
        write: bool,
        silent: bool,
        port: DartPort,
    ) -> IsarDartTxn {
        let (tx, rx): (Sender<AsyncJob>, Receiver<AsyncJob>) = mpsc::channel();
        let txn = Arc::new(Mutex::new(None));
        let txn_clone = txn.clone();
        run_async(move || {
            let new_txn = isar.begin_txn(write, silent);
            match new_txn {
                Ok(new_txn) => {
                    txn_clone.lock().unwrap().replace(IsarTxnSend(new_txn));
                    dart_post_int(port, 0);
                    loop {
                        let (job, stop) = rx.recv().unwrap();
                        job();
                        if stop {
                            break;
                        }
                    }
                }
                Err(e) => {
                    dart_post_int(port, e.into_dart_err_code());
                }
            }
        });

        IsarDartTxn::Async { tx, port, txn }
    }

    pub fn exec_async_internal<F: FnOnce() -> Result<()> + Send + 'static>(
        job: F,
        port: DartPort,
        tx: Sender<AsyncJob>,
        stop: bool,
    ) {
        let handle_response_job = move || {
            let result = match job() {
                Ok(()) => 0,
                Err(e) => e.into_dart_err_code(),
            };
            dart_post_int(port, result);
        };
        tx.send((Box::new(handle_response_job), stop)).unwrap();
    }

    pub fn exec(
        &mut self,
        job: Box<dyn FnOnce(&mut IsarTxn) -> Result<()> + Send + 'static>,
    ) -> Result<()> {
        match self.borrow_mut() {
            IsarDartTxn::Sync { ref mut txn } => {
                if let Some(ref mut txn) = txn {
                    job(txn)
                } else {
                    Err(IsarError::TransactionClosed {})
                }
            }
            IsarDartTxn::Async { txn, tx, port } => {
                let txn = txn.clone();
                let job = move || -> Result<()> {
                    let mut lock = txn.lock().unwrap();
                    if let Some(ref mut txn) = *lock {
                        job(&mut txn.0)
                    } else {
                        Err(IsarError::TransactionClosed {})
                    }
                };
                IsarDartTxn::exec_async_internal(job, *port, tx.clone(), false);
                Ok(())
            }
        }
    }

    pub fn finish(self, commit: bool) -> Result<()> {
        match self {
            IsarDartTxn::Sync { mut txn } => {
                if let Some(txn) = txn.take() {
                    if commit {
                        txn.commit()
                    } else {
                        txn.abort();
                        Ok(())
                    }
                } else {
                    Err(IsarError::TransactionClosed {})
                }
            }
            IsarDartTxn::Async { txn, tx, port } => {
                let txn = txn.clone();
                let job = move || -> Result<()> {
                    let mut lock = txn.lock().unwrap();
                    if let Some(txn) = (*lock).take() {
                        if commit {
                            txn.0.commit()
                        } else {
                            txn.0.abort();
                            Ok(())
                        }
                    } else {
                        Err(IsarError::TransactionClosed {})
                    }
                };
                IsarDartTxn::exec_async_internal(job, port, tx.clone(), true);
                Ok(())
            }
        }
    }
}
