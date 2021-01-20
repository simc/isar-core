use crate::dart::{dart_post_int, DartPort};
use crate::error::DartErrCode;
use isar_core::error::{IsarError, Result};
use isar_core::instance::IsarInstance;
use isar_core::txn::IsarTxn;
use once_cell::sync::Lazy;
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

struct IsarTxnSend(IsarTxn<'static>);

unsafe impl Send for IsarTxnSend {}

pub struct IsarAsyncTxn {
    tx: Sender<AsyncJob>,
    port: DartPort,
    txn: Arc<Mutex<Option<IsarTxnSend>>>,
}

impl IsarAsyncTxn {
    pub fn new(isar: &'static IsarInstance, write: bool, port: DartPort) -> Self {
        let (tx, rx): (Sender<AsyncJob>, Receiver<AsyncJob>) = mpsc::channel();
        let async_txn = IsarAsyncTxn {
            tx,
            port,
            txn: Arc::new(Mutex::new(None)),
        };
        let txn = async_txn.txn.clone();
        run_async(move || {
            let new_txn = isar.begin_txn(write);
            match new_txn {
                Ok(new_txn) => {
                    txn.lock().unwrap().replace(IsarTxnSend(new_txn));
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

        async_txn
    }

    pub fn exec_internal<F: FnOnce() -> Result<()> + Send + 'static>(&self, job: F, stop: bool) {
        let port = self.port;
        let handle_response_job = move || {
            let result = match job() {
                Ok(()) => 0,
                Err(e) => e.into_dart_err_code(),
            };
            dart_post_int(port, result);
        };
        self.tx.send((Box::new(handle_response_job), stop)).unwrap();
    }

    pub fn exec<F: FnOnce(&mut IsarTxn) -> Result<()> + Send + 'static>(&self, job: F) {
        let txn = self.txn.clone();
        let job = move || -> Result<()> {
            let mut lock = txn.lock().unwrap();
            if let Some(ref mut txn) = *lock {
                job(&mut txn.0)
            } else {
                Err(IsarError::TransactionClosed {})
            }
        };
        self.exec_internal(job, false);
    }

    pub fn commit(self) {
        let txn = self.txn.clone();
        let job = move || -> Result<()> {
            let mut lock = txn.lock().unwrap();
            if let Some(txn) = (*lock).take() {
                txn.0.commit()
            } else {
                Err(IsarError::TransactionClosed {})
            }
        };
        self.exec_internal(job, true);
    }

    pub fn abort(self) {
        let txn = self.txn.clone();
        let job = move || -> Result<()> {
            let mut txn = txn.lock().unwrap();
            if let Some(txn) = txn.take() {
                txn.0.abort();
                Ok(())
            } else {
                Err(IsarError::TransactionClosed {})
            }
        };
        self.exec_internal(job, true);
    }
}
