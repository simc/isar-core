use crate::error::{IsarError, Result};
use crate::lmdb::error::lmdb_result;
use crate::lmdb::txn::Txn;
use core::ptr;
use lmdb_sys as ffi;
use std::ffi::CString;

pub struct Env {
    env: *mut ffi::MDB_env,
}

unsafe impl Sync for Env {}
unsafe impl Send for Env {}

impl Env {
    pub fn create(path: &str, max_dbs: u32, max_size: usize) -> Result<Env> {
        let path = CString::new(path.as_bytes()).unwrap();
        let mut env: *mut ffi::MDB_env = ptr::null_mut();
        unsafe {
            lmdb_result(ffi::mdb_env_create(&mut env))?;

            let err_code = ffi::mdb_env_set_mapsize(env, max_size);
            if err_code != ffi::MDB_SUCCESS {
                ffi::mdb_env_close(env);
                lmdb_result(err_code)?;
            }

            let err_code = ffi::mdb_env_set_maxdbs(env, max_dbs);
            if err_code != ffi::MDB_SUCCESS {
                ffi::mdb_env_close(env);
                lmdb_result(err_code)?;
            }

            let err_code = ffi::mdb_env_open(env, path.as_ptr(), 0, 0o600);
            if err_code != ffi::MDB_SUCCESS {
                ffi::mdb_env_close(env);
                if err_code == 2 {
                    Err(IsarError::PathError {})?;
                } else {
                    lmdb_result(err_code)?;
                }
            }
        }
        Ok(Env { env })
    }

    pub fn txn(&self, write: bool) -> Result<Txn> {
        let flags = if write { 0 } else { ffi::MDB_RDONLY };
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        unsafe {
            lmdb_result(ffi::mdb_txn_begin(
                self.env,
                ptr::null_mut(),
                flags,
                &mut txn,
            ))?
        }
        Ok(Txn::new(txn, write))
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        if !self.env.is_null() {
            unsafe { ffi::mdb_env_close(self.env) }
            self.env = ptr::null_mut();
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create() {
        get_env();
    }

    pub fn get_env() -> Env {
        let dir = tempdir().unwrap();
        Env::create(dir.path().to_str().unwrap(), 50, 100000).unwrap()
    }
}
