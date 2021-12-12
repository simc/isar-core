use crate::error::{IsarError, Result};
use crate::mdbx::error::lmdb_result;
use crate::mdbx::txn::Txn;
use core::ptr;
use std::ffi::CString;

pub struct Env {
    env: *mut ffi::MDBX_env,
}

unsafe impl Sync for Env {}
unsafe impl Send for Env {}

impl Env {
    pub fn create(path: &str, max_dbs: u64, max_size: usize) -> Result<Env> {
        let path = CString::new(path.as_bytes()).unwrap();
        let mut env: *mut ffi::MDBX_env = ptr::null_mut();
        unsafe {
            lmdb_result(ffi::mdbx_env_create(&mut env))?;
            lmdb_result(ffi::mdbx_env_set_option(env, ffi::MDBX_opt_max_db, max_dbs))?;

            let err_code = ffi::mdbx_env_open(
                env,
                path.as_ptr(),
                ffi::MDBX_NOTLS | ffi::MDBX_EXCLUSIVE | ffi::MDBX_NOMETASYNC,
                0o600,
            );

            match err_code {
                ffi::MDBX_SUCCESS => Ok(Env { env }),
                ffi::MDBX_EPERM | ffi::MDBX_ENOFILE => Err(IsarError::PathError {}),
                e => {
                    lmdb_result(e)?;
                    unreachable!()
                }
            }
        }
    }

    pub fn txn(&self, write: bool) -> Result<Txn> {
        let flags = if write { 0 } else { ffi::MDBX_RDONLY };
        let mut txn: *mut ffi::MDBX_txn = ptr::null_mut();
        unsafe {
            lmdb_result(ffi::mdbx_txn_begin_ex(
                self.env,
                ptr::null_mut(),
                flags,
                &mut txn,
                ptr::null_mut(),
            ))?;
        }
        Ok(Txn::new(txn))
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        if !self.env.is_null() {
            unsafe {
                ffi::mdbx_env_close_ex(self.env, false);
            }
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
