use crate::error::{IsarError, Result};
use crate::lmdb::error::lmdb_result;
use crate::lmdb::txn::Txn;
use crate::lmdb::{from_mdb_val, from_mdb_val_mut, to_mdb_val};
use boring::error::ErrorStack;
use boring::symm::{Cipher, Crypter, Mode};
use core::ptr;
use lmdb_sys as ffi;
use lmdb_sys::MDB_val;
use std::ffi::CString;

const GCM_TAG_SIZE: usize = 16;
const ENOMEM: i32 = 12;

pub struct Env {
    env: *mut ffi::MDB_env,
}

unsafe impl Sync for Env {}
unsafe impl Send for Env {}

impl Env {
    pub fn create(
        path: &str,
        max_dbs: u32,
        max_size: usize,
        encryption_key: Option<&[u8]>,
    ) -> Result<Env> {
        let path = CString::new(path.as_bytes()).unwrap();
        let mut env: *mut ffi::MDB_env = ptr::null_mut();
        unsafe {
            lmdb_result(ffi::mdb_env_create(&mut env))?;

            let err_code = ffi::mdb_env_set_maxdbs(env, max_dbs);
            if err_code != ffi::MDB_SUCCESS {
                ffi::mdb_env_close(env);
                lmdb_result(err_code)?;
            }

            if let Some(encryption_key) = encryption_key {
                let key = to_mdb_val(encryption_key);
                let err_code = ffi::mdb_env_set_encrypt(
                    env,
                    Some(crypto),
                    &key as *const _,
                    GCM_TAG_SIZE as u32,
                );
                if err_code != ffi::MDB_SUCCESS {
                    ffi::mdb_env_close(env);
                    lmdb_result(err_code)?;
                }
            }

            let mut current_max_size = max_size;
            let mut err_code;
            loop {
                err_code = ffi::mdb_env_set_mapsize(env, max_size);
                if err_code != ffi::MDB_SUCCESS {
                    ffi::mdb_env_close(env);
                    lmdb_result(err_code)?;
                }
                err_code = ffi::mdb_env_open(env, path.as_ptr(), ffi::MDB_NOTLS, 0o600);
                match err_code {
                    ffi::MDB_SUCCESS => {
                        return Ok(Env { env });
                    }
                    ENOMEM => {
                        if current_max_size * 10 > max_size {
                            current_max_size = (current_max_size as f64 / 1.5) as usize;
                        } else {
                            break;
                        }
                    }
                    _ => {
                        break;
                    }
                }
            }

            ffi::mdb_env_close(env);
            if err_code == 2 {
                return Err(IsarError::PathError {});
            } else {
                lmdb_result(err_code)?;
            }
        }
        unreachable!();
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

unsafe extern "C" fn crypto(
    src: *const MDB_val,
    dst: *mut MDB_val,
    key: *const MDB_val,
    encdec: ::libc::c_int,
) -> ::libc::c_int {
    fn run(
        src_bytes: &[u8],
        dst_bytes: &mut [u8],
        key_bytes: &[u8],
        iv_bytes: &[u8],
        tag_bytes: &mut [u8],
        mode: Mode,
    ) -> std::result::Result<(), ErrorStack> {
        let cipher = Cipher::aes_256_gcm();
        let mut crypter = Crypter::new(cipher, mode, key_bytes, Some(&iv_bytes[0..12]))?;
        crypter.pad(false);
        crypter.update(src_bytes, dst_bytes)?;
        if let Mode::Decrypt = mode {
            crypter.set_tag(tag_bytes)?;
        }
        let count = crypter.finalize(dst_bytes)?;
        if let Mode::Encrypt = mode {
            crypter.get_tag(tag_bytes)?;
        }
        assert_eq!(count, 0);
        Ok(())
    }

    let src_bytes = from_mdb_val(src.as_ref().unwrap());
    let dst = dst.as_mut().unwrap();
    let dst_bytes = from_mdb_val_mut(dst);
    let key_bytes = from_mdb_val(key.as_ref().unwrap());
    let iv_bytes = from_mdb_val(key.add(1).as_ref().unwrap());
    let tag_bytes = from_mdb_val_mut((key.add(2) as *mut MDB_val).as_mut().unwrap());

    let mode = if encdec == 1 {
        Mode::Encrypt
    } else {
        Mode::Decrypt
    };

    if run(src_bytes, dst_bytes, key_bytes, iv_bytes, tag_bytes, mode).is_ok() {
        0
    } else {
        1
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
        Env::create(dir.path().to_str().unwrap(), 50, 100000, None).unwrap()
    }
}
