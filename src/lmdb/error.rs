use crate::lmdb::error::LmdbError::Other;
use libc::c_int;
use lmdb_sys as ffi;
use lmdb_sys::mdb_strerror;
use std::ffi::CStr;
use std::result::Result;

#[derive(Debug)]
pub enum LmdbError {
    KeyExist {},
    NotFound {},
    MapFull {},
    CryptoFail {},
    Other { code: i32, message: String },
}

impl LmdbError {
    pub fn from_err_code(err_code: c_int) -> LmdbError {
        match err_code {
            ffi::MDB_KEYEXIST => LmdbError::KeyExist {},
            ffi::MDB_NOTFOUND => LmdbError::NotFound {},
            ffi::MDB_MAP_FULL => LmdbError::MapFull {},
            ffi::MDB_ENV_ENCRYPTION | ffi::MDB_CRYPTO_FAIL => LmdbError::CryptoFail {},
            other => unsafe {
                let err_raw = mdb_strerror(other);
                let err = CStr::from_ptr(err_raw);
                Other {
                    code: err_code,
                    message: err.to_str().unwrap().to_string(),
                }
            },
        }
    }

    pub fn to_err_code(&self) -> i32 {
        match self {
            LmdbError::KeyExist {} => ffi::MDB_KEYEXIST,
            LmdbError::NotFound {} => ffi::MDB_NOTFOUND,
            LmdbError::MapFull {} => ffi::MDB_MAP_FULL,
            LmdbError::CryptoFail {} => ffi::MDB_CRYPTO_FAIL,
            LmdbError::Other {
                code: other,
                message: _,
            } => *other,
        }
    }
}

#[inline]
pub fn lmdb_result(err_code: c_int) -> Result<(), LmdbError> {
    if err_code == ffi::MDB_SUCCESS {
        Ok(())
    } else {
        Err(LmdbError::from_err_code(err_code))
    }
}
