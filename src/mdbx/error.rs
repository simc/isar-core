use crate::mdbx::error::LmdbError::Other;
use libc::c_int;
use std::ffi::CStr;
use std::result::Result;

#[derive(Debug)]
pub enum LmdbError {
    KeyExist {},
    NotFound {},
    NoData {},
    MapFull {},
    Other { code: i32, message: String },
}

impl LmdbError {
    pub fn from_err_code(err_code: c_int) -> LmdbError {
        match err_code {
            ffi::MDBX_KEYEXIST => LmdbError::KeyExist {},
            ffi::MDBX_NOTFOUND => LmdbError::NotFound {},
            ffi::MDBX_ENODATA => LmdbError::NoData {},
            ffi::MDBX_MAP_FULL => LmdbError::MapFull {},
            other => unsafe {
                let err_raw = ffi::mdbx_strerror(other);
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
            LmdbError::KeyExist {} => ffi::MDBX_KEYEXIST,
            LmdbError::NotFound {} => ffi::MDBX_NOTFOUND,
            LmdbError::NoData {} => ffi::MDBX_ENODATA,
            LmdbError::MapFull {} => ffi::MDBX_MAP_FULL,
            LmdbError::Other {
                code: other,
                message: _,
            } => *other,
        }
    }
}

#[inline]
pub fn lmdb_result(err_code: c_int) -> Result<bool, LmdbError> {
    match err_code {
        ffi::MDBX_SUCCESS => Ok(false),
        ffi::MDBX_RESULT_TRUE => Ok(true),
        other => Err(LmdbError::from_err_code(other)),
    }
}
