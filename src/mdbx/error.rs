use crate::mdbx::error::MdbxError::Other;
use libc::c_int;
use std::ffi::CStr;
use std::result::Result;

#[derive(Debug)]
pub enum MdbxError {
    KeyExist {},
    NotFound {},
    NoData {},
    MapFull {},
    Other { code: i32, message: String },
}

impl MdbxError {
    pub fn from_err_code(err_code: c_int) -> MdbxError {
        match err_code {
            ffi::MDBX_KEYEXIST => MdbxError::KeyExist {},
            ffi::MDBX_NOTFOUND => MdbxError::NotFound {},
            ffi::MDBX_ENODATA => MdbxError::NoData {},
            ffi::MDBX_MAP_FULL => MdbxError::MapFull {},
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
            MdbxError::KeyExist {} => ffi::MDBX_KEYEXIST,
            MdbxError::NotFound {} => ffi::MDBX_NOTFOUND,
            MdbxError::NoData {} => ffi::MDBX_ENODATA,
            MdbxError::MapFull {} => ffi::MDBX_MAP_FULL,
            MdbxError::Other {
                code: other,
                message: _,
            } => *other,
        }
    }
}

#[inline]
pub fn mdbx_result(err_code: c_int) -> Result<bool, MdbxError> {
    match err_code {
        ffi::MDBX_SUCCESS => Ok(false),
        ffi::MDBX_RESULT_TRUE => Ok(true),
        other => Err(MdbxError::from_err_code(other)),
    }
}
