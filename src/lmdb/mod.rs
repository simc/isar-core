#![allow(clippy::missing_safety_doc)]

use core::slice;
use lmdb_sys as ffi;
use std::ffi::c_void;

pub mod cursor;
pub mod db;
pub mod env;
pub mod error;
pub mod txn;

pub type KeyVal<'txn> = (&'txn [u8], &'txn [u8]);

pub const EMPTY_KEY: ffi::MDB_val = ffi::MDB_val {
    mv_size: 0,
    mv_data: 0 as *mut c_void,
};

pub const EMPTY_VAL: ffi::MDB_val = ffi::MDB_val {
    mv_size: 0,
    mv_data: 0 as *mut c_void,
};

#[inline]
pub unsafe fn from_mdb_val<'a>(val: ffi::MDB_val) -> &'a [u8] {
    slice::from_raw_parts(val.mv_data as *const u8, val.mv_size as usize)
}

#[inline]
pub unsafe fn to_mdb_val(value: &[u8]) -> ffi::MDB_val {
    ffi::MDB_val {
        mv_size: value.len(),
        mv_data: value.as_ptr() as *mut libc::c_void,
    }
}

#[inline]
pub fn check_below_upper_key(mut key: &[u8], upper_key: &[u8]) -> bool {
    if upper_key.len() < key.len() {
        key = &key[0..(upper_key.len())]
    }
    upper_key >= key
}
