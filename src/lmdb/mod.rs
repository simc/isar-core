#![allow(clippy::missing_safety_doc)]

use core::slice;
use lmdb_sys as ffi;
use std::cmp::{min, Ordering};
use std::ffi::c_void;

pub mod cursor;
pub mod db;
pub mod env;
pub mod error;
pub mod txn;

pub type KeyVal<'txn> = (Key<'txn>, &'txn [u8]);

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

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Key<'a>(pub &'a [u8]);

impl<'a> Ord for Key<'a> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        let len = min(self.0.len(), other.0.len());
        let cmp = (&self.0[0..len]).cmp(&other.0[0..len]);
        if cmp == Ordering::Equal {
            self.0.len().cmp(&other.0.len())
        } else {
            cmp
        }
    }
}

impl<'a> PartialOrd for Key<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
