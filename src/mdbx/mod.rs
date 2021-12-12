#![allow(clippy::missing_safety_doc)]

use core::slice;
use std::cmp::{min, Ordering};
use std::ffi::c_void;

pub mod cursor;
pub mod db;
pub mod env;
pub mod error;
pub mod txn;

pub type KeyVal<'txn> = (&'txn [u8], &'txn [u8]);

pub const EMPTY_KEY: ffi::MDBX_val = ffi::MDBX_val {
    iov_len: 0,
    iov_base: 0 as *mut c_void,
};

pub const EMPTY_VAL: ffi::MDBX_val = ffi::MDBX_val {
    iov_len: 0,
    iov_base: 0 as *mut c_void,
};

#[inline]
pub unsafe fn from_mdb_val<'a>(val: &ffi::MDBX_val) -> &'a [u8] {
    slice::from_raw_parts(val.iov_base as *const u8, val.iov_len as usize)
}

#[inline]
pub unsafe fn from_mdb_val_mut<'a>(val: &mut ffi::MDBX_val) -> &'a mut [u8] {
    slice::from_raw_parts_mut(val.iov_base as *mut u8, val.iov_len as usize)
}

#[inline]
pub unsafe fn to_mdb_val(value: &[u8]) -> ffi::MDBX_val {
    ffi::MDBX_val {
        iov_len: value.len(),
        iov_base: value.as_ptr() as *mut libc::c_void,
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct ByteKey<'a> {
    pub bytes: &'a [u8],
}

impl<'a> ByteKey<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        ByteKey { bytes }
    }

    #[inline]
    pub fn cmp_bytes(&self, other: &[u8]) -> Ordering {
        let len = min(self.bytes.len(), other.len());
        let cmp = (&self.bytes[0..len]).cmp(&other[0..len]);
        if cmp == Ordering::Equal {
            self.bytes.len().cmp(&other.len())
        } else {
            cmp
        }
    }
}

impl<'a> Ord for ByteKey<'a> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_bytes(other.bytes)
    }
}

impl<'a> PartialOrd for ByteKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
