#![allow(clippy::missing_safety_doc)]

use crate::mdbx::cursor::Cursor;
use core::slice;
use std::cmp::{min, Ordering};
use std::collections::HashSet;
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
pub unsafe fn to_mdb_val(value: &[u8]) -> ffi::MDBX_val {
    ffi::MDBX_val {
        iov_len: value.len() as ffi::size_t,
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

pub fn debug_dump_db(cursor: &mut Cursor, int_key: bool) -> HashSet<(Vec<u8>, Vec<u8>)> {
    let mut entries = HashSet::new();
    let lower = if int_key {
        u64::MIN.to_le_bytes().to_vec()
    } else {
        vec![]
    };
    let upper = u64::MAX.to_le_bytes();
    cursor
        .iter_between(&lower, &upper, false, false, true, |_, key, val| {
            entries.insert((key.to_vec(), val.to_vec()));
            Ok(true)
        })
        .unwrap();
    entries
}
