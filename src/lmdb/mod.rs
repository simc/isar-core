#![allow(clippy::missing_safety_doc)]

use crate::error::{IsarError, Result};
use crate::instance::IsarInstance;
use core::slice;
use std::cmp::{min, Ordering};
use std::convert::TryInto;
use std::ffi::c_void;
use std::mem::transmute;

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

const ID_PREFIX_MASK: u64 = 0xffff_ffff_ffff;
const ID_OFFSET: i64 = i64::MIN - IsarInstance::MIN_ID;

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

pub trait Key: Ord + Copy {
    fn as_bytes(&self) -> &[u8];

    fn cmp_bytes(&self, other: &[u8]) -> Ordering;
}

pub fn verify_id(id: i64) -> Result<()> {
    if id < IsarInstance::MIN_ID || id > IsarInstance::MAX_ID {
        Err(IsarError::InvalidObjectId {})
    } else {
        Ok(())
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct IntKey {
    key: u64,
}

impl IntKey {
    pub fn new(prefix: u16, id: i64) -> Self {
        let unsigned_id = unsafe { transmute::<i64, u64>(id + ID_OFFSET) };
        let unsigned_id = unsigned_id ^ 1 << 63;
        let unsigned = unsigned_id | ((prefix as u64) << 48);
        IntKey { key: unsigned }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        IntKey {
            key: u64::from_le_bytes(bytes.try_into().unwrap()),
        }
    }

    pub fn get_prefix(&self) -> u16 {
        (self.key >> 48) as u16
    }

    pub fn get_id(&self) -> i64 {
        let signed = unsafe { transmute::<u64, i64>((self.key & ID_PREFIX_MASK) ^ 1 << 63) };
        signed - ID_OFFSET
    }
}

impl Key for IntKey {
    fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(&self.key as *const u64 as *const u8, 8) }
    }

    #[inline]
    fn cmp_bytes(&self, other: &[u8]) -> Ordering {
        let other_key = u64::from_le_bytes(other.try_into().unwrap());
        self.key.cmp(&other_key)
    }
}

impl Ord for IntKey {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_bytes(other.as_bytes())
    }
}

impl PartialOrd for IntKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct ByteKey<'a> {
    bytes: &'a [u8],
}

impl<'a> ByteKey<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        ByteKey { bytes }
    }
}

impl<'a> Key for ByteKey<'a> {
    fn as_bytes(&self) -> &[u8] {
        self.bytes
    }

    #[inline]
    fn cmp_bytes(&self, other: &[u8]) -> Ordering {
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
        self.cmp_bytes(other.as_bytes())
    }
}

impl<'a> PartialOrd for ByteKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
