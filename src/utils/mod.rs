#![allow(clippy::missing_safety_doc)]

use crate::error::{IsarError, Result};
use std::convert::TryInto;
use std::mem::transmute;

#[macro_use]
pub mod debug;

#[macro_export]
macro_rules! option (
    ($option:expr, $value:expr) => {
        if $option {
            Some($value)
        } else {
            None
        }
    };
);

#[inline]
pub fn signed_to_unsigned(value: i64) -> u64 {
    let unsigned = unsafe { transmute::<i64, u64>(value) };
    unsigned ^ 1 << 63
}

#[inline]
pub fn unsigned_to_signed(value: u64) -> i64 {
    unsafe { transmute::<u64, i64>(value ^ 1 << 63) }
}

pub const MIN_OID: i64 = -(1 << 47);
pub const MAX_OID: i64 = (1 << 47) - 1;
const OID_PREFIX_MASK: u64 = 0xffff_ffff_ffff;
const OID_OFFSET: i64 = i64::MIN - MIN_OID;

pub fn oid_to_bytes(oid: i64, prefix: u16) -> Result<[u8; 8]> {
    if oid >= MIN_OID && oid <= MAX_OID {
        let unsigned = signed_to_unsigned(oid + OID_OFFSET) | ((prefix as u64) << 48);
        Ok(unsigned.to_le_bytes())
    } else {
        Err(IsarError::InvalidObjectId {})
    }
}

pub fn oid_from_bytes(bytes: &[u8]) -> (i64, u16) {
    let unsigned = u64::from_le_bytes(bytes.try_into().unwrap());
    let oid = unsigned_to_signed(unsigned & OID_PREFIX_MASK) - OID_OFFSET;
    let prefix = (unsigned >> 48) as u16;
    (oid, prefix)
}
