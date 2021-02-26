#![feature(allocator_api)]
#![allow(clippy::missing_safety_doc)]

use isar_core::error::{illegal_arg, Result};
use std::ffi::CStr;
use std::os::raw::c_char;

#[macro_use]
mod error;

mod async_txn;
pub mod crud;
mod dart;
pub mod filter;
pub mod instance;
pub mod link;
pub mod query;
pub mod raw_object_set;
pub mod txn;
pub mod watchers;
pub mod where_clause;

pub unsafe fn from_c_str<'a>(str: *const c_char) -> Result<&'a str> {
    match CStr::from_ptr(str).to_str() {
        Ok(str) => Ok(str),
        Err(_) => illegal_arg("The provided String is not valid."),
    }
}

pub struct UintSend(&'static mut u32);

unsafe impl Send for UintSend {}

pub struct BoolSend(&'static mut bool);

unsafe impl Send for BoolSend {}
