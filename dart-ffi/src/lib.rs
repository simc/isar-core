#![feature(allocator_api)]
#![allow(clippy::missing_safety_doc)]

use isar_core::error::{illegal_arg, Result};
use std::ffi::CStr;
use std::mem;
use std::os::raw::c_char;

#[macro_use]
mod error;

pub mod crud;
mod dart;
pub mod filter;
pub mod index_key;
pub mod instance;
pub mod link;
pub mod query;
pub mod query_aggregation;
pub mod raw_object_set;
pub mod txn;
pub mod watchers;

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

#[no_mangle]
pub unsafe extern "C" fn isar_find_word_boundaries(
    input: *const c_char,
    length: *mut u32,
) -> *mut u32 {
    let str = from_c_str(input).unwrap();
    let mut result = vec![];
    for (offset, word) in str.unicode_word_indices() {
        result.push(offset as u32);
        result.push((offset + word.encode_utf16().count()) as u32);
    }
    result.shrink_to_fit();
    length.write(result.len() as u32);
    let result_ptr = result.as_mut_ptr();
    mem::forget(result);
    result_ptr
}

#[no_mangle]
pub unsafe extern "C" fn isar_free_word_boundaries(boundaries: *mut u32, length: u32) {
    Vec::from_raw_parts(boundaries, length as usize, length as usize);
}
