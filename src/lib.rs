#![allow(clippy::new_without_default)]
#![feature(num_as_ne_bytes)]

#[cfg(not(target_endian = "little"))]
compile_error!("Only little endian systems are supported.");

#[cfg(not(target_pointer_width = "64"))]
compile_error!("Only 64-bit systems are supported at this time.");

pub mod collection;
pub mod error;
mod index;
pub mod instance;
mod link;
mod lmdb;
pub mod object;
pub mod query;
pub mod schema;
pub mod txn;
mod utils;
pub mod watch;
