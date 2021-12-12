#![allow(clippy::new_without_default)]

#[cfg(not(target_endian = "little"))]
compile_error!("Only little endian systems are supported.");

#[cfg(not(target_pointer_width = "64"))]
compile_error!("Only 64-bit systems are supported at this time.");

pub mod collection;
mod cursor;
pub mod error;
pub mod index;
pub mod instance;
pub mod key;
mod link;
mod mdbx;
pub mod object;
pub mod query;
pub mod schema;
pub mod txn;
pub mod verify;
pub mod watch;

// todo check missing property in isarobject
