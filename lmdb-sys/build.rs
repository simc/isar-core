extern crate cc;
extern crate pkg_config;

#[cfg(feature = "bindgen")]
extern crate bindgen;

#[cfg(feature = "bindgen")]
#[path = "bindgen.rs"]
mod generate;

use cc::Build;
use std::env;
use std::path::PathBuf;

fn main() {
    #[cfg(feature = "bindgen")]
    generate::generate();

    let mut lmdb = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    lmdb.push("lmdb");
    lmdb.push("libraries");
    lmdb.push("liblmdb");

    if pkg_config::find_library("liblmdb").is_err() {
        let mut builder = Build::new();

        builder
            .file(lmdb.join("mdb.c"))
            .file(lmdb.join("midl.c"))
            .flag_if_supported("-Wno-unused-parameter")
            .flag_if_supported("-Wbad-function-cast")
            .flag_if_supported("-Wuninitialized");
        builder.compile("liblmdb.a")
    }
}
