use bindgen::callbacks::{IntKind, ParseCallbacks};
use std::process::Command;
use std::{env, fs, path::PathBuf};

#[derive(Debug)]
struct Callbacks;

impl ParseCallbacks for Callbacks {
    fn int_macro(&self, name: &str, _value: i64) -> Option<IntKind> {
        match name {
            "MDBX_SUCCESS"
            | "MDBX_KEYEXIST"
            | "MDBX_NOTFOUND"
            | "MDBX_PAGE_NOTFOUND"
            | "MDBX_CORRUPTED"
            | "MDBX_PANIC"
            | "MDBX_VERSION_MISMATCH"
            | "MDBX_INVALID"
            | "MDBX_MAP_FULL"
            | "MDBX_DBS_FULL"
            | "MDBX_READERS_FULL"
            | "MDBX_TLS_FULL"
            | "MDBX_TXN_FULL"
            | "MDBX_CURSOR_FULL"
            | "MDBX_PAGE_FULL"
            | "MDBX_MAP_RESIZED"
            | "MDBX_INCOMPATIBLE"
            | "MDBX_BAD_RSLOT"
            | "MDBX_BAD_TXN"
            | "MDBX_BAD_VALSIZE"
            | "MDBX_BAD_DBI"
            | "MDBX_LOG_DONTCHANGE"
            | "MDBX_DBG_DONTCHANGE"
            | "MDBX_RESULT_TRUE"
            | "MDBX_UNABLE_EXTEND_MAPSIZE"
            | "MDBX_PROBLEM"
            | "MDBX_LAST_LMDB_ERRCODE"
            | "MDBX_BUSY"
            | "MDBX_EMULTIVAL"
            | "MDBX_EBADSIGN"
            | "MDBX_WANNA_RECOVERY"
            | "MDBX_EKEYMISMATCH"
            | "MDBX_TOO_LARGE"
            | "MDBX_THREAD_MISMATCH"
            | "MDBX_TXN_OVERLAPPING"
            | "MDBX_LAST_ERRCODE" => Some(IntKind::Int),
            _ => Some(IntKind::UInt),
        }
    }
}

const LIBMDBX_REPO: &str = "https://gitflic.ru/project/erthink/libmdbx.git";
const LIBMDBX_TAG: &str = "v0.11.6";

fn main() {
    fs::remove_dir_all("libmdbx").unwrap();

    Command::new("git")
        .arg("clone")
        .arg(LIBMDBX_REPO)
        .arg("--depth")
        .arg("1")
        .arg("--branch")
        .arg(LIBMDBX_TAG)
        .output()
        .unwrap();

    Command::new("make")
        .arg("release-assets")
        .current_dir("libmdbx")
        .output()
        .unwrap();

    let mut mdbx = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    mdbx.push("libmdbx");
    mdbx.push("dist");
    let core_path = mdbx.join("mdbx.c");
    let core = fs::read_to_string(core_path.as_path()).unwrap();
    let core = core.replace("CharToOemBuffA(buf, buf, size)", "false");
    fs::write(core_path.as_path(), core).unwrap();

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    let bindings = bindgen::Builder::default()
        .header(mdbx.join("mdbx.h").to_string_lossy())
        .allowlist_var("^(MDBX|mdbx)_.*")
        .allowlist_type("^(MDBX|mdbx)_.*")
        .allowlist_function("^(MDBX|mdbx)_.*")
        .rustified_enum("^(MDBX_option_t|MDBX_cursor_op)")
        .size_t_is_usize(false)
        .ctypes_prefix("::libc")
        .parse_callbacks(Box::new(Callbacks))
        .layout_tests(false)
        .prepend_enum_name(false)
        .generate_comments(true)
        .disable_header_comment()
        .rustfmt_bindings(true)
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

    let mut cc_builder = cc::Build::new();
    let flags = format!("{:?}", cc_builder.get_compiler().cflags_env());
    cc_builder
        .flag_if_supported("-Wno-everything")
        .flag_if_supported("-miphoneos-version-min=10.0");

    if cfg!(windows) {
        let dst = cmake::Config::new(&mdbx)
            .define("MDBX_INSTALL_STATIC", "1")
            .define("MDBX_BUILD_CXX", "0")
            .define("MDBX_BUILD_TOOLS", "0")
            .define("MDBX_BUILD_SHARED_LIBRARY", "0")
            .define("MDBX_TXN_CHECKOWNER", "0")
            .define("MDBX_ENV_CHECKPID", "0")
            .define("MDBX_DISABLE_PAGECHECKS", "1")
            .define("MDBX_ENABLE_PGOP_STAT", "0")
            // Setting HAVE_LIBM=1 is necessary to override issues with `pow` detection on Windows
            .define("HAVE_LIBM", "1")
            .cflag("/w")
            .init_c_cfg(cc_builder)
            .build();

        println!("cargo:rustc-link-lib=mdbx");
        println!(
            "cargo:rustc-link-search=native={}",
            dst.join("lib").display()
        );

        if cfg!(windows) {
            println!(r"cargo:rustc-link-lib=ntdll");
            println!(r"cargo:rustc-link-search=C:\windows\system32");
        }
    } else {
        cc_builder
            .define("MDBX_BUILD_FLAGS", flags.as_str())
            .define("MDBX_TXN_CHECKOWNER", "0")
            .define("MDBX_ENV_CHECKPID", "0")
            .define("MDBX_OSX_SPEED_INSTEADOF_DURABILITY", "1")
            .define("MDBX_DISABLE_PAGECHECKS", "1")
            .define("MDBX_ENABLE_PGOP_STAT", "0")
            .file(mdbx.join("mdbx.c"))
            .compile("libmdbx.a");
    }
}
