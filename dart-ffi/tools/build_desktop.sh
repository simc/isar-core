#!/bin/bash

case $(uname | tr '[:upper:]' '[:lower:]') in
  linux*)
    cargo build --release
    mv "target/release/libisar_core_dart_ffi.so" "libisar_linux_x64.so"
    ;;
  darwin*)
    if [ "$1" = "x64" ]; then
      rustup target add x86_64-apple-darwin
      cargo build --release --target x86_64-apple-darwin
      mv "target/x86_64-apple-darwin/release/libisar_core_dart_ffi.dylib" "libisar_macos_x64.dylib"
    else
      rustup target add aarch64-apple-darwin
      cargo build --release --target aarch64-apple-darwin
      mv "target/aarch64-apple-darwin/release/libisar_core_dart_ffi.dylib" "libisar_macos_arm64.dylib"
    fi
    ;;
  *)
    cargo build --release
    mv "target/release/isar_core_dart_ffi.dll" "isar_windows_x64.dll"
    ;;
esac