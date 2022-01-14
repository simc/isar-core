#!/bin/bash

case $(uname | tr '[:upper:]' '[:lower:]') in
  linux*)
    cargo build --target x86_64-unknown-linux-gnu --release
    mv "target/x86_64-unknown-linux-gnu/release/libisar.so" "libisar_linux_x64.so"
    ;;
  darwin*)
    if [ "$1" = "x64" ]; then
      rustup target add x86_64-apple-darwin
      cargo build --target x86_64-apple-darwin --release
      mv "target/x86_64-apple-darwin/release/libisar.dylib" "libisar_macos_x64.dylib"
    else
      rustup target add aarch64-apple-darwin
      cargo build --target aarch64-apple-darwin --release
      mv "target/aarch64-apple-darwin/release/libisar.dylib" "libisar_macos_arm64.dylib"
    fi
    ;;
  *)
    cargo build --target x86_64-pc-windows-msvc --release
    mv "target/x86_64-pc-windows-msvc/release/isar.dll" "isar_windows_x64.dll"
    ;;
esac