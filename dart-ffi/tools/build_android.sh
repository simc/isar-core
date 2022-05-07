#!/bin/bash

if [[ "$(uname -s)" == "Darwin" ]]; then
    export NDK_HOST_TAG="darwin-x86_64"
elif [[ "$(uname -s)" == "Linux" ]]; then
    export NDK_HOST_TAG="linux-x86_64"
else
    echo "Unsupported OS."
    exit
fi

NDK=${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-"$ANDROID_SDK_ROOT/ndk"}}
COMPILER_DIR="$NDK/toolchains/llvm/prebuilt/$NDK_HOST_TAG/bin"
export PATH="$COMPILER_DIR:$PATH"

echo "$COMPILER_DIR"

export CARGO_TARGET_I686_LINUX_ANDROID_AR="$COMPILER_DIR/i686-linux-android-ar"
export CARGO_TARGET_I686_LINUX_ANDROID_LINKER="$COMPILER_DIR/i686-linux-android29-clang"
export CARGO_TARGET_X86_64_LINUX_ANDROID_AR="$COMPILER_DIR/x86_64-linux-android-ar"
export CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER="$COMPILER_DIR/x86_64-linux-android29-clang"
export CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_AR="$COMPILER_DIR/armv7a-linux-androideabi-ar"
export CARGO_TARGET_ARMV7_LINUX_ANDROIDEABI_LINKER="$COMPILER_DIR/armv7a-linux-androideabi29-clang"
export CARGO_TARGET_AARCH64_LINUX_ANDROID_AR="$COMPILER_DIR/aarch64-linux-android-ar"
export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$COMPILER_DIR/aarch64-linux-android29-clang"

if [ "$1" = "x86" ]; then
  rustup target add i686-linux-android
  cargo +nightly build -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target i686-linux-android --release
  mv "target/i686-linux-android/release/libisar.so" "libisar_android_x86.so"
elif [ "$1" = "x64" ]; then
  rustup target add x86_64-linux-android
  cargo +nightly build -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target x86_64-linux-android --release
  mv "target/x86_64-linux-android/release/libisar.so" "libisar_android_x64.so"
elif [ "$1" = "armv7" ]; then
  rustup target add armv7-linux-androideabi
  cargo +nightly build -Z build-std=std,panic_abort --target armv7-linux-androideabi --release
  mv "target/armv7-linux-androideabi/release/libisar.so" "libisar_android_armv7.so"
else
  rustup target add aarch64-linux-android
  cargo +nightly build -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target aarch64-linux-android --release
  mv "target/aarch64-linux-android/release/libisar.so" "libisar_android_arm64.so"
fi






